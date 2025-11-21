use std::sync::Arc;

use axum::{
    extract::{Path, State},
    Json,
};
use openleadr_wire::ven::VenId;
use reqwest::StatusCode;
use serde::Deserialize;
use tracing::{info, trace};
use validator::Validate;

use openleadr_wire::resource::{BlResourceRequest, Resource, ResourceId, ResourceRequest};

use crate::{
    api::{AppResponse, TargetQueryParams, ValidatedJson, ValidatedQuery},
    data_source::{ResourceCrud, VenObjectPrivacy},
    error::AppError,
    jwt::{Scope, User},
};

pub async fn get_all(
    State(resource_source): State<Arc<dyn ResourceCrud>>,
    ValidatedQuery(query_params): ValidatedQuery<QueryParams>,
    User(user): User,
) -> AppResponse<Vec<Resource>> {
    trace!(?query_params);

    let resources = if user.scope.contains(Scope::ReadAll) {
        resource_source.retrieve_all(&query_params, &None).await?
    } else if user.scope.contains(Scope::ReadVenObjects) {
        resource_source
            .retrieve_all(&query_params, &Some(user.client_id()?))
            .await?
    } else {
        return Err(AppError::Forbidden(
            "Missing 'read_all' or 'read_ven_objects' scope",
        ));
    };

    trace!(
        client_id = user.sub,
        "retrieved {} resources",
        resources.len()
    );

    Ok(Json(resources))
}

pub async fn get(
    State(resource_source): State<Arc<dyn ResourceCrud>>,
    Path(id): Path<ResourceId>,
    User(user): User,
) -> AppResponse<Resource> {
    let resource = if user.scope.contains(Scope::ReadAll) {
        resource_source.retrieve(&id, &None).await?
    } else if user.scope.contains(Scope::ReadVenObjects) {
        resource_source
            .retrieve(&id, &Some(user.client_id()?))
            .await?
    } else {
        return Err(AppError::Forbidden(
            "Missing 'read_all' or 'read_ven_objects' scope",
        ));
    };

    trace!(
        %resource.id,
        resource.resource_name=resource.content.resource_name,
        client_id = user.sub,
        "resource retrieved"
    );

    Ok(Json(resource))
}

pub async fn add(
    State(resource_source): State<Arc<dyn ResourceCrud>>,
    State(object_privacy): State<Arc<dyn VenObjectPrivacy>>,
    User(user): User,
    ValidatedJson(new_resource): ValidatedJson<ResourceRequest>,
) -> Result<(StatusCode, Json<Resource>), AppError> {
    // FIXME: how to restrict client logics (aka VENs) from creating resources for other vens?
    //  See also https://github.com/oadr3-org/specification/discussions/371
    let new_resource = match new_resource {
        ResourceRequest::BlResourceRequest(new_resource) => new_resource,
        ResourceRequest::VenResourceRequest(new_resource) => BlResourceRequest {
            client_id: user.client_id()?,
            // FIXME see https://github.com/oadr3-org/specification/discussions/372
            targets: object_privacy
                .targets_by_client_id(&user.client_id()?)
                .await?,
            resource_name: new_resource.resource_name,
            ven_id: new_resource.ven_id,
            attributes: new_resource.attributes,
        },
    };

    let resource = if user.scope.contains(Scope::WriteVens) {
        resource_source.create(new_resource, &None).await?
    } else {
        return Err(AppError::Forbidden("Missing 'write_vens' scope"));
    };

    info!(
        %resource.id,
        resource.resource_name=resource.content.resource_name,
        client_id = user.sub,
        "resource added"
    );

    Ok((StatusCode::CREATED, Json(resource)))
}

pub async fn edit(
    State(resource_source): State<Arc<dyn ResourceCrud>>,
    State(object_privacy): State<Arc<dyn VenObjectPrivacy>>,
    Path(id): Path<ResourceId>,
    User(user): User,
    ValidatedJson(update): ValidatedJson<ResourceRequest>,
) -> AppResponse<Resource> {
    // FIXME: how to restrict client logics (aka VENs) from creating resources for other vens?
    //  See also https://github.com/oadr3-org/specification/discussions/371
    let update = match update {
        ResourceRequest::BlResourceRequest(update) => update,
        ResourceRequest::VenResourceRequest(update) => BlResourceRequest {
            client_id: user.client_id()?,
            // FIXME see https://github.com/oadr3-org/specification/discussions/372
            targets: object_privacy
                .targets_by_client_id(&user.client_id()?)
                .await?,
            resource_name: update.resource_name,
            ven_id: update.ven_id,
            attributes: update.attributes,
        },
    };

    let resource = if user.scope.contains(Scope::WriteVens) {
        resource_source.update(&id, update, &None).await?
    } else {
        return Err(AppError::Forbidden("Missing 'write_vens' scope"));
    };

    info!(
        %resource.id,
        resource.resource_name=resource.content.resource_name,
        client_id = user.sub,
        "resource updated"
    );

    Ok(Json(resource))
}

pub async fn delete(
    State(resource_source): State<Arc<dyn ResourceCrud>>,
    Path(id): Path<ResourceId>,
    User(user): User,
) -> AppResponse<Resource> {
    let resource = if user.scope.contains(Scope::WriteVens) {
        // FIXME how to prevent VEN clients to delete other clients' resources?
        //  See also https://github.com/oadr3-org/specification/discussions/371
        resource_source.delete(&id, &None).await?
    } else {
        return Err(AppError::Forbidden("Missing 'write_vens' scope"));
    };

    info!(%id, client_id = user.sub, "deleted resource");
    Ok(Json(resource))
}

#[derive(Deserialize, Validate, Debug)]
#[serde(rename_all = "camelCase")]
pub struct QueryParams {
    #[validate(length(min = 1, max = 128))]
    pub(crate) resource_name: Option<String>,
    #[serde(rename = "venID")]
    pub(crate) ven_id: Option<VenId>,
    #[serde(flatten)]
    pub(crate) targets: TargetQueryParams,
    #[serde(default)]
    #[validate(range(min = 0))]
    pub(crate) skip: i64,
    #[validate(range(min = 1, max = 50))]
    #[serde(default = "get_50")]
    pub(crate) limit: i64,
}

fn get_50() -> i64 {
    50
}

#[cfg(test)]
#[cfg(feature = "live-db-test")]
mod test {
    use crate::{api::test::ApiTest};
    use axum::body::Body;
    use openleadr_wire::{
        problem::Problem,
        resource::{Resource, ResourceRequest, VenResourceRequest},
    };
    use reqwest::{Method, StatusCode};
    use sqlx::PgPool;
    use crate::jwt::Scope;

    #[sqlx::test(fixtures("users", "vens", "resources"))]
    async fn test_get_all(db: PgPool) {
        let test = ApiTest::new(db.clone(), "test-client",vec![Scope::WriteVens]).await;

        let (status, resources) = test
            .request::<Vec<Resource>>(Method::GET, "/resources?venID=ven-1", Body::empty())
            .await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(resources.len(), 2);

        let (status, resources) = test
            .request::<Vec<Resource>>(Method::GET, "/resources?venID=ven-2", Body::empty())
            .await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(resources.len(), 3);

        // test with ven user
        let test = ApiTest::new(db, "ven-1-client-id", vec![Scope::WriteVens, Scope::ReadTargets]).await;

        let (status, resources) = test
            .request::<Vec<Resource>>(Method::GET, "/resources?venID=ven-1", Body::empty())
            .await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(resources.len(), 2);

        let (status, _) = test
            .request::<serde_json::Value>(Method::GET, "/resources?venID=ven-2", Body::empty())
            .await;
        assert_eq!(status, StatusCode::FORBIDDEN);
    }

    #[sqlx::test(fixtures("users", "vens", "resources"))]
    async fn get_all_filtered(db: PgPool) {
        let test = ApiTest::new(db.clone(), "test-client",vec![Scope::WriteVens]).await;

        let (status, resources) = test
            .request::<Vec<Resource>>(Method::GET, "/resources?skip=1", Body::empty())
            .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(resources.len(), 1);

        let (status, resources) = test
            .request::<Vec<Resource>>(Method::GET, "/resources?limit=1", Body::empty())
            .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(resources.len(), 1);

        let (status, resources) = test
            .request::<Vec<Resource>>(Method::GET, "/resources?targets=group-1", Body::empty())
            .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(resources.len(), 1);

        let (status, resources) = test
            .request::<Vec<Resource>>(
                Method::GET,
                "/resources?targets=group-1&targets=group-2",
                Body::empty(),
            )
            .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(resources.len(), 2);
    }

    #[sqlx::test(fixtures("users", "vens", "resources"))]
    async fn get_single_resource(db: PgPool) {
        let test = ApiTest::new(db.clone(), "test-client",vec![Scope::WriteVens]).await;

        let (status, resource) = test
            .request::<Resource>(
                Method::GET,
                "/resources/resource-1?venID=ven-1",
                Body::empty(),
            )
            .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(resource.id.as_str(), "resource-1");

        // test with ven user
        let test = ApiTest::new(db, "ven-1-client-id", vec![Scope::ReadTargets, Scope::WriteVens]).await;

        let (status, resource) = test
            .request::<Resource>(
                Method::GET,
                "/resources/resource-1?venID=ven-1",
                Body::empty(),
            )
            .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(resource.id.as_str(), "resource-1");

        let (status, _) = test
            .request::<Problem>(
                Method::GET,
                "/resources/resource-2?venID=ven-1",
                Body::empty(),
            )
            .await;
        assert_eq!(status, StatusCode::NOT_FOUND);

        let (status, _) = test
            .request::<Problem>(
                Method::GET,
                "/resources/resource-2?venID=ven-2",
                Body::empty(),
            )
            .await;
        assert_eq!(status, StatusCode::FORBIDDEN);
    }

    #[sqlx::test(fixtures("users", "vens", "resources"))]
    async fn add_edit_delete(db: PgPool) {
        let test = ApiTest::new(db.clone(), "test-client", vec![Scope::WriteVens]).await;

        let (status, resource) = test
            .request::<Resource>(
                Method::POST,
                "/resources",
                Body::from(r#"{"resourceName":"new-resource", "venID": "ven-1", "objectType": "VEN_RESOURCE_REQUEST"}"#),
            )
            .await;
        assert_eq!(status, StatusCode::CREATED);
        assert_eq!(resource.content.resource_name, "new-resource");

        let resource_id = resource.id.as_str();

        let (status, resource) = test
            .request::<Resource>(
                Method::PUT,
                &format!("/resources/{resource_id}"),
                Body::from(r#"{"resourceName":"updated-resource", "venID": "ven-1", "objectType": "VEN_RESOURCE_REQUEST"}"#),
            )
            .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(resource.content.resource_name, "updated-resource");

        let (status, resource) = test
            .request::<Resource>(
                Method::GET,
                &format!("/resources/{resource_id}"),
                Body::empty(),
            )
            .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(resource.content.resource_name, "updated-resource");

        let (status, _) = test
            .request::<Resource>(
                Method::DELETE,
                &format!("/resources/{resource_id}"),
                Body::empty(),
            )
            .await;
        assert_eq!(status, StatusCode::OK);

        let (status, _) = test
            .request::<Problem>(
                Method::GET,
                &format!("/resources/{resource_id}"),
                Body::empty(),
            )
            .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    #[sqlx::test(fixtures("users", "vens"))]
    async fn name_constraint_validation(db: PgPool) {
        let test = ApiTest::new(db, "test-client", vec![Scope::ReadAll, Scope::WriteVens]).await;

        let resources = [
            ResourceRequest::VenResourceRequest(VenResourceRequest {
                resource_name: "".to_string(),
                ven_id: "ven-1".parse().unwrap(),
                attributes: None,
            }),
            ResourceRequest::VenResourceRequest(VenResourceRequest {
                resource_name: "This is more than 128 characters long and should be \
                                rejected This is more than \
                                128 characters long and should be rejected asdfasd"
                    .to_string(),
                ven_id: "ven-1".parse().unwrap(),
                attributes: None,
            }),
        ];

        for resource in &resources {
            let (status, error) = test
                .request::<Problem>(
                    Method::POST,
                    "/resources",
                    Body::from(serde_json::to_vec(&resource).unwrap()),
                )
                .await;

            assert_eq!(status, StatusCode::BAD_REQUEST);
            assert!(error
                .detail
                .unwrap()
                .contains("outside of allowed range 1..=128"))
        }
    }
}
