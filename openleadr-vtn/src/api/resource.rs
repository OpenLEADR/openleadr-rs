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

use openleadr_wire::resource::{Resource, ResourceId, ResourceRequest};

use crate::{
    api::{AppResponse, TargetQueryParams, ValidatedJson, ValidatedQuery},
    data_source::ResourceCrud,
    error::AppError,
    jwt::User,
};

fn has_write_permission(User(claims): &User, ven_id: &VenId) -> Result<(), AppError> {
    if claims.is_ven_manager() {
        return Ok(());
    }

    if claims.is_ven() && claims.ven_ids().contains(ven_id) {
        return Ok(());
    }

    Err(AppError::Forbidden(
        "User not authorized to access this resource",
    ))
}

pub async fn get_all(
    State(resource_source): State<Arc<dyn ResourceCrud>>,
    ValidatedQuery(query_params): ValidatedQuery<QueryParams>,
    user: User,
) -> AppResponse<Vec<Resource>> {
    // FIXME object privacy
    // has_write_permission(&user, &ven_id)?;
    trace!(?query_params);

    let resources = resource_source.retrieve_all(&query_params, &user).await?;

    Ok(Json(resources))
}

pub async fn get(
    State(resource_source): State<Arc<dyn ResourceCrud>>,
    Path(id): Path<ResourceId>,
    user: User,
) -> AppResponse<Resource> {
    // FIXME object privacy
    // has_write_permission(&user, &ven_id)?;
    let ven = resource_source.retrieve(&id, &user).await?;

    Ok(Json(ven))
}

pub async fn add(
    State(resource_source): State<Arc<dyn ResourceCrud>>,
    user: User,
    ValidatedJson(new_resource): ValidatedJson<ResourceRequest>,
) -> Result<(StatusCode, Json<Resource>), AppError> {
    // FIXME object privacy
    // has_write_permission(&user, &ven_id)?;
    let ven = resource_source.create(new_resource, &user).await?;

    Ok((StatusCode::CREATED, Json(ven)))
}

pub async fn edit(
    State(resource_source): State<Arc<dyn ResourceCrud>>,
    Path(id): Path<ResourceId>,
    user: User,
    ValidatedJson(content): ValidatedJson<ResourceRequest>,
) -> AppResponse<Resource> {
    // FIXME object privacy
    // has_write_permission(&user, &ven_id)?;
    let resource = resource_source.update(&id, content, &user).await?;

    info!(%resource.id, resource.resource_name=resource.content.resource_name, "resource updated");

    Ok(Json(resource))
}

pub async fn delete(
    State(resource_source): State<Arc<dyn ResourceCrud>>,
    Path(id): Path<ResourceId>,
    user: User,
) -> AppResponse<Resource> {
    // FIXME object privacy
    // has_write_permission(&user, &ven_id)?;
    let resource = resource_source.delete(&id, &user).await?;
    info!(%id, "deleted resource");
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
    #[validate(nested)]
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
mod test {
    use crate::{api::test::ApiTest, jwt::AuthRole};
    use axum::body::Body;
    use openleadr_wire::{
        problem::Problem,
        resource::{Resource, ResourceRequest, VenResourceRequest},
    };
    use reqwest::{Method, StatusCode};
    use sqlx::PgPool;

    #[sqlx::test(fixtures("users", "vens", "resources"))]
    async fn test_get_all(db: PgPool) {
        let test = ApiTest::new(db.clone(), vec![AuthRole::VenManager]).await;

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
        let test = ApiTest::new(db, vec![AuthRole::VEN("ven-1".parse().unwrap())]).await;

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
        let test = ApiTest::new(db.clone(), vec![AuthRole::VenManager]).await;

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
        let test = ApiTest::new(db.clone(), vec![AuthRole::VenManager]).await;

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
        let test = ApiTest::new(db, vec![AuthRole::VEN("ven-1".parse().unwrap())]).await;

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
        let test = ApiTest::new(db.clone(), vec![AuthRole::VenManager]).await;

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
        let test = ApiTest::new(db, vec![AuthRole::AnyBusiness]).await;

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
