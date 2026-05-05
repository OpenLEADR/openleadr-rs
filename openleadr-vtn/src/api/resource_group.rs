use std::sync::Arc;

use axum::{
    extract::{Path, State},
    Json,
};
use reqwest::StatusCode;
use serde::Deserialize;
use tracing::{info, trace};
use validator::Validate;

use openleadr_wire::{
    resource_group::{BlResourceGroupRequest, ResourceGroup, ResourceGroupId},
    subscription::{AnyObject, Operation},
};

use crate::{
    api::{
        subscription::{self, NotifierState},
        AppResponse, TargetQueryParams, ValidatedJson, ValidatedQuery,
    },
    data_source::{EventCrud, ResourceGroupCrud},
    error::AppError,
    jwt::{Scope, User},
};

pub async fn get_all(
    State(resource_group_source): State<Arc<dyn ResourceGroupCrud>>,
    ValidatedQuery(query_params): ValidatedQuery<QueryParams>,
    User(user): User,
) -> AppResponse<Vec<ResourceGroup>> {
    trace!(?query_params);

    let resource_groups = if user.scope.contains(Scope::ReadAll) {
        resource_group_source
            .retrieve_all(&query_params, &None)
            .await?
    } else if user.scope.contains(Scope::ReadVenObjects) {
        resource_group_source
            .retrieve_all(&query_params, &Some(user.client_id()?))
            .await?
    } else {
        return Err(AppError::Forbidden(
            "Missing 'read_all' or 'read_ven_objects' scope",
        ));
    };

    trace!(
        client_id = user.sub,
        "retrieved {} resource groups",
        resource_groups.len()
    );

    Ok(Json(resource_groups))
}

pub async fn get(
    State(resource_group_source): State<Arc<dyn ResourceGroupCrud>>,
    Path(id): Path<ResourceGroupId>,
    User(user): User,
) -> AppResponse<ResourceGroup> {
    let resource_group = if user.scope.contains(Scope::ReadAll) {
        resource_group_source.retrieve(&id, &None).await?
    } else if user.scope.contains(Scope::ReadVenObjects) {
        resource_group_source
            .retrieve(&id, &Some(user.client_id()?))
            .await?
    } else {
        return Err(AppError::Forbidden(
            "Missing 'read_all' or 'read_ven_objects' scope",
        ));
    };

    trace!(
        %resource_group.id,
        resource.resource_group_name=resource_group.content.resource_group_name,
        client_id = user.sub,
        "resource group retrieved"
    );

    Ok(Json(resource_group))
}

pub async fn add(
    State(event_source): State<Arc<dyn EventCrud>>,
    State(resource_group_source): State<Arc<dyn ResourceGroupCrud>>,
    State(notifier_state): State<Arc<NotifierState>>,
    User(user): User,
    ValidatedJson(new_resource_group): ValidatedJson<BlResourceGroupRequest>,
) -> Result<(StatusCode, Json<ResourceGroup>), AppError> {
    let resource_group = if user.scope.contains(Scope::WriteVensBl) {
        resource_group_source
            .create(new_resource_group, &None)
            .await?
    } else {
        return Err(AppError::Forbidden("Missing 'write_vens_bl' scope"));
    };

    info!(
        %resource_group.id,
        resource_group.resource_group_name=resource_group.content.resource_group_name,
        "resource added"
    );

    subscription::notify(
        &*event_source,
        &notifier_state,
        Operation::Create,
        AnyObject::ResourceGroup(resource_group.clone()),
    )
    .await;

    Ok((StatusCode::CREATED, Json(resource_group)))
}

pub async fn edit(
    State(event_source): State<Arc<dyn EventCrud>>,
    State(resource_group_source): State<Arc<dyn ResourceGroupCrud>>,
    State(notifier_state): State<Arc<NotifierState>>,
    Path(id): Path<ResourceGroupId>,
    User(user): User,
    ValidatedJson(update): ValidatedJson<BlResourceGroupRequest>,
) -> AppResponse<ResourceGroup> {
    let new_resource_group = BlResourceGroupRequest {
        resource_group_name: update.resource_group_name,
        // TODO: Check if this is still correct
        // VEN clients are not allowed to specify the targets of their resources
        targets: update.targets,
        attributes: update.attributes,
        children: update.children,
    };

    let resource_group = if user.scope.contains(Scope::WriteVensBl) {
        resource_group_source
            .update(&id, new_resource_group, &None)
            .await?
    } else {
        return Err(AppError::Forbidden("Missing 'write_vens_bl' scope"));
    };

    info!(
        %resource_group.id,
        resource.resource_group_name=resource_group.content.resource_group_name,
        "resource group updated"
    );

    subscription::notify(
        &*event_source,
        &notifier_state,
        Operation::Update,
        AnyObject::ResourceGroup(resource_group.clone()),
    )
    .await;

    Ok(Json(resource_group))
}

pub async fn delete(
    State(event_source): State<Arc<dyn EventCrud>>,
    State(resource_group_source): State<Arc<dyn ResourceGroupCrud>>,
    State(notifier_state): State<Arc<NotifierState>>,
    User(user): User,
    Path(id): Path<ResourceGroupId>,
) -> AppResponse<ResourceGroup> {
    let resource_group = if user.scope.contains(Scope::WriteVensBl) {
        resource_group_source.delete(&id, &None).await?
    } else {
        return Err(AppError::Forbidden("Missing 'write_vens_bl' scope"));
    };

    info!(%id, "deleted resource group");

    dbg!(&resource_group);

    subscription::notify(
        &*event_source,
        &notifier_state,
        Operation::Delete,
        AnyObject::ResourceGroup(resource_group.clone()),
    )
    .await;

    Ok(Json(resource_group))
}

#[derive(Deserialize, Validate, Debug)]
#[serde(rename_all = "camelCase")]
pub struct QueryParams {
    #[validate(length(min = 1, max = 128))]
    pub(crate) resource_group_name: Option<String>,
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
    use crate::{api::test::ApiTest, jwt::Scope};
    use axum::body::Body;
    use openleadr_wire::{
        problem::Problem,
        resource_group::{BlResourceGroupRequest, ResourceGroup, ResourceGroupChild},
    };
    use reqwest::{Method, StatusCode};
    use sqlx::PgPool;

    #[sqlx::test(fixtures("users", "vens"))]
    async fn name_constraint_validation(db: PgPool) {
        let test = ApiTest::new(db, "test-client", vec![Scope::ReadAll, Scope::WriteVensVen]).await;

        let resources = [
            BlResourceGroupRequest {
                resource_group_name: "".to_string(),
                attributes: None,
                targets: vec![],
                children: vec![],
            },
            BlResourceGroupRequest {
                resource_group_name: "This is more than 128 characters long and should be \
                                rejected This is more than \
                                128 characters long and should be rejected asdfasd"
                    .to_string(),
                attributes: None,
                targets: vec![],
                children: vec![],
            },
        ];

        for resource in &resources {
            let (status, error) = test
                .request::<Problem>(
                    Method::POST,
                    "/resource_groups",
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

    #[sqlx::test(fixtures("users", "vens", "resources", "resource_groups"))]
    async fn test_get_all(db: PgPool) {
        let test = ApiTest::new(db.clone(), "test-client", vec![Scope::ReadAll]).await;

        let (status, resource_groups) = test
            .request::<Vec<ResourceGroup>>(Method::GET, "/resource_groups", Body::empty())
            .await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(resource_groups.len(), 5);
    }

    #[sqlx::test(fixtures("vens", "resources", "resource_groups"))]
    async fn filter_by_name(db: PgPool) {
        let test = ApiTest::new(db.clone(), "ven-1-client-id", vec![Scope::ReadVenObjects]).await;

        let (status, resource_group) = test
            .request::<Vec<ResourceGroup>>(
                Method::GET,
                "/resource_groups?resourceGroupName=resource-group-2-name",
                Body::empty(),
            )
            .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(resource_group[0].id.as_str(), "resource-group-2");
        assert_eq!(resource_group.len(), 1);
    }

    #[sqlx::test(fixtures("vens", "resources", "resource_groups"))]
    async fn bl_get_single_resource_group(db: PgPool) {
        let test = ApiTest::new(db.clone(), "ven-1-client-id", vec![Scope::ReadAll]).await;

        let (status, resource_group) = test
            .request::<ResourceGroup>(
                Method::GET,
                "/resource_groups/resource-group-1",
                Body::empty(),
            )
            .await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(resource_group.id.as_str(), "resource-group-1");
    }

    #[sqlx::test(fixtures("vens", "resources", "resource_groups"))]
    async fn ven_get_single_resource_group(db: PgPool) {
        let test = ApiTest::new(db, "ven-1-client-id", vec![Scope::ReadVenObjects]).await;

        let (status, resource_group) = test
            .request::<ResourceGroup>(
                Method::GET,
                "/resource_groups/resource-group-1",
                Body::empty(),
            )
            .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(resource_group.id.as_str(), "resource-group-1");

        assert_eq!(resource_group.content.children.len(), 2);
        for child in [
            ResourceGroupChild::ResourceGroup("resource-group-3".parse().unwrap()),
            ResourceGroupChild::ResourceGroup("resource-group-2".parse().unwrap()),
        ] {
            assert!(resource_group.content.children.contains(&child));
        }
    }

    #[sqlx::test(fixtures("vens", "resources", "resource_groups"))]
    async fn resource_group_can_be_its_own_child(db: PgPool) {
        let test = ApiTest::new(db, "ven-1-client-id", vec![Scope::ReadAll]).await;

        let (status, resource_group) = test
            .request::<ResourceGroup>(Method::GET, "/resource_groups/ouroboros", Body::empty())
            .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(resource_group.id.as_str(), "ouroboros");

        // This resource group _should_ be its own child
        assert!(resource_group
            .content
            .children
            .contains(&ResourceGroupChild::ResourceGroup(
                "ouroboros".parse().unwrap()
            )));
        assert_eq!(resource_group.content.children.len(), 1)
    }

    #[sqlx::test(fixtures("vens", "resources", "resource_groups"))]
    async fn add_resource_group(db: PgPool) {
        let test = ApiTest::new(db.clone(), "test-client", vec![Scope::WriteVensBl]).await;

        let (status, resource_group) = test
            .request::<ResourceGroup>(
                Method::POST,
                "/resource_groups",
                Body::from(
                    r#"
                  {
                    "resourceGroupName":"new-resource-group",
                    "children":[
                        {
                            "type":"resource_group",
                            "id":"resource-group-1"
                        },
                        {
                            "type":"ven_resource",
                            "id":"resource-1"
                        }
                    ]
                  }"#,
                ),
            )
            .await;
        assert_eq!(status, StatusCode::CREATED);
        assert_eq!(
            resource_group.content.resource_group_name,
            "new-resource-group"
        );
        assert_eq!(resource_group.content.children.len(), 2);
        for child in [
            ResourceGroupChild::ResourceGroup("resource-group-1".parse().unwrap()),
            ResourceGroupChild::VenResource("resource-1".parse().unwrap()),
        ] {
            assert!(resource_group.content.children.contains(&child));
        }
    }

    #[sqlx::test(fixtures("vens", "resources", "resource_groups"))]
    async fn bl_update_resource_group(db: PgPool) {
        let test = ApiTest::new(
            db.clone(),
            "test-client",
            vec![Scope::WriteVensBl, Scope::ReadAll],
        )
        .await;

        let (status, resource_group) = test
            .request::<ResourceGroup>(
                Method::PUT,
                "/resource_groups/resource-group-1",
                Body::from(
                    r#"
                  {
                    "resourceGroupName":"updated-resource-group",
                    "targets": ["group-3"],
                    "objectType": "RESOURCE_GROUP_REQUEST",
                    "children": [
                        {
                            "type":"resource_group",
                            "id":"resource-group-2"
                        },
                        {
                            "type":"ven_resource",
                            "id":"resource-1"
                        }
                    ]
                  }"#,
                ),
            )
            .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            resource_group.content.resource_group_name,
            "updated-resource-group"
        );
        assert_eq!(
            resource_group.content.targets,
            vec!["group-3".parse().unwrap()]
        );

        assert_eq!(resource_group.content.children.len(), 2);
        for child in [
            ResourceGroupChild::ResourceGroup("resource-group-2".parse().unwrap()),
            ResourceGroupChild::VenResource("resource-1".parse().unwrap()),
        ] {
            assert!(resource_group.content.children.contains(&child));
        }

        let (status, resource_group) = test
            .request::<ResourceGroup>(
                Method::GET,
                "/resource_groups/resource-group-1",
                Body::empty(),
            )
            .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            resource_group.content.resource_group_name,
            "updated-resource-group"
        );
        assert_eq!(
            resource_group.content.targets,
            vec!["group-3".parse().unwrap()]
        );

        assert_eq!(resource_group.content.children.len(), 2);
        for child in [
            ResourceGroupChild::ResourceGroup("resource-group-2".parse().unwrap()),
            ResourceGroupChild::VenResource("resource-1".parse().unwrap()),
        ] {
            assert!(resource_group.content.children.contains(&child));
        }
    }

    #[sqlx::test(fixtures("vens", "resources", "resource_groups"))]
    async fn bl_delete_resource_group(db: PgPool) {
        let test = ApiTest::new(
            db.clone(),
            "test-client",
            vec![Scope::WriteVensBl, Scope::ReadAll],
        )
        .await;
        let (status, rg) = test
            .request::<ResourceGroup>(
                Method::DELETE,
                "/resource_groups/resource-group-2",
                Body::empty(),
            )
            .await;
        assert_eq!(status, StatusCode::OK);

        assert_eq!(rg.content.children.len(), 2);
        for child in [
            ResourceGroupChild::ResourceGroup("resource-group-4".parse().unwrap()),
            ResourceGroupChild::VenResource("resource-1".parse().unwrap()),
        ] {
            assert!(rg.content.children.contains(&child));
        }

        let (status, _) = test
            .request::<Problem>(
                Method::GET,
                "/resource_groups/resource-group-2",
                Body::empty(),
            )
            .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }
}
