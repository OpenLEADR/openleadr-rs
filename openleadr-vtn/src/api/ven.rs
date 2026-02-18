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
    subscription::{AnyObject, Operation},
    ven::{BlVenRequest, Ven, VenId, VenRequest},
};

use crate::{
    api::{
        subscription, subscription::NotifierState, AppResponse, TargetQueryParams, ValidatedJson,
        ValidatedQuery,
    },
    data_source::VenCrud,
    error::AppError,
    jwt::{Scope, User},
};

pub async fn get_all(
    State(ven_source): State<Arc<dyn VenCrud>>,
    ValidatedQuery(query_params): ValidatedQuery<QueryParams>,
    User(user): User,
) -> AppResponse<Vec<Ven>> {
    trace!(?query_params);

    let vens = if user.scope.contains(Scope::ReadAll) {
        ven_source.retrieve_all(&query_params, &None).await?
    } else if user.scope.contains(Scope::ReadVenObjects) {
        ven_source
            .retrieve_all(&query_params, &Some(user.client_id()?))
            .await?
    } else {
        return Err(AppError::Forbidden(
            "Missing 'read_all' or 'read_ven_objects' scope",
        ));
    };

    trace!(client_id = user.sub, "retrieved {} VENs", vens.len());

    Ok(Json(vens))
}

pub async fn get(
    State(ven_source): State<Arc<dyn VenCrud>>,
    Path(id): Path<VenId>,
    User(user): User,
) -> AppResponse<Ven> {
    let ven = if user.scope.contains(Scope::ReadAll) {
        ven_source.retrieve(&id, &None).await?
    } else if user.scope.contains(Scope::ReadVenObjects) {
        ven_source.retrieve(&id, &Some(user.client_id()?)).await?
    } else {
        return Err(AppError::Forbidden(
            "Missing 'read_all' or 'read_ven_objects' scope",
        ));
    };

    trace!(%ven.id, ven.ven_name=ven.content.ven_name, client_id = user.sub, "VEN retrieved");

    Ok(Json(ven))
}

pub async fn add(
    State(ven_source): State<Arc<dyn VenCrud>>,
    State(notifier_state): State<Arc<NotifierState>>,
    User(user): User,
    ValidatedJson(new_ven): ValidatedJson<VenRequest>,
) -> Result<(StatusCode, Json<Ven>), AppError> {
    // FIXME: how to restrict client logics (aka VENs) from creating VENs for other clients?
    //  See also https://github.com/oadr3-org/specification/discussions/371
    let new_ven = match new_ven {
        VenRequest::BlVenRequest(new_ven) => new_ven,
        VenRequest::VenVenRequest(new_ven) => BlVenRequest {
            client_id: user.client_id()?,
            // There cannot be another VEN object with the user.client_id() yet and a VEN isn't
            // allowed to assign its own targets. Therefore, we have to initialize the targets with an empty
            // vector.
            targets: vec![],
            ven_name: new_ven.ven_name,
            attributes: new_ven.attributes,
        },
    };

    let ven = if user.scope.contains(Scope::WriteVens) {
        ven_source.create(new_ven, &None).await?
    } else {
        return Err(AppError::Forbidden("Missing 'write_vens' scope"));
    };

    info!(%ven.id, ven.ven_name=ven.content.ven_name, client_id = user.sub, "VEN added");

    subscription::notify(
        &notifier_state,
        Operation::Create,
        AnyObject::Ven(ven.clone()),
    );

    Ok((StatusCode::CREATED, Json(ven)))
}

pub async fn edit(
    State(ven_source): State<Arc<dyn VenCrud>>,
    State(notifier_state): State<Arc<NotifierState>>,
    Path(id): Path<VenId>,
    User(user): User,
    ValidatedJson(update): ValidatedJson<VenRequest>,
) -> AppResponse<Ven> {
    // FIXME: how to restrict client logics (aka VENs) from creating VENs for other clients?
    //  See also https://github.com/oadr3-org/specification/discussions/371
    let update = match update {
        VenRequest::BlVenRequest(new_ven) => new_ven,
        VenRequest::VenVenRequest(new_ven) => {
            let org_ven = ven_source.retrieve(&id, &Some(user.client_id()?)).await?;

            BlVenRequest {
                client_id: org_ven.content.client_id,
                // VEN clients are not allowed to change their targets
                targets: org_ven.content.targets,
                ven_name: new_ven.ven_name,
                attributes: new_ven.attributes,
            }
        }
    };

    let ven = if user.scope.contains(Scope::WriteVens) {
        ven_source.update(&id, update, &None).await?
    } else {
        return Err(AppError::Forbidden("Missing 'write_vens' scope"));
    };

    info!(%ven.id, ven.ven_name=ven.content.ven_name, client_id = user.sub, "VEN updated");

    subscription::notify(
        &notifier_state,
        Operation::Update,
        AnyObject::Ven(ven.clone()),
    );

    Ok(Json(ven))
}

pub async fn delete(
    State(ven_source): State<Arc<dyn VenCrud>>,
    State(notifier_state): State<Arc<NotifierState>>,
    Path(id): Path<VenId>,
    User(user): User,
) -> AppResponse<Ven> {
    let ven = if user.scope.contains(Scope::WriteVens) {
        // FIXME how to prevent VEN clients to delete other clients' VENs?
        //  See also https://github.com/oadr3-org/specification/discussions/371
        ven_source.delete(&id, &None).await?
    } else {
        return Err(AppError::Forbidden("Missing 'write_vens' scope"));
    };

    info!(%ven.id, ven.ven_name=ven.content.ven_name, client_id = user.sub, "VEN deleted");

    subscription::notify(
        &notifier_state,
        Operation::Delete,
        AnyObject::Ven(ven.clone()),
    );

    Ok(Json(ven))
}

#[derive(Deserialize, Validate, Debug)]
#[serde(rename_all = "camelCase")]
pub struct QueryParams {
    #[validate(length(min = 1, max = 128))]
    pub(crate) ven_name: Option<String>,
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
mod tests {
    use crate::{api::test::ApiTest, jwt::Scope};
    use axum::{body::Body, http::StatusCode};
    use openleadr_wire::{
        problem::Problem,
        ven::{BlVenRequest, VenRequest, VenVenRequest},
        Ven,
    };
    use reqwest::Method;
    use sqlx::PgPool;

    mod permissions {
        use super::*;

        #[sqlx::test(fixtures("vens"))]
        async fn cannot_read_vens_without_correct_scope(db: PgPool) {
            let test = ApiTest::new(
                db.clone(),
                "test-client",
                Scope::all()
                    .into_iter()
                    .filter(|s| *s != Scope::ReadAll && *s != Scope::ReadVenObjects)
                    .collect(),
            )
            .await;

            let (status, _) = test
                .request::<Problem>(Method::GET, "/vens", Body::empty())
                .await;

            assert_eq!(status, StatusCode::FORBIDDEN);
        }

        #[sqlx::test(fixtures("vens"))]
        async fn cannot_read_ven_without_correct_scope(db: PgPool) {
            let test = ApiTest::new(
                db.clone(),
                "test-client",
                Scope::all()
                    .into_iter()
                    .filter(|s| *s != Scope::ReadAll && *s != Scope::ReadVenObjects)
                    .collect(),
            )
            .await;

            let (status, _) = test
                .request::<Problem>(Method::GET, "/vens/ven-1", Body::empty())
                .await;

            assert_eq!(status, StatusCode::FORBIDDEN);
        }

        #[sqlx::test(fixtures("vens"))]
        async fn cannot_add_ven_without_correct_scope(db: PgPool) {
            let test = ApiTest::new(
                db.clone(),
                "test-client",
                Scope::all()
                    .into_iter()
                    .filter(|s| *s != Scope::WriteVens)
                    .collect(),
            )
            .await;

            let (status, _) = test
                .request::<Problem>(
                    Method::POST,
                    "/vens",
                    Body::from(
                        r#"
                        {
                          "objectType": "BL_VEN_REQUEST",
                          "clientID": "new-ven-client-id",
                          "venName": "new-ven-name"
                        }"#,
                    ),
                )
                .await;

            assert_eq!(status, StatusCode::FORBIDDEN);
        }

        #[sqlx::test(fixtures("vens"))]
        async fn cannot_edit_vev_without_correct_scope(db: PgPool) {
            let test = ApiTest::new(
                db.clone(),
                "ven-1-client-id",
                Scope::all()
                    .into_iter()
                    .filter(|s| *s != Scope::WriteVens)
                    .collect(),
            )
            .await;

            let (status, _) = test
                .request::<Problem>(
                    Method::PUT,
                    "/vens/ven-1",
                    Body::from(
                        r#"
                        {
                          "objectType": "BL_VEN_REQUEST",
                          "clientID": "ven-1-client-id",
                          "venName": "ven-1-name"
                        }"#,
                    ),
                )
                .await;

            assert_eq!(status, StatusCode::FORBIDDEN);
        }

        #[sqlx::test(fixtures("vens"))]
        async fn cannot_delete_resource_without_correct_scope(db: PgPool) {
            let test = ApiTest::new(
                db.clone(),
                "test-client",
                Scope::all()
                    .into_iter()
                    .filter(|s| *s != Scope::WriteVens)
                    .collect(),
            )
            .await;

            let (status, _) = test
                .request::<Problem>(Method::DELETE, "/vens/ven-1", Body::empty())
                .await;

            assert_eq!(status, StatusCode::FORBIDDEN);
        }
    }

    #[sqlx::test(fixtures("users", "vens"))]
    async fn get_all_unfiltered(db: PgPool) {
        let test = ApiTest::new(db, "test-client", vec![Scope::ReadAll]).await;

        let (status, mut vens) = test
            .request::<Vec<Ven>>(Method::GET, "/vens", Body::empty())
            .await;
        assert_eq!(status, StatusCode::OK);

        assert_eq!(vens.len(), 5);
        vens.sort_by(|a, b| a.id.as_str().cmp(b.id.as_str()));
        assert_eq!(vens[0].id.as_str(), "ven-1");
        assert_eq!(vens[1].id.as_str(), "ven-2");
    }

    #[sqlx::test(fixtures("users", "vens"))]
    async fn get_all_filtered(db: PgPool) {
        let test = ApiTest::new(db.clone(), "test-client", vec![Scope::ReadAll]).await;

        let (status, vens) = test
            .request::<Vec<Ven>>(Method::GET, "/vens?skip=1", Body::empty())
            .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(vens.len(), 4);

        let (status, vens) = test
            .request::<Vec<Ven>>(Method::GET, "/vens?limit=1", Body::empty())
            .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(vens.len(), 1);

        let (status, vens) = test
            .request::<Vec<Ven>>(Method::GET, "/vens?targets=group-1", Body::empty())
            .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(vens.len(), 3);
        assert_eq!(vens[0].id.as_str(), "ven-1");
        assert_eq!(vens[1].id.as_str(), "ven-3");
        assert_eq!(vens[2].id.as_str(), "ven-4");

        let (status, vens) = test
            .request::<Vec<Ven>>(
                Method::GET,
                "/vens?targets=group-1&targets=group-2",
                Body::empty(),
            )
            .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(vens.len(), 1);
        assert_eq!(vens[0].id.as_str(), "ven-4");
    }

    #[sqlx::test(fixtures("users", "vens"))]
    async fn get_all_ven_user(db: PgPool) {
        let test = ApiTest::new(db, "ven-1-client-id", vec![Scope::ReadVenObjects]).await;

        let (status, vens) = test
            .request::<Vec<Ven>>(Method::GET, "/vens", Body::empty())
            .await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(vens.len(), 1);
        assert_eq!(vens[0].id.as_str(), "ven-1");

        let (status, vens) = test
            .request::<Vec<Ven>>(
                Method::GET,
                "/vens?targets=group-1&targets=private-value",
                Body::empty(),
            )
            .await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(vens.len(), 1);
        assert_eq!(vens[0].id.as_str(), "ven-1");

        let (status, vens) = test
            .request::<Vec<Ven>>(Method::GET, "/vens?targets=group-2", Body::empty())
            .await;

        assert_eq!(status, StatusCode::OK);
        assert!(vens.is_empty());
    }

    #[sqlx::test(fixtures("users", "vens"))]
    async fn get_single_bl_client(db: PgPool) {
        let test = ApiTest::new(db, "test-client", vec![Scope::ReadAll]).await;

        let (status, ven) = test
            .request::<Ven>(Method::GET, "/vens/ven-1", Body::empty())
            .await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(ven.id.as_str(), "ven-1");
    }

    #[sqlx::test(fixtures("users", "vens"))]
    async fn get_single_ven_client(db: PgPool) {
        let test = ApiTest::new(db, "ven-1-client-id", vec![Scope::ReadVenObjects]).await;

        let (status, ven) = test
            .request::<Ven>(Method::GET, "/vens/ven-1", Body::empty())
            .await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(ven.id.as_str(), "ven-1");

        let (status, problem) = test
            .request::<Problem>(Method::GET, "/vens/ven-2", Body::empty())
            .await;

        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(problem.status, StatusCode::NOT_FOUND);
    }

    #[sqlx::test(fixtures("users", "vens"))]
    async fn add_edit_delete_ven(db: PgPool) {
        // TODO the scope write_vens is not separating between VEN and BL clients
        //  See also https://github.com/oadr3-org/specification/discussions/371
        //  Adopt the test as soon as this discussion is settled
        let test = ApiTest::new(
            db,
            "test-client",
            vec![Scope::WriteVens, Scope::ReadVenObjects],
        )
        .await;

        let new_ven = r#"{"venName":"new-ven", "objectType": "VEN_VEN_REQUEST"}"#;
        let (status, ven) = test
            .request::<Ven>(Method::POST, "/vens", Body::from(new_ven))
            .await;

        assert_eq!(status, StatusCode::CREATED);
        assert_eq!(ven.content.ven_name, "new-ven");

        let ven_id = ven.id.as_str();

        let (status, ven) = test
            .request::<Ven>(Method::GET, &format!("/vens/{ven_id}"), Body::empty())
            .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(ven.id.as_str(), ven_id);

        let new_ven = r#"{"venName":"new-ven-2", "objectType": "VEN_VEN_REQUEST"}"#;
        let (status, ven) = test
            .request::<Ven>(Method::PUT, &format!("/vens/{ven_id}"), Body::from(new_ven))
            .await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(ven.content.ven_name, "new-ven-2");

        let (status, ven) = test
            .request::<Ven>(Method::GET, &format!("/vens/{ven_id}"), Body::empty())
            .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(ven.content.ven_name, "new-ven-2");
        assert_eq!(ven.id.as_str(), ven_id);

        let (status, ven) = test
            .request::<Ven>(Method::DELETE, &format!("/vens/{ven_id}"), Body::empty())
            .await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(ven.id.as_str(), ven_id);

        let (status, _) = test
            .request::<Problem>(Method::GET, &format!("/vens/{ven_id}"), Body::empty())
            .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    #[sqlx::test(fixtures("users", "vens"))]
    async fn cannot_create_ven_with_conflicting_client_id(db: PgPool) {
        // This VEN should not be able to create a new VEN object, as there already exists a VEN object with the same client_id
        // See also https://github.com/oadr3-org/specification/issues/374
        let test = ApiTest::new(db, "ven-1-client-id", vec![Scope::WriteVens]).await;

        let new_ven = r#"{"venName":"new-ven", "objectType": "VEN_VEN_REQUEST"}"#;
        let (status, problem) = test
            .request::<Problem>(Method::POST, "/vens", Body::from(new_ven))
            .await;

        assert_eq!(status, StatusCode::CONFLICT);
        assert_eq!(problem.status, StatusCode::CONFLICT);
    }

    #[sqlx::test(fixtures("users", "vens", "resources"))]
    async fn ven_cannot_edit_their_targets(db: PgPool) {
        // TODO the scope write_vens is not separating between VEN and BL clients
        //  See also https://github.com/oadr3-org/specification/discussions/371
        //  Adopt the test as soon as this discussion is settled. Especially test that a VEN cannot
        //  sneak in a "objectType": "BL_VEN_REQUEST"
        let test = ApiTest::new(db, "ven-1-client-id", vec![Scope::WriteVens]).await;

        let updated_ven = r#"
            {
              "venName":"updated-name",
              "objectType": "VEN_VEN_REQUEST",
               "targets": ["group-2"]
            }"#;
        let (status, ven) = test
            .request::<Ven>(Method::PUT, "/vens/ven-1", Body::from(updated_ven))
            .await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(ven.content.ven_name, "updated-name");
        assert_eq!(
            ven.content.targets,
            vec!["group-1".parse().unwrap(), "private-value".parse().unwrap()]
        );
    }

    #[sqlx::test(fixtures("users", "vens"))]
    async fn bl_can_edit_ven_targets(db: PgPool) {
        // TODO the scope write_vens is not separating between VEN and BL clients
        //  See also https://github.com/oadr3-org/specification/discussions/371
        //  Adopt the test as soon as this discussion is settled. Especially grant BL level access
        let test = ApiTest::new(db, "ven-1-client-id", vec![Scope::WriteVens]).await;

        let updated_ven = r#"
            {
              "venName":"updated-name",
              "objectType": "BL_VEN_REQUEST",
              "targets": ["group-2"],
              "clientID": "ven-1-client-id"
            }"#;
        let (status, ven) = test
            .request::<Ven>(Method::PUT, "/vens/ven-1", Body::from(updated_ven))
            .await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(ven.content.ven_name, "updated-name");
        assert_eq!(ven.content.targets, vec!["group-2".parse().unwrap()]);
    }

    #[sqlx::test(fixtures("users", "vens"))]
    async fn cannot_update_client_id(db: PgPool) {
        // TODO the scope write_vens is not separating between VEN and BL clients
        //  See also https://github.com/oadr3-org/specification/discussions/371
        //  Adopt the test as soon as this discussion is settled. Especially grant BL level access
        let test = ApiTest::new(db, "ven-1-client-id", vec![Scope::WriteVens]).await;

        // Updating the client_id should fail
        let updated_ven = r#"
            {
              "venName":"updated-name",
              "objectType": "BL_VEN_REQUEST",
              "targets": [],
              "clientID": "ven-2-client-id"
            }"#;
        let (status, problem) = test
            .request::<Problem>(Method::PUT, "/vens/ven-1", Body::from(updated_ven))
            .await;

        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(problem.status, StatusCode::BAD_REQUEST);
    }

    #[sqlx::test]
    async fn name_constraint_validation(db: PgPool) {
        let test = ApiTest::new(db, "test-client", vec![Scope::ReadAll]).await;

        let vens = [
            VenRequest::BlVenRequest(BlVenRequest::new("client_id".parse().unwrap(), "".to_string(), None, vec![])),
            VenRequest::BlVenRequest(BlVenRequest::new("client_id".parse().unwrap(), "This is more than 128 characters long and should be rejected This is more than 128 characters long and should be rejected asdfasd".to_string(), None, vec![])),
            VenRequest::VenVenRequest(VenVenRequest{attributes: None, ven_name: "".to_string()}),
            VenRequest::VenVenRequest(VenVenRequest{attributes: None, ven_name: "This is more than 128 characters long and should be rejected This is more than 128 characters long and should be rejected asdfasd".to_string()}),
                                     ];

        for ven in &vens {
            let (status, error) = test
                .request::<Problem>(
                    Method::POST,
                    "/vens",
                    Body::from(serde_json::to_vec(&ven).unwrap()),
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
