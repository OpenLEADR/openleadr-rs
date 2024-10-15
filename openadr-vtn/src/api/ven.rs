use std::sync::Arc;

use axum::{
    extract::{Path, State},
    Json,
};
use reqwest::StatusCode;
use serde::Deserialize;
use tracing::{info, trace};
use validator::{Validate, ValidationError};

use openadr_wire::{
    target::TargetLabel,
    ven::{Ven, VenContent, VenId},
};

use crate::{
    api::{AppResponse, ValidatedJson, ValidatedQuery},
    data_source::VenCrud,
    error::AppError,
    jwt::{User, VenManagerUser},
};

pub async fn get_all(
    State(ven_source): State<Arc<dyn VenCrud>>,
    ValidatedQuery(query_params): ValidatedQuery<QueryParams>,
    User(user): User,
) -> AppResponse<Vec<Ven>> {
    trace!(?query_params);

    let vens = ven_source
        .retrieve_all(&query_params, &user.try_into()?)
        .await?;

    trace!("retrieved {} VENs", vens.len());

    Ok(Json(vens))
}

pub async fn get(
    State(ven_source): State<Arc<dyn VenCrud>>,
    Path(id): Path<VenId>,
    User(user): User,
) -> AppResponse<Ven> {
    let ven = ven_source.retrieve(&id, &user.try_into()?).await?;

    trace!(%ven.id, ven.ven_name=ven.content.ven_name, "VEN retrieved");

    Ok(Json(ven))
}

pub async fn add(
    State(ven_source): State<Arc<dyn VenCrud>>,
    VenManagerUser(user): VenManagerUser,
    ValidatedJson(new_ven): ValidatedJson<VenContent>,
) -> Result<(StatusCode, Json<Ven>), AppError> {
    let ven = ven_source.create(new_ven, &user.try_into()?).await?;

    info!(%ven.id, ven.ven_name=ven.content.ven_name, "VEN added");

    Ok((StatusCode::CREATED, Json(ven)))
}

pub async fn edit(
    State(ven_source): State<Arc<dyn VenCrud>>,
    Path(id): Path<VenId>,
    VenManagerUser(user): VenManagerUser,
    ValidatedJson(content): ValidatedJson<VenContent>,
) -> AppResponse<Ven> {
    let ven = ven_source.update(&id, content, &user.try_into()?).await?;

    info!(%ven.id, ven.ven_name=ven.content.ven_name, "VEN updated");

    Ok(Json(ven))
}

pub async fn delete(
    State(ven_source): State<Arc<dyn VenCrud>>,
    Path(id): Path<VenId>,
    VenManagerUser(user): VenManagerUser,
) -> AppResponse<Ven> {
    let ven = ven_source.delete(&id, &user.try_into()?).await?;
    info!(%ven.id, ven.ven_name=ven.content.ven_name, "VEN deleted");
    Ok(Json(ven))
}

#[derive(Deserialize, Validate, Debug)]
#[validate(schema(function = "validate_target_type_value_pair"))]
#[serde(rename_all = "camelCase")]
pub struct QueryParams {
    #[validate(length(min = 1, max = 128))]
    pub(crate) ven_name: Option<String>,
    pub(crate) target_type: Option<TargetLabel>,
    pub(crate) target_values: Option<Vec<String>>,
    #[serde(default)]
    #[validate(range(min = 0))]
    pub(crate) skip: i64,
    #[validate(range(min = 1, max = 50))]
    #[serde(default = "get_50")]
    pub(crate) limit: i64,
}

fn validate_target_type_value_pair(query: &QueryParams) -> Result<(), ValidationError> {
    if query.target_type.is_some() == query.target_values.is_some() {
        Ok(())
    } else {
        Err(ValidationError::new("targetType and targetValues query parameter must either both be set or not set at the same time."))
    }
}

fn get_50() -> i64 {
    50
}

#[cfg(test)]
mod tests {
    use crate::{api::test::ApiTest, jwt::AuthRole};
    use axum::{body::Body, http::StatusCode};
    use openadr_wire::{problem::Problem, ven::VenContent, Ven};
    use reqwest::Method;
    use sqlx::PgPool;

    fn default() -> VenContent {
        VenContent {
            object_type: Default::default(),
            ven_name: "".to_string(),
            attributes: None,
            targets: None,
            resources: None,
        }
    }

    #[sqlx::test(fixtures("users", "vens"))]
    async fn get_all_unfiletred(db: PgPool) {
        let test = ApiTest::new(db, vec![AuthRole::VenManager]);

        let (status, mut vens) = test
            .request::<Vec<Ven>>(Method::GET, "/vens", Body::empty())
            .await;
        assert_eq!(status, StatusCode::OK);

        assert_eq!(vens.len(), 2);
        vens.sort_by(|a, b| a.id.as_str().cmp(b.id.as_str()));
        assert_eq!(vens[0].id.as_str(), "ven-1");
        assert_eq!(vens[1].id.as_str(), "ven-2");
    }

    #[sqlx::test(fixtures("users", "vens"))]
    async fn get_all_filetred(db: PgPool) {
        let test = ApiTest::new(db.clone(), vec![AuthRole::VenManager]);

        let (status, vens) = test
            .request::<Vec<Ven>>(Method::GET, "/vens?skip=1", Body::empty())
            .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(vens.len(), 1);

        let (status, vens) = test
            .request::<Vec<Ven>>(Method::GET, "/vens?limit=1", Body::empty())
            .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(vens.len(), 1);

        let (status, vens) = test
            .request::<Vec<Ven>>(
                Method::GET,
                "/vens?targetType=GROUP&targetValues=group-1",
                Body::empty(),
            )
            .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(vens.len(), 1);
        assert_eq!(vens[0].id.as_str(), "ven-1");

        let test = ApiTest::new(db, vec![AuthRole::VEN("ven-1".parse().unwrap())]);

        let (status, vens) = test
            .request::<Vec<Ven>>(Method::GET, "/vens", Body::empty())
            .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(vens.len(), 1);
    }

    #[sqlx::test(fixtures("users", "vens"))]
    async fn get_all_ven_user(db: PgPool) {
        let test = ApiTest::new(db, vec![AuthRole::VEN("ven-1".parse().unwrap())]);

        let (status, vens) = test
            .request::<Vec<Ven>>(Method::GET, "/vens", Body::empty())
            .await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(vens.len(), 1);
        assert_eq!(vens[0].id.as_str(), "ven-1");
    }

    #[sqlx::test(fixtures("users", "vens"))]
    async fn get_single(db: PgPool) {
        let test = ApiTest::new(db, vec![AuthRole::VenManager]);

        let (status, ven) = test
            .request::<Ven>(Method::GET, "/vens/ven-1", Body::empty())
            .await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(ven.id.as_str(), "ven-1");
    }

    #[sqlx::test(fixtures("users", "vens"))]
    async fn add_edit_delete_ven(db: PgPool) {
        let test = ApiTest::new(db, vec![AuthRole::VenManager]);

        let new_ven = r#"{"venName":"new-ven"}"#;
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

        let new_ven = r#"{"venName":"new-ven-2"}"#;
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

    #[sqlx::test]
    async fn name_constraint_validation(db: PgPool) {
        let test = ApiTest::new(db, vec![AuthRole::VenManager]);

        let vens = [
            VenContent {
                ven_name: "".to_string(),
                ..default()
            },
            VenContent {
                ven_name: "This is more than 128 characters long and should be rejected This is more than 128 characters long and should be rejected asdfasd".to_string(),
                ..default()
            },
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
