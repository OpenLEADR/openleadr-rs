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

    Ok(Json(vens))
}

pub async fn get(
    State(ven_source): State<Arc<dyn VenCrud>>,
    Path(id): Path<VenId>,
    User(user): User,
) -> AppResponse<Ven> {
    let ven = ven_source.retrieve(&id, &user.try_into()?).await?;

    Ok(Json(ven))
}

pub async fn add(
    State(ven_source): State<Arc<dyn VenCrud>>,
    VenManagerUser(user): VenManagerUser,
    ValidatedJson(new_ven): ValidatedJson<VenContent>,
) -> Result<(StatusCode, Json<Ven>), AppError> {
    let ven = ven_source.create(new_ven, &user.try_into()?).await?;

    Ok((StatusCode::CREATED, Json(ven)))
}

pub async fn edit(
    State(ven_source): State<Arc<dyn VenCrud>>,
    Path(id): Path<VenId>,
    VenManagerUser(user): VenManagerUser,
    ValidatedJson(content): ValidatedJson<VenContent>,
) -> AppResponse<Ven> {
    let ven = ven_source.update(&id, content, &user.try_into()?).await?;

    info!(%ven.id, ven.ven_name=ven.content.ven_name, "ven updated");

    Ok(Json(ven))
}

pub async fn delete(
    State(ven_source): State<Arc<dyn VenCrud>>,
    Path(id): Path<VenId>,
    VenManagerUser(user): VenManagerUser,
) -> AppResponse<Ven> {
    let ven = ven_source.delete(&id, &user.try_into()?).await?;
    info!(%id, "deleted ven");
    Ok(Json(ven))
}

#[derive(Deserialize, Validate, Debug)]
#[validate(schema(function = "validate_target_type_value_pair"))]
#[serde(rename_all = "camelCase")]
pub struct QueryParams {
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
    use axum::{body::Body, http};
    use openadr_wire::Ven;
    use reqwest::Method;
    use sqlx::PgPool;

    use crate::{api::test::ApiTest, jwt::AuthRole};

    #[sqlx::test(fixtures("users", "vens"))]
    async fn get_all_unfiletred(db: PgPool) {
        let test = ApiTest::new(db, vec![AuthRole::VenManager]);

        let (status, mut vens) = test
            .request::<Vec<Ven>>(Method::GET, "/vens", Body::empty())
            .await;
        assert_eq!(status, http::StatusCode::OK);

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
        assert_eq!(status, http::StatusCode::OK);
        assert_eq!(vens.len(), 1);

        let (status, vens) = test
            .request::<Vec<Ven>>(Method::GET, "/vens?limit=1", Body::empty())
            .await;
        assert_eq!(status, http::StatusCode::OK);
        assert_eq!(vens.len(), 1);

        let (status, vens) = test
            .request::<Vec<Ven>>(
                Method::GET,
                "/vens?targetType=VEN_NAME&targetValues=ven-2-name",
                Body::empty(),
            )
            .await;
        assert_eq!(status, http::StatusCode::OK);
        assert_eq!(vens.len(), 1);
        assert_eq!(vens[0].id.as_str(), "ven-2");

        let test = ApiTest::new(db, vec![AuthRole::VEN("ven-1".parse().unwrap())]);

        let (status, vens) = test
            .request::<Vec<Ven>>(
                Method::GET,
                "/vens?targetType=VEN_NAME&targetValues=ven-1-name",
                Body::empty(),
            )
            .await;
        assert_eq!(status, http::StatusCode::OK);
        assert_eq!(vens.len(), 1);

        let (status, vens) = test
            .request::<Vec<Ven>>(
                Method::GET,
                "/vens?targetType=VEN_NAME&targetValues=ven-2-name",
                Body::empty(),
            )
            .await;
        assert_eq!(status, http::StatusCode::OK);
        assert_eq!(vens.len(), 0);
    }

    #[sqlx::test(fixtures("users", "vens"))]
    async fn get_all_ven_user(db: PgPool) {
        let test = ApiTest::new(db, vec![AuthRole::VEN("ven-1".parse().unwrap())]);

        let (status, vens) = test
            .request::<Vec<Ven>>(Method::GET, "/vens", Body::empty())
            .await;

        assert_eq!(status, http::StatusCode::OK);
        assert_eq!(vens.len(), 1);
        assert_eq!(vens[0].id.as_str(), "ven-1");
    }

    #[sqlx::test(fixtures("users", "vens"))]
    async fn get_single(db: PgPool) {
        let test = ApiTest::new(db, vec![AuthRole::VenManager]);

        let (status, ven) = test
            .request::<Ven>(Method::GET, "/vens/ven-1", Body::empty())
            .await;

        assert_eq!(status, http::StatusCode::OK);
        assert_eq!(ven.id.as_str(), "ven-1");
    }

    #[sqlx::test(fixtures("users", "vens"))]
    async fn add_edit_delete_ven(db: PgPool) {
        let test = ApiTest::new(db, vec![AuthRole::VenManager]);

        let new_ven = r#"{"venName":"new-ven"}"#;
        let (status, ven) = test
            .request::<Ven>(Method::POST, "/vens", Body::from(new_ven))
            .await;

        assert_eq!(status, http::StatusCode::CREATED);
        assert_eq!(ven.content.ven_name, "new-ven");

        let ven_id = ven.id.as_str();

        let (status, ven) = test
            .request::<Ven>(Method::GET, &format!("/vens/{ven_id}"), Body::empty())
            .await;
        assert_eq!(status, http::StatusCode::OK);
        assert_eq!(ven.id.as_str(), ven_id);

        let new_ven = r#"{"venName":"new-ven-2"}"#;
        let (status, ven) = test
            .request::<Ven>(Method::PUT, &format!("/vens/{ven_id}"), Body::from(new_ven))
            .await;

        assert_eq!(status, http::StatusCode::OK);
        assert_eq!(ven.content.ven_name, "new-ven-2");

        let (status, ven) = test
            .request::<Ven>(Method::GET, &format!("/vens/{ven_id}"), Body::empty())
            .await;
        assert_eq!(status, http::StatusCode::OK);
        assert_eq!(ven.content.ven_name, "new-ven-2");
        assert_eq!(ven.id.as_str(), ven_id);

        let (status, ven) = test
            .request::<Ven>(Method::DELETE, &format!("/vens/{ven_id}"), Body::empty())
            .await;

        assert_eq!(status, http::StatusCode::OK);
        assert_eq!(ven.id.as_str(), ven_id);

        let (status, _) = test
            .request::<serde_json::Value>(Method::GET, &format!("/vens/{ven_id}"), Body::empty())
            .await;
        assert_eq!(status, http::StatusCode::NOT_FOUND);
    }
}
