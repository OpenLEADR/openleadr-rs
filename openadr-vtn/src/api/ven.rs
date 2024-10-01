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
    if user.is_ven() {
        if !user.ven_ids().iter().any(|vid| *vid == id) {
            return Err(AppError::Forbidden("User does not have access to this VEN"));
        }
    } else if !user.is_ven_manager() {
        return Err(AppError::Forbidden("User is not a VEN or VEN Manager"));
    }

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
    use axum::{
        body::Body,
        http::{self, Request, Response},
        Router,
    };
    use http_body_util::BodyExt;
    use openadr_wire::Ven;
    use serde::de::DeserializeOwned;
    use sqlx::PgPool;
    use tower::ServiceExt;

    use crate::{
        api::test::jwt_test_token,
        data_source::PostgresStorage,
        jwt::{AuthRole, JwtManager},
        state::AppState,
    };

    async fn request_all(app: Router, token: &str) -> Response<Body> {
        app.oneshot(
            Request::builder()
                .method(http::Method::GET)
                .uri("/vens")
                .header(http::header::AUTHORIZATION, format!("Bearer {}", token))
                .header(http::header::CONTENT_TYPE, mime::APPLICATION_JSON.as_ref())
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap()
    }

    async fn get_response_json<T: DeserializeOwned>(response: Response<Body>) -> T {
        let body = response.into_body().collect().await.unwrap().to_bytes();

        serde_json::from_slice(&body).unwrap()
    }

    fn test_state(db: PgPool) -> AppState {
        let store = PostgresStorage::new(db).unwrap();
        let jwt_manager = JwtManager::from_base64_secret("test").unwrap();

        AppState::new(store, jwt_manager)
    }

    #[sqlx::test(fixtures("users", "vens"))]
    async fn get_all_unfiletred(db: PgPool) {
        let state = test_state(db);
        let token = jwt_test_token(&state, vec![AuthRole::VenManager]);
        let app = state.into_router();

        let resp = request_all(app, &token).await;

        assert_eq!(resp.status(), http::StatusCode::OK);
        let mut vens: Vec<Ven> = get_response_json(resp).await;

        assert_eq!(vens.len(), 2);
        vens.sort_by(|a, b| a.id.as_str().cmp(b.id.as_str()));
        assert_eq!(vens[0].id.as_str(), "ven-1");
        assert_eq!(vens[1].id.as_str(), "ven-2");
    }

    #[sqlx::test(fixtures("users", "vens"))]
    async fn get_all_ven_user(db: PgPool) {
        let state = test_state(db);
        let token = jwt_test_token(&state, vec![AuthRole::VEN("ven-1".parse().unwrap())]);
        let app = state.into_router();

        let resp = request_all(app, &token).await;

        assert_eq!(resp.status(), http::StatusCode::OK);
        let vens: Vec<Ven> = get_response_json(resp).await;

        assert_eq!(vens.len(), 1);
        assert_eq!(vens[0].id.as_str(), "ven-1");
    }
}
