use crate::error::AppError;
use axum::{
    async_trait,
    extract::{
        rejection::{FormRejection, JsonRejection},
        FromRequest, FromRequestParts, Request,
    },
    Form, Json,
};
use axum_extra::extract::{Query, QueryRejection};
use serde::de::DeserializeOwned;
use validator::Validate;

pub mod auth;
pub mod event;
pub mod program;
pub mod report;
pub mod resource;
pub mod user;
pub mod ven;

pub type AppResponse<T> = Result<Json<T>, AppError>;

#[derive(Debug, Clone)]
pub struct ValidatedForm<T>(T);

#[derive(Debug, Clone)]
pub struct ValidatedQuery<T>(pub T);

#[derive(Debug, Clone)]
pub struct ValidatedJson<T>(pub T);

#[async_trait]
impl<T, S> FromRequest<S> for ValidatedJson<T>
where
    T: DeserializeOwned + Validate,
    S: Send + Sync,
    Json<T>: FromRequest<S, Rejection = JsonRejection>,
{
    type Rejection = AppError;

    async fn from_request(req: Request, state: &S) -> Result<Self, Self::Rejection> {
        let Json(value) = Json::<T>::from_request(req, state).await?;
        value.validate()?;
        Ok(ValidatedJson(value))
    }
}

#[async_trait]
impl<T, S> FromRequestParts<S> for ValidatedQuery<T>
where
    T: DeserializeOwned + Validate,
    S: Send + Sync,
    Query<T>: FromRequestParts<S, Rejection = QueryRejection>,
{
    type Rejection = AppError;

    async fn from_request_parts(
        parts: &mut axum::http::request::Parts,
        state: &S,
    ) -> Result<Self, Self::Rejection> {
        let Query(value) = Query::<T>::from_request_parts(parts, state).await?;
        value.validate()?;
        Ok(ValidatedQuery(value))
    }
}

#[async_trait]
impl<T, S> FromRequest<S> for ValidatedForm<T>
where
    T: DeserializeOwned + Validate,
    S: Send + Sync,
    Form<T>: FromRequest<S, Rejection = FormRejection>,
{
    type Rejection = AppError;

    async fn from_request(req: Request, state: &S) -> Result<Self, Self::Rejection> {
        let Form(value) = Form::<T>::from_request(req, state).await?;
        value.validate()?;
        Ok(ValidatedForm(value))
    }
}

#[cfg(test)]
#[cfg(feature = "live-db-test")]
mod test {
    use crate::{
        data_source::PostgresStorage,
        jwt::{AuthRole, JwtManager},
        state::AppState,
    };
    use axum::{
        body::Body,
        http::{self, Request, StatusCode},
        response::Response,
        Router,
    };
    use http_body_util::BodyExt;
    use openadr_wire::problem::Problem;
    use serde::de::DeserializeOwned;
    use sqlx::PgPool;
    use tower::ServiceExt;

    pub(crate) struct ApiTest {
        router: Router,
        token: String,
    }

    impl ApiTest {
        pub(crate) fn new(db: PgPool, roles: Vec<AuthRole>) -> Self {
            let store = PostgresStorage::new(db).unwrap();
            let jwt_manager = JwtManager::from_base64_secret("test").unwrap();
            let app_state = AppState::new(store, jwt_manager);

            let token = app_state
                .jwt_manager
                .create(
                    std::time::Duration::from_secs(60),
                    "test_admin".to_string(),
                    roles,
                )
                .unwrap();

            let router = app_state.into_router();

            Self { router, token }
        }

        pub(crate) async fn request<T: DeserializeOwned>(
            &self,
            method: http::Method,
            path: &str,
            body: Body,
        ) -> (http::StatusCode, T) {
            let response = self
                .router
                .clone()
                .oneshot(
                    Request::builder()
                        .method(method)
                        .uri(path)
                        .header(
                            http::header::AUTHORIZATION,
                            format!("Bearer {}", self.token),
                        )
                        .header(http::header::CONTENT_TYPE, mime::APPLICATION_JSON.as_ref())
                        .body(body)
                        .unwrap(),
                )
                .await
                .unwrap();

            let status = response.status();
            let body = response.into_body().collect().await.unwrap().to_bytes();
            let json_body = serde_json::from_slice(&body).unwrap();

            (status, json_body)
        }
    }

    pub(crate) fn jwt_test_token(state: &AppState, roles: Vec<AuthRole>) -> String {
        state
            .jwt_manager
            .create(
                std::time::Duration::from_secs(60),
                "test_admin".to_string(),
                roles,
            )
            .unwrap()
    }

    pub(crate) async fn state(db: PgPool) -> AppState {
        let store = PostgresStorage::new(db).unwrap();
        AppState::new(store, JwtManager::from_base64_secret("test").unwrap())
    }

    async fn into_problem(response: Response<Body>) -> Problem {
        let body = response.into_body().collect().await.unwrap().to_bytes();
        serde_json::from_slice(&body).unwrap()
    }

    #[sqlx::test]
    async fn unsupported_media_type(db: PgPool) {
        let state = state(db).await;
        let token = jwt_test_token(&state, vec![AuthRole::AnyBusiness, AuthRole::UserManager]);
        let mut app = state.into_router();

        let response = (&mut app)
            .oneshot(
                Request::builder()
                    .method(http::Method::POST)
                    .uri("/programs")
                    .header(http::header::AUTHORIZATION, format!("Bearer {}", token))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::UNSUPPORTED_MEDIA_TYPE);
        into_problem(response).await;

        let response = app
            .oneshot(
                Request::builder()
                    .method(http::Method::POST)
                    .uri("/auth/token")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::UNSUPPORTED_MEDIA_TYPE);
        into_problem(response).await;
    }

    #[sqlx::test]
    async fn method_not_allowed(db: PgPool) {
        let state = state(db).await;
        let app = state.into_router();

        let response = app
            .oneshot(
                Request::builder()
                    .method(http::Method::DELETE)
                    .uri("/programs")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);

        into_problem(response).await;
    }
}
