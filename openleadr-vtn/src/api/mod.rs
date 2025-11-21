use std::fmt::Debug;
use crate::{error::AppError, state::AppState};
use axum::{
    extract::{
        rejection::{FormRejection, JsonRejection},
        FromRequest, FromRequestParts, Request, State,
    },
    response::IntoResponse,
    Form, Json,
};
use axum_extra::extract::{Query, QueryRejection};
use openleadr_wire::target::Target;
use reqwest::StatusCode;
use serde::{de::DeserializeOwned, Deserialize};
use validator::Validate;

pub(crate) mod auth;
pub(crate) mod event;
pub(crate) mod program;
pub(crate) mod report;
pub(crate) mod resource;
#[cfg(feature = "internal-oauth")]
pub(crate) mod user;
pub(crate) mod ven;

pub(crate) type AppResponse<T> = Result<Json<T>, AppError>;

#[derive(Debug, Clone)]
pub(crate) struct ValidatedForm<T>(T);

#[derive(Debug, Clone)]
pub(crate) struct ValidatedQuery<T>(pub T);

#[derive(Debug, Clone)]
pub(crate) struct ValidatedJson<T>(pub T);

#[derive(Deserialize, Debug, Clone)]
#[serde(transparent)]
pub(crate) struct TargetQueryParams(pub Option<Vec<Target>>);

impl TargetQueryParams {
    pub fn as_deref(&self) -> &[Target] {
        self.0.as_deref().unwrap_or_default()
    }
}

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

impl<T, S> FromRequestParts<S> for ValidatedQuery<T>
where
    T: DeserializeOwned + Validate + Debug,
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

pub async fn healthcheck(State(app_state): State<AppState>) -> Result<impl IntoResponse, AppError> {
    if !app_state.storage.connection_active() {
        return Err(AppError::StorageConnectionError);
    }

    Ok((StatusCode::OK, "OK"))
}

#[cfg(test)]
#[cfg(feature = "live-db-test")]
mod test {
    use crate::{data_source::PostgresStorage, jwt::Scope, state::AppState};
    use axum::{
        body::Body,
        http::{self, Request, StatusCode},
        Router,
    };
    use http_body_util::BodyExt;
    use openleadr_wire::problem::Problem;
    use reqwest::Method;
    use serde::de::DeserializeOwned;
    use sqlx::PgPool;
    use std::fmt::Display;
    use tower::ServiceExt;

    pub(crate) struct ApiTest {
        router: Router,
        token: String,
    }

    impl ApiTest {
        pub(crate) async fn new(db: PgPool, client_id: impl Display, scope: Vec<Scope>) -> Self {
            let store = PostgresStorage::new(db).unwrap();
            let app_state = AppState::new(store).await;

            let token = app_state
                .jwt_manager
                .create(
                    std::time::Duration::from_secs(60),
                    client_id.to_string(),
                    scope,
                )
                .unwrap();

            let router = app_state.into_router();

            Self { router, token }
        }

        pub(crate) async fn request<T: DeserializeOwned>(
            &self,
            method: Method,
            path: &str,
            body: Body,
        ) -> (StatusCode, T) {
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
            if status.is_server_error() {
                panic!("{status}: {}", String::from_utf8_lossy(&body));
            }
            let json_body = serde_json::from_slice(&body)
                .unwrap_or_else(|e| panic!("{e}: {}", String::from_utf8_lossy(&body)));

            (status, json_body)
        }

        pub(crate) async fn empty_request(&self, method: Method, path: &str) -> StatusCode {
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
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();

            response.status()
        }
    }

    #[cfg(feature = "internal-oauth")]
    pub(crate) fn jwt_test_token(
        state: &AppState,
        client_id: impl Display,
        scope: Vec<Scope>,
    ) -> String {
        state
            .jwt_manager
            .create(
                std::time::Duration::from_secs(60),
                client_id.to_string(),
                scope,
            )
            .unwrap()
    }

    pub(crate) async fn state(db: PgPool) -> AppState {
        let store = PostgresStorage::new(db).unwrap();
        AppState::new(store).await
    }

    #[sqlx::test]
    async fn unsupported_media_type(db: PgPool) {
        let mut test = ApiTest::new(
            db.clone(),
            "test-client",
            vec![Scope::ReadAll, Scope::WritePrograms],
        )
        .await;

        let response = (&mut test.router)
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/programs")
                    .header(
                        http::header::AUTHORIZATION,
                        format!("Bearer {}", test.token),
                    )
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        let status = response.status();
        let body = String::from_utf8(
            response
                .into_body()
                .collect()
                .await
                .unwrap()
                .to_bytes()
                .to_vec(),
        )
        .unwrap();
        println!("Response body: {}", body);
        assert_eq!(status, StatusCode::UNSUPPORTED_MEDIA_TYPE);

        let (status, _) = test
            .request::<Problem>(Method::POST, "/auth/token", Body::empty())
            .await;

        assert_eq!(status, StatusCode::UNSUPPORTED_MEDIA_TYPE);
    }

    #[sqlx::test]
    async fn method_not_allowed(db: PgPool) {
        let test = ApiTest::new(db.clone(), "test-client", vec![]).await;

        let (status, _) = test
            .request::<Problem>(Method::DELETE, "/programs", Body::empty())
            .await;

        assert_eq!(status, StatusCode::METHOD_NOT_ALLOWED);
    }

    #[sqlx::test]
    async fn not_found(db: PgPool) {
        let test = ApiTest::new(db.clone(), "test-client", vec![Scope::WriteVens]).await;

        let (status, _) = test
            .request::<Problem>(Method::GET, "/not-existent", Body::empty())
            .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    #[sqlx::test]
    async fn healthcheck(db: PgPool) {
        let test = ApiTest::new(db.clone(), "test-client", vec![]).await;

        let status = test.empty_request(Method::GET, "/health").await;
        assert_eq!(status, StatusCode::OK);
    }
}
