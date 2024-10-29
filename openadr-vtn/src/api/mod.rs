use crate::error::AppError;
use crate::state::AppState;
use axum::extract::State;
use axum::{
    async_trait,
    extract::{
        rejection::{FormRejection, JsonRejection},
        FromRequest, FromRequestParts, Request,
    },
    response::IntoResponse,
    Form, Json,
};
use axum_extra::extract::{Query, QueryRejection};
use reqwest::StatusCode;
use serde::de::DeserializeOwned;
use validator::Validate;

pub(crate) mod auth;
pub(crate) mod event;
pub(crate) mod program;
pub(crate) mod report;
pub(crate) mod resource;
pub(crate) mod user;
pub(crate) mod ven;

pub(crate) type AppResponse<T> = Result<Json<T>, AppError>;

#[derive(Debug, Clone)]
pub(crate) struct ValidatedForm<T>(T);

#[derive(Debug, Clone)]
pub(crate) struct ValidatedQuery<T>(pub T);

#[derive(Debug, Clone)]
pub(crate) struct ValidatedJson<T>(pub T);

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

pub async fn healthcheck(State(app_state): State<AppState>) -> Result<impl IntoResponse, AppError> {
    if !app_state.storage.connection_active() {
        return Err(AppError::SqlConnectionPoolClosed);
    }

    Ok((StatusCode::OK, "OK"))
}

#[cfg(test)]
#[cfg(feature = "live-db-test")]
mod test {
    use crate::{data_source::PostgresStorage, jwt::AuthRole, state::AppState};
    use axum::{
        body::Body,
        http::{self, Request, StatusCode},
        Router,
    };
    use http_body_util::BodyExt;
    use openadr_wire::problem::Problem;
    use reqwest::Method;
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
            let app_state = AppState::new(store);

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
            let json_body = serde_json::from_slice(&body).unwrap();

            (status, json_body)
        }

        ///Helper added because request would panic at the serde_json step if no body was returned
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
                        .header(http::header::CONTENT_TYPE, mime::APPLICATION_JSON.as_ref())
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();

            let status = response.status();
            status
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
        AppState::new(store)
    }

    #[sqlx::test]
    async fn unsupported_media_type(db: PgPool) {
        let mut test = ApiTest::new(
            db.clone(),
            vec![AuthRole::AnyBusiness, AuthRole::UserManager],
        );

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

        assert_eq!(response.status(), StatusCode::UNSUPPORTED_MEDIA_TYPE);

        let (status, _) = test
            .request::<Problem>(Method::POST, "/auth/token", Body::empty())
            .await;

        assert_eq!(status, StatusCode::UNSUPPORTED_MEDIA_TYPE);
    }

    #[sqlx::test]
    async fn method_not_allowed(db: PgPool) {
        let test = ApiTest::new(db.clone(), vec![]);

        let (status, _) = test
            .request::<Problem>(Method::DELETE, "/programs", Body::empty())
            .await;

        assert_eq!(status, StatusCode::METHOD_NOT_ALLOWED);
    }

    #[sqlx::test]
    async fn not_found(db: PgPool) {
        let test = ApiTest::new(db.clone(), vec![AuthRole::VenManager]);

        let (status, _) = test
            .request::<Problem>(Method::GET, "/not-existent", Body::empty())
            .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    #[sqlx::test]
    async fn healthcheck(db: PgPool) {
        let test = ApiTest::new(db.clone(), vec![]);

        let status = test.empty_request(Method::GET, "/health").await;
        assert_eq!(status, StatusCode::OK);
    }
}
