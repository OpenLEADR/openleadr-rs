#[cfg(feature = "sqlx")]
use argon2::password_hash;
use axum::{
    Json,
    extract::rejection::{FormRejection, JsonRejection},
    http::{HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
};
use axum_extra::extract::QueryRejection;
use openleadr_wire::{IdentifierError, problem::Problem};
#[cfg(feature = "sqlx")]
use sqlx::error::DatabaseError;
#[cfg(feature = "sqlx")]
use tracing::warn;
use tracing::{error, info, trace};
use uuid::Uuid;

#[derive(thiserror::Error, Debug)]
pub enum AppError {
    #[error("Invalid request: {0}")]
    Validation(#[from] validator::ValidationErrors),
    #[error("Invalid request: {0}")]
    Json(JsonRejection),
    #[error("Invalid request: {0}")]
    Form(FormRejection),
    #[error("Invalid request: {0}")]
    QueryParams(#[from] QueryRejection),
    #[error("Object not found")]
    NotFound,
    #[error("Bad request: {0}")]
    BadRequest(&'static str),
    #[error("Forbidden: {0}")]
    Forbidden(&'static str),
    #[error("Not implemented {0}")]
    NotImplemented(&'static str),
    #[cfg(feature = "sqlx")]
    #[error("Conflict: {0}")]
    Conflict(String, Option<Box<dyn DatabaseError>>),
    #[cfg(feature = "sqlx")]
    #[error("Unprocessable Content: {0}")]
    ForeignKeyConstraintViolated(String, Option<Box<dyn DatabaseError>>),
    #[error("Authentication error: {0}")]
    Auth(String),
    #[cfg(feature = "sqlx")]
    #[error("database error: {0}")]
    Sql(sqlx::Error),
    #[error("MQTT error: {0}")]
    Mqtt(paho_mqtt::Error),
    #[error("Storage connection pool closed")]
    StorageConnectionError,
    #[cfg(feature = "sqlx")]
    #[error("Json (de)serialization error : {0}")]
    SerdeJsonInternalServerError(serde_json::Error),
    #[cfg(feature = "sqlx")]
    #[error("Json (de)serialization error : {0}")]
    SerdeJsonBadRequest(serde_json::Error),
    #[error("Malformed Identifier")]
    Identifier(#[from] IdentifierError),
    #[error("Method not allowed")]
    MethodNotAllowed,
    #[cfg(feature = "sqlx")]
    #[error("Password Hash error: {0}")]
    PasswordHashError(password_hash::Error),
    #[error("Unsupported Media Type: {0}")]
    UnsupportedMediaType(String),
}

#[cfg(feature = "sqlx")]
impl From<sqlx::Error> for AppError {
    fn from(err: sqlx::Error) -> Self {
        match err {
            sqlx::Error::RowNotFound => Self::NotFound,
            sqlx::Error::Database(err) if err.is_unique_violation() => {
                Self::Conflict("Conflict".to_string(), Some(err))
            }
            sqlx::Error::Database(err) if err.is_foreign_key_violation() => {
                Self::ForeignKeyConstraintViolated(
                    "A foreign key constraint is violated".to_string(),
                    Some(err),
                )
            }
            _ => Self::Sql(err),
        }
    }
}

impl From<paho_mqtt::Error> for AppError {
    fn from(value: paho_mqtt::Error) -> Self {
        Self::Mqtt(value)
    }
}

impl From<JsonRejection> for AppError {
    fn from(rejection: JsonRejection) -> Self {
        match rejection {
            JsonRejection::MissingJsonContentType(text) => {
                AppError::UnsupportedMediaType(text.to_string())
            }
            _ => AppError::Json(rejection),
        }
    }
}

impl From<FormRejection> for AppError {
    fn from(rejection: FormRejection) -> Self {
        match rejection {
            FormRejection::InvalidFormContentType(text) => {
                AppError::UnsupportedMediaType(text.to_string())
            }
            _ => AppError::Form(rejection),
        }
    }
}
#[cfg(feature = "sqlx")]
impl From<password_hash::Error> for AppError {
    fn from(hash_err: password_hash::Error) -> Self {
        Self::PasswordHashError(hash_err)
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let reference = Uuid::new_v4();

        let problem = match self {
            AppError::Validation(err) => {
                trace!(%reference,
                    "Received invalid request: {}",
                    err
                );
                Problem {
                    r#type: Default::default(),
                    title: Some(StatusCode::BAD_REQUEST.to_string()),
                    status: StatusCode::BAD_REQUEST,
                    detail: Some(err.to_string()),
                    instance: Some(reference.to_string()),
                }
            }
            AppError::Json(err) => {
                trace!(%reference,
                    "Received invalid JSON in request: {}",
                    err.body_text(),
                );
                Problem {
                    r#type: Default::default(),
                    title: Some(StatusCode::BAD_REQUEST.to_string()),
                    status: StatusCode::BAD_REQUEST,
                    detail: Some(err.body_text()),
                    instance: Some(reference.to_string()),
                }
            }
            AppError::Form(err) => {
                trace!(%reference,
                    "Received invalid form data: {}",
                    err
                );
                Problem {
                    r#type: Default::default(),
                    title: Some(StatusCode::BAD_REQUEST.to_string()),
                    status: StatusCode::BAD_REQUEST,
                    detail: Some(err.to_string()),
                    instance: Some(reference.to_string()),
                }
            }
            AppError::QueryParams(err) => {
                trace!(%reference,
                    "Received invalid query parameters: {}",
                    err
                );
                Problem {
                    r#type: Default::default(),
                    title: Some(StatusCode::BAD_REQUEST.to_string()),
                    status: StatusCode::BAD_REQUEST,
                    detail: Some(err.to_string()),
                    instance: Some(reference.to_string()),
                }
            }
            AppError::NotFound => {
                trace!(%reference, "Object not found");
                Problem {
                    r#type: Default::default(),
                    title: Some(StatusCode::NOT_FOUND.to_string()),
                    status: StatusCode::NOT_FOUND,
                    detail: None,
                    instance: Some(reference.to_string()),
                }
            }
            AppError::BadRequest(err) => {
                trace!(%reference,
                    "Received invalid request: {}",
                    err
                );
                Problem {
                    r#type: Default::default(),
                    title: Some(StatusCode::BAD_REQUEST.to_string()),
                    status: StatusCode::BAD_REQUEST,
                    detail: Some(err.to_string()),
                    instance: Some(reference.to_string()),
                }
            }
            AppError::Forbidden(err) => {
                trace!(%reference,
                    "Forbidden: {}",
                    err
                );
                Problem {
                    r#type: Default::default(),
                    title: Some(StatusCode::FORBIDDEN.to_string()),
                    status: StatusCode::FORBIDDEN,
                    detail: Some(err.to_string()),
                    instance: Some(reference.to_string()),
                }
            }
            AppError::NotImplemented(err) => {
                error!(%reference, "Not implemented: {}", err);
                Problem {
                    r#type: Default::default(),
                    title: Some(StatusCode::NOT_IMPLEMENTED.to_string()),
                    status: StatusCode::NOT_IMPLEMENTED,
                    detail: Some(err.to_string()),
                    instance: Some(reference.to_string()),
                }
            }
            #[cfg(feature = "sqlx")]
            AppError::Conflict(err, db_err) => {
                warn!(%reference, "Conflict: {}, DB err: {:?}", err, db_err);
                Problem {
                    r#type: Default::default(),
                    title: Some(StatusCode::CONFLICT.to_string()),
                    status: StatusCode::CONFLICT,
                    detail: Some(err.to_string()),
                    instance: Some(reference.to_string()),
                }
            }
            AppError::Auth(err) => {
                trace!(%reference,
                    "Authentication error: {}",
                    err
                );
                Problem {
                    r#type: Default::default(),
                    title: Some(StatusCode::UNAUTHORIZED.to_string()),
                    status: StatusCode::UNAUTHORIZED,
                    detail: Some(err.to_string()),
                    instance: Some(reference.to_string()),
                }
            }
            #[cfg(feature = "sqlx")]
            AppError::Sql(err) => {
                error!(%reference, "SQL error: {}", err);
                Problem {
                    r#type: Default::default(),
                    title: Some(StatusCode::INTERNAL_SERVER_ERROR.to_string()),
                    status: StatusCode::INTERNAL_SERVER_ERROR,
                    detail: Some("A database error occurred".to_string()),
                    instance: Some(reference.to_string()),
                }
            }
            AppError::Mqtt(err) => {
                error!(%reference, "MQTT error: {}", err);
                Problem {
                    r#type: Default::default(),
                    title: Some(StatusCode::INTERNAL_SERVER_ERROR.to_string()),
                    status: StatusCode::INTERNAL_SERVER_ERROR,
                    detail: Some("An mqtt error occurred".to_string()),
                    instance: Some(reference.to_string()),
                }
            }
            AppError::StorageConnectionError => {
                error!(%reference, "Storage connection pool closed");
                Problem {
                    r#type: Default::default(),
                    title: Some(StatusCode::INTERNAL_SERVER_ERROR.to_string()),
                    status: StatusCode::INTERNAL_SERVER_ERROR,
                    detail: Some("Storage connection pool closed".to_string()),
                    instance: Some(reference.to_string()),
                }
            }
            #[cfg(feature = "sqlx")]
            AppError::SerdeJsonInternalServerError(err) => {
                trace!(%reference, "serde json error: {}", err);
                Problem {
                    r#type: Default::default(),
                    title: Some(StatusCode::INTERNAL_SERVER_ERROR.to_string()),
                    status: StatusCode::INTERNAL_SERVER_ERROR,
                    detail: None,
                    instance: Some(reference.to_string()),
                }
            }
            #[cfg(feature = "sqlx")]
            AppError::SerdeJsonBadRequest(err) => {
                trace!(%reference, "serde json error: {}", err);
                Problem {
                    r#type: Default::default(),
                    title: Some(StatusCode::BAD_REQUEST.to_string()),
                    status: StatusCode::BAD_REQUEST,
                    detail: Some(err.to_string()),
                    instance: Some(reference.to_string()),
                }
            }
            AppError::Identifier(err) => {
                trace!(%reference,
                    "Malformed identifier: {}",
                    err
                );
                Problem {
                    r#type: Default::default(),
                    title: Some(StatusCode::BAD_REQUEST.to_string()),
                    status: StatusCode::BAD_REQUEST,
                    detail: Some(err.to_string()),
                    instance: Some(reference.to_string()),
                }
            }
            #[cfg(feature = "sqlx")]
            AppError::ForeignKeyConstraintViolated(err, db_err) => {
                trace!(%reference,
                    "Unprocessable Content: {}, DB details: {:?}",
                    err,
                    db_err
                );
                Problem {
                    r#type: Default::default(),
                    title: Some(StatusCode::CONFLICT.to_string()),
                    status: StatusCode::CONFLICT,
                    detail: Some(err.to_string()),
                    instance: Some(reference.to_string()),
                }
            }
            AppError::MethodNotAllowed => {
                trace!(%reference,
                    "Method not allowed"
                );
                Problem {
                    r#type: Default::default(),
                    title: Some(StatusCode::METHOD_NOT_ALLOWED.to_string()),
                    status: StatusCode::METHOD_NOT_ALLOWED,
                    detail: Some("See allow headers for allowed methods".to_string()),
                    instance: Some(reference.to_string()),
                }
            }
            #[cfg(feature = "sqlx")]
            AppError::PasswordHashError(err) => {
                warn!(%reference,
                "Password hash error: {}",
                err);
                Problem {
                    r#type: Default::default(),
                    title: Some(StatusCode::INTERNAL_SERVER_ERROR.to_string()),
                    status: StatusCode::INTERNAL_SERVER_ERROR,
                    detail: Some("An internal error occurred".to_string()),
                    instance: Some(reference.to_string()),
                }
            }
            AppError::UnsupportedMediaType(err) => {
                info!(%reference, "Unsupported media type: {}", err);
                Problem {
                    r#type: Default::default(),
                    title: Some(StatusCode::UNSUPPORTED_MEDIA_TYPE.to_string()),
                    status: StatusCode::UNSUPPORTED_MEDIA_TYPE,
                    detail: Some(err),
                    instance: Some(reference.to_string()),
                }
            }
        };

        let mut response = (problem.status, Json(problem)).into_response();
        if response.status() == StatusCode::UNAUTHORIZED {
            response.headers_mut().insert(
                header::WWW_AUTHENTICATE,
                HeaderValue::from_static(r#"Bearer realm="VTN""#),
            );
        }
        response
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use http_body_util::BodyExt;

    async fn problem_of(err: AppError) -> (Problem, axum::http::HeaderMap) {
        let response = err.into_response();
        let headers = response.headers().clone();
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let problem: Problem = serde_json::from_slice(&body)
            .unwrap_or_else(|e| panic!("{e}: {}", String::from_utf8_lossy(&body)));
        (problem, headers)
    }

    // Each arm of `into_response` writes `title` and `status` as two separate
    // literals, so this catches a copy-paste mismatch.
    fn assert_title_matches_status(problem: &Problem) {
        assert_eq!(problem.title, Some(problem.status.to_string()))
    }

    #[tokio::test]
    async fn wrapped_causes_do_not_reach_the_client() {
        for err in [
            #[cfg(feature = "sqlx")]
            AppError::Sql(sqlx::Error::Protocol("relation users_secret".into())), //passes
            AppError::Mqtt(paho_mqtt::Error::Failure), //passes
            AppError::PasswordHashError(password_hash::Error::Password),
        ] {
            let source = err.to_string();
            let debug = format!("{err:?}");
            let (problem, _) = problem_of(err).await;

            assert_title_matches_status(&problem);
            assert!(
                problem.status.is_server_error(),
                "expected 5xx for {debug}, got {}",
                problem.status
            );
            assert!(!problem.detail.unwrap_or_default().contains(&source))
        }
    }

    #[tokio::test]
    async fn not_found_omits_cause() {
        let (problem, _) = problem_of(AppError::NotFound).await;
        assert_eq!(problem.status, StatusCode::NOT_FOUND);
        assert_title_matches_status(&problem);
        assert!(problem.detail.is_none());
    }

    #[tokio::test]
    async fn unauthorized_carries_www_authenticate() {
        let (problem, headers) = problem_of(AppError::Auth("bad token".to_string())).await;
        assert_eq!(problem.status, StatusCode::UNAUTHORIZED);
        assert_eq!(headers[header::WWW_AUTHENTICATE], r#"Bearer realm="VTN""#)
    }

    #[tokio::test]
    async fn forbidden_omits_www_authenticate() {
        let (problem, headers) = problem_of(AppError::Forbidden("Forbidden")).await;
        assert_eq!(problem.status, StatusCode::FORBIDDEN);
        assert!(!headers.contains_key(header::WWW_AUTHENTICATE));
    }
}
