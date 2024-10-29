use argon2::password_hash;
use axum::{
    extract::rejection::{FormRejection, JsonRejection},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use axum_extra::extract::QueryRejection;
use openadr_wire::{problem::Problem, IdentifierError};
use serde::{Deserialize, Serialize};
#[cfg(feature = "sqlx")]
use sqlx::error::DatabaseError;
use tracing::{error, info, trace, warn};
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

impl From<password_hash::Error> for AppError {
    fn from(hash_err: password_hash::Error) -> Self {
        Self::PasswordHashError(hash_err)
    }
}

impl AppError {
    fn into_problem(self) -> Problem {
        let reference = Uuid::new_v4();

        match self {
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
                    title: Some(StatusCode::BAD_REQUEST.to_string()),
                    status: StatusCode::BAD_REQUEST,
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
        }
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let problem = self.into_problem();
        (problem.status, Json(problem)).into_response()
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct ProblemUri(String);

impl Default for ProblemUri {
    fn default() -> Self {
        Self("about:blank".to_string())
    }
}
