use std::sync::Arc;

use crate::{api::ValidatedForm, data_source::AuthSource, jwt::JwtManager};
use axum::{
    extract::State,
    http::{Response, StatusCode},
    response::IntoResponse,
    Json,
};
use axum_extra::{
    headers::{authorization::Basic, Authorization},
    TypedHeader,
};
use openadr_wire::oauth::{OAuthError, OAuthErrorType};
use reqwest::header;
use serde::Deserialize;
use validator::Validate;

#[derive(Debug, Deserialize, Validate)]
pub struct AccessTokenRequest {
    grant_type: String,
    // TODO: handle scope
    // scope: Option<String>,
    client_id: Option<String>,
    client_secret: Option<String>,
}

pub struct ResponseOAuthError(pub OAuthError);

impl IntoResponse for ResponseOAuthError {
    fn into_response(self) -> Response<axum::body::Body> {
        match self.0.error {
            OAuthErrorType::InvalidClient => (
                StatusCode::UNAUTHORIZED,
                [(header::WWW_AUTHENTICATE, r#"Basic realm="VTN""#)],
                Json(self.0),
            )
                .into_response(),
            OAuthErrorType::ServerError => {
                (StatusCode::INTERNAL_SERVER_ERROR, Json(self.0)).into_response()
            }
            _ => (StatusCode::BAD_REQUEST, Json(self.0)).into_response(),
        }
    }
}

impl From<jsonwebtoken::errors::Error> for ResponseOAuthError {
    fn from(_: jsonwebtoken::errors::Error) -> Self {
        ResponseOAuthError(
            OAuthError::new(OAuthErrorType::ServerError)
                .with_description("Could not issue a new token".to_string()),
        )
    }
}

impl From<OAuthError> for ResponseOAuthError {
    fn from(err: OAuthError) -> Self {
        ResponseOAuthError(err)
    }
}

#[derive(Debug, serde::Serialize)]
pub struct AccessTokenResponse {
    access_token: String,
    token_type: &'static str,
    expires_in: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    scope: Option<String>,
}

impl IntoResponse for AccessTokenResponse {
    fn into_response(self) -> Response<axum::body::Body> {
        IntoResponse::into_response((StatusCode::OK, Json(self)))
    }
}

/// RFC 6749 client credentials grant flow
pub(crate) async fn token(
    State(auth_source): State<Arc<dyn AuthSource>>,
    State(jwt_manager): State<Arc<JwtManager>>,
    authorization: Option<TypedHeader<Authorization<Basic>>>,
    ValidatedForm(request): ValidatedForm<AccessTokenRequest>,
) -> Result<AccessTokenResponse, ResponseOAuthError> {
    if request.grant_type != "client_credentials" {
        return Err(OAuthError::new(OAuthErrorType::UnsupportedGrantType)
            .with_description("Only client_credentials grant type is supported".to_string())
            .into());
    }

    let auth_header = authorization
        .as_ref()
        .map(|TypedHeader(auth)| (auth.username(), auth.password()));

    let auth_body = request
        .client_id
        .as_ref()
        .map(|client_id| {
            (
                client_id.as_str(),
                request.client_secret.as_deref().unwrap_or(""),
            )
        })
        .or_else(|| request.client_secret.as_ref().map(|cr| ("", cr.as_str())));

    if auth_header.is_some() && auth_body.is_some() {
        return Err(OAuthError::new(OAuthErrorType::InvalidRequest)
            .with_description("Both header and body authentication provided".to_string())
            .into());
    }

    let Some((client_id, client_secret)) = auth_body.or(auth_header) else {
        return Err(OAuthError::new(OAuthErrorType::InvalidClient)
            .with_description(
                "No valid authentication data provided, client_id and client_secret required"
                    .to_string(),
            )
            .into());
    };

    // check that the client_id and client_secret are valid
    let Some(user) = auth_source
        .check_credentials(client_id, client_secret)
        .await
    else {
        return Err(OAuthError::new(OAuthErrorType::InvalidClient)
            .with_description("Invalid client_id or client_secret".to_string())
            .into());
    };

    let expiration = std::time::Duration::from_secs(3600 * 24 * 30);
    let token = jwt_manager.create(expiration, user.client_id, user.roles)?;

    Ok(AccessTokenResponse {
        access_token: token,
        token_type: "bearer",
        expires_in: expiration.as_secs(),
        scope: None,
    })
}
