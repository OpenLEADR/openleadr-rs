#[cfg(feature = "internal-oauth")]
use crate::{api::ValidatedForm, data_source::AuthSource, jwt::JwtManager};
#[cfg(feature = "internal-oauth")]
use axum::extract::State;
#[cfg(feature = "internal-oauth")]
use axum_extra::headers::{authorization::Basic, Authorization};
#[cfg(feature = "internal-oauth")]
use serde::Deserialize;
#[cfg(feature = "internal-oauth")]
use std::sync::Arc;
#[cfg(feature = "internal-oauth")]
use validator::Validate;

use crate::error::AppError;
use axum::{
    http::{header::AUTHORIZATION, HeaderMap, Response, StatusCode},
    response::IntoResponse,
    Json,
};
use axum_extra::headers::{authorization::Bearer, Header};
use openleadr_wire::oauth::{OAuthError, OAuthErrorType};
use reqwest::header;
use tracing::trace;

#[derive(Debug, Deserialize, Validate)]
#[cfg(feature = "internal-oauth")]
pub struct AccessTokenRequest {
    grant_type: String,
    // TODO: handle scope
    // scope: Option<String>,
    client_id: Option<String>,
    client_secret: Option<String>,
}

#[derive(Debug)]
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
            OAuthErrorType::OAuthNotEnabled => AppError::NotFound.into_response(),
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
#[cfg(feature = "internal-oauth")]
pub(crate) async fn token(
    State(auth_source): State<Arc<dyn AuthSource>>,
    State(jwt_manager): State<Arc<JwtManager>>,
    headers: HeaderMap,
    ValidatedForm(request): ValidatedForm<AccessTokenRequest>,
) -> Result<AccessTokenResponse, ResponseOAuthError> {
    if request.grant_type != "client_credentials" {
        return Err(OAuthError::new(OAuthErrorType::UnsupportedGrantType)
            .with_description("Only client_credentials grant type is supported".to_string())
            .into());
    }

    let mut auth_header = None;
    if let Some(header) = headers.get(AUTHORIZATION) {
        if let Ok(basic_auth) = Authorization::<Basic>::decode(&mut [header].into_iter()) {
            auth_header = Some((
                basic_auth.username().to_string(),
                basic_auth.password().to_string(),
            ))
        } else if Authorization::<Bearer>::decode(&mut [header].into_iter()).is_ok() {
            trace!("login request contained Bearer token which got ignored")
        }
    }

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

    let Some((client_id, client_secret)) =
        auth_body.or(auth_header.as_ref().map(|(a, b)| (a.as_str(), b.as_str())))
    else {
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
        token_type: "Bearer",
        expires_in: expiration.as_secs(),
        scope: None,
    })
}
