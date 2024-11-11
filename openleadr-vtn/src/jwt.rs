use std::sync::Arc;

use crate::{api::auth::ResponseOAuthError, error::AppError};
use axum::{
    async_trait,
    extract::{FromRef, FromRequestParts},
    http::request::Parts,
};
use axum_extra::{
    headers::{authorization::Bearer, Authorization},
    TypedHeader,
};
use jsonwebtoken::{encode, DecodingKey, EncodingKey, Header};
use openleadr_wire::{
    oauth::{OAuthError, OAuthErrorType},
    ven::VenId,
};
use tracing::trace;

pub struct JwtManager {
    encoding_key: Option<EncodingKey>,
    decoding_key: DecodingKey,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(test, derive(PartialOrd, Ord))]
#[serde(tag = "role", content = "id")]
pub enum AuthRole {
    UserManager,
    VenManager,
    Business(String),
    AnyBusiness,
    VEN(VenId),
}

impl AuthRole {
    pub fn is_business(&self) -> bool {
        matches!(self, AuthRole::Business(_) | AuthRole::AnyBusiness)
    }

    pub fn is_ven(&self) -> bool {
        matches!(self, AuthRole::VEN(_))
    }

    pub fn is_user_manager(&self) -> bool {
        matches!(self, AuthRole::UserManager)
    }

    pub fn is_ven_manager(&self) -> bool {
        matches!(self, AuthRole::VenManager)
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub(crate) struct Claims {
    exp: usize,
    nbf: usize,
    pub(crate) sub: String,
    pub(crate) roles: Vec<AuthRole>,
}

#[cfg(test)]
#[cfg(feature = "live-db-test")]
impl Claims {
    pub(crate) fn new(roles: Vec<AuthRole>) -> Self {
        Self {
            exp: 0,
            nbf: 0,
            sub: "".to_string(),
            roles,
        }
    }

    pub(crate) fn any_business_user() -> Claims {
        Claims::new(vec![AuthRole::AnyBusiness])
    }
}

#[derive(Debug)]
pub enum BusinessIds {
    Specific(Vec<String>),
    Any,
}

impl Claims {
    pub fn ven_ids(&self) -> Vec<VenId> {
        self.roles
            .iter()
            .filter_map(|role| {
                if let AuthRole::VEN(id) = role {
                    Some(id.clone())
                } else {
                    None
                }
            })
            .collect()
    }

    pub fn ven_ids_string(&self) -> Vec<String> {
        self.roles
            .iter()
            .filter_map(|role| {
                if let AuthRole::VEN(id) = role {
                    Some(id.to_string())
                } else {
                    None
                }
            })
            .collect()
    }

    pub fn business_ids(&self) -> BusinessIds {
        let mut ids = vec![];

        for role in &self.roles {
            match role {
                AuthRole::Business(id) => ids.push(id.clone()),
                AuthRole::AnyBusiness => return BusinessIds::Any,
                _ => {}
            }
        }

        BusinessIds::Specific(ids)
    }

    pub fn is_ven(&self) -> bool {
        self.roles.iter().any(AuthRole::is_ven)
    }

    pub fn is_business(&self) -> bool {
        self.roles.iter().any(AuthRole::is_business)
    }

    pub fn is_user_manager(&self) -> bool {
        self.roles.iter().any(AuthRole::is_user_manager)
    }

    pub fn is_ven_manager(&self) -> bool {
        self.roles.iter().any(AuthRole::is_ven_manager)
    }
}

impl JwtManager {
    /// Create a new JWT manager with a specific encoding and decoding key
    pub fn new(encoding_key: Option<EncodingKey>, decoding_key: DecodingKey) -> Self {
        Self {
            encoding_key,
            decoding_key,
        }
    }

    /// Create a new JWT token with the given claims and expiration time
    pub(crate) fn create(
        &self,
        expires_in: std::time::Duration,
        client_id: String,
        roles: Vec<AuthRole>,
    ) -> Result<String, ResponseOAuthError> {
        let now = chrono::Utc::now();
        let exp = now + expires_in;

        let claims = Claims {
            exp: exp.timestamp() as usize,
            nbf: now.timestamp() as usize,
            sub: client_id,
            roles,
        };

        if let Some(encoding_key) = &self.encoding_key {
            let token = encode(&Header::default(), &claims, encoding_key)?;
            Ok(token)
        } else {
            Err(OAuthError {
                error: OAuthErrorType::OAuthNotEnabled,
                error_description: None,
                error_uri: None,
            })?
        }
    }

    /// Decode and validate a given JWT token, returning the validated claims
    fn decode_and_validate(&self, token: &str) -> Result<Claims, jsonwebtoken::errors::Error> {
        let validation = jsonwebtoken::Validation::default();
        let token_data = jsonwebtoken::decode::<Claims>(token, &self.decoding_key, &validation)?;
        Ok(token_data.claims)
    }
}

/// User claims extracted from the request
pub struct User(pub(crate) Claims);

/// User claims extracted from the request, with the requirement that the user is a business user
pub struct BusinessUser(pub(crate) Claims);

/// User claims extracted from the request, with the requirement that the user is a VEN user
pub struct VENUser(pub(crate) Claims);

/// User claims extracted from the request, with the requirement that the user is a user manager
#[allow(dead_code)]
pub struct UserManagerUser(pub(crate) Claims);

/// User claims extracted from the request, with the requirement that the user is a VEN manager
pub struct VenManagerUser(pub(crate) Claims);

#[async_trait]
impl<S: Send + Sync> FromRequestParts<S> for User
where
    Arc<JwtManager>: FromRef<S>,
{
    type Rejection = AppError;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        let Ok(bearer) =
            TypedHeader::<Authorization<Bearer>>::from_request_parts(parts, state).await
        else {
            return Err(AppError::Auth(
                "Authorization via Bearer token in Authorization header required".to_string(),
            ));
        };

        let jwt_manager = Arc::<JwtManager>::from_ref(state);

        let Ok(claims) = jwt_manager.decode_and_validate(bearer.0.token()) else {
            return Err(AppError::Forbidden("Invalid authentication token provided"));
        };

        trace!(user = ?claims, "Extracted User from request");

        Ok(User(claims))
    }
}

#[async_trait]
impl<S: Send + Sync> FromRequestParts<S> for BusinessUser
where
    Arc<JwtManager>: FromRef<S>,
{
    type Rejection = AppError;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        let User(user) = User::from_request_parts(parts, state).await?;
        if !user.is_business() {
            return Err(AppError::Forbidden("User does not have the required role"));
        }
        Ok(BusinessUser(user))
    }
}

#[async_trait]
impl<S: Send + Sync> FromRequestParts<S> for VENUser
where
    Arc<JwtManager>: FromRef<S>,
{
    type Rejection = AppError;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        let User(user) = User::from_request_parts(parts, state).await?;
        if !user.is_ven() {
            return Err(AppError::Forbidden("User does not have the required role"));
        }
        Ok(VENUser(user))
    }
}

#[async_trait]
impl<S: Send + Sync> FromRequestParts<S> for UserManagerUser
where
    Arc<JwtManager>: FromRef<S>,
{
    type Rejection = AppError;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        let User(user) = User::from_request_parts(parts, state).await?;
        if !user.is_user_manager() {
            return Err(AppError::Forbidden("User does not have the required role"));
        }
        Ok(UserManagerUser(user))
    }
}

#[async_trait]
impl<S: Send + Sync> FromRequestParts<S> for VenManagerUser
where
    Arc<JwtManager>: FromRef<S>,
{
    type Rejection = AppError;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        let User(user) = User::from_request_parts(parts, state).await?;
        if !user.is_ven_manager() {
            return Err(AppError::Auth(
                "User does not have the required role".to_string(),
            ));
        }
        Ok(VenManagerUser(user))
    }
}
