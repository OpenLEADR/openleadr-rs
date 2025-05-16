use std::sync::Arc;

#[cfg(feature = "internal-oauth")]
use crate::api::auth::ResponseOAuthError;
#[cfg(feature = "internal-oauth")]
use jsonwebtoken::{encode, Header};
#[cfg(feature = "internal-oauth")]
use openleadr_wire::oauth::{OAuthError, OAuthErrorType};

use crate::error::AppError;
use axum::{
    extract::{FromRef, FromRequestParts},
    http::request::Parts,
};
use axum_extra::{
    headers::{authorization::Bearer, Authorization},
    TypedHeader,
};
use jsonwebtoken::{DecodingKey, EncodingKey, Validation};
use openleadr_wire::ven::VenId;
use tracing::trace;

pub struct JwtManager {
    #[cfg(feature = "internal-oauth")]
    encoding_key: Option<EncodingKey>,
    decoding_key: DecodingKey,
    validation: Validation,
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

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(test, derive(PartialOrd, Ord))]
#[serde(tag = "role", content = "id")]
enum InitialRole {
    UserManager,
    VenManager,
    Business(String),
    AnyBusiness,
    VEN(VenId),
    #[serde(rename = "read_all")]
    ReadAll,
    #[serde(rename = "write_programs")]
    WritePrograms,
    #[serde(rename = "write_reports")]
    WriteReports,
    #[serde(rename = "write_events")]
    WriteEvents,
    #[serde(rename = "write_vens")]
    WriteVens,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub(crate) struct Claims {
    exp: usize,
    nbf: usize,
    pub(crate) sub: String,
    pub(crate) roles: Vec<AuthRole>,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct InitialClaims {
    exp: usize,
    nbf: usize,
    sub: String,
    roles: Vec<InitialRole>,
}

impl InitialClaims {
    /// The initial claims can contain alternative roles like ReadAll, WritePrograms, etc.
    /// these are mapped to our internal AuthRoles here.
    fn map_initial_roles(&self) -> Vec<AuthRole> {
        let i = &self.roles;
        let mut roles = Vec::new();

        // If read_all && write_vens -> VenManager
        if i.iter().any(|r| r == &InitialRole::ReadAll)
            && i.iter().any(|r| r == &InitialRole::WriteVens)
        {
            roles.push(AuthRole::VenManager);
        }

        // If read_all && write_reports -> Ven("anonymous")
        if i.iter().any(|r| r == &InitialRole::ReadAll)
            && i.iter().any(|r| r == &InitialRole::WriteReports)
        {
            roles.push(AuthRole::VEN(VenId::new("anonymous").unwrap()));
        }

        // If read_all && write_programs && write_events -> AnyBusiness
        if i.iter().any(|r| r == &InitialRole::ReadAll)
            && i.iter().any(|r| r == &InitialRole::WritePrograms)
            && i.iter().any(|r| r == &InitialRole::WriteEvents)
        {
            roles.push(AuthRole::AnyBusiness);
        }

        for role in i {
            match role {
                // Keep UserManager
                InitialRole::UserManager => roles.push(AuthRole::UserManager),

                // Keep VenManager, if not given already
                InitialRole::VenManager => {
                    if !roles.iter().any(|r| r == &AuthRole::VenManager) {
                        roles.push(AuthRole::VenManager);
                    }
                }

                // Keep AnyBusiness, if not given already
                InitialRole::AnyBusiness => {
                    if !roles.iter().any(|r| r == &AuthRole::AnyBusiness) {
                        roles.push(AuthRole::AnyBusiness);
                    }
                }

                // Keep Business
                InitialRole::Business(x) => roles.push(AuthRole::Business(x.to_string())),

                // Keep VEN
                InitialRole::VEN(x) => roles.push(AuthRole::VEN(x.clone())),

                // Other roles should already be processed
                _ => {}
            }
        }

        roles
    }
}

impl TryFrom<InitialClaims> for Claims {
    type Error = jsonwebtoken::errors::Error;

    fn try_from(initial: InitialClaims) -> Result<Self, Self::Error> {
        Ok(Claims {
            roles: initial.map_initial_roles(),
            exp: initial.exp,
            nbf: initial.nbf,
            sub: initial.sub,
        })
    }
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
    pub fn new(
        encoding_key: Option<EncodingKey>,
        decoding_key: DecodingKey,
        validation: Validation,
    ) -> Self {
        if !cfg!(feature = "internal-oauth") && encoding_key.is_some() {
            panic!("You should not provide a JWT encoding key as the 'internal-oauth' feature is disabled. \
            Please recompile with the 'internal-oauth' feature enabled if you want to use it.");
        }
        #[cfg(feature = "internal-oauth")]
        {
            Self {
                encoding_key,
                decoding_key,
                validation,
            }
        }
        #[cfg(not(feature = "internal-oauth"))]
        {
            Self {
                decoding_key,
                validation,
            }
        }
    }

    /// Create a new JWT token with the given claims and expiration time
    #[cfg(feature = "internal-oauth")]
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
    /// Note that token roles are remapped into our internal roles
    fn decode_and_validate(&self, token: &str) -> Result<Claims, jsonwebtoken::errors::Error> {
        let initial_token =
            jsonwebtoken::decode::<InitialClaims>(token, &self.decoding_key, &self.validation)?;
        initial_token.claims.try_into()
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

impl<S: Send + Sync> FromRequestParts<S> for User
where
    Arc<JwtManager>: FromRef<S>,
{
    type Rejection = AppError;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        let Ok(TypedHeader(bearer)) =
            TypedHeader::<Authorization<Bearer>>::from_request_parts(parts, state).await
        else {
            return Err(AppError::Auth(
                "Authorization via Bearer token in Authorization header required".to_string(),
            ));
        };

        let jwt_manager = Arc::<JwtManager>::from_ref(state);

        let Ok(claims) = jwt_manager.decode_and_validate(bearer.token()) else {
            return Err(AppError::Forbidden("Invalid authentication token provided"));
        };

        trace!(user = ?claims, "Extracted User from request");

        Ok(User(claims))
    }
}

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
