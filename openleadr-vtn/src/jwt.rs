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
use jsonwebtoken::{Algorithm, DecodingKey, EncodingKey, Validation};
use openleadr_wire::ven::VenId;
use tracing::trace;
use crate::state::OAuthKeyType;

use serde::{Serialize, Deserialize};
use std::env;

pub struct JwtManager {
    #[cfg(feature = "internal-oauth")]
    encoding_key: Option<EncodingKey>,
    decoding_key: Option<DecodingKey>,
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

#[derive(Serialize, Deserialize)]
#[serde(remote = "Algorithm")]
pub enum AlgorithmDef {
    HS256,
    HS384,
    HS512,
    ES256,
    ES384,
    RS256,
    RS384,
    RS512,
    PS256,
    PS384,
    PS512,
    EdDSA,
}

#[derive(Debug, PartialEq, Eq, Clone, Deserialize)]
struct RsaKey {
  kty: OAuthKeyType,
  #[serde(with = "AlgorithmDef")]
  alg: Algorithm,
  n: String,
  e: String,
  kid: String,
}

#[derive(Debug, PartialEq, Eq, Clone, Deserialize)]
struct RsaKeys {
  keys: Vec<RsaKey>,
}

#[derive(Debug, PartialEq, Eq, Clone, Deserialize)]
struct EcKey {
  kty: OAuthKeyType,
  #[serde(with = "AlgorithmDef")]
  alg: Algorithm,
  x: String,
  y: String,
  crv: String,
  kid: String,
}

#[derive(Debug, PartialEq, Eq, Clone, Deserialize)]
struct EcKeys {
  keys: Vec<EcKey>,
}

#[derive(Debug, PartialEq, Eq, Clone, Deserialize)]
struct EdKey {
  kty: OAuthKeyType,
  #[serde(with = "AlgorithmDef")]
  alg: Algorithm,
  x: String,
  crv: String,
  kid: String,
}

#[derive(Debug, PartialEq, Eq, Clone, Deserialize)]
struct EdKeys {
  keys: Vec<EdKey>,
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
    pub fn new(
        encoding_key: Option<EncodingKey>,
        decoding_key: Option<DecodingKey>,
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
    async fn decode_and_validate(&self, token: &str) -> Result<Claims, ResponseOAuthError> {

        // Fetch server keys
        let keys = self.fetch_keys().await;

        // Try multiple keys; if fail then try to fetch new keys
        if keys.len() < 1 {
            return Err(OAuthError::new(OAuthErrorType::NoAvailableKeys)
                .with_description("No usable keys returned from the OAuth server".to_string())
                .into());
        }

        let mut error;

        for decoding_key in keys {
            let token_data = jsonwebtoken::decode::<Claims>(token, &decoding_key, &self.validation);

            match token_data {
                Result::Ok(data) => return Ok(data.claims),

                // Ignore and try next key
                Err(err) => {
                    println!("ERROR --> {:?}", err);
                    error = err;
                }
            }
        }

        return Err(OAuthError::new(OAuthErrorType::UnsupportedGrantType)
            .with_description("No usabe".to_string())
            .into());
    }

    /// Fetch OAUTH decoding keys from OAUTH_JWKS_LOCATION
    pub async fn fetch_keys(&self) -> Vec<DecodingKey> {

        let mut keys = Vec::new();
        let key_type: OAuthKeyType = env::var("OAUTH_KEY_TYPE").ok().map(|k| k.parse().expect("Invalid value for OAUTH_KEY_TYPE environment variable. Allowed are HMAC, RSA, EC, and ED.")).unwrap();

        match key_type {
            OAuthKeyType::Hmac => {},
            OAuthKeyType::Rsa => {
                let jwks_location = env::var("OAUTH_JWKS_LOCATION").expect("OAUTH_JWKS_LOCATION environment variable must be set for external OAuth provider with key type RSA");
                let rsa_params = reqwest::get(jwks_location).await.expect("Could not reach OAUTH_JWKS_LOCATION");
                let rsa_keys: RsaKeys = rsa_params.json().await.expect("Could not parse RSA key from OAUTH_JWKS_LOCATION");

                for key in rsa_keys.keys {
                  if key.kty == OAuthKeyType::Rsa && self.validation.algorithms.contains(&key.alg) {
                    keys.push(DecodingKey::from_rsa_components(&key.n, &key.e).expect("Cannot read RSA key"));
                  }
                }

                if keys.len() < 1 {
                  panic!("No usuable keys found at OAUTH_JWKS_LOCATION");
                }
            }
            OAuthKeyType::Ec => {
                let jwks_location = env::var("OAUTH_JWKS_LOCATION").expect("OAUTH_JWKS_LOCATION environment variable must be set for external OAuth provider with key type EC");
                let ec_params = reqwest::get(jwks_location).await.expect("Could not reach OAUTH_JWKS_LOCATION");
                let ec_keys: EcKeys = ec_params.json().await.expect("Could not parse EC key from OAUTH_JWKS_LOCATION");

                let mut keys = Vec::new();
                for key in ec_keys.keys {
                  if key.kty == OAuthKeyType::Ec && self.validation.algorithms.contains(&key.alg) {
                    keys.push(DecodingKey::from_ec_components(&key.x, &key.y).expect("Cannot read EC key"));
                  }
                }
            }
            OAuthKeyType::Ed => {
                let jwks_location = env::var("OAUTH_JWKS_LOCATION").expect("OAUTH_JWKS_LOCATION environment variable must be set for external OAuth provider with key type EC");
                let ed_params = reqwest::get(jwks_location).await.expect("Could not reach OAUTH_JWKS_LOCATION");
                let ed_keys: EdKeys = ed_params.json().await.expect("Could not parse EC key from OAUTH_JWKS_LOCATION");

                let mut keys = Vec::new();
                for key in ed_keys.keys {
                    if key.kty == OAuthKeyType::Ed && self.validation.algorithms.contains(&key.alg) {
                        keys.push(DecodingKey::from_ed_components(&key.x).expect("Cannot read Ed key"));
                    }
                }
            }
        }

        keys
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

        let Ok(claims) = jwt_manager.decode_and_validate(bearer.token()).await else {
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
