use std::sync::Arc;

#[cfg(feature = "internal-oauth")]
use jsonwebtoken::{encode, Header};

use crate::api::auth::ResponseOAuthError;
use openleadr_wire::oauth::{OAuthError, OAuthErrorType};

use crate::{error::AppError, state::OAuthKeyType};
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
use tracing::{trace, warn};

use serde::{de::DeserializeOwned, Deserialize, Deserializer, Serialize};
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

mod opt_algorithm_def {
    use super::{Algorithm, AlgorithmDef};
    use serde::{Deserialize, Deserializer};

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<Algorithm>, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Helper(#[serde(with = "AlgorithmDef")] Algorithm);

        let helper = Option::deserialize(deserializer)?;
        Ok(helper.map(|Helper(external)| external))
    }
}

fn deserialize_vec_skipping_invalid<'de, D, T>(deserializer: D) -> Result<Vec<T>, D::Error>
where
    D: Deserializer<'de>,
    T: DeserializeOwned,
{
    // Deserialize as Vec<Value> first
    let raw_vec: Vec<serde_json::Value> = Vec::deserialize(deserializer)?;

    // Try to deserialize each element into T, skipping errors
    let mut result = Vec::new();
    for val in raw_vec {
        match serde_json::from_value(val) {
            Ok(item) => result.push(item),
            Err(err) => warn!("Ignoring invalid JWK: {err:?}"),
        }
    }
    Ok(result)
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(test, derive(PartialOrd, Ord))]
pub enum Scope {
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
    #[serde(untagged)]
    UnknownScope(String),
}

#[derive(Debug, PartialEq, Eq, Clone, Deserialize)]
struct RsaKey {
    kty: OAuthKeyType,
    #[serde(default, with = "opt_algorithm_def")]
    alg: Option<Algorithm>,
    n: String,
    e: String,
    kid: String,
}

#[derive(Debug, PartialEq, Eq, Clone, Deserialize)]
struct RsaKeys {
    #[serde(deserialize_with = "deserialize_vec_skipping_invalid")]
    keys: Vec<RsaKey>,
}

#[derive(Debug, PartialEq, Eq, Clone, Deserialize)]
struct EcKey {
    kty: OAuthKeyType,
    #[serde(default, with = "opt_algorithm_def")]
    alg: Option<Algorithm>,
    x: String,
    y: String,
    crv: String,
    kid: String,
}

#[derive(Debug, PartialEq, Eq, Clone, Deserialize)]
struct EcKeys {
    #[serde(deserialize_with = "deserialize_vec_skipping_invalid")]
    keys: Vec<EcKey>,
}

#[derive(Debug, PartialEq, Eq, Clone, Deserialize)]
struct EdKey {
    kty: OAuthKeyType,
    #[serde(default, with = "opt_algorithm_def")]
    alg: Option<Algorithm>,
    x: String,
    crv: String,
    kid: String,
}

#[derive(Debug, PartialEq, Eq, Clone, Deserialize)]
struct EdKeys {
    #[serde(deserialize_with = "deserialize_vec_skipping_invalid")]
    keys: Vec<EdKey>,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub(crate) struct Claims {
    exp: usize,
    nbf: usize,
    pub(crate) sub: String,
    pub(crate) roles: Vec<AuthRole>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct InitialClaims {
    exp: usize,
    nbf: usize,
    sub: String,
    #[serde(default)]
    // Allow the roles claim to either contain the internal roles structure of OpenLEADR
    // or the scopes structure of the typical scope claim.
    // or the internal roles of OpenLEADR.
    // This is needed since a major auth provider (Entra) does not support setting
    // scopes for machine-to-machine authentication. Only allowing the roles claim
    // to be set.
    roles: Option<RolesOrScopes>,
    #[serde(default)]
    scope: Option<Scopes>,
}

#[derive(Clone, Debug, serde::Serialize)]
struct Scopes {
    scopes: Vec<Scope>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(untagged)]
enum RolesOrScopes {
    AuthRoles(Vec<AuthRole>),
    Scopes(Vec<Scope>),
}

impl<'de> Deserialize<'de> for Scopes {
    fn deserialize<D>(deserializer: D) -> Result<Scopes, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s: &str = Deserialize::deserialize(deserializer)?;
        let parts = s.split(" ");

        let mut scopes: Vec<Scope> = Vec::new();
        for part in parts {
            match part {
                "read_all" => scopes.push(Scope::ReadAll),
                "write_vens" => scopes.push(Scope::WriteVens),
                "write_programs" => scopes.push(Scope::WritePrograms),
                "write_events" => scopes.push(Scope::WriteEvents),
                "write_reports" => scopes.push(Scope::WriteReports),
                _ => {
                    trace!("Unknown scope encountered: {:?}", part);
                }
            }
        }

        Ok(Scopes { scopes })
    }
}

impl InitialClaims {
    fn scopes_to_roles(scopes: &[Scope]) -> Vec<AuthRole> {
        let mut roles = Vec::new();

        if scopes.contains(&Scope::ReadAll) && scopes.contains(&Scope::WriteVens) {
            roles.push(AuthRole::VenManager);
        }

        if scopes.contains(&Scope::ReadAll) && scopes.contains(&Scope::WriteReports) {
            roles.push(AuthRole::VEN(VenId::new("anonymous").unwrap()));
        }

        if scopes.contains(&Scope::ReadAll)
            && scopes.contains(&Scope::WritePrograms)
            && scopes.contains(&Scope::WriteEvents)
        {
            roles.push(AuthRole::AnyBusiness);
        }

        roles
    }

    fn map_scope_roles_to_internal_roles(&self) -> Vec<AuthRole> {
        match &self.roles {
            Some(RolesOrScopes::Scopes(scope_list)) => Self::scopes_to_roles(scope_list),
            _ => vec![],
        }
    }

    fn map_scope_to_roles(&self) -> Vec<AuthRole> {
        match &self.scope {
            None => vec![],
            Some(s) => Self::scopes_to_roles(&s.scopes),
        }
    }
}

impl TryFrom<InitialClaims> for Claims {
    type Error = ResponseOAuthError;

    fn try_from(initial: InitialClaims) -> Result<Self, Self::Error> {
        match initial.roles {
            // when roles are empty, check scopes
            // and map these to our roles
            None => {
                if initial.scope.is_none() {
                    return Err(OAuthError::new(OAuthErrorType::InvalidGrant)
                        .with_description(
                            "Token must contain valid roles or a valid scope".to_string(),
                        )
                        .into());
                }

                Ok(Claims {
                    roles: initial.map_scope_to_roles(),
                    exp: initial.exp,
                    nbf: initial.nbf,
                    sub: initial.sub,
                })
            }

            // otherwise ignore scope and use the roles claim.
            // The roles claim can be one of two types:
            // 1. The internal auth roles from OpenLEADR
            // 2. The standard list of oadr scopes, similar to the scopes claim.
            // Reason 2 exists since Entra ID does not allow setting the scope claim
            // for machine to machine authentication, forcing the usage of the roles claim.
            // To ensure a wide range of compatibility with auth providers, both are supported here.
            Some(RolesOrScopes::AuthRoles(roles)) => Ok(Claims {
                roles,
                exp: initial.exp,
                nbf: initial.nbf,
                sub: initial.sub,
            }),

            Some(RolesOrScopes::Scopes(_)) => {
                let roles = initial.map_scope_roles_to_internal_roles();
                Ok(Claims {
                    roles,
                    exp: initial.exp,
                    nbf: initial.nbf,
                    sub: initial.sub,
                })
            }
        }
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
        match &self.decoding_key {
            Some(key) => {
                let token_data =
                    jsonwebtoken::decode::<InitialClaims>(token, key, &self.validation)?;
                token_data.claims.try_into()
            }

            None => {
                // Fetch server keys
                let keys = self.fetch_keys().await;

                if keys.is_empty() {
                    return Err(OAuthError::new(OAuthErrorType::NoAvailableKeys)
                        .with_description(
                            "No usable keys returned from the OAuth server".to_string(),
                        )
                        .into());
                }

                for decoding_key in keys {
                    // Go through the list of known decoding keys and try to validate and decode the token
                    let token_data = jsonwebtoken::decode::<InitialClaims>(
                        token,
                        &decoding_key,
                        &self.validation,
                    );

                    match token_data {
                        Result::Ok(data) => {
                            return data.claims.try_into();
                        }

                        // Otherwise ignore and try next key
                        Err(_) => {
                            trace!("Signature failed");
                        }
                    }
                }

                Err(OAuthError::new(OAuthErrorType::UnsupportedGrantType)
                    .with_description("No usable keys found".to_string())
                    .into())
            }
        }
    }

    /// Fetch OAUTH decoding keys from OAUTH_JWKS_LOCATION
    pub async fn fetch_keys(&self) -> Vec<DecodingKey> {
        let mut keys = Vec::new();
        let key_type: OAuthKeyType = env::var("OAUTH_KEY_TYPE").ok().map(|k| k.parse().expect("Invalid value for OAUTH_KEY_TYPE environment variable. Allowed are HMAC, RSA, EC, and ED.")).unwrap();

        match key_type {
            OAuthKeyType::Hmac => {}
            OAuthKeyType::Rsa => {
                let jwks_location = env::var("OAUTH_JWKS_LOCATION").expect("OAUTH_JWKS_LOCATION environment variable must be set for external OAuth provider with key type RSA");
                let rsa_params = reqwest::get(jwks_location)
                    .await
                    .expect("Could not reach OAUTH_JWKS_LOCATION");
                let rsa_keys: RsaKeys = rsa_params
                    .json()
                    .await
                    .expect("Could not parse RSA key from OAUTH_JWKS_LOCATION");

                for key in rsa_keys.keys {
                    let should_add = match &key.alg {
                        Some(alg) => self.validation.algorithms.contains(alg),
                        None => true, // allow if no alg specified inside the JWK. Optional in the JWK spec (not provided by Entra for example)
                    };

                    if key.kty == OAuthKeyType::Rsa && should_add {
                        keys.push(
                            DecodingKey::from_rsa_components(&key.n, &key.e)
                                .expect("Cannot read RSA key"),
                        );
                    }
                }
            }
            OAuthKeyType::Ec => {
                let jwks_location = env::var("OAUTH_JWKS_LOCATION").expect("OAUTH_JWKS_LOCATION environment variable must be set for external OAuth provider with key type EC");
                let ec_params = reqwest::get(jwks_location)
                    .await
                    .expect("Could not reach OAUTH_JWKS_LOCATION");
                let ec_keys: EcKeys = ec_params
                    .json()
                    .await
                    .expect("Could not parse EC key from OAUTH_JWKS_LOCATION");

                for key in ec_keys.keys {
                    let should_add = match &key.alg {
                        Some(alg) => self.validation.algorithms.contains(alg),
                        None => true, // allow if no alg specified inside the JWK. Optional in the JWK spec (not provided by Entra for example)
                    };

                    if key.kty == OAuthKeyType::Ec && should_add {
                        keys.push(
                            DecodingKey::from_ec_components(&key.x, &key.y)
                                .expect("Cannot read EC key"),
                        );
                    }
                }
            }
            OAuthKeyType::Ed => {
                let jwks_location = env::var("OAUTH_JWKS_LOCATION").expect("OAUTH_JWKS_LOCATION environment variable must be set for external OAuth provider with key type EC");
                let ed_params = reqwest::get(jwks_location)
                    .await
                    .expect("Could not reach OAUTH_JWKS_LOCATION");
                let ed_keys: EdKeys = ed_params
                    .json()
                    .await
                    .expect("Could not parse EC key from OAUTH_JWKS_LOCATION");

                for key in ed_keys.keys {
                    let should_add = match &key.alg {
                        Some(alg) => self.validation.algorithms.contains(alg),
                        None => true, // allow if no alg specified inside the JWK. Optional in the JWK spec (not provided by Entra for example)
                    };

                    if key.kty == OAuthKeyType::Ed && should_add {
                        keys.push(
                            DecodingKey::from_ed_components(&key.x).expect("Cannot read Ed key"),
                        );
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

#[cfg(test)]
mod tests {
    use crate::jwt::{AuthRole, Claims, InitialClaims, RolesOrScopes, Scope, Scopes, VenId};

    #[test]
    fn test_no_roles_no_scope_into_claims() {
        let initial = InitialClaims {
            exp: 10,
            nbf: 10,
            sub: "test".to_string(),
            roles: None,
            scope: None,
        };

        let claims: Result<Claims, _> = initial.try_into();
        assert!(claims.is_err());
    }

    #[test]
    fn test_initial_roles_into_claims() {
        let initial = InitialClaims {
            exp: 10,
            nbf: 10,
            sub: "test".to_string(),
            roles: Some(RolesOrScopes::AuthRoles(vec![
                AuthRole::AnyBusiness,
                AuthRole::VenManager,
            ])),
            scope: None,
        };

        let claims: Result<Claims, _> = initial.clone().try_into();
        assert!(claims.is_ok());

        let values = claims.unwrap();
        assert_eq!(values.exp, initial.exp);
        assert_eq!(values.nbf, initial.nbf);
        assert_eq!(values.sub, initial.sub);
        assert_eq!(
            values.roles,
            vec![AuthRole::AnyBusiness, AuthRole::VenManager]
        );
    }

    #[test]
    fn test_scope_ignored_if_roles_present() {
        let initial = InitialClaims {
            exp: 10,
            nbf: 10,
            sub: "test".to_string(),
            roles: Some(RolesOrScopes::AuthRoles(vec![AuthRole::AnyBusiness])),
            scope: Some(Scopes {
                scopes: vec![Scope::ReadAll, Scope::WriteVens],
            }),
        };

        let claims: Result<Claims, _> = initial.clone().try_into();
        assert!(claims.is_ok());

        let values = claims.unwrap();
        assert_eq!(values.roles, vec![AuthRole::AnyBusiness]);
    }

    #[test]
    fn test_scope_into_any_business_role() {
        let initial = InitialClaims {
            exp: 10,
            nbf: 10,
            sub: "test".to_string(),
            roles: None,
            scope: Some(Scopes {
                scopes: vec![Scope::ReadAll, Scope::WritePrograms, Scope::WriteEvents],
            }),
        };

        let claims: Result<Claims, _> = initial.clone().try_into();
        assert!(claims.is_ok());

        let values = claims.unwrap();
        assert_eq!(values.exp, initial.exp);
        assert_eq!(values.nbf, initial.nbf);
        assert_eq!(values.sub, initial.sub);
        assert_eq!(values.roles, vec![AuthRole::AnyBusiness]);
    }

    #[test]
    fn test_scope_into_ven_manager_role() {
        let initial = InitialClaims {
            exp: 10,
            nbf: 10,
            sub: "test".to_string(),
            roles: None,
            scope: Some(Scopes {
                scopes: vec![Scope::ReadAll, Scope::WriteVens],
            }),
        };

        let claims: Result<Claims, _> = initial.clone().try_into();
        assert!(claims.is_ok());

        let values = claims.unwrap();
        assert_eq!(values.exp, initial.exp);
        assert_eq!(values.nbf, initial.nbf);
        assert_eq!(values.sub, initial.sub);
        assert_eq!(values.roles, vec![AuthRole::VenManager]);
    }

    #[test]
    fn test_scope_into_anonymous_ven_role() {
        let initial = InitialClaims {
            exp: 10,
            nbf: 10,
            sub: "test".to_string(),
            roles: None,
            scope: Some(Scopes {
                scopes: vec![Scope::ReadAll, Scope::WriteReports],
            }),
        };

        let claims: Result<Claims, _> = initial.clone().try_into();
        assert!(claims.is_ok());

        let values = claims.unwrap();
        assert_eq!(values.exp, initial.exp);
        assert_eq!(values.nbf, initial.nbf);
        assert_eq!(values.sub, initial.sub);
        assert_eq!(
            values.roles,
            vec![AuthRole::VEN(VenId::new("anonymous").unwrap())]
        );
    }

    #[test]
    fn test_scope_into_multiple_roles() {
        let initial = InitialClaims {
            exp: 10,
            nbf: 10,
            sub: "test".to_string(),
            roles: None,
            scope: Some(Scopes {
                scopes: vec![
                    Scope::ReadAll,
                    Scope::WriteVens,
                    Scope::WritePrograms,
                    Scope::WriteEvents,
                ],
            }),
        };

        let claims: Result<Claims, _> = initial.clone().try_into();
        assert!(claims.is_ok());

        let values = claims.unwrap();
        assert_eq!(values.exp, initial.exp);
        assert_eq!(values.nbf, initial.nbf);
        assert_eq!(values.sub, initial.sub);
        assert_eq!(
            values.roles,
            vec![AuthRole::VenManager, AuthRole::AnyBusiness]
        );
    }

    #[test]
    fn test_oadr_roles_into_any_business_role() {
        let initial = InitialClaims {
            exp: 10,
            nbf: 10,
            sub: "test".to_string(),
            roles: Some(RolesOrScopes::Scopes(vec![
                Scope::ReadAll,
                Scope::WritePrograms,
                Scope::WriteEvents,
            ])),
            scope: None,
        };

        let claims: Result<Claims, _> = initial.clone().try_into();
        assert!(claims.is_ok());

        let values = claims.unwrap();
        assert_eq!(values.exp, initial.exp);
        assert_eq!(values.nbf, initial.nbf);
        assert_eq!(values.sub, initial.sub);
        assert_eq!(values.roles, vec![AuthRole::AnyBusiness]);
    }

    #[test]
    fn test_oadr_roles_into_ven_manager_role() {
        let initial = InitialClaims {
            exp: 10,
            nbf: 10,
            sub: "test".to_string(),
            roles: Some(RolesOrScopes::Scopes(vec![
                Scope::ReadAll,
                Scope::WriteVens,
            ])),
            scope: None,
        };

        let claims: Result<Claims, _> = initial.clone().try_into();
        assert!(claims.is_ok());

        let values = claims.unwrap();
        assert_eq!(values.exp, initial.exp);
        assert_eq!(values.nbf, initial.nbf);
        assert_eq!(values.sub, initial.sub);
        assert_eq!(values.roles, vec![AuthRole::VenManager]);
    }

    #[test]
    fn test_oadr_roles_into_anonymous_ven_role() {
        let initial = InitialClaims {
            exp: 10,
            nbf: 10,
            sub: "test".to_string(),
            roles: Some(RolesOrScopes::Scopes(vec![
                Scope::ReadAll,
                Scope::WriteReports,
            ])),
            scope: None,
        };

        let claims: Result<Claims, _> = initial.clone().try_into();
        assert!(claims.is_ok());

        let values = claims.unwrap();
        assert_eq!(values.exp, initial.exp);
        assert_eq!(values.nbf, initial.nbf);
        assert_eq!(values.sub, initial.sub);
        assert_eq!(
            values.roles,
            vec![AuthRole::VEN(VenId::new("anonymous").unwrap())]
        );
    }
}
