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
use tracing::{trace, warn};

use derive_more::AsRef;
use openleadr_wire::ClientId;
use serde::{de::DeserializeOwned, Deserialize, Deserializer, Serialize};
use std::env;

pub struct JwtManager {
    #[cfg(feature = "internal-oauth")]
    encoding_key: Option<EncodingKey>,
    decoding_key: Option<DecodingKey>,
    validation: Validation,
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

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, sqlx::Type)]
#[sqlx(type_name = "scope", rename_all = "snake_case")]
#[serde(rename_all = "snake_case")]
#[cfg_attr(test, derive(PartialOrd, Ord))]
pub enum Scope {
    ReadAll,
    ReadTargets,
    ReadVenObjects,
    WritePrograms,
    WriteEvents,
    WriteReports,
    WriteSubscriptions,
    WriteVens,
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
    /// (subject): Subject of the JWT (the user)
    pub(crate) sub: String,
    /// (expiration time): Time after which the JWT expires
    exp: i64,
    /// (issued at time): Time at which the JWT was issued; can be used to determine age of the JWT
    iat: Option<i64>,
    /// (not before time): Time before which the JWT must not be accepted for processing
    nbf: Option<i64>,
    #[serde(default, alias = "roles")]
    pub(crate) scope: Scopes,
}

impl Claims {
    pub fn client_id(&self) -> Result<ClientId, AppError> {
        self.sub.parse::<ClientId>().map_err(|err| {
            AppError::Auth(format!(
                "OAuth2 subject cannot be parsed as OpenADR clientId: {err}"
            ))
        })
    }
}

#[derive(Clone, Debug, serde::Serialize, Default, derive_more::From, AsRef)]
#[serde(transparent)]
pub struct Scopes(Vec<Scope>);

impl Scopes {
    pub fn contains(&self, scope: Scope) -> bool {
        self.0.contains(&scope)
    }
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
                "read_targets" => scopes.push(Scope::ReadTargets),
                "read_ven_objects" => scopes.push(Scope::ReadVenObjects),
                "write_programs" => scopes.push(Scope::WritePrograms),
                "write_events" => scopes.push(Scope::WriteEvents),
                "write_reports" => scopes.push(Scope::WriteReports),
                "write_subscriptions" => scopes.push(Scope::WriteSubscriptions),
                "write_vens" => scopes.push(Scope::WriteVens),
                _ => {
                    trace!("Unknown scope encountered: {:?}", part);
                }
            }
        }

        Ok(Scopes(scopes))
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
        scope: impl Into<Scopes>,
    ) -> Result<String, ResponseOAuthError> {
        let now = chrono::Utc::now();
        let exp = now + expires_in;

        let claims = Claims {
            exp: exp.timestamp(),
            iat: Some(now.timestamp()),
            nbf: Some(now.timestamp()),
            sub: client_id,
            scope: scope.into(),
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
                let token_data = jsonwebtoken::decode::<Claims>(token, key, &self.validation)?;
                Ok(Self::check_time(token_data.claims)?)
            }
            None => {
                // Fetch server keys with kid references
                let keys = self.fetch_keys_with_kid().await;

                if keys.is_empty() {
                    return Err(OAuthError::new(OAuthErrorType::NoAvailableKeys)
                        .with_description(
                            "No usable keys returned from the OAuth server".to_string(),
                        )
                        .into());
                }

                for (kid, decoding_key) in keys.iter() {
                    let key_ref = kid.as_deref().unwrap_or("no_kid");

                    let token_data =
                        jsonwebtoken::decode::<Claims>(token, decoding_key, &self.validation);

                    match token_data {
                        Ok(data) => {
                            return Ok(Self::check_time(data.claims)?);
                        }
                        Err(e) => {
                            use jsonwebtoken::errors::ErrorKind;
                            match e.kind() {
                                ErrorKind::InvalidSignature => {
                                    warn!("JWT signature failed for kid={}: {e}", key_ref);
                                    // In case of invalid signature, try next key
                                }
                                _ => {
                                    tracing::error!(
                                        "JWT validation failed for kid={}: {e}",
                                        key_ref
                                    );
                                    // Stop trying, return error
                                    return Err(OAuthError::new(OAuthErrorType::InvalidGrant)
                                        .with_description(format!("JWT validation failed: {e}"))
                                        .into());
                                }
                            }
                        }
                    }
                }

                Err(OAuthError::new(OAuthErrorType::UnsupportedGrantType)
                    .with_description("No usable keys found".to_string())
                    .into())
            }
        }
    }

    fn check_time(claims: Claims) -> Result<Claims, OAuthError> {
        let now = chrono::Utc::now().timestamp();

        if let Some(nbf) = claims.nbf {
            if now < nbf {
                return Err(OAuthError {
                    error: OAuthErrorType::NotYetValid,
                    error_description: Some(
                        "The 'nbf' claim disallows using this token already".to_string(),
                    ),
                    error_uri: None,
                });
            }
        };

        if claims.exp < now {
            return Err(OAuthError {
                error: OAuthErrorType::Expired,
                error_description: Some(
                    "The 'exp' claim disallows using this token anymore".to_string(),
                ),
                error_uri: None,
            });
        }

        Ok(claims)
    }

    /// Fetch OAUTH decoding keys from OAUTH_JWKS_LOCATION
    pub async fn fetch_keys_with_kid(&self) -> Vec<(Option<String>, DecodingKey)> {
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
                        keys.push((
                            Some(key.kid.clone()),
                            DecodingKey::from_rsa_components(&key.n, &key.e)
                                .expect("Cannot read RSA key"),
                        ));
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
                        keys.push((
                            Some(key.kid.clone()),
                            DecodingKey::from_ec_components(&key.x, &key.y)
                                .expect("Cannot read EC key"),
                        ));
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
                        keys.push((
                            Some(key.kid.clone()),
                            DecodingKey::from_ed_components(&key.x).expect("Cannot read Ed key"),
                        ));
                    }
                }
            }
        }

        keys
    }
}

/// User claims extracted from the request
pub struct User(pub(crate) Claims);

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

#[cfg(test)]
mod tests {
    use crate::jwt::{Claims, Scope, Scopes};
    use std::fmt::Display;

    #[cfg(feature = "live-db-test")]
    impl Claims {
        pub(crate) fn new(sub: impl Display, scope: Vec<Scope>) -> Self {
            Self {
                exp: 0,
                iat: Some(0),
                nbf: Some(0),
                sub: sub.to_string(),
                scope: scope.into(),
            }
        }
    }

    #[test]
    fn test_no_scope_into_claims() {
        let initial = Claims {
            exp: 10,
            iat: None,
            nbf: Some(10),
            sub: "test".to_string(),
            scope: vec![].into(),
        };

        let claims: Result<Claims, _> = initial.try_into();
        assert!(claims.is_err());
    }

    #[test]
    fn test_initial_roles_into_claims() {
        let initial = InitialClaims {
            exp: 10,
            nbf: Some(10),
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
            nbf: Some(10),
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
            nbf: Some(10),
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
            nbf: Some(10),
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
            nbf: Some(10),
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
            nbf: Some(10),
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
            nbf: Some(10),
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
            nbf: Some(10),
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
            nbf: Some(10),
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
