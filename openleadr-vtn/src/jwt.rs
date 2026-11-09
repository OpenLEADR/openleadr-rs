use std::{
    sync::Arc,
    time::{Duration, Instant},
};

#[cfg(feature = "internal-oauth")]
use jsonwebtoken::{Header, encode};
use tokio::{
    sync::{Mutex, OwnedMutexGuard, RwLock},
    task::JoinHandle,
};

use crate::api::auth::ResponseOAuthError;
use openleadr_wire::{
    Ven,
    oauth::{OAuthError, OAuthErrorType},
};

use crate::{error::AppError, state::OAuthKeyType};
use axum::{
    extract::{FromRef, FromRequestParts},
    http::request::Parts,
};
use axum_extra::{
    TypedHeader,
    headers::{Authorization, authorization::Bearer},
};
#[cfg(feature = "internal-oauth")]
use jsonwebtoken::EncodingKey;
use jsonwebtoken::{
    Algorithm, DecodingKey, Validation,
    jwk::{AlgorithmParameters, Jwk},
};
use tracing::{trace, warn};

use derive_more::AsRef;
use openleadr_wire::ClientId;
use reqwest::Url;
use serde::{Deserialize, Deserializer, de, de::Visitor};
use std::{fmt, str::FromStr};

type JwkKeys = Vec<(Option<String>, DecodingKey)>;

/// Time the keys are used before they are refreshed
const DEFAULT_JWKS_CACHE_TTL: Duration = Duration::from_secs(300);
/// Minimum time between two fetches of the JWKS, to prevent a flood of requests to the OAuth server
const DEFAULT_JWKS_REFRESH_COOLDOWN: Duration = Duration::from_secs(10);
/// Keys that could not be refreshed for this long are no longer used, so revoked keys stop being
/// accepted even if the OAuth server cannot be reached
const JWKS_MAX_STALE: Duration = Duration::from_secs(3600);

fn duration_from_env(name: &str, default: Duration) -> Duration {
    match std::env::var(name) {
        Err(_) => default,
        Ok(value) => match value.trim().parse::<u64>() {
            Ok(secs) if secs >= 1 => Duration::from_secs(secs),
            _ => {
                warn!(
                    "Ignoring {name}={value:?}: it must be a whole number of seconds, at least 1. Using {default:?}"
                );
                default
            }
        },
    }
}

struct CachedKeys {
    keys: Arc<JwkKeys>,
    fetched_at: Instant,
}

#[derive(Deserialize)]
struct RawJwks {
    keys: Vec<serde_json::Value>,
}

/// Cache of the keys published at `OAUTH_JWKS_LOCATION`, so they are not fetched for every request
struct JwksCache {
    http: reqwest::Client,
    location: String,
    key_type: OAuthKeyType,
    algorithms: Vec<Algorithm>,
    ttl: Duration,
    refresh_cooldown: Duration,
    max_stale: Duration,
    cached: RwLock<Option<CachedKeys>>,
    /// Held while fetching, so only one fetch runs at a time. Contains the start of the last fetch.
    last_attempt: Arc<Mutex<Option<Instant>>>,
}

impl JwksCache {
    fn from_env(location: String, key_type: OAuthKeyType, algorithms: Vec<Algorithm>) -> Self {
        Self::new(
            location,
            key_type,
            algorithms,
            duration_from_env("OAUTH_JWKS_CACHE_TTL_SECONDS", DEFAULT_JWKS_CACHE_TTL),
            duration_from_env(
                "OAUTH_JWKS_REFRESH_COOLDOWN_SECONDS",
                DEFAULT_JWKS_REFRESH_COOLDOWN,
            ),
        )
    }

    fn new(
        location: String,
        key_type: OAuthKeyType,
        algorithms: Vec<Algorithm>,
        ttl: Duration,
        refresh_cooldown: Duration,
    ) -> Self {
        Self {
            http: reqwest::Client::builder()
                .connect_timeout(Duration::from_secs(2))
                .timeout(Duration::from_secs(5))
                .build()
                .expect("Could not build the HTTP client used to fetch OAUTH_JWKS_LOCATION"),
            location,
            key_type,
            algorithms,
            ttl,
            refresh_cooldown,
            max_stale: JWKS_MAX_STALE,
            cached: RwLock::new(None),
            last_attempt: Arc::new(Mutex::new(None)),
        }
    }

    /// The cached keys and when they were fetched, unless they are older than the max stale time
    async fn cached_keys(&self) -> Option<(Arc<JwkKeys>, Instant)> {
        self.cached
            .read()
            .await
            .as_ref()
            .filter(|c| c.fetched_at.elapsed() < self.max_stale)
            .map(|c| (c.keys.clone(), c.fetched_at))
    }

    fn unavailable() -> OAuthError {
        OAuthError::new(OAuthErrorType::NoAvailableKeys)
            .with_description("OAuth server keys are currently unavailable".to_string())
    }

    async fn cached_keys_or_unavailable(&self) -> Result<Arc<JwkKeys>, OAuthError> {
        self.cached_keys()
            .await
            .map(|(keys, _)| keys)
            .ok_or_else(Self::unavailable)
    }

    fn in_cooldown(&self, last_attempt: &Option<Instant>) -> bool {
        last_attempt.is_some_and(|t| t.elapsed() < self.refresh_cooldown)
    }

    /// Returns the keys to validate tokens with. Expired keys are returned right away and
    /// refreshed in the background, `force` waits for a refresh (limited by the cooldown).
    async fn keys(self: &Arc<Self>, force: bool) -> Result<Arc<JwkKeys>, OAuthError> {
        match self.cached_keys().await {
            Some((keys, fetched_at)) if !force => {
                if fetched_at.elapsed() >= self.ttl
                    && let Ok(last_attempt) = self.last_attempt.clone().try_lock_owned()
                    && !self.in_cooldown(&last_attempt)
                {
                    // Not awaited, the keys are used when the fetch is done
                    drop(self.spawn_fetch(last_attempt));
                }
                Ok(keys)
            }
            _ => {
                let last_attempt = self.last_attempt.clone().lock_owned().await;
                if self.in_cooldown(&last_attempt) {
                    return self.cached_keys_or_unavailable().await;
                }
                self.spawn_fetch(last_attempt)
                    .await
                    .unwrap_or_else(|_| Err(Self::unavailable()))
            }
        }
    }

    /// Fetch the keys in a task, so the fetch is not cancelled when the request is
    fn spawn_fetch(
        self: &Arc<Self>,
        mut last_attempt: OwnedMutexGuard<Option<Instant>>,
    ) -> JoinHandle<Result<Arc<JwkKeys>, OAuthError>> {
        *last_attempt = Some(Instant::now());
        let this = self.clone();
        tokio::spawn(async move {
            // Releases the lock when the task ends, also on a panic
            let _fetching = last_attempt;
            this.fetch_and_store().await
        })
    }

    /// Fetch the keys and replace the cached keys, or use the cached keys if that fails
    async fn fetch_and_store(&self) -> Result<Arc<JwkKeys>, OAuthError> {
        match self.fetch().await {
            Ok(keys) if !keys.is_empty() => {
                let keys = Arc::new(keys);
                *self.cached.write().await = Some(CachedKeys {
                    keys: keys.clone(),
                    fetched_at: Instant::now(),
                });
                Ok(keys)
            }
            Ok(_) => {
                warn!("OAUTH_JWKS_LOCATION returned no usable keys");
                self.cached_keys_or_unavailable().await
            }
            Err(err) => {
                warn!("Could not fetch keys from OAUTH_JWKS_LOCATION: {err}");
                self.cached_keys_or_unavailable().await
            }
        }
    }

    /// Fetch OAUTH decoding keys from OAUTH_JWKS_LOCATION
    async fn fetch(&self) -> Result<JwkKeys, String> {
        let RawJwks { keys } = self
            .http
            .get(&self.location)
            .send()
            .await
            .and_then(|r| r.error_for_status())
            .map_err(|err| format!("Could not reach OAUTH_JWKS_LOCATION: {err}"))?
            .json()
            .await
            .map_err(|err| format!("Could not parse keys from OAUTH_JWKS_LOCATION: {err}"))?;

        Ok(keys
            .into_iter()
            .filter_map(|key| self.parse_key(key))
            .collect())
    }

    /// Parse a key, if it is of the expected key type and algorithm
    fn parse_key(&self, key: serde_json::Value) -> Option<(Option<String>, DecodingKey)> {
        // allow if no alg specified inside the JWK. Optional in the JWK spec (not provided by Entra for example)
        if let Some(alg) = key.get("alg")
            && !serde_json::from_value::<Algorithm>(alg.clone())
                .is_ok_and(|alg| self.algorithms.contains(&alg))
        {
            return None;
        }

        let jwk: Jwk = serde_json::from_value(key)
            .inspect_err(|err| warn!("Ignoring invalid JWK: {err}"))
            .ok()?;

        if !matches!(
            (&self.key_type, &jwk.algorithm),
            (OAuthKeyType::Rsa, AlgorithmParameters::RSA(_))
                | (OAuthKeyType::Ec, AlgorithmParameters::EllipticCurve(_))
                | (OAuthKeyType::Ed, AlgorithmParameters::OctetKeyPair(_))
        ) {
            return None;
        }

        // Without a kid, a token naming a kid would never match and refresh the keys on every request
        let Some(kid) = jwk.common.key_id.clone() else {
            warn!("Ignoring JWK without kid");
            return None;
        };

        DecodingKey::from_jwk(&jwk)
            .inspect_err(|err| warn!("Ignoring unusable JWK kid={kid}: {err}"))
            .ok()
            .map(|decoding_key| (Some(kid), decoding_key))
    }
}

pub struct JwtManager {
    #[cfg(feature = "internal-oauth")]
    encoding_key: Option<EncodingKey>,
    decoding_key: Option<DecodingKey>,
    jwks_cache: Option<Arc<JwksCache>>,
    validation: Validation,
    token_url: Url,
}

/// Deserializes the JWT `aud` claim which can be either a single string or an array of strings.
/// Always returns `Option<Vec<String>>` internally.
mod string_or_vec {
    use serde::{Deserializer, de};
    use std::fmt;

    struct StringOrVecVisitor;

    impl<'de> de::Visitor<'de> for StringOrVecVisitor {
        type Value = Option<Vec<String>>;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str("a string or an array of strings")
        }

        fn visit_none<E>(self) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            Ok(None)
        }
        fn visit_unit<E>(self) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            Ok(None)
        }

        fn visit_str<E>(self, s: &str) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            Ok(Some(vec![s.to_owned()]))
        }

        fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
        where
            A: de::SeqAccess<'de>,
        {
            let mut result = Vec::new();
            while let Some(item) = seq.next_element::<String>()? {
                result.push(item);
            }
            Ok(Some(result))
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<Vec<String>>, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(StringOrVecVisitor)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize, sqlx::Type)]
#[sqlx(type_name = "scope", rename_all = "snake_case")]
#[serde(rename_all = "snake_case")]
#[cfg_attr(test, derive(PartialOrd, Ord))]
pub enum Scope {
    ReadAll,
    ReadTargets,
    ReadVenObjects,
    WritePrograms,
    WriteEvents,
    WriteReportsBl,
    #[serde(alias = "write_reports")]
    WriteReportsVen,
    WriteSubscriptionsBl,
    #[serde(alias = "write_subscriptions")]
    WriteSubscriptionsVen,
    WriteVensBl,
    #[serde(alias = "write_vens")]
    WriteVensVen,

    #[cfg(feature = "internal-oauth")]
    /// This scope is not standard, but used to represent full access to manage the users
    /// in the internal OAuth system. Should be used only for prototyping and testing purposes.
    WriteUsers,
}

impl FromStr for Scope {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "read_all" => Ok(Scope::ReadAll),
            "read_targets" => Ok(Scope::ReadTargets),
            "read_ven_objects" => Ok(Scope::ReadVenObjects),
            "write_programs" => Ok(Scope::WritePrograms),
            "write_events" => Ok(Scope::WriteEvents),
            "write_reports_bl" => Ok(Scope::WriteReportsBl),
            "write_reports_ven" => Ok(Scope::WriteReportsVen),
            "write_reports" => Ok(Scope::WriteReportsVen),
            "write_subscriptions_bl" => Ok(Scope::WriteSubscriptionsBl),
            "write_subscriptions_ven" => Ok(Scope::WriteSubscriptionsVen),
            "write_vens_bl" => Ok(Scope::WriteVensBl),
            "write_vens_ven" => Ok(Scope::WriteVensVen),
            "write_vens" => Ok(Scope::WriteVensVen),
            #[cfg(feature = "internal-oauth")]
            "write_users" => Ok(Scope::WriteUsers),
            _ => Err(format!("Invalid scope: {}", s)),
        }
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize, PartialEq)]
pub(crate) struct Claims {
    /// (subject): Subject of the JWT (the user)
    pub(crate) sub: String,
    /// (expiration time): Time after which the JWT expires
    exp: i64,
    /// (issued at time): Time at which the JWT was issued; can be used to determine age of the JWT
    iat: Option<i64>,
    /// (not before time): Time before which the JWT must not be accepted for processing
    nbf: Option<i64>,
    /// (audience): Recipient the JWT is intended for (the VTN).
    /// Can be a single string or an array of strings in the JWT, always deserialized as Vec.
    #[serde(default, deserialize_with = "string_or_vec::deserialize")]
    aud: Option<Vec<String>>,
    #[serde(default)]
    scope: Scopes,
    #[serde(default)]
    roles: Scopes,
}

impl Claims {
    pub fn client_id(&self) -> Result<ClientId, AppError> {
        self.sub.parse::<ClientId>().map_err(|err| {
            AppError::Auth(format!(
                "OAuth2 subject cannot be parsed as OpenADR clientId: {err}"
            ))
        })
    }

    /// Returns true if either the `scope` or `roles` claim contains the given scope.
    pub(crate) fn has_scope(&self, scope: Scope) -> bool {
        self.scope.contains(scope) || self.roles.contains(scope)
    }

    /// A read-only set of claims to use as the basis for mqtt object privacy.
    pub(crate) fn temporary_claims_for_mqtt_ven(ven: &Ven) -> Self {
        Self {
            sub: ven.content.client_id.as_str().into(),
            exp: 0,
            iat: None,
            nbf: None,
            aud: None,
            scope: vec![].into(),
            roles: vec![Scope::ReadVenObjects, Scope::ReadTargets].into(),
        }
    }

    #[cfg(test)]
    pub(crate) fn from_scopes(scopes: Vec<Scope>) -> Self {
        Self {
            sub: "test".into(),
            exp: 0,
            iat: None,
            nbf: None,
            aud: None,
            scope: vec![].into(),
            roles: scopes.into(),
        }
    }
}

#[derive(Clone, Debug, serde::Serialize, Default, derive_more::From, AsRef, PartialEq)]
#[serde(transparent)]
pub struct Scopes(Vec<Scope>);

impl Scopes {
    pub fn contains(&self, scope: Scope) -> bool {
        self.0.contains(&scope)
    }
}

struct ScopesVisitor;

impl<'de> Visitor<'de> for ScopesVisitor {
    type Value = Scopes;

    // A human-readable name for the type being deserialized.
    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a space-separated string of scopes or an array of strings")
    }

    // This method is called if the deserializer finds a string.
    fn visit_str<E>(self, s: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        let mut scopes = Vec::new();

        for part in s.split(' ') {
            match part.parse() {
                Ok(scope) => scopes.push(scope),
                Err(err) => warn!("Ignoring invalid scope: {err}"),
            }
        }

        Ok(Scopes(scopes))
    }

    // This method is called if the deserializer finds an array (JSON sequence).
    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: de::SeqAccess<'de>,
    {
        let mut scopes = Vec::new();
        // First parse as String and only then attempt to parse as Scope
        while let Some(scope_str) = seq.next_element::<String>()? {
            match scope_str.parse::<Scope>() {
                Ok(scope) => scopes.push(scope),
                Err(err) => warn!("Ignoring invalid scope: {err}"),
            }
        }
        Ok(Scopes(scopes))
    }
}

impl<'de> Deserialize<'de> for Scopes {
    fn deserialize<D>(deserializer: D) -> Result<Scopes, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(ScopesVisitor)
    }
}

impl JwtManager {
    /// Create a new JWT manager with a specific decoding key
    pub(crate) fn new(
        decoding_key: Option<DecodingKey>,
        jwks_location: Option<String>,
        key_type: OAuthKeyType,
        validation: Validation,
        token_url: Url,
    ) -> Self {
        // Keys are only fetched (and cached) when there is no static decoding key.
        let jwks_cache = match (&decoding_key, jwks_location) {
            (None, Some(location)) if key_type != OAuthKeyType::Hmac => Some(Arc::new(
                JwksCache::from_env(location, key_type, validation.algorithms.clone()),
            )),
            _ => None,
        };
        Self {
            #[cfg(feature = "internal-oauth")]
            encoding_key: None,
            decoding_key,
            jwks_cache,
            validation,
            token_url,
        }
    }

    /// Create a new JWT manager with a specific encoding and decoding key
    #[cfg(feature = "internal-oauth")]
    pub(crate) fn new_internal_auth(
        encoding_key: EncodingKey,
        decoding_key: DecodingKey,
        validation: Validation,
        token_url: Url,
    ) -> Self {
        Self {
            encoding_key: Some(encoding_key),
            decoding_key: Some(decoding_key),
            jwks_cache: None,
            validation,
            token_url,
        }
    }

    pub fn token_url(&self) -> &Url {
        &self.token_url
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

        let aud = if let Some(aud) = &self.validation.aud.as_ref() {
            let mut aud = aud.iter().cloned().collect::<Vec<_>>();
            aud.sort();
            Some(aud)
        } else {
            None
        };
        let claims = Claims {
            sub: client_id,
            exp: exp.timestamp(),
            iat: Some(now.timestamp()),
            nbf: Some(now.timestamp()),
            aud,
            scope: scope.into(),
            roles: Scopes::default(),
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
                let token_data = jsonwebtoken::decode::<Claims>(token, key, &self.validation)
                    .inspect_err(|err| warn!("received invalid authentication token: {err}"))?;
                Ok(Self::check_time(token_data.claims)?)
            }
            None => {
                let Some(cache) = &self.jwks_cache else {
                    return Err(OAuthError::new(OAuthErrorType::NoAvailableKeys)
                        .with_description(
                            "No key or OAUTH_JWKS_LOCATION configured to validate tokens with"
                                .to_string(),
                        )
                        .into());
                };
                // Get the server keys with kid references, from the cache if possible
                let mut keys = cache.keys(false).await?;

                // A token naming a key we do not know about means the OAuth server may have
                // rotated its keys: refresh once (rate limited) and look again.
                if let Some(kid) = jsonwebtoken::decode_header(token).ok().and_then(|h| h.kid)
                    && !keys
                        .iter()
                        .any(|(known, _)| known.as_deref() == Some(kid.as_str()))
                {
                    keys = cache.keys(true).await?;
                }

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

        if let Some(nbf) = claims.nbf
            && now < nbf
        {
            warn!("received token not yet valid: nbf={nbf} now={now}");
            return Err(OAuthError {
                error: OAuthErrorType::NotYetValid,
                error_description: Some(
                    "The 'nbf' claim disallows using this token already".to_string(),
                ),
                error_uri: None,
            });
        };

        if claims.exp < now {
            warn!("received expired token: exp={} now={now}", claims.exp);
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
            return Err(AppError::Auth(
                "Invalid authentication token provided".to_string(),
            ));
        };

        trace!(user = ?claims, "Extracted User from request");

        Ok(User(claims))
    }
}

#[cfg(test)]
mod test {
    use crate::{
        api::test::ApiTest,
        jwt::{Claims, Scope},
    };
    use axum::{body::Body, http::Method};
    use openleadr_wire::problem::Problem;
    use sqlx::PgPool;

    impl Scope {
        pub fn all() -> Vec<Scope> {
            vec![
                Scope::ReadAll,
                Scope::ReadTargets,
                Scope::ReadVenObjects,
                Scope::WritePrograms,
                Scope::WriteEvents,
                Scope::WriteReportsBl,
                Scope::WriteReportsVen,
                Scope::WriteSubscriptionsBl,
                Scope::WriteSubscriptionsVen,
                Scope::WriteVensBl,
                Scope::WriteVensVen,
                Scope::WriteUsers,
            ]
        }
    }

    #[ignore] // FIXME avoid OAUTH_BASE64_SECRET env mangling in tests
    #[sqlx::test]
    async fn sub_deserialization(db: PgPool) {
        let test = ApiTest::new(
            db,
            "This is not a valid client ID",
            vec![Scope::ReadVenObjects],
        )
        .await;
        let (status_code, problem) = test
            .request::<Problem>(Method::GET, "/vens", Body::empty())
            .await;
        assert_eq!(problem.detail, Some("OAuth2 subject cannot be parsed as OpenADR clientId: identifier contains characters besides [a-zA-Z0-9_-]: This is not a valid client ID".to_string()));
        assert_eq!(status_code, 401);
    }

    #[test]
    fn check_both_scopes_and_roles() {
        let claim = r#"{
            "sub": "test",
            "exp": 1,
            "scope": ["read_targets"],
            "roles": ["write_reports"]
        }"#;
        let claim = serde_json::from_str::<Claims>(claim).unwrap();
        assert!(claim.has_scope(Scope::ReadTargets));
        assert!(claim.has_scope(Scope::WriteReportsVen));
    }

    #[test]
    fn missing_quote_does_not_timeout() {
        let missing_quotes_around_scope = r#"{
            "sub": "test",
            "exp": 1,
            "scope": [unquoted_value],
        }"#;
        let claim = serde_json::from_str::<Claims>(missing_quotes_around_scope);

        // This is invalid JSON, but should not hang
        let claim_error = claim.unwrap_err();
        assert!(
            claim_error
                .to_string()
                .starts_with("expected value at line")
        );
    }

    #[test]
    fn invalid_scope_is_ignored() {
        let claim = r#"{
            "sub": "test",
            "exp": 1,
            "scope": ["bogus"]
        }"#;
        let claim = serde_json::from_str::<Claims>(claim).unwrap();
        assert_eq!(
            claim,
            Claims {
                sub: "test".to_string(),
                aud: None,
                exp: 1,
                iat: None,
                nbf: None,
                scope: crate::jwt::Scopes(Vec::with_capacity(0)),
                roles: crate::jwt::Scopes(Vec::with_capacity(0)),
            }
        );
    }

    #[test]
    fn claims_deserializes() {
        let claim_no_aud = r#"{ "sub": "test-no", "exp": 1}"#;
        let claim_single_aud_str =
            r#"{ "sub": "test-single-str", "aud": "single_audience_str", "exp": 2}"#;
        let claim_single_aud_vec =
            r#"{ "sub": "test-single-vec", "aud": ["single_audience_vec"], "exp": 3}"#;
        let claim_multiple_aud =
            r#"{ "sub": "test-multiple", "aud": ["audience_1", "audience_2"], "exp": 4}"#;

        let claim_no_aud = serde_json::from_str::<Claims>(claim_no_aud).unwrap();
        let claim_single_aud_vec = serde_json::from_str::<Claims>(claim_single_aud_vec).unwrap();
        let claim_single_aud_str = serde_json::from_str::<Claims>(claim_single_aud_str).unwrap();
        let claim_multiple_aud = serde_json::from_str::<Claims>(claim_multiple_aud).unwrap();

        assert_eq!(
            claim_no_aud,
            Claims {
                sub: "test-no".to_string(),
                aud: None,
                exp: 1,
                iat: None,
                nbf: None,
                scope: crate::jwt::Scopes(Vec::with_capacity(0)),
                roles: crate::jwt::Scopes(Vec::with_capacity(0)),
            }
        );
        assert_eq!(
            claim_single_aud_str,
            Claims {
                sub: "test-single-str".to_string(),
                aud: Some(vec!["single_audience_str".to_string()]),
                exp: 2,
                iat: None,
                nbf: None,
                scope: crate::jwt::Scopes(Vec::with_capacity(0)),
                roles: crate::jwt::Scopes(Vec::with_capacity(0)),
            }
        );
        assert_eq!(
            claim_single_aud_vec,
            Claims {
                sub: "test-single-vec".to_string(),
                aud: Some(vec!["single_audience_vec".to_string()]),
                exp: 3,
                iat: None,
                nbf: None,
                scope: crate::jwt::Scopes(Vec::with_capacity(0)),
                roles: crate::jwt::Scopes(Vec::with_capacity(0)),
            }
        );
        assert_eq!(
            claim_multiple_aud,
            Claims {
                sub: "test-multiple".to_string(),
                aud: Some(vec!["audience_1".to_string(), "audience_2".to_string(),]),
                exp: 4,
                iat: None,
                nbf: None,
                scope: crate::jwt::Scopes(Vec::with_capacity(0)),
                roles: crate::jwt::Scopes(Vec::with_capacity(0)),
            }
        );
    }
}

#[cfg(test)]
impl JwtManager {
    /// Override the JWKS cache timings (tests only).
    fn with_jwks_cache_settings(mut self, ttl: Duration, refresh_cooldown: Duration) -> Self {
        let old = self
            .jwks_cache
            .take()
            .expect("JWKS cache must be configured");
        self.jwks_cache = Some(Arc::new(JwksCache::new(
            old.location.clone(),
            old.key_type,
            old.algorithms.clone(),
            ttl,
            refresh_cooldown,
        )));
        self
    }
}

#[cfg(test)]
mod jwks_cache_test {
    // Tests for fetching and caching keys from OAUTH_JWKS_LOCATION, using a local JWKS server.
    // The RSA keys below are throw-away keys, generated for these tests only (not secrets).

    use super::*;
    use axum::{Router, extract::State, http::StatusCode, response::IntoResponse, routing::get};
    use std::sync::{
        Mutex as StdMutex,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    };

    const KEY_A_PEM: &str = r#"-----BEGIN PRIVATE KEY-----
MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQDzcASw2omXft8w
YuVpYDY11Eh67San7dkkE9pcrSsx0LvlNNa2uSkbKobnLVNoKUlQrE7WAjXN8T39
lyqYUI4/Glmffkv7bxylRDOawNIYhlo3ws6Ur55zm3dDG2qQ+UPjjrfsHDfc0fdR
6fO6BvDOlmSpxf4lkq2IacBXRClS7ga3fQmSjp7byovFmsSyaEfvZ3PocsPivzNz
Y56NzJmwXONC43XqkP72ECGnhh6PXssG1q8OCrNPe9foWr+rnKH+e48q+VYcrs8q
DepLNpI+i41zqX3V8wCMsQh/fNUvO4ycR6wg8r0tWIJ9/9smiFUV4NXVYbC7D+fI
+oiO2fohAgMBAAECggEACnQ6KVebLseuoTrv/+JXNaDSA4BlZ3L//wwAtbfkidk9
RmqqWVKyhJWDEVCpmyihur6JjKhv49ILL9WS6VNVRnm01VP0wp2LCQBYqHnFE/NK
EsixeyJfDzcjDB1Ualkf/JjThmbQUdHs+WYu9qNHalG8TSug3j9yJxBmh4DYdvSH
IX/Ys1nQaKcRQJUNhxwDanPx85gF9B3tVBmG0K+iNq9ZtT9M91J2KIKqKHWucHoN
y6/g3CFhgGRoMBcB69Dfslf26TeQiv3YP94HThU31GtcNcCHVNqEyvxx5a9OGdU9
ET/fNIVfy8Cz3XL3/xMmwpcj222WeKz1nihgj+i0yQKBgQD7i1spACVhHgHSmd2A
9fa8xig6adY2koc0Nk558f9JuPSrt7BJid8lsA/uairuBwwI9tFXYnv+M9Q0dH5s
CNZ3QNLSe0rIMnpJgjGHU1ttjDwAScZAgvYiuAV1WS2jD9bSjrdYi0v57dpC+3aN
+pbFrm0EoNMew5JXHiY0Dvc3OQKBgQD3v+bHjQgazcLpEj4Ij+zrWD3EOu1SUsmz
H8K6ahLLHF2kG9+d+kF9funR1MySLT+kiPYRabO9U5rCmIjVlywiPSlG1yN+9Tus
vRBSREASuwDHPmaI9UP2YOlNh403i4b7nVhvSshmqQr3X/xt/lMEjVAYyS6JB1AO
wIwEs0EyKQKBgQDQ9KWIfKeQcmXfNZPMcgZCMHXweETLVpNb6xTrtfx0mGEfWFrn
69DjtMq3KZh53H7B345tAdGwjAtme7MdOYY20DGRSXwIq+3rrGxKVxNnAAxDzQUU
R/YBR065eZgwkZLJnGZv9l7pwg0CgXOtY5HlqwzCwHpAXwPsvI3NxvTT+QKBgQCy
H7/+MxDjrJJ5XktJfqk5X/0wq6lbafhol+o6eu8+JwvQZNX20Cnr36ae3Z+jbXL3
ih1Y0aWP9p8rT935qzHpAA5AR4fWzNx1yo0EiNuEW/0oQ14XFDgj2POEcXP/vopz
N2KQybSh0uH2j4V/U74+ZJd06phRnvdZU+iXrIWwWQKBgHLoXYMnlK6mRJHhmR8P
miBq5kxcTzYaSGrQ97XdOpEMuhdpehzSBR7fK7KnmojZXM0kW5cnvWuzZnUA7z0+
EAjIVRHbWwpOfVYNaB709b11+T42U3tv4V1GRAqcevT+wRo8YNMsuzHxrlJ+3C/V
rouecEEcdM9ESGV3NT/7NdG4
-----END PRIVATE KEY-----
"#; // gitleaks:allow
    const KEY_A_N: &str = "83AEsNqJl37fMGLlaWA2NdRIeu0mp-3ZJBPaXK0rMdC75TTWtrkpGyqG5y1TaClJUKxO1gI1zfE9_ZcqmFCOPxpZn35L-28cpUQzmsDSGIZaN8LOlK-ec5t3QxtqkPlD44637Bw33NH3UenzugbwzpZkqcX-JZKtiGnAV0QpUu4Gt30Jko6e28qLxZrEsmhH72dz6HLD4r8zc2OejcyZsFzjQuN16pD-9hAhp4Yej17LBtavDgqzT3vX6Fq_q5yh_nuPKvlWHK7PKg3qSzaSPouNc6l91fMAjLEIf3zVLzuMnEesIPK9LViCff_bJohVFeDV1WGwuw_nyPqIjtn6IQ";
    const KEY_B_PEM: &str = r#"-----BEGIN PRIVATE KEY-----
MIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQCZVTGl02OARs2a
vwK+qBdM66g+PeRg/XPiE9XAbJIIoSeQaBwHEg5SoT6+KK9A4Z3zmInOFhtzz0ik
vcQvHu6pmE0gvKLTo0NDvZlpFhCohBHzi3Mkk6iPifc0enzM2MtiKNkG+uaf3VcS
+OXI5UvH4mPBtnapozLk2BPMSj+66kABDIIUajceGgiW09hUapmMiUH8ag/NuAPK
AYVR3pS4lJmoiBuCIs95T7zf/unv5oR3QzDlLSxlLl7KgE1Yuwflh4nxdsUlfrIt
Is/bh5AC0FOe8MRRzAJjWjV5ZJUrbq5YzZi65ovreg8JqTER8aKP+7AyG7e/rc0x
oQVotq2HAgMBAAECggEANWRFg/UeRiohaPuPNJ1qc6XxcS7zCdTnl7cJbNZBaZDY
KoAeFlYyt5IfO28GS4Tkj9CzV5g6GfL5XCgaGsBeIZ7/aGFXH3PPtdRdCYsd5NgM
Oi56OyfRf53ufw8RyvDIrVcE71nJmawkqF4UAYbagvExhGAJZNuHmskk57OJCogB
qfylmq4kyj9jjNVzKKWtkjqVRF1gJjiv1Ktx2L+rK5dNVlbBexylLToBdJL3LzC7
Iy/j72jbSm+qMZKRH6Tes2fcyRYsPW9r9vb0E44fhWAOb36guVP+U0aeIS0Ih5Bb
DPycEAOIpc6ac2mqYCxeRRo77hzfRDY5rkXWdJ+uiQKBgQDXSBxSbmZOK/q6g3GR
xbBMAVmyMFyOxkKxMhheLPO9wBHDHlLUPtPjlRX8ua9Lbr/6vSJHCEQo5jCVutMU
CeKroEnZLjBGxFYpGE/gJk5u06CmT0bRK6nPdfSl3lJn9x4x/cLyeGO9eEXl9Ous
fZw9wpK93GND7i0eLHZkmB2suQKBgQC2VYg9QJLpWdG/ATGlTyPzkLbq/og2Nx6m
4w6Tb1II7VgJYNF9pjGCoQWvld0QVlUbFFOSk/qUKl515OCLP1SsnE8DtvmuM1kP
WzTecA5zMBPDXutrzfJTEFdkyf0HQ5g2VyFdhWFaRGNm8UGrCVcLHtuqvk7ZAO6x
rFqBBJSMPwKBgDV84iSM73g+rxYGMIJyZqILt9hw8Z47njheZPWJH6omfYNIFnZL
gePKl+Lp06bfRm2q7khNwnmS78FT4Lva1OmlcpO2SSJcB7hAJSBhBB2/EOwhSEwS
GCfGe9l2Sm0wVd0/2l7aEJXzuxIubxu1PcuOe10W08b991HG5EeMD9tpAoGAe/vl
TGFghLTC33LcY2BRPgoll93EmbkZarny62ZDDjb2KkpfqWUdPYpo52CslbNe468K
tR1SwvWcjeY4mfdSiJ0xTYFafo4D/WEzrh3cdY83oR4mT4QXR0xRYQPIvuvD0Q+m
bIWKEWt3fESJNth/C9zL2mrWCtGLfd2JHToUGNkCgYBxmaWw0POs5dUoyd9JiviS
iQ2J0+C0y+fP+q7c8oni4DkXSTDCfMUbrPssmHGolQpCxYK4cPdZ4akaaYK5X8x9
YfKajyWaUpmoWvwPXWkQJkK5vQZ1GJ+S1hI5A2qfvCSz6xrkNIgSUwZ6VcPBGjvy
7xNr5qWWXsJmPF6tBlvn5Q==
-----END PRIVATE KEY-----
"#; // gitleaks:allow
    const KEY_B_N: &str = "mVUxpdNjgEbNmr8CvqgXTOuoPj3kYP1z4hPVwGySCKEnkGgcBxIOUqE-viivQOGd85iJzhYbc89IpL3ELx7uqZhNILyi06NDQ72ZaRYQqIQR84tzJJOoj4n3NHp8zNjLYijZBvrmn91XEvjlyOVLx-JjwbZ2qaMy5NgTzEo_uupAAQyCFGo3HhoIltPYVGqZjIlB_GoPzbgDygGFUd6UuJSZqIgbgiLPeU-83_7p7-aEd0Mw5S0sZS5eyoBNWLsH5YeJ8XbFJX6yLSLP24eQAtBTnvDEUcwCY1o1eWSVK26uWM2YuuaL63oPCakxEfGij_uwMhu3v63NMaEFaLathw";

    #[derive(Clone)]
    struct JwksServer {
        hits: Arc<AtomicUsize>,
        fail: Arc<AtomicBool>,
        delay_ms: Arc<AtomicU64>,
        body: Arc<StdMutex<String>>,
    }

    impl JwksServer {
        fn hits(&self) -> usize {
            self.hits.load(Ordering::SeqCst)
        }

        fn set_keys(&self, keys: &[(&str, &str)]) {
            let keys: Vec<_> = keys
                .iter()
                .map(|(kid, n)| {
                    format!(r#"{{"kty":"RSA","kid":"{kid}","alg":"RS256","use":"sig","n":"{n}","e":"AQAB"}}"#)
                })
                .collect();
            *self.body.lock().unwrap() = format!(r#"{{"keys":[{}]}}"#, keys.join(","));
        }

        async fn wait_for_hits(&self, hits: usize) {
            for _ in 0..100 {
                if self.hits() >= hits {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            panic!("expected {hits} JWKS fetches, got {}", self.hits());
        }
    }

    async fn jwks_handler(State(s): State<JwksServer>) -> axum::response::Response {
        s.hits.fetch_add(1, Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(s.delay_ms.load(Ordering::SeqCst))).await;
        if s.fail.load(Ordering::SeqCst) {
            return StatusCode::SERVICE_UNAVAILABLE.into_response();
        }
        (
            [("content-type", "application/json")],
            s.body.lock().unwrap().clone(),
        )
            .into_response()
    }

    async fn spawn_jwks_server() -> (String, JwksServer) {
        let server = JwksServer {
            hits: Arc::new(AtomicUsize::new(0)),
            fail: Arc::new(AtomicBool::new(false)),
            delay_ms: Arc::new(AtomicU64::new(0)),
            body: Arc::new(StdMutex::new(String::new())),
        };
        server.set_keys(&[("key-a", KEY_A_N)]);
        let app = Router::new()
            .route("/jwks", get(jwks_handler))
            .with_state(server.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        (format!("http://{addr}/jwks"), server)
    }

    fn validation() -> Validation {
        let mut validation = Validation::new(Algorithm::RS256);
        validation.validate_nbf = true;
        validation.validate_aud = false;
        validation
    }

    fn manager(url: String, ttl: Duration, cooldown: Duration) -> JwtManager {
        JwtManager::new(
            None,
            Some(url),
            OAuthKeyType::Rsa,
            validation(),
            "http://localhost/auth/token".parse().unwrap(),
        )
        .with_jwks_cache_settings(ttl, cooldown)
    }

    fn token(kid: &str, pem: &str) -> String {
        let now = chrono::Utc::now().timestamp();
        let claims = Claims {
            sub: "client-1".to_string(),
            exp: now + 3600,
            iat: Some(now),
            nbf: Some(now - 10),
            aud: None,
            scope: Scopes::default(),
            roles: Scopes::default(),
        };
        let mut header = jsonwebtoken::Header::new(Algorithm::RS256);
        header.kid = Some(kid.to_string());
        jsonwebtoken::encode(
            &header,
            &claims,
            &jsonwebtoken::EncodingKey::from_rsa_pem(pem.as_bytes()).unwrap(),
        )
        .unwrap()
    }

    const LONG: Duration = Duration::from_secs(3600);

    #[tokio::test]
    async fn keys_are_fetched_once_for_many_requests() {
        let (url, server) = spawn_jwks_server().await;
        let manager = manager(url, LONG, LONG);
        let token = token("key-a", KEY_A_PEM);
        for _ in 0..50 {
            assert!(manager.decode_and_validate(&token).await.is_ok());
        }
        assert_eq!(server.hits(), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_requests_share_one_fetch() {
        let (url, server) = spawn_jwks_server().await;
        let manager = Arc::new(manager(url, LONG, LONG));
        let token = Arc::new(token("key-a", KEY_A_PEM));
        let tasks: Vec<_> = (0..64)
            .map(|_| {
                let (manager, token) = (manager.clone(), token.clone());
                tokio::spawn(async move { manager.decode_and_validate(&token).await.is_ok() })
            })
            .collect();
        for task in tasks {
            assert!(task.await.unwrap());
        }
        assert_eq!(server.hits(), 1);
    }

    #[tokio::test]
    async fn expired_keys_are_used_while_refreshing_in_the_background() {
        let (url, server) = spawn_jwks_server().await;
        let manager = manager(url, Duration::from_millis(50), Duration::ZERO);
        let token = token("key-a", KEY_A_PEM);
        assert!(manager.decode_and_validate(&token).await.is_ok());

        // The keys expire and the OAuth server becomes slow: requests must not wait for it.
        tokio::time::sleep(Duration::from_millis(120)).await;
        server.delay_ms.store(800, Ordering::SeqCst);
        let started = Instant::now();
        assert!(manager.decode_and_validate(&token).await.is_ok());
        assert!(
            started.elapsed() < Duration::from_millis(300),
            "request waited for the slow OAuth server: {:?}",
            started.elapsed()
        );
        // ... while the refresh does happen in the background.
        server.wait_for_hits(2).await;
    }

    #[tokio::test]
    async fn unknown_kid_refreshes_once_and_is_rate_limited() {
        let (url, server) = spawn_jwks_server().await;
        let cooldown = Duration::from_millis(500);
        let manager = manager(url, LONG, cooldown);
        assert!(
            manager
                .decode_and_validate(&token("key-a", KEY_A_PEM))
                .await
                .is_ok()
        );
        assert_eq!(server.hits(), 1);

        // A token with an unknown kid right after a fetch is rate limited: no new fetch.
        server.set_keys(&[("key-a", KEY_A_N), ("key-b", KEY_B_N)]);
        assert!(
            manager
                .decode_and_validate(&token("key-b", KEY_B_PEM))
                .await
                .is_err()
        );
        assert_eq!(server.hits(), 1);

        // Once the cooldown has passed, the OAuth server's rotated key (key-b) is picked up.
        tokio::time::sleep(cooldown + Duration::from_millis(100)).await;
        assert!(
            manager
                .decode_and_validate(&token("key-b", KEY_B_PEM))
                .await
                .is_ok()
        );
        assert_eq!(server.hits(), 2);

        // Tokens with a kid nobody knows must not make us hammer the OAuth server either.
        for _ in 0..20 {
            let _ = manager
                .decode_and_validate(&token("unknown", KEY_A_PEM))
                .await;
        }
        assert_eq!(server.hits(), 2);
    }

    #[tokio::test]
    async fn previous_keys_are_used_when_a_refresh_fails() {
        let (url, server) = spawn_jwks_server().await;
        let manager = manager(url, Duration::from_millis(50), Duration::ZERO);
        let token = token("key-a", KEY_A_PEM);
        assert!(manager.decode_and_validate(&token).await.is_ok());

        server.fail.store(true, Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(120)).await;
        assert!(manager.decode_and_validate(&token).await.is_ok());
        server.wait_for_hits(2).await; // the refresh was attempted, and failed
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(manager.decode_and_validate(&token).await.is_ok());
    }

    #[tokio::test]
    async fn keys_that_could_not_be_refreshed_for_too_long_are_not_used() {
        let (url, server) = spawn_jwks_server().await;
        let mut manager = manager(url, Duration::from_millis(50), Duration::ZERO);
        Arc::get_mut(manager.jwks_cache.as_mut().unwrap())
            .unwrap()
            .max_stale = Duration::from_millis(300);
        let token = token("key-a", KEY_A_PEM);
        assert!(manager.decode_and_validate(&token).await.is_ok());

        // The OAuth server goes down: the last known keys are used, but not forever.
        server.fail.store(true, Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(120)).await;
        assert!(manager.decode_and_validate(&token).await.is_ok());
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(manager.decode_and_validate(&token).await.is_err());
    }

    #[tokio::test]
    async fn keys_removed_by_the_oauth_server_are_no_longer_accepted() {
        let (url, server) = spawn_jwks_server().await;
        let manager = manager(url, Duration::from_millis(50), Duration::ZERO);
        let token_a = token("key-a", KEY_A_PEM);
        assert!(manager.decode_and_validate(&token_a).await.is_ok());

        // The OAuth server revokes key-a and only publishes key-b.
        server.set_keys(&[("key-b", KEY_B_N)]);
        tokio::time::sleep(Duration::from_millis(120)).await;
        let _ = manager.decode_and_validate(&token_a).await; // triggers the background refresh
        server.wait_for_hits(2).await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(manager.decode_and_validate(&token_a).await.is_err());
        assert!(
            manager
                .decode_and_validate(&token("key-b", KEY_B_PEM))
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn a_cancelled_request_does_not_cancel_the_fetch() {
        let (url, server) = spawn_jwks_server().await;
        server.delay_ms.store(300, Ordering::SeqCst);
        let manager = Arc::new(manager(url, LONG, LONG));
        let token = token("key-a", KEY_A_PEM);

        // The client disconnects while the keys are being fetched
        let request = {
            let (manager, token) = (manager.clone(), token.clone());
            tokio::spawn(async move { manager.decode_and_validate(&token).await })
        };
        server.wait_for_hits(1).await;
        request.abort();

        tokio::time::sleep(Duration::from_millis(500)).await;
        assert!(manager.decode_and_validate(&token).await.is_ok());
        assert_eq!(server.hits(), 1);
    }

    #[tokio::test]
    async fn failing_oauth_server_without_cached_keys_is_an_error_not_a_panic() {
        let (url, server) = spawn_jwks_server().await;
        server.fail.store(true, Ordering::SeqCst);
        let manager = manager(url, LONG, LONG);
        assert!(
            manager
                .decode_and_validate(&token("key-a", KEY_A_PEM))
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn an_empty_key_set_is_not_cached() {
        let (url, server) = spawn_jwks_server().await;
        *server.body.lock().unwrap() = r#"{"keys":[]}"#.to_string();
        let cooldown = Duration::from_millis(200);
        let manager = manager(url, LONG, cooldown);
        let token = token("key-a", KEY_A_PEM);
        assert!(manager.decode_and_validate(&token).await.is_err());

        // The OAuth server recovers; after the cooldown we use its keys.
        server.set_keys(&[("key-a", KEY_A_N)]);
        tokio::time::sleep(cooldown + Duration::from_millis(100)).await;
        assert!(manager.decode_and_validate(&token).await.is_ok());
    }

    #[tokio::test]
    async fn a_static_decoding_key_does_not_use_the_jwks_cache() {
        let manager = JwtManager::new(
            Some(DecodingKey::from_secret(b"secret")),
            Some("http://localhost/jwks".to_string()),
            OAuthKeyType::Rsa,
            validation(),
            "http://localhost/auth/token".parse().unwrap(),
        );
        assert!(manager.jwks_cache.is_none());
    }
}
