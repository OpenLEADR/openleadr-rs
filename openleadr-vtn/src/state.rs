#[cfg(feature = "internal-oauth")]
use crate::api::auth;
#[cfg(feature = "internal-oauth")]
use crate::{api::user, data_source::AuthSource};
#[cfg(feature = "internal-oauth")]
use axum::routing::{delete, post};

use crate::{
    api::{event, healthcheck, program, report, resource, ven},
    data_source::{DataSource, EventCrud, ProgramCrud, ReportCrud, ResourceCrud, VenCrud},
    error::AppError,
    jwt::JwtManager,
};
use axum::{
    extract::{FromRef, Request},
    middleware,
    middleware::Next,
    response::IntoResponse,
    routing::get,
};
use base64::{
    alphabet,
    engine::{general_purpose::PAD, GeneralPurpose},
    Engine,
};
use jsonwebtoken::{Algorithm, DecodingKey, EncodingKey, Validation};
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use std::{
    cmp::PartialEq,
    env,
    env::VarError,
    io::{BufReader, Read},
    str::FromStr,
    sync::Arc,
};
use tower_http::trace::TraceLayer;
use tracing::{info, warn};

#[derive(Clone, FromRef)]
pub struct AppState {
    pub storage: Arc<dyn DataSource>,
    pub jwt_manager: Arc<JwtManager>,
}

#[derive(Debug, Default, Copy, Clone)]
pub(crate) enum OAuthType {
    #[default]
    Internal,
    External,
}

impl FromStr for OAuthType {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "internal" => Ok(Self::Internal),
            "external" => Ok(Self::External),
            _ => Err(()),
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub(crate) enum OAuthKeyType {
    Hmac,
    Rsa,
    Ec,
    #[serde(rename = "OKP")]
    Ed,
}

impl FromStr for OAuthKeyType {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "hmac" => Ok(Self::Hmac),
            "rsa" => Ok(Self::Rsa),
            "ec" => Ok(Self::Ec),
            "ed" => Ok(Self::Ed),
            _ => Err(()),
        }
    }
}

fn audiences_from_env() -> Result<Vec<String>, VarError> {
    env::var("OAUTH_VALID_AUDIENCES").map(|audience_str| {
        // Split the string by commas and collect into a vector
        audience_str.split(',').map(|s| s.to_string()).collect()
    })
}

pub(crate) fn hmac_from_env() -> Result<Vec<u8>, VarError> {
    env::var("OAUTH_BASE64_SECRET").map(|base64_secret| {
        let secret = GeneralPurpose::new(&alphabet::STANDARD, PAD)
            .decode(base64_secret)
            .expect("OAUTH_BASE64_SECRET contains invalid base64 string");
        if secret.len() < 32 {
            // https://datatracker.ietf.org/doc/html/rfc7518#section-3.2
            panic!("OAUTH_BASE64_SECRET must have at least 32 bytes");
        }
        secret
    })
}

fn signing_algorithms_from_key_type(key_type: &OAuthKeyType) -> Vec<Algorithm> {
    match key_type {
        OAuthKeyType::Hmac => {
            vec![Algorithm::HS256, Algorithm::HS384, Algorithm::HS512]
        }
        OAuthKeyType::Rsa => {
            vec![
                Algorithm::RS256,
                Algorithm::RS384,
                Algorithm::RS512,
                Algorithm::PS256,
                Algorithm::PS384,
                Algorithm::PS512,
            ]
        }
        OAuthKeyType::Ec => {
            vec![Algorithm::ES256, Algorithm::ES384]
        }
        OAuthKeyType::Ed => {
            vec![Algorithm::EdDSA]
        }
    }
}

fn internal_oauth_from_env(key_type: Option<OAuthKeyType>) -> JwtManager {
    if let Some(k_type) = key_type {
        if k_type != OAuthKeyType::Hmac {
            panic!("Internal OAuth provider only supports HMAC JWT keys");
        }
    }

    let secret = hmac_from_env().unwrap_or_else(|_| {
        warn!("Generating random secret as OAUTH_BASE64_SECRET env var was not found");
        let secret: [u8; 32] = rand::random();
        secret.to_vec()
    });

    let valid_audiences = audiences_from_env().unwrap_or_else(|_| {
        // audiences are optional since tokens provisioned from the internal oauth do
        // not currently include the `aud` claim in the token.
        info!("Default valid audiences to empty list as OAUTH_VALID_AUDIENCES env var was not set");
        Vec::<String>::new()
    });

    let mut validation = Validation::default();
    validation.algorithms =
        signing_algorithms_from_key_type(&key_type.unwrap_or(OAuthKeyType::Hmac));
    validation.set_audience(&valid_audiences);

    JwtManager::new(
        Some(EncodingKey::from_secret(&secret)),
        Some(DecodingKey::from_secret(&secret)),
        validation,
    )
}

async fn external_oauth_from_env(key_type: Option<OAuthKeyType>) -> JwtManager {
    let key_type = key_type.expect("Must specify key type for external OAuth provider. Use OAUTH_KEY_TYPE environment variable");

    let valid_audiences = audiences_from_env().expect(
        "OAUTH_VALID_AUDIENCES environment variable must be set for external Oauth provider",
    );

    let mut validation = Validation::default();
    validation.algorithms = signing_algorithms_from_key_type(&key_type);
    validation.set_audience(&valid_audiences);

    let oauth_jwks_location = env::var("OAUTH_JWKS_LOCATION");
    let oauth_keyfile = env::var("OAUTH_PEM");

    // Try to load decoding key from environment;
    //
    // for HMAC by loading OAUTH_BASE64_SECRET
    // for other key types, by looking at OAUTH_PEM
    let key = match key_type {
        OAuthKeyType::Hmac => {
            let secret = hmac_from_env().expect("OAUTH_BASE64_SECRET environment variable must be set for external OAuth provider with key type HMAC");
            Some(DecodingKey::from_secret(&secret))
        }

        OAuthKeyType::Rsa => match oauth_keyfile {
            Ok(rsa_file) => {
                let pem_bytes = BufReader::new(
                    std::fs::File::open(rsa_file)
                        .expect("File specified in OAUTH_PEM environment variable does not exist"),
                )
                .bytes()
                .collect::<Result<Vec<u8>, _>>()
                .expect("Cannot read RSA key");

                Some(DecodingKey::from_rsa_pem(&pem_bytes).expect("Cannot read RSA key"))
            }
            Err(_) => None,
        },

        OAuthKeyType::Ec => match oauth_keyfile {
            Ok(ec_file) => {
                let pem_bytes = BufReader::new(
                    std::fs::File::open(ec_file)
                        .expect("File specified in OAUTH_PEM environment variable does not exist"),
                )
                .bytes()
                .collect::<Result<Vec<u8>, _>>()
                .expect("Cannot read RSA key");

                Some(DecodingKey::from_ec_pem(&pem_bytes).expect("Cannot read EC key"))
            }
            Err(_) => None,
        },

        OAuthKeyType::Ed => match oauth_keyfile {
            Ok(ed_file) => {
                let pem_bytes = BufReader::new(
                    std::fs::File::open(ed_file)
                        .expect("File specified in OAUTH_PEM environment variable does not exist"),
                )
                .bytes()
                .collect::<Result<Vec<u8>, _>>()
                .expect("Cannot read RSA key");

                Some(DecodingKey::from_ed_pem(&pem_bytes).expect("Cannot read Ed key"))
            }
            Err(_) => None,
        },
    };

    // If no decoding key was found, then OAUTH_JWKS_LOCATION must be used
    if key.is_none() && oauth_jwks_location.is_err() {
        panic!("OAUTH_PEM or OAUTH_JWKS_LOCATION environment variable must be set for external OAuth provider with the given key type");
    }

    JwtManager::new(None, key, validation)
}

impl AppState {
    pub async fn new<S: DataSource>(storage: S) -> Self {
        let oauth_type: OAuthType = env::var("OAUTH_TYPE")
            .inspect_err(|_|{
            info!("Did not find OAUTH_TYPE environment variable, using internal OAuth provider.")}
            )
            .map(|env| env.parse()
                .expect("Invalid value for OAUTH_TYPE environment variable. Allowed are INTERNAL and EXTERNAL."))
            .unwrap_or_default();

        let key_type: Option<OAuthKeyType> = env::var("OAUTH_KEY_TYPE").ok().map(|k| k.parse().expect("Invalid value for OAUTH_KEY_TYPE environment variable. Allowed are HMAC, RSA, EC, and ED."));

        let jwt_manager = match oauth_type {
            OAuthType::Internal => internal_oauth_from_env(key_type),
            OAuthType::External => external_oauth_from_env(key_type).await,
        };

        Self {
            storage: Arc::new(storage),
            jwt_manager: Arc::new(jwt_manager),
        }
    }

    fn router_without_state() -> axum::Router<Self> {
        #[allow(unused_mut)]
        let mut router = axum::Router::new()
            .route("/health", get(healthcheck))
            .route("/programs", get(program::get_all).post(program::add))
            .route(
                "/programs/{id}",
                get(program::get).put(program::edit).delete(program::delete),
            )
            .route("/reports", get(report::get_all).post(report::add))
            .route(
                "/reports/{id}",
                get(report::get).put(report::edit).delete(report::delete),
            )
            .route("/events", get(event::get_all).post(event::add))
            .route(
                "/events/{id}",
                get(event::get).put(event::edit).delete(event::delete),
            )
            .route("/vens", get(ven::get_all).post(ven::add))
            .route(
                "/vens/{id}",
                get(ven::get).put(ven::edit).delete(ven::delete),
            )
            .route(
                "/vens/{ven_id}/resources",
                get(resource::get_all).post(resource::add),
            )
            .route(
                "/vens/{ven_id}/resources/{id}",
                get(resource::get)
                    .put(resource::edit)
                    .delete(resource::delete),
            );
        #[cfg(feature = "internal-oauth")]
        {
            router = router
                .route("/auth/token", post(auth::token))
                .route("/users", get(user::get_all).post(user::add_user))
                .route(
                    "/users/{id}",
                    get(user::get)
                        .put(user::edit)
                        .delete(user::delete_user)
                        .post(user::add_credential),
                )
                .route(
                    "/users/{user_id}/{client_id}",
                    delete(user::delete_credential),
                );
        }
        router
            .fallback(handler_404)
            .layer(middleware::from_fn(method_not_allowed))
            .layer(TraceLayer::new_for_http())
    }

    pub fn into_router(self) -> axum::Router {
        Self::router_without_state().with_state(self)
    }
}

async fn method_not_allowed(req: Request, next: Next) -> impl IntoResponse {
    let resp = next.run(req).await;
    let status = resp.status();
    match status {
        StatusCode::METHOD_NOT_ALLOWED => Err(AppError::MethodNotAllowed),
        _ => Ok(resp),
    }
}

async fn handler_404() -> AppError {
    AppError::NotFound
}

#[cfg(feature = "internal-oauth")]
impl FromRef<AppState> for Arc<dyn AuthSource> {
    fn from_ref(state: &AppState) -> Arc<dyn AuthSource> {
        state.storage.auth()
    }
}

impl FromRef<AppState> for Arc<dyn ProgramCrud> {
    fn from_ref(state: &AppState) -> Arc<dyn ProgramCrud> {
        state.storage.programs()
    }
}

impl FromRef<AppState> for Arc<dyn EventCrud> {
    fn from_ref(state: &AppState) -> Arc<dyn EventCrud> {
        state.storage.events()
    }
}

impl FromRef<AppState> for Arc<dyn ReportCrud> {
    fn from_ref(state: &AppState) -> Arc<dyn ReportCrud> {
        state.storage.reports()
    }
}

impl FromRef<AppState> for Arc<dyn VenCrud> {
    fn from_ref(state: &AppState) -> Arc<dyn VenCrud> {
        state.storage.vens()
    }
}

impl FromRef<AppState> for Arc<dyn ResourceCrud> {
    fn from_ref(state: &AppState) -> Arc<dyn ResourceCrud> {
        state.storage.resources()
    }
}

#[cfg(test)]
mod test {
    use super::*;

    struct MockDataSource {}
    impl DataSource for MockDataSource {
        fn programs(&self) -> Arc<dyn ProgramCrud> {
            unimplemented!()
        }

        fn reports(&self) -> Arc<dyn ReportCrud> {
            unimplemented!()
        }

        fn events(&self) -> Arc<dyn EventCrud> {
            unimplemented!()
        }

        fn vens(&self) -> Arc<dyn VenCrud> {
            unimplemented!()
        }

        fn resources(&self) -> Arc<dyn ResourceCrud> {
            unimplemented!()
        }

        #[cfg(feature = "internal-oauth")]
        fn auth(&self) -> Arc<dyn AuthSource> {
            unimplemented!()
        }

        fn connection_active(&self) -> bool {
            unimplemented!()
        }
    }

    mod state_from_env_var {
        use super::*;
        use serial_test::serial;

        fn clean_env() {
            env::remove_var("OAUTH_BASE64_SECRET");
            env::remove_var("OAUTH_TYPE");
            env::remove_var("OAUTH_KEY_TYPE");
            env::remove_var("OAUTH_PEM");
            env::remove_var("OAUTH_JWKS_LOCATION");
            env::remove_var("OAUTH_VALID_AUDIENCES");
        }

        #[tokio::test]
        #[should_panic(expected = "OAUTH_BASE64_SECRET must have at least 32 bytes")]
        #[serial]
        async fn internal_oauth_short_secret() {
            clean_env();
            env::set_var("OAUTH_BASE64_SECRET", "1234");
            AppState::new(MockDataSource {}).await;
        }

        #[tokio::test]
        #[should_panic(expected = "OAUTH_BASE64_SECRET contains invalid base64 string")]
        #[serial]
        async fn internal_oauth_invalid_base64_secret() {
            clean_env();
            env::set_var("OAUTH_BASE64_SECRET", "&");
            AppState::new(MockDataSource {}).await;
        }

        #[tokio::test]
        #[serial]
        async fn implicit_internal_oauth() {
            clean_env();
            env::set_var(
                "OAUTH_BASE64_SECRET",
                "60QL3fluRYn/21n0zNoPe1np5aB6P9C75b0Nbkwu4FM=",
            );
            AppState::new(MockDataSource {}).await;
        }

        #[tokio::test]
        #[serial]
        async fn explicit_internal_oauth() {
            clean_env();
            env::set_var("OAUTH_TYPE", "INTERNAL");
            env::set_var(
                "OAUTH_BASE64_SECRET",
                "60QL3fluRYn/21n0zNoPe1np5aB6P9C75b0Nbkwu4FM=",
            );
            AppState::new(MockDataSource {}).await;
        }

        #[tokio::test]
        #[serial]
        async fn explicit_internal_explicit_key_type_oauth() {
            clean_env();
            env::set_var("OAUTH_TYPE", "INTERNAL");
            env::set_var("OAUTH_KEY_TYPE", "HMAC");
            env::set_var(
                "OAUTH_BASE64_SECRET",
                "60QL3fluRYn/21n0zNoPe1np5aB6P9C75b0Nbkwu4FM=",
            );
            AppState::new(MockDataSource {}).await;
        }

        #[tokio::test]
        #[should_panic(expected = "Internal OAuth provider only supports HMAC JWT keys")]
        #[serial]
        async fn explicit_internal_explicit_wrong_key_type_oauth() {
            clean_env();
            env::set_var("OAUTH_TYPE", "INTERNAL");
            env::set_var("OAUTH_KEY_TYPE", "RSA");
            env::set_var(
                "OAUTH_BASE64_SECRET",
                "60QL3fluRYn/21n0zNoPe1np5aB6P9C75b0Nbkwu4FM=",
            );
            AppState::new(MockDataSource {}).await;
        }

        #[tokio::test]
        #[should_panic(
            expected = "Must specify key type for external OAuth provider. Use OAUTH_KEY_TYPE environment variable"
        )]
        #[serial]
        async fn external_missing_key_type_oauth() {
            clean_env();
            env::set_var("OAUTH_TYPE", "EXTERNAL");
            env::set_var("OAUTH_VALID_AUDIENCES", "http://localhost:3000,");
            env::set_var("OAUTH_PEM", "./tests/assets/public-rsa.pem");
            AppState::new(MockDataSource {}).await;
        }

        #[tokio::test]
        #[should_panic(
            expected = "OAUTH_PEM or OAUTH_JWKS_LOCATION environment variable must be set for external OAuth provider with the given key type"
        )]
        #[serial]
        async fn external_missing_jwks_location_oauth_and_oauth_pem() {
            clean_env();
            env::set_var("OAUTH_TYPE", "EXTERNAL");
            env::set_var("OAUTH_VALID_AUDIENCES", "http://localhost:3000,");
            env::set_var("OAUTH_KEY_TYPE", "RSA");
            AppState::new(MockDataSource {}).await;
        }

        #[tokio::test]
        #[should_panic(
            expected = "OAUTH_VALID_AUDIENCES environment variable must be set for external Oauth provider"
        )]
        #[serial]
        async fn external_missing_valid_audiences_oauth() {
            clean_env();
            env::set_var("OAUTH_TYPE", "EXTERNAL");
            env::set_var("OAUTH_KEY_TYPE", "RSA");
            env::set_var("OAUTH_JWKS_LOCATION", "http://localhost:3000/jwks");
            AppState::new(MockDataSource {}).await;
        }

        #[tokio::test]
        #[serial]
        async fn external_rsa() {
            clean_env();
            env::set_var("OAUTH_TYPE", "EXTERNAL");
            env::set_var("OAUTH_KEY_TYPE", "RSA");
            env::set_var("OAUTH_JWKS_LOCATION", "http://localhost:3000/jwks");
            env::set_var("OAUTH_VALID_AUDIENCES", "http://localhost:3000,");
            AppState::new(MockDataSource {}).await;
        }

        #[tokio::test]
        #[should_panic(expected = "Cannot read EC key: Error(InvalidKeyFormat)")]
        #[serial]
        async fn external_provide_rsa_key_instead_of_ec() {
            clean_env();
            env::set_var("OAUTH_TYPE", "EXTERNAL");
            env::set_var("OAUTH_KEY_TYPE", "EC");
            env::set_var("OAUTH_VALID_AUDIENCES", "http://localhost:3000,");
            env::set_var("OAUTH_PEM", "./tests/assets/public-rsa.pem");
            AppState::new(MockDataSource {}).await;
        }

        #[tokio::test]
        #[should_panic(expected = "Cannot read Ed key: Error(InvalidKeyFormat)")]
        #[serial]
        async fn external_provide_rsa_key_instead_of_ed() {
            clean_env();
            env::set_var("OAUTH_TYPE", "EXTERNAL");
            env::set_var("OAUTH_KEY_TYPE", "ED");
            env::set_var("OAUTH_VALID_AUDIENCES", "http://localhost:3000,");
            env::set_var("OAUTH_PEM", "./tests/assets/public-rsa.pem");
            AppState::new(MockDataSource {}).await;
        }
    }
}
