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
use jsonwebtoken::{DecodingKey, EncodingKey};
use reqwest::StatusCode;
use std::{cmp::PartialEq, env, env::VarError, fs::File, io::Read, str::FromStr, sync::Arc};
use tower_http::trace::TraceLayer;
use tracing::{info, warn};

#[derive(Clone, FromRef)]
pub struct AppState {
    pub storage: Arc<dyn DataSource>,
    pub jwt_manager: Arc<JwtManager>,
}

#[derive(Debug, Default, Copy, Clone)]
enum OAuthType {
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

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
enum OAuthKeyType {
    Hmac,
    Rsa,
    Ec,
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

fn hmac_from_env() -> Result<Vec<u8>, VarError> {
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
    JwtManager::new(
        Some(EncodingKey::from_secret(&secret)),
        DecodingKey::from_secret(&secret),
    )
}

fn external_oauth_from_env(key_type: Option<OAuthKeyType>) -> JwtManager {
    let key_type = key_type.expect("Must specify key type for external OAuth provider. Use OAUTH_KEY_TYPE environment variable");
    match key_type {
        OAuthKeyType::Hmac => {
            let secret = hmac_from_env().expect("OAUTH_BASE64_SECRET environment variable must be set for external OAuth provider with key type HMAC");
            JwtManager::new(None, DecodingKey::from_secret(&secret))
        }
        OAuthKeyType::Rsa => {
            let rsa_file = env::var("OAUTH_PEM").expect("OAUTH_PEM environment variable must be set for external OAuth provider with key type RSA");
            let pem_bytes = File::open(rsa_file)
                .expect("File specified in OAUTH_PEM environment variable does not exist")
                .bytes()
                .collect::<Result<Vec<u8>, _>>()
                .expect("Cannot read RSA key");
            JwtManager::new(
                None,
                DecodingKey::from_rsa_pem(&pem_bytes).expect("Cannot read RSA key"),
            )
        }
        OAuthKeyType::Ec => {
            let ec_file = env::var("OAUTH_PEM").expect("OAUTH_PEM environment variable must be set for external OAuth provider with key type EC");
            let pem_bytes = File::open(ec_file)
                .expect("File specified in OAUTH_PEM environment variable does not exist")
                .bytes()
                .collect::<Result<Vec<u8>, _>>()
                .expect("Cannot read EC key");
            JwtManager::new(
                None,
                DecodingKey::from_ec_pem(&pem_bytes).expect("Cannot read EC key"),
            )
        }
        OAuthKeyType::Ed => {
            let ed_file = env::var("OAUTH_PEM").expect("OAUTH_PEM environment variable must be set for external OAuth provider with key type ED");
            let pem_bytes = File::open(ed_file)
                .expect("File specified in OAUTH_PEM environment variable does not exist")
                .bytes()
                .collect::<Result<Vec<u8>, _>>()
                .expect("Cannot read Ed key");
            JwtManager::new(
                None,
                DecodingKey::from_ed_pem(&pem_bytes).expect("Cannot read Ed key"),
            )
        }
    }
}

impl AppState {
    pub fn new<S: DataSource>(storage: S) -> Self {
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
            OAuthType::External => external_oauth_from_env(key_type),
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
                "/programs/:id",
                get(program::get).put(program::edit).delete(program::delete),
            )
            .route("/reports", get(report::get_all).post(report::add))
            .route(
                "/reports/:id",
                get(report::get).put(report::edit).delete(report::delete),
            )
            .route("/events", get(event::get_all).post(event::add))
            .route(
                "/events/:id",
                get(event::get).put(event::edit).delete(event::delete),
            )
            .route("/vens", get(ven::get_all).post(ven::add))
            .route(
                "/vens/:id",
                get(ven::get).put(ven::edit).delete(ven::delete),
            )
            .route(
                "/vens/:ven_id/resources",
                get(resource::get_all).post(resource::add),
            )
            .route(
                "/vens/:ven_id/resources/:id",
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
                    "/users/:id",
                    get(user::get)
                        .put(user::edit)
                        .delete(user::delete_user)
                        .post(user::add_credential),
                )
                .route(
                    "/users/:user_id/:client_id",
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
        }

        #[test]
        #[should_panic(expected = "OAUTH_BASE64_SECRET must have at least 32 bytes")]
        #[serial]
        fn internal_oauth_short_secret() {
            clean_env();
            env::set_var("OAUTH_BASE64_SECRET", "1234");
            AppState::new(MockDataSource {});
        }

        #[test]
        #[should_panic(expected = "OAUTH_BASE64_SECRET contains invalid base64 string")]
        #[serial]
        fn internal_oauth_invalid_base64_secret() {
            clean_env();
            env::set_var("OAUTH_BASE64_SECRET", "&");
            AppState::new(MockDataSource {});
        }

        #[test]
        #[serial]
        fn implicit_internal_oauth() {
            clean_env();
            env::set_var(
                "OAUTH_BASE64_SECRET",
                "60QL3fluRYn/21n0zNoPe1np5aB6P9C75b0Nbkwu4FM=",
            );
            AppState::new(MockDataSource {});
        }

        #[test]
        #[serial]
        fn explicit_internal_oauth() {
            clean_env();
            env::set_var("OAUTH_TYPE", "INTERNAL");
            env::set_var(
                "OAUTH_BASE64_SECRET",
                "60QL3fluRYn/21n0zNoPe1np5aB6P9C75b0Nbkwu4FM=",
            );
            AppState::new(MockDataSource {});
        }

        #[test]
        #[serial]
        fn explicit_internal_explicit_key_type_oauth() {
            clean_env();
            env::set_var("OAUTH_TYPE", "INTERNAL");
            env::set_var("OAUTH_KEY_TYPE", "HMAC");
            env::set_var(
                "OAUTH_BASE64_SECRET",
                "60QL3fluRYn/21n0zNoPe1np5aB6P9C75b0Nbkwu4FM=",
            );
            AppState::new(MockDataSource {});
        }

        #[test]
        #[should_panic(expected = "Internal OAuth provider only supports HMAC JWT keys")]
        #[serial]
        fn explicit_internal_explicit_wrong_key_type_oauth() {
            clean_env();
            env::set_var("OAUTH_TYPE", "INTERNAL");
            env::set_var("OAUTH_KEY_TYPE", "RSA");
            env::set_var(
                "OAUTH_BASE64_SECRET",
                "60QL3fluRYn/21n0zNoPe1np5aB6P9C75b0Nbkwu4FM=",
            );
            AppState::new(MockDataSource {});
        }

        #[test]
        #[should_panic(
            expected = "Must specify key type for external OAuth provider. Use OAUTH_KEY_TYPE environment variable"
        )]
        #[serial]
        fn external_missing_key_type_oauth() {
            clean_env();
            env::set_var("OAUTH_TYPE", "EXTERNAL");
            env::set_var("OAUTH_PEM", "./key.pem");
            AppState::new(MockDataSource {});
        }

        #[test]
        #[should_panic(
            expected = "OAUTH_PEM environment variable must be set for external OAuth provider with key type RSA"
        )]
        #[serial]
        fn external_missing_key_oauth() {
            clean_env();
            env::set_var("OAUTH_TYPE", "EXTERNAL");
            env::set_var("OAUTH_KEY_TYPE", "RSA");
            AppState::new(MockDataSource {});
        }

        #[test]
        #[serial]
        fn external_rsa() {
            clean_env();
            env::set_var("OAUTH_TYPE", "EXTERNAL");
            env::set_var("OAUTH_KEY_TYPE", "RSA");
            env::set_var("OAUTH_PEM", "./tests/assets/public-rsa.pem");
            AppState::new(MockDataSource {});
        }

        #[test]
        #[should_panic(expected = "Cannot read EC key: Error(InvalidKeyFormat)")]
        #[serial]
        fn external_provide_rsa_key_instead_of_ec() {
            clean_env();
            env::set_var("OAUTH_TYPE", "EXTERNAL");
            env::set_var("OAUTH_KEY_TYPE", "EC");
            env::set_var("OAUTH_PEM", "./tests/assets/public-rsa.pem");
            AppState::new(MockDataSource {});
        }

        #[test]
        #[should_panic(expected = "Cannot read Ed key: Error(InvalidKeyFormat)")]
        #[serial]
        fn external_provide_rsa_key_instead_of_ed() {
            clean_env();
            env::set_var("OAUTH_TYPE", "EXTERNAL");
            env::set_var("OAUTH_KEY_TYPE", "ED");
            env::set_var("OAUTH_PEM", "./tests/assets/public-rsa.pem");
            AppState::new(MockDataSource {});
        }
    }
}
