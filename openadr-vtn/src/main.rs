use base64::{
    alphabet,
    engine::{general_purpose::PAD, GeneralPurpose},
    Engine,
};
use std::env;
use tokio::{net::TcpListener, signal};
use tracing::{error, info, warn};
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

#[cfg(feature = "postgres")]
use openadr_vtn::data_source::PostgresStorage;
use openadr_vtn::{jwt::JwtManager, state::AppState};

#[tokio::main]
async fn main() {
    tracing_subscriber::registry()
        .with(fmt::layer().with_file(true).with_line_number(true))
        .with(EnvFilter::from_default_env())
        .init();

    let addr = "0.0.0.0:3000";
    let listener = TcpListener::bind(addr).await.unwrap();
    info!("listening on http://{}", listener.local_addr().unwrap());

    #[cfg(feature = "postgres")]
    let storage = PostgresStorage::from_env().await.unwrap();

    #[cfg(not(feature = "postgres"))]
    compile_error!(
        "No storage backend selected. Please enable the `postgres` feature flag during compilation"
    );

    let secret = env::var("OAUTH_BASE64_SECRET")
        .map(|base64_secret| {
            let secret = GeneralPurpose::new(&alphabet::STANDARD, PAD)
                .decode(base64_secret)
                .expect("OAUTH_BASE64_SECRET contains invalid base64 string");
            if secret.len() < 32 {
                // https://datatracker.ietf.org/doc/html/rfc7518#section-3.2
                panic!("OAUTH_BASE64_SECRET must have at least 32 bytes");
            }
            secret
        })
        .unwrap_or_else(|_| {
            warn!("Generating random secret as OAUTH_BASE64_SECRET env var was not found");
            let secret: [u8; 32] = rand::random();
            secret.to_vec()
        });
    let state = AppState::new(storage, JwtManager::from_secret(&secret));
    if let Err(e) = axum::serve(listener, state.into_router())
        .with_graceful_shutdown(shutdown_signal())
        .await
    {
        error!("webserver crashed: {}", e);
    }
}

async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}
