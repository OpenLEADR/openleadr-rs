use tokio::{net::TcpListener, signal};
use tracing::{error, info, warn};
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

#[cfg(feature = "postgres")]
use openleadr_vtn::data_source::PostgresStorage;
use openleadr_vtn::{data_source::Migrate, state::AppState};

use openleadr_vtn::mdns::register_mdns_vtn_service;

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

    if let Err(e) = storage.migrate().await {
        warn!("Database migration failed: {}", e);
    }

    let state = AppState::new(storage).await;
    let router = state.into_router();

    #[cfg(any(
        feature = "compression-br",
        feature = "compression-deflate",
        feature = "compression-gzip",
        feature = "compression-zstd"
    ))]
    let router = router.layer(tower_http::compression::CompressionLayer::new());

    // TODO: Make the mDNS service registration more robust and configurable (e.g., allow users to specify the service name, type, and metadata through configuration)
    let _mdns_handle = register_mdns_vtn_service(
        "vtn.local.".to_string(),
        "_openadr-http._tcp.local.".to_string(),
        "openleadr-vtn".to_string(), // If multiple VTNs are running on the same network, use a unique instance name
        listener.local_addr().unwrap().port(),
    ).await;

    if let Err(e) = axum::serve(listener, router)
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
