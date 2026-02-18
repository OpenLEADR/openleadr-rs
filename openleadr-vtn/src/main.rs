use openleadr_vtn::{VtnConfig, VtnServer};
use tokio::signal;
use tracing::{error, info};
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

#[tokio::main]
async fn main() {
    tracing_subscriber::registry()
        .with(fmt::layer().with_file(true).with_line_number(true))
        .with(EnvFilter::from_default_env())
        .init();

    // Load config from environment
    let vtn_config = VtnConfig::from_env();
    let server = VtnServer::new(vtn_config).await.unwrap();

    info!(
        "VTN listening on http://{}",
        server.listener.local_addr().unwrap()
    );

    if let Err(e) = axum::serve(server.listener, server.router)
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
