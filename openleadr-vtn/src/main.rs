use tokio::signal;
use tracing::{error, info};
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};
use openleadr_vtn::create_vtn_server;

#[tokio::main]
async fn main() {
    tracing_subscriber::registry()
        .with(fmt::layer().with_file(true).with_line_number(true))
        .with(EnvFilter::from_default_env())
        .init();

    let server = create_vtn_server().await.unwrap();

    info!("VTN listening on http://{}", server.listener.local_addr().unwrap());

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
