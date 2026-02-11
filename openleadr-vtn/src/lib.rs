mod api;
pub mod data_source;
mod error;
pub mod jwt;
pub mod state;
pub mod mdns;

#[cfg(feature = "postgres")]
use crate::data_source::PostgresStorage;
use crate::{data_source::Migrate, state::AppState};

use tracing::{error, info, warn};
use mdns_sd::ServiceDaemon;
use crate::mdns::register_mdns_vtn_service;
use tokio::net::TcpListener;

pub struct VtnServer {
    pub mdns_handle: ServiceDaemon,
    pub router: axum::Router,
    pub listener: TcpListener,
}

pub async fn create_vtn_server() -> Result<VtnServer, Box<dyn std::error::Error>> {
    let port = std::env::var("PORT")
        .unwrap_or_else(|_| "3000".to_string());
    
    let mdns_ip = std::env::var("MDNS_IP_ADDRESS")
        .unwrap_or_else(|_| "127.0.0.1".to_string());
    let mdns_host_name = std::env::var("MDNS_HOST_NAME")
        .unwrap_or_else(|_| "vtn.local.".to_string());
    let mdns_service_type = std::env::var("MDNS_SERVICE_TYPE")
        .unwrap_or_else(|_| "_openadr-http._tcp.local.".to_string());
    let mdns_server_name = std::env::var("MDNS_SERVER_NAME")
        .unwrap_or_else(|_| "openleadr-vtn".to_string());

    let addr = format!("0.0.0.0:{}", port);
    let listener = TcpListener::bind(addr).await.unwrap();
    info!("listening on http://{}", listener.local_addr().unwrap());

    #[cfg(feature = "postgres")]
    let storage = PostgresStorage::from_env().await?;

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

    let mdns_handle = register_mdns_vtn_service(
        mdns_host_name,
        mdns_service_type,
        mdns_server_name,
        mdns_ip,
        listener.local_addr().unwrap().port(),
    ).await;

    Ok(VtnServer {
        mdns_handle,
        router,
        listener,
    })
}