mod api;
pub mod data_source;
mod error;
pub mod jwt;
pub mod mdns;
pub mod state;

#[cfg(feature = "postgres")]
use crate::data_source::PostgresStorage;
use crate::{data_source::Migrate, state::AppState};

use crate::mdns::register_mdns_vtn_service;
use mdns_sd::ServiceDaemon;
use tokio::net::TcpListener;
use tracing::{info, warn};

pub struct VtnServer {
    pub mdns_handle: ServiceDaemon,
    pub router: axum::Router,
    pub listener: TcpListener,
}

#[derive(Clone, Debug)]
pub struct VtnConfig {
    pub port: u16,
    pub mdns_ip_address: String,
    pub mdns_host_name: String,
    pub mdns_service_type: String,
    pub mdns_server_name: String,
    pub mdns_base_path: String,
}

impl Default for VtnConfig {
    fn default() -> Self {
        Self {
            port: 3000,
            mdns_ip_address: "127.0.0.1".to_string(),
            mdns_host_name: "vtn.local.".to_string(),
            mdns_service_type: "_openadr3._tcp.local.".to_string(),
            mdns_server_name: "openleadr-vtn".to_string(),
            mdns_base_path: "".to_string(),
        }
    }
}

impl VtnConfig {
    /// Load configuration from environment variables
    pub fn from_env() -> Self {
        Self {
            port: std::env::var("PORT")
                .ok()
                .and_then(|s| s.parse::<u16>().ok())
                .unwrap_or(3000),
            mdns_ip_address: std::env::var("MDNS_IP_ADDRESS")
                .unwrap_or_else(|_| "127.0.0.1".to_string()),
            mdns_host_name: std::env::var("MDNS_HOST_NAME")
                .unwrap_or_else(|_| "vtn.local.".to_string()),
            mdns_service_type: std::env::var("MDNS_SERVICE_TYPE")
                .unwrap_or_else(|_| "_openadr3._tcp.local.".to_string()),
            mdns_server_name: std::env::var("MDNS_SERVER_NAME")
                .unwrap_or_else(|_| "openleadr-vtn".to_string()),
            mdns_base_path: std::env::var("MDNS_BASE_PATH").unwrap_or_else(|_| "".to_string()),
        }
    }
}

pub async fn create_vtn_server(config: VtnConfig) -> Result<VtnServer, Box<dyn std::error::Error>> {
    let addr = format!("0.0.0.0:{}", config.port);
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

    let mdns_handle = register_mdns_vtn_service(&config).await;

    Ok(VtnServer {
        mdns_handle,
        router,
        listener,
    })
}
