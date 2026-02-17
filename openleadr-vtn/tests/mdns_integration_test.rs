use openleadr_client::{discover_local_vtns, DiscoveredVtn};
use openleadr_vtn::{create_vtn_server, VtnConfig};
use std::time::Duration;
use tokio::sync::oneshot;

#[tokio::test]
async fn test_vtn_client_mdns_discovery() {
    let vtn_config = VtnConfig {
        port: 3999,
        mdns_ip_address: "127.0.0.1".to_string(),
        mdns_host_name: "localhost.local.".to_string(),
        mdns_service_type: "_openadr3._tcp.local.".to_string(),
        mdns_server_name: "test-vtn-integration".to_string(),
        mdns_base_path: "".to_string(),
    };

    // Simulate VTN registration
    let server = create_vtn_server(vtn_config).await.unwrap();
    let server_port = server.listener.local_addr().unwrap().port();

    // Create shutdown channel
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

    // Start server with graceful shutdown
    let server_handle = tokio::spawn(async move {
        axum::serve(server.listener, server.router)
            .with_graceful_shutdown(async {
                shutdown_rx.await.ok();
            })
            .await
            .unwrap();
    });

    // Give it time to broadcast
    tokio::time::sleep(Duration::from_millis(500)).await;

    let vtns: Vec<DiscoveredVtn> = discover_local_vtns("_openadr3._tcp.local.", 1, Some(1)).await;

    // Gracefully shut down
    shutdown_tx.send(()).ok();
    server_handle.await.ok();

    assert!(!vtns.is_empty(), "Should discover at least one VTN");
    assert!(
        vtns.iter().any(|vtn| vtn.url.port() == Some(server_port)),
        "Should find VTN on port {}",
        server_port
    );
}
