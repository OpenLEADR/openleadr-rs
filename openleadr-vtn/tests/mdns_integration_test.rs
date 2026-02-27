#[cfg(feature = "mdns")]
use openleadr_client::{discover_local_vtns, DiscoveredVtn};
#[cfg(feature = "mdns")]
use openleadr_vtn::{VtnConfig, VtnServer};
#[cfg(feature = "mdns")]
use tokio::{sync::oneshot, time::Duration};

#[tokio::test]
#[cfg(feature = "mdns")]
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
    let server = VtnServer::new(vtn_config).await.unwrap();
    let server_port = server.listener.local_addr().unwrap().port();

    // Make sure mDNS is ready before discovery
    let is_ready = server
        .wait_for_mdns_ready(Duration::from_secs(5))
        .await
        .expect("mDNS browse should not fail");

    assert!(
        is_ready,
        "mDNS service should become discoverable within 5 seconds"
    );

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

    let vtns: Vec<DiscoveredVtn> = discover_local_vtns(
        "_openadr3._tcp.local.",
        tokio::time::Duration::from_secs(1),
        Some(1),
    )
    .await;

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
