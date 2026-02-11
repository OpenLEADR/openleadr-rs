use std::time::Duration;
use openleadr_vtn::create_vtn_server;
use openleadr_client::discover_local_vtns;
use tokio::sync::oneshot;

#[tokio::test]
async fn test_vtn_client_mdns_discovery() {
    std::env::set_var("PORT", "3999"); // Random port
    std::env::set_var("MDNS_SERVICE_TYPE", "_openadr-test._tcp.local.");
    std::env::set_var("MDNS_SERVER_NAME", "test-vtn-integration");
    std::env::set_var("MDNS_IP_ADDRESS", "127.0.0.1");

    // Simulate VTN registration
    let server = create_vtn_server().await.unwrap();
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
    
    let vtns = discover_local_vtns("_openadr-test._tcp.local.").await;
        
    // Gracefully shut down
    shutdown_tx.send(()).ok();
    server_handle.await.ok();

    assert!(!vtns.is_empty(), "Should discover at least one VTN");
    assert!(
        vtns.iter().any(|url| url.port() == Some(server_port)),
        "Should find VTN on port {}",
        server_port
    );
}