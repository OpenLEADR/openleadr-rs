use mdns_sd::{ServiceDaemon, ServiceInfo};

pub async fn register_mdns_vtn_service(host_name: String, service_type: String, server_name: String, port: u16) -> ServiceDaemon {
    let mdns = ServiceDaemon::new().expect("Failed to create daemon");
    
    // Include metadata about the VTN service, such as version and API path
    let properties = [("version", "3.1"), ("path", "/programs")];

    let vtn_service = ServiceInfo::new(
        &service_type,
        &server_name,
        &host_name,
        "", // Auto-detect local IP
        port,
        &properties[..],
    ).expect("valid service info");

    mdns.register(vtn_service).expect("Failed to register VTN service");
    mdns
}


#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use mdns_sd::ServiceEvent;

    #[tokio::test]
    async fn test_mdns_registration_and_discovery() {
        // Purpose of this unit test is a sanity check to ensure mDNS integration works correctly on localhost:
        // 1. That it can be discovered on localhost
        // 2. That the metadata properties are correctly registered
        // 3. On discovery, all properties are intact
        let service_type = "_openadr-test._tcp.local.";
        let server_name = "test-vtn-instance";
        let host_name = "localhost.local."; 
        let port = 1234;

        // Use a SINGLE daemon for both advertising and browsing so that we can reliably discover the service on localhost without network complexities.
        let mdns_daemon = ServiceDaemon::new().expect("Failed to create daemon");
        
        // Include metadata about the VTN service
        let properties = [("version", "3.1"), ("path", "/programs")];

        let vtn_service = ServiceInfo::new(
            service_type,
            server_name,
            host_name,
            "127.0.0.1",
            port,
            &properties[..],
        ).expect("valid service info");

        mdns_daemon.register(vtn_service).expect("Failed to register VTN service");
                
        // We will browse for it on the same daemon
        let receiver = mdns_daemon.browse(service_type).expect("Failed to browse");

        // Give the service time to be advertised
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Search for the ServiceResolved event, timeout after 5 seconds if not found
        let mut found = false;
        let timeout = tokio::time::sleep(Duration::from_secs(5));
        tokio::pin!(timeout);

        loop {
            tokio::select! {
                event = async { receiver.recv_async().await } => {
                    match event {
                        Ok(ServiceEvent::ServiceResolved(info)) => {
                            if info.get_fullname().contains(server_name) {
                                assert_eq!(info.get_port(), port);
                                
                                // Get properties and check the value as a string
                                let props = info.get_properties();
                                let version_str = props.get_property_val_str("version");                                
                                assert_eq!(version_str, Some("3.1"));
                                
                                found = true;
                                break;
                            }
                        }
                        Ok(_) => {
                            // You might see other events like ServiceFound before ServiceResolved
                            continue;
                        }
                        Err(e) => {
                            eprintln!("Error receiving event: {:?}", e);
                            break;
                        }
                    }
                }
                _ = &mut timeout => {
                    println!("Timeout reached");
                    break;
                }
            }
        }

        assert!(found, "The mDNS service was not discovered within the 5s timeout.");
    }
}