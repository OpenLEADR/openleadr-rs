use crate::VtnConfig;
use mdns_sd::{ServiceDaemon, ServiceInfo};

pub async fn register_mdns_vtn_service(config: &VtnConfig) -> ServiceDaemon {
    let mdns = ServiceDaemon::new().expect("Failed to create daemon");

    let local_url = format!(
        "http://{}:{}/{}",
        config.mdns_host_name, config.port, config.mdns_base_path
    );

    // TXT record metadata, as listed in the OpenADR 3.1.0 Definition (mDNS and DNS-SD)
    let mut properties = vec![
        ("version", "3.1"),
        ("base_path", config.mdns_base_path.as_str()),
        ("local_url", local_url.as_str()),
        // The OpenADR endpoints require an access token
        ("requires_auth", "True"),
    ];
    if let Some(program_names) = &config.mdns_program_names {
        properties.push(("program_names", program_names.as_str()));
    }
    if let Some(openapi_url) = &config.mdns_openapi_url {
        properties.push(("openapi_url", openapi_url.as_str()));
    }

    let vtn_service = ServiceInfo::new(
        &config.mdns_service_type,
        &config.mdns_server_name,
        &config.mdns_host_name,
        &config.mdns_ip_address,
        config.port,
        &properties[..],
    )
    .expect("valid service info");

    mdns.register(vtn_service)
        .expect("Failed to register VTN service");
    mdns
}

#[cfg(test)]
mod tests {
    use super::*;
    use mdns_sd::ServiceEvent;
    use std::time::Duration;

    #[tokio::test]
    async fn test_mdns_registration_and_discovery() {
        // Purpose of this unit test is a sanity check to ensure mDNS integration works correctly on localhost:
        // 1. That it can be discovered on localhost
        // 2. That the metadata properties are correctly registered
        // 3. On discovery, all properties are intact
        let config = VtnConfig {
            port: 1234,
            mdns_ip_address: "127.0.0.1".to_string(),
            mdns_host_name: "localhost.local.".to_string(),
            mdns_service_type: "_openadr3._tcp.local.".to_string(),
            mdns_server_name: "test-vtn-instance".to_string(),
            mdns_base_path: "".to_string(),
            mdns_program_names: Some("program-1,program-2".to_string()),
            mdns_openapi_url: Some("https://example.com/openapi.yaml".to_string()),
            mqtt_url: None,
            mqtt_advertise_url: None,
            mqtt_username: None,
            mqtt_password: None,
            mqtt_topic_prefix: String::new(),
            mqtt_ca_path: None,
        };

        // Use a single daemon for both advertising and browsing so that we can reliably discover the service on localhost without network complexities.
        let mdns_daemon = register_mdns_vtn_service(&config).await;

        // We will browse for it on the same daemon
        let receiver = mdns_daemon
            .browse(&config.mdns_service_type)
            .expect("Failed to browse");

        // Search for the ServiceResolved event
        let mut found = false;
        loop {
            tokio::select! {
                event = async { receiver.recv_async().await } => {
                    match event {
                        Ok(ServiceEvent::ServiceResolved(info)) => {
                            if info.get_fullname().contains(&config.mdns_server_name) {
                                assert_eq!(info.get_port(), config.port);

                                // Get properties and check the value as a string
                                let props = info.get_properties();
                                let version_str = props.get_property_val_str("version");
                                assert_eq!(version_str, Some("3.1"));
                                assert_eq!(props.get_property_val_str("requires_auth"), Some("True"));
                                assert_eq!(
                                    props.get_property_val_str("program_names"),
                                    Some("program-1,program-2")
                                );
                                assert_eq!(
                                    props.get_property_val_str("openapi_url"),
                                    Some("https://example.com/openapi.yaml")
                                );

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
                _ = tokio::time::sleep(Duration::from_secs(5)) => {
                    break;
                }
            }
        }

        assert!(
            found,
            "The mDNS service was not discovered within the 5s timeout.",
        );
    }
}
