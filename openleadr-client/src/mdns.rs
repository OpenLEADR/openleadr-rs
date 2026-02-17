use mdns_sd::{ServiceDaemon, ServiceEvent};
use tracing::{info, warn};

#[derive(Debug, Clone)]
/// Represents a VTN discovered via mDNS, including its connection URL and metadata
pub struct DiscoveredVtn {
    /// The full URL to connect to the VTN (from local_url TXT property)
    pub url: url::Url,
    /// The mDNS instance name (e.g., "openleadr-vtn")
    pub instance_name: String,
    /// OpenADR version (e.g., "3.1.0")
    pub version: String,
    /// API base path (e.g., "" for root or "openadr3/3.1.0")
    pub base_path: String,
}


/// Enables VENs to discover available VTNs via service type (i.e. "_openadr3._tcp.local.") without needing manual configuration of IP addresses or ports
pub async fn discover_local_vtns(
    service_type: &str,
    timeout: u64,
    vtn_limit: Option<usize>,
) -> Vec<DiscoveredVtn> {
    let mdns = ServiceDaemon::new().unwrap();
    let receiver = mdns.browse(&service_type).expect("Failed to browse");

    let mut found_vtns = Vec::new();

    loop {
        tokio::select! {
            event = receiver.recv_async() => {
                if let Ok(ServiceEvent::ServiceResolved(info)) = event {                       
                    // Get local_url from TXT properties (required by OpenADR 3.1 spec)
                    if let Some(local_url_str) = info.get_properties().get_property_val_str("local_url") {
                        match url::Url::parse(local_url_str) {
                            Ok(url) => {
                                found_vtns.push(DiscoveredVtn {
                                    url,
                                    instance_name: info.get_fullname().to_string(),
                                    version: info.get_properties().get_property_val_str("version").unwrap_or("unknown").to_string(),
                                    base_path: info.get_properties().get_property_val_str("base_path").unwrap_or("").to_string(),
                                });
                                
                                // Check if we've hit the limit
                                if let Some(vtn_limit) = vtn_limit {
                                    if found_vtns.len() >= vtn_limit {
                                        return found_vtns;
                                    }
                                }
                            }
                            Err(e) => {
                                warn!("Failed to parse local_url '{}': {}", local_url_str, e);
                            }
                        }
                    } else {
                        warn!("VTN {} missing required 'local_url' property, skipping", info.get_fullname());
                    }
                }
            }
            _ = tokio::time::sleep(std::time::Duration::from_secs(timeout)) => {
                info!("Discovery timeout reached. Found {} VTN(s)", found_vtns.len());
                break;
            }
        }
    }
    found_vtns
}
