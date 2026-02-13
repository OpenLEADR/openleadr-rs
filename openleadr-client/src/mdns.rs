use mdns_sd::{ServiceDaemon, ServiceEvent};

/// Enables VENs to discover available VTNs via service type (i.e. "_openadr-http._tcp.local.") without needing manual configuration of IP addresses or ports
pub async fn discover_local_vtns(service_type: &str) -> Vec<url::Url> {
    let mdns = ServiceDaemon::new().unwrap();
    let receiver = mdns.browse(&service_type).expect("Failed to browse");

    let mut found_urls = Vec::new();
    
    // Browse for a few seconds to find local servers
    let timeout = tokio::time::sleep(std::time::Duration::from_secs(20));
    tokio::pin!(timeout);

    loop {
        tokio::select! {
            event = receiver.recv_async() => { 
                if let Ok(ServiceEvent::ServiceResolved(info)) = event {
                    if let Some(host) = info.get_addresses().iter().next() {
                        let port = info.get_port();
                        if let Ok(url) = url::Url::parse(&format!("http://{}:{}", host, port)) {
                            found_urls.push(url);
                        }
                    }
                }
            }
            _ = &mut timeout => break,
        }
    }
    found_urls
}
