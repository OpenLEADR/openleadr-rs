use mdns_sd::{ServiceDaemon, ServiceInfo};

pub async fn register_mdns_vtn_service(host_name: String, service_type: String, server_name: String, port: u16) {
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
}
