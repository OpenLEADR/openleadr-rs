use openleadr_client::{discover_local_vtns, DiscoveredVtn};
use tracing::info;
use tracing_subscriber;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    info!("Searching for VTN servers...");

    let vtns: Vec<DiscoveredVtn> = discover_local_vtns(
        "_openadr3._tcp.local.",
        tokio::time::Duration::from_secs(1),
        None,
    )
    .await;

    if vtns.is_empty() {
        info!("No VTNs found :(");
    } else {
        info!("Found {} VTN(s):", vtns.len());
        for (i, v) in vtns.iter().enumerate() {
            info!("  {}. {}", i + 1, v.url);
        }
    }
}
