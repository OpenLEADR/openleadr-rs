use openleadr_client::discover_local_vtns;
use tracing::info;

#[tokio::main]
async fn main() {
    info!("Searching for VTN servers...");

    let vtns = discover_local_vtns("_openadr3._tcp.local.").await;

    if vtns.is_empty() {
        info!("No VTNs found :(");
    } else {
        info!("Found {} VTN(s):", vtns.len());
        for (i, url) in vtns.iter().enumerate() {
            info!("  {}. {}", i + 1, url);
        }
    }
}
