use openleadr_client::discover_local_vtns;

#[tokio::main]
async fn main() {
    println!("Searching for VTN servers...");
    
    let vtns = discover_local_vtns("_openadr-http._tcp.local.").await;
    
    if vtns.is_empty() {
        println!("No VTNs found :(");
    } else {
        println!("Found {} VTN(s):", vtns.len());
        for (i, url) in vtns.iter().enumerate() {
            println!("  {}. {}", i + 1, url);
        }
    }
}