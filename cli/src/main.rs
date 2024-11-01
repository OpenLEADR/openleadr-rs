use clap::Parser;
use dotenvy::dotenv;
use openleadr_client::{everest::start_everest, Client};
use openleadr_vtn::start_vtn;
const CRATE_VERSION: &str = env!("CARGO_PKG_VERSION");

#[derive(Parser, Debug, Clone, Default)]
#[command(author, version, about, long_about = None)]
pub enum Command {
    /// Start a single VEN
    Ven,
    /// List the Cargo version of the cli package
    Version,
    #[default]
    /// Start the VTN server
    Vtn,
    Everest,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenv().ok();
    let cmd = Command::parse();
    match cmd {
        Command::Ven => {
            println!("Starting client");
            let _ = Client::start_client().await;
        }
        Command::Version => println!("Workspace version {}", CRATE_VERSION),
        Command::Vtn => {
            println!("Starting VTN");
            let _ = start_vtn().await;
        }
        Command::Everest => {
            println!("Starting Everest");
            let _ = start_everest().await;
        }
    }
    Ok(())
}
