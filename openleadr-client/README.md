![maintenance-status](https://img.shields.io/badge/maintenance-actively--developed-brightgreen.svg)
![codecov](https://codecov.io/gh/OpenLEADR/openleadr-rs/graph/badge.svg?token=BKQ0QW9G8H)
![Checks](https://github.com/OpenLEADR/openleadr-rs/actions/workflows/checks.yml/badge.svg?branch=main)
![Crates.io Version](https://img.shields.io/crates/v/openleadr-client)

# OpenADR 3.0 VEN client library in Rust

![LF energy OpenLEADR logo](../openleadr-logo.svg)

This is a client library to interact with an OpenADR 3.0 complaint VTN server.
It mainly wraps the HTTP REST interface into an easy-to-use Rust API.

The following contains information specific to the client library.
If you are interested in information about the whole project, please visit the [project level Readme](../README.md).

### Basic usage
For a basic example of how to use the client library, see the following.
You can find detailed documentation of the library at [docs.rs](https://docs.rs/openleadr-client/latest/openleadr_client/).
```rust
async fn main() {
    let credentials = ClientCredentials::new("client_id".to_string(), "client_secret".to_string());
    let client = Client::with_url("https://your-vtn.com".try_into().unwrap(), Some(credentials));

    let new_program = ProgramContent::new("example-program-name".to_string());
    let example_program = client.create_program(new_program).await.unwrap();

    let mut new_event = example_program.new_event();
    new_event.priority = Priority::new(10);
    new_event.event_name = Some("Some descriptive name".to_string());
    example_program.create_event(new_event).await.unwrap(); 
}
```

We plan to create a CLI binary using this library as well.
See [#52](https://github.com/OpenLEADR/openleadr-rs/issues/52) for the current progress.