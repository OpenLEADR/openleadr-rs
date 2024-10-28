# OpenADR 3.0 VEN client

This is a client library to interact with an OpenADR 3.0 complaint VTN server.
It mainly wraps the HTTP REST interface into an easy-to-use Rust API.

Basic usage
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