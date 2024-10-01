use openadr_client::{Client, ClientCredentials, MockClientRef, ProgramClient};
use openadr_wire::program::ProgramContent;
use sqlx::PgPool;
use std::env::VarError;
use url::Url;

// FIXME make this function independent of the storage backend
pub async fn setup_mock_client(db: PgPool) -> Client {
    use openadr_vtn::{data_source::PostgresStorage, jwt::JwtManager, state::AppState};

    // let auth_info = AuthInfo::bl_admin();
    let client_credentials = ClientCredentials::admin();

    let storage = PostgresStorage::new(db).unwrap();
    // storage.auth.try_write().unwrap().push(auth_info);

    let app_state = AppState::new(storage, JwtManager::from_secret(b"test"));

    MockClientRef::new(app_state.into_router()).into_client(Some(client_credentials))
}

pub fn setup_url_client(url: Url) -> Client {
    Client::with_url(url, Some(ClientCredentials::admin()))
}

pub async fn setup_client(db: PgPool) -> Client {
    match std::env::var("OPENADR_RS_VTN_URL") {
        Ok(url) => match url.parse() {
            Ok(url) => setup_url_client(url),
            Err(e) => panic!("Could not parse URL: {e}"),
        },
        Err(VarError::NotPresent) => setup_mock_client(db).await,
        Err(VarError::NotUnicode(e)) => panic!("Could not parse URL: {e:?}"),
    }
}

#[allow(unused)]
pub async fn setup_program_client(program_name: impl ToString, db: PgPool) -> ProgramClient {
    let client = setup_client(db).await;

    let program_content = ProgramContent {
        object_type: None,
        program_name: program_name.to_string(),
        program_long_name: Some("program_long_name".to_string()),
        retailer_name: Some("retailer_name".to_string()),
        retailer_long_name: Some("retailer_long_name".to_string()),
        program_type: None,
        country: None,
        principal_subdivision: None,
        time_zone_offset: None,
        interval_period: None,
        program_descriptions: None,
        binding_events: None,
        local_price: None,
        payload_descriptors: None,
        targets: None,
    };

    client.create_program(program_content).await.unwrap()
}
