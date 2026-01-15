use async_trait::async_trait;
use axum::body::Body;
use http_body_util::BodyExt;
use openleadr_client::{Client, ClientCredentials, ClientKind, HttpClient, ProgramClient};
use openleadr_vtn::{data_source::PostgresStorage, state::AppState};
use openleadr_wire::program::ProgramRequest;
use reqwest::{Method, RequestBuilder, Response};
use sqlx::PgPool;
use std::{env::VarError, ops::Deref, sync::Arc};
use tower::{Service, ServiceExt};
use url::Url;

#[allow(dead_code)]
pub enum AuthRole {
    Bl,
    Ven,
}

fn default_credentials(auth_role: AuthRole) -> ClientCredentials {
    let (id, secr) = match auth_role {
        AuthRole::Bl => ("bl-client", "bl-client"),
        AuthRole::Ven => ("ven-client", "ven-client"),
    };

    ClientCredentials::new(id.to_string(), secr.to_string())
}

#[derive(Debug)]
pub struct MockClientRef {
    router: Arc<tokio::sync::Mutex<axum::Router>>,
}

impl MockClientRef {
    pub fn new(router: axum::Router) -> Self {
        MockClientRef {
            router: Arc::new(tokio::sync::Mutex::new(router)),
        }
    }

    pub fn into_client<K: ClientKind>(self, auth: Option<ClientCredentials>) -> Client<K> {
        Client::with_http_client(
            "https://example.com/".parse().unwrap(),
            "https://example.com/auth/token".parse().unwrap(),
            Box::new(self),
            auth,
        )
    }
}

#[async_trait]
impl HttpClient for MockClientRef {
    fn request_builder(&self, method: Method, url: Url) -> RequestBuilder {
        reqwest::Client::new().request(method, url)
    }

    async fn send(&self, req: RequestBuilder) -> reqwest::Result<Response> {
        let request = axum::http::Request::try_from(req.build()?)?;

        let response =
            ServiceExt::<axum::http::Request<Body>>::ready(&mut *self.router.lock().await)
                .await
                .unwrap()
                .call(request)
                .await
                .unwrap();

        let (parts, body) = response.into_parts();
        let body = body.collect().await.unwrap().to_bytes();
        let body = reqwest::Body::from(body);
        let response = axum::http::Response::from_parts(parts, body);

        Ok(response.into())
    }
}

pub struct TestContext<K> {
    pub client: Client<K>,
}

impl<K> Deref for TestContext<K> {
    type Target = Client<K>;
    fn deref(&self) -> &Self::Target {
        &self.client
    }
}

#[allow(unused)]
pub async fn setup<K: ClientKind>(auth_role: AuthRole) -> TestContext<K> {
    dotenvy::dotenv().unwrap();
    match std::env::var("OPENLEADR_RS_VTN_URL") {
        Ok(url) => match url.parse() {
            Ok(url) => TestContext {
                client: setup_url_client(url),
            },
            Err(e) => panic!("Could not parse URL: {e}"),
        },
        Err(VarError::NotPresent) => match std::env::var("DATABASE_URL") {
            Ok(db_url) => {
                let db = PgPool::connect(&db_url).await.unwrap();
                local_vtn_test_client(db, auth_role).await
            }
            Err(_) => panic!("Must either set DATABASE_URL or OPENLEADR_RS_VTN_URL env var"),
        },
        Err(VarError::NotUnicode(e)) => panic!("Could not parse URL: {e:?}"),
    }
}

async fn local_vtn_test_client<K: ClientKind>(db: PgPool, auth_role: AuthRole) -> TestContext<K> {
    let cred = default_credentials(auth_role);
    let storage = PostgresStorage::new(db).unwrap();

    let router = AppState::new(storage).await.into_router();
    TestContext {
        client: MockClientRef::new(router).into_client(Some(cred)),
    }
}

// FIXME make this function independent of the storage backend
pub async fn setup_mock_client<K: ClientKind>(db: PgPool) -> Client<K> {
    // let auth_info = AuthInfo::bl_admin();
    let client_credentials = ClientCredentials::new("admin".to_string(), "admin".to_string());

    let storage = PostgresStorage::new(db).unwrap();
    // storage.auth.try_write().unwrap().push(auth_info);

    let app_state = AppState::new(storage).await;

    MockClientRef::new(app_state.into_router()).into_client(Some(client_credentials))
}

pub fn setup_url_client<K: ClientKind>(url: Url) -> Client<K> {
    Client::with_url(
        url,
        Some(ClientCredentials::new(
            "admin".to_string(),
            "admin".to_string(),
        )),
    )
}

pub async fn setup_client<K: ClientKind>(db: PgPool) -> Client<K> {
    match std::env::var("OPENADR_VTN_URL") {
        Ok(url) => match url.parse() {
            Ok(url) => setup_url_client(url),
            Err(e) => panic!("Could not parse URL: {e}"),
        },
        Err(VarError::NotPresent) => setup_mock_client(db).await,
        Err(VarError::NotUnicode(e)) => panic!("Could not parse URL: {e:?}"),
    }
}

#[allow(unused)]
pub async fn setup_program_client<K: ClientKind>(
    program_name: impl ToString,
    db: PgPool,
) -> ProgramClient<K> {
    let client = setup_client(db).await;

    let program_content = ProgramRequest {
        program_name: program_name.to_string(),
        interval_period: None,
        program_descriptions: None,
        payload_descriptors: None,
        attributes: None,
        targets: vec![],
    };

    client.create_program(program_content).await.unwrap()
}
