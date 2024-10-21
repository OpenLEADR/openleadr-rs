mod error;
mod event;
mod program;
mod report;
mod resource;
mod target;
mod timeline;
mod ven;

use axum::async_trait;
use openadr_wire::{event::EventId, Event, Ven};
use std::{
    fmt::Debug,
    future::Future,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::RwLock;

use reqwest::{Method, RequestBuilder, Response};
use url::Url;

pub use error::*;
pub use event::*;
pub use program::*;
pub use report::*;
pub use resource::*;
pub use target::*;
pub use timeline::*;
pub use ven::*;

use crate::error::Result;
use openadr_wire::ven::VenContent;
pub(crate) use openadr_wire::{
    event::EventContent,
    program::{ProgramContent, ProgramId},
    target::TargetLabel,
    Program,
};

#[async_trait]
pub trait HttpClient: Debug {
    fn request_builder(&self, method: Method, url: Url) -> RequestBuilder;
    async fn send(&self, req: RequestBuilder) -> reqwest::Result<Response>;
}

/// Client used for interaction with a VTN.
///
/// Can be used to implement both, the VEN and the business logic
#[derive(Debug, Clone)]
pub struct Client {
    client_ref: Arc<ClientRef>,
}

pub struct ClientCredentials {
    pub client_id: String,
    client_secret: String,
    pub refresh_margin: Duration,
    pub default_credential_expires_in: Duration,
}

impl Debug for ClientCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(std::any::type_name::<Self>())
            .field("client_id", &self.client_id)
            .field("refresh_margin", &self.refresh_margin)
            .field(
                "default_credential_expires_in",
                &self.default_credential_expires_in,
            )
            .finish_non_exhaustive()
    }
}

impl ClientCredentials {
    pub fn new(client_id: String, client_secret: String) -> Self {
        Self {
            client_id,
            client_secret,
            refresh_margin: Duration::from_secs(60),
            default_credential_expires_in: Duration::from_secs(3600),
        }
    }

    pub fn admin() -> Self {
        Self::new("admin".to_string(), "admin".to_string())
    }
}

struct AuthToken {
    token: String,
    expires_in: Duration,
    since: Instant,
}

impl Debug for AuthToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(std::any::type_name::<Self>())
            .field("expires_in", &self.expires_in)
            .field("since", &self.since)
            .finish_non_exhaustive()
    }
}

#[derive(Debug)]
pub struct ClientRef {
    client: Box<dyn HttpClient + Send + Sync>,
    base_url: Url,
    default_page_size: usize,
    auth_data: Option<ClientCredentials>,
    auth_token: RwLock<Option<AuthToken>>,
}

impl ClientRef {
    /// This ensures the client is authenticated.
    ///
    /// We follow the process according to RFC 6749, section 4.4 (client
    /// credentials grant). The client id and secret are by default sent via
    /// HTTP Basic Auth.
    async fn ensure_auth(&self) -> Result<()> {
        // if there is no auth data we don't do any authentication
        let Some(auth_data) = &self.auth_data else {
            return Ok(());
        };

        // if there is a token and it is valid long enough, we don't have to do anything
        if let Some(token) = self.auth_token.read().await.as_ref() {
            if token.since.elapsed() < token.expires_in - auth_data.refresh_margin {
                return Ok(());
            }
        }

        #[derive(serde::Serialize)]
        struct AccessTokenRequest {
            grant_type: &'static str,
            #[serde(skip_serializing_if = "Option::is_none")]
            scope: Option<String>,
            #[serde(skip_serializing_if = "Option::is_none")]
            client_id: Option<String>,
            #[serde(skip_serializing_if = "Option::is_none")]
            client_secret: Option<String>,
        }

        // we should authenticate
        let auth_url = self.base_url.join("auth/token")?;
        let request =
            self.client
                .request_builder(Method::POST, auth_url)
                .form(&AccessTokenRequest {
                    grant_type: "client_credentials",
                    scope: None,
                    client_id: None,
                    client_secret: None,
                });
        let request = request.basic_auth(&auth_data.client_id, Some(&auth_data.client_secret));
        let request = request.header("Accept", "application/json");
        let since = Instant::now();
        let res = self.client.send(request).await?;
        if !res.status().is_success() {
            let problem = res.json::<openadr_wire::oauth::OAuthError>().await?;
            return Err(Error::AuthProblem(problem));
        }

        #[derive(Debug, serde::Deserialize)]
        struct AuthResult {
            access_token: String,
            token_type: String,
            #[serde(default)]
            expires_in: Option<u64>,
            // Refresh tokens aren't supported currently
            // #[serde(default)]
            // refresh_token: Option<String>,
            // #[serde(default)]
            // scope: Option<String>,
            // #[serde(flatten)]
            // other: std::collections::HashMap<String, serde_json::Value>,
        }

        let auth_result = res.json::<AuthResult>().await?;
        if auth_result.token_type.to_lowercase() != "bearer" {
            return Err(Error::OAuthTokenNotBearer);
        }
        let token = AuthToken {
            token: auth_result.access_token,
            expires_in: auth_result
                .expires_in
                .map(Duration::from_secs)
                .unwrap_or(auth_data.default_credential_expires_in),
            since,
        };

        *self.auth_token.write().await = Some(token);
        Ok(())
    }

    async fn request<T: serde::de::DeserializeOwned>(
        &self,
        mut request: RequestBuilder,
        query: &[(&str, &str)],
    ) -> Result<T> {
        self.ensure_auth().await?;
        request = request.header("Accept", "application/json");
        if !query.is_empty() {
            request = request.query(&query);
        }

        // read token and insert in request if available
        {
            let token = self.auth_token.read().await;
            if let Some(token) = token.as_ref() {
                request = request.bearer_auth(&token.token);
            }
        }
        let res = self.client.send(request).await?;

        // handle any errors returned by the server
        if !res.status().is_success() {
            let problem = res.json::<openadr_wire::problem::Problem>().await?;
            return Err(crate::error::Error::from(problem));
        }

        Ok(res.json().await?)
    }

    async fn get<T: serde::de::DeserializeOwned>(
        &self,
        path: &str,
        query: &[(&str, &str)],
    ) -> Result<T> {
        let url = self.base_url.join(path)?;
        let request = self.client.request_builder(Method::GET, url);
        self.request(request, query).await
    }

    async fn post<S, T>(&self, path: &str, body: &S, query: &[(&str, &str)]) -> Result<T>
    where
        S: serde::ser::Serialize + Sync,
        T: serde::de::DeserializeOwned,
    {
        let url = self.base_url.join(path)?;
        let request = self.client.request_builder(Method::POST, url).json(body);
        self.request(request, query).await
    }

    async fn put<S, T>(&self, path: &str, body: &S, query: &[(&str, &str)]) -> Result<T>
    where
        S: serde::ser::Serialize + Sync,
        T: serde::de::DeserializeOwned,
    {
        let url = self.base_url.join(path)?;
        let request = self.client.request_builder(Method::PUT, url).json(body);
        self.request(request, query).await
    }

    async fn delete<T>(&self, path: &str, query: &[(&str, &str)]) -> Result<T>
    where
        T: serde::de::DeserializeOwned,
    {
        let url = self.base_url.join(path)?;
        let request = self.client.request_builder(Method::DELETE, url);
        self.request(request, query).await
    }

    fn default_page_size(&self) -> usize {
        self.default_page_size
    }

    async fn iterate_pages<T, Fut>(
        &self,
        single_page_req: impl Fn(usize, usize) -> Fut,
    ) -> Result<Vec<T>>
    where
        Fut: Future<Output = Result<Vec<T>>>,
    {
        let page_size = self.default_page_size();
        let mut items = vec![];
        let mut page = 0;
        // TODO: pagination should depend on that the server indicated there are more results
        loop {
            let received = single_page_req(page * page_size, page_size).await?;
            let received_all = received.len() < page_size;
            for item in received {
                items.push(item);
            }

            if received_all {
                break;
            } else {
                page += 1;
            }
        }

        Ok(items)
    }
}

#[derive(Debug)]
pub struct ReqwestClientRef {
    client: reqwest::Client,
}

#[async_trait]
impl HttpClient for ReqwestClientRef {
    fn request_builder(&self, method: Method, url: Url) -> RequestBuilder {
        self.client.request(method, url)
    }

    async fn send(&self, req: RequestBuilder) -> std::result::Result<Response, reqwest::Error> {
        req.send().await
    }
}

pub struct PaginationOptions {
    pub skip: usize,
    pub limit: usize,
}

#[derive(Debug, Clone)]
pub enum Filter<'a> {
    None,
    By(TargetLabel, &'a [&'a str]),
}

impl<'a> Filter<'a> {
    pub(crate) fn to_query_params(&'a self) -> Vec<(&'a str, &'a str)> {
        let mut query = vec![];
        if let Filter::By(ref target_label, target_values) = self {
            query.push(("targetType", target_label.as_str()));

            for target_value in *target_values {
                query.push(("targetValues", *target_value));
            }
        }
        query
    }
}

impl Client {
    /// Create a new client for a VTN located at the specified URL
    pub fn with_url(base_url: Url, auth: Option<ClientCredentials>) -> Self {
        let client = reqwest::Client::new();
        Self::with_reqwest(base_url, client, auth)
    }

    /// Create a new client, but use the specific reqwest client instead of
    /// the default one. This allows you to configure proxy settings, timeouts, etc.
    pub fn with_reqwest(
        base_url: Url,
        client: reqwest::Client,
        auth: Option<ClientCredentials>,
    ) -> Self {
        Self::with_http_client(base_url, Box::new(ReqwestClientRef { client }), auth)
    }

    pub fn with_http_client(
        base_url: Url,
        client: Box<dyn HttpClient + Send + Sync>,
        auth: Option<ClientCredentials>,
    ) -> Self {
        let client_ref = ClientRef {
            client,
            base_url,
            default_page_size: 50,
            auth_data: auth,
            auth_token: RwLock::new(None),
        };
        Self::new(client_ref)
    }

    fn new(client_ref: ClientRef) -> Self {
        Client {
            client_ref: Arc::new(client_ref),
        }
    }

    /// Create a new program on the VTN
    pub async fn create_program(&self, program_content: ProgramContent) -> Result<ProgramClient> {
        let program = self
            .client_ref
            .post("programs", &program_content, &[])
            .await?;
        Ok(ProgramClient::from_program(self.clone(), program))
    }

    /// Lowlevel operation that gets a list of programs from the VTN with the given query parameters
    pub async fn get_programs(
        &self,
        filter: Filter<'_>,
        pagination: PaginationOptions,
    ) -> Result<Vec<ProgramClient>> {
        // convert query params
        let skip_str = pagination.skip.to_string();
        let limit_str = pagination.limit.to_string();
        // insert into query params
        let mut query: Vec<(&str, &str)> = vec![("skip", &skip_str), ("limit", &limit_str)];

        query.extend_from_slice(filter.to_query_params().as_slice());

        // send request and return response
        let programs: Vec<Program> = self.client_ref.get("programs", &query).await?;
        Ok(programs
            .into_iter()
            .map(|program| ProgramClient::from_program(self.clone(), program))
            .collect())
    }

    /// Get a list of programs from the VTN with the given query parameters
    pub async fn get_program_list(&self, filter: Filter<'_>) -> Result<Vec<ProgramClient>> {
        self.client_ref
            .iterate_pages(|skip, limit| {
                self.get_programs(filter.clone(), PaginationOptions { skip, limit })
            })
            .await
    }

    /// Get a program by id
    pub async fn get_program_by_id(&self, id: &ProgramId) -> Result<ProgramClient> {
        let program = self
            .client_ref
            .get(&format!("programs/{}", id.as_str()), &[])
            .await?;

        Ok(ProgramClient::from_program(self.clone(), program))
    }

    /// Create a new event on the VTN
    pub async fn create_event(&self, event_data: EventContent) -> Result<EventClient> {
        let event = self.client_ref.post("events", &event_data, &[]).await?;
        Ok(EventClient::from_event(self.client_ref.clone(), event))
    }

    /// Lowlevel operation that gets a list of events from the VTN with the given query parameters
    pub async fn get_events(
        &self,
        program_id: Option<&ProgramId>,
        filter: Filter<'_>,
        pagination: PaginationOptions,
    ) -> Result<Vec<EventClient>> {
        // convert query params
        let skip_str = pagination.skip.to_string();
        let limit_str = pagination.limit.to_string();
        // insert into query params
        let mut query: Vec<(&str, &str)> = vec![("skip", &skip_str), ("limit", &limit_str)];

        query.extend_from_slice(filter.to_query_params().as_slice());

        if let Some(program_id) = program_id {
            query.push(("programID", program_id.as_str()));
        }

        // send request and return response
        let events: Vec<Event> = self.client_ref.get("events", &query).await?;
        Ok(events
            .into_iter()
            .map(|event| EventClient::from_event(self.client_ref.clone(), event))
            .collect())
    }

    /// Get a list of events from the VTN with the given query parameters
    pub async fn get_event_list(
        &self,
        program_id: Option<&ProgramId>,
        filter: Filter<'_>,
    ) -> Result<Vec<EventClient>> {
        self.client_ref
            .iterate_pages(|skip, limit| {
                self.get_events(
                    program_id,
                    filter.clone(),
                    PaginationOptions { skip, limit },
                )
            })
            .await
    }

    /// Get an event by id
    pub async fn get_event_by_id(&self, id: &EventId) -> Result<EventClient> {
        let event = self
            .client_ref
            .get(&format!("events/{}", id.as_str()), &[])
            .await?;

        Ok(EventClient::from_event(self.client_ref.clone(), event))
    }

    pub async fn create_ven(&self, ven: VenContent) -> Result<VenClient> {
        let ven = self.client_ref.post("vens", &ven, &[]).await?;
        Ok(VenClient::from_ven(self.client_ref.clone(), ven))
    }

    async fn get_vens(
        &self,
        skip: usize,
        limit: usize,
        ven_name: Option<&str>,
        filter: Filter<'_>,
    ) -> Result<Vec<VenClient>> {
        let skip_str = skip.to_string();
        let limit_str = limit.to_string();
        let mut query: Vec<(&str, &str)> = vec![("skip", &skip_str), ("limit", &limit_str)];

        query.extend_from_slice(filter.to_query_params().as_slice());

        if let Some(ven_name) = ven_name {
            query.push(("venName", ven_name));
        }

        // send request and return response
        let vens: Vec<Ven> = self.client_ref.get("vens", &query).await?;
        Ok(vens
            .into_iter()
            .map(|ven| VenClient::from_ven(self.client_ref.clone(), ven))
            .collect())
    }

    pub async fn get_ven_list(
        &self,
        ven_name: Option<&str>,
        filter: Filter<'_>,
    ) -> Result<Vec<VenClient>> {
        self.client_ref
            .iterate_pages(|skip, limit| self.get_vens(skip, limit, ven_name, filter.clone()))
            .await
    }
}
