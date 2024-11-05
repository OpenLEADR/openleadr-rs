#![warn(missing_docs)]

//! # OpenADR 3.0 VEN client
//!
//! This is a client library to interact with an OpenADR 3.0 complaint VTN server.
//! It mainly wraps the HTTP REST interface into an easy-to-use Rust API.
//!
//! Basic usage
//! ```no_run
//! # use openleadr_client::{Client, ClientCredentials};
//! # use openleadr_wire::event::{EventInterval, EventType, EventValuesMap, Priority};
//! # use openleadr_wire::program::ProgramContent;
//! # use openleadr_wire::values_map::Value;
//! # tokio_test::block_on(async {
//! let credentials =
//!     ClientCredentials::new("client_id".to_string(), "client_secret".to_string());
//! let client = Client::with_url(
//!     "https://your-vtn.com".try_into().unwrap(),
//!     Some(credentials),
//! );
//! let new_program = ProgramContent::new("example-program-name".to_string());
//! let example_program = client.create_program(new_program).await.unwrap();
//! let mut new_event = example_program.new_event(vec![EventInterval {
//!     id: 0,
//!     interval_period: None,
//!     payloads: vec![EventValuesMap {
//!         value_type: EventType::Price,
//!         values: vec![Value::Number(1.23)],
//!     }],
//! }]);
//! new_event.priority = Priority::new(10);
//! new_event.event_name = Some("Some descriptive name".to_string());
//! example_program.create_event(new_event).await.unwrap();
//! # })
//! ```

mod error;
mod event;
mod program;
mod report;
mod resource;
mod target;
mod timeline;
mod ven;

use axum::async_trait;
use openleadr_wire::{event::EventId, Event, Ven};
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
use openleadr_wire::ven::{VenContent, VenId};
pub(crate) use openleadr_wire::{
    event::EventContent,
    program::{ProgramContent, ProgramId},
    target::TargetType,
    Program,
};

#[async_trait]
/// Abstracts the implementation used for actual requests.
///
/// This is used for testing purposes such that we don't need
/// to run an actual server instance but instead directly call into the axum router
pub trait HttpClient: Debug {
    #[allow(missing_docs)]
    fn request_builder(&self, method: Method, url: Url) -> RequestBuilder;
    #[allow(missing_docs)]
    async fn send(&self, req: RequestBuilder) -> reqwest::Result<Response>;
}

/// Client for managing top-level entities on a VTN, i.e., programs and VENs.
///
/// Can be used to implement both, the VEN and the business logic.
///
/// If using the VTN of this project with the built-in OAuth authentication provider,
/// the [`Client`] also allows managing the users.  
#[derive(Debug, Clone)]
pub struct Client {
    client_ref: Arc<ClientRef>,
}

/// Credentials necessary for authentication at the VTN
pub struct ClientCredentials {
    #[allow(missing_docs)]
    pub client_id: String,
    client_secret: String,
    /// Margin to refresh the authentication token with the client_id and client_secret before it expired
    /// This is helpful to prevent an "unauthorized"
    /// due to small differences in client/server times and network latency
    ///
    /// **Default:** 60 sec
    pub refresh_margin: Duration,
    /// Time the authorization token is typically valid for.
    ///
    /// **Default:** 3600 sec, i.e., one hour
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
    /// Creates new [`ClientCredentials`] with default values for
    /// [`refresh_margin`](ClientCredentials::refresh_margin) and
    /// [`default_credential_expires_in`](ClientCredentials::default_credential_expires_in)
    /// (60 and 3600 sec, respectively)
    pub fn new(client_id: String, client_secret: String) -> Self {
        Self {
            client_id,
            client_secret,
            refresh_margin: Duration::from_secs(60),
            default_credential_expires_in: Duration::from_secs(3600),
        }
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
struct ClientRef {
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
        // if there is no auth data, we don't do any authentication
        let Some(auth_data) = &self.auth_data else {
            return Ok(());
        };

        // if there is a token, and it is valid long enough, we don't have to do anything
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
            let problem = res.json::<openleadr_wire::oauth::OAuthError>().await?;
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
            let problem = res.json::<openleadr_wire::problem::Problem>().await?;
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

    async fn post<S, T>(&self, path: &str, body: &S) -> Result<T>
    where
        S: serde::ser::Serialize + Sync,
        T: serde::de::DeserializeOwned,
    {
        let url = self.base_url.join(path)?;
        let request = self.client.request_builder(Method::POST, url).json(body);
        self.request(request, &[]).await
    }

    async fn put<S, T>(&self, path: &str, body: &S) -> Result<T>
    where
        S: serde::ser::Serialize + Sync,
        T: serde::de::DeserializeOwned,
    {
        let url = self.base_url.join(path)?;
        let request = self.client.request_builder(Method::PUT, url).json(body);
        self.request(request, &[]).await
    }

    async fn delete<T>(&self, path: &str) -> Result<T>
    where
        T: serde::de::DeserializeOwned,
    {
        let url = self.base_url.join(path)?;
        let request = self.client.request_builder(Method::DELETE, url);
        self.request(request, &[]).await
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
struct ReqwestClientRef {
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

/// Allows setting specific `skip` and `limit` values for list queries.
///
/// In most cases, you should not need this functionality
/// but use the `_list` functions
/// that automatically try to iterate though all pages to retrieve all entities
pub struct PaginationOptions {
    #[allow(missing_docs)]
    pub skip: usize,
    #[allow(missing_docs)]
    pub limit: usize,
}

/// Filter based on TargetType and TargetValues as specified for various items.
///
/// **Please note:** This does only filter based on what is stored in the `target` field of an item
/// (e.g., [`ProgramContent::targets`]) and should not get interpreted by the server.
/// For example, setting the [`TargetType`] to [`ProgramName`](TargetType::ProgramName)
/// will not filter based on the [`program_name`](ProgramContent::program_name)
/// value but only consider what is stored in the [`targets`](`ProgramContent::targets`)
/// of that program.
///
/// Unfortunately, the specification is not very clear about this behavior,
/// so some servers might interpret it differently.
/// There has been some discussion with the authors of the standard in
/// <https://github.com/oadr3-org/openadr3-vtn-reference-implementation/issues/83> (sadly not public).
#[derive(Debug, Clone)]
pub enum Filter<'a> {
    /// Do not apply any filtering
    None,
    /// Filter by [`TargetType`] and a list of values.
    ///
    /// It will be encoded to the request as query parameters,
    /// e.g., `/programs?targetType=GROUP&targetValues=Group-1&targetValues=Group-2`.
    By(TargetType, &'a [&'a str]),
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

    /// Create a new client with a specific [`reqwest::Client`] instead of
    /// the default one. This allows configuring proxy settings, timeouts, etc.
    pub fn with_reqwest(
        base_url: Url,
        client: reqwest::Client,
        auth: Option<ClientCredentials>,
    ) -> Self {
        Self::with_http_client(base_url, Box::new(ReqwestClientRef { client }), auth)
    }

    /// Create a new client with anything that implements the [`HttpClient`] trait.
    ///
    /// This is mainly helpful for the integration tests
    /// and should most likely not be used for other purposes.
    /// Please use [`Client::with_reqwest`] for detailed HTTP client configuration.
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

    /// Create a new program on the VTN.
    pub async fn create_program(&self, program_content: ProgramContent) -> Result<ProgramClient> {
        let program = self.client_ref.post("programs", &program_content).await?;
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

    /// Get all programs from the VTN with the given query parameters
    ///
    /// It automatically tries to iterate pages where necessary.
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

    /// Get all events from the VTN with the given query parameters.
    ///
    /// It automatically tries to iterate pages where necessary.
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

    /// Create a new VEN entity at the VTN. The content should be created with [`VenContent::new`].
    pub async fn create_ven(&self, ven: VenContent) -> Result<VenClient> {
        let ven = self.client_ref.post("vens", &ven).await?;
        Ok(VenClient::from_ven(self.client_ref.clone(), ven))
    }

    async fn get_vens(
        &self,
        skip: usize,
        limit: usize,
        filter: Filter<'_>,
    ) -> Result<Vec<VenClient>> {
        let skip_str = skip.to_string();
        let limit_str = limit.to_string();
        let mut query: Vec<(&str, &str)> = vec![("skip", &skip_str), ("limit", &limit_str)];

        query.extend_from_slice(filter.to_query_params().as_slice());

        // send request and return response
        let vens: Vec<Ven> = self.client_ref.get("vens", &query).await?;
        Ok(vens
            .into_iter()
            .map(|ven| VenClient::from_ven(self.client_ref.clone(), ven))
            .collect())
    }

    /// Get all VENs from the VTN with the given query parameters.
    ///
    /// The client automatically tries to iterate pages where necessary.
    pub async fn get_ven_list(&self, filter: Filter<'_>) -> Result<Vec<VenClient>> {
        self.client_ref
            .iterate_pages(|skip, limit| self.get_vens(skip, limit, filter.clone()))
            .await
    }

    /// Get VEN by id from VTN
    pub async fn get_ven_by_id(&self, id: &VenId) -> Result<VenClient> {
        let ven = self
            .client_ref
            .get(&format!("vens/{}", id.as_str()), &[])
            .await?;
        Ok(VenClient::from_ven(self.client_ref.clone(), ven))
    }

    /// Get VEN by name from VTN.
    /// According to the spec, a [`ven_name`](VenContent::ven_name) must be unique for the whole VTN instance.
    pub async fn get_ven_by_name(&self, name: &str) -> Result<VenClient> {
        let mut vens: Vec<Ven> = self.client_ref.get("vens", &[("venName", name)]).await?;
        match vens[..] {
            [] => Err(Error::ObjectNotFound),
            [_] => Ok(VenClient::from_ven(self.client_ref.clone(), vens.remove(0))),
            [..] => Err(Error::DuplicateObject),
        }
    }
}
