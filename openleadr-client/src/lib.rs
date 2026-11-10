#![warn(missing_docs)]
#![deny(rustdoc::broken_intra_doc_links)]
#![deny(rustdoc::private_intra_doc_links)]

//! # OpenADR 3.1 VEN client
//!
//! This is a client library to interact with an OpenADR 3.1 complaint VTN server.
//! It mainly wraps the HTTP REST interface into an easy-to-use Rust API.
//!
//! Basic usage
//! ```no_run
//! # use openleadr_client::{Client, ClientCredentials, BusinessLogic};
//! # use openleadr_wire::event::{EventInterval, EventType, EventValuesMap, Priority};
//! # use openleadr_wire::program::ProgramRequest;
//! # use openleadr_wire::values_map::Value;
//! # tokio_test::block_on(async {
//! let credentials =
//!     ClientCredentials::new("client_id".to_string(), "client_secret".to_string());
//! let client = Client::<BusinessLogic>::with_url(
//!     "https://your-vtn.com".parse().unwrap(),
//!     Some(credentials),
//! );
//! let new_program = ProgramRequest::new("example-program-name".to_string());
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
//!
//! If you want to use a separate OAuth provider that is not built into the VTN server,
//! you can do so as well.
//! ```no_run
//! # use openleadr_client::{Client, ClientCredentials, BusinessLogic};
//! # let credentials =
//! #    ClientCredentials::new("client_id".to_string(), "client_secret".to_string());
//! // optionally, you can build special configuration into the reqwest client here as well
//! let reqwest_client = reqwest::Client::new();
//! let client = Client::<BusinessLogic>::with_details(
//!     "https://your-vtn.com".parse().unwrap(),
//!     "https://your-oauth-provider.com".parse().unwrap(),
//!     reqwest_client,
//!     Some(credentials),
//! );
//! ```

mod error;
mod event;
#[cfg(feature = "mdns")]
mod mdns;
mod program;
mod report;
mod resource;
mod timeline;
mod ven;

use async_trait::async_trait;
use openleadr_wire::{
    Event, Ven,
    event::EventId,
    subscription::{
        MqttNotifierAuthentication, Notification, NotifierTopicsResponse, NotifiersResponse,
        Operation,
    },
};
use paho_mqtt::{ConnectOptionsBuilder, QoS};
use std::{
    collections::HashMap,
    fmt::Debug,
    future::Future,
    marker::PhantomData,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::{Mutex, RwLock, broadcast};

use reqwest::{Method, RequestBuilder, Response};
use url::Url;

pub use error::*;
pub use event::*;
#[cfg(feature = "mdns")]
pub use mdns::*;
pub use program::*;
pub use report::*;
pub use resource::*;
pub use timeline::*;
pub use ven::*;

use crate::{error::Result, sealed::SealedClientKind};
use openleadr_wire::{
    Program,
    program::{ProgramId, ProgramRequest},
    ven::{BlVenRequest, VenId, VenRequest, VenVenRequest},
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
pub struct Client<K> {
    client_ref: Arc<ClientRef<K>>,
}

/// Client for managing an mqtt connection with a VTN.
///
/// This can be used to publish reports via mqtt, and to receive notifications
/// via [`MqttClient::subscribe`] and [`MqttClient::notifications`].
#[derive(Clone)]
pub struct MqttClient<K> {
    client_ref: Arc<ClientRef<K>>,
    mqtt_client: paho_mqtt::AsyncClient,
    messages: broadcast::Sender<Option<paho_mqtt::Message>>,
    subscriptions: Arc<Mutex<HashMap<(MqttSubscriptionTarget, Option<Operation>), String>>>,
}

const MQTT_NOTIFICATION_BUFFER: usize = 128;

impl<K: Debug> Debug for MqttClient<K> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MqttClient")
            .field("client_ref", &self.client_ref)
            .field("mqtt_client", &"<mqtt_client>")
            .finish()
    }
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

mod sealed {
    use std::fmt::Debug;

    use crate::{BusinessLogic, VirtualEndNode};

    #[derive(Debug, Clone)]
    pub struct VenEventTopics {
        pub(crate) upload_topic: String,
    }

    pub trait SealedClientKind {
        type EventTopics: Debug + Clone;
    }

    impl SealedClientKind for BusinessLogic {
        type EventTopics = ();
    }
    impl SealedClientKind for VirtualEndNode {
        type EventTopics = VenEventTopics;
    }
}

/// This trait is used to mark the kind of [`Client`] to use,
/// either a [`BusinessLogic`], or a [`VirtualEndNode`]
pub trait ClientKind: Copy + SealedClientKind {}
#[derive(Copy, Clone, Debug)]
/// Used to mark the kind of [`Client`], see also [`ClientKind`]
pub struct BusinessLogic;
impl ClientKind for BusinessLogic {}
#[derive(Copy, Clone, Debug)]
/// Used to mark the kind of [`Client`], see also [`ClientKind`]
pub struct VirtualEndNode;
impl ClientKind for VirtualEndNode {}

struct ClientRef<K> {
    client: Box<dyn HttpClient + Send + Sync>,
    vtn_base_url: Url,
    oauth_base_url: Url,
    default_page_size: usize,
    auth_data: Option<ClientCredentials>,
    auth_token: RwLock<Option<AuthToken>>,
    phantom: PhantomData<K>,
}

impl<K> Debug for ClientRef<K> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(std::any::type_name::<Self>())
            .field("client", &"<http_client>")
            .field("vtn_base_url", &self.vtn_base_url)
            .field("oauth_base_url", &self.oauth_base_url)
            .field("default_page_size", &self.default_page_size)
            .field("auth_data", &self.auth_data)
            .field("auth_token", &self.auth_token)
            .finish()
    }
}

impl<K: ClientKind> ClientRef<K> {
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
        if let Some(token) = self.auth_token.read().await.as_ref()
            && token.since.elapsed() < token.expires_in - auth_data.refresh_margin
        {
            return Ok(());
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
        let request = self
            .client
            .request_builder(Method::POST, self.oauth_base_url.clone())
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
        let url = self.vtn_base_url.join(path)?;
        let request = self.client.request_builder(Method::GET, url);
        self.request(request, query).await
    }

    async fn post<S, T>(&self, path: &str, body: &S) -> Result<T>
    where
        S: serde::ser::Serialize + Sync,
        T: serde::de::DeserializeOwned,
    {
        let url = self.vtn_base_url.join(path)?;
        let request = self.client.request_builder(Method::POST, url).json(body);
        self.request(request, &[]).await
    }

    async fn put<S, T>(&self, path: &str, body: &S) -> Result<T>
    where
        S: serde::ser::Serialize + Sync,
        T: serde::de::DeserializeOwned,
    {
        let url = self.vtn_base_url.join(path)?;
        let request = self.client.request_builder(Method::PUT, url).json(body);
        self.request(request, &[]).await
    }

    async fn delete<T>(&self, path: &str) -> Result<T>
    where
        T: serde::de::DeserializeOwned,
    {
        let url = self.vtn_base_url.join(path)?;
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

/// Filter based on Target as specified for various items.
///
/// **Please note:** This does only filter based on what is stored in the `target` field of an item
/// (e.g., [`ProgramRequest::targets`]) and should not get interpreted by the server.
///
/// Unfortunately, the specification is not very clear about this behavior,
/// so some servers might interpret it differently.
/// There has been some discussion with the authors of the standard in
/// <https://github.com/oadr3-org/openadr3-vtn-reference-implementation/issues/83> (sadly not public).
#[derive(Debug, Clone)]
pub enum Filter<'a, S: AsRef<str>> {
    /// Do not apply any filtering
    None,
    /// Filter a list of [`Target`](openleadr_wire::target::Target).
    ///
    /// It will be encoded to the request as query parameters,
    /// e.g., `/programs?targets=group-1&targets=group-2`.
    By(&'a [S]),
}

impl<'a> Filter<'a, &'static str> {
    /// Create a new filter that does not apply any filtering.
    pub const fn none() -> Filter<'a, &'static str> {
        Filter::None
    }
}

impl<'a, S: AsRef<str>> Filter<'a, S> {
    pub(crate) fn to_query_params(&'a self) -> Vec<(&'a str, &'a str)> {
        let mut query = vec![];
        if let Filter::By(target_values) = self {
            for target_value in *target_values {
                query.push(("targets", target_value.as_ref()));
            }
        }
        query
    }
}

impl Client<VirtualEndNode> {
    /// Create a new VEN entity at the VTN.
    pub async fn create_ven(&self, ven: VenVenRequest) -> Result<VenClient<VirtualEndNode>> {
        let ven = self
            .client_ref
            .post("vens", &VenRequest::VenVenRequest(ven))
            .await?;
        Ok(VenClient::from_ven(self.client_ref.clone(), ven))
    }
}

impl Client<BusinessLogic> {
    /// Create a new VEN entity at the VTN.
    pub async fn create_ven(&self, ven: BlVenRequest) -> Result<VenClient<BusinessLogic>> {
        let ven = self
            .client_ref
            .post("vens", &VenRequest::BlVenRequest(ven))
            .await?;
        Ok(VenClient::from_ven(self.client_ref.clone(), ven))
    }
}

impl<K: ClientKind> Client<K> {
    /// Create a new client for a VTN located at the specified URL.
    ///
    /// This assumes that the VTN also works as an OAuth provider
    /// and exposes an API endpoint at `<base_url>/auth/token` to retrieve a token.
    /// If you want to use another OAuth provider, please use [`Self::with_details`].
    pub fn with_url(base_url: Url, auth: Option<ClientCredentials>) -> Self {
        let client = reqwest::Client::new();
        let oauth_base_url = base_url.join("/auth/token").unwrap();
        Self::with_details(base_url, oauth_base_url, client, auth)
    }

    /// Create a new client with more details than [`Self::with_url`].
    ///
    /// It allows specifying a [`reqwest::Client`] instead of the default one.
    /// This allows configuring proxy settings, timeouts, etc.
    ///
    /// Additionally, it allows for a separate `oauth_base_url`,
    /// which is relevant if you don't want or cannot rely on an OAuth provider at the default URL.
    pub fn with_details(
        vtn_base_url: Url,
        oauth_base_url: Url,
        client: reqwest::Client,
        auth: Option<ClientCredentials>,
    ) -> Self {
        Self::with_http_client(
            vtn_base_url,
            oauth_base_url,
            Box::new(ReqwestClientRef { client }),
            auth,
        )
    }

    /// Create a new client with anything that implements the [`HttpClient`] trait.
    ///
    /// This is mainly helpful for the integration tests
    /// and should most likely not be used for other purposes.
    /// Please use [`Client::with_details`] for detailed HTTP client configuration.
    pub fn with_http_client(
        vtn_base_url: Url,
        oauth_base_url: Url,
        client: Box<dyn HttpClient + Send + Sync>,
        auth: Option<ClientCredentials>,
    ) -> Self {
        let client_ref = ClientRef {
            client,
            vtn_base_url,
            oauth_base_url,
            default_page_size: 50,
            auth_data: auth,
            auth_token: RwLock::new(None),
            phantom: PhantomData::<K>,
        };
        Self::new(client_ref)
    }

    fn new(client_ref: ClientRef<K>) -> Self {
        Client {
            client_ref: Arc::new(client_ref),
        }
    }

    /// Create a new program on the VTN.
    pub async fn create_program(
        &self,
        program_content: ProgramRequest,
    ) -> Result<ProgramClient<K>> {
        let program = self.client_ref.post("programs", &program_content).await?;
        Ok(ProgramClient::from_program(self.clone(), program))
    }

    /// Lowlevel operation that gets a list of programs from the VTN with the given query parameters
    pub async fn get_programs(
        &self,
        filter: Filter<'_, impl AsRef<str>>,
        pagination: PaginationOptions,
    ) -> Result<Vec<ProgramClient<K>>> {
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
    pub async fn get_program_list(
        &self,
        filter: Filter<'_, impl AsRef<str> + Clone>,
    ) -> Result<Vec<ProgramClient<K>>> {
        self.client_ref
            .iterate_pages(|skip, limit| {
                self.get_programs(filter.clone(), PaginationOptions { skip, limit })
            })
            .await
    }

    /// Get program by name from VTN.
    /// According to the spec, a [`program_name`](ProgramRequest::program_name) must be unique for the whole VTN instance.
    pub async fn get_program_by_name(&self, name: &str) -> Result<ProgramClient<K>> {
        let mut programs: Vec<Program> = self
            .client_ref
            .get("programs", &[("programName", name)])
            .await?;
        match programs[..] {
            [] => Err(Error::ObjectNotFound),
            [_] => Ok(ProgramClient::from_program(
                self.clone(),
                programs.remove(0),
            )),
            [..] => Err(Error::DuplicateObject),
        }
    }

    /// Get a program by id
    pub async fn get_program_by_id(&self, id: &ProgramId) -> Result<ProgramClient<K>> {
        let program = self
            .client_ref
            .get(&format!("programs/{}", id.as_str()), &[])
            .await?;

        Ok(ProgramClient::from_program(self.clone(), program))
    }

    /// Low-level operation that gets a list of events from the VTN with the given query parameters
    ///
    /// To automatically iterate pages, use [`self.get_event_list`](Self::get_event_list)
    pub async fn get_events(
        &self,
        program_id: Option<&ProgramId>,
        filter: Filter<'_, impl AsRef<str>>,
        pagination: PaginationOptions,
    ) -> Result<Vec<EventClient<K>>> {
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
        filter: Filter<'_, impl AsRef<str> + Clone>,
    ) -> Result<Vec<EventClient<K>>> {
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
    pub async fn get_event_by_id(&self, id: &EventId) -> Result<EventClient<K>> {
        let event = self
            .client_ref
            .get(&format!("events/{}", id.as_str()), &[])
            .await?;

        Ok(EventClient::from_event(self.client_ref.clone(), event))
    }

    async fn get_vens(
        &self,
        skip: usize,
        limit: usize,
        filter: Filter<'_, impl AsRef<str>>,
    ) -> Result<Vec<VenClient<K>>> {
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
    pub async fn get_ven_list(
        &self,
        filter: Filter<'_, impl AsRef<str> + Clone>,
    ) -> Result<Vec<VenClient<K>>> {
        self.client_ref
            .iterate_pages(|skip, limit| self.get_vens(skip, limit, filter.clone()))
            .await
    }

    /// Get VEN by id from VTN
    pub async fn get_ven_by_id(&self, id: &VenId) -> Result<VenClient<K>> {
        let ven = self
            .client_ref
            .get(&format!("vens/{}", id.as_str()), &[])
            .await?;
        Ok(VenClient::from_ven(self.client_ref.clone(), ven))
    }

    /// Get VEN by name from VTN.
    /// According to the spec, a [`ven_name`](BlVenRequest::ven_name) must be unique for the whole VTN instance.
    pub async fn get_ven_by_name(&self, name: &str) -> Result<VenClient<K>> {
        let mut vens: Vec<Ven> = self.client_ref.get("vens", &[("venName", name)]).await?;
        match vens[..] {
            [] => Err(Error::ObjectNotFound),
            [_] => Ok(VenClient::from_ven(self.client_ref.clone(), vens.remove(0))),
            [..] => Err(Error::DuplicateObject),
        }
    }

    /// Open an mqtt client
    pub async fn connect_mqtt(&self) -> Result<MqttClient<K>> {
        let notifier_data: NotifiersResponse = self.client_ref.get("notifiers", &[]).await?;
        let Some(mqtt_info) = notifier_data.mqtt else {
            return Err(Error::NotSupported);
        };
        let MqttNotifierAuthentication::Oauth2BearerToken { username } = mqtt_info.authentication
        else {
            return Err(Error::NotSupported);
        };

        let mut last_error = Error::NotSupported;
        for url in mqtt_info.uris {
            let mut client = match paho_mqtt::AsyncClient::new(url) {
                Ok(client) => client,
                Err(e) => {
                    last_error = e.into();
                    continue;
                }
            };

            // Best practice to open the stream before connecting
            let incoming = client.get_stream(None);

            let connection = client
                .connect(
                    ConnectOptionsBuilder::new()
                        .user_name(&username)
                        .password(
                            self.client_ref
                                .auth_token
                                .read()
                                .await
                                .as_ref()
                                .map(|inner| inner.token.as_bytes())
                                .unwrap_or_default(),
                        )
                        .finalize(),
                )
                .await;

            if let Err(e) = connection {
                last_error = e.into();
                continue;
            }

            let (messages, _) = broadcast::channel(MQTT_NOTIFICATION_BUFFER);
            let forward = messages.clone();

            tokio::spawn(async move {
                while let Ok(message) = incoming.recv().await {
                    // An error only means nobody is listening right now.
                    let _ = forward.send(message);
                }
            });

            return Ok(MqttClient {
                client_ref: self.client_ref.clone(),
                mqtt_client: client,
                messages,
                subscriptions: Arc::new(Mutex::new(HashMap::new())),
            });
        }
        Err(last_error)
    }
}

impl MqttClient<VirtualEndNode> {
    /// Internal operation that fetches the topic for uploading of new reports.
    async fn upload_topic(&self) -> Result<String> {
        let response: NotifierTopicsResponse = self
            .client_ref
            .get("notifiers/mqtt/topics/upload/report", &[])
            .await?;

        response.topics.create.ok_or(Error::NotSupported)
    }

    /// Low-level operation that gets a list of events from the VTN with the given query parameters
    ///
    /// To automatically iterate pages, use [`self.get_event_list`](Self::get_event_list)
    pub async fn get_events(
        &self,
        program_id: Option<&ProgramId>,
        filter: Filter<'_, impl AsRef<str>>,
        pagination: PaginationOptions,
    ) -> Result<Vec<MqttEventClient<VirtualEndNode>>> {
        let topic = self.upload_topic().await?;

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
            .map(|event| {
                MqttEventClient::from_event(topic.clone(), self.mqtt_client.clone(), event)
            })
            .collect())
    }

    /// Get all events from the VTN with the given query parameters.
    ///
    /// It automatically tries to iterate pages where necessary.
    pub async fn get_event_list(
        &self,
        program_id: Option<&ProgramId>,
        filter: Filter<'_, impl AsRef<str> + Clone>,
    ) -> Result<Vec<MqttEventClient<VirtualEndNode>>> {
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
    pub async fn get_event_by_id(&self, id: &EventId) -> Result<MqttEventClient<VirtualEndNode>> {
        let topic = self.upload_topic().await?;

        let event = self
            .client_ref
            .get(&format!("events/{}", id.as_str()), &[])
            .await?;

        Ok(MqttEventClient::from_event(
            topic,
            self.mqtt_client.clone(),
            event,
        ))
    }
}

/// The objects whose create, update, and delete notifications an [`MqttClient`]
/// can subscribe to.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum MqttSubscriptionTarget {
    /// All [`Program`]s
    Programs,
    /// A single [`Program`], identified by its [`ProgramId`]
    Program(ProgramId),
    /// All [`Event`]s
    Events,
    /// The [`Event`]s of the [`Program`] identified by its [`ProgramId`]
    ProgramEvents(ProgramId),
    /// All [`Report`](openleadr_wire::report::Report)s
    Reports,
    /// All [`Ven`]s
    Vens,
    /// A single [`Ven`], identified by its [`VenId`]
    Ven(VenId),
    /// All [`Resource`](openleadr_wire::resource::Resource)s
    Resources,
    /// All [`ResourceGroup`](openleadr_wire::resource_group::ResourceGroup)s
    ResourceGroups,
    /// The [`Event`]s of the [`Ven`] identified by its [`VenId`]
    VenEvents(VenId),
    /// The [`Program`]s of the [`Ven`] identified by its [`VenId`]
    VenPrograms(VenId),
    /// The [`Resource`](openleadr_wire::resource::Resource)s of the [`Ven`]
    /// identified by its [`VenId`]
    VenResources(VenId),
    /// The [`ResourceGroup`](openleadr_wire::resource_group::ResourceGroup)s of
    /// the [`Ven`] identified by its [`VenId`]
    VenResourceGroups(VenId),
}

impl MqttSubscriptionTarget {
    fn endpoint(&self) -> String {
        let path = match self {
            Self::Programs => "programs".to_string(),
            Self::Program(program_id) => format!("programs/{program_id}"),
            Self::Events => "events".to_string(),
            Self::ProgramEvents(program_id) => format!("programs/{program_id}/events"),
            Self::Reports => "reports".to_string(),
            Self::Vens => "vens".to_string(),
            Self::Ven(ven_id) => format!("vens/{ven_id}"),
            Self::Resources => "resources".to_string(),
            Self::ResourceGroups => "resource_groups".to_string(),
            Self::VenEvents(ven_id) => format!("vens/{ven_id}/events"),
            Self::VenPrograms(ven_id) => format!("vens/{ven_id}/programs"),
            Self::VenResources(ven_id) => format!("vens/{ven_id}/resources"),
            Self::VenResourceGroups(ven_id) => format!("vens/{ven_id}/resource_groups"),
        };
        format!("notifiers/mqtt/topics/{path}")
    }
}

impl<K: ClientKind> MqttClient<K> {
    /// Subscribes to MQTT notifications on the VTN, optionally filtered by
    /// `operation_filter`.
    ///
    /// The notifications themselves are delivered to every receiver created
    /// with [`notifications`](Self::notifications), mixed with those of all
    /// other subscriptions of this client.
    ///
    /// The VTN may publish one change on several topics (e.g., a targeted event
    /// on both the [`Events`](MqttSubscriptionTarget::Events) and the
    /// [`VenEvents`](MqttSubscriptionTarget::VenEvents) topic), so overlapping
    /// subscriptions can deliver duplicates. Use [`Notification::id`] to
    /// deduplicate.
    ///
    /// # Errors
    ///
    /// - [`Error::NotSupported`] if the VTN does not offer the requested topic
    /// - [`Error::MqttSubscriptionRejected`] if the broker refused the
    ///   subscription, typically because this client may not read the topic.
    pub async fn subscribe(
        &mut self,
        target: MqttSubscriptionTarget,
        operation_filter: Option<Operation>,
    ) -> Result<()> {
        let NotifierTopicsResponse { topics } =
            self.client_ref.get(&target.endpoint(), &[]).await?;

        let topic = match operation_filter {
            None => topics.all.ok_or(Error::NotSupported)?,
            Some(Operation::Create) => topics.create.ok_or(Error::NotSupported)?,
            Some(Operation::Update) => topics.update,
            Some(Operation::Delete) => topics.delete,
        };

        let response = self
            .mqtt_client
            .subscribe(topic.clone(), QoS::AtMostOnce)
            .await?;

        // With MQTT v3, a refused subscription is not an error but a granted QoS of 0x80.
        if response.subscribe_response().is_some_and(|qos| qos >= 0x80) {
            return Err(Error::MqttSubscriptionRejected { topic });
        }

        self.subscriptions
            .lock()
            .await
            .insert((target, operation_filter), topic);

        Ok(())
    }

    /// Removes the subscription previously made with [`subscribe`](Self::subscribe)
    /// for the same `target` and `operation_filter`.
    ///
    /// # Errors
    ///
    /// - [`Error::MqttNotSubscribed`] if this client has no subscription for
    ///   `target` and `operation_filter`
    /// - [`Error::Mqtt`] if the broker failed to process the unsubscribe request.
    ///   The subscription is kept, so the call can be retried.
    pub async fn unsubscribe(
        &self,
        target: MqttSubscriptionTarget,
        operation_filter: Option<Operation>,
    ) -> Result<()> {
        let key = (target, operation_filter);

        let topic = self
            .subscriptions
            .lock()
            .await
            .get(&key)
            .cloned()
            .ok_or(Error::MqttNotSubscribed)?;

        self.mqtt_client.unsubscribe(topic).await?;
        self.subscriptions.lock().await.remove(&key);
        Ok(())
    }

    /// Creates a receiver for the notifications of all subscriptions of this client.
    ///
    /// Each receiver gets every notification that arrives after it was created.
    /// Clones of this [`MqttClient`] share subscriptions and notifications.
    pub fn notifications(&self) -> MqttNotifications {
        MqttNotifications {
            receiver: self.messages.subscribe(),
        }
    }
}

/// Receives the notifications for all subscriptions of an [`MqttClient`].
///
/// Created by [`MqttClient::notifications`].
#[derive(Debug)]
pub struct MqttNotifications {
    receiver: broadcast::Receiver<Option<paho_mqtt::Message>>,
}

impl MqttNotifications {
    /// Waits for the next notification.
    ///
    /// Returns `None` once the [`MqttClient`] and everything created from it
    /// has been dropped.
    ///
    /// # Errors
    ///
    /// Errors do not end the stream; calling `recv` again continues with the
    /// next notification.
    ///
    /// - [`Error::Serde`] if a message is not a valid [`Notification`].
    /// - [`Error::MqttNotificationsLagged`] if this receiver fell behind and
    ///   notifications were dropped.
    /// - [`Error::Mqtt`] with [`paho_mqtt::Error::Disconnected`] if the
    ///   connection to the broker was lost. Subscriptions are not restored
    ///   automatically; reconnect with [`Client::connect_mqtt`] and subscribe
    ///   again.
    pub async fn recv(&mut self) -> Option<Result<Notification>> {
        match self.receiver.recv().await {
            Ok(Some(message)) => {
                Some(serde_json::from_slice(message.payload()).map_err(Into::into))
            }
            Ok(None) => Some(Err(Error::Mqtt(paho_mqtt::Error::Disconnected))),
            Err(broadcast::error::RecvError::Lagged(skipped)) => {
                Some(Err(Error::MqttNotificationsLagged(skipped)))
            }
            Err(broadcast::error::RecvError::Closed) => None,
        }
    }
}
