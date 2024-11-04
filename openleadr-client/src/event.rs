use std::sync::Arc;

use crate::{
    error::{Error, Result},
    ClientRef, ReportClient,
};
use openleadr_wire::{event::EventContent, report::ReportContent, Event, Report};

/// Client to manage the data of a specific event and the reports contained in that event
///
/// Can be created by a [`ProgramClient`](crate::ProgramClient)
/// ```no_run
/// # use openleadr_client::{Client, Filter};
/// # use openleadr_wire::event::Priority;
/// let client = Client::with_url("https://your-vtn.com".try_into().unwrap(), None);
/// # tokio_test::block_on(async {
/// let program = client.get_program_by_id(&"program-1".parse().unwrap()).await.unwrap();
///
/// // retrieve all events in that specific program
/// let mut events = program.get_event_list(Filter::None).await.unwrap();
/// let mut event = events.remove(0);
///
/// // Set event priority to maximum
/// event.content_mut().priority = Priority::MAX;
/// event.update().await.unwrap()
/// # })
/// ```
#[derive(Debug)]
pub struct EventClient {
    client: Arc<ClientRef>,
    data: Event,
}

impl EventClient {
    pub(super) fn from_event(client: Arc<ClientRef>, event: Event) -> Self {
        Self {
            client,
            data: event,
        }
    }

    /// Get the id of the event
    pub fn id(&self) -> &openleadr_wire::event::EventId {
        &self.data.id
    }

    /// Get the time the event was created on the VTN
    pub fn created_date_time(&self) -> chrono::DateTime<chrono::Utc> {
        self.data.created_date_time
    }

    /// Get the time the event was last modified on the VTN
    pub fn modification_date_time(&self) -> chrono::DateTime<chrono::Utc> {
        self.data.modification_date_time
    }

    /// Read the data of the event
    pub fn content(&self) -> &EventContent {
        &self.data.content
    }

    /// Modify the data of the event.
    /// Make sure to call [`update`](Self::update)
    /// after your modifications to store them on the VTN
    pub fn content_mut(&mut self) -> &mut EventContent {
        &mut self.data.content
    }

    /// Stores any modifications made to the event content at the server
    /// and refreshes the locally stored data with the returned VTN data
    pub async fn update(&mut self) -> Result<()> {
        self.data = self
            .client
            .put(&format!("events/{}", self.id()), &self.data.content)
            .await?;
        Ok(())
    }

    /// Delete the event from the VTN
    pub async fn delete(self) -> Result<Event> {
        self.client.delete(&format!("events/{}", self.id())).await
    }

    /// Create a new report object
    pub fn new_report(&self, client_name: String) -> ReportContent {
        ReportContent {
            program_id: self.content().program_id.clone(),
            event_id: self.id().clone(),
            client_name,
            report_name: None,
            payload_descriptors: None,
            resources: vec![],
        }
    }

    /// Create a new report on the VTN.
    /// The content should be created with [`EventClient::new_report`]
    /// to automatically insert the correct program ID and event ID
    pub async fn create_report(&self, report_data: ReportContent) -> Result<ReportClient> {
        if report_data.program_id != self.content().program_id {
            return Err(Error::InvalidParentObject);
        }

        if &report_data.event_id != self.id() {
            return Err(Error::InvalidParentObject);
        }

        let report = self.client.post("events", &report_data).await?;
        Ok(ReportClient::from_report(self.client.clone(), report))
    }

    async fn get_reports_req(
        &self,
        client_name: Option<&str>,
        skip: usize,
        limit: usize,
    ) -> Result<Vec<ReportClient>> {
        let skip_str = skip.to_string();
        let limit_str = limit.to_string();

        let mut query = vec![
            ("programID", self.content().program_id.as_str()),
            ("eventID", self.id().as_str()),
            ("skip", &skip_str),
            ("limit", &limit_str),
        ];

        if let Some(client_name) = client_name {
            query.push(("clientName", client_name));
        }

        let reports: Vec<Report> = self.client.get("reports", &query).await?;
        Ok(reports
            .into_iter()
            .map(|report| ReportClient::from_report(self.client.clone(), report))
            .collect())
    }

    /// Get all reports from the VTN, possibly filtered by `client_name`, trying to paginate whenever possible
    pub async fn get_report_list(&self, client_name: Option<&str>) -> Result<Vec<ReportClient>> {
        self.client
            .iterate_pages(|skip, limit| self.get_reports_req(client_name, skip, limit))
            .await
    }
}
