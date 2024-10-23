use openadr_wire::{event::Priority, Program};

use crate::{
    error::{Error, Result},
    Client, EventClient, EventContent, Filter, PaginationOptions, ProgramContent, ProgramId,
    Timeline,
};

/// A client for interacting with the data in a specific program and the events
/// contained in the program.
#[derive(Debug)]
pub struct ProgramClient {
    client: Client,
    data: Program,
}

impl ProgramClient {
    pub(super) fn from_program(client: Client, program: Program) -> Self {
        Self {
            client,
            data: program,
        }
    }

    /// Get the id of the program
    pub fn id(&self) -> &ProgramId {
        &self.data.id
    }

    /// Get the time the program was created on the VTN
    pub fn created_date_time(&self) -> chrono::DateTime<chrono::Utc> {
        self.data.created_date_time
    }

    /// Get the time the program was last modified on the VTN
    pub fn modification_date_time(&self) -> chrono::DateTime<chrono::Utc> {
        self.data.modification_date_time
    }

    /// Read the data of the program
    pub fn content(&self) -> &ProgramContent {
        &self.data.content
    }

    /// Modify the data of the program, make sure to update the program on the
    /// VTN once your modifications are complete.
    pub fn content_mut(&mut self) -> &mut ProgramContent {
        &mut self.data.content
    }

    /// Save any modifications of the program to the VTN
    pub async fn update(&mut self) -> Result<()> {
        let res = self
            .client
            .client_ref
            .put(&format!("programs/{}", self.id()), &self.data.content, &[])
            .await?;
        self.data = res;
        Ok(())
    }

    /// Delete the program from the VTN
    pub async fn delete(self) -> Result<Program> {
        self.client
            .client_ref
            .delete(&format!("programs/{}", self.id()), &[])
            .await
    }

    /// Create a new event on the VTN
    pub async fn create_event(&self, event_data: EventContent) -> Result<EventClient> {
        if &event_data.program_id != self.id() {
            return Err(Error::InvalidParentObject);
        }
        let event = self
            .client
            .client_ref
            .post("events", &event_data, &[])
            .await?;
        Ok(EventClient::from_event(
            self.client.client_ref.clone(),
            event,
        ))
    }

    /// Create a new event object within the program
    pub fn new_event(&self) -> EventContent {
        EventContent {
            program_id: self.id().clone(),
            event_name: None,
            priority: Priority::UNSPECIFIED,
            targets: None,
            report_descriptors: None,
            payload_descriptors: None,
            interval_period: None,
            intervals: vec![],
        }
    }

    pub async fn get_events_request(
        &self,
        filter: Filter<'_>,
        pagination: PaginationOptions,
    ) -> Result<Vec<EventClient>> {
        self.client
            .get_events(Some(self.id()), filter, pagination)
            .await
    }

    /// Get a list of events from the VTN with the given query parameters
    pub async fn get_event_list(&self, filter: Filter<'_>) -> Result<Vec<EventClient>> {
        self.client.get_event_list(Some(self.id()), filter).await
    }

    pub async fn get_timeline(&mut self) -> Result<Timeline> {
        let events = self.get_event_list(Filter::None).await?;
        let events = events.iter().map(|e| e.content()).collect();
        Timeline::from_events(self.content(), events).ok_or(Error::InvalidInterval)
    }
}
