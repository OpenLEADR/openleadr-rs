use std::sync::Arc;

use crate::{
    error::{Error, Result},
    ClientRef, ReportClient,
};
use openadr_wire::{
    event::EventContent,
    report::{ReportContent, ReportObjectType},
    Event, Report,
};

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

    pub fn id(&self) -> &openadr_wire::event::EventId {
        &self.data.id
    }

    pub fn created_date_time(&self) -> chrono::DateTime<chrono::Utc> {
        self.data.created_date_time
    }

    pub fn modification_date_time(&self) -> chrono::DateTime<chrono::Utc> {
        self.data.modification_date_time
    }

    pub fn content(&self) -> &EventContent {
        &self.data.content
    }

    pub fn content_mut(&mut self) -> &mut EventContent {
        &mut self.data.content
    }

    /// Save any modifications of the event to the VTN
    pub async fn update(&mut self) -> Result<()> {
        let res = self
            .client
            .put(&format!("events/{}", self.id()), &self.data.content, &[])
            .await?;
        self.data = res;
        Ok(())
    }

    /// Delete the event from the VTN
    pub async fn delete(self) -> Result<Event> {
        self.client
            .delete(&format!("events/{}", self.id()), &[])
            .await
    }

    /// Create a new report object
    pub fn new_report(&self) -> ReportContent {
        ReportContent {
            object_type: Some(ReportObjectType::Report),
            program_id: self.content().program_id.clone(),
            event_id: self.id().clone(),
            client_name: "".to_string(),
            report_name: None,
            payload_descriptors: None,
            resources: vec![],
        }
    }

    /// Create a new report for the event
    pub async fn create_report(&self, report_data: ReportContent) -> Result<ReportClient> {
        if report_data.program_id != self.content().program_id {
            return Err(Error::InvalidParentObject);
        }

        if &report_data.event_id != self.id() {
            return Err(Error::InvalidParentObject);
        }

        let report = self.client.post("events", &report_data, &[]).await?;
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

    /// Get all reports from the VTN for a specific client, trying to paginate whenever possible
    pub async fn get_client_reports(&self, client_name: &str) -> Result<Vec<ReportClient>> {
        let page_size = self.client.default_page_size();
        let mut reports = vec![];
        let mut page = 0;
        loop {
            let received = self
                .get_reports_req(Some(client_name), page * page_size, page_size)
                .await?;
            let received_all = received.len() < page_size;
            for report in received {
                reports.push(report);
            }

            if received_all {
                break;
            } else {
                page += 1;
            }
        }

        Ok(reports)
    }

    /// Get all reports from the VTN, trying to paginate whenever possible
    pub async fn get_all_reports(&self) -> Result<Vec<ReportClient>> {
        let page_size = self.client.default_page_size();
        let mut reports = vec![];
        let mut page = 0;
        loop {
            let received = self
                .get_reports_req(None, page * page_size, page_size)
                .await?;
            let received_all = received.len() < page_size;
            for report in received {
                reports.push(report);
            }

            if received_all {
                break;
            } else {
                page += 1;
            }
        }

        Ok(reports)
    }
}
