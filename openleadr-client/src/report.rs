use std::sync::Arc;

use openleadr_wire::{report::ReportContent, Report};

use crate::{error::Result, ClientRef};

#[derive(Debug)]
pub struct ReportClient {
    client: Arc<ClientRef>,
    data: Report,
}

impl ReportClient {
    pub(super) fn from_report(client: Arc<ClientRef>, report: Report) -> Self {
        Self {
            client,
            data: report,
        }
    }

    pub fn id(&self) -> &openleadr_wire::report::ReportId {
        &self.data.id
    }

    pub fn created_date_time(&self) -> &chrono::DateTime<chrono::Utc> {
        &self.data.created_date_time
    }

    pub fn modification_date_time(&self) -> &chrono::DateTime<chrono::Utc> {
        &self.data.modification_date_time
    }

    pub fn data(&self) -> &ReportContent {
        &self.data.content
    }

    pub fn data_mut(&mut self) -> &mut ReportContent {
        &mut self.data.content
    }

    /// Save any modifications of the report to the VTN
    pub async fn update(&mut self) -> Result<()> {
        let res = self
            .client
            .put(&format!("reports/{}", self.id()), &self.data.content)
            .await?;
        self.data = res;
        Ok(())
    }

    /// Delete the report from the VTN
    pub async fn delete(self) -> Result<()> {
        self.client.delete(&format!("reports/{}", self.id())).await
    }
}
