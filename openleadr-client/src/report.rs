use std::sync::Arc;

use openleadr_wire::{report::ReportRequest, Report};

use crate::{error::Result, ClientKind, ClientRef};

/// Client to manage the data of a specific report
///
/// Can be created by a [`EventClient`](crate::EventClient)
/// ```no_run
/// # use openleadr_client::{Client, Filter, BusinessLogic};
/// # use openleadr_wire::event::Priority;
/// let client = Client::<BusinessLogic>::with_url("https://your-vtn.com".try_into().unwrap(), None);
/// # tokio_test::block_on(async {
/// let event = client.get_event_by_id(&"event-1".parse().unwrap()).await.unwrap();
///
/// // retrieve all reports in that specific event, optionally filtered by the client name
/// let mut reports = event.get_report_list(Some("client-name")).await.unwrap();
/// let mut report = reports.remove(0);
///
/// // change report name
/// report.content_mut().report_name = Some("new-report-name".to_string());
/// report.update().await.unwrap()
/// # })
/// ```
#[derive(Debug, Clone)]
pub struct ReportClient<K> {
    client: Arc<ClientRef<K>>,
    data: Report,
}

impl<K: ClientKind> ReportClient<K> {
    pub(super) fn from_report(client: Arc<ClientRef<K>>, report: Report) -> Self {
        Self {
            client,
            data: report,
        }
    }

    /// Get the id of the report
    pub fn id(&self) -> &openleadr_wire::report::ReportId {
        &self.data.id
    }

    /// Get the time the report was created on the VTN
    pub fn created_date_time(&self) -> &chrono::DateTime<chrono::Utc> {
        &self.data.created_date_time
    }

    /// Get the time the report was last modified on the VTN
    pub fn modification_date_time(&self) -> &chrono::DateTime<chrono::Utc> {
        &self.data.modification_date_time
    }

    /// Read the data of the report
    pub fn content(&self) -> &ReportRequest {
        &self.data.content
    }

    /// Modify the data of the report.
    /// Make sure to call [`update`](Self::update)
    /// after your modifications to store them on the VTN
    pub fn content_mut(&mut self) -> &mut ReportRequest {
        &mut self.data.content
    }

    /// Stores any modifications made to the report content at the server
    /// and refreshes the locally stored data with the returned VTN data
    pub async fn update(&mut self) -> Result<()> {
        let res = self
            .client
            .put(&format!("reports/{}", self.id()), &self.data.content)
            .await?;
        self.data = res;
        Ok(())
    }

    /// Delete the report from the VTN
    pub async fn delete(self) -> Result<Report> {
        self.client.delete(&format!("reports/{}", self.id())).await
    }
}
