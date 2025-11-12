use crate::{ClientKind, ClientRef, Result};
use chrono::{DateTime, Utc};
use openleadr_wire::resource::{BlResourceRequest, Resource, ResourceId, ResourceRequest};
use std::sync::Arc;

/// A client
/// for interacting with the data in a specific resource
/// stored as a child element of a VEN on the VTN.
///
/// To retrieve or create a resource, refer to the [`VenClient`](crate::VenClient).
#[derive(Debug, Clone)]
pub struct ResourceClient<K> {
    client: Arc<ClientRef<K>>,
    data: Resource,
}

impl<K: ClientKind> ResourceClient<K> {
    pub(super) fn from_resource(client: Arc<ClientRef<K>>, resource: Resource) -> Self {
        Self {
            client,
            data: resource,
        }
    }

    /// Get the resource ID
    pub fn id(&self) -> &ResourceId {
        &self.data.id
    }

    /// Get the time the resource was created on the VTN
    pub fn created_date_time(&self) -> DateTime<Utc> {
        self.data.created_date_time
    }

    /// Get the time the resource was last updated on the VTN
    pub fn modification_date_time(&self) -> DateTime<Utc> {
        self.data.modification_date_time
    }

    /// Read the content of the resource
    pub fn content(&self) -> &BlResourceRequest {
        &self.data.content
    }

    /// Modify the data of the resource.
    /// Make sure to call [`update`](Self::update)
    /// after your modifications to store them on the VTN.
    pub fn content_mut(&mut self) -> &mut BlResourceRequest {
        &mut self.data.content
    }

    /// Stores any modifications made to the resource content at the VTN
    /// and refreshes the data stored locally with the returned VTN data
    pub async fn update(&mut self) -> Result<()> {
        self.data = self
            .client
            .put(
                &format!("resources/{}", self.id()),
                &ResourceRequest::BlResourceRequest(self.data.content.clone()),
            )
            .await?;
        Ok(())
    }

    /// Delete the resource from the VTN
    pub async fn delete(self) -> Result<Resource> {
        self.client
            .delete(&format!("resources/{}", self.id()))
            .await
    }
}
