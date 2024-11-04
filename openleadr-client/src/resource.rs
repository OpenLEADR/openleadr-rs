use crate::{ClientRef, Result};
use chrono::{DateTime, Utc};
use openleadr_wire::{
    resource::{Resource, ResourceContent, ResourceId},
    ven::VenId,
};
use std::sync::Arc;

/// A client
/// for interacting with the data in a specific resource
/// stored as a child element of a VEN on the VTN.
///
/// To retrieve or create a resource, refer to the [`VenClient`](crate::VenClient).
#[derive(Debug)]
pub struct ResourceClient {
    client: Arc<ClientRef>,
    ven_id: VenId,
    data: Resource,
}

impl ResourceClient {
    pub(super) fn from_resource(client: Arc<ClientRef>, ven_id: VenId, resource: Resource) -> Self {
        Self {
            client,
            ven_id,
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
    pub fn content(&self) -> &ResourceContent {
        &self.data.content
    }

    /// Modify the data of the resource.
    /// Make sure to call [`update`](Self::update)
    /// after your modifications to store them on the VTN.
    pub fn content_mut(&mut self) -> &mut ResourceContent {
        &mut self.data.content
    }

    /// Stores any modifications made to the resource content at the VTN
    /// and refreshes the data stored locally with the returned VTN data
    pub async fn update(&mut self) -> Result<()> {
        self.data = self
            .client
            .put(
                &format!("vens/{}/resources/{}", self.ven_id, self.id()),
                &self.data.content,
            )
            .await?;
        Ok(())
    }

    /// Delete the resource from the VTN
    pub async fn delete(self) -> Result<Resource> {
        self.client
            .delete(&format!("vens/{}/resources/{}", self.ven_id, self.id()))
            .await
    }
}
