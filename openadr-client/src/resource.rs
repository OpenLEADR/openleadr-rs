use crate::{ClientRef, Result};
use chrono::{DateTime, Utc};
use openadr_wire::{
    resource::{Resource, ResourceContent, ResourceId},
    ven::VenId,
};
use std::sync::Arc;

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

    pub fn id(&self) -> &ResourceId {
        &self.data.id
    }

    pub fn created_date_time(&self) -> DateTime<Utc> {
        self.data.created_date_time
    }

    pub fn modification_date_time(&self) -> DateTime<Utc> {
        self.data.modification_date_time
    }

    pub fn content(&self) -> &ResourceContent {
        &self.data.content
    }

    pub fn content_mut(&mut self) -> &mut ResourceContent {
        &mut self.data.content
    }

    /// Save any modifications of the resource to the VTN
    pub async fn update(&mut self) -> Result<()> {
        self.data = self
            .client
            .put(
                &format!("vens/{}/resources/{}", self.ven_id, self.id()),
                &self.data.content,
                &[],
            )
            .await?;
        Ok(())
    }

    /// Delete the resource from the VTN
    pub async fn delete(self) -> Result<Resource> {
        self.client
            .delete(
                &format!("vens/{}/resources/{}", self.ven_id, self.id()),
                &[],
            )
            .await
    }
}
