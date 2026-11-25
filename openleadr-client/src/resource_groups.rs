use crate::{BusinessLogic, ClientKind, ClientRef, Result};
use chrono::{DateTime, Utc};
use openleadr_wire::resource_group::{BlResourceGroupRequest, ResourceGroup, ResourceGroupId};
use std::sync::Arc;

/// A client for interacting with the data in a specific resource group on the VTN.
///
/// To retrieve a resource group, refer to the [`Client`](crate::Client). Only the business
/// logic can create, update, or delete resource groups, see
/// [`Client::create_resource_group`](crate::Client::create_resource_group).
#[derive(Debug, Clone)]
pub struct ResourceGroupClient<K> {
    client: Arc<ClientRef<K>>,
    data: ResourceGroup,
}

impl<K: ClientKind> ResourceGroupClient<K> {
    pub(super) fn from_resource_group(
        client: Arc<ClientRef<K>>,
        resource_group: ResourceGroup,
    ) -> Self {
        Self {
            client,
            data: resource_group,
        }
    }

    /// Get the resource_group ID
    pub fn id(&self) -> &ResourceGroupId {
        &self.data.id
    }

    /// Get the time the resource_group was created on the VTN
    pub fn created_date_time(&self) -> DateTime<Utc> {
        self.data.created_date_time
    }

    /// Get the time the resource_group was last updated on the VTN
    pub fn modification_date_time(&self) -> DateTime<Utc> {
        self.data.modification_date_time
    }

    /// Read the content of the resource_group
    pub fn content(&self) -> &BlResourceGroupRequest {
        &self.data.content
    }

    /// Modify the data of the resource_group.
    /// Make sure to call [`update`](Self::update)
    /// after your modifications to store them on the VTN.
    pub fn content_mut(&mut self) -> &mut BlResourceGroupRequest {
        &mut self.data.content
    }
}

impl ResourceGroupClient<BusinessLogic> {
    /// Stores any modifications made to the resource_group content at the VTN
    /// and refreshes the data stored locally with the returned VTN data
    pub async fn update(&mut self) -> Result<()> {
        self.data = self
            .client
            .put(
                &format!("resource_groups/{}", self.id()),
                &self.data.content,
            )
            .await?;
        Ok(())
    }

    /// Delete the resource_group from the VTN
    pub async fn delete(self) -> Result<ResourceGroup> {
        self.client
            .delete(&format!("resource_groups/{}", self.id()))
            .await
    }
}
