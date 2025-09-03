use crate::{resource::ResourceClient, ClientRef, Error, Result};
use chrono::{DateTime, Utc};
use openleadr_wire::ven::BlVenRequest;
use openleadr_wire::{
    resource::{Resource, ResourceId, ResourceRequest},
    ven::VenId,
    Ven,
};
use std::sync::Arc;

/// A client for interacting with the data in a specific VEN and the resources contained in the VEN.
#[derive(Debug, Clone)]
pub struct VenClient {
    client: Arc<ClientRef>,
    data: Ven,
}

impl VenClient {
    pub(super) fn from_ven(client: Arc<ClientRef>, data: Ven) -> Self {
        Self { client, data }
    }

    /// Get the VEN ID
    pub fn id(&self) -> &VenId {
        &self.data.id
    }

    /// Get the time the VEN was created on the VTN
    pub fn created_date_time(&self) -> DateTime<Utc> {
        self.data.created_date_time
    }

    /// Get the time the VEN was last modified on the VTN
    pub fn modification_date_time(&self) -> DateTime<Utc> {
        self.data.modification_date_time
    }

    /// Read the content of the VEN
    pub fn content(&self) -> &BlVenRequest {
        &self.data.content
    }

    /// Modify the content of the VEN.
    /// Make sure to call [`update`](Self::update)
    /// after your modifications to store them on the VTN.
    pub fn content_mut(&mut self) -> &mut BlVenRequest {
        &mut self.data.content
    }

    /// Stores any modifications made to the VEN content at the VTN
    /// and refreshes the data stored locally with the returned VTN data
    pub async fn update(&mut self) -> Result<()> {
        self.data = self
            .client
            .put(&format!("vens/{}", self.id()), &self.data.content)
            .await?;
        Ok(())
    }

    /// Delete the VEN from the VTN.
    ///
    /// Depending on the VTN implementation,
    /// you may need to delete all associated resources before you can delete the VEN
    pub async fn delete(self) -> Result<Ven> {
        self.client.delete(&format!("vens/{}", self.id())).await
    }

    /// Create a resource as a child of this VEN
    pub async fn create_resource(&self, resource: ResourceRequest) -> Result<ResourceClient> {
        let resource = self
            .client
            .post(&format!("vens/{}/resources", self.id()), &resource)
            .await?;
        Ok(ResourceClient::from_resource(
            Arc::clone(&self.client),
            self.id().clone(),
            resource,
        ))
    }

    async fn get_resources_req(
        &self,
        resource_name: Option<&str>,
        skip: usize,
        limit: usize,
    ) -> Result<Vec<ResourceClient>> {
        let skip_str = skip.to_string();
        let limit_str = limit.to_string();

        let mut query: Vec<(&str, &str)> = vec![("skip", &skip_str), ("limit", &limit_str)];

        if let Some(resource_name) = resource_name {
            query.push(("resourceName", resource_name));
        }

        let resources: Vec<Resource> = self
            .client
            .get(&format!("/vens/{}/resources", self.id()), &query)
            .await?;
        Ok(resources
            .into_iter()
            .map(|resource| {
                ResourceClient::from_resource(Arc::clone(&self.client), self.id().clone(), resource)
            })
            .collect())
    }

    /// Get all resources stored as children of this VEN.
    ///
    /// The client automatically tries to iterate pages where necessary.
    pub async fn get_all_resources(
        &self,
        resource_name: Option<&str>,
    ) -> Result<Vec<ResourceClient>> {
        self.client
            .iterate_pages(|skip, limit| self.get_resources_req(resource_name, skip, limit))
            .await
    }

    /// Get a resource by its ID
    pub async fn get_resource_by_id(&self, id: &ResourceId) -> Result<ResourceClient> {
        let resource = self
            .client
            .get(&format!("vens/{}/resources/{}", self.id(), id), &[])
            .await?;
        Ok(ResourceClient::from_resource(
            Arc::clone(&self.client),
            self.id().clone(),
            resource,
        ))
    }

    /// Get VEN by name from VTN.
    /// According to the spec, a [`resource_name`](ResourceContent::resource_name) must be unique per VEN.
    pub async fn get_resource_by_name(&self, name: &str) -> Result<ResourceClient> {
        let mut resources: Vec<Resource> = self
            .client
            .get(
                &format!("vens/{}/resources", self.id()),
                &[("resourceName", name)],
            )
            .await?;
        match resources[..] {
            [] => Err(Error::ObjectNotFound),
            [_] => Ok(ResourceClient::from_resource(
                Arc::clone(&self.client),
                self.id().clone(),
                resources.remove(0),
            )),
            [..] => Err(Error::DuplicateObject),
        }
    }
}
