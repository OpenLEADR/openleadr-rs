use crate::{resource::ResourceClient, ClientRef, Error, Result};
use chrono::{DateTime, Utc};
use openleadr_wire::{
    resource::{Resource, ResourceContent, ResourceId},
    ven::{VenContent, VenId},
    Ven,
};
use std::sync::Arc;

#[derive(Debug)]
pub struct VenClient {
    client: Arc<ClientRef>,
    data: Ven,
}

impl VenClient {
    pub(super) fn from_ven(client: Arc<ClientRef>, data: Ven) -> Self {
        Self { client, data }
    }

    pub fn id(&self) -> &VenId {
        &self.data.id
    }

    pub fn created_date_time(&self) -> DateTime<Utc> {
        self.data.created_date_time
    }

    pub fn modification_date_time(&self) -> DateTime<Utc> {
        self.data.modification_date_time
    }

    pub fn content(&self) -> &VenContent {
        &self.data.content
    }

    /// Modify the data of the VEN.
    /// Make sure to call [`update`](Self::update)
    /// after your modifications to store them on the VTN
    pub fn content_mut(&mut self) -> &mut VenContent {
        &mut self.data.content
    }

    /// Stores any modifications made to the VEN content at the server
    /// and refreshes the locally stored data with the returned VTN data
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

    pub async fn create_resource(&self, resource: ResourceContent) -> Result<ResourceClient> {
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

    pub async fn get_all_resources(
        &self,
        resource_name: Option<&str>,
    ) -> Result<Vec<ResourceClient>> {
        self.client
            .iterate_pages(|skip, limit| self.get_resources_req(resource_name, skip, limit))
            .await
    }

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
