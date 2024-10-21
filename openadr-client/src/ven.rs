use crate::{resource::ResourceClient, ClientRef, Result};
use chrono::{DateTime, Utc};
use openadr_wire::{
    resource::{Resource, ResourceContent},
    ven::{VenContent, VenId},
    Ven,
};
use std::sync::Arc;

pub struct VenClient {
    client: Arc<ClientRef>,
    data: Ven,
}

impl VenClient {
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

    pub fn content_mut(&mut self) -> &mut VenContent {
        &mut self.data.content
    }

    pub async fn update(&mut self) -> Result<()> {
        self.data = self
            .client
            .put(&format!("vens/{}", self.id()), &self.data.content, &[])
            .await?;
        Ok(())
    }

    pub async fn delete(self) -> Result<Ven> {
        self.client
            .delete(&format!("vens/{}", self.id()), &[])
            .await
    }

    pub fn new_resource(&self, name: String) -> ResourceContent {
        ResourceContent {
            object_type: Some(Default::default()),
            resource_name: name,
            attributes: None,
            targets: None,
        }
    }

    pub async fn create_resource(&self, resource: ResourceContent) -> Result<ResourceClient> {
        let resource = self
            .client
            .post(&format!("vens/{}/resources", self.id()), &resource, &[])
            .await?;
        Ok(ResourceClient::from_resource(
            self.client.clone(),
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
}
