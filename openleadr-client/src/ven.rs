use crate::{
    resource::ResourceClient, BusinessLogic, ClientKind, ClientRef, Error, Result, VirtualEndNode,
};
use chrono::{DateTime, Utc};
use openleadr_wire::{
    resource::{BlResourceRequest, Resource, ResourceId, ResourceRequest, VenResourceRequest},
    target::Target,
    values_map::ValuesMap,
    ven::{BlVenRequest, VenId, VenRequest},
    ClientId, Ven,
};
use std::{fmt::Display, sync::Arc};

/// A client for interacting with the data in a specific VEN and the resources contained in the VEN.
#[derive(Debug, Clone)]
pub struct VenClient<K> {
    client: Arc<ClientRef<K>>,
    data: Ven,
}

impl VenClient<BusinessLogic> {
    /// Create a resource as a child of this VEN
    pub async fn create_resource<S: Display>(
        &self,
        name: S,
        attributes: Option<Vec<ValuesMap>>,
        client_id: ClientId,
        targets: Vec<Target>,
    ) -> Result<ResourceClient<BusinessLogic>> {
        let resource = self
            .client
            .post(
                "resources",
                &ResourceRequest::BlResourceRequest(BlResourceRequest {
                    client_id,
                    targets,
                    resource_name: name.to_string(),
                    ven_id: self.data.id.clone(),
                    attributes,
                }),
            )
            .await?;
        Ok(ResourceClient::from_resource(
            Arc::clone(&self.client),
            resource,
        ))
    }
}

impl VenClient<VirtualEndNode> {
    /// Create a resource as a child of this VEN
    pub async fn create_resource<S: Display>(
        &self,
        name: S,
        attributes: Option<Vec<ValuesMap>>,
    ) -> Result<ResourceClient<VirtualEndNode>> {
        let resource = self
            .client
            .post(
                "resources",
                &ResourceRequest::VenResourceRequest(VenResourceRequest {
                    resource_name: name.to_string(),
                    ven_id: self.data.id.clone(),
                    attributes,
                }),
            )
            .await?;
        Ok(ResourceClient::from_resource(
            Arc::clone(&self.client),
            resource,
        ))
    }
}

impl<K: ClientKind> VenClient<K> {
    pub(super) fn from_ven(client: Arc<ClientRef<K>>, data: Ven) -> Self {
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
            .put(
                &format!("vens/{}", self.id()),
                &VenRequest::BlVenRequest(self.data.content.clone()),
            )
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

    async fn get_resources_req(
        &self,
        resource_name: Option<&str>,
        skip: usize,
        limit: usize,
    ) -> Result<Vec<ResourceClient<K>>> {
        let skip_str = skip.to_string();
        let limit_str = limit.to_string();

        let mut query: Vec<(&str, &str)> = vec![("skip", &skip_str), ("limit", &limit_str)];

        if let Some(resource_name) = resource_name {
            query.push(("resourceName", resource_name));
        }

        let resources: Vec<Resource> = self.client.get("/resources", &query).await?;
        Ok(resources
            .into_iter()
            .map(|resource| ResourceClient::from_resource(Arc::clone(&self.client), resource))
            .collect())
    }

    /// Get all resources stored as children of this VEN.
    ///
    /// The client automatically tries to iterate pages where necessary.
    pub async fn get_all_resources(
        &self,
        resource_name: Option<&str>,
    ) -> Result<Vec<ResourceClient<K>>> {
        self.client
            .iterate_pages(|skip, limit| self.get_resources_req(resource_name, skip, limit))
            .await
    }

    /// Get a resource by its ID
    pub async fn get_resource_by_id(&self, id: &ResourceId) -> Result<ResourceClient<K>> {
        let resource = self.client.get(&format!("resources/{}", id), &[]).await?;
        Ok(ResourceClient::from_resource(
            Arc::clone(&self.client),
            resource,
        ))
    }

    /// Get VEN by name from VTN.
    /// According to the spec, a [`resource_name`](ResourceContent::resource_name) must be unique per VEN.
    pub async fn get_resource_by_name(&self, name: &str) -> Result<ResourceClient<K>> {
        let mut resources: Vec<Resource> = self
            .client
            .get("resources", &[("resourceName", name)])
            .await?;
        match resources[..] {
            [] => Err(Error::ObjectNotFound),
            [_] => Ok(ResourceClient::from_resource(
                Arc::clone(&self.client),
                resources.remove(0),
            )),
            [..] => Err(Error::DuplicateObject),
        }
    }
}
