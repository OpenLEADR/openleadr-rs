use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, skip_serializing_none, DefaultOnNull};
use std::{fmt::Display, str::FromStr};
use validator::Validate;

use crate::{
    resource::Resource, target::Target, values_map::ValuesMap, Identifier, IdentifierError,
};

/// A resource group may contain either one or more nested resource groups or one or more VEN
/// resources, managed by the BL
#[skip_serializing_none]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Validate)]
#[serde(
    rename_all = "camelCase",
    tag = "objectType",
    rename = "RESOURCE_GROUP"
)]
pub struct ResourceGroup {
    // TODO: Is this still VTN assigned?
    /// URL safe VTN assigned object ID.
    pub id: ResourceGroupId,
    /// datetime in ISO 8601 format
    #[serde(with = "crate::serde_rfc3339")]
    pub created_date_time: DateTime<Utc>,
    /// datetime in ISO 8601 format
    #[serde(with = "crate::serde_rfc3339")]
    pub modification_date_time: DateTime<Utc>,

    // TODO: Unsure if there should also be a VENResourceRequest, like for regular resources
    #[serde(flatten)]
    #[validate(nested)]
    pub content: BlResourceGroupRequest,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum ResourceGroupChild {
    ResourceGroup(ResourceGroup),
    VENResource(Resource),
}

#[skip_serializing_none]
#[serde_as]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Validate)]
#[serde(rename_all = "camelCase")]
pub struct BlResourceGroupRequest {
    /// A list of targets.
    #[serde(default)]
    #[serde_as(deserialize_as = "DefaultOnNull")]
    pub targets: Vec<Target>,
    /// User generated identifier, resource may be configured with identifier out-of-band.
    #[serde(deserialize_with = "crate::string_within_range_inclusive::<1, 128, _>")]
    pub resource_group_name: String,
    /// A list of valuesMap objects describing attributes.
    pub attributes: Option<Vec<ValuesMap>>,

    pub children: Vec<ResourceGroupChild>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Hash, Eq)]
pub struct ResourceGroupId(pub(crate) Identifier);

impl Display for ResourceGroupId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl ResourceGroupId {
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }

    pub fn new(identifier: &str) -> Option<Self> {
        Some(Self(identifier.parse().ok()?))
    }
}

impl FromStr for ResourceGroupId {
    type Err = IdentifierError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(s.parse()?))
    }
}

#[cfg(test)]
mod test {
    use super::{BlResourceGroupRequest, ResourceGroup, ResourceGroupId};

    #[test]
    fn example_roundtrip() {
        let example = r#"
        {
          "id": "test-resource-group",
          "createdDateTime": "2023-06-15T09:30:00Z",
          "modificationDateTime": "2023-06-15T09:30:00Z",
          "resourceGroupName": "RESOURCE_GROUP_0999",
          "objectType": "RESOURCE_GROUP",
          "targets": [
            "resource_group_0999"
          ],
          "children": []
        }"#;

        let parsed = serde_json::from_str::<ResourceGroup>(example).unwrap();

        let expected = ResourceGroup {
            id: ResourceGroupId("test-resource-group".parse().unwrap()),
            created_date_time: "2023-06-15T09:30:00Z".parse().unwrap(),
            modification_date_time: "2023-06-15T09:30:00Z".parse().unwrap(),
            content: BlResourceGroupRequest {
                attributes: None,
                targets: vec!["resource_group_0999".parse().unwrap()],
                resource_group_name: "RESOURCE_GROUP_0999".to_string(),
                children: vec![],
            },
        };

        assert_eq!(expected, parsed);

        let serialized = serde_json::to_string_pretty(&expected).unwrap();

        // Make sure the "objectType" discriminator is only printed once
        let num_object_type = serialized
            .as_bytes()
            .windows("objectType".len())
            .filter(|&w| w == "objectType".as_bytes())
            .count();

        assert_eq!(num_object_type, 1);

        // Make sure the correct discriminator is printed
        assert!(serialized.contains(r#""objectType": "RESOURCE_GROUP""#));

        let parsed = serde_json::from_str::<ResourceGroup>(&serialized).unwrap();
        assert_eq!(expected, parsed);
    }

    // TODO: BL_RESOURCE_GROUP_REQUEST as discriminator?
    // TODO: Unsure if there should also be a VENResourceRequest, like for regular resources
    #[test]
    fn request_discriminator() {
        let bl_request = r#"
        {
          "resourceGroupName": "RESOURCE_0999",
          "objectType": "BL_RESOURCE_GROUP_REQUEST",
          "targets": []
        }"#;

        let parsed_bl_request = serde_json::from_str::<BlResourceGroupRequest>(bl_request).unwrap();

        assert_eq!(
            parsed_bl_request,
            BlResourceGroupRequest {
                targets: vec![],
                resource_group_name: "RESOURCE_0999".to_string(),
                attributes: None,
                children: vec![],
            }
        );
    }
}
