use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, skip_serializing_none, DefaultOnNull};
use std::{fmt::Display, str::FromStr};
use validator::Validate;

use crate::{target::Target, values_map::ValuesMap, ven::VenId, Identifier, IdentifierError};

/// A resource is an energy device or system subject to control by a VEN.
#[skip_serializing_none]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Validate)]
#[serde(rename_all = "camelCase", tag = "objectType", rename = "RESOURCE")]
pub struct Resource {
    /// URL safe VTN assigned object ID.
    pub id: ResourceId,
    /// datetime in ISO 8601 format
    #[serde(with = "crate::serde_rfc3339")]
    pub created_date_time: DateTime<Utc>,
    /// datetime in ISO 8601 format
    #[serde(with = "crate::serde_rfc3339")]
    pub modification_date_time: DateTime<Utc>,
    #[serde(flatten)]
    #[validate(nested)]
    pub content: BlResourceRequest,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE", tag = "objectType")]
pub enum ResourceRequest {
    BlResourceRequest(BlResourceRequest),
    VenResourceRequest(VenResourceRequest),
}

impl Validate for ResourceRequest {
    fn validate(&self) -> Result<(), validator::ValidationErrors> {
        match self {
            ResourceRequest::BlResourceRequest(x) => x.validate(),
            ResourceRequest::VenResourceRequest(x) => x.validate(),
        }
    }
}

impl ResourceRequest {
    pub fn resource_name(&self) -> &str {
        match self {
            ResourceRequest::BlResourceRequest(r) => &r.resource_name,
            ResourceRequest::VenResourceRequest(r) => &r.resource_name,
        }
    }

    pub fn attributes(&self) -> Option<&[ValuesMap]> {
        match self {
            ResourceRequest::BlResourceRequest(r) => r.attributes.as_deref(),
            ResourceRequest::VenResourceRequest(r) => r.attributes.as_deref(),
        }
    }
}

#[skip_serializing_none]
#[serde_as]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Validate)]
#[serde(rename_all = "camelCase")]
pub struct BlResourceRequest {
    /// A list of targets.
    #[serde(default)]
    #[serde_as(deserialize_as = "DefaultOnNull")]
    pub targets: Vec<Target>,
    /// User generated identifier, resource may be configured with identifier out-of-band.
    #[serde(deserialize_with = "crate::string_within_range_inclusive::<1, 128, _>")]
    pub resource_name: String,
    /// VTN provisioned on object creation based on the path, e.g., POST <>/ven/{venID}/resources.
    #[serde(rename = "venID")]
    pub ven_id: VenId,
    /// A list of valuesMap objects describing attributes.
    pub attributes: Option<Vec<ValuesMap>>,
}

#[skip_serializing_none]
#[serde_as]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Validate)]
#[serde(rename_all = "camelCase")]
pub struct VenResourceRequest {
    /// User generated identifier, resource may be configured with identifier out-of-band.
    #[serde(deserialize_with = "crate::string_within_range_inclusive::<1, 128, _>")]
    pub resource_name: String,
    /// A list of valuesMap objects describing attributes.
    pub attributes: Option<Vec<ValuesMap>>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Hash, Eq)]
pub struct ResourceId(pub(crate) Identifier);

impl Display for ResourceId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl ResourceId {
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }

    pub fn new(identifier: &str) -> Option<Self> {
        Some(Self(identifier.parse().ok()?))
    }
}

impl FromStr for ResourceId {
    type Err = IdentifierError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(s.parse()?))
    }
}

#[cfg(test)]
mod test {
    use super::{BlResourceRequest, Resource, ResourceId, ResourceRequest, VenResourceRequest};
    use crate::ven::VenId;

    #[test]
    fn example_roundtrip() {
        let example = r#"
        {
          "id": "test-resource",
          "createdDateTime": "2023-06-15T09:30:00Z",
          "modificationDateTime": "2023-06-15T09:30:00Z",
          "resourceName": "RESOURCE_0999",
          "objectType": "RESOURCE",
          "clientID": "ven_client",
          "venID": "0",
          "targets": [
            "resource_0999"
          ]
        }"#;

        let parsed = serde_json::from_str::<Resource>(example).unwrap();

        let expected = Resource {
            id: ResourceId("test-resource".parse().unwrap()),
            created_date_time: "2023-06-15T09:30:00Z".parse().unwrap(),
            modification_date_time: "2023-06-15T09:30:00Z".parse().unwrap(),
            content: BlResourceRequest {
                attributes: None,
                targets: vec!["resource_0999".parse().unwrap()],
                resource_name: "RESOURCE_0999".to_string(),
                ven_id: VenId("0".parse().unwrap()),
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
        assert!(serialized.contains(r#""objectType": "RESOURCE""#));

        let parsed = serde_json::from_str::<Resource>(&serialized).unwrap();
        assert_eq!(expected, parsed);
    }

    #[test]
    fn request_discriminator() {
        let ven_request = r#"
        {
          "resourceName": "RESOURCE_0999",
          "objectType": "VEN_RESOURCE_REQUEST",
          "venID": "0"
        }"#;

        let parsed_ven_request = serde_json::from_str::<ResourceRequest>(ven_request).unwrap();

        assert_eq!(
            parsed_ven_request,
            ResourceRequest::VenResourceRequest(VenResourceRequest {
                resource_name: "RESOURCE_0999".to_string(),
                attributes: None,
            })
        );

        let bl_request = r#"
        {
          "resourceName": "RESOURCE_0999",
          "objectType": "BL_RESOURCE_REQUEST",
          "venID": "0",
          "clientID": "ven_client"
        }"#;

        let parsed_bl_request = serde_json::from_str::<ResourceRequest>(bl_request).unwrap();

        assert_eq!(
            parsed_bl_request,
            ResourceRequest::BlResourceRequest(BlResourceRequest {
                targets: vec![],
                resource_name: "RESOURCE_0999".to_string(),
                ven_id: VenId("0".parse().unwrap()),
                attributes: None,
            })
        );

        let ven_request_with_bl_fields = r#"
        {
          "resourceName": "RESOURCE_0999",
          "objectType": "VEN_RESOURCE_REQUEST",
          "venID": "0",
          "clientID": "ven_client"
        }"#;

        let parsed_ven_request_with_bl_fields =
            serde_json::from_str::<ResourceRequest>(ven_request_with_bl_fields).unwrap();

        assert_eq!(
            parsed_ven_request_with_bl_fields,
            ResourceRequest::VenResourceRequest(VenResourceRequest {
                resource_name: "RESOURCE_0999".to_string(),
                attributes: None,
            })
        );
    }
}
