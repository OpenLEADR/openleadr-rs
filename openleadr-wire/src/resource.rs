use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, skip_serializing_none, DefaultOnNull};
use std::{fmt::Display, str::FromStr};
use validator::Validate;

use crate::{
    target::Target, values_map::ValuesMap, ven::VenId, ClientId, Identifier, IdentifierError,
};

/// A resource is an energy device or system subject to control by a VEN.
#[skip_serializing_none]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Validate)]
#[serde(rename_all = "camelCase")]
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
#[serde(rename_all = "camelCase", untagged)]
pub enum ResourceRequest {
    BlRequestRequest(BlResourceRequest),
    VenRequestRequest(VenResourceRequest),
}

impl Validate for ResourceRequest {
    fn validate(&self) -> Result<(), validator::ValidationErrors> {
        match self {
            ResourceRequest::BlRequestRequest(x) => x.validate(),
            ResourceRequest::VenRequestRequest(x) => x.validate(),
        }
    }
}

#[skip_serializing_none]
#[serde_as]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Validate)]
#[serde(
    rename_all = "camelCase",
    tag = "objectType",
    rename = "BL_RESOURCE_REQUEST"
)]
pub struct BlResourceRequest {
    #[serde(rename = "clientID")]
    pub client_id: ClientId,
    /// A list of targets.
    pub targets: Option<Vec<Target>>,
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
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Validate)]
#[serde(
    rename_all = "camelCase",
    tag = "objectType",
    rename = "VEN_RESOURCE_REQUEST"
)]
pub struct VenResourceRequest {
    /// User generated identifier, resource may be configured with identifier out-of-band.
    #[serde(deserialize_with = "crate::string_within_range_inclusive::<1, 128, _>")]
    pub resource_name: String,
    /// VTN provisioned on object creation based on the path, e.g., POST <>/ven/{venID}/resources.
    #[serde(rename = "venID")]
    pub ven_id: VenId,
    /// A list of valuesMap objects describing attributes.
    #[serde(default)]
    #[serde_as(deserialize_as = "DefaultOnNull")]
    pub attributes: Vec<ValuesMap>,
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
