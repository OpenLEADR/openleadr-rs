use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;
use std::{fmt::Display, str::FromStr};
use utoipa::ToSchema;
use validator::Validate;

use crate::{values_map::ValuesMap, ven::VenId, Identifier, IdentifierError};

/// A resource is an energy device or system subject to control by a VEN.
#[skip_serializing_none]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Validate, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct Resource {
    /// URL safe VTN assigned object ID.
    #[schema(required = false)]
    pub id: ResourceId,
    /// datetime in ISO 8601 format
    #[schema(required = false, example = "2023-06-15T09:30:00Z")]
    #[serde(with = "crate::serde_rfc3339")]
    pub created_date_time: DateTime<Utc>,
    /// datetime in ISO 8601 format
    #[schema(required = false, example = "2023-06-15T09:30:00Z")]
    #[serde(with = "crate::serde_rfc3339")]
    pub modification_date_time: DateTime<Utc>,
    /// URL safe VTN assigned object ID.
    #[schema(required = false)]
    #[serde(rename = "venID")]
    pub ven_id: VenId,
    #[serde(flatten)]
    #[validate(nested)]
    pub content: ResourceContent,
}

#[skip_serializing_none]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Validate, ToSchema)]
#[serde(rename_all = "camelCase", tag = "objectType", rename = "RESOURCE")]
pub struct ResourceContent {
    /// User generated identifier, resource may be configured with identifier out-of-band.
    #[serde(deserialize_with = "crate::string_within_range_inclusive::<1, 128, _>")]
    pub resource_name: String,
    /// A list of valuesMap objects describing attributes.
    #[schema(nullable = false)]
    pub attributes: Option<Vec<ValuesMap>>,
    /// A list of valuesMap objects describing target criteria.
    #[schema(nullable = false)]
    pub targets: Option<Vec<ValuesMap>>,
}

/// Used as discriminator, e.g. notification.object
#[derive(Clone, Copy, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ObjectType {
    #[default]
    Resource,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Hash, Eq, ToSchema)]
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
