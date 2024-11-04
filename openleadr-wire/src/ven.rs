use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;
use std::{fmt::Display, str::FromStr};
use utoipa::{IntoParams, ToSchema};
use validator::Validate;

use crate::{
    resource::Resource, target::TargetMap, values_map::ValuesMap, Identifier, IdentifierError,
};

/// Ven represents a client with the ven role.
#[skip_serializing_none]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Validate)]
#[serde(rename_all = "camelCase")]
pub struct Ven {
    /// URL safe VTN assigned object ID.
    pub id: VenId,
    /// datetime in ISO 8601 format
    #[serde(with = "crate::serde_rfc3339")]
    pub created_date_time: DateTime<Utc>,
    /// datetime in ISO 8601 format
    #[serde(with = "crate::serde_rfc3339")]
    pub modification_date_time: DateTime<Utc>,

    #[serde(flatten)]
    #[validate(nested)]
    pub content: VenContent,
}

#[skip_serializing_none]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Validate)]
#[serde(rename_all = "camelCase", tag = "objectType", rename = "VEN")]
pub struct VenContent {
    /// User generated identifier, may be VEN identifier provisioned during program enrollment.
    #[serde(deserialize_with = "crate::string_within_range_inclusive::<1, 128, _>")]
    pub ven_name: String,
    /// A list of valuesMap objects describing attributes.
    pub attributes: Option<Vec<ValuesMap>>,
    /// A list of valuesMap objects describing target criteria.
    pub targets: Option<TargetMap>,
    /// A list of resource objects representing end-devices or systems.
    resources: Option<Vec<Resource>>,
}

impl VenContent {
    pub fn new(
        ven_name: String,
        attributes: Option<Vec<ValuesMap>>,
        targets: Option<TargetMap>,
        resources: Option<Vec<Resource>>,
    ) -> Self {
        Self {
            ven_name,
            attributes,
            targets,
            resources,
        }
    }

    pub fn resources(&self) -> Option<&[Resource]> {
        self.resources.as_deref()
    }
}

/// Numeric ID of ven.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Hash, Eq, PartialOrd, Ord, ToSchema, IntoParams)]
#[into_params(names("ven_id"))]
pub struct VenId(pub(crate) Identifier);

impl Display for VenId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl VenId {
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }

    pub fn new(identifier: &str) -> Option<Self> {
        Some(Self(identifier.parse().ok()?))
    }
}

impl FromStr for VenId {
    type Err = IdentifierError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(s.parse()?))
    }
}
