use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, skip_serializing_none, DefaultOnNull};
use std::{fmt::Display, str::FromStr};
use validator::Validate;

use crate::{target::Target, values_map::ValuesMap, ClientId, Identifier, IdentifierError};

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
    pub content: BlVenRequest,
}

#[skip_serializing_none]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE", tag = "objectType")]
pub enum VenRequest {
    BlVenRequest(BlVenRequest),
    VenVenRequest(VenVenRequest),
}

impl Validate for VenRequest {
    fn validate(&self) -> Result<(), validator::ValidationErrors> {
        match self {
            VenRequest::BlVenRequest(x) => x.validate(),
            VenRequest::VenVenRequest(x) => x.validate(),
        }
    }
}

impl VenRequest {
    pub fn client_id(&self) -> Option<&ClientId> {
        match self {
            VenRequest::BlVenRequest(r) => Some(&r.client_id),
            VenRequest::VenVenRequest(_) => None,
        }
    }

    pub fn ven_name(&self) -> &str {
        match self {
            VenRequest::BlVenRequest(r) => &r.ven_name,
            VenRequest::VenVenRequest(r) => &r.ven_name,
        }
    }

    pub fn attributes(&self) -> Option<&[ValuesMap]> {
        match self {
            VenRequest::BlVenRequest(r) => r.attributes.as_deref(),
            VenRequest::VenVenRequest(r) => r.attributes.as_deref(),
        }
    }

    pub fn targets(&self) -> &[Target] {
        match self {
            VenRequest::BlVenRequest(r) => &r.targets,
            VenRequest::VenVenRequest(_) => {
                // FIXME object privacy
                &[]
            }
        }
    }
}

#[skip_serializing_none]
#[serde_as]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Validate)]
#[serde(rename_all = "camelCase")]
pub struct BlVenRequest {
    #[serde(rename = "clientID")]
    pub client_id: ClientId,
    /// A list of targets.
    #[serde(default)]
    #[serde_as(deserialize_as = "DefaultOnNull")]
    pub targets: Vec<Target>,
    /// User generated identifier, may be VEN identifier provisioned during program enrollment.
    #[serde(deserialize_with = "crate::string_within_range_inclusive::<1, 128, _>")]
    pub ven_name: String,
    /// A list of valuesMap objects describing attributes.
    pub attributes: Option<Vec<ValuesMap>>,
}

#[skip_serializing_none]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Validate)]
#[serde(rename_all = "camelCase")]
pub struct VenVenRequest {
    /// User generated identifier, may be VEN identifier provisioned during program enrollment.
    #[serde(deserialize_with = "crate::string_within_range_inclusive::<1, 128, _>")]
    pub ven_name: String,
    /// A list of valuesMap objects describing attributes.
    pub attributes: Option<Vec<ValuesMap>>,
}

impl BlVenRequest {
    pub fn new(
        client_id: ClientId,
        ven_name: String,
        attributes: Option<Vec<ValuesMap>>,
        targets: Vec<Target>,
    ) -> Self {
        Self {
            client_id,
            ven_name,
            attributes,
            targets,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Hash, Eq, PartialOrd, Ord)]
#[serde(transparent)]
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
