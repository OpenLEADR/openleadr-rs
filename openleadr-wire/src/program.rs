//! Types used for the `program/` endpoint

use crate::{
    event::EventPayloadDescriptor, interval::IntervalPeriod, report::ReportPayloadDescriptor,
    target::Target, values_map::ValuesMap, Identifier, IdentifierError,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, skip_serializing_none, DefaultOnNull};
use std::{fmt::Display, str::FromStr};
use validator::Validate;

pub type Programs = Vec<Program>;

/// Provides program specific metadata from VTN to VEN.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Validate)]
#[serde(rename_all = "camelCase")]
pub struct Program {
    /// VTN provisioned on object creation.
    ///
    /// URL safe VTN assigned object ID.
    pub id: ProgramId,

    /// VTN provisioned on object creation.
    ///
    /// datetime in ISO 8601 format
    #[serde(with = "crate::serde_rfc3339")]
    pub created_date_time: DateTime<Utc>,

    /// VTN provisioned on object modification.
    ///
    /// datetime in ISO 8601 format
    #[serde(with = "crate::serde_rfc3339")]
    pub modification_date_time: DateTime<Utc>,

    #[serde(flatten)]
    #[validate(nested)]
    pub content: ProgramRequest,
}

#[skip_serializing_none]
#[serde_as]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Validate)]
#[serde(rename_all = "camelCase", tag = "objectType", rename = "PROGRAM")]
pub struct ProgramRequest {
    /// Short name to uniquely identify program.
    #[serde(deserialize_with = "crate::string_within_range_inclusive::<1, 128, _>")]
    pub program_name: String,
    /// The temporal span of the program, which could be years-long.
    pub interval_period: Option<IntervalPeriod>,
    /// A list of programDescriptions
    #[validate(nested)]
    pub program_descriptions: Option<Vec<ProgramDescription>>,
    /// A list of payloadDescriptors.
    pub payload_descriptors: Option<Vec<PayloadDescriptor>>,
    pub attributes: Option<Vec<ValuesMap>>,
    /// A list of targets.
    #[serde(default)]
    #[serde_as(deserialize_as = "DefaultOnNull")]
    pub targets: Vec<Target>,
}

impl ProgramRequest {
    pub fn new(name: impl ToString) -> ProgramRequest {
        ProgramRequest {
            program_name: name.to_string(),
            interval_period: Default::default(),
            program_descriptions: Default::default(),
            payload_descriptors: Default::default(),
            attributes: Default::default(),
            targets: Default::default(),
        }
    }
}

// example: object-999
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Hash, Eq)]
pub struct ProgramId(pub(crate) Identifier);

impl Display for ProgramId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl ProgramId {
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }

    pub fn new(identifier: &str) -> Option<Self> {
        Some(Self(identifier.parse().ok()?))
    }
}

impl FromStr for ProgramId {
    type Err = IdentifierError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(s.parse()?))
    }
}

#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize, Validate)]
pub struct ProgramDescription {
    /// A human or machine readable program description
    #[serde(rename = "URL")]
    #[validate(url)]
    pub url: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "objectType", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum PayloadDescriptor {
    EventPayloadDescriptor(EventPayloadDescriptor),
    ReportPayloadDescriptor(ReportPayloadDescriptor),
}

#[cfg(test)]
mod test {
    use crate::Duration;

    use super::*;

    #[test]
    fn example_parses() {
        let example = r#"[
                  {
                    "id": "object-999",
                    "createdDateTime": "2023-06-15T09:30:00Z",
                    "modificationDateTime": "2023-06-15T09:30:00Z",
                    "objectType": "PROGRAM",
                    "programName": "ResTOU",
                    "intervalPeriod": {
                      "start": "2023-06-15T09:30:00Z",
                      "duration": "PT1H",
                      "randomizeStart": "PT1H"
                    },
                    "programDescriptions": null,
                    "payloadDescriptors": null,
                    "attributes": null,
                    "targets": null
                  }
                ]"#;

        let parsed = serde_json::from_str::<Programs>(example).unwrap();

        let expected = vec![Program {
            id: ProgramId("object-999".parse().unwrap()),
            created_date_time: "2023-06-15T09:30:00Z".parse().unwrap(),
            modification_date_time: "2023-06-15T09:30:00Z".parse().unwrap(),
            content: ProgramRequest {
                program_name: "ResTOU".into(),
                interval_period: Some(IntervalPeriod {
                    start: "2023-06-15T09:30:00Z".parse().unwrap(),
                    duration: Some(Duration::PT1H),
                    randomize_start: Some(Duration::PT1H),
                }),
                program_descriptions: None,
                payload_descriptors: None,
                attributes: None,
                targets: vec![],
            },
        }];

        assert_eq!(expected, parsed);
    }

    #[test]
    fn parses_minimal() {
        let example = r#"{"programName":"test"}"#;

        assert_eq!(
            serde_json::from_str::<ProgramRequest>(example).unwrap(),
            ProgramRequest {
                program_name: "test".to_string(),
                interval_period: None,
                program_descriptions: None,
                payload_descriptors: None,
                attributes: None,
                targets: vec![],
            }
        );
    }
}
