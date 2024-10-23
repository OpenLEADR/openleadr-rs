//! Types used for the `program/` endpoint

use crate::{
    event::EventPayloadDescriptor, interval::IntervalPeriod, report::ReportPayloadDescriptor,
    target::TargetMap, Duration, IdentifierError,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;
use std::{fmt::Display, str::FromStr};
use validator::Validate;

use super::Identifier;

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
    pub content: ProgramContent,
}

#[skip_serializing_none]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Validate)]
#[serde(rename_all = "camelCase", tag = "objectType", rename = "PROGRAM")]
pub struct ProgramContent {
    /// Short name to uniquely identify program.
    #[serde(deserialize_with = "crate::string_within_range_inclusive::<1, 128, _>")]
    pub program_name: String,
    /// Long name of program for human readability.
    pub program_long_name: Option<String>,
    /// Short name of energy retailer providing the program.
    pub retailer_name: Option<String>,
    /// Long name of energy retailer for human readability.
    pub retailer_long_name: Option<String>,
    /// A program defined categorization.
    pub program_type: Option<String>,
    /// Alpha-2 code per ISO 3166-1.
    pub country: Option<String>,
    /// Coding per ISO 3166-2. E.g. state in US.
    pub principal_subdivision: Option<String>,
    /// duration in ISO 8601 format
    ///
    /// Number of hours different from UTC for the standard time applicable to the program.
    // TODO: aaaaaah why???
    pub time_zone_offset: Option<Duration>,
    pub interval_period: Option<IntervalPeriod>,
    /// A list of programDescriptions
    #[validate(nested)]
    pub program_descriptions: Option<Vec<ProgramDescription>>,
    /// True if events are fixed once transmitted.
    pub binding_events: Option<bool>,
    /// True if events have been adapted from a grid event.
    pub local_price: Option<bool>,
    /// A list of payloadDescriptors.
    pub payload_descriptors: Option<Vec<PayloadDescriptor>>,
    /// A list of valuesMap objects.
    pub targets: Option<TargetMap>,
}

impl ProgramContent {
    pub fn new(name: impl ToString) -> ProgramContent {
        ProgramContent {
            program_name: name.to_string(),
            program_long_name: Default::default(),
            retailer_name: Default::default(),
            retailer_long_name: Default::default(),
            program_type: Default::default(),
            country: Default::default(),
            principal_subdivision: Default::default(),
            time_zone_offset: Default::default(),
            interval_period: Default::default(),
            program_descriptions: Default::default(),
            binding_events: Default::default(),
            local_price: Default::default(),
            payload_descriptors: Default::default(),
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
                    "programLongName": "Residential Time of Use-A",
                    "retailerName": "ACME",
                    "retailerLongName": "ACME Electric Inc.",
                    "programType": "PRICING_TARIFF",
                    "country": "US",
                    "principalSubdivision": "CO",
                    "timeZoneOffset": "PT1H",
                    "intervalPeriod": {
                      "start": "2023-06-15T09:30:00Z",
                      "duration": "PT1H",
                      "randomizeStart": "PT1H"
                    },
                    "programDescriptions": null,
                    "bindingEvents": false,
                    "localPrice": false,
                    "payloadDescriptors": null,
                    "targets": null
                  }
                ]"#;

        let parsed = serde_json::from_str::<Programs>(example).unwrap();

        let expected = vec![Program {
            id: ProgramId("object-999".parse().unwrap()),
            created_date_time: "2023-06-15T09:30:00Z".parse().unwrap(),
            modification_date_time: "2023-06-15T09:30:00Z".parse().unwrap(),
            content: ProgramContent {
                program_name: "ResTOU".into(),
                program_long_name: Some("Residential Time of Use-A".into()),
                retailer_name: Some("ACME".into()),
                retailer_long_name: Some("ACME Electric Inc.".into()),
                program_type: Some("PRICING_TARIFF".into()),
                country: Some("US".into()),
                principal_subdivision: Some("CO".into()),
                time_zone_offset: Some(Duration::PT1H),
                interval_period: Some(IntervalPeriod {
                    start: "2023-06-15T09:30:00Z".parse().unwrap(),
                    duration: Some(Duration::PT1H),
                    randomize_start: Some(Duration::PT1H),
                }),
                program_descriptions: None,
                binding_events: Some(false),
                local_price: Some(false),
                payload_descriptors: None,
                targets: None,
            },
        }];

        assert_eq!(expected, parsed);
    }

    #[test]
    fn parses_minimal() {
        let example = r#"{"programName":"test"}"#;

        assert_eq!(
            serde_json::from_str::<ProgramContent>(example).unwrap(),
            ProgramContent {
                program_name: "test".to_string(),
                program_long_name: None,
                retailer_name: None,
                retailer_long_name: None,
                program_type: None,
                country: None,
                principal_subdivision: None,
                time_zone_offset: None,
                interval_period: None,
                program_descriptions: None,
                binding_events: None,
                local_price: None,
                payload_descriptors: None,
                targets: None,
            }
        );
    }
}
