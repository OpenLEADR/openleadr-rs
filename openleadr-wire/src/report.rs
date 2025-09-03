//! Types used for the `report/` endpoint

use crate::{
    event::EventId,
    interval::{Interval, IntervalPeriod},
    target::Target,
    ClientId, Identifier, IdentifierError, Unit,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;
use std::{
    fmt::{Display, Formatter},
    str::FromStr,
};
use validator::{Validate, ValidateRange};

/// report object.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Validate)]
#[serde(rename_all = "camelCase")]
pub struct Report {
    /// URL safe VTN assigned object ID.
    pub id: ReportId,
    /// datetime in ISO 8601 format
    #[serde(with = "crate::serde_rfc3339")]
    pub created_date_time: DateTime<Utc>,
    /// datetime in ISO 8601 format
    #[serde(with = "crate::serde_rfc3339")]
    pub modification_date_time: DateTime<Utc>,
    #[serde(flatten)]
    #[validate(nested)]
    pub content: ReportRequest,
    #[serde(rename = "clientID")]
    pub client_id: ClientId,
}

#[skip_serializing_none]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Validate)]
#[serde(rename_all = "camelCase", tag = "objectType", rename = "REPORT")]
pub struct ReportRequest {
    /// ID attribute of the event object this report is associated with.
    #[serde(rename = "eventID")]
    pub event_id: EventId,
    /// User generated identifier; may be VEN ID provisioned during program enrollment.
    #[serde(deserialize_with = "crate::string_within_range_inclusive::<1, 128, _>")]
    pub client_name: String,
    /// User defined string for use in debugging or User Interface.
    pub report_name: Option<String>,
    /// A list of reportPayloadDescriptors.
    ///
    /// An optional list of objects that provide context to payload types.
    #[validate(nested)]
    pub payload_descriptors: Option<Vec<ReportPayloadDescriptor>>,
    /// A list of objects containing report data for a set of resources.
    pub resources: Vec<ReportResource>,
}

impl ReportRequest {
    pub fn with_client_name(mut self, client_name: &str) -> Self {
        self.client_name = client_name.to_string();
        self
    }

    pub fn with_name(mut self, name: &str) -> Self {
        self.report_name = Some(name.to_string());
        self
    }

    pub fn with_payload_descriptors(mut self, descriptors: Vec<ReportPayloadDescriptor>) -> Self {
        self.payload_descriptors = Some(descriptors);
        self
    }

    pub fn with_resources(mut self, resources: Vec<ReportResource>) -> Self {
        self.resources = resources;
        self
    }
}

/// URL safe VTN assigned object ID
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Hash, Eq)]
pub struct ReportId(pub(crate) Identifier);

impl ReportId {
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl Display for ReportId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl FromStr for ReportId {
    type Err = IdentifierError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(s.parse()?))
    }
}

/// Report data associated with a resource.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReportResource {
    /// User generated identifier. A value of AGGREGATED_REPORT indicates an aggregation of more
    /// that one resource's data
    pub resource_name: ResourceName,
    /// Defines default start and durations of intervals.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub interval_period: Option<IntervalPeriod>,
    /// A list of interval objects.
    pub intervals: Vec<Interval>,
}

/// An object that may be used to request a report from a VEN.
// TODO: replace "-1 means" with proper enum
#[skip_serializing_none]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReportDescriptor {
    /// Represents the nature of values.
    ///
    /// See enumerations in Definitions for defined string values, or use privately defined strings
    pub payload_type: ReportType,
    /// Enumerated or private string signifying the type of reading.
    pub reading_type: Option<ReadingType>,
    /// Units of measure.
    pub units: Option<Unit>,
    /// A list of targets.
    pub targets: Option<Vec<Target>>,
    /// True if report should aggregate results from all targeted resources. False if report includes results for each resource.
    #[serde(default = "bool_false")]
    pub aggregate: bool,
    /// The interval on which to generate a report. -1 indicates generate report at end of last interval.
    #[serde(default = "neg_one")]
    pub start_interval: i32,
    /// The number of intervals to include in a report. -1 indicates that all intervals are to be included.
    #[serde(default = "neg_one")]
    pub num_intervals: i32,
    /// True indicates report on intervals preceding startInterval. False indicates report on intervals following startInterval (e.g. forecast).
    #[serde(default = "bool_true")]
    pub historical: bool,
    /// Number of intervals that elapse between reports. -1 indicates same as numIntervals.
    #[serde(default = "neg_one")]
    pub frequency: i32,
    /// Number of times to repeat report. 1 indicates generate one report. -1 indicates repeat indefinitely.
    #[serde(default = "pos_one")]
    pub repeat: i32,
    /// Indicates VEN report interval options. See User Guide.
    #[serde(default)]
    pub report_intervals: ReportIntervals,
}

impl ReportDescriptor {
    /// An object that may be used to request a report from a VEN. See OpenADR REST User Guide for detailed description of how configure a report request.
    pub fn new(payload_type: ReportType) -> Self {
        Self {
            payload_type,
            reading_type: None,
            units: None,
            targets: None,
            aggregate: false,
            start_interval: -1,
            num_intervals: -1,
            historical: true,
            frequency: -1,
            repeat: 1,
            report_intervals: Default::default(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Default)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ReportIntervals {
    #[default]
    Intervals,
    SubIntervals,
    OpenIntervals,
}

fn bool_false() -> bool {
    false
}

fn bool_true() -> bool {
    true
}

fn neg_one() -> i32 {
    -1
}

fn pos_one() -> i32 {
    1
}

/// Contextual information used to interpret report payload values. E.g. a USAGE payload simply
/// contains a usage value, an associated descriptor provides necessary context such as units and
/// data quality.
#[skip_serializing_none]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Validate)]
#[serde(
    rename_all = "camelCase",
    tag = "objectType",
    rename = "REPORT_PAYLOAD_DESCRIPTOR"
)]
pub struct ReportPayloadDescriptor {
    /// Represents the nature of values.
    ///
    /// See enumerations in Definitions for defined string values, or use privately defined strings
    pub payload_type: ReportType,
    /// Enumerated or private string signifying the type of reading.
    #[serde(skip_serializing_if = "ReadingType::is_default", default)]
    pub reading_type: ReadingType,
    /// Units of measure.
    pub units: Option<Unit>,
    /// A quantification of the accuracy of a set of payload values.
    pub accuracy: Option<f32>,
    /// A quantification of the confidence in a set of payload values.
    #[validate(range(min = Confidence(0), max = Confidence(100)))]
    pub confidence: Option<Confidence>,
}

impl ReportPayloadDescriptor {
    pub fn new(payload_type: ReportType) -> Self {
        Self {
            payload_type,
            reading_type: Default::default(),
            units: None,
            accuracy: None,
            confidence: None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, PartialOrd)]
pub struct Confidence(u8);

impl ValidateRange<Confidence> for Confidence {
    fn greater_than(&self, _: Confidence) -> Option<bool> {
        None
    }

    fn less_than(&self, _: Confidence) -> Option<bool> {
        None
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        values_map::{Value, ValueType, ValuesMap},
        Duration,
    };

    use super::*;

    #[test]
    fn test_report_type_serialization() {
        assert_eq!(
            serde_json::to_string(&ReportType::Baseline).unwrap(),
            r#""BASELINE""#
        );
        assert_eq!(
            serde_json::to_string(&ReportType::RegulationSetpoint).unwrap(),
            r#""REGULATION_SETPOINT""#
        );
        assert_eq!(
            serde_json::to_string(&ReportType::Private(String::from("something else"))).unwrap(),
            r#""something else""#
        );
        assert_eq!(
            serde_json::from_str::<ReportType>(r#""DEMAND""#).unwrap(),
            ReportType::Demand
        );
        assert_eq!(
            serde_json::from_str::<ReportType>(r#""EXPORT_RESERVATION_FEE""#).unwrap(),
            ReportType::ExportReservationFee
        );
        assert_eq!(
            serde_json::from_str::<ReportType>(r#""something else""#).unwrap(),
            ReportType::Private(String::from("something else"))
        );

        assert!(serde_json::from_str::<ReportType>(r#""""#).is_err());
        assert!(serde_json::from_str::<ReportType>(&format!("\"{}\"", "x".repeat(129))).is_err());
    }

    #[test]
    fn test_reading_type_serialization() {
        assert_eq!(
            serde_json::to_string(&ReadingType::DirectRead).unwrap(),
            r#""DIRECT_READ""#
        );
        assert_eq!(
            serde_json::to_string(&ReadingType::Private(String::from("something else"))).unwrap(),
            r#""something else""#
        );
        assert_eq!(
            serde_json::from_str::<ReadingType>(r#""AVERAGE""#).unwrap(),
            ReadingType::Average
        );
        assert_eq!(
            serde_json::from_str::<ReadingType>(r#""something else""#).unwrap(),
            ReadingType::Private(String::from("something else"))
        );
    }

    #[test]
    fn descriptor_parses_minimal() {
        let json = r#"{"payloadType":"hello"}"#;
        let expected = ReportDescriptor::new(ReportType::Private("hello".into()));

        assert_eq!(
            serde_json::from_str::<ReportDescriptor>(json).unwrap(),
            expected
        );
    }

    #[test]
    fn parses_minimal_report() {
        let example = r#"{"eventID":"e1","clientName":"c","resources":[]}"#;
        let expected = ReportRequest {
            event_id: EventId("e1".parse().unwrap()),
            client_name: "c".to_string(),
            report_name: None,
            payload_descriptors: None,
            resources: vec![],
        };

        assert_eq!(
            serde_json::from_str::<ReportRequest>(example).unwrap(),
            expected
        );
    }

    #[test]
    fn test_resource_name_serialization() {
        assert_eq!(
            serde_json::to_string(&ResourceName::AggregatedReport).unwrap(),
            r#""AGGREGATED_REPORT""#
        );
        assert_eq!(
            serde_json::to_string(&ResourceName::Private(String::from("something else"))).unwrap(),
            r#""something else""#
        );
        assert_eq!(
            serde_json::from_str::<ResourceName>(r#""AGGREGATED_REPORT""#).unwrap(),
            ResourceName::AggregatedReport
        );
        assert_eq!(
            serde_json::from_str::<ResourceName>(r#""something else""#).unwrap(),
            ResourceName::Private(String::from("something else"))
        );

        assert!(serde_json::from_str::<ResourceName>(r#""""#).is_err());
        assert!(serde_json::from_str::<ResourceName>(&format!("\"{}\"", "x".repeat(129))).is_err());
    }

    #[test]
    fn parses_example() {
        let example = r#"[{
            "id": "object-999",
            "createdDateTime": "2023-06-15T09:30:00Z",
            "modificationDateTime": "2023-06-15T09:30:00Z",
            "objectType": "REPORT",
            "programID": "object-999",
            "eventID": "object-999",
            "clientName": "VEN-999",
            "reportName": "Battery_usage_04112023",
            "payloadDescriptors": null,
            "resources": [
              {
                "resourceName": "RESOURCE-999",
                "intervalPeriod": {
                  "start": "2023-06-15T09:30:00Z",
                  "duration": "PT1H",
                  "randomizeStart": "PT1H"
                },
                "intervals": [
                  {
                    "id": 0,
                    "intervalPeriod": {
                      "start": "2023-06-15T09:30:00Z",
                      "duration": "PT1H",
                      "randomizeStart": "PT1H"
                    },
                    "payloads": [
                      {
                        "type": "PRICE",
                        "values": [0.17]
                      }
                    ]
                  }
                ]
              }
            ],
            "clientID": "249rj49jiej"
          }]"#;

        let expected = Report {
            id: ReportId("object-999".parse().unwrap()),
            created_date_time: "2023-06-15T09:30:00Z".parse().unwrap(),
            modification_date_time: "2023-06-15T09:30:00Z".parse().unwrap(),
            content: ReportRequest {
                event_id: EventId("object-999".parse().unwrap()),
                client_name: "VEN-999".into(),
                report_name: Some("Battery_usage_04112023".into()),
                payload_descriptors: None,
                resources: vec![ReportResource {
                    resource_name: ResourceName::Private("RESOURCE-999".into()),
                    interval_period: Some(IntervalPeriod {
                        start: "2023-06-15T09:30:00Z".parse().unwrap(),
                        duration: Some(Duration::PT1H),
                        randomize_start: Some(Duration::PT1H),
                    }),
                    intervals: vec![Interval {
                        id: 0,
                        interval_period: Some(IntervalPeriod {
                            start: "2023-06-15T09:30:00Z".parse().unwrap(),
                            duration: Some(Duration::PT1H),
                            randomize_start: Some(Duration::PT1H),
                        }),
                        payloads: vec![ValuesMap {
                            value_type: ValueType("PRICE".into()),
                            values: vec![Value::Number(0.17)],
                        }],
                    }],
                }],
            },
            client_id: ClientId::new("249rj49jiej").unwrap(),
        };

        assert_eq!(
            serde_json::from_str::<Vec<Report>>(example).unwrap()[0],
            expected
        );
    }
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Debug)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ReportType {
    Reading,
    Usage,
    Demand,
    Setpoint,
    DeltaUsage,
    Baseline,
    OperatingState,
    UpRegulationAvailable,
    DownRegulationAvailable,
    RegulationSetpoint,
    StorageUsableCapacity,
    StorageChargeLevel,
    StorageMaxDischargePower,
    StorageMaxChargePower,
    SimpleLevel,
    UsageForecast,
    StorageDispatchForecast,
    LoadShedDeltaAvailable,
    GenerationDeltaAvailable,
    DataQuality,
    ImportReservationCapacity,
    ImportReservationFee,
    ExportReservationCapacity,
    ExportReservationFee,
    #[serde(untagged)]
    Private(
        #[serde(deserialize_with = "crate::string_within_range_inclusive::<1, 128, _>")] String,
    ),
}

#[derive(Clone, Default, Serialize, Deserialize, PartialEq, Eq, Debug)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ReadingType {
    #[default]
    DirectRead,
    Estimated,
    Summed,
    Mean,
    Peak,
    Forecast,
    Average,
    #[serde(untagged)]
    Private(String),
}

impl ReadingType {
    fn is_default(&self) -> bool {
        *self == Self::default()
    }
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Debug)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ResourceName {
    AggregatedReport,
    #[serde(untagged)]
    Private(
        #[serde(deserialize_with = "crate::string_within_range_inclusive::<1, 128, _>")] String,
    ),
}
