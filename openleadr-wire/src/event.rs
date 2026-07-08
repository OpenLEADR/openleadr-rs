//! Types used for the `event/` endpoint

use crate::{
    Duration, Identifier, IdentifierError, Unit,
    interval::IntervalPeriod,
    program::ProgramId,
    report::ReportDescriptor,
    target::Target,
    values_map::{Value, ValueKind},
};
use chrono::{DateTime, Utc};
use iso_currency::Currency;
use serde::{Deserialize, Serialize};
use serde_with::{DefaultOnNull, serde_as, skip_serializing_none};
use std::{
    fmt::{Display, Formatter},
    str::FromStr,
};
use validator::{Validate, ValidationError};

/// Event object to communicate a Demand Response request to VEN. If intervalPeriod is present, sets
/// default start time and duration of intervals.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Validate)]
#[serde(rename_all = "camelCase")]
pub struct Event {
    /// URL safe VTN assigned object ID.
    pub id: EventId,
    /// datetime in ISO 8601 format
    #[serde(with = "crate::serde_rfc3339")]
    pub created_date_time: DateTime<Utc>,
    /// datetime in ISO 8601 format
    #[serde(with = "crate::serde_rfc3339")]
    pub modification_date_time: DateTime<Utc>,
    #[serde(flatten)]
    #[validate(nested)]
    pub content: EventRequest,
}

#[skip_serializing_none]
#[serde_as]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Validate)]
#[serde(rename_all = "camelCase")]
pub struct EventRequest {
    /// URL safe VTN assigned object ID.
    #[serde(rename = "programID")]
    pub program_id: ProgramId,
    /// User defined string for use in debugging or User Interface.
    pub event_name: Option<String>,
    /// Optional duration of event. May be used to loop intervals. See User Guide.
    pub duration: Option<Duration>,
    /// Relative priority of event. A lower number is a higher priority.
    pub priority: Priority,
    /// A list of targets.
    #[serde(default)]
    #[serde_as(deserialize_as = "DefaultOnNull")]
    pub targets: Vec<Target>,
    /// A list of reportDescriptor objects. Used to request reports from VEN.
    pub report_descriptors: Option<Vec<ReportDescriptor>>,
    /// A list of payloadDescriptor objects.
    pub payload_descriptors: Option<Vec<EventPayloadDescriptor>>,
    /// Defines default start and durations of intervals.
    pub interval_period: Option<IntervalPeriod>,
    /// A list of interval objects.
    #[validate(nested)]
    pub intervals: Option<Vec<EventInterval>>,
}

impl EventRequest {
    pub fn new(program_id: ProgramId) -> Self {
        Self {
            program_id,
            event_name: None,
            duration: None,
            priority: Priority::UNSPECIFIED,
            targets: vec![],
            report_descriptors: None,
            payload_descriptors: None,
            interval_period: None,
            intervals: None,
        }
    }

    pub fn with_event_name(mut self, event_name: impl ToString) -> Self {
        self.event_name = Some(event_name.to_string());
        self
    }

    pub fn with_priority(self, priority: Priority) -> Self {
        Self { priority, ..self }
    }

    pub fn with_targets(mut self, targets: Vec<Target>) -> Self {
        self.targets = targets;
        self
    }

    pub fn with_report_descriptors(mut self, report_descriptors: Vec<ReportDescriptor>) -> Self {
        self.report_descriptors = Some(report_descriptors);
        self
    }

    pub fn with_payload_descriptors(
        mut self,
        payload_descriptors: Vec<EventPayloadDescriptor>,
    ) -> Self {
        self.payload_descriptors = Some(payload_descriptors);
        self
    }

    pub fn with_interval_period(mut self, interval_period: IntervalPeriod) -> Self {
        self.interval_period = Some(interval_period);
        self
    }

    pub fn with_intervals(mut self, intervals: Vec<EventInterval>) -> Self {
        self.intervals = Some(intervals);
        self
    }
}

/// URL safe VTN assigned object ID
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Hash, Eq)]
pub struct EventId(pub(crate) Identifier);

impl Display for EventId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl EventId {
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl FromStr for EventId {
    type Err = IdentifierError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(s.parse()?))
    }
}

/// Relative priority of an event
///
/// `0` indicates the highest priority.
///
/// **Interpretation of the specification:** [`Priority::UNSPECIFIED`] has a lower priority than any other value,
/// i.e., equals to [`Priority::MIN`]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Priority(Option<u32>);

impl Priority {
    pub const UNSPECIFIED: Self = Self(None);

    pub const MAX: Self = Self(Some(0));
    pub const MIN: Self = Self::UNSPECIFIED;

    pub const fn new(val: u32) -> Self {
        Self(Some(val))
    }
}

impl PartialOrd for Priority {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Priority {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        use std::cmp::Ordering;

        match (self.0, other.0) {
            (None, None) => Ordering::Equal,
            (None, Some(_)) => Ordering::Less,
            (Some(_), None) => Ordering::Greater,
            (Some(s), Some(o)) => s.cmp(&o).reverse(),
        }
    }
}

impl From<Option<i64>> for Priority {
    fn from(value: Option<i64>) -> Self {
        Self(value.and_then(|i| i.unsigned_abs().try_into().ok()))
    }
}

impl From<Priority> for Option<i64> {
    fn from(value: Priority) -> Self {
        value.0.map(|u| u.into())
    }
}

/// Contextual information used to interpret event valuesMap values. E.g. a PRICE payload simply
/// contains a price value, an associated descriptor provides necessary context such as units and
/// currency.
#[skip_serializing_none]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EventPayloadDescriptor {
    /// Represents the nature of values.
    ///
    /// See enumerations in Definitions for defined string values, or use privately defined strings
    pub payload_type: EventType,
    /// Units of measure.
    pub units: Option<Unit>,
    /// Currency of price payload.
    pub currency: Option<Currency>,
}

impl EventPayloadDescriptor {
    pub fn new(payload_type: EventType) -> Self {
        Self {
            payload_type,
            units: None,
            currency: None,
        }
    }
}

/// An object defining a temporal window and a list of valuesMaps. if intervalPeriod present may set
/// temporal aspects of interval or override event.intervalPeriod.
#[skip_serializing_none]
#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize, Validate)]
#[serde(rename_all = "camelCase")]
pub struct EventInterval {
    /// A client generated number assigned an interval object. Not a sequence number.
    pub id: i32,
    /// Defines default start and durations of intervals.
    pub interval_period: Option<IntervalPeriod>,
    /// A list of valuesMap objects.
    #[validate(length(min = 1))]
    pub payloads: Vec<EventValuesMap>,
}

impl EventInterval {
    pub fn new(id: i32, payloads: Vec<EventValuesMap>) -> Self {
        Self {
            id,
            interval_period: None,
            payloads,
        }
    }
}

/// Represents one or more values associated with a type. E.g. a type of PRICE contains a single float value.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Validate)]
#[validate(schema(function = "validate_payload"))]
pub struct EventValuesMap {
    /// Enumerated or private string signifying the nature of values. E.G. \"PRICE\" indicates value is to be interpreted as a currency.
    #[serde(rename = "type")]
    pub value_type: EventType,
    /// A list of data points. Most often a singular value such as a price.
    pub values: Vec<Value>,
}

impl<'de> Deserialize<'de> for EventValuesMap {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Raw {
            #[serde(rename = "type")]
            value_type: EventType,
            values: Vec<Value>,
        }

        let Raw { value_type, values } = Raw::deserialize(deserializer)?;
        let values = values
            .into_iter()
            .map(|v| coerce_value(&value_type, v))
            .collect();
        Ok(EventValuesMap { value_type, values })
    }
}

fn coerce_value(value_type: &EventType, value: Value) -> Value {
    match value {
        Value::Integer(i) if value_type.expected_value_kind() == ValueKind::Number => {
            Value::Number(i as f64)
        }
        other => other,
    }
}

/// Validate each value in the payload matches the given value type.
///
/// Errors on the first mistyped value. It might be useful to return all validation errors rather
/// than just the first one, but the validator crate doesn't seem to support this yet.
/// See https://github.com/Keats/validator/issues/326
fn validate_payload(payload: &EventValuesMap) -> Result<(), ValidationError> {
    for value in &payload.values {
        validate_value(&payload.value_type, value)?
    }
    Ok(())
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Debug)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum EventType {
    Simple,
    Price,
    ChargeStateSetpoint,
    DispatchSetpoint,
    DispatchSetpointRelative,
    ControlSetpoint,
    ExportPrice,
    #[serde(rename = "GHG")]
    GHG,
    Curve,
    #[serde(rename = "OLS")]
    OLS,
    ImportCapacitySubscription,
    ImportCapacityReservation,
    ImportCapacityReservationFee,
    ImportCapacityAvailable,
    ImportCapacityAvailablePrice,
    ExportCapacitySubscription,
    ExportCapacityReservation,
    ExportCapacityReservationFee,
    ExportCapacityAvailable,
    ExportCapacityAvailablePrice,
    ImportCapacityLimit,
    ExportCapacityLimit,
    AlertGridEmergency,
    AlertBlackStart,
    AlertPossibleOutage,
    AlertFlexAlert,
    AlertFire,
    AlertFreezing,
    AlertWind,
    AlertTsunami,
    AlertAirQuality,
    AlertOther,
    #[serde(rename = "CTA2045_REBOOT")]
    CTA2045Reboot,
    #[serde(rename = "CTA2045_SET_OVERRIDE_STATUS")]
    CTA2045SetOverrideStatus,
    #[serde(untagged)]
    #[serde(deserialize_with = "crate::string_within_range_inclusive::<1, 128, _>")]
    Private(String),
}

impl EventType {
    fn expected_value_kind(&self) -> ValueKind {
        use EventType::*;
        match self {
            Price
            | ExportPrice
            | GHG
            | OLS
            | ChargeStateSetpoint
            | DispatchSetpoint
            | DispatchSetpointRelative
            | ImportCapacitySubscription
            | ImportCapacityReservation
            | ImportCapacityReservationFee
            | ImportCapacityAvailable
            | ImportCapacityAvailablePrice
            | ExportCapacitySubscription
            | ExportCapacityReservation
            | ExportCapacityReservationFee
            | ExportCapacityAvailable
            | ExportCapacityAvailablePrice
            | ImportCapacityLimit
            | ExportCapacityLimit => ValueKind::Number,

            Simple | CTA2045Reboot | CTA2045SetOverrideStatus => ValueKind::Integer,

            Curve => ValueKind::Point,

            AlertGridEmergency | AlertBlackStart | AlertPossibleOutage | AlertFlexAlert
            | AlertFire | AlertFreezing | AlertWind | AlertTsunami | AlertAirQuality
            | AlertOther => ValueKind::String,

            ControlSetpoint | Private(_) => ValueKind::Any,
        }
    }
}

fn validate_value(value_type: &EventType, value: &Value) -> Result<(), ValidationError> {
    let expected = value_type.expected_value_kind();
    if expected == ValueKind::Any || value.kind() == expected {
        Ok(())
    } else {
        Err(validate_value_error(value_type, value))
    }
}

fn validate_value_error(value_type: &EventType, value: &Value) -> ValidationError {
    let cow = format!("value {value:?} must match the given type {value_type:?}").into();
    ValidationError::new("values must match the given type").with_message(cow)
}

#[cfg(test)]
mod tests {
    use crate::{Duration, values_map::Value};
    use std::borrow::Cow;

    use super::*;

    #[test]
    fn priority_order() {
        assert_eq!(Priority::MAX, Priority::new(0));
        assert!(Priority::MAX > Priority::MIN);
        assert_eq!(Priority::MIN, Priority::UNSPECIFIED);
        assert!(Priority::new(5) > Priority::UNSPECIFIED);
        assert!(Priority::new(5) > Priority::new(6));
        assert!(Priority::new(u32::MAX) > Priority::UNSPECIFIED);
    }

    #[test]
    fn test_event_serialization() {
        assert_eq!(
            serde_json::to_string(&EventType::Simple).unwrap(),
            r#""SIMPLE""#
        );
        assert_eq!(
            serde_json::to_string(&EventType::CTA2045Reboot).unwrap(),
            r#""CTA2045_REBOOT""#
        );
        assert_eq!(
            serde_json::from_str::<EventType>(r#""GHG""#).unwrap(),
            EventType::GHG
        );
        assert_eq!(
            serde_json::from_str::<EventType>(r#""something else""#).unwrap(),
            EventType::Private(String::from("something else"))
        );

        assert!(serde_json::from_str::<EventType>(r#""""#).is_err());
        assert!(serde_json::from_str::<EventType>(&format!("\"{}\"", "x".repeat(129))).is_err());
    }

    #[test]
    fn parse_minimal() {
        let example = r#"{"programID":"foo"}"#;
        assert_eq!(
            serde_json::from_str::<EventRequest>(example).unwrap(),
            EventRequest {
                program_id: ProgramId("foo".parse().unwrap()),
                event_name: None,
                duration: None,
                priority: Priority::MIN,
                targets: vec![],
                report_descriptors: None,
                payload_descriptors: None,
                interval_period: None,
                intervals: None,
            }
        );
    }

    #[test]
    fn example_parses() {
        let example = r#"[{
                                    "id": "object-999-foo",
                                    "createdDateTime": "2023-06-15T09:30:00Z",
                                    "modificationDateTime": "2023-06-15T09:30:00Z",
                                    "objectType": "EVENT",
                                    "programID": "object-999",
                                    "eventName": "price event 11-18-2022",
                                    "duration": "PT1H",
                                    "priority": 0,
                                    "targets": null,
                                    "reportDescriptors": null,
                                    "payloadDescriptors": null,
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
                                            "values": [
                                              0.17
                                            ]
                                          }
                                        ]
                                      }
                                    ]
                                  }]"#;

        let expected = Event {
            id: EventId("object-999-foo".parse().unwrap()),
            created_date_time: "2023-06-15T09:30:00Z".parse().unwrap(),
            modification_date_time: "2023-06-15T09:30:00Z".parse().unwrap(),
            content: EventRequest {
                program_id: ProgramId("object-999".parse().unwrap()),
                event_name: Some("price event 11-18-2022".into()),
                duration: Some(Duration::PT1H),
                priority: Priority::MAX,
                targets: Default::default(),
                report_descriptors: None,
                payload_descriptors: None,
                interval_period: Some(IntervalPeriod {
                    start: "2023-06-15T09:30:00Z".parse().unwrap(),
                    duration: Some(Duration::PT1H),
                    randomize_start: Some(Duration::PT1H),
                }),
                intervals: Some(vec![EventInterval {
                    id: 0,
                    interval_period: Some(IntervalPeriod {
                        start: "2023-06-15T09:30:00Z".parse().unwrap(),
                        duration: Some(Duration::PT1H),
                        randomize_start: Some(Duration::PT1H),
                    }),
                    payloads: vec![EventValuesMap {
                        value_type: EventType::Price,
                        values: vec![Value::Number(0.17)],
                    }],
                }]),
            },
        };

        assert_eq!(
            serde_json::from_str::<Vec<Event>>(example).unwrap()[0],
            expected
        );
    }

    #[test]
    fn test_currency() {
        // deserialize
        let example = r#"{"payloadType":"SIMPLE","currency":"EUR"}"#;

        let expected = EventPayloadDescriptor {
            payload_type: EventType::Simple,
            units: None,
            currency: Some(Currency::EUR),
        };

        assert_eq!(
            serde_json::from_str::<EventPayloadDescriptor>(example).unwrap(),
            expected
        );

        // round-trip
        let source = EventPayloadDescriptor {
            payload_type: EventType::Price,
            units: Some(Unit::Volts),
            currency: Some(Currency::USD),
        };

        let serialized = serde_json::to_string(&source).unwrap();

        assert_eq!(
            source,
            serde_json::from_str::<EventPayloadDescriptor>(&serialized).unwrap()
        );
    }

    #[test]
    fn test_validate_value_positive() {
        let input = r#"{"type":"SIMPLE","values":[1]}"#;
        let expected = Ok(());
        let actual = serde_json::from_str::<EventValuesMap>(input)
            .unwrap()
            .validate();
        assert_eq!(actual, expected);
    }

    #[test]
    fn validate_private_value() {
        let input = r#"{"type":"WHATEVER","values":["Private types must accept all values"]}"#;
        let expected = Ok(());
        let actual = serde_json::from_str::<EventValuesMap>(input)
            .unwrap()
            .validate();
        assert_eq!(actual, expected);

        let input = r#"{"type":"WHATEVER","values":[1]}"#;
        let expected = Ok(());
        let actual = serde_json::from_str::<EventValuesMap>(input)
            .unwrap()
            .validate();
        assert_eq!(actual, expected);

        let input = r#"{"type":"WHATEVER","values":[{"x": 1, "y": 3}]}"#;
        let expected = Ok(());
        let actual = serde_json::from_str::<EventValuesMap>(input)
            .unwrap()
            .validate();
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_validate_value_negative() {
        let input = r#"{"type":"SIMPLE","values":["string"]}"#;
        let expected = {
            use std::collections::HashMap;
            use validator::{ValidationErrors, ValidationErrorsKind};
            let mut hash_map = HashMap::new();
            let validation_errors_kind = {
                let value = Value::String("string".to_string());
                ValidationErrorsKind::Field(vec![validate_value_error(&EventType::Simple, &value)])
            };
            hash_map.insert(Cow::from("__all__"), validation_errors_kind);
            Err(ValidationErrors(hash_map))
        };
        let actual = serde_json::from_str::<EventValuesMap>(input)
            .unwrap()
            .validate();
        assert_eq!(actual, expected);
    }

    #[test]
    fn price_integer_json_coerces_to_number() {
        // `1` and `1.0` are the same price — both must land as Number(1.0).
        let from_int: EventValuesMap =
            serde_json::from_str(r#"{"type": "PRICE", "values": [1]}"#).unwrap();
        let from_float: EventValuesMap =
            serde_json::from_str(r#"{"type": "PRICE", "values": [1.0]}"#).unwrap();

        assert_eq!(from_int.values, vec![Value::Number(1.0)]);
        assert_eq!(from_float.values, vec![Value::Number(1.0)]);
        assert!(from_int.validate().is_ok());
        assert!(from_float.validate().is_ok());
    }

    #[test]
    fn price_coerces_every_element() {
        let map: EventValuesMap =
            serde_json::from_str(r#"{"type": "PRICE", "values": [1, 2.5, 3]}"#).unwrap();
        assert_eq!(
            map.values,
            vec![Value::Number(1.0), Value::Number(2.5), Value::Number(3.0)]
        );
    }

    #[test]
    fn integer_typed_values_are_not_coerced() {
        let map: EventValuesMap =
            serde_json::from_str(r#"{"type": "SIMPLE", "values": [1]}"#).unwrap();
        assert_eq!(map.values, vec![Value::Integer(1)]);
        assert!(map.validate().is_ok());
    }

    #[test]
    fn genuine_mismatch_is_still_rejected() {
        let map: EventValuesMap =
            serde_json::from_str(r#"{"type": "PRICE", "values": ["1"]}"#).unwrap();
        assert!(map.validate().is_err());
    }

    #[test]
    fn boolean_value_is_rejected() {
        // No EventType expects a boolean, so `true` under any concrete type is a mismatch.
        // This exercises the ValueKind::Boolean / Value::kind() path specifically.
        let map: EventValuesMap =
            serde_json::from_str(r#"{"type": "PRICE", "values": [true]}"#).unwrap();
        assert!(map.validate().is_err());
    }
}
