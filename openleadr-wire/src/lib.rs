//! Wire format definitions for OpenADR endpoints
//!
//! The types in this module model the messages sent over the wire in OpenADR 3.0.
//! Most types are originally generated from the OpenAPI specification of OpenADR
//! and manually modified to be more idiomatic.

pub use event::Event;
pub use program::Program;
pub use report::Report;
use serde::{de::Unexpected, Deserialize, Deserializer, Serialize, Serializer};
use std::fmt::Display;
pub use ven::Ven;

pub mod event;
pub mod interval;
pub mod oauth;
pub mod problem;
pub mod program;
pub mod report;
pub mod resource;
pub mod target;
pub mod values_map;
pub mod ven;

pub mod serde_rfc3339 {
    use super::*;

    use chrono::{DateTime, TimeZone, Utc};

    pub fn serialize<S, Tz>(time: &DateTime<Tz>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
        Tz: TimeZone,
    {
        serializer.serialize_str(&time.to_rfc3339())
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<DateTime<Utc>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let rfc_str = <String as Deserialize>::deserialize(deserializer)?;

        match DateTime::parse_from_rfc3339(&rfc_str) {
            Ok(datetime) => Ok(datetime.into()),
            Err(_) => Err(serde::de::Error::invalid_value(
                Unexpected::Str(&rfc_str),
                &"Invalid RFC3339 string",
            )),
        }
    }
}

pub fn string_within_range_inclusive<'de, const MIN: usize, const MAX: usize, D>(
    deserializer: D,
) -> Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    let string = <String as Deserialize>::deserialize(deserializer)?;
    let len = string.len();

    if (MIN..=MAX).contains(&len) {
        Ok(string.to_string())
    } else {
        Err(serde::de::Error::invalid_value(
            Unexpected::Str(&string),
            &IdentifierError::InvalidLength(len).to_string().as_str(),
        ))
    }
}

/// A string that matches `/^[a-zA-Z0-9_-]*$/` with length in 1..=128
#[derive(Debug, Clone, Serialize, PartialEq, Eq, Hash, PartialOrd, Ord, sqlx::Type)]
#[sqlx(transparent)]
pub struct Identifier(#[serde(deserialize_with = "identifier")] String);

impl<'de> Deserialize<'de> for Identifier {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s: String = Deserialize::deserialize(deserializer)?;

        match Self::validate(&s) {
            Ok(()) => Ok(Identifier(s)),
            Err(e) => Err(serde::de::Error::invalid_value(
                Unexpected::Str(&s),
                &e.to_string().as_str(),
            )),
        }
    }
}

#[derive(thiserror::Error, Debug)]
pub enum IdentifierError {
    #[error("string length {0} outside of allowed range 1..=128")]
    InvalidLength(usize),
    #[error("identifier contains characters besides [a-zA-Z0-9_-]")]
    InvalidCharacter,
    #[error("this identifier name is not allowed: {0}")]
    ForbiddenName(String),
}

const FORBIDDEN_NAMES: &[&str] = &["null"];

impl std::str::FromStr for Identifier {
    type Err = IdentifierError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::validate(s).map(|()| Identifier(s.to_string()))
    }
}

impl Identifier {
    fn validate(s: &str) -> Result<(), IdentifierError> {
        let is_valid_character = |b: u8| b.is_ascii_alphanumeric() || b == b'_' || b == b'-';

        if !(1..=128).contains(&s.len()) {
            Err(IdentifierError::InvalidLength(s.len()))
        } else if !s.bytes().all(is_valid_character) {
            Err(IdentifierError::InvalidCharacter)
        } else if FORBIDDEN_NAMES.contains(&s.to_ascii_lowercase().as_str()) {
            Err(IdentifierError::ForbiddenName(s.to_string()))
        } else {
            Ok(())
        }
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Display for Identifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// An ISO 8601 formatted duration
#[derive(Clone, Debug, PartialEq)]
pub struct Duration(iso8601_duration::Duration);

impl<'de> Deserialize<'de> for Duration {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = String::deserialize(deserializer)?;
        let duration = raw
            .parse::<iso8601_duration::Duration>()
            .map_err(|_| "iso8601_duration::ParseDurationError")
            .map_err(serde::de::Error::custom)?;

        Ok(Self(duration))
    }
}

impl Serialize for Duration {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.to_string().serialize(serializer)
    }
}

impl Duration {
    /// Because iso8601 durations can include months and years, they don't independently have a
    /// fixed duration. Their real duration (in real units like seconds) can only be determined
    /// when a starting time is given.
    ///
    /// NOTE: does not consider leap seconds!
    pub fn to_chrono_at_datetime<Tz: chrono::TimeZone>(
        &self,
        at: chrono::DateTime<Tz>,
    ) -> chrono::Duration {
        self.0.to_chrono_at_datetime(at)
    }

    /// One (1) hour
    pub const PT1H: Self = Self(iso8601_duration::Duration {
        year: 0.0,
        month: 0.0,
        day: 0.0,
        hour: 1.0,
        minute: 0.0,
        second: 0.0,
    });

    /// Indicates that an event's intervals continue indefinitely into the future until the event is
    /// deleted or modified. This effectively represents an infinite duration.
    pub const P999Y: Self = Self(iso8601_duration::Duration {
        year: 9999.0,
        month: 0.0,
        day: 0.0,
        hour: 0.0,
        minute: 0.0,
        second: 0.0,
    });

    pub const PT0S: Self = Self(iso8601_duration::Duration {
        year: 0.0,
        month: 0.0,
        day: 0.0,
        hour: 0.0,
        minute: 0.0,
        second: 0.0,
    });

    pub const fn hours(hour: f32) -> Self {
        Self(iso8601_duration::Duration {
            year: 0.0,
            month: 0.0,
            day: 0.0,
            hour,
            minute: 0.0,
            second: 0.0,
        })
    }
}

impl std::str::FromStr for Duration {
    type Err = iso8601_duration::ParseDurationError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let duration = s.parse::<iso8601_duration::Duration>()?;
        Ok(Self(duration))
    }
}

impl Display for Duration {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let iso8601_duration::Duration {
            year,
            month,
            day,
            hour,
            minute,
            second,
        } = self.0;

        f.write_fmt(format_args!(
            "P{year}Y{month}M{day}DT{hour}H{minute}M{second}S",
        ))
    }
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Clone)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum OperatingState {
    Normal,
    Error,
    IdleNormal,
    RunningNormal,
    RunningCurtailed,
    RunningHeightened,
    IdleCurtailed,
    #[serde(rename = "SGD_ERROR_CONDITION")]
    SGDErrorCondition,
    IdleHeightened,
    IdleOptedOut,
    RunningOptedOut,
    #[serde(untagged)]
    Private(String),
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Debug)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum DataQuality {
    /// No known reasons to doubt the data.
    Ok,
    /// The data item is currently unavailable.
    Missing,
    /// The data item has been estimated from other available information.
    Estimated,
    /// The data item is suspected to be bad or is known to be.
    Bad,
    /// An application specific privately defined data quality setting.
    #[serde(untagged)]
    Private(String),
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Debug)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Attribute {
    /// Describes a single geographic point. Values contains 2 floats, generally
    /// representing longitude and latitude. Demand Response programs may define
    /// their own use of these fields.
    Location,
    /// Describes a geographic area. Application specific data. Demand Response
    /// programs may define their own use of these fields, such as GeoJSON
    /// polygon data.
    Area,
    /// The maximum consumption as a float, in kiloWatts.
    MaxPowerConsumption,
    /// The maximum power the device can export as a float, in kiloWatts.
    MaxPowerExport,
    /// A free-form short description of a VEN or resource.
    Description,
    /// An application specific privately defined attribute.
    #[serde(untagged)]
    Private(String),
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Debug)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Unit {
    /// Kilowatt-hours (kWh)
    #[serde(rename = "KWH")]
    KWH,
    /// Greenhouse gas emissions (g/kWh)
    #[serde(rename = "GHG")]
    GHG,
    /// Voltage (V)
    Volts,
    /// Current (A)
    Amps,
    /// Temperature (C)
    Celcius,
    /// Temperature (F)
    Fahrenheit,
    /// Percentage (%)
    Percent,
    /// Kilowatts
    #[serde(rename = "KW")]
    KW,
    /// Kilovolt-ampere hours (kVAh)
    #[serde(rename = "KVAH")]
    KVAH,
    /// Kilovolt-amperes reactive hours (kVARh)
    #[serde(rename = "KVARH")]
    KVARH,
    /// Kilovolt-amperes (kVA)
    #[serde(rename = "KVA")]
    KVA,
    /// Kilovolt-amperes reactive (kVAR)
    #[serde(rename = "KVAR")]
    KVAR,
    /// An application specific privately defined unit.
    #[serde(untagged)]
    Private(String),
}

// example: 249rj49jiej
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Hash, Eq)]
pub struct ClientId(pub(crate) Identifier);

impl Display for ClientId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl ClientId {
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }

    pub fn new(identifier: &str) -> Option<Self> {
        Some(Self(identifier.parse().ok()?))
    }
}

#[cfg(test)]
mod tests {
    use crate::{Attribute, DataQuality, Identifier, OperatingState, Unit};

    #[test]
    fn test_operating_state_serialization() {
        assert_eq!(
            serde_json::to_string(&OperatingState::SGDErrorCondition).unwrap(),
            r#""SGD_ERROR_CONDITION""#
        );
        assert_eq!(
            serde_json::to_string(&OperatingState::Error).unwrap(),
            r#""ERROR""#
        );
        assert_eq!(
            serde_json::to_string(&OperatingState::Private(String::from("something else")))
                .unwrap(),
            r#""something else""#
        );
        assert_eq!(
            serde_json::from_str::<OperatingState>(r#""NORMAL""#).unwrap(),
            OperatingState::Normal
        );
        assert_eq!(
            serde_json::from_str::<OperatingState>(r#""something else""#).unwrap(),
            OperatingState::Private(String::from("something else"))
        );
    }

    #[test]
    fn test_data_quality_serialization() {
        assert_eq!(serde_json::to_string(&DataQuality::Ok).unwrap(), r#""OK""#);
        assert_eq!(
            serde_json::to_string(&DataQuality::Private(String::from("something else"))).unwrap(),
            r#""something else""#
        );
        assert_eq!(
            serde_json::from_str::<DataQuality>(r#""MISSING""#).unwrap(),
            DataQuality::Missing
        );
        assert_eq!(
            serde_json::from_str::<DataQuality>(r#""something else""#).unwrap(),
            DataQuality::Private(String::from("something else"))
        );
    }

    #[test]
    fn test_attribute_serialization() {
        assert_eq!(
            serde_json::to_string(&Attribute::Area).unwrap(),
            r#""AREA""#
        );
        assert_eq!(
            serde_json::to_string(&Attribute::Private(String::from("something else"))).unwrap(),
            r#""something else""#
        );
        assert_eq!(
            serde_json::from_str::<Attribute>(r#""MAX_POWER_EXPORT""#).unwrap(),
            Attribute::MaxPowerExport
        );
        assert_eq!(
            serde_json::from_str::<Attribute>(r#""something else""#).unwrap(),
            Attribute::Private(String::from("something else"))
        );
    }

    #[test]
    fn test_unit_serialization() {
        assert_eq!(serde_json::to_string(&Unit::KVARH).unwrap(), r#""KVARH""#);
        assert_eq!(
            serde_json::to_string(&Unit::Private(String::from("something else"))).unwrap(),
            r#""something else""#
        );
        assert_eq!(
            serde_json::from_str::<Unit>(r#""CELCIUS""#).unwrap(),
            Unit::Celcius
        );
        assert_eq!(
            serde_json::from_str::<Unit>(r#""something else""#).unwrap(),
            Unit::Private(String::from("something else"))
        );
    }

    impl quickcheck::Arbitrary for super::Duration {
        fn arbitrary(g: &mut quickcheck::Gen) -> Self {
            // the iso8601_duration library uses an f32 to store the values, which starts losing
            // precision at 24-bit integers.
            super::Duration(iso8601_duration::Duration {
                year: (<u32 as quickcheck::Arbitrary>::arbitrary(g) & 0x00FF_FFFF) as f32,
                month: (<u32 as quickcheck::Arbitrary>::arbitrary(g) & 0x00FF_FFFF) as f32,
                day: (<u32 as quickcheck::Arbitrary>::arbitrary(g) & 0x00FF_FFFF) as f32,
                hour: (<u32 as quickcheck::Arbitrary>::arbitrary(g) & 0x00FF_FFFF) as f32,
                minute: (<u32 as quickcheck::Arbitrary>::arbitrary(g) & 0x00FF_FFFF) as f32,
                second: (<u32 as quickcheck::Arbitrary>::arbitrary(g) & 0x00FF_FFFF) as f32,
            })
        }
    }

    #[test]
    fn duration_to_string_from_str_roundtrip() {
        quickcheck::quickcheck(test as fn(_) -> bool);

        fn test(input: super::Duration) -> bool {
            let roundtrip = input.to_string().parse::<super::Duration>().unwrap();

            assert_eq!(input.0, roundtrip.0);

            input.0 == roundtrip.0
        }
    }

    #[test]
    fn deserialize_identifier() {
        assert_eq!(
            serde_json::from_str::<Identifier>(r#""example-999""#).unwrap(),
            Identifier("example-999".to_string())
        );
        assert!(serde_json::from_str::<Identifier>(r#""þingvellir-999""#)
            .unwrap_err()
            .to_string()
            .contains("identifier contains characters besides"));

        let long = "x".repeat(128);
        assert_eq!(
            serde_json::from_str::<Identifier>(&format!("\"{long}\"")).unwrap(),
            Identifier(long)
        );

        let too_long = "x".repeat(129);
        assert!(
            serde_json::from_str::<Identifier>(&format!("\"{too_long}\""))
                .unwrap_err()
                .to_string()
                .contains("string length 129 outside of allowed range 1..=128")
        );

        assert!(serde_json::from_str::<Identifier>("\"\"")
            .unwrap_err()
            .to_string()
            .contains("string length 0 outside of allowed range 1..=128"));
    }

    #[test]
    fn deserialize_string_within_range_inclusive() {
        use serde::Deserialize;

        #[derive(Debug, Deserialize, PartialEq, Eq)]
        struct Test(
            #[serde(deserialize_with = "super::string_within_range_inclusive::<1, 128, _>")] String,
        );

        let long = "x".repeat(128);
        assert_eq!(
            serde_json::from_str::<Test>(&format!("\"{long}\"")).unwrap(),
            Test(long)
        );

        let too_long = "x".repeat(129);
        assert!(serde_json::from_str::<Test>(&format!("\"{too_long}\""))
            .unwrap_err()
            .to_string()
            .contains("string length 129 outside of allowed range 1..=128"));

        assert!(serde_json::from_str::<Test>("\"\"")
            .unwrap_err()
            .to_string()
            .contains("string length 0 outside of allowed range 1..=128"));
    }
}
