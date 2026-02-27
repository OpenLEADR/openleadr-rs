//! Descriptions of temporal periods

use crate::{values_map::ValuesMap, Duration};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;

/// An object defining a temporal window and a list of valuesMaps. if intervalPeriod present may set
/// temporal aspects of interval or override event.intervalPeriod.
#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Interval {
    /// A client generated number assigned an interval object. Not a sequence number.
    pub id: i32,
    /// Defines start and durations of intervals.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub interval_period: Option<IntervalPeriod>,
    /// A list of valuesMap objects.
    pub payloads: Vec<ValuesMap>,
}

impl Interval {
    pub fn new(id: i32, payloads: Vec<ValuesMap>) -> Self {
        Self {
            id,
            interval_period: None,
            payloads,
        }
    }
}

/// Defines temporal aspects of intervals.
///
/// A start of "0001-01-01" or "0001-01-01T00:00:00" may indicate 'now'. See User Guide.
/// A duration of "P9999Y" may indicate infinity. See User Guide.
/// A randomizeStart indicates absolute range of client applied offset to start. See User Guide.
#[skip_serializing_none]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IntervalPeriod {
    /// The start time of an interval or set of intervals.
    #[serde(with = "crate::serde_rfc3339")]
    // FIXME field not required, though, it's unclear how to interpret it if it's missing
    pub start: DateTime<Utc>,
    /// The duration of an interval or set of intervals.
    pub duration: Option<Duration>,
    /// Indicates a randomization time that may be applied to start.
    pub randomize_start: Option<Duration>,
}

impl IntervalPeriod {
    pub fn new(start: DateTime<Utc>) -> Self {
        Self {
            start,
            duration: None,
            randomize_start: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::interval::IntervalPeriod;

    #[test]
    fn parse_interval_period() {
        let only_start = r#"{"start": "2021-01-01T00:00:00Z"}"#;
        let interval: IntervalPeriod = serde_json::from_str(only_start).unwrap();
        assert_eq!(interval.start.to_rfc3339(), "2021-01-01T00:00:00+00:00");
        assert!(interval.duration.is_none());
        assert!(interval.randomize_start.is_none());

        let start_now = r#"{"start": "0001-01-01T00:00:00Z"}"#;
        let interval: IntervalPeriod = serde_json::from_str(start_now).unwrap();
        assert_eq!(interval.start.to_rfc3339(), "0001-01-01T00:00:00+00:00");
        assert!(interval.duration.is_none());
        assert!(interval.randomize_start.is_none());

        let infinit_duration = r#"{"duration": "P9999Y", "start":"2021-01-01T00:00:00Z"}"#;
        let interval: IntervalPeriod = serde_json::from_str(infinit_duration).unwrap();
        assert_eq!(interval.duration.unwrap().to_string(), "P9999Y0M0DT0H0M0S");
        assert_eq!(interval.start.to_rfc3339(), "2021-01-01T00:00:00+00:00");
        assert!(interval.randomize_start.is_none());

        let all_fields = r#"{
                  "duration": "P0Y1M2DT3H4M5S",
                  "start": "2021-01-01T01:02:03Z",
                  "randomizeStart": "PT3M"
               }"#;
        let interval: IntervalPeriod = serde_json::from_str(all_fields).unwrap();
        assert_eq!(interval.duration.unwrap().to_string(), "P0Y1M2DT3H4M5S");
        assert_eq!(interval.start.to_rfc3339(), "2021-01-01T01:02:03+00:00");
        assert_eq!(
            interval.randomize_start.unwrap().to_string(),
            "P0Y0M0DT0H3M0S"
        );
    }
}
