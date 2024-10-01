//! Descriptions of temporal periods

use crate::{values_map::ValuesMap, Duration};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// An object defining a temporal window and a list of valuesMaps. if intervalPeriod present may set
/// temporal aspects of interval or override event.intervalPeriod.
#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Interval {
    /// A client generated number assigned an interval object. Not a sequence number.
    pub id: i32,
    /// Defines default start and durations of intervals.
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

/// Defines temporal aspects of intervals. A duration of default null indicates infinity. A
/// randomizeStart of default null indicates no randomization.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IntervalPeriod {
    /// The start time of an interval or set of intervals.
    #[serde(with = "crate::serde_rfc3339")]
    pub start: DateTime<Utc>,
    /// The duration of an interval or set of intervals.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub duration: Option<Duration>,
    /// Indicates a randomization time that may be applied to start.
    #[serde(skip_serializing_if = "Option::is_none")]
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
