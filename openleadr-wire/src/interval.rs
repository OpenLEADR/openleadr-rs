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
    // FIXME field not required
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
