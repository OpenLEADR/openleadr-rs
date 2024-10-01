#![allow(dead_code)]
use std::{collections::HashSet, ops::Range};

use chrono::{DateTime, Utc};
use tracing::warn;

use openadr_wire::{
    event::{EventContent, EventValuesMap, Priority},
    interval::IntervalPeriod,
    program::ProgramContent,
};

#[derive(Debug, Clone, PartialEq, Eq)]
struct InternalInterval {
    /// Id so that split itervals with a randomized start don't start randomly twice
    id: u32,
    /// Relative priority of event
    priority: Priority,
    /// Indicates a randomization time that may be applied to start.
    randomize_start: Option<chrono::Duration>,
    /// The actual values that are active during this interval
    value_map: Vec<EventValuesMap>,
}

/// A sequence of ordered, non-overlapping intervals and associated values.
///
/// Intervals are sorted by their timestamp. The intervals will not overlap, but there may be gaps
/// between intervals.
#[allow(unused)]
#[derive(Clone, Default, Debug)]
pub struct Timeline {
    data: rangemap::RangeMap<DateTime<Utc>, InternalInterval>,
}

impl Timeline {
    pub fn new() -> Self {
        Self {
            data: rangemap::RangeMap::new(),
        }
    }

    /// Returns:
    ///
    /// - `None` if no interval is specified in the input
    /// - `Some(timeline)` otherwise
    pub fn from_events(program: &ProgramContent, mut events: Vec<&EventContent>) -> Option<Self> {
        let mut data = Self::default();

        events.sort_by_key(|e| e.priority);

        for (id, event) in events.iter().enumerate() {
            // SPEC ASSUMPTION: At least one of the following `interval_period`s must be given on the program,
            // on the event, or on the interval
            let default_period = event
                .interval_period
                .as_ref()
                .or(program.interval_period.as_ref());

            for event_interval in &event.intervals {
                // use the even't interval period when the interval doesn't specify one
                let period = event_interval.interval_period.as_ref().or(default_period)?;

                let IntervalPeriod {
                    start,
                    duration,
                    randomize_start,
                } = period;

                let range = match duration {
                    Some(duration) => *start..*start + duration.to_chrono_at_datetime(*start),
                    None => *start..DateTime::<Utc>::MAX_UTC,
                };

                let interval = InternalInterval {
                    id: id as u32,
                    randomize_start: randomize_start
                        .as_ref()
                        .map(|d| d.to_chrono_at_datetime(*start)),
                    value_map: event_interval.payloads.clone(),
                    priority: event.priority,
                };

                for (existing_range, existing) in data.data.overlapping(&range) {
                    if existing.priority == event.priority {
                        warn!(?existing_range, ?existing, new_range = ?range, new = ?interval, "Overlapping ranges with equal priority");
                    }
                }

                data.data.insert(range, interval);
            }
        }

        Some(data)
    }

    pub fn iter(&self) -> Iter<'_> {
        Iter {
            iter: self.data.iter(),
            seen: HashSet::default(),
        }
    }

    pub fn at_datetime(
        &self,
        datetime: &DateTime<Utc>,
    ) -> Option<(&Range<DateTime<Utc>>, Interval)> {
        let (range, internal_interval) = self.data.get_key_value(datetime)?;

        let interval = Interval {
            randomize_start: internal_interval.randomize_start,
            value_map: &internal_interval.value_map,
        };

        Some((range, interval))
    }

    pub fn next_update(&self, datetime: &DateTime<Utc>) -> Option<DateTime<Utc>> {
        if let Some((k, _)) = self.at_datetime(datetime) {
            return Some(k.end);
        }

        let (last_range, _) = self.data.last_range_value()?;

        let (range, _) = self.data.overlapping(*datetime..last_range.end).next()?;

        Some(range.start)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Interval<'a> {
    /// Indicates a randomization time that may be applied to start.
    pub randomize_start: Option<chrono::Duration>,
    /// The actual values that are active during this interval
    pub value_map: &'a [EventValuesMap],
}

pub struct Iter<'a> {
    iter: rangemap::map::Iter<'a, DateTime<Utc>, InternalInterval>,
    seen: HashSet<u32>,
}

impl<'a> Iterator for Iter<'a> {
    type Item = (&'a Range<DateTime<Utc>>, Interval<'a>);

    fn next(&mut self) -> Option<Self::Item> {
        let (range, internal) = self.iter.next()?;

        let interval = Interval {
            // only the first occurence of an id should randomize its start
            randomize_start: match self.seen.insert(internal.id) {
                true => internal.randomize_start,
                false => None,
            },
            value_map: &internal.value_map,
        };

        Some((range, interval))
    }
}

#[cfg(test)]
mod test {
    use std::ops::Range;

    use chrono::{DateTime, Duration, Utc};

    use openadr_wire::{event::EventInterval, program::ProgramId, values_map::Value};

    use super::*;

    fn test_program_id() -> ProgramId {
        ProgramId::new("test-program-id").unwrap()
    }

    fn test_event_content(range: Range<u32>, value: i64) -> EventContent {
        EventContent::new(
            test_program_id(),
            vec![event_interval_with_value(range, value)],
        )
    }

    fn event_interval_with_value(range: Range<u32>, value: i64) -> EventInterval {
        EventInterval {
            id: range.start as _,
            interval_period: Some(IntervalPeriod {
                start: DateTime::UNIX_EPOCH + Duration::hours(range.start.into()),
                duration: Some(openadr_wire::Duration::hours(
                    (range.end - range.start) as _,
                )),
                randomize_start: None,
            }),
            payloads: vec![EventValuesMap {
                value_type: openadr_wire::event::EventType::Price,
                values: vec![Value::Integer(value)],
            }],
        }
    }

    fn interval_with_value(
        id: u32,
        range: Range<u32>,
        value: i64,
        priority: Priority,
    ) -> (Range<DateTime<Utc>>, InternalInterval) {
        let start = DateTime::UNIX_EPOCH + Duration::hours(range.start.into());
        let end = DateTime::UNIX_EPOCH + Duration::hours(range.end.into());

        (
            start..end,
            InternalInterval {
                id,
                randomize_start: None,
                value_map: vec![EventValuesMap {
                    value_type: openadr_wire::event::EventType::Price,
                    values: vec![Value::Integer(value)],
                }],
                priority,
            },
        )
    }

    // the spec does not specify the behavior when two intervals with the same priority overlap.
    // Our current implementation uses `RangeMap`, and its behavior is to overwrite the existing
    // range with a new one. In other words: the event that is inserted last wins.
    #[test]
    fn overlap_same_priority() {
        let program = ProgramContent::new("p");

        let event1 = test_event_content(0..10, 42);
        let event2 = test_event_content(5..15, 43);

        // first come, last serve
        let tl1 = Timeline::from_events(&program, vec![&event1, &event2]).unwrap();
        assert_eq!(
            tl1.data.into_iter().collect::<Vec<_>>(),
            vec![
                interval_with_value(0, 0..5, 42, Priority::UNSPECIFIED),
                interval_with_value(1, 5..15, 43, Priority::UNSPECIFIED),
            ]
        );

        // first come, last serve
        let tl2 = Timeline::from_events(&program, vec![&event2, &event1]).unwrap();
        assert_eq!(
            tl2.data.into_iter().collect::<Vec<_>>(),
            vec![
                interval_with_value(1, 0..10, 42, Priority::UNSPECIFIED),
                interval_with_value(0, 10..15, 43, Priority::UNSPECIFIED),
            ]
        );
    }

    #[test]
    fn overlap_lower_priority() {
        let event1 = test_event_content(0..10, 42).with_priority(Priority::new(1));
        let event2 = test_event_content(5..15, 43).with_priority(Priority::new(2));

        let tl = Timeline::from_events(&ProgramContent::new("p"), vec![&event1, &event2]).unwrap();
        assert_eq!(
            tl.data.into_iter().collect::<Vec<_>>(),
            vec![
                interval_with_value(1, 0..10, 42, Priority::new(1)),
                interval_with_value(0, 10..15, 43, Priority::new(2)),
            ],
            "a lower priority event MUST NOT overwrite a higher priority one",
        );

        let tl = Timeline::from_events(&ProgramContent::new("p"), vec![&event2, &event1]).unwrap();
        assert_eq!(
            tl.data.into_iter().collect::<Vec<_>>(),
            vec![
                interval_with_value(1, 0..10, 42, Priority::new(1)),
                interval_with_value(0, 10..15, 43, Priority::new(2)),
            ],
            "a lower priority event MUST NOT overwrite a higher priority one",
        );
    }

    #[test]
    fn overlap_higher_priority() {
        let event1 = test_event_content(0..10, 42).with_priority(Priority::new(2));
        let event2 = test_event_content(5..15, 43).with_priority(Priority::new(1));

        let tl = Timeline::from_events(&ProgramContent::new("p"), vec![&event1, &event2]).unwrap();
        assert_eq!(
            tl.data.into_iter().collect::<Vec<_>>(),
            vec![
                interval_with_value(0, 0..5, 42, Priority::new(2)),
                interval_with_value(1, 5..15, 43, Priority::new(1)),
            ],
            "a higher priority event MUST overwrite a lower priority one",
        );

        let tl = Timeline::from_events(&ProgramContent::new("p"), vec![&event2, &event1]).unwrap();
        assert_eq!(
            tl.data.into_iter().collect::<Vec<_>>(),
            vec![
                interval_with_value(0, 0..5, 42, Priority::new(2)),
                interval_with_value(1, 5..15, 43, Priority::new(1)),
            ],
            "a higher priority event MUST overwrite a lower priority one",
        );
    }

    #[test]
    fn randomize_start_not_duplicated() {
        let event1 = test_event_content(5..10, 42).with_priority(Priority::MAX);

        let event2 = {
            let range = 0..15;
            let value = 43;
            EventContent::new(
                test_program_id(),
                vec![EventInterval {
                    id: range.start as _,
                    interval_period: Some(IntervalPeriod {
                        start: DateTime::UNIX_EPOCH + Duration::hours(range.start.into()),
                        duration: Some(openadr_wire::Duration::hours(
                            (range.end - range.start) as _,
                        )),
                        randomize_start: Some(openadr_wire::Duration::hours(5.0)),
                    }),
                    payloads: vec![EventValuesMap {
                        value_type: openadr_wire::event::EventType::Price,
                        values: vec![Value::Integer(value)],
                    }],
                }],
            )
        };

        let tl = Timeline::from_events(&ProgramContent::new("p"), vec![&event1, &event2]).unwrap();
        assert_eq!(
            tl.iter().map(|(_, i)| i).collect::<Vec<_>>(),
            vec![
                Interval {
                    randomize_start: Some(Duration::hours(5)),
                    value_map: &[EventValuesMap {
                        value_type: openadr_wire::event::EventType::Price,
                        values: vec![Value::Integer(43)],
                    }],
                },
                Interval {
                    randomize_start: None,
                    value_map: &[EventValuesMap {
                        value_type: openadr_wire::event::EventType::Price,
                        values: vec![Value::Integer(42)],
                    }],
                },
                Interval {
                    randomize_start: None,
                    value_map: &[EventValuesMap {
                        value_type: openadr_wire::event::EventType::Price,
                        values: vec![Value::Integer(43)],
                    }],
                },
            ],
            "when an event is split, only the first interval should retain `randomize_start`",
        );
    }
}
