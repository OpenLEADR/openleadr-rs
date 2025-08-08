use std::{collections::HashSet, ops::Range};

use chrono::{DateTime, Utc};
use tracing::warn;

use openleadr_wire::{
    event::{EventContent, EventValuesMap, Priority},
    interval::IntervalPeriod,
    Program,
};

#[derive(Debug, Clone, PartialEq, Eq)]
struct InternalInterval {
    /// Id so that split intervals with a randomized start don't start randomly twice
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
    /// Create an empty [`Timeline`]
    pub fn new() -> Self {
        Self {
            data: rangemap::RangeMap::new(),
        }
    }

    /// Creates a [`Timeline`] form a [`Program`], and the [`Event`](crate::Event)s that belong to it.
    ///
    /// It sorts events according to their priority and builds the timeline accordingly.
    /// The timeline can have gaps if the intervals in the events contain gaps.
    /// The event with the highest priority always takes presence at a specific time point.
    /// Therefore, a long-lasting, low-priority event can be interrupted by a short, high-priority event,
    /// for example.
    /// In this case, the long-lasting event will be split into two parts,
    /// such that the high-priority event fits in between.
    ///
    /// ```text
    /// Input:
    /// |------------------------long, low prio--------------------------|    |--another-interval--|
    ///                    |-----short, high prio---|
    /// Result:
    /// |--long, low prio--|-----short, high prio---|---long, low prio---|    |--another-interval--|
    /// ```
    ///
    /// This function logs at `warn` level if provided with an [`Event`](crate::Event)
    /// those [`program_id`](crate::EventContent::program_id) does not match with the [`Program::id`].
    /// The corresponding event will be ignored then building the timeline.
    ///
    /// This function also logs at `warn`
    /// level if there are two overlapping events at the same priority.
    /// There is no guarantee which event will take precedence, though in the current implementation
    /// the event stored later in the `events` param will take precedence.
    ///
    /// There must be an [`IntervalPeriod`] present on the event level,
    /// or the individual intervals.
    /// If both are specified, the individual period takes precedence over the one specified in the event.
    /// If for an interval, there is no period present, and none specified in the event,
    /// then this function will return [`None`]
    pub fn from_events(program: &Program, mut events: Vec<&EventContent>) -> Option<Self> {
        let mut data = Self::default();

        events.sort_by_key(|e| e.priority);

        for (id, event) in events.iter().enumerate() {
            if event.program_id != program.id {
                warn!(?event, %program.id, "skipping event that does not belong into the program; different program id");
                continue;
            }

            let default_period = event.interval_period.as_ref();

            let mut current_start = default_period.map(|p| p.start);

            for event_interval in &event.intervals {
                // use the event interval period when the interval doesn't specify one
                let (start, duration, randomize_start) =
                    match event_interval.interval_period.as_ref() {
                        Some(IntervalPeriod {
                            start,
                            duration,
                            randomize_start,
                        }) => (start, duration, randomize_start),
                        None => (
                            &current_start?,
                            &default_period?.duration,
                            &default_period?.randomize_start,
                        ),
                    };

                let range = match duration {
                    Some(duration) => *start..*start + duration.to_chrono_at_datetime(*start),
                    None => *start..DateTime::<Utc>::MAX_UTC,
                };

                current_start = Some(range.end);

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

    /// Get an iterator over the [`Interval`]s in this [`Timeline`]
    pub fn iter(&self) -> Iter<'_> {
        Iter {
            iter: self.data.iter(),
            seen: HashSet::default(),
        }
    }

    /// Returns the [`Interval`] applicable at the requested time point and the range it is valid for.
    pub fn at_datetime(
        &self,
        datetime: &DateTime<Utc>,
    ) -> Option<(&Range<DateTime<Utc>>, Interval<'_>)> {
        let (range, internal_interval) = self.data.get_key_value(datetime)?;

        let interval = Interval {
            randomize_start: internal_interval.randomize_start,
            value_map: &internal_interval.value_map,
        };

        Some((range, interval))
    }

    /// Returns the time when to next change takes effect.
    ///
    /// **Example:**
    /// ```text
    ///   |--interval 1--|---interval 2---|       |---interval 3---|
    ///   ↑              ↑                ↑       ↑                ↑
    /// 08:03          09:56            10:59   11:01            12:00
    /// ```
    /// For the timeline illustrated above,
    /// * `next_update(09:56)` would return `Some(10:59)`
    /// * `next_update(10:00)` would return `Some(10:59)`
    /// * `next_update(11:00)` would return `Some(11:01)`
    /// * `next_update(12:00)` would return `None`
    /// * `next_update(12:01)` would return `None`
    pub fn next_update(&self, datetime: &DateTime<Utc>) -> Option<DateTime<Utc>> {
        if let Some((k, _)) = self.at_datetime(datetime) {
            return Some(k.end);
        }

        let (last_range, _) = self.data.last_range_value()?;

        let (range, _) = self.data.overlapping(*datetime..last_range.end).next()?;

        Some(range.start)
    }
}

/// Holds the data stored in a [`Timeline`].
///
/// This data type is returned by the Iterator over the [`Timeline`] and by the [`at_datetime`](Timeline::at_datetime)
/// method.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Interval<'a> {
    randomize_start: Option<chrono::Duration>,
    value_map: &'a [EventValuesMap],
}

impl Interval<'_> {
    /// Indicates a randomization time that may be applied to start.
    pub fn randomize_start(&self) -> Option<chrono::Duration> {
        self.randomize_start
    }

    /// The actual values that are active during this interval
    pub fn value_map(&self) -> &[EventValuesMap] {
        self.value_map
    }
}

/// Iterator over [`Timeline`].
///
/// **Important:** The specification does not specify how to handle the `randomize_start`
/// for overlapping intervals.
/// This implementation sets the [`randomize_start`](Interval::randomize_start)
/// value to [`None`] if it's not the first part of that interval.
/// A single interval can be split in multiple parts
/// if it is partly covered by an event with higher priority.
/// See [`Timeline::from_events`].
///
/// **Example:**
/// ```text
/// |----interval 1----|-----interval 2---|----interval 1----|
/// ```
/// If `interval 1` has a `randomize_start` specified,
/// this iterator will only set this in the first part, but `None` in the second.
pub struct Iter<'a> {
    iter: rangemap::map::Iter<'a, DateTime<Utc>, InternalInterval>,
    seen: HashSet<u32>,
}

impl<'a> Iterator for Iter<'a> {
    type Item = (&'a Range<DateTime<Utc>>, Interval<'a>);

    fn next(&mut self) -> Option<Self::Item> {
        let (range, internal) = self.iter.next()?;

        let interval = Interval {
            // only the first occurrence of an id should randomize its start
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

    use super::*;
    use openleadr_wire::{
        event::EventInterval,
        program::{ProgramContent, ProgramId},
        values_map::Value,
    };

    fn test_program_id() -> ProgramId {
        ProgramId::new("test-program-id").unwrap()
    }

    fn test_event_content(range: Range<u32>, value: i64) -> EventContent {
        EventContent::new(
            test_program_id(),
            vec![event_interval_with_value(range, value)],
        )
    }

    fn test_program(name: &str) -> Program {
        Program {
            id: test_program_id(),
            created_date_time: Default::default(),
            modification_date_time: Default::default(),
            content: ProgramContent::new(name),
        }
    }

    fn event_interval_with_value(range: Range<u32>, value: i64) -> EventInterval {
        EventInterval {
            id: range.start as _,
            interval_period: Some(IntervalPeriod {
                start: DateTime::UNIX_EPOCH + Duration::hours(range.start.into()),
                duration: Some(openleadr_wire::Duration::hours(
                    (range.end - range.start) as _,
                )),
                randomize_start: None,
            }),
            payloads: vec![EventValuesMap {
                value_type: openleadr_wire::event::EventType::Price,
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
                    value_type: openleadr_wire::event::EventType::Price,
                    values: vec![Value::Integer(value)],
                }],
                priority,
            },
        )
    }

    // The spec does not specify the behavior when two intervals with the same priority overlap.
    // Our current implementation uses `RangeMap`, and its behavior is to overwrite the existing
    // range with a new one.
    // In other words: the event which is inserted last wins.
    #[test]
    fn overlap_same_priority() {
        let program = test_program("p");

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

        let tl = Timeline::from_events(&test_program("p"), vec![&event1, &event2]).unwrap();
        assert_eq!(
            tl.data.into_iter().collect::<Vec<_>>(),
            vec![
                interval_with_value(1, 0..10, 42, Priority::new(1)),
                interval_with_value(0, 10..15, 43, Priority::new(2)),
            ],
            "a lower priority event MUST NOT overwrite a higher priority one",
        );

        let tl = Timeline::from_events(&test_program("p"), vec![&event2, &event1]).unwrap();
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

        let tl = Timeline::from_events(&test_program("p"), vec![&event1, &event2]).unwrap();
        assert_eq!(
            tl.data.into_iter().collect::<Vec<_>>(),
            vec![
                interval_with_value(0, 0..5, 42, Priority::new(2)),
                interval_with_value(1, 5..15, 43, Priority::new(1)),
            ],
            "a higher priority event MUST overwrite a lower priority one",
        );

        let tl = Timeline::from_events(&test_program("p"), vec![&event2, &event1]).unwrap();
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
    fn default_interval() {
        let program = test_program("p");

        let event_intervals = vec![
            EventInterval::new(
                0,
                vec![EventValuesMap {
                    value_type: openleadr_wire::event::EventType::Price,
                    values: vec![Value::Number(1.23)],
                }],
            ),
            EventInterval::new(
                1,
                vec![EventValuesMap {
                    value_type: openleadr_wire::event::EventType::Simple,
                    values: vec![Value::Number(2.34)],
                }],
            ),
        ];

        let mut event = EventContent::new(program.id.clone(), event_intervals);

        event.interval_period = Some(IntervalPeriod {
            start: DateTime::UNIX_EPOCH,
            duration: Some(openleadr_wire::Duration::hours(5.)),
            randomize_start: None,
        });

        let timeline = Timeline::from_events(&program, vec![&event]).unwrap();

        let interval = timeline
            .at_datetime(&(DateTime::UNIX_EPOCH + Duration::hours(2)))
            .unwrap();
        assert_eq!(
            interval.1.value_map[0].value_type,
            openleadr_wire::event::EventType::Price
        );

        let interval = timeline
            .at_datetime(&(DateTime::UNIX_EPOCH + Duration::hours(8)))
            .unwrap();
        assert_eq!(
            interval.1.value_map[0].value_type,
            openleadr_wire::event::EventType::Simple
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
                        duration: Some(openleadr_wire::Duration::hours(
                            (range.end - range.start) as _,
                        )),
                        randomize_start: Some(openleadr_wire::Duration::hours(5.0)),
                    }),
                    payloads: vec![EventValuesMap {
                        value_type: openleadr_wire::event::EventType::Price,
                        values: vec![Value::Integer(value)],
                    }],
                }],
            )
        };

        let tl = Timeline::from_events(&test_program("p"), vec![&event1, &event2]).unwrap();
        assert_eq!(
            tl.iter().map(|(_, i)| i).collect::<Vec<_>>(),
            vec![
                Interval {
                    randomize_start: Some(Duration::hours(5)),
                    value_map: &[EventValuesMap {
                        value_type: openleadr_wire::event::EventType::Price,
                        values: vec![Value::Integer(43)],
                    }],
                },
                Interval {
                    randomize_start: None,
                    value_map: &[EventValuesMap {
                        value_type: openleadr_wire::event::EventType::Price,
                        values: vec![Value::Integer(42)],
                    }],
                },
                Interval {
                    randomize_start: None,
                    value_map: &[EventValuesMap {
                        value_type: openleadr_wire::event::EventType::Price,
                        values: vec![Value::Integer(43)],
                    }],
                },
            ],
            "when an event is split, only the first interval should retain `randomize_start`",
        );
    }
}
