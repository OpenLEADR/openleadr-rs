use chrono::{DateTime, Utc};
use openadr_wire::{
    event::{EventType, EventValuesMap},
    values_map::Value,
};

use openadr_client::{ProgramClient, Timeline};
use std::{error::Error, time::Duration};
use tokio::{
    select,
    sync::{
        mpsc,
        mpsc::{Receiver, Sender},
    },
};
use uuid::Uuid;

async fn wait_for_next_start(clock: &impl Clock, timeline: &Timeline) {
    let now = clock.now();

    let Some(next) = timeline.next_update(&now) else {
        return std::future::pending().await; // Wait forever
    };

    // if the wait time is negative, return immediately
    match (next - now).to_std() {
        Err(_) => {}
        Ok(wait_time) => tokio::time::sleep(wait_time).await,
    }
}

trait Clock {
    fn now(&self) -> DateTime<Utc>;
}

struct ChronoClock;

impl Clock for ChronoClock {
    fn now(&self) -> DateTime<Utc> {
        Utc::now()
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let client = openadr_client::Client::with_url("http://localhost:3000/".try_into()?, None);
    let program = client
        .get_program_by_id(&"program-1".parse().unwrap())
        .await?;

    // channel used to send new timelines
    let (sender, receiver) = mpsc::channel(1);
    let poll_interval = Duration::from_secs(30);
    tokio::spawn(poll_timeline(program, poll_interval, sender));

    let (output_sender, mut output_receiver) = mpsc::channel(1);
    tokio::spawn(update_listener(ChronoClock, receiver, output_sender));

    tokio::spawn(async move {
        while let Some(enforced_limits) = output_receiver.recv().await {
            eprintln!("received by mock everest: {:?}", enforced_limits);
        }
    });

    Ok(())
}

async fn poll_timeline(
    mut program: ProgramClient,
    poll_interval: Duration,
    sender: Sender<Timeline>,
) -> Result<(), openadr_client::Error> {
    loop {
        tokio::time::sleep(poll_interval).await;

        let timeline = program.get_timeline().await?;

        let Ok(_) = sender.send(timeline).await else {
            return Ok(());
        };
    }
}

async fn update_listener(
    clock: impl Clock,
    mut receiver: Receiver<Timeline>,
    sender: Sender<EnforcedLimits>,
) {
    let mut timeline = Timeline::new();
    loop {
        // wait for the next thing to respond to. That is either:
        //
        // - the next interval from our timeline is starting
        // - the timeline got updated
        select! {
            result = receiver.recv() => {
                match result {
                    None => break, // sender was dropped
                    Some(new_timeline) => timeline = new_timeline,
                }
            }
            () = wait_for_next_start(&clock, &timeline) => {
                //  fall through
            }
        }

        let now = clock.now();

        // normally, we expect this to return Some(_), but due to e.g. time synchronization,
        // we may wake up without any interval to actually send upstream.
        let Some(current) = timeline.at_datetime(&now) else {
            continue;
        };

        let mut schedule = Vec::new();
        let mut valid_until = None;

        for (range, interval) in timeline.iter() {
            // skip anything that is already complete
            if range.end < now {
                continue;
            }

            valid_until = Ord::max(valid_until, Some(range.end));

            if let Some(limits_to_root) = LimitsRes::try_from_event_values(interval.value_map) {
                let entry = ScheduleResEntry {
                    timestamp: range.start,
                    limits_to_root,
                };

                schedule.push(entry);
            }
        }

        let opt_limits = LimitsRes::try_from_event_values(current.1.value_map);
        if let (Some(valid_until), Some(limits_root_side)) = (valid_until, opt_limits) {
            let enforced_limits = EnforcedLimits {
                uuid: Uuid::new_v4().to_string(),
                valid_until,
                limits_root_side,
                schedule,
            };

            let Ok(()) = sender.send(enforced_limits).await else {
                break;
            };
        }
    }
}

// https://github.com/tdittr/everest-core/blob/openadr/types/energy.yaml#L213
#[derive(Debug, Clone, PartialEq)]
struct EnforcedLimits {
    uuid: String,
    valid_until: DateTime<Utc>,
    limits_root_side: LimitsRes,
    schedule: Vec<ScheduleResEntry>,
}

// https://github.com/tdittr/everest-core/blob/openadr/types/energy.yaml#L125
#[derive(Debug, Clone, Copy, PartialEq)]
struct ScheduleResEntry {
    timestamp: DateTime<Utc>,
    limits_to_root: LimitsRes,
}

// https://github.com/tdittr/everest-core/blob/openadr/types/energy.yaml#L46
#[derive(Debug, Clone, Copy, PartialEq)]
struct LimitsRes {
    // NOTE: that W is uppercase if we ever need to serialize this!
    total_power_w: f64,
}

impl LimitsRes {
    fn try_from_event_values(values: &[EventValuesMap]) -> Option<Self> {
        for EventValuesMap { value_type, values } in values {
            if let EventType::ImportCapacityLimit = value_type {
                let total_power_w = match &values[..] {
                    [Value::Integer(value)] => *value as f64,
                    [Value::Number(value)] => *value,
                    other => panic!("invalid values {other:?}"),
                };

                return Some(Self { total_power_w });
            }
        }

        None
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use openadr_wire::{
        event::{EventContent, EventInterval},
        interval::IntervalPeriod,
        program::{ProgramContent, ProgramId},
    };
    use std::sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    };

    struct TestingClock(AtomicU64);

    impl Clock for Arc<TestingClock> {
        fn now(&self) -> DateTime<Utc> {
            let millis = self.0.load(Ordering::Relaxed);
            chrono::DateTime::<Utc>::from_timestamp_millis(millis as i64).unwrap()
        }
    }

    impl TestingClock {
        pub fn new(now: DateTime<Utc>) -> Arc<Self> {
            Arc::new(Self(AtomicU64::new(
                now.timestamp_millis().try_into().unwrap(),
            )))
        }

        async fn advance(&self, duration: Duration) {
            self.0
                .fetch_add(duration.as_millis().try_into().unwrap(), Ordering::Relaxed);
            tokio::time::advance(duration).await;
        }
    }

    const HOUR: chrono::TimeDelta = chrono::TimeDelta::hours(1);
    const MINUTE: chrono::TimeDelta = chrono::TimeDelta::minutes(1);

    #[tokio::test(start_paused = true)]
    async fn test_everest_update() {
        let clock = TestingClock::new(chrono::DateTime::UNIX_EPOCH + (HOUR * 9) + (MINUTE * 42));
        let past = tokio::time::Instant::now();

        let (input_sender, input_receiver) = mpsc::channel(1);
        let (output_sender, mut output_receiver) = mpsc::channel(1);

        let handle = tokio::spawn(update_listener(
            Arc::clone(&clock),
            input_receiver,
            output_sender,
        ));
        assert!(!handle.is_finished());
        assert!(!output_receiver.is_closed());
        assert!(output_receiver.is_empty());

        assert_eq!(past, tokio::time::Instant::now());
        clock.advance(Duration::from_secs(60)).await;
        assert!(output_receiver.is_empty());

        let event1_ts = chrono::DateTime::UNIX_EPOCH + (HOUR * 9);
        let event2_ts = chrono::DateTime::UNIX_EPOCH + (HOUR * 10);

        let timeline = create_timeline(vec![(event1_ts, 42.0), (event2_ts, 21.0)]);
        input_sender.send(timeline).await.unwrap();

        let output = output_receiver.recv().await.unwrap();
        assert_eq!(output.limits_root_side.total_power_w, 42.0);
        assert_eq!(
            output.schedule,
            vec![
                ScheduleResEntry {
                    timestamp: event1_ts,
                    limits_to_root: LimitsRes {
                        total_power_w: 42.0
                    }
                },
                ScheduleResEntry {
                    timestamp: event2_ts,
                    limits_to_root: LimitsRes {
                        total_power_w: 21.0
                    }
                }
            ]
        );

        clock.advance(Duration::from_secs(60 * 60)).await;
        let output = output_receiver.recv().await.unwrap();
        assert_eq!(output.limits_root_side.total_power_w, 21.0);
        assert_eq!(
            output.schedule,
            vec![ScheduleResEntry {
                timestamp: event2_ts,
                limits_to_root: LimitsRes {
                    total_power_w: 21.0
                }
            }]
        );
    }

    fn create_timeline(entries: Vec<(DateTime<Utc>, f64)>) -> Timeline {
        let intervals = entries
            .into_iter()
            .map(|(start_time, value)| EventInterval {
                id: 0,
                interval_period: Some(IntervalPeriod::new(start_time)),
                payloads: vec![EventValuesMap {
                    value_type: EventType::ImportCapacityLimit,
                    values: vec![Value::Number(value)],
                }],
            })
            .collect();

        let program = ProgramContent::new("Limits for Arthur Dent");
        let event = EventContent::new(ProgramId::new("ad").unwrap(), intervals);
        let events = vec![&event];

        Timeline::from_events(&program, events).unwrap()
    }
}
