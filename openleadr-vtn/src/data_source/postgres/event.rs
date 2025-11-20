use crate::{
    api::event::QueryParams,
    data_source::{
        postgres::{get_ven_targets, to_json_value},
        Crud, EventCrud,
    },
    error::AppError,
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use openleadr_wire::{
    event::{EventId, EventRequest, Priority},
    target::Target,
    ClientId, Event,
};
use sqlx::{error::BoxDynError, PgPool};
use std::str::FromStr;
use tracing::error;

#[async_trait]
impl EventCrud for PgEventStorage {}

pub(crate) struct PgEventStorage {
    db: PgPool,
}

impl From<PgPool> for PgEventStorage {
    fn from(db: PgPool) -> Self {
        Self { db }
    }
}

#[derive(Debug)]
struct PostgresEvent {
    id: String,
    created_date_time: DateTime<Utc>,
    modification_date_time: DateTime<Utc>,
    program_id: String,
    event_name: Option<String>,
    duration: Option<String>,
    priority: Priority,
    targets: Vec<Target>,
    report_descriptors: Option<serde_json::Value>,
    payload_descriptors: Option<serde_json::Value>,
    interval_period: Option<serde_json::Value>,
    intervals: serde_json::Value,
}

impl TryFrom<PostgresEvent> for Event {
    type Error = AppError;

    #[tracing::instrument(name = "TryFrom<PostgresEvent> for Event")]
    fn try_from(value: PostgresEvent) -> Result<Self, Self::Error> {
        let report_descriptors = match value.report_descriptors {
            None => None,
            Some(t) => serde_json::from_value(t)
                .inspect_err(|err| {
                    error!(
                        ?err,
                        "Failed to deserialize JSON from DB to `Vec<ReportDescriptor>`"
                    )
                })
                .map_err(AppError::SerdeJsonInternalServerError)?,
        };

        let payload_descriptors = match value.payload_descriptors {
            None => None,
            Some(t) => serde_json::from_value(t)
                .inspect_err(|err| {
                    error!(
                        ?err,
                        "Failed to deserialize JSON from DB to `Vec<EventPayloadDescriptor>`"
                    )
                })
                .map_err(AppError::SerdeJsonInternalServerError)?,
        };

        let interval_period = match value.interval_period {
            None => None,
            Some(t) => serde_json::from_value(t)
                .inspect_err(|err| {
                    error!(
                        ?err,
                        "Failed to deserialize JSON from DB to `IntervalPeriod`"
                    )
                })
                .map_err(AppError::SerdeJsonInternalServerError)?,
        };

        Ok(Self {
            id: EventId::from_str(&value.id)?,
            created_date_time: value.created_date_time,
            modification_date_time: value.modification_date_time,
            content: EventRequest {
                program_id: value.program_id.parse()?,
                event_name: value.event_name,
                duration: value
                    .duration
                    .map(|d| FromStr::from_str(&d))
                    .transpose()
                    .map_err(|err| {
                        AppError::Sql(sqlx::Error::Decode(BoxDynError::from(format!(
                            "Failed to decode ISO8601 formatted duration stored in DB: {err:?}"
                        ))))
                    })?,
                priority: value.priority,
                targets: value.targets,
                report_descriptors,
                payload_descriptors,
                interval_period,
                intervals: serde_json::from_value(value.intervals)
                    .map_err(AppError::SerdeJsonInternalServerError)?,
            },
        })
    }
}

#[async_trait]
impl Crud for PgEventStorage {
    type Type = Event;
    type Id = EventId;
    type NewType = EventRequest;
    type Error = AppError;
    type Filter = QueryParams;
    type PermissionFilter = Option<ClientId>;

    async fn create(
        &self,
        new: Self::NewType,
        _client_id: &Self::PermissionFilter,
    ) -> Result<Self::Type, Self::Error> {
        Ok(sqlx::query_as!(
            PostgresEvent,
            r#"
            INSERT INTO event (id, created_date_time, modification_date_time, program_id, event_name, priority, targets, report_descriptors, payload_descriptors, interval_period, intervals, duration)
            VALUES (gen_random_uuid(), now(), now(), $1, $2, $3, $4, $5, $6, $7, $8, $9)
            RETURNING
                id,
                created_date_time,
                modification_date_time,
                program_id,
                event_name,
                priority,
                targets as "targets:Vec<Target>",
                report_descriptors,
                payload_descriptors,
                interval_period,
                intervals,
                duration
            "#,
            new.program_id.as_str(),
            new.event_name,
            Into::<Option<i64>>::into(new.priority),
            new.targets.as_slice() as &[Target],
            to_json_value(new.report_descriptors)?,
            to_json_value(new.payload_descriptors)?,
            to_json_value(new.interval_period)?,
            serde_json::to_value(&new.intervals).map_err(AppError::SerdeJsonBadRequest)?,
            new.duration.map(|d| d.to_string()),
        )
            .fetch_one(&self.db)
            .await?
            .try_into()?
        )
    }

    async fn retrieve(
        &self,
        id: &Self::Id,
        client_id: &Self::PermissionFilter,
    ) -> Result<Self::Type, Self::Error> {
        let ven_targets = get_ven_targets(self.db.clone(), client_id).await?;

        Ok(sqlx::query_as!(
            PostgresEvent,
            r#"
            SELECT e.id,
                   e.created_date_time,
                   e.modification_date_time,
                   e.program_id,
                   e.event_name,
                   e.priority,
                   e.targets as "targets:Vec<Target>",
                   e.report_descriptors,
                   e.payload_descriptors,
                   e.interval_period,
                   e.intervals,
                   e.duration
            FROM event e
            WHERE e.id = $1
              AND e.targets @> $2
            "#,
            id.as_str(),
            ven_targets as _
        )
        .fetch_one(&self.db)
        .await?
        .try_into()?)
    }

    async fn retrieve_all(
        &self,
        filter: &Self::Filter,
        client_id: &Self::PermissionFilter,
    ) -> Result<Vec<Self::Type>, Self::Error> {
        let ven_targets = get_ven_targets(self.db.clone(), client_id).await?;

        Ok(sqlx::query_as!(
            PostgresEvent,
            r#"
            SELECT e.id,
                   e.created_date_time,
                   e.modification_date_time,
                   e.program_id,
                   e.event_name,
                   e.priority,
                   e.targets as "targets:Vec<Target>",
                   e.report_descriptors,
                   e.payload_descriptors,
                   e.interval_period,
                   e.intervals,
                   e.duration
            FROM event e
            WHERE ($1::text IS NULL OR e.program_id like $1)
              AND e.targets @> $2
              -- TODO What should happen if the event has no targets set?
              --  Shall all VENs have access to it or only those without any targets?
              AND e.targets @> $3
            GROUP BY e.id, e.priority, e.created_date_time
            ORDER BY priority ASC , created_date_time DESC
            OFFSET $4 LIMIT $5
            "#,
            filter.program_id.as_ref().map(|id| id.as_str()),
            filter.targets.as_deref() as _,
            ven_targets as _,
            filter.skip,
            filter.limit
        )
        .fetch_all(&self.db)
        .await?
        .into_iter()
        .map(TryInto::try_into)
        .collect::<Result<_, _>>()?)
    }

    async fn update(
        &self,
        id: &Self::Id,
        new: Self::NewType,
        _client_id: &Self::PermissionFilter,
    ) -> Result<Self::Type, Self::Error> {
        Ok(sqlx::query_as!(
            PostgresEvent,
            r#"
            UPDATE event
            SET modification_date_time = now(),
                program_id = $2,
                event_name = $3,
                priority = $4,
                targets = $5,
                report_descriptors = $6,
                payload_descriptors = $7,
                interval_period = $8,
                intervals = $9,
                duration = $10
            WHERE id = $1
            RETURNING
                id,
                created_date_time,
                modification_date_time,
                program_id,
                event_name,
                priority,
                targets as "targets:Vec<Target>",
                report_descriptors,
                payload_descriptors,
                interval_period,
                intervals,
                duration
            "#,
            id.as_str(),
            new.program_id.as_str(),
            new.event_name,
            Into::<Option<i64>>::into(new.priority),
            new.targets.as_slice() as &[Target],
            to_json_value(new.report_descriptors)?,
            to_json_value(new.payload_descriptors)?,
            to_json_value(new.interval_period)?,
            serde_json::to_value(&new.intervals).map_err(AppError::SerdeJsonBadRequest)?,
            new.duration.map(|d| d.to_string())
        )
        .fetch_one(&self.db)
        .await?
        .try_into()?)
    }

    async fn delete(
        &self,
        id: &Self::Id,
        _client_id: &Self::PermissionFilter,
    ) -> Result<Self::Type, Self::Error> {
        Ok(sqlx::query_as!(
            PostgresEvent,
            r#"
            DELETE
            FROM event
            WHERE id = $1
            RETURNING
                id,
                created_date_time,
                modification_date_time,
                program_id,
                event_name,
                priority,
                targets as "targets:Vec<Target>",
                report_descriptors,
                payload_descriptors,
                interval_period,
                intervals,
                duration
            "#,
            id.as_str()
        )
        .fetch_one(&self.db)
        .await?
        .try_into()?)
    }
}

#[cfg(test)]
#[cfg(feature = "live-db-test")]
mod tests {
    use sqlx::PgPool;
    use std::str::FromStr;

    use crate::{
        api::{event::QueryParams, TargetQueryParams},
        data_source::{postgres::event::PgEventStorage, Crud},
        error::AppError,
        jwt::{Claims, User},
    };
    use chrono::{DateTime, Duration, Utc};
    use openleadr_wire::{
        event::{EventInterval, EventRequest, EventType, EventValuesMap},
        interval::IntervalPeriod,
        target::Target,
        values_map::Value,
        Event,
    };

    impl Default for QueryParams {
        fn default() -> Self {
            Self {
                program_id: None,
                targets: TargetQueryParams(None),
                skip: 0,
                limit: 50,
            }
        }
    }

    fn event_1() -> Event {
        Event {
            id: "event-1".parse().unwrap(),
            created_date_time: "2024-07-25 08:31:10.776000 +00:00".parse().unwrap(),
            modification_date_time: "2024-07-25 08:31:10.776000 +00:00".parse().unwrap(),
            content: EventRequest {
                program_id: "program-1".parse().unwrap(),
                event_name: Some("event-1-name".to_string()),
                duration: None,
                priority: Some(4).into(),
                targets: vec![
                    Target::from_str("group-1").unwrap(),
                    Target::from_str("private-value").unwrap(),
                ],
                report_descriptors: None,
                payload_descriptors: None,
                interval_period: Some(IntervalPeriod {
                    start: "2023-06-15T09:30:00+00:00".parse().unwrap(),
                    duration: Some("P0Y0M0DT1H0M0S".parse().unwrap()),
                    randomize_start: Some("P0Y0M0DT1H0M0S".parse().unwrap()),
                }),
                intervals: Some(vec![EventInterval {
                    id: 3,
                    interval_period: Some(IntervalPeriod {
                        start: "2023-06-15T09:30:00+00:00".parse().unwrap(),
                        duration: Some("P0Y0M0DT1H0M0S".parse().unwrap()),
                        randomize_start: Some("P0Y0M0DT1H0M0S".parse().unwrap()),
                    }),
                    payloads: vec![EventValuesMap {
                        value_type: EventType::Price,
                        values: vec![Value::Number(0.17)],
                    }],
                }]),
            },
        }
    }

    fn event_2() -> Event {
        Event {
            id: "event-2".parse().unwrap(),
            created_date_time: "2024-07-25 08:31:10.776000 +00:00".parse().unwrap(),
            modification_date_time: "2024-07-25 08:31:10.776000 +00:00".parse().unwrap(),
            content: EventRequest {
                program_id: "program-2".parse().unwrap(),
                event_name: Some("event-2-name".to_string()),
                duration: None,
                priority: None.into(),
                targets: vec![Target::from_str("target-1").unwrap()],
                report_descriptors: None,
                payload_descriptors: None,
                interval_period: None,
                intervals: Some(vec![EventInterval {
                    id: 3,
                    interval_period: None,
                    payloads: vec![EventValuesMap {
                        value_type: EventType::Private("SOME_PAYLOAD".to_string()),
                        values: vec![Value::String("value".to_string())],
                    }],
                }]),
            },
        }
    }

    fn event_3() -> Event {
        Event {
            id: "event-3".parse().unwrap(),
            content: EventRequest {
                program_id: "program-3".parse().unwrap(),
                event_name: Some("event-3-name".to_string()),
                ..event_2().content
            },
            ..event_2()
        }
    }

    mod get_all {
        use super::*;

        #[sqlx::test(fixtures("programs", "events"))]
        async fn default_get_all(db: PgPool) {
            let repo: PgEventStorage = db.into();
            let mut events = repo
                .retrieve_all(&Default::default(), &User(Claims::any_business_user()))
                .await
                .unwrap();
            assert_eq!(events.len(), 3);
            events.sort_by(|a, b| a.id.as_str().cmp(b.id.as_str()));
            assert_eq!(events, vec![event_1(), event_2(), event_3()]);
        }

        #[sqlx::test(fixtures("programs", "events"))]
        async fn limit_get_all(db: PgPool) {
            let repo: PgEventStorage = db.into();
            let events = repo
                .retrieve_all(
                    &QueryParams {
                        limit: 1,
                        ..Default::default()
                    },
                    &User(Claims::any_business_user()),
                )
                .await
                .unwrap();
            assert_eq!(events.len(), 1);
            assert_eq!(events, vec![event_1()]);
        }

        #[sqlx::test(fixtures("programs", "events"))]
        async fn skip_get_all(db: PgPool) {
            let repo: PgEventStorage = db.into();
            let events = repo
                .retrieve_all(
                    &QueryParams {
                        skip: 1,
                        ..Default::default()
                    },
                    &User(Claims::any_business_user()),
                )
                .await
                .unwrap();
            assert_eq!(events.len(), 2);

            let events = repo
                .retrieve_all(
                    &QueryParams {
                        skip: 20,
                        ..Default::default()
                    },
                    &User(Claims::any_business_user()),
                )
                .await
                .unwrap();
            assert_eq!(events.len(), 0);
        }

        #[sqlx::test(fixtures("programs", "events"))]
        async fn filter_target_get_all(db: PgPool) {
            let repo: PgEventStorage = db.into();

            let events = repo
                .retrieve_all(
                    &QueryParams {
                        targets: TargetQueryParams(Some(vec!["group-1".parse().unwrap()])),
                        ..Default::default()
                    },
                    &User(Claims::any_business_user()),
                )
                .await
                .unwrap();
            assert_eq!(events.len(), 1);
            assert_eq!(events, vec![event_1()]);

            let mut events = repo
                .retrieve_all(
                    &QueryParams {
                        targets: TargetQueryParams(Some(vec!["target-1".parse().unwrap()])),
                        ..Default::default()
                    },
                    &User(Claims::any_business_user()),
                )
                .await
                .unwrap();
            assert_eq!(events.len(), 2);
            events.sort_by(|a, b| a.id.as_str().cmp(b.id.as_str()));
            assert_eq!(events, vec![event_2(), event_3()]);

            let events = repo
                .retrieve_all(
                    &QueryParams {
                        targets: TargetQueryParams(Some(vec!["not-existent".parse().unwrap()])),
                        ..Default::default()
                    },
                    &User(Claims::any_business_user()),
                )
                .await
                .unwrap();
            assert_eq!(events.len(), 0);
        }

        #[sqlx::test(fixtures("programs"))]
        async fn filter_multiple_targets(db: PgPool) {
            let repo: PgEventStorage = db.into();

            let events = repo
                .retrieve_all(
                    &QueryParams {
                        targets: TargetQueryParams(Some(vec!["private-value".parse().unwrap()])),
                        ..Default::default()
                    },
                    &User(Claims::any_business_user()),
                )
                .await
                .unwrap();
            assert_eq!(events.len(), 0);
        }

        #[sqlx::test(fixtures("programs", "events"))]
        async fn filter_program_id_get_all(db: PgPool) {
            let repo: PgEventStorage = db.into();

            let events = repo
                .retrieve_all(
                    &QueryParams {
                        program_id: Some("program-1".parse().unwrap()),
                        ..Default::default()
                    },
                    &User(Claims::any_business_user()),
                )
                .await
                .unwrap();
            assert_eq!(events.len(), 1);
            assert_eq!(events, vec![event_1()]);

            let events = repo
                .retrieve_all(
                    &QueryParams {
                        program_id: Some("program-1".parse().unwrap()),
                        targets: TargetQueryParams(None),
                        ..Default::default()
                    },
                    &User(Claims::any_business_user()),
                )
                .await
                .unwrap();
            assert_eq!(events.len(), 1);
            assert_eq!(events, vec![event_1()]);

            let events = repo
                .retrieve_all(
                    &QueryParams {
                        program_id: Some("not-existent".parse().unwrap()),
                        ..Default::default()
                    },
                    &User(Claims::any_business_user()),
                )
                .await
                .unwrap();
            assert_eq!(events.len(), 0);
        }
    }

    mod get {
        use super::*;

        #[sqlx::test(fixtures("programs", "events"))]
        async fn get_existing(db: PgPool) {
            let repo: PgEventStorage = db.into();
            let event = repo
                .retrieve(
                    &"event-1".parse().unwrap(),
                    &User(Claims::any_business_user()),
                )
                .await
                .unwrap();
            assert_eq!(event, event_1());
        }

        #[sqlx::test(fixtures("programs", "events"))]
        async fn get_not_existing(db: PgPool) {
            let repo: PgEventStorage = db.into();
            let event = repo
                .retrieve(
                    &"not-existent".parse().unwrap(),
                    &User(Claims::any_business_user()),
                )
                .await;
            assert!(matches!(event, Err(AppError::NotFound)));
        }
    }

    mod add {
        use super::*;

        #[sqlx::test(fixtures("programs"))]
        async fn add(db: PgPool) {
            let repo: PgEventStorage = db.into();
            let event = repo
                .create(event_1().content, &User(Claims::any_business_user()))
                .await
                .unwrap();
            assert_eq!(event.content, event_1().content);
            assert!(event.created_date_time < Utc::now() + Duration::minutes(10));
            assert!(event.created_date_time > Utc::now() - Duration::minutes(10));
            assert!(event.modification_date_time < Utc::now() + Duration::minutes(10));
            assert!(event.modification_date_time > Utc::now() - Duration::minutes(10));
        }

        #[sqlx::test(fixtures("programs", "events"))]
        async fn add_existing_conflict_name(db: PgPool) {
            let repo: PgEventStorage = db.into();
            let event = repo
                .create(event_1().content, &User(Claims::any_business_user()))
                .await;
            assert!(event.is_ok());
        }
    }

    mod modify {
        use super::*;

        #[sqlx::test(fixtures("programs", "events"))]
        async fn updates_modify_time(db: PgPool) {
            let repo: PgEventStorage = db.into();
            let event = repo
                .update(
                    &"event-1".parse().unwrap(),
                    event_1().content,
                    &User(Claims::any_business_user()),
                )
                .await
                .unwrap();
            assert_eq!(event.content, event_1().content);
            assert_eq!(
                event.created_date_time,
                "2024-07-25 08:31:10.776000 +00:00"
                    .parse::<DateTime<Utc>>()
                    .unwrap()
            );
            assert!(event.modification_date_time < Utc::now() + Duration::minutes(10));
            assert!(event.modification_date_time > Utc::now() - Duration::minutes(10));
        }

        #[sqlx::test(fixtures("programs", "events"))]
        async fn update(db: PgPool) {
            let repo: PgEventStorage = db.into();
            let mut updated = event_2().content;
            updated.event_name = Some("updated-name".to_string());
            let event = repo
                .update(
                    &"event-1".parse().unwrap(),
                    updated.clone(),
                    &User(Claims::any_business_user()),
                )
                .await
                .unwrap();
            assert_eq!(event.content, updated);
            let event = repo
                .retrieve(
                    &"event-1".parse().unwrap(),
                    &User(Claims::any_business_user()),
                )
                .await
                .unwrap();
            assert_eq!(event.content, updated);
        }

        #[sqlx::test(fixtures("programs", "events"))]
        async fn update_name_conflict(db: PgPool) {
            let repo: PgEventStorage = db.into();
            let event = repo
                .update(
                    &"event-1".parse().unwrap(),
                    event_2().content,
                    &User(Claims::any_business_user()),
                )
                .await;
            assert!(event.is_ok());
        }
    }

    mod delete {
        use super::*;

        #[sqlx::test(fixtures("programs", "events"))]
        async fn delete_existing(db: PgPool) {
            let repo: PgEventStorage = db.into();
            let event = repo
                .delete(
                    &"event-1".parse().unwrap(),
                    &User(Claims::any_business_user()),
                )
                .await
                .unwrap();
            assert_eq!(event, event_1());

            let event = repo
                .retrieve(
                    &"event-1".parse().unwrap(),
                    &User(Claims::any_business_user()),
                )
                .await;
            assert!(matches!(event, Err(AppError::NotFound)));

            let event = repo
                .retrieve(
                    &"event-2".parse().unwrap(),
                    &User(Claims::any_business_user()),
                )
                .await
                .unwrap();
            assert_eq!(event, event_2());
        }

        #[sqlx::test(fixtures("programs", "events"))]
        async fn delete_not_existing(db: PgPool) {
            let repo: PgEventStorage = db.into();
            let event = repo
                .delete(
                    &"not-existent".parse().unwrap(),
                    &User(Claims::any_business_user()),
                )
                .await;
            assert!(matches!(event, Err(AppError::NotFound)));
        }
    }
}
