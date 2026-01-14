use crate::{
    api::event::QueryParams,
    data_source::{
        postgres::{get_ven_targets, intersection, to_json_value},
        Crud, EventCrud,
    },
    error::AppError,
    jwt::Scope,
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

    /// The `client_id` is set if the request has [`ReadTargets`](Scope::ReadTargets) scope (VEN clients).
    /// The `client_id` is not set if the request has [`ReadAll`](Scope::ReadAll) scope (BL clients).
    async fn retrieve(
        &self,
        id: &Self::Id,
        client_id: &Self::PermissionFilter,
    ) -> Result<Self::Type, Self::Error> {
        match client_id {
            None => self.retrieve_without_client_id(id).await,
            Some(client_id) => self.retrieve_with_client_id(id, client_id).await,
        }
    }

    /// The `client_id` is set if the request has [`ReadTargets`](Scope::ReadTargets) scope (VEN clients).
    /// The `client_id` is not set if the request has [`ReadAll`](Scope::ReadAll) scope (BL clients).
    async fn retrieve_all(
        &self,
        filter: &Self::Filter,
        client_id: &Self::PermissionFilter,
    ) -> Result<Vec<Self::Type>, Self::Error> {
        match client_id {
            None => self.retrieve_all_without_client_id(filter).await,
            Some(client_id) => self.retrieve_all_with_client_id(filter, client_id).await,
        }
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

impl PgEventStorage {
    /// The `client_id` functions as a permission filter here.
    /// It is provided if the request has [`ReadTargets`](Scope::ReadTargets) scope, which
    /// is the case for VEN clients (aka. customer logic).
    /// BL clients have a [`ReadAll`](Scope::ReadAll) scope, and therefore the API layer will
    /// call the [`retrieve_all_without_client_id`](PgEventStorage::retrieve_all_without_client_id) function.
    async fn retrieve_all_with_client_id(
        &self,
        filter: &QueryParams,
        client_id: &ClientId,
    ) -> Result<Vec<Event>, AppError> {
        let ven_targets = get_ven_targets(self.db.clone(), client_id).await?;

        let filter_targets = intersection(&ven_targets, filter.targets.as_deref());

        sqlx::query_as!(
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
              -- according to the spec, we MUST only test query params
              -- against the event that the VEN object (and its resources) have as targets.
              -- Therefore, $2 is the intersection of the VEN targets and the filter targets.
              AND ($2::text[] IS NULL OR e.targets @> $2)
              AND (
                  -- IF the ven targets have at least one target in common with the event
                    e.targets && $3
                        -- or IF the event targets are empty
                        OR array_length(e.targets, 1) IS NULL
                  )
            ORDER BY priority ASC, created_date_time DESC
            OFFSET $4 LIMIT $5
            "#,
            filter.program_id.as_ref().map(|id| id.as_str()),
            filter_targets as _,
            ven_targets as _,
            filter.skip,
            filter.limit
        )
        .fetch_all(&self.db)
        .await?
        .into_iter()
        .map(|mut e| {
            // Limit the targets displayed to the VEN to the targets the VEN has access to.
            // Compare to the spec v3.1.1, Definition.md:
            //      Target hiding: For program and event objects, a VTN will only include requested targets in a response. This prevents VENs from learning targets that have
            //      not been explicitly assigned to them by BL. Target hiding is not performed on ven, resource, or subscription objects as these objects are read-able only by a specific VEN.
            e.targets = intersection(&e.targets, &ven_targets)
                .into_iter()
                .cloned()
                .collect();
            e
        })
        .map(TryInto::try_into)
        .collect::<Result<_, _>>()
    }

    /// The `client_id` functions as a permission filter here.
    /// It is provided if the request has [`ReadTargets`](Scope::ReadTargets) scope, which
    /// is the case for VEN clients (aka. customer logic).
    /// BL clients have a [`ReadAll`](Scope::ReadAll) scope, and therefore the API layer will
    /// call the [`retrieve_without_client_id`](PgEventStorage::retrieve_without_client_id) function.
    async fn retrieve_with_client_id(
        &self,
        id: &EventId,
        client_id: &ClientId,
    ) -> Result<Event, AppError> {
        let ven_targets = get_ven_targets(self.db.clone(), client_id).await?;

        let mut pg_event = sqlx::query_as!(
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
              AND (
                  -- IF the ven targets have at least one target in common with the event
                    e.targets && $2
                        -- or IF the event targets are empty
                        OR array_length(e.targets, 1) IS NULL
                  )
            "#,
            id.as_str(),
            ven_targets as _,
        )
        .fetch_one(&self.db)
        .await?;

        // Limit the targets displayed to the VEN to the targets the VEN has access to.
        // Compare to the spec v3.1.1, Definition.md:
        //      Target hiding: For program and event objects, a VTN will only include requested targets in a response. This prevents VENs from learning targets that have
        //      not been explicitly assigned to them by BL. Target hiding is not performed on ven, resource, or subscription objects as these objects are read-able only by a specific VEN.
        pg_event.targets = intersection(&pg_event.targets, &ven_targets)
            .into_iter()
            .cloned()
            .collect();

        pg_event.try_into()
    }

    async fn retrieve_all_without_client_id(
        &self,
        filter: &QueryParams,
    ) -> Result<Vec<Event>, AppError> {
        sqlx::query_as!(
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
              -- IF filter targets are empty, do not filter.
              -- IF filter targets are not empty, filter only if they are in the event targets.
              AND ($2::text[] IS NULL OR e.targets @> $2)
            ORDER BY priority ASC, created_date_time DESC
            OFFSET $3 LIMIT $4
            "#,
            filter.program_id.as_ref().map(|id| id.as_str()),
            filter.targets.as_deref() as _,
            filter.skip,
            filter.limit
        )
        .fetch_all(&self.db)
        .await?
        .into_iter()
        .map(TryInto::try_into)
        .collect::<Result<_, _>>()
    }

    async fn retrieve_without_client_id(&self, id: &EventId) -> Result<Event, AppError> {
        sqlx::query_as!(
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
            "#,
            id.as_str(),
        )
        .fetch_one(&self.db)
        .await?
        .try_into()
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

    fn event_4() -> Event {
        Event {
            id: "event-4".parse().unwrap(),
            content: EventRequest {
                program_id: "program-3".parse().unwrap(),
                event_name: Some("event-4-name".to_string()),
                targets: vec![
                    Target::from_str("target-1").unwrap(),
                    Target::from_str("group-1").unwrap(),
                ],
                ..event_2().content
            },
            ..event_2()
        }
    }

    fn event_5() -> Event {
        Event {
            id: "event-5".parse().unwrap(),
            content: EventRequest {
                program_id: "program-3".parse().unwrap(),
                event_name: Some("event-5-name".to_string()),
                targets: vec![],
                ..event_2().content
            },
            ..event_2()
        }
    }
    pub fn without_targets<'a>(event: Event, targets: impl IntoIterator<Item = &'a str>) -> Event {
        let targets: Vec<Target> = targets.into_iter().map(|t| t.parse().unwrap()).collect();
        Event {
            content: EventRequest {
                targets: event
                    .content
                    .targets
                    .into_iter()
                    .filter(|t| !targets.contains(t))
                    .collect(),
                ..event.content
            },
            ..event
        }
    }

    mod get_all {
        use super::*;
        use openleadr_wire::ClientId;

        #[sqlx::test(fixtures("programs", "events"))]
        async fn default_get_all(db: PgPool) {
            let repo: PgEventStorage = db.into();
            let mut events = repo.retrieve_all(&Default::default(), &None).await.unwrap();
            assert_eq!(events.len(), 5);
            events.sort_by(|a, b| a.id.as_str().cmp(b.id.as_str()));
            assert_eq!(
                events,
                vec![event_1(), event_2(), event_3(), event_4(), event_5()]
            );
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
                    &None,
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
                    &None,
                )
                .await
                .unwrap();
            assert_eq!(events.len(), 4);

            let events = repo
                .retrieve_all(
                    &QueryParams {
                        skip: 20,
                        ..Default::default()
                    },
                    &None,
                )
                .await
                .unwrap();
            assert_eq!(events.len(), 0);
        }

        #[sqlx::test(fixtures("programs", "events", "vens"))]
        // Check that a client_id that can't be found returns all and only events that don't have targets.
        async fn filter_target_get_all_ven_not_found(db: PgPool) {
            let repo: PgEventStorage = db.into();

            let ven: ClientId = "does-not-exist".parse().unwrap();
            let events = repo
                .retrieve_all(&QueryParams::default(), &Some(ven.clone()))
                .await
                .unwrap();
            assert_eq!(events.len(), 1);
            assert_eq!(events, vec![event_5()]);
        }

        #[sqlx::test(fixtures("programs", "events", "vens"))]
        // Check that a client_id that matches a VEN without targets returns all and only events that don't have targets.
        async fn filter_target_get_all_ven_without_targets(db: PgPool) {
            let repo: PgEventStorage = db.into();

            let ven: ClientId = "ven-has-no-targets-client-id".parse().unwrap();
            let events = repo
                .retrieve_all(&QueryParams::default(), &Some(ven.clone()))
                .await
                .unwrap();
            assert_eq!(events.len(), 1);
            assert_eq!(events, vec![event_5()]);
        }

        #[sqlx::test(fixtures("programs", "events", "vens"))]
        // As this test does use a client_id, it is mimicking the functionality
        // when a VEN client does the request.
        async fn filter_target_get_all_ven_client(db: PgPool) {
            let repo: PgEventStorage = db.into();

            // Has access to targets "group-1" and "private-value"
            let ven_1: ClientId = "ven-1-client-id".parse().unwrap();

            // Has access to targets "group-2"
            let ven_2: ClientId = "ven-2-client-id".parse().unwrap();

            // Has access to targets "group-1"
            let ven_3: ClientId = "ven-3-client-id".parse().unwrap();

            // Has access to targets "group-2"
            let ven_4: ClientId = "ven-4-client-id".parse().unwrap();

            // ven_1 has access to targets "group-1" and "private-value"
            // which should allow access to event-1, event-4, and event-5
            let events = repo
                .retrieve_all(&QueryParams::default(), &Some(ven_1.clone()))
                .await
                .unwrap();
            assert_eq!(events.len(), 3);
            assert_eq!(
                events,
                vec![
                    event_1(),
                    without_targets(event_4(), ["target-1"]),
                    event_5()
                ]
            );

            let events = repo
                .retrieve_all(
                    &QueryParams {
                        targets: TargetQueryParams(Some(vec!["group-1".parse().unwrap()])),
                        ..Default::default()
                    },
                    &Some(ven_1.clone()),
                )
                .await
                .unwrap();
            assert_eq!(events.len(), 2);
            assert_eq!(
                events,
                vec![event_1(), without_targets(event_4(), ["target-1"])]
            );

            let events = repo
                .retrieve_all(
                    &QueryParams {
                        targets: TargetQueryParams(Some(vec![
                            "group-1".parse().unwrap(),
                            "private-value".parse().unwrap(),
                        ])),
                        ..Default::default()
                    },
                    &Some(ven_1.clone()),
                )
                .await
                .unwrap();
            assert_eq!(events.len(), 1);
            assert_eq!(events, vec![event_1()]);

            // filtering on "target-1" should not have any effect on the result
            // compared to no filtering as ven_1 does not have access to "target-1"
            let events = repo
                .retrieve_all(
                    &QueryParams {
                        targets: TargetQueryParams(Some(vec!["target-1".parse().unwrap()])),
                        ..Default::default()
                    },
                    &Some(ven_1.clone()),
                )
                .await
                .unwrap();
            assert_eq!(events.len(), 3);

            let events = repo
                .retrieve_all(
                    &QueryParams {
                        targets: TargetQueryParams(Some(vec![
                            "group-1".parse().unwrap(),
                            "target-1".parse().unwrap(),
                        ])),
                        ..Default::default()
                    },
                    &Some(ven_1.clone()),
                )
                .await
                .unwrap();
            assert_eq!(events.len(), 2);

            // ven_2 has access to target "group-2" for which there is no event with that target.
            // Therefore, it should allow only access to event-5.
            let events = repo
                .retrieve_all(&QueryParams::default(), &Some(ven_2.clone()))
                .await
                .unwrap();
            assert_eq!(events.len(), 1);
            assert_eq!(events, vec![event_5()]);

            // filtering on "target-1" should not have any effect on the result
            // compared to no filtering as ven_2 does not have access to "target-1"
            let events = repo
                .retrieve_all(
                    &QueryParams {
                        targets: TargetQueryParams(Some(vec!["target-1".parse().unwrap()])),
                        ..Default::default()
                    },
                    &Some(ven_2.clone()),
                )
                .await
                .unwrap();
            assert_eq!(events.len(), 1);
            assert_eq!(events, vec![event_5()]);

            // ven_3 has access to target "group-1" which should allow access to event-1, event-4, and event-5
            let events = repo
                .retrieve_all(&QueryParams::default(), &Some(ven_3.clone()))
                .await
                .unwrap();
            assert_eq!(events.len(), 3);

            // filtering on "target-1" should not have any effect on the result
            // compared to no filtering as ven_3 does not have access to "target-1"
            let events = repo
                .retrieve_all(
                    &QueryParams {
                        targets: TargetQueryParams(Some(vec!["target-1".parse().unwrap()])),
                        ..Default::default()
                    },
                    &Some(ven_3.clone()),
                )
                .await
                .unwrap();
            assert_eq!(events.len(), 3);

            let events = repo
                .retrieve_all(
                    &QueryParams {
                        targets: TargetQueryParams(Some(vec!["group-1".parse().unwrap()])),
                        ..Default::default()
                    },
                    &Some(ven_3.clone()),
                )
                .await
                .unwrap();
            assert_eq!(events.len(), 2);

            // ven_4 has access to targets "group-1" and "group-2" which should allow access to event-1, event-4, and event-5
            let events = repo
                .retrieve_all(&QueryParams::default(), &Some(ven_4.clone()))
                .await
                .unwrap();
            assert_eq!(events.len(), 3);

            // filtering on "target-1" should not have any effect on the result
            // compared to no filtering as ven_4 does not have access to "target-1"
            let events = repo
                .retrieve_all(
                    &QueryParams {
                        targets: TargetQueryParams(Some(vec!["target-1".parse().unwrap()])),
                        ..Default::default()
                    },
                    &Some(ven_4.clone()),
                )
                .await
                .unwrap();
            assert_eq!(events.len(), 3);
        }

        #[sqlx::test(fixtures("programs", "events"))]
        // As this test does not use a client_id, it is mimicking the functionality
        // when a BL client does the request.
        async fn filter_target_get_all_bl_client(db: PgPool) {
            let repo: PgEventStorage = db.into();

            let mut events = repo
                .retrieve_all(
                    &QueryParams {
                        targets: TargetQueryParams(None),
                        ..Default::default()
                    },
                    &None,
                )
                .await
                .unwrap();
            assert_eq!(events.len(), 5);
            events.sort_by(|a, b| a.id.as_str().cmp(b.id.as_str()));
            assert_eq!(
                events,
                vec![event_1(), event_2(), event_3(), event_4(), event_5()]
            );

            let events = repo
                .retrieve_all(
                    &QueryParams {
                        targets: TargetQueryParams(Some(vec!["group-1".parse().unwrap()])),
                        ..Default::default()
                    },
                    &None,
                )
                .await
                .unwrap();
            assert_eq!(events.len(), 2);
            assert_eq!(events, vec![event_1(), event_4()]);

            let mut events = repo
                .retrieve_all(
                    &QueryParams {
                        targets: TargetQueryParams(Some(vec!["target-1".parse().unwrap()])),
                        ..Default::default()
                    },
                    &None,
                )
                .await
                .unwrap();
            assert_eq!(events.len(), 3);
            events.sort_by(|a, b| a.id.as_str().cmp(b.id.as_str()));
            assert_eq!(events, vec![event_2(), event_3(), event_4()]);

            let events = repo
                .retrieve_all(
                    &QueryParams {
                        targets: TargetQueryParams(Some(vec!["not-existent".parse().unwrap()])),
                        ..Default::default()
                    },
                    &None,
                )
                .await
                .unwrap();
            assert_eq!(events.len(), 0);
        }

        #[sqlx::test(fixtures("programs", "events"))]
        async fn filter_multiple_targets(db: PgPool) {
            let repo: PgEventStorage = db.into();

            let events = repo
                .retrieve_all(
                    &QueryParams {
                        targets: TargetQueryParams(Some(vec![
                            "private-value".parse().unwrap(),
                            "group-1".parse().unwrap(),
                        ])),
                        ..Default::default()
                    },
                    &None,
                )
                .await
                .unwrap();
            assert_eq!(events.len(), 1);

            let events = repo
                .retrieve_all(
                    &QueryParams {
                        targets: TargetQueryParams(Some(vec![
                            "private-value".parse().unwrap(),
                            "target-1".parse().unwrap(),
                        ])),
                        ..Default::default()
                    },
                    &None,
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
                    &None,
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
                    &None,
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
                    &None,
                )
                .await
                .unwrap();
            assert_eq!(events.len(), 0);
        }
    }

    mod get {
        use super::*;
        use openleadr_wire::ClientId;

        #[sqlx::test(fixtures("programs", "events"))]
        async fn get_existing(db: PgPool) {
            let repo: PgEventStorage = db.into();
            let event = repo
                .retrieve(&"event-1".parse().unwrap(), &None)
                .await
                .unwrap();
            assert_eq!(event, event_1());
        }

        #[sqlx::test(fixtures("programs", "events"))]
        async fn get_not_existing(db: PgPool) {
            let repo: PgEventStorage = db.into();
            let event = repo.retrieve(&"not-existent".parse().unwrap(), &None).await;
            assert!(matches!(event, Err(AppError::NotFound)));
        }

        #[sqlx::test(fixtures("programs", "events", "vens"))]
        async fn get_as_ven_client(db: PgPool) {
            let repo: PgEventStorage = db.into();

            // Has access to targets "group-1" and "private-value"
            let ven_1: ClientId = "ven-1-client-id".parse().unwrap();

            // Has access to no targets
            let ven_5: ClientId = "ven-has-no-targets-client-id".parse().unwrap();

            // ven_1 has access to targets "group-1" and should therefore be able to
            // access event-1, event-4, and event-5
            let event = repo
                .retrieve(&"event-1".parse().unwrap(), &Some(ven_1.clone()))
                .await
                .unwrap();
            assert_eq!(event, event_1());
            let err = repo
                .retrieve(&"event-2".parse().unwrap(), &Some(ven_1.clone()))
                .await;
            assert!(matches!(err, Err(AppError::NotFound)));
            let event = repo
                .retrieve(&"event-4".parse().unwrap(), &Some(ven_1.clone()))
                .await
                .unwrap();
            assert_eq!(event, without_targets(event_4(), ["target-1"]));
            let event = repo
                .retrieve(&"event-5".parse().unwrap(), &Some(ven_1.clone()))
                .await
                .unwrap();
            assert_eq!(event, event_5());

            // ven_5 has no access to any targets and therefore should not be able to access event-5 only
            let err = repo
                .retrieve(&"event-1".parse().unwrap(), &Some(ven_5.clone()))
                .await;
            assert!(matches!(err, Err(AppError::NotFound)));
            let event = repo
                .retrieve(&"event-5".parse().unwrap(), &Some(ven_5.clone()))
                .await
                .unwrap();
            assert_eq!(event, event_5());
        }
    }

    mod add {
        use super::*;

        #[sqlx::test(fixtures("programs"))]
        async fn add(db: PgPool) {
            let repo: PgEventStorage = db.into();
            let event = repo.create(event_1().content, &None).await.unwrap();
            assert_eq!(event.content, event_1().content);
            assert!(event.created_date_time < Utc::now() + Duration::minutes(10));
            assert!(event.created_date_time > Utc::now() - Duration::minutes(10));
            assert!(event.modification_date_time < Utc::now() + Duration::minutes(10));
            assert!(event.modification_date_time > Utc::now() - Duration::minutes(10));
        }

        #[sqlx::test(fixtures("programs", "events"))]
        async fn add_existing_conflict_name(db: PgPool) {
            let repo: PgEventStorage = db.into();
            let event = repo.create(event_1().content, &None).await;
            assert!(event.is_ok());
        }
    }

    mod modify {
        use super::*;

        #[sqlx::test(fixtures("programs", "events"))]
        async fn updates_modify_time(db: PgPool) {
            let repo: PgEventStorage = db.into();
            let event = repo
                .update(&"event-1".parse().unwrap(), event_1().content, &None)
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
                .update(&"event-1".parse().unwrap(), updated.clone(), &None)
                .await
                .unwrap();
            assert_eq!(event.content, updated);
            let event = repo
                .retrieve(&"event-1".parse().unwrap(), &None)
                .await
                .unwrap();
            assert_eq!(event.content, updated);
        }

        #[sqlx::test(fixtures("programs", "events"))]
        async fn update_name_conflict(db: PgPool) {
            let repo: PgEventStorage = db.into();
            let event = repo
                .update(&"event-1".parse().unwrap(), event_2().content, &None)
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
                .delete(&"event-1".parse().unwrap(), &None)
                .await
                .unwrap();
            assert_eq!(event, event_1());

            let event = repo.retrieve(&"event-1".parse().unwrap(), &None).await;
            assert!(matches!(event, Err(AppError::NotFound)));

            let event = repo
                .retrieve(&"event-2".parse().unwrap(), &None)
                .await
                .unwrap();
            assert_eq!(event, event_2());
        }

        #[sqlx::test(fixtures("programs", "events"))]
        async fn delete_not_existing(db: PgPool) {
            let repo: PgEventStorage = db.into();
            let event = repo.delete(&"not-existent".parse().unwrap(), &None).await;
            assert!(matches!(event, Err(AppError::NotFound)));
        }
    }
}
