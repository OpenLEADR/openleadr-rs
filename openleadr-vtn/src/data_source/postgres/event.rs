use crate::{
    api::event::QueryParams,
    data_source::{
        postgres::{extract_business_ids, to_json_value, PgId, PgTargetsFilter},
        Crud, EventCrud,
    },
    error::AppError,
    jwt::{BusinessIds, Claims, User},
};
use axum::async_trait;
use chrono::{DateTime, Utc};
use openleadr_wire::{
    event::{EventContent, EventId, Priority},
    Event,
};
use sqlx::PgPool;
use std::str::FromStr;
use tracing::{error, trace};

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
    priority: Priority,
    targets: Option<serde_json::Value>,
    report_descriptors: Option<serde_json::Value>,
    payload_descriptors: Option<serde_json::Value>,
    interval_period: Option<serde_json::Value>,
    intervals: serde_json::Value,
}

impl TryFrom<PostgresEvent> for Event {
    type Error = AppError;

    #[tracing::instrument(name = "TryFrom<PostgresEvent> for Event")]
    fn try_from(value: PostgresEvent) -> Result<Self, Self::Error> {
        let targets = match value.targets {
            None => None,
            Some(t) => serde_json::from_value(t)
                .inspect_err(|err| {
                    error!(?err, "Failed to deserialize JSON from DB to `TargetMap`")
                })
                .map_err(AppError::SerdeJsonInternalServerError)?,
        };

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
            content: EventContent {
                program_id: value.program_id.parse()?,
                event_name: value.event_name,
                priority: value.priority,
                targets,
                report_descriptors,
                payload_descriptors,
                interval_period,
                intervals: serde_json::from_value(value.intervals)
                    .map_err(AppError::SerdeJsonInternalServerError)?,
            },
        })
    }
}

#[derive(Default, Debug)]
struct PostgresFilter<'a> {
    program_id: Option<&'a str>,
    targets: Vec<PgTargetsFilter<'a>>,

    skip: i64,
    limit: i64,
}

impl<'a> From<&'a QueryParams> for PostgresFilter<'a> {
    fn from(query: &'a QueryParams) -> Self {
        let mut filter = Self {
            program_id: query.program_id.as_ref().map(|id| id.as_str()),
            skip: query.skip,
            limit: query.limit,
            ..Default::default()
        };
        if let Some(ref label) = query.target_type {
            if let Some(values) = query.target_values.as_ref() {
                filter.targets = values
                    .iter()
                    .map(|value| PgTargetsFilter {
                        label: label.as_str(),
                        value: [value.clone()],
                    })
                    .collect()
            }
        };

        filter
    }
}

struct MaybePgId {
    id: Option<String>,
}

async fn check_write_permission(
    program_id: &str,
    user: &Claims,
    db: &PgPool,
) -> Result<(), AppError> {
    if let Some(business_ids) = extract_business_ids(user) {
        let MaybePgId { id } = sqlx::query_as!(
            MaybePgId,
            r#"
            SELECT business_id AS id FROM program WHERE id = $1
            "#,
            program_id
        )
        .fetch_one(db)
        .await?;

        // If no business is connected, anyone may write
        if let Some(id) = id {
            if !business_ids.contains(&id) {
                Err(AppError::Auth("You do not have write permissions for events belonging to a program that belongs to another business logic".to_string()))?;
            }
        }
    };
    Ok(())
}

#[async_trait]
impl Crud for PgEventStorage {
    type Type = Event;
    type Id = EventId;
    type NewType = EventContent;
    type Error = AppError;
    type Filter = QueryParams;
    type PermissionFilter = User;

    async fn create(
        &self,
        new: Self::NewType,
        User(user): &Self::PermissionFilter,
    ) -> Result<Self::Type, Self::Error> {
        check_write_permission(new.program_id.as_str(), user, &self.db).await?;

        Ok(sqlx::query_as!(
            PostgresEvent,
            r#"
            INSERT INTO event (id, created_date_time, modification_date_time, program_id, event_name, priority, targets, report_descriptors, payload_descriptors, interval_period, intervals)
            VALUES (gen_random_uuid(), now(), now(), $1, $2, $3, $4, $5, $6, $7, $8)
            RETURNING *
            "#,
            new.program_id.as_str(),
            new.event_name,
            Into::<Option<i64>>::into(new.priority),
            to_json_value(new.targets)?,
            to_json_value(new.report_descriptors)?,
            to_json_value(new.payload_descriptors)?,
            to_json_value(new.interval_period)?,
            serde_json::to_value(&new.intervals).map_err(AppError::SerdeJsonBadRequest)?,
        )
            .fetch_one(&self.db)
            .await?
            .try_into()?
        )
    }

    async fn retrieve(
        &self,
        id: &Self::Id,
        User(user): &Self::PermissionFilter,
    ) -> Result<Self::Type, Self::Error> {
        let business_ids = match user.business_ids() {
            BusinessIds::Specific(ids) => Some(ids),
            BusinessIds::Any => None,
        };

        Ok(sqlx::query_as!(
            PostgresEvent,
            r#"
            SELECT e.*
            FROM event e
              JOIN program p ON e.program_id = p.id
              LEFT JOIN ven_program vp ON p.id = vp.program_id
            WHERE e.id = $1
              AND (
                  ($2 AND (vp.ven_id IS NULL OR vp.ven_id = ANY($3))) 
                  OR 
                  ($4 AND ($5::text[] IS NULL OR p.business_id = ANY ($5)))
                  )
            "#,
            id.as_str(),
            user.is_ven(),
            &user.ven_ids_string(),
            user.is_business(),
            business_ids.as_deref(),
        )
        .fetch_one(&self.db)
        .await?
        .try_into()?)
    }

    async fn retrieve_all(
        &self,
        filter: &Self::Filter,
        User(user): &Self::PermissionFilter,
    ) -> Result<Vec<Self::Type>, Self::Error> {
        let pg_filter: PostgresFilter = filter.into();
        trace!(?pg_filter);

        let business_ids = match user.business_ids() {
            BusinessIds::Specific(ids) => Some(ids),
            BusinessIds::Any => None,
        };

        Ok(sqlx::query_as!(
            PostgresEvent,
            r#"
            SELECT e.*
            FROM event e
              JOIN program p on p.id = e.program_id
              LEFT JOIN ven_program vp ON p.id = vp.program_id
              LEFT JOIN LATERAL (
                  SELECT e.id as e_id, 
                         json_array(jsonb_array_elements(e.targets)) <@ $2::jsonb AS target_test )
                  ON e.id = e_id
            WHERE ($1::text IS NULL OR e.program_id like $1)
              AND ($2::jsonb = '[]'::jsonb OR target_test)
              AND (
                  ($3 AND (vp.ven_id IS NULL OR vp.ven_id = ANY($4)))
                  OR 
                  ($5 AND ($6::text[] IS NULL OR p.business_id = ANY ($6)))
                  )
            GROUP BY e.id, e.priority, e.created_date_time
            ORDER BY priority ASC , created_date_time DESC
            OFFSET $7 LIMIT $8
            "#,
            pg_filter.program_id,
            serde_json::to_value(pg_filter.targets)
                .map_err(AppError::SerdeJsonInternalServerError)?,
            user.is_ven(),
            &user.ven_ids_string(),
            user.is_business(),
            business_ids.as_deref(),
            pg_filter.skip,
            pg_filter.limit
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
        User(user): &Self::PermissionFilter,
    ) -> Result<Self::Type, Self::Error> {
        check_write_permission(new.program_id.as_str(), user, &self.db).await?;

        let previous_program_id = sqlx::query_as!(
            PgId,
            r#"SELECT program_id AS id FROM event WHERE id = $1"#,
            id.as_str()
        )
        .fetch_one(&self.db)
        .await?;

        // make sure, you cannot 'steal' an event from another business
        if previous_program_id.id != new.program_id.as_str() {
            check_write_permission(&previous_program_id.id, user, &self.db).await?;
        }

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
                intervals = $9
            WHERE id = $1
            RETURNING *
            "#,
            id.as_str(),
            new.program_id.as_str(),
            new.event_name,
            Into::<Option<i64>>::into(new.priority),
            to_json_value(new.targets)?,
            to_json_value(new.report_descriptors)?,
            to_json_value(new.payload_descriptors)?,
            to_json_value(new.interval_period)?,
            serde_json::to_value(&new.intervals).map_err(AppError::SerdeJsonBadRequest)?,
        )
        .fetch_one(&self.db)
        .await?
        .try_into()?)
    }

    async fn delete(
        &self,
        id: &Self::Id,
        User(user): &Self::PermissionFilter,
    ) -> Result<Self::Type, Self::Error> {
        let program_id = sqlx::query_as!(
            PgId,
            r#"SELECT program_id AS id FROM event WHERE id = $1"#,
            id.as_str()
        )
        .fetch_one(&self.db)
        .await?;

        check_write_permission(&program_id.id, user, &self.db).await?;

        Ok(sqlx::query_as!(
            PostgresEvent,
            r#"
            DELETE FROM event WHERE id = $1 RETURNING *
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

    use crate::{
        api::event::QueryParams,
        data_source::{postgres::event::PgEventStorage, Crud},
        error::AppError,
        jwt::{Claims, User},
    };
    use chrono::{DateTime, Duration, Utc};
    use openleadr_wire::{
        event::{EventContent, EventInterval, EventType, EventValuesMap},
        interval::IntervalPeriod,
        target::{TargetEntry, TargetLabel, TargetMap},
        values_map::Value,
        Event,
    };

    impl Default for QueryParams {
        fn default() -> Self {
            Self {
                program_id: None,
                target_type: None,
                target_values: None,
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
            content: EventContent {
                program_id: "program-1".parse().unwrap(),
                event_name: Some("event-1-name".to_string()),
                priority: Some(4).into(),
                targets: Some(TargetMap(vec![
                    TargetEntry {
                        label: TargetLabel::Group,
                        values: ["group-1".to_string()],
                    },
                    TargetEntry {
                        label: TargetLabel::Private("PRIVATE_LABEL".to_string()),
                        values: ["private value".to_string()],
                    },
                ])),
                report_descriptors: None,
                payload_descriptors: None,
                interval_period: Some(IntervalPeriod {
                    start: "2023-06-15T09:30:00+00:00".parse().unwrap(),
                    duration: Some("P0Y0M0DT1H0M0S".parse().unwrap()),
                    randomize_start: Some("P0Y0M0DT1H0M0S".parse().unwrap()),
                }),
                intervals: vec![EventInterval {
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
                }],
            },
        }
    }

    fn event_2() -> Event {
        Event {
            id: "event-2".parse().unwrap(),
            created_date_time: "2024-07-25 08:31:10.776000 +00:00".parse().unwrap(),
            modification_date_time: "2024-07-25 08:31:10.776000 +00:00".parse().unwrap(),
            content: EventContent {
                program_id: "program-2".parse().unwrap(),
                event_name: Some("event-2-name".to_string()),
                priority: None.into(),
                targets: Some(TargetMap(vec![TargetEntry {
                    label: TargetLabel::Private("SOME_TARGET".to_string()),
                    values: ["target-1".to_string()],
                }])),
                report_descriptors: None,
                payload_descriptors: None,
                interval_period: None,
                intervals: vec![EventInterval {
                    id: 3,
                    interval_period: None,
                    payloads: vec![EventValuesMap {
                        value_type: EventType::Private("SOME_PAYLOAD".to_string()),
                        values: vec![Value::String("value".to_string())],
                    }],
                }],
            },
        }
    }

    fn event_3() -> Event {
        Event {
            id: "event-3".parse().unwrap(),
            content: EventContent {
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
        async fn filter_target_type_and_value_get_all(db: PgPool) {
            let repo: PgEventStorage = db.into();

            let events = repo
                .retrieve_all(
                    &QueryParams {
                        target_type: Some(TargetLabel::Group),
                        target_values: Some(vec!["group-1".to_string()]),
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
                        target_type: Some(TargetLabel::Private("SOME_TARGET".to_string())),
                        target_values: Some(vec!["target-1".to_string()]),
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
                        target_type: Some(TargetLabel::Group),
                        target_values: Some(vec!["not-existent".to_string()]),
                        ..Default::default()
                    },
                    &User(Claims::any_business_user()),
                )
                .await
                .unwrap();
            assert_eq!(events.len(), 0);

            let events = repo
                .retrieve_all(
                    &QueryParams {
                        target_type: Some(TargetLabel::Private("NOT_EXISTENT".to_string())),
                        target_values: Some(vec!["target-1".to_string()]),
                        ..Default::default()
                    },
                    &User(Claims::any_business_user()),
                )
                .await
                .unwrap();
            assert_eq!(events.len(), 0);

            let events = repo
                .retrieve_all(
                    &QueryParams {
                        target_type: Some(TargetLabel::Group),
                        target_values: Some(vec!["target-1".to_string()]),
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
                        target_type: Some(TargetLabel::Group),
                        target_values: Some(vec!["private value".to_string()]),
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
                        target_type: Some(TargetLabel::Group),
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
