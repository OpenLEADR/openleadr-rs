use crate::{
    api::program::QueryParams,
    data_source::{
        postgres::{extract_business_id, extract_vens, to_json_value, PgTargetsFilter},
        Crud, ProgramCrud,
    },
    error::AppError,
    jwt::User,
};
use axum::async_trait;
use chrono::{DateTime, Utc};
use openadr_wire::{
    program::{ProgramContent, ProgramId},
    Program,
};
use sqlx::PgPool;
use tracing::{error, trace};

#[async_trait]
impl ProgramCrud for PgProgramStorage {}

pub(crate) struct PgProgramStorage {
    db: PgPool,
}

impl From<PgPool> for PgProgramStorage {
    fn from(db: PgPool) -> Self {
        Self { db }
    }
}

#[derive(Debug)]
struct PostgresProgram {
    id: String,
    created_date_time: DateTime<Utc>,
    modification_date_time: DateTime<Utc>,
    program_name: String,
    program_long_name: Option<String>,
    retailer_name: Option<String>,
    retailer_long_name: Option<String>,
    program_type: Option<String>,
    country: Option<String>,
    principal_subdivision: Option<String>,
    interval_period: Option<serde_json::Value>,
    program_descriptions: Option<serde_json::Value>,
    binding_events: Option<bool>,
    local_price: Option<bool>,
    payload_descriptors: Option<serde_json::Value>,
    targets: Option<serde_json::Value>,
}

impl TryFrom<PostgresProgram> for Program {
    type Error = AppError;

    #[tracing::instrument(name = "TryFrom<PostgresProgram> for Program")]
    fn try_from(value: PostgresProgram) -> Result<Self, Self::Error> {
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
        let program_descriptions = match value.program_descriptions {
            None => None,
            Some(t) => serde_json::from_value(t)
                .inspect_err(|err| {
                    error!(
                        ?err,
                        "Failed to deserialize JSON from DB to `Vec<ProgramDescription>`"
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
                        "Failed to deserialize JSON from DB to `Vec<PayloadDescriptor>`"
                    )
                })
                .map_err(AppError::SerdeJsonInternalServerError)?,
        };
        let targets = match value.targets {
            None => None,
            Some(t) => serde_json::from_value(t)
                .inspect_err(|err| {
                    error!(?err, "Failed to deserialize JSON from DB to `TargetMap`")
                })
                .map_err(AppError::SerdeJsonInternalServerError)?,
        };

        Ok(Self {
            id: value.id.parse()?,
            created_date_time: value.created_date_time,
            modification_date_time: value.modification_date_time,
            content: ProgramContent {
                object_type: Some(Default::default()),
                program_name: value.program_name,
                program_long_name: value.program_long_name,
                retailer_name: value.retailer_name,
                retailer_long_name: value.retailer_long_name,
                program_type: value.program_type,
                country: value.country,
                principal_subdivision: value.principal_subdivision,
                time_zone_offset: None,
                interval_period,
                program_descriptions,
                binding_events: value.binding_events,
                local_price: value.local_price,
                payload_descriptors,
                targets,
            },
        })
    }
}

#[derive(Debug, Default)]
struct PostgresFilter<'a> {
    targets: Vec<PgTargetsFilter<'a>>,

    skip: i64,
    limit: i64,
}

impl<'a> From<&'a QueryParams> for PostgresFilter<'a> {
    fn from(query: &'a QueryParams) -> Self {
        let mut filter = Self {
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

#[async_trait]
impl Crud for PgProgramStorage {
    type Type = Program;
    type Id = ProgramId;
    type NewType = ProgramContent;
    type Error = AppError;
    type Filter = QueryParams;
    type PermissionFilter = User;

    async fn create(
        &self,
        new: Self::NewType,
        User(user): &Self::PermissionFilter,
    ) -> Result<Self::Type, Self::Error> {
        let (targets, vens) = extract_vens(new.targets);
        let business_id = extract_business_id(user)?;

        let mut tx = self.db.begin().await?;

        let program: Program = sqlx::query_as!(
            PostgresProgram,
            r#"
            INSERT INTO program (id,
                                 created_date_time,
                                 modification_date_time,
                                 program_name,
                                 program_long_name,
                                 retailer_name,
                                 retailer_long_name,
                                 program_type,
                                 country,
                                 principal_subdivision,
                                 interval_period,
                                 program_descriptions,
                                 binding_events,
                                 local_price,
                                 payload_descriptors,
                                 targets,
                                 business_id)
            VALUES (gen_random_uuid(), now(), now(), $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
            RETURNING id,
                      created_date_time,
                      modification_date_time,
                      program_name,
                      program_long_name,
                      retailer_name,
                      retailer_long_name,
                      program_type,
                      country,
                      principal_subdivision,
                      interval_period,
                      program_descriptions,
                      binding_events,
                      local_price,
                      payload_descriptors,
                      targets
            "#,
            new.program_name,
            new.program_long_name,
            new.retailer_name,
            new.retailer_long_name,
            new.program_type,
            new.country,
            new.principal_subdivision,
            to_json_value(new.interval_period)?,
            to_json_value(new.program_descriptions)?,
            new.binding_events,
            new.local_price,
            to_json_value(new.payload_descriptors)?,
            to_json_value(targets)?,
            business_id,
        )
            .fetch_one(&mut *tx)
            .await?
            .try_into()?;

        if let Some(vens) = vens {
            let rows_affected = sqlx::query!(
                r#"
                INSERT INTO ven_program (program_id, ven_id)
                    (SELECT $1, id FROM ven WHERE ven_name = ANY ($2))
                "#,
                program.id.as_str(),
                &vens
            )
            .execute(&mut *tx)
            .await?
            .rows_affected();
            if rows_affected as usize != vens.len() {
                Err(AppError::Conflict(
                    "One or multiple VEN names linked in the program do not exist".to_string(),
                    None,
                ))?
            }
        };
        tx.commit().await?;
        Ok(program)
    }

    async fn retrieve(
        &self,
        id: &Self::Id,
        User(user): &Self::PermissionFilter,
    ) -> Result<Self::Type, Self::Error> {
        Ok(sqlx::query_as!(
            PostgresProgram,
            r#"
            SELECT p.id,
                   p.created_date_time,
                   p.modification_date_time,
                   p.program_name,
                   p.program_long_name,
                   p.retailer_name,
                   p.retailer_long_name,
                   p.program_type,
                   p.country,
                   p.principal_subdivision,
                   p.interval_period,
                   p.program_descriptions,
                   p.binding_events,
                   p.local_price,
                   p.payload_descriptors,
                   p.targets
            FROM program p
              LEFT JOIN ven_program vp ON p.id = vp.program_id
            WHERE id = $1
              AND (NOT $2 OR vp.ven_id IS NULL OR vp.ven_id = ANY($3)) -- Filter for VEN ids
            "#,
            id.as_str(),
            user.is_ven(),
            &user.ven_ids_string()
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

        Ok(sqlx::query_as!(
            PostgresProgram,
            r#"
            SELECT p.id AS "id!", 
                   p.created_date_time AS "created_date_time!", 
                   p.modification_date_time AS "modification_date_time!",
                   p.program_name AS "program_name!",
                   p.program_long_name,
                   p.retailer_name,
                   p.retailer_long_name,
                   p.program_type,
                   p.country,
                   p.principal_subdivision,
                   p.interval_period,
                   p.program_descriptions,
                   p.binding_events,
                   p.local_price,
                   p.payload_descriptors,
                   p.targets
            FROM program p
              LEFT JOIN ven_program vp ON p.id = vp.program_id
              LEFT JOIN ven v ON v.id = vp.ven_id
              LEFT JOIN LATERAL (
                  SELECT p.id as p_id, 
                         json_array(jsonb_array_elements(p.targets)) <@ $1::jsonb AS target_test )
                  ON p.id = p_id
            WHERE ($1::jsonb = '[]'::jsonb OR target_test)
              AND (
                  ($2 AND (vp.ven_id IS NULL OR vp.ven_id = ANY($3)))
                  OR
                  ($4)
                  )
            GROUP BY p.id, p.created_date_time
            ORDER BY p.created_date_time DESC
            OFFSET $5 LIMIT $6
            "#,
            serde_json::to_value(pg_filter.targets)
                .map_err(AppError::SerdeJsonInternalServerError)?,
            user.is_ven(),
            &user.ven_ids_string(),
            user.is_business(),
            pg_filter.skip,
            pg_filter.limit,
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
        let (targets, vens) = extract_vens(new.targets);
        let business_id = extract_business_id(user)?;

        let mut tx = self.db.begin().await?;

        let program: Program = sqlx::query_as!(
            PostgresProgram,
            r#"
            UPDATE program p
            SET modification_date_time = now(),
                program_name = $2,
                program_long_name = $3,
                retailer_name = $4,
                retailer_long_name = $5,
                program_type = $6,
                country = $7,
                principal_subdivision = $8,
                interval_period = $9,
                program_descriptions = $10,
                binding_events = $11,
                local_price = $12,
                payload_descriptors = $13,
                targets = $14
            WHERE id = $1
                AND ($15::text IS NULL OR business_id = $15)
            RETURNING p.id,
                   p.created_date_time,
                   p.modification_date_time,
                   p.program_name,
                   p.program_long_name,
                   p.retailer_name,
                   p.retailer_long_name,
                   p.program_type,
                   p.country,
                   p.principal_subdivision,
                   p.interval_period,
                   p.program_descriptions,
                   p.binding_events,
                   p.local_price,
                   p.payload_descriptors,
                   p.targets
            "#,
            id.as_str(),
            new.program_name,
            new.program_long_name,
            new.retailer_name,
            new.retailer_long_name,
            new.program_type,
            new.country,
            new.principal_subdivision,
            to_json_value(new.interval_period)?,
            to_json_value(new.program_descriptions)?,
            new.binding_events,
            new.local_price,
            to_json_value(new.payload_descriptors)?,
            to_json_value(targets)?,
            business_id
        )
        .fetch_one(&mut *tx)
        .await?
        .try_into()?;

        if let Some(vens) = vens {
            sqlx::query!(
                r#"
                DELETE FROM ven_program WHERE program_id = $1
                "#,
                program.id.as_str()
            )
            .execute(&mut *tx)
            .await?;

            let rows_affected = sqlx::query!(
                r#"
                INSERT INTO ven_program (program_id, ven_id)
                    (SELECT $1, id FROM ven WHERE ven_name = ANY($2))
                "#,
                program.id.as_str(),
                &vens
            )
            .execute(&mut *tx)
            .await?
            .rows_affected();
            if rows_affected as usize != vens.len() {
                Err(AppError::BadRequest(
                    "One or multiple VEN names linked in the program do not exist",
                ))?
            }
        };
        tx.commit().await?;
        Ok(program)
    }

    async fn delete(
        &self,
        id: &Self::Id,
        User(user): &Self::PermissionFilter,
    ) -> Result<Self::Type, Self::Error> {
        let business_id = extract_business_id(user)?;

        Ok(sqlx::query_as!(
            PostgresProgram,
            r#"
            DELETE FROM program p
                   WHERE id = $1
                     AND ($2::text IS NULL OR business_id = $2)
            RETURNING p.id,
                   p.created_date_time,
                   p.modification_date_time,
                   p.program_name,
                   p.program_long_name,
                   p.retailer_name,
                   p.retailer_long_name,
                   p.program_type,
                   p.country,
                   p.principal_subdivision,
                   p.interval_period,
                   p.program_descriptions,
                   p.binding_events,
                   p.local_price,
                   p.payload_descriptors,
                   p.targets
            "#,
            id.as_str(),
            business_id,
        )
        .fetch_one(&self.db)
        .await?
        .try_into()?)
    }
}

#[cfg(test)]
#[cfg(feature = "live-db-test")]
mod tests {
    use crate::{
        api::program::QueryParams,
        data_source::{postgres::program::PgProgramStorage, Crud},
        error::AppError,
        jwt::{Claims, User},
    };
    use openadr_wire::{
        event::{EventPayloadDescriptor, EventType},
        interval::IntervalPeriod,
        program::{PayloadDescriptor, ProgramContent, ProgramDescription},
        target::{TargetEntry, TargetLabel, TargetMap},
        Program,
    };
    use sqlx::PgPool;

    impl Default for QueryParams {
        fn default() -> Self {
            Self {
                target_type: None,
                target_values: None,
                skip: 0,
                limit: 50,
            }
        }
    }

    fn program_1() -> Program {
        Program {
            id: "program-1".parse().unwrap(),
            created_date_time: "2024-07-25 08:31:10.776000 +00:00".parse().unwrap(),
            modification_date_time: "2024-07-25 08:31:10.776000 +00:00".parse().unwrap(),
            content: ProgramContent {
                object_type: Some(Default::default()),
                program_name: "program-1".to_string(),
                program_long_name: Some("program long name".to_string()),
                retailer_name: Some("retailer name".to_string()),
                retailer_long_name: Some("retailer long name".to_string()),
                program_type: Some("program type".to_string()),
                country: Some("country".to_string()),
                principal_subdivision: Some("principal-subdivision".to_string()),
                time_zone_offset: None,
                interval_period: Some(IntervalPeriod::new(
                    "2024-07-25 08:31:10.776000 +00:00".parse().unwrap(),
                )),
                program_descriptions: Some(vec![ProgramDescription {
                    url: "https://program-description-1.com".to_string(),
                }]),
                binding_events: Some(false),
                local_price: Some(true),
                payload_descriptors: Some(vec![PayloadDescriptor::EventPayloadDescriptor(
                    EventPayloadDescriptor::new(EventType::ExportPrice),
                )]),
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
            },
        }
    }

    fn program_2() -> Program {
        Program {
            id: "program-2".parse().unwrap(),
            created_date_time: "2024-07-25 08:31:10.776000 +00:00".parse().unwrap(),
            modification_date_time: "2024-07-25 08:31:10.776000 +00:00".parse().unwrap(),
            content: ProgramContent {
                object_type: Some(Default::default()),
                program_name: "program-2".to_string(),
                program_long_name: None,
                retailer_name: None,
                retailer_long_name: None,
                program_type: None,
                country: None,
                principal_subdivision: None,
                time_zone_offset: None,
                interval_period: None,
                program_descriptions: None,
                binding_events: None,
                local_price: None,
                payload_descriptors: None,
                targets: None,
            },
        }
    }

    fn program_3() -> Program {
        Program {
            id: "program-3".parse().unwrap(),
            content: ProgramContent {
                program_name: "program-3".to_string(),
                ..program_2().content
            },
            ..program_2()
        }
    }

    mod get_all {
        use super::*;
        use openadr_wire::target::TargetLabel;

        #[sqlx::test(fixtures("programs"))]
        async fn default_get_all(db: PgPool) {
            let repo: PgProgramStorage = db.into();
            let mut programs = repo
                .retrieve_all(&Default::default(), &User(Claims::any_business_user()))
                .await
                .unwrap();
            assert_eq!(programs.len(), 3);
            programs.sort_by(|a, b| a.id.as_str().cmp(b.id.as_str()));
            assert_eq!(programs, vec![program_1(), program_2(), program_3()]);
        }

        #[sqlx::test(fixtures("programs"))]
        async fn limit_get_all(db: PgPool) {
            let repo: PgProgramStorage = db.into();
            let programs = repo
                .retrieve_all(
                    &QueryParams {
                        limit: 1,
                        ..Default::default()
                    },
                    &User(Claims::any_business_user()),
                )
                .await
                .unwrap();
            assert_eq!(programs.len(), 1);
        }

        #[sqlx::test(fixtures("programs"))]
        async fn skip_get_all(db: PgPool) {
            let repo: PgProgramStorage = db.into();
            let programs = repo
                .retrieve_all(
                    &QueryParams {
                        skip: 1,
                        ..Default::default()
                    },
                    &User(Claims::any_business_user()),
                )
                .await
                .unwrap();
            assert_eq!(programs.len(), 2);

            let programs = repo
                .retrieve_all(
                    &QueryParams {
                        skip: 3,
                        ..Default::default()
                    },
                    &User(Claims::any_business_user()),
                )
                .await
                .unwrap();
            assert_eq!(programs.len(), 0);
        }

        #[sqlx::test(fixtures("programs"))]
        async fn filter_target_get_all(db: PgPool) {
            let repo: PgProgramStorage = db.into();

            let programs = repo
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
            assert_eq!(programs.len(), 1);

            let programs = repo
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
            assert_eq!(programs.len(), 0);
        }

        #[sqlx::test(fixtures("programs"))]
        async fn filter_multiple_targets(db: PgPool) {
            let repo: PgProgramStorage = db.into();

            let programs = repo
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
            assert_eq!(programs.len(), 0);
        }
    }

    mod get {
        use super::*;

        #[sqlx::test(fixtures("programs"))]
        async fn get_existing(db: PgPool) {
            let repo: PgProgramStorage = db.into();

            let program = repo
                .retrieve(
                    &"program-1".parse().unwrap(),
                    &User(Claims::any_business_user()),
                )
                .await
                .unwrap();
            assert_eq!(program, program_1());
        }

        #[sqlx::test(fixtures("programs"))]
        async fn get_not_existent(db: PgPool) {
            let repo: PgProgramStorage = db.into();
            let program = repo
                .retrieve(
                    &"program-not-existent".parse().unwrap(),
                    &User(Claims::any_business_user()),
                )
                .await;

            assert!(matches!(program, Err(AppError::NotFound)));
        }
    }

    mod add {
        use super::*;
        use chrono::{Duration, Utc};

        #[sqlx::test]
        async fn add(db: PgPool) {
            let repo: PgProgramStorage = db.into();

            let program = repo
                .create(program_1().content, &User(Claims::any_business_user()))
                .await
                .unwrap();
            assert!(program.created_date_time < Utc::now() + Duration::minutes(10));
            assert!(program.created_date_time > Utc::now() - Duration::minutes(10));
            assert!(program.modification_date_time < Utc::now() + Duration::minutes(10));
            assert!(program.modification_date_time > Utc::now() - Duration::minutes(10));
        }

        #[sqlx::test(fixtures("programs"))]
        async fn add_existing_name(db: PgPool) {
            let repo: PgProgramStorage = db.into();

            let program = repo
                .create(program_1().content, &User(Claims::any_business_user()))
                .await;
            assert!(matches!(program, Err(AppError::Conflict(_, _))));
        }
    }

    mod modify {
        use super::*;
        use chrono::{DateTime, Duration, Utc};

        #[sqlx::test(fixtures("programs"))]
        async fn updates_modify_time(db: PgPool) {
            let repo: PgProgramStorage = db.into();
            let program = repo
                .update(
                    &"program-1".parse().unwrap(),
                    program_1().content,
                    &User(Claims::any_business_user()),
                )
                .await
                .unwrap();

            assert_eq!(program.content, program_1().content);
            assert_eq!(
                program.created_date_time,
                "2024-07-25 08:31:10.776000 +00:00"
                    .parse::<DateTime<Utc>>()
                    .unwrap()
            );
            assert!(program.modification_date_time < Utc::now() + Duration::minutes(10));
            assert!(program.modification_date_time > Utc::now() - Duration::minutes(10));
        }

        #[sqlx::test(fixtures("programs"))]
        async fn update(db: PgPool) {
            let repo: PgProgramStorage = db.into();
            let mut updated = program_2().content;
            updated.program_name = "updated_name".parse().unwrap();

            let program = repo
                .update(
                    &"program-1".parse().unwrap(),
                    updated.clone(),
                    &User(Claims::any_business_user()),
                )
                .await
                .unwrap();

            assert_eq!(program.content, updated);
            let program = repo
                .retrieve(
                    &"program-1".parse().unwrap(),
                    &User(Claims::any_business_user()),
                )
                .await
                .unwrap();
            assert_eq!(program.content, updated);
        }
    }

    mod delete {
        use super::*;

        #[sqlx::test(fixtures("programs"))]
        async fn delete_existing(db: PgPool) {
            let repo: PgProgramStorage = db.into();
            let program = repo
                .delete(
                    &"program-1".parse().unwrap(),
                    &User(Claims::any_business_user()),
                )
                .await
                .unwrap();
            assert_eq!(program, program_1());

            let program = repo
                .retrieve(
                    &"program-1".parse().unwrap(),
                    &User(Claims::any_business_user()),
                )
                .await;
            assert!(matches!(program, Err(AppError::NotFound)));

            let program = repo
                .retrieve(
                    &"program-2".parse().unwrap(),
                    &User(Claims::any_business_user()),
                )
                .await
                .unwrap();
            assert_eq!(program, program_2());
        }

        #[sqlx::test(fixtures("programs"))]
        async fn delete_not_existing(db: PgPool) {
            let repo: PgProgramStorage = db.into();
            let program = repo
                .delete(
                    &"program-not-existing".parse().unwrap(),
                    &User(Claims::any_business_user()),
                )
                .await;
            assert!(matches!(program, Err(AppError::NotFound)));
        }
    }
}
