use crate::{
    api::program::QueryParams,
    data_source::{
        postgres::{extract_business_id, to_json_value},
        Crud, ProgramCrud,
    },
    error::AppError,
    jwt::User,
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use openleadr_wire::{
    program::{ProgramContent, ProgramId},
    target::Target,
    Program,
};
use sqlx::PgPool;
use tracing::error;

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
    targets: Vec<String>,
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
        let targets = value
            .targets
            .into_iter()
            .map(|t| {
                Target::new(&t)
                    .inspect_err(|err| {
                        error!(
                            ?err,
                            "Failed to deserialize text[] from DB to `Vec<Target>`"
                        )
                    })
                    .map_err(AppError::Identifier)
            })
            .collect::<Result<Vec<Target>, AppError>>()?;

        Ok(Self {
            id: value.id.parse()?,
            created_date_time: value.created_date_time,
            modification_date_time: value.modification_date_time,
            content: ProgramContent {
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
                targets: Some(targets),
            },
        })
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
        let business_id = extract_business_id(user)?;
        let targets = new
            .targets
            .unwrap_or(vec![])
            .into_iter()
            .map(|t| t.as_str().to_owned())
            .collect::<Vec<String>>();

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
            &targets,
            business_id,
        )
            .fetch_one(&self.db)
            .await?
            .try_into()?;

        Ok(program)
    }

    async fn retrieve(
        &self,
        id: &Self::Id,
        User(user): &Self::PermissionFilter,
    ) -> Result<Self::Type, Self::Error> {
        let _ = user; // FIXME implement object privacy

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
            WHERE id = $1
            "#,
            id.as_str(),
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
        let _ = user; // FIXME implement object privacy

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
            WHERE ($1::text[] IS NULL OR p.targets && $1) -- FIXME use @> for and rather than or filtering
            GROUP BY p.id, p.created_date_time
            ORDER BY p.created_date_time DESC
            OFFSET $2 LIMIT $3
            "#,
            filter.targets.targets.as_deref(),
            filter.skip,
            filter.limit,
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
        let _ = user; // FIXME implement object privacy

        let targets = new
            .targets
            .unwrap_or(vec![])
            .into_iter()
            .map(|t| t.as_str().to_owned())
            .collect::<Vec<String>>();

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
            &targets,
        )
        .fetch_one(&self.db)
        .await?
        .try_into()?;

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
        api::{program::QueryParams, TargetQueryParams},
        data_source::{postgres::program::PgProgramStorage, Crud},
        error::AppError,
        jwt::{Claims, User},
    };
    use openleadr_wire::{
        event::{EventPayloadDescriptor, EventType},
        interval::IntervalPeriod,
        program::{PayloadDescriptor, ProgramContent, ProgramDescription},
        target::Target,
        Program,
    };
    use sqlx::PgPool;

    impl Default for QueryParams {
        fn default() -> Self {
            Self {
                targets: TargetQueryParams { targets: None },
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
                targets: Some(vec![
                    Target::new("group-1").unwrap(),
                    Target::new("private-value").unwrap(),
                ]),
            },
        }
    }

    fn program_2() -> Program {
        Program {
            id: "program-2".parse().unwrap(),
            created_date_time: "2024-07-25 08:31:10.776000 +00:00".parse().unwrap(),
            modification_date_time: "2024-07-25 08:31:10.776000 +00:00".parse().unwrap(),
            content: ProgramContent {
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
                targets: Some(vec![
                    Target::new("group-1").unwrap(),
                    Target::new("group-2").unwrap(),
                ]),
            },
        }
    }

    fn program_3() -> Program {
        Program {
            id: "program-3".parse().unwrap(),
            content: ProgramContent {
                program_name: "program-3".to_string(),
                targets: None,
                ..program_2().content
            },
            ..program_2()
        }
    }

    mod get_all {
        use super::*;

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
                        targets: TargetQueryParams {
                            targets: Some(vec!["group-1".to_string()]),
                        },
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
                        targets: TargetQueryParams {
                            targets: Some(vec!["not-existent".to_string()]),
                        },
                        ..Default::default()
                    },
                    &User(Claims::any_business_user()),
                )
                .await
                .unwrap();
            assert_eq!(programs.len(), 0);
        }

        #[sqlx::test(fixtures("programs"))]
        async fn filter_multiple_target_values(db: PgPool) {
            let repo: PgProgramStorage = db.into();

            let programs = repo
                .retrieve_all(
                    &QueryParams {
                        targets: TargetQueryParams {
                            targets: Some(vec!["group-1".to_string(), "group-2".to_string()]),
                        },
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
                        targets: TargetQueryParams {
                            targets: Some(vec![
                                "group-1".to_string(),
                                "group-not-existent".to_string(),
                            ]),
                        },
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
                        targets: TargetQueryParams {
                            targets: Some(vec!["group-2".to_string()]),
                        },
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
                        targets: TargetQueryParams {
                            targets: Some(vec!["group-1".to_string()]),
                        },
                        ..Default::default()
                    },
                    &User(Claims::any_business_user()),
                )
                .await
                .unwrap();
            assert_eq!(programs.len(), 2);
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
