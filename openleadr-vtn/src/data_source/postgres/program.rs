use crate::{
    api::program::QueryParams,
    data_source::{
        postgres::{get_ven_targets, to_json_value},
        Crud, ProgramCrud,
    },
    error::AppError,
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use openleadr_wire::{
    program::{ProgramId, ProgramRequest},
    target::Target,
    ClientId, Program,
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
    interval_period: Option<serde_json::Value>,
    program_descriptions: Option<serde_json::Value>,
    payload_descriptors: Option<serde_json::Value>,
    attributes: Option<serde_json::Value>,
    targets: Vec<Target>,
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

        let attributes = match value.attributes {
            None => None,
            Some(t) => serde_json::from_value(t)
                .inspect_err(|err| {
                    error!(
                        ?err,
                        "Failed to deserialize JSON from DB to `Vec<ValuesMap>`"
                    )
                })
                .map_err(AppError::SerdeJsonInternalServerError)?,
        };

        Ok(Self {
            id: value.id.parse()?,
            created_date_time: value.created_date_time,
            modification_date_time: value.modification_date_time,
            content: ProgramRequest {
                program_name: value.program_name,
                interval_period,
                program_descriptions,
                payload_descriptors,
                attributes,
                targets: value.targets,
            },
        })
    }
}

#[async_trait]
impl Crud for PgProgramStorage {
    type Type = Program;
    type Id = ProgramId;
    type NewType = ProgramRequest;
    type Error = AppError;
    type Filter = QueryParams;
    type PermissionFilter = Option<ClientId>;

    async fn create(
        &self,
        new: Self::NewType,
        _client_id: &Self::PermissionFilter,
    ) -> Result<Self::Type, Self::Error> {
        let program: Program = sqlx::query_as!(
            PostgresProgram,
            r#"
            INSERT INTO program (id,
                                 created_date_time,
                                 modification_date_time,
                                 program_name,
                                 interval_period,
                                 program_descriptions,
                                 payload_descriptors,
                                 targets,
                                 attributes)
            VALUES (gen_random_uuid(), now(), now(), $1, $2, $3, $4, $5, $6)
            RETURNING id,
                      created_date_time,
                      modification_date_time,
                      program_name,
                      interval_period,
                      program_descriptions,
                      payload_descriptors,
                      targets as  "targets:Vec<Target>",
                      attributes
            "#,
            new.program_name,
            to_json_value(new.interval_period)?,
            to_json_value(new.program_descriptions)?,
            to_json_value(new.payload_descriptors)?,
            new.targets.as_slice() as &[Target],
            to_json_value(new.attributes)?,
        )
        .fetch_one(&self.db)
        .await?
        .try_into()?;

        Ok(program)
    }

    async fn retrieve(
        &self,
        id: &Self::Id,
        client_id: &Self::PermissionFilter,
    ) -> Result<Self::Type, Self::Error> {
        let ven_targets = get_ven_targets(self.db.clone(), client_id).await?;

        Ok(sqlx::query_as!(
            PostgresProgram,
            r#"
            SELECT p.id,
                   p.created_date_time,
                   p.modification_date_time,
                   p.program_name,
                   p.interval_period,
                   p.program_descriptions,
                   p.payload_descriptors,
                   p.targets as  "targets:Vec<Target>",
                   p.attributes
            FROM program p
            WHERE id = $1
              AND p.targets @> $2
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
            PostgresProgram,
            r#"
            SELECT p.id AS "id!",
                   p.created_date_time AS "created_date_time!",
                   p.modification_date_time AS "modification_date_time!",
                   p.program_name AS "program_name!",
                   p.interval_period,
                   p.program_descriptions,
                   p.payload_descriptors,
                   p.targets as  "targets:Vec<Target>",
                   p.attributes
            FROM program p
            WHERE ($1::text[] IS NULL OR p.targets @> $1)
              AND p.targets @> $2
            GROUP BY p.id, p.created_date_time
            ORDER BY p.created_date_time DESC
            OFFSET $3 LIMIT $4
            "#,
            filter.targets.as_deref() as _,
            ven_targets as _,
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
        _client_id: &Self::PermissionFilter,
    ) -> Result<Self::Type, Self::Error> {
        let program: Program = sqlx::query_as!(
            PostgresProgram,
            r#"
            UPDATE program p
            SET modification_date_time = now(),
                program_name = $2,
                interval_period = $3,
                program_descriptions = $4,
                payload_descriptors = $5,
                targets = $6,
                attributes = $7
            WHERE id = $1
            RETURNING p.id,
                   p.created_date_time,
                   p.modification_date_time,
                   p.program_name,
                   p.interval_period,
                   p.program_descriptions,
                   p.payload_descriptors,
                   p.targets as "targets:Vec<Target>",
                   p.attributes
            "#,
            id.as_str(),
            new.program_name,
            to_json_value(new.interval_period)?,
            to_json_value(new.program_descriptions)?,
            to_json_value(new.payload_descriptors)?,
            new.targets.as_slice() as _,
            to_json_value(new.attributes)?
        )
        .fetch_one(&self.db)
        .await?
        .try_into()?;

        Ok(program)
    }

    async fn delete(
        &self,
        id: &Self::Id,
        _client_id: &Self::PermissionFilter,
    ) -> Result<Self::Type, Self::Error> {
        Ok(sqlx::query_as!(
            PostgresProgram,
            r#"
            DELETE FROM program p
                   WHERE id = $1
            RETURNING p.id,
                   p.created_date_time,
                   p.modification_date_time,
                   p.program_name,
                   p.interval_period,
                   p.program_descriptions,
                   p.payload_descriptors,
                   p.targets as  "targets:Vec<Target>",
                   p.attributes
            "#,
            id.as_str(),
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
        program::{PayloadDescriptor, ProgramDescription, ProgramRequest},
        target::Target,
        Program,
    };
    use sqlx::PgPool;
    use std::str::FromStr;

    impl Default for QueryParams {
        fn default() -> Self {
            Self {
                targets: TargetQueryParams(None),
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
            content: ProgramRequest {
                program_name: "program-1".to_string(),
                interval_period: Some(IntervalPeriod::new(
                    "2024-07-25 08:31:10.776000 +00:00".parse().unwrap(),
                )),
                program_descriptions: Some(vec![ProgramDescription {
                    url: "https://program-description-1.com".to_string(),
                }]),
                payload_descriptors: Some(vec![PayloadDescriptor::EventPayloadDescriptor(
                    EventPayloadDescriptor::new(EventType::ExportPrice),
                )]),
                attributes: None,
                targets: vec![
                    Target::from_str("group-1").unwrap(),
                    Target::from_str("private-value").unwrap(),
                ],
            },
        }
    }

    fn program_2() -> Program {
        Program {
            id: "program-2".parse().unwrap(),
            created_date_time: "2024-07-25 08:31:10.776000 +00:00".parse().unwrap(),
            modification_date_time: "2024-07-25 08:31:10.776000 +00:00".parse().unwrap(),
            content: ProgramRequest {
                program_name: "program-2".to_string(),
                interval_period: None,
                program_descriptions: None,
                payload_descriptors: None,
                attributes: None,
                targets: vec![
                    Target::from_str("group-1").unwrap(),
                    Target::from_str("group-2").unwrap(),
                ],
            },
        }
    }

    fn program_3() -> Program {
        Program {
            id: "program-3".parse().unwrap(),
            content: ProgramRequest {
                program_name: "program-3".to_string(),
                targets: vec![],
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
                        targets: TargetQueryParams(Some(vec!["group-1".parse().unwrap()])),
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
                        targets: TargetQueryParams(Some(vec!["not-existent".parse().unwrap()])),
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
                        targets: TargetQueryParams(Some(vec![
                            "group-1".parse().unwrap(),
                            "group-2".parse().unwrap(),
                        ])),
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
                        targets: TargetQueryParams(Some(vec![
                            "group-1".parse().unwrap(),
                            "group-not-existent".parse().unwrap(),
                        ])),
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
                        targets: TargetQueryParams(Some(vec!["group-2".parse().unwrap()])),
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
                        targets: TargetQueryParams(Some(vec!["group-1".parse().unwrap()])),
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
