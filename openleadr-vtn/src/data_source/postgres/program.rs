use crate::{
    api::program::QueryParams,
    data_source::{
        postgres::{get_ven_targets, intersection, to_json_value},
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

impl PgProgramStorage {
    /// The `client_id` functions as a permission filter here.
    /// It is provided if the request has [`ReadTargets`](Scope::ReadTargets) scope, which
    /// is the case for VEN clients (aka. customer logic).
    /// BL clients have a [`ReadAll`](Scope::ReadAll) scope, and therefore the API layer will
    /// call the [`retrieve_all_without_client_id`](PgProgramStorage::retrieve_all_without_client_id) function.
    async fn retrieve_all_with_client_id(
        &self,
        filter: &QueryParams,
        client_id: &ClientId,
    ) -> Result<Vec<Program>, AppError> {
        let ven_targets = get_ven_targets(self.db.clone(), client_id).await?;

        let filter_targets = intersection(&ven_targets, filter.targets.as_deref());

        sqlx::query_as!(
            PostgresProgram,
            r#"
            SELECT p.id,
                   p.created_date_time,
                   p.modification_date_time,
                   p.program_name,
                   p.interval_period,
                   p.program_descriptions,
                   p.payload_descriptors,
                   p.targets AS "targets:Vec<Target>",
                   p.attributes
            FROM program p
            WHERE
              -- according to the spec, we MUST only test query params
              -- against the program that the VEN object (and its resources) have as targets.
              -- Therefore, $1 is the intersection of the VEN targets and the filter targets.
              ($1::text[] IS NULL OR p.targets @> $1)
              AND (
                  -- IF the ven targets have at least one target in common with the program
                    p.targets && $2
                        -- or IF the program targets are empty
                        OR array_length(p.targets, 1) IS NULL
                  )
            ORDER BY created_date_time DESC
            OFFSET $3 LIMIT $4
            "#,
            filter_targets as _,
            ven_targets as _,
            filter.skip,
            filter.limit
        )
        .fetch_all(&self.db)
        .await?
        .into_iter()
        .map(|mut p| {
            // Limit the targets displayed to the VEN to the targets the VEN has access to.
            // Compare to the spec v3.1.1, Definition.md:
            //      Target hiding: For program and event objects, a VTN will only include requested targets in a response. This prevents VENs from learning targets that have
            //      not been explicitly assigned to them by BL. Target hiding is not performed on ven, resource, or subscription objects as these objects are read-able only by a specific VEN.
            p.targets = intersection(&p.targets, &ven_targets)
                .into_iter()
                .cloned()
                .collect();
            p
        })
        .map(TryInto::try_into)
        .collect::<Result<_, _>>()
    }

    /// The `client_id` functions as a permission filter here.
    /// It is provided if the request has [`ReadTargets`](Scope::ReadTargets) scope, which
    /// is the case for VEN clients (aka. customer logic).
    /// BL clients have a [`ReadAll`](Scope::ReadAll) scope, and therefore the API layer will
    /// call the [`retrieve_without_client_id`](PgProgramStorage::retrieve_without_client_id) function.
    async fn retrieve_with_client_id(
        &self,
        id: &ProgramId,
        client_id: &ClientId,
    ) -> Result<Program, AppError> {
        let ven_targets = get_ven_targets(self.db.clone(), client_id).await?;

        let mut pg_program = sqlx::query_as!(
            PostgresProgram,
            r#"
            SELECT p.id,
                   p.created_date_time,
                   p.modification_date_time,
                   p.program_name,
                   p.interval_period,
                   p.program_descriptions,
                   p.payload_descriptors,
                   p.targets AS "targets:Vec<Target>",
                   p.attributes
            FROM program p
            WHERE p.id = $1
              AND (
                  -- IF the ven targets have at least one target in common with the program
                    p.targets && $2
                        -- or IF the program targets are empty
                        OR array_length(p.targets, 1) IS NULL
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
        pg_program.targets = intersection(&pg_program.targets, &ven_targets)
            .into_iter()
            .cloned()
            .collect();

        pg_program.try_into()
    }

    async fn retrieve_all_without_client_id(
        &self,
        filter: &QueryParams,
    ) -> Result<Vec<Program>, AppError> {
        sqlx::query_as!(
            PostgresProgram,
            r#"
            SELECT p.id,
                   p.created_date_time,
                   p.modification_date_time,
                   p.program_name,
                   p.interval_period,
                   p.program_descriptions,
                   p.payload_descriptors,
                   p.targets AS "targets:Vec<Target>",
                   p.attributes
            FROM program p
            WHERE
              -- IF filter targets are empty, do not filter.
              -- IF filter targets are not empty, filter only if they are in the program targets.
              ($1::text[] IS NULL OR p.targets @> $1)
            ORDER BY created_date_time DESC
            OFFSET $2 LIMIT $3
            "#,
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

    async fn retrieve_without_client_id(&self, id: &ProgramId) -> Result<Program, AppError> {
        sqlx::query_as!(
            PostgresProgram,
            r#"
            SELECT p.id,
                   p.created_date_time,
                   p.modification_date_time,
                   p.program_name,
                   p.interval_period,
                   p.program_descriptions,
                   p.payload_descriptors,
                   p.targets AS "targets:Vec<Target>",
                   p.attributes
            FROM program p
            WHERE p.id = $1
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
    use crate::{
        api::{program::QueryParams, TargetQueryParams},
        data_source::{postgres::program::PgProgramStorage, Crud},
        error::AppError,
    };
    use openleadr_wire::{
        event::{EventPayloadDescriptor, EventType},
        interval::IntervalPeriod,
        program::{PayloadDescriptor, ProgramDescription, ProgramRequest},
        target::Target,
        ClientId, Program,
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

    pub fn without_targets<'a>(
        program: Program,
        targets: impl IntoIterator<Item = &'a str>,
    ) -> Program {
        let targets: Vec<Target> = targets.into_iter().map(|t| t.parse().unwrap()).collect();
        Program {
            content: ProgramRequest {
                targets: program
                    .content
                    .targets
                    .into_iter()
                    .filter(|t| !targets.contains(t))
                    .collect(),
                ..program.content
            },
            ..program
        }
    }

    mod get_all {
        use super::*;
        use openleadr_wire::ClientId;

        #[sqlx::test(fixtures("programs"))]
        async fn default_get_all(db: PgPool) {
            let repo: PgProgramStorage = db.into();
            let mut programs = repo.retrieve_all(&Default::default(), &None).await.unwrap();
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
                    &None,
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
                    &None,
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
                    &None,
                )
                .await
                .unwrap();
            assert_eq!(programs.len(), 0);
        }

        #[sqlx::test(fixtures("programs"))]
        // As this test does not use a client_id, it is mimicking the functionality
        // when a BL client does the request.
        async fn filter_target_get_all_bl_client(db: PgPool) {
            let repo: PgProgramStorage = db.into();

            let programs = repo
                .retrieve_all(
                    &QueryParams {
                        targets: TargetQueryParams(Some(vec!["group-1".parse().unwrap()])),
                        ..Default::default()
                    },
                    &None,
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
                    &None,
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
                    &None,
                )
                .await
                .unwrap();
            assert_eq!(programs.len(), 1);

            let programs = repo
                .retrieve_all(
                    &QueryParams {
                        targets: TargetQueryParams(Some(vec![
                            "group-1".parse().unwrap(),
                            "group-not-existent".parse().unwrap(),
                        ])),
                        ..Default::default()
                    },
                    &None,
                )
                .await
                .unwrap();
            assert_eq!(programs.len(), 0);

            let programs = repo
                .retrieve_all(
                    &QueryParams {
                        targets: TargetQueryParams(Some(vec!["group-2".parse().unwrap()])),
                        ..Default::default()
                    },
                    &None,
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
                    &None,
                )
                .await
                .unwrap();
            assert_eq!(programs.len(), 2);
        }

        #[sqlx::test(fixtures("programs", "vens"))]
        // As this test does use a client_id, it is mimicking the functionality
        // when a VEN client does the request.
        async fn filter_target_get_all_ven_client(db: PgPool) {
            let repo: PgProgramStorage = db.into();

            // Has access to targets "group-1" and "private-value"
            let ven_1: ClientId = "ven-1-client-id".parse().unwrap();

            // Has access to targets "group-2"
            let ven_2: ClientId = "ven-2-client-id".parse().unwrap();

            // ven_1 has access to targets "group-1" and "private-value"
            // which should allow access to program-1 and program-2, and program-3
            let programs = repo
                .retrieve_all(&QueryParams::default(), &Some(ven_1.clone()))
                .await
                .unwrap();
            assert_eq!(programs.len(), 3);
            assert_eq!(
                programs,
                vec![
                    program_1(),
                    without_targets(program_2(), ["group-2"]),
                    program_3()
                ]
            );

            let programs = repo
                .retrieve_all(
                    &QueryParams {
                        targets: TargetQueryParams(Some(vec!["group-1".parse().unwrap()])),
                        ..Default::default()
                    },
                    &Some(ven_1.clone()),
                )
                .await
                .unwrap();
            assert_eq!(programs.len(), 2);
            assert_eq!(
                programs,
                vec![program_1(), without_targets(program_2(), ["group-2"])]
            );

            let programs = repo
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
            assert_eq!(programs.len(), 1);
            assert_eq!(programs, vec![program_1()]);

            // filtering on "group-2" should not have any effect on the result
            // compared to no filtering as ven_1 does not have access to "group-2"
            let programs = repo
                .retrieve_all(
                    &QueryParams {
                        targets: TargetQueryParams(Some(vec!["group-2".parse().unwrap()])),
                        ..Default::default()
                    },
                    &Some(ven_1.clone()),
                )
                .await
                .unwrap();
            assert_eq!(programs.len(), 3);

            let programs = repo
                .retrieve_all(
                    &QueryParams {
                        targets: TargetQueryParams(Some(vec![
                            "group-1".parse().unwrap(),
                            "group-2".parse().unwrap(),
                        ])),
                        ..Default::default()
                    },
                    &Some(ven_1.clone()),
                )
                .await
                .unwrap();
            assert_eq!(programs.len(), 2);

            // ven_2 has access to target "group-2" which matches program-2.
            // Therefore, it should allow access to program-2 and program-3.
            let programs = repo
                .retrieve_all(&QueryParams::default(), &Some(ven_2.clone()))
                .await
                .unwrap();
            assert_eq!(programs.len(), 2);
            assert_eq!(
                programs,
                vec![without_targets(program_2(), ["group-1"]), program_3()]
            );

            // filtering on "group-1" should not have any effect on the result
            // compared to no filtering as ven_2 does not have access to "group-1"
            let programs = repo
                .retrieve_all(
                    &QueryParams {
                        targets: TargetQueryParams(Some(vec!["group-1".parse().unwrap()])),
                        ..Default::default()
                    },
                    &Some(ven_2.clone()),
                )
                .await
                .unwrap();
            assert_eq!(programs.len(), 2);
        }
    }

    #[sqlx::test(fixtures("programs", "vens"))]
    // Check that a client_id that can't be found returns all and only programs that don't have targets.
    async fn filter_target_get_all_ven_not_found(db: PgPool) {
        let repo: PgProgramStorage = db.into();

        let ven: ClientId = "does-not-exist".parse().unwrap();
        let programs = repo
            .retrieve_all(&QueryParams::default(), &Some(ven.clone()))
            .await
            .unwrap();
        assert_eq!(programs.len(), 1);
        assert_eq!(programs, vec![program_3()]);
    }

    #[sqlx::test(fixtures("programs", "vens"))]
    // Check that a client_id that matches a VEN without targets returns all and only programs that don't have targets.
    async fn filter_target_get_all_ven_without_targets(db: PgPool) {
        let repo: PgProgramStorage = db.into();

        let ven: ClientId = "ven-has-no-targets-client-id".parse().unwrap();
        let programs = repo
            .retrieve_all(&QueryParams::default(), &Some(ven.clone()))
            .await
            .unwrap();
        assert_eq!(programs.len(), 1);
        assert_eq!(programs, vec![program_3()]);
    }

    mod get {
        use super::*;

        #[sqlx::test(fixtures("programs"))]
        async fn get_existing(db: PgPool) {
            let repo: PgProgramStorage = db.into();

            let program = repo
                .retrieve(&"program-1".parse().unwrap(), &None)
                .await
                .unwrap();
            assert_eq!(program, program_1());
        }

        #[sqlx::test(fixtures("programs"))]
        async fn get_not_existent(db: PgPool) {
            let repo: PgProgramStorage = db.into();
            let program = repo
                .retrieve(&"program-not-existent".parse().unwrap(), &None)
                .await;

            assert!(matches!(program, Err(AppError::NotFound)));
        }

        #[sqlx::test(fixtures("programs", "vens"))]
        async fn get_as_ven_client(db: PgPool) {
            let repo: PgProgramStorage = db.into();

            // Has access to targets "group-2"
            let ven_2: ClientId = "ven-2-client-id".parse().unwrap();

            // ven_1 has access to target "group-2" and should therefore be able to
            // access program-2 and program-3.
            let err = repo
                .retrieve(&"program-1".parse().unwrap(), &Some(ven_2.clone()))
                .await;
            assert!(matches!(err, Err(AppError::NotFound)));
            let program = repo
                .retrieve(&"program-2".parse().unwrap(), &Some(ven_2.clone()))
                .await
                .unwrap();
            assert_eq!(program, without_targets(program_2(), ["group-1"]));
            let program = repo
                .retrieve(&"program-3".parse().unwrap(), &Some(ven_2.clone()))
                .await
                .unwrap();
            assert_eq!(program, program_3());
        }

        #[sqlx::test(fixtures("programs", "vens"))]
        async fn get_as_ven_without_targets(db: PgPool) {
            let repo: PgProgramStorage = db.into();

            // Has access to no targets
            let ven: ClientId = "ven-has-no-targets-client-id".parse().unwrap();

            // ven has no access to any targets and therefore should be able to access program-3 only
            let err = repo
                .retrieve(&"program-2".parse().unwrap(), &Some(ven.clone()))
                .await;
            assert!(matches!(err, Err(AppError::NotFound)));
            let event = repo
                .retrieve(&"program-3".parse().unwrap(), &Some(ven.clone()))
                .await
                .unwrap();
            assert_eq!(event, program_3());
        }

        #[sqlx::test(fixtures("programs", "vens"))]
        async fn get_as_ven_not_found(db: PgPool) {
            let repo: PgProgramStorage = db.into();

            let ven: ClientId = "ven-does-not-exist".parse().unwrap();

            // VEN object does not exist and therefore should be able to access program-3 only
            let err = repo
                .retrieve(&"program-2".parse().unwrap(), &Some(ven.clone()))
                .await;
            assert!(matches!(err, Err(AppError::NotFound)));
            let event = repo
                .retrieve(&"program-3".parse().unwrap(), &Some(ven.clone()))
                .await
                .unwrap();
            assert_eq!(event, program_3());
        }
    }

    mod add {
        use super::*;
        use chrono::{Duration, Utc};

        #[sqlx::test]
        async fn add(db: PgPool) {
            let repo: PgProgramStorage = db.into();

            let program = repo.create(program_1().content, &None).await.unwrap();
            assert!(program.created_date_time < Utc::now() + Duration::minutes(10));
            assert!(program.created_date_time > Utc::now() - Duration::minutes(10));
            assert!(program.modification_date_time < Utc::now() + Duration::minutes(10));
            assert!(program.modification_date_time > Utc::now() - Duration::minutes(10));
        }

        #[sqlx::test(fixtures("programs"))]
        async fn add_existing_name(db: PgPool) {
            let repo: PgProgramStorage = db.into();

            let program = repo.create(program_1().content, &None).await;
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
                .update(&"program-1".parse().unwrap(), program_1().content, &None)
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
                .update(&"program-1".parse().unwrap(), updated.clone(), &None)
                .await
                .unwrap();

            assert_eq!(program.content, updated);
            let program = repo
                .retrieve(&"program-1".parse().unwrap(), &None)
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
                .delete(&"program-1".parse().unwrap(), &None)
                .await
                .unwrap();
            assert_eq!(program, program_1());

            let program = repo.retrieve(&"program-1".parse().unwrap(), &None).await;
            assert!(matches!(program, Err(AppError::NotFound)));

            let program = repo
                .retrieve(&"program-2".parse().unwrap(), &None)
                .await
                .unwrap();
            assert_eq!(program, program_2());
        }

        #[sqlx::test(fixtures("programs"))]
        async fn delete_not_existing(db: PgPool) {
            let repo: PgProgramStorage = db.into();
            let program = repo
                .delete(&"program-not-existing".parse().unwrap(), &None)
                .await;
            assert!(matches!(program, Err(AppError::NotFound)));
        }
    }
}
