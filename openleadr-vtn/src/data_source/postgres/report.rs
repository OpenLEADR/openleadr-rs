use crate::{
    api::report::QueryParams,
    data_source::{
        postgres::{extract_business_ids, to_json_value},
        Crud, ReportCrud,
    },
    error::AppError,
    jwt::User,
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use openleadr_wire::{
    report::{ReportContent, ReportId},
    Report,
};
use sqlx::PgPool;
use tracing::{error, info, trace};

#[async_trait]
impl ReportCrud for PgReportStorage {}

pub(crate) struct PgReportStorage {
    db: PgPool,
}
impl From<PgPool> for PgReportStorage {
    fn from(db: PgPool) -> Self {
        Self { db }
    }
}

#[derive(Debug)]
struct PostgresReport {
    id: String,
    created_date_time: DateTime<Utc>,
    modification_date_time: DateTime<Utc>,
    program_id: String,
    event_id: String,
    client_name: String,
    report_name: Option<String>,
    payload_descriptors: Option<serde_json::Value>,
    resources: serde_json::Value,
}

impl TryFrom<PostgresReport> for Report {
    type Error = AppError;

    #[tracing::instrument(name = "TryFrom<PostgresReport> for Report")]
    fn try_from(value: PostgresReport) -> Result<Self, Self::Error> {
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
        let resources = serde_json::from_value(value.resources)
            .inspect_err(|err| error!(?err, "Failed to deserialize JSON from DB to `TargetMap`"))
            .map_err(AppError::SerdeJsonInternalServerError)?;

        Ok(Self {
            id: value.id.parse()?,
            created_date_time: value.created_date_time,
            modification_date_time: value.modification_date_time,
            content: ReportContent {
                program_id: value.program_id.parse()?,
                event_id: value.event_id.parse()?,
                client_name: value.client_name,
                report_name: value.report_name,
                payload_descriptors,
                resources,
            },
        })
    }
}

#[async_trait]
impl Crud for PgReportStorage {
    type Type = Report;
    type Id = ReportId;
    type NewType = ReportContent;
    type Error = AppError;
    type Filter = QueryParams;
    type PermissionFilter = User;

    async fn create(
        &self,
        new: Self::NewType,
        User(user): &Self::PermissionFilter,
    ) -> Result<Self::Type, Self::Error> {
        let _ = user; // FIXME implement object privacy

        let program_id = sqlx::query_scalar!(
            r#"
            SELECT program_id AS id FROM event WHERE id = $1
            "#,
            new.event_id.as_str(),
        )
        .fetch_one(&self.db)
        .await?;

        if program_id != new.program_id.as_str() {
            return Err(AppError::BadRequest(
                "event_id and program_id have to point to the same program",
            ));
        }

        let report: Report = sqlx::query_as!(
            PostgresReport,
            r#"
            INSERT INTO report (id, created_date_time, modification_date_time, program_id, event_id, client_name, report_name, payload_descriptors, resources)
            VALUES (gen_random_uuid(), now(), now(), $1, $2, $3, $4, $5, $6)
            RETURNING *
            "#,
            new.program_id.as_str(),
            new.event_id.as_str(),
            new.client_name,
            new.report_name,
            to_json_value(new.payload_descriptors)?,
            serde_json::to_value(new.resources).map_err(AppError::SerdeJsonBadRequest)?,
        )
            .fetch_one(&self.db)
            .await?
            .try_into()?;

        info!(report_id = report.id.as_str(), "created report");

        Ok(report)
    }

    async fn retrieve(
        &self,
        id: &Self::Id,
        User(user): &Self::PermissionFilter,
    ) -> Result<Self::Type, Self::Error> {
        let _ = user; // FIXME implement object privacy

        let report: Report = sqlx::query_as!(
            PostgresReport,
            r#"
            SELECT r.*
            FROM report r
                JOIN program p ON p.id = r.program_id
            WHERE r.id = $1
            "#,
            id.as_str(),
        )
        .fetch_one(&self.db)
        .await?
        .try_into()?;

        trace!(report_id = report.id.as_str(), "retrieved report");

        Ok(report)
    }

    async fn retrieve_all(
        &self,
        filter: &Self::Filter,
        User(user): &Self::PermissionFilter,
    ) -> Result<Vec<Self::Type>, Self::Error> {
        let _ = user; // FIXME implement object privacy

        let reports = sqlx::query_as!(
            PostgresReport,
            r#"
            SELECT r.*
            FROM report r
                JOIN program p ON p.id = r.program_id
            WHERE ($1::text IS NULL OR $1 like r.program_id)
              AND ($2::text IS NULL OR $2 like r.event_id)
              AND ($3::text IS NULL OR $3 like r.client_name)
            ORDER BY r.created_date_time DESC
            OFFSET $4 LIMIT $5
            "#,
            filter.program_id.clone().map(|x| x.to_string()),
            filter.event_id.clone().map(|x| x.to_string()),
            filter.client_name,
            filter.skip,
            filter.limit,
        )
        .fetch_all(&self.db)
        .await?
        .into_iter()
        .map(TryInto::try_into)
        .collect::<Result<Vec<Report>, _>>()?;

        trace!("retrieved {} reports", reports.len());

        Ok(reports)
    }

    async fn update(
        &self,
        id: &Self::Id,
        new: Self::NewType,
        User(user): &Self::PermissionFilter,
    ) -> Result<Self::Type, Self::Error> {
        let _ = user; // FIXME implement object privacy

        let report: Report = sqlx::query_as!(
            PostgresReport,
            r#"
            UPDATE report r
            SET modification_date_time = now(),
                program_id = $2,
                event_id = $3,
                client_name = $4,
                report_name = $5,
                payload_descriptors = $6,
                resources = $7
            FROM program p
            WHERE r.id = $1
              AND (p.id = r.program_id)
            RETURNING r.*
            "#,
            id.as_str(),
            new.program_id.as_str(),
            new.event_id.as_str(),
            new.client_name,
            new.report_name,
            to_json_value(new.payload_descriptors)?,
            serde_json::to_value(new.resources).map_err(AppError::SerdeJsonBadRequest)?,
        )
        .fetch_one(&self.db)
        .await?
        .try_into()?;

        info!(report_id = report.id.as_str(), "updated report");

        Ok(report)
    }

    async fn delete(
        &self,
        id: &Self::Id,
        User(user): &Self::PermissionFilter,
    ) -> Result<Self::Type, Self::Error> {
        let business_ids = extract_business_ids(user);

        let report: Report = sqlx::query_as!(
            PostgresReport,
            r#"
            DELETE FROM report r
                   USING program p
                   WHERE r.id = $1
                     AND r.program_id = p.id
                     AND ($2::text[] IS NULL OR p.business_id = ANY($2))
                   RETURNING r.*
            "#,
            id.as_str(),
            business_ids.as_deref(),
        )
        .fetch_one(&self.db)
        .await?
        .try_into()?;

        info!(report_id = report.id.as_str(), "deleted report");

        Ok(report)
    }
}
