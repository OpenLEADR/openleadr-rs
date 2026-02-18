use std::sync::Arc;

use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use serde::{Deserialize, Serialize};
use tracing::{info, instrument, trace};
use validator::Validate;

use openleadr_wire::{
    event::EventId,
    program::ProgramId,
    report::{ReportId, ReportRequest},
    subscription::{AnyObject, Operation},
    Report,
};

use crate::{
    api::{subscription, subscription::NotifierState, AppResponse, ValidatedJson, ValidatedQuery},
    data_source::ReportCrud,
    error::AppError,
    jwt::{Scope, User},
};

#[instrument(skip(user, report_source))]
pub async fn get_all(
    State(report_source): State<Arc<dyn ReportCrud>>,
    ValidatedQuery(query_params): ValidatedQuery<QueryParams>,
    User(user): User,
) -> AppResponse<Vec<Report>> {
    let reports = if user.scope.contains(Scope::ReadAll) {
        report_source.retrieve_all(&query_params, &None).await?
    } else if user.scope.contains(Scope::ReadVenObjects) {
        report_source
            .retrieve_all(&query_params, &Some(user.client_id()?))
            .await?
    } else {
        return Err(AppError::Forbidden(
            "Missing 'read_all' or 'read_ven_objects' scope",
        ));
    };

    trace!(client_id = user.sub, "retrieved {} reports", reports.len());

    Ok(Json(reports))
}

#[instrument(skip(user, report_source))]
pub async fn get(
    State(report_source): State<Arc<dyn ReportCrud>>,
    Path(id): Path<ReportId>,
    User(user): User,
) -> AppResponse<Report> {
    let report = if user.scope.contains(Scope::ReadAll) {
        report_source.retrieve(&id, &None).await?
    } else if user.scope.contains(Scope::ReadVenObjects) {
        report_source
            .retrieve(&id, &Some(user.client_id()?))
            .await?
    } else {
        return Err(AppError::Forbidden(
            "Missing 'read_all' or 'read_ven_objects' scope",
        ));
    };

    trace!(%report.id, report.report_name=report.content.report_name, client_id = user.sub, "retrieved report");

    Ok(Json(report))
}

#[instrument(skip(user, report_source, notifier_state))]
pub async fn add(
    State(report_source): State<Arc<dyn ReportCrud>>,
    State(notifier_state): State<Arc<NotifierState>>,
    User(user): User,
    ValidatedJson(new_report): ValidatedJson<ReportRequest>,
) -> Result<(StatusCode, Json<Report>), AppError> {
    let report = if user.scope.contains(Scope::WriteReports) {
        report_source
            .create(new_report, &Some(user.client_id()?))
            .await?
    } else {
        return Err(AppError::Forbidden("Missing 'write_reports' scope"));
    };

    info!(%report.id, report_name=?report.content.report_name, client_id = user.sub, "report created");

    subscription::notify(
        &notifier_state,
        Operation::Create,
        AnyObject::Report(report.clone()),
    );

    Ok((StatusCode::CREATED, Json(report)))
}

#[instrument(skip(user, report_source, notifier_state))]
pub async fn edit(
    State(report_source): State<Arc<dyn ReportCrud>>,
    State(notifier_state): State<Arc<NotifierState>>,
    Path(id): Path<ReportId>,
    User(user): User,
    ValidatedJson(content): ValidatedJson<ReportRequest>,
) -> AppResponse<Report> {
    let report = if user.scope.contains(Scope::WriteReports) {
        report_source
            .update(&id, content, &Some(user.client_id()?))
            .await?
    } else {
        return Err(AppError::Forbidden("Missing 'write_reports' scope"));
    };

    info!(%report.id, report_name=?report.content.report_name, client_id = user.sub, "report updated");

    subscription::notify(
        &notifier_state,
        Operation::Update,
        AnyObject::Report(report.clone()),
    );

    Ok(Json(report))
}

#[instrument(skip(user, report_source, notifier_state))]
pub async fn delete(
    State(report_source): State<Arc<dyn ReportCrud>>,
    State(notifier_state): State<Arc<NotifierState>>,
    User(user): User,
    Path(id): Path<ReportId>,
) -> AppResponse<Report> {
    // The specification does only allow VEN clients to have write access to reports.
    // Therefore, we can safely filter for the client_id, as there is no specified use-case
    // where a BL client can delete a report.
    // If a BL tried to delete a report, it would either fail by not having the `write_reports` scope or because it
    // or because the BLs client_id does not match the reports client_id.
    let report = if user.scope.contains(Scope::WriteReports) {
        report_source.delete(&id, &Some(user.client_id()?)).await?
    } else {
        return Err(AppError::Forbidden("Missing 'write_reports' scope"));
    };

    info!(%id, report_name=?report.content.report_name, client_id = user.sub, "deleted report");

    subscription::notify(
        &notifier_state,
        Operation::Delete,
        AnyObject::Report(report.clone()),
    );

    Ok(Json(report))
}

#[derive(Serialize, Deserialize, Validate, Debug)]
#[serde(rename_all = "camelCase")]
pub struct QueryParams {
    #[serde(rename = "programID")]
    pub(crate) program_id: Option<ProgramId>,
    #[serde(rename = "eventID")]
    pub(crate) event_id: Option<EventId>,
    #[validate(length(min = 1, max = 128))]
    pub(crate) client_name: Option<String>,
    #[serde(default)]
    pub(crate) skip: i64,
    #[validate(range(min = 1, max = 50))]
    #[serde(default = "get_50")]
    pub(crate) limit: i64,
}

fn get_50() -> i64 {
    50
}

#[cfg(test)]
#[cfg(feature = "live-db-test")]
mod test {
    use crate::{api::test::ApiTest, jwt::Scope};
    use axum::{body::Body, http, http::StatusCode};
    use openleadr_wire::{
        problem::Problem,
        report::{ReportPayloadDescriptor, ReportRequest, ReportType},
    };
    use sqlx::PgPool;

    fn default() -> ReportRequest {
        ReportRequest {
            event_id: "asdf".parse().unwrap(),
            client_name: "".to_string(),
            report_name: None,
            payload_descriptors: None,
            resources: vec![],
        }
    }

    #[sqlx::test]
    async fn name_constraint_validation(db: PgPool) {
        let test = ApiTest::new(
            db,
            "ven-1-client-id",
            vec![Scope::WriteVens, Scope::ReadTargets],
        )
        .await;

        let reports = [
            ReportRequest {
                report_name: Some("".to_string()),
                ..default()
            },
            ReportRequest {
                report_name: Some("This is more than 128 characters long and should be rejected This is more than 128 characters long and should be rejected asdfasd".to_string()),
                ..default()
            },
            ReportRequest {
                payload_descriptors: Some(vec![
                    ReportPayloadDescriptor{
                        payload_type: ReportType::Private("".to_string()),
                        reading_type: Default::default(),
                        units: None,
                        accuracy: None,
                        confidence: None,
                    }
                ]),
                ..default()
            },
            ReportRequest {
                payload_descriptors: Some(vec![
                    ReportPayloadDescriptor{
                        payload_type: ReportType::Private("This is more than 128 characters long and should be rejected This is more than 128 characters long and should be rejected asdfasd".to_string()),
                        reading_type: Default::default(),
                        units: None,
                        accuracy: None,
                        confidence: None,
                    }
                ]),
                ..default()
            },
        ];

        for report in &reports {
            let (status, error) = test
                .request::<Problem>(
                    http::Method::POST,
                    "/reports",
                    Body::from(serde_json::to_vec(&report).unwrap()),
                )
                .await;

            assert_eq!(status, StatusCode::BAD_REQUEST);
            assert!(error
                .detail
                .unwrap()
                .contains("outside of allowed range 1..=128"))
        }
    }
}
