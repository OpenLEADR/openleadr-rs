use std::sync::Arc;

use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use serde::{Deserialize, Serialize};
use tracing::{info, instrument};
use validator::Validate;

use openadr_wire::{
    event::EventId,
    program::ProgramId,
    report::{ReportContent, ReportId},
    Report,
};

use crate::{
    api::{AppResponse, ValidatedJson, ValidatedQuery},
    data_source::ReportCrud,
    error::AppError,
    jwt::{BusinessUser, User, VENUser},
};

#[instrument(skip(user, report_source))]
pub async fn get_all(
    State(report_source): State<Arc<dyn ReportCrud>>,
    ValidatedQuery(query_params): ValidatedQuery<QueryParams>,
    User(user): User,
) -> AppResponse<Vec<Report>> {
    let reports = report_source.retrieve_all(&query_params, &user).await?;

    Ok(Json(reports))
}

#[instrument(skip(user, report_source))]
pub async fn get(
    State(report_source): State<Arc<dyn ReportCrud>>,
    Path(id): Path<ReportId>,
    User(user): User,
) -> AppResponse<Report> {
    let report: Report = report_source.retrieve(&id, &user).await?;
    Ok(Json(report))
}

#[instrument(skip(user, report_source))]
pub async fn add(
    State(report_source): State<Arc<dyn ReportCrud>>,
    VENUser(user): VENUser,
    ValidatedJson(new_report): ValidatedJson<ReportContent>,
) -> Result<(StatusCode, Json<Report>), AppError> {
    let report = report_source.create(new_report, &user).await?;

    info!(%report.id, report_name=?report.content.report_name, "report created");

    Ok((StatusCode::CREATED, Json(report)))
}

#[instrument(skip(user, report_source))]
pub async fn edit(
    State(report_source): State<Arc<dyn ReportCrud>>,
    Path(id): Path<ReportId>,
    VENUser(user): VENUser,
    ValidatedJson(content): ValidatedJson<ReportContent>,
) -> AppResponse<Report> {
    let report = report_source.update(&id, content, &user).await?;

    info!(%report.id, report_name=?report.content.report_name, "report updated");

    Ok(Json(report))
}

#[instrument(skip(user, report_source))]
pub async fn delete(
    State(report_source): State<Arc<dyn ReportCrud>>,
    // TODO this contradicts the spec, which says that only VENs have write access
    BusinessUser(user): BusinessUser,
    Path(id): Path<ReportId>,
) -> AppResponse<Report> {
    let report = report_source.delete(&id, &user).await?;
    info!(%id, "deleted report");
    Ok(Json(report))
}

#[derive(Serialize, Deserialize, Validate, Debug)]
#[serde(rename_all = "camelCase")]
pub struct QueryParams {
    #[serde(rename = "programID")]
    pub(crate) program_id: Option<ProgramId>,
    #[serde(rename = "eventID")]
    pub(crate) event_id: Option<EventId>,
    pub(crate) client_name: Option<String>,
    #[serde(default)]
    pub(crate) skip: i64,
    // TODO how to interpret limit = 0 and what is the default?
    #[validate(range(max = 50))]
    #[serde(default = "get_50")]
    pub(crate) limit: i64,
}

fn get_50() -> i64 {
    50
}
