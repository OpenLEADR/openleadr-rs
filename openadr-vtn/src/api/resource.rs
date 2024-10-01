use std::sync::Arc;

use axum::{
    extract::{Path, State},
    Json,
};
use openadr_wire::ven::VenId;
use reqwest::StatusCode;
use serde::Deserialize;
use tracing::{info, trace};
use validator::{Validate, ValidationError};

use openadr_wire::{
    resource::{Resource, ResourceContent, ResourceId},
    target::TargetLabel,
};

use crate::{
    api::{AppResponse, ValidatedJson, ValidatedQuery},
    data_source::ResourceCrud,
    error::AppError,
    jwt::{Claims, User},
};

fn has_write_permission(user_claims: &Claims, ven_id: &VenId) -> Result<(), AppError> {
    if user_claims.is_ven_manager() {
        return Ok(());
    }

    if user_claims.is_ven() && user_claims.ven_ids().contains(ven_id) {
        return Ok(());
    }

    Err(AppError::Forbidden(
        "User not authorized to access this resource",
    ))
}

pub async fn get_all(
    State(resource_source): State<Arc<dyn ResourceCrud>>,
    Path(ven_id): Path<VenId>,
    ValidatedQuery(query_params): ValidatedQuery<QueryParams>,
    User(user): User,
) -> AppResponse<Vec<Resource>> {
    has_write_permission(&user, &ven_id)?;
    trace!(?query_params);

    let resources = resource_source
        .retrieve_all(ven_id, &query_params, &user)
        .await?;

    Ok(Json(resources))
}

pub async fn get(
    State(resource_source): State<Arc<dyn ResourceCrud>>,
    Path((ven_id, id)): Path<(VenId, ResourceId)>,
    User(user): User,
) -> AppResponse<Resource> {
    has_write_permission(&user, &ven_id)?;
    let ven = resource_source.retrieve(&id, ven_id, &user).await?;

    Ok(Json(ven))
}

pub async fn add(
    State(resource_source): State<Arc<dyn ResourceCrud>>,
    User(user): User,
    Path(ven_id): Path<VenId>,
    ValidatedJson(new_resource): ValidatedJson<ResourceContent>,
) -> Result<(StatusCode, Json<Resource>), AppError> {
    has_write_permission(&user, &ven_id)?;
    let ven = resource_source.create(new_resource, ven_id, &user).await?;

    Ok((StatusCode::CREATED, Json(ven)))
}

pub async fn edit(
    State(resource_source): State<Arc<dyn ResourceCrud>>,
    Path((ven_id, id)): Path<(VenId, ResourceId)>,
    User(user): User,
    ValidatedJson(content): ValidatedJson<ResourceContent>,
) -> AppResponse<Resource> {
    has_write_permission(&user, &ven_id)?;
    let resource = resource_source.update(&id, ven_id, content, &user).await?;

    info!(%resource.id, resource.resource_name=resource.content.resource_name, "resource updated");

    Ok(Json(resource))
}

pub async fn delete(
    State(resource_source): State<Arc<dyn ResourceCrud>>,
    Path((ven_id, id)): Path<(VenId, ResourceId)>,
    User(user): User,
) -> AppResponse<Resource> {
    has_write_permission(&user, &ven_id)?;
    let resource = resource_source.delete(&id, ven_id, &user).await?;
    info!(%id, "deleted resource");
    Ok(Json(resource))
}

#[derive(Deserialize, Validate, Debug)]
#[validate(schema(function = "validate_target_type_value_pair"))]
#[serde(rename_all = "camelCase")]
pub struct QueryParams {
    pub(crate) target_type: Option<TargetLabel>,
    pub(crate) target_values: Option<Vec<String>>,
    #[serde(default)]
    #[validate(range(min = 0))]
    pub(crate) skip: i64,
    #[validate(range(min = 1, max = 50))]
    #[serde(default = "get_50")]
    pub(crate) limit: i64,
}

fn validate_target_type_value_pair(query: &QueryParams) -> Result<(), ValidationError> {
    if query.target_type.is_some() == query.target_values.is_some() {
        Ok(())
    } else {
        Err(ValidationError::new("targetType and targetValues query parameter must either both be set or not set at the same time."))
    }
}

fn get_50() -> i64 {
    50
}
