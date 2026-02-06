use std::{collections::HashMap, sync::Arc};

use axum::{
    extract::{Path, State},
    Json,
};
use openleadr_wire::{
    program::ProgramId,
    subscription::{Subscription, SubscriptionId, SubscriptionRequest},
    ObjectType,
};
use reqwest::StatusCode;
use serde::Deserialize;
use tokio::sync::Mutex;
use tracing::{info, trace};
use validator::Validate;

use crate::{
    api::{AppResponse, ValidatedJson, ValidatedQuery},
    data_source::SubscriptionCrud,
    error::AppError,
    jwt::{Scope, User},
    state::AppState,
};

pub(crate) struct NotifierState {
    subscriptions: Mutex<HashMap<SubscriptionId, Subscription>>,
}

impl NotifierState {
    pub(crate) async fn load_from_storage(
        storage: &dyn SubscriptionCrud,
    ) -> Result<Self, AppError> {
        let subscriptions = storage
            .retrieve_all(
                &QueryParams {
                    program_id: None,
                    objects: None,
                    skip: 0,
                    limit: i64::MAX,
                },
                &None,
            )
            .await?;

        Ok(Self {
            subscriptions: Mutex::new(
                subscriptions
                    .into_iter()
                    .map(|subscription| (subscription.id.clone(), subscription))
                    .collect(),
            ),
        })
    }
}

pub async fn get_all(
    State(subscription_source): State<Arc<dyn SubscriptionCrud>>,
    ValidatedQuery(query_params): ValidatedQuery<QueryParams>,
    User(user): User,
) -> AppResponse<Vec<Subscription>> {
    trace!(?query_params);

    let resources = if user.scope.contains(Scope::ReadAll) {
        subscription_source
            .retrieve_all(&query_params, &None)
            .await?
    } else if user.scope.contains(Scope::ReadVenObjects) {
        subscription_source
            .retrieve_all(&query_params, &Some(user.client_id()?))
            .await?
    } else {
        return Err(AppError::Forbidden(
            "Missing 'read_all' or 'read_ven_objects' scope",
        ));
    };

    trace!(
        client_id = user.sub,
        "retrieved {} resources",
        resources.len()
    );

    Ok(Json(resources))
}

pub async fn get(
    State(subscription_source): State<Arc<dyn SubscriptionCrud>>,
    Path(id): Path<SubscriptionId>,
    User(user): User,
) -> AppResponse<Subscription> {
    let subscription = if user.scope.contains(Scope::ReadAll) {
        subscription_source.retrieve(&id, &None).await?
    } else if user.scope.contains(Scope::ReadVenObjects) {
        subscription_source
            .retrieve(&id, &Some(user.client_id()?))
            .await?
    } else {
        return Err(AppError::Forbidden(
            "Missing 'read_all' or 'read_ven_objects' scope",
        ));
    };

    trace!(
        %subscription.id,
        subscription.program_id=?subscription.content.program_id,
        client_id = user.sub,
        "subscription retrieved"
    );

    Ok(Json(subscription))
}

pub async fn add(
    State(subscription_source): State<Arc<dyn SubscriptionCrud>>,
    State(app_state): State<AppState>,
    User(user): User,
    ValidatedJson(new_subscription): ValidatedJson<SubscriptionRequest>,
) -> Result<(StatusCode, Json<Subscription>), AppError> {
    let client_id = user.client_id()?;

    let subscription = if user.scope.contains(Scope::WriteSubscriptions) {
        subscription_source
            .create(new_subscription, &Some(client_id))
            .await?
    } else {
        return Err(AppError::Forbidden("Missing 'write_vens' scope"));
    };

    app_state
        .notifier
        .subscriptions
        .lock()
        .await
        .insert(subscription.id.clone(), subscription.clone());

    info!(
        %subscription.id,
        subscription.program_id=?subscription.content.program_id,
        client_id = user.sub,
        "resource added"
    );

    Ok((StatusCode::CREATED, Json(subscription)))
}

pub async fn edit(
    State(subscription_source): State<Arc<dyn SubscriptionCrud>>,
    State(app_state): State<AppState>,
    Path(id): Path<SubscriptionId>,
    User(user): User,
    ValidatedJson(update): ValidatedJson<SubscriptionRequest>,
) -> AppResponse<Subscription> {
    let subscription = if user.scope.contains(Scope::WriteSubscriptions) {
        subscription_source
            .update(&id, update, &Some(user.client_id()?))
            .await?
    } else {
        return Err(AppError::Forbidden("Missing 'write_subscriptions' scope"));
    };

    app_state
        .notifier
        .subscriptions
        .lock()
        .await
        .insert(subscription.id.clone(), subscription.clone());

    info!(
        %subscription.id,
        subscription.program_id=?subscription.content.program_id,
        client_id = user.sub,
        "resource updated"
    );

    Ok(Json(subscription))
}

pub async fn delete(
    State(subscription_source): State<Arc<dyn SubscriptionCrud>>,
    State(app_state): State<AppState>,
    Path(id): Path<SubscriptionId>,
    User(user): User,
) -> AppResponse<Subscription> {
    let subscription = if user.scope.contains(Scope::WriteSubscriptions) {
        subscription_source
            .delete(&id, &Some(user.client_id()?))
            .await?
    } else {
        return Err(AppError::Forbidden("Missing 'write_subscriptions' scope"));
    };

    app_state
        .notifier
        .subscriptions
        .lock()
        .await
        .remove(&subscription.id);

    info!(%id, client_id = user.sub, "deleted subscription");

    Ok(Json(subscription))
}

#[derive(Deserialize, Validate, Debug)]
#[serde(rename_all = "camelCase")]
pub struct QueryParams {
    #[serde(rename = "programID")]
    pub(crate) program_id: Option<ProgramId>,
    pub(crate) objects: Option<Vec<ObjectType>>,
    #[serde(default)]
    #[validate(range(min = 0))]
    pub(crate) skip: i64,
    #[validate(range(min = 1, max = 50))]
    #[serde(default = "get_50")]
    pub(crate) limit: i64,
}

fn get_50() -> i64 {
    50
}
