use std::{collections::HashMap, sync::Arc};

use axum::{
    extract::{
        self,
        ws::{Message, WebSocketUpgrade},
        Path,
    },
    response::Response,
    Json,
};
use openleadr_wire::{
    program::ProgramId,
    subscription::{
        AnyObject, Notification, NotifiersResponse, Operation, Subscription, SubscriptionId,
        SubscriptionRequest,
    },
    ClientId, ObjectType,
};
use reqwest::StatusCode;
use serde::Deserialize;
use tokio::sync::{mpsc, Mutex};
use tracing::{info, trace};
use validator::Validate;

use crate::{
    api::{AppResponse, ValidatedJson, ValidatedQuery},
    data_source::SubscriptionCrud,
    error::AppError,
    jwt::{Scope, User},
    state::AppState,
};

pub(crate) struct State {
    websockets: Mutex<HashMap<ClientId, mpsc::UnboundedSender<Notification>>>,
}

impl State {
    pub(crate) fn new() -> Self {
        Self {
            websockets: Mutex::new(HashMap::new()),
        }
    }
}

pub async fn get_all(
    axum::extract::State(subscription_source): axum::extract::State<Arc<dyn SubscriptionCrud>>,
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
    axum::extract::State(subscription_source): axum::extract::State<Arc<dyn SubscriptionCrud>>,
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
    axum::extract::State(subscription_source): axum::extract::State<Arc<dyn SubscriptionCrud>>,
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

    info!(
        %subscription.id,
        subscription.program_id=?subscription.content.program_id,
        client_id = user.sub,
        "resource added"
    );

    Ok((StatusCode::CREATED, Json(subscription)))
}

pub async fn edit(
    axum::extract::State(subscription_source): axum::extract::State<Arc<dyn SubscriptionCrud>>,
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

    info!(
        %subscription.id,
        subscription.program_id=?subscription.content.program_id,
        client_id = user.sub,
        "resource updated"
    );

    Ok(Json(subscription))
}

pub async fn delete(
    axum::extract::State(subscription_source): axum::extract::State<Arc<dyn SubscriptionCrud>>,
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

    info!(%id, client_id = user.sub, "deleted subscription");

    Ok(Json(subscription))
}

#[derive(Deserialize, Validate, Debug)]
#[serde(rename_all = "camelCase")]
pub struct QueryParams {
    #[serde(rename = "programID")]
    pub(crate) program_id: Option<ProgramId>,
    #[serde(default)]
    pub(crate) objects: Vec<ObjectType>,
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

pub(crate) fn notify(operation: Operation, object: AnyObject) {
    trace!(id = %object.id(), object = ?object, "notify {operation:?}");
}

pub(crate) async fn notifier_get() -> Json<NotifiersResponse> {
    Json(NotifiersResponse { websocket: true })
}

pub(crate) async fn notifier_websocket_get(
    extract::State(state): extract::State<AppState>,
    User(user): User,
    ws: WebSocketUpgrade,
) -> Result<Response, AppError> {
    let client_id = user.client_id()?;

    let mut websockets = state.notifier.websockets.lock().await;
    if websockets.contains_key(&client_id) {
        return Err(AppError::Conflict(
            "websocket connection already open".to_owned(),
            None,
        ));
    }
    let (tx, mut rx) = mpsc::unbounded_channel(); // FIXME use bounded channel
    websockets.insert(client_id.clone(), tx);
    drop(websockets);

    Ok(ws.on_upgrade(|mut socket| async move {
        while let Some(msg) = rx.recv().await {
            if socket
                .send(Message::Text(serde_json::to_string(&msg).unwrap().into()))
                .await
                .is_err()
            {
                break;
            }
        }
        state.notifier.websockets.lock().await.remove(&client_id);
    }))
}
