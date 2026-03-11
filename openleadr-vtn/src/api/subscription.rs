use std::{collections::HashMap, sync::Arc};

use axum::{
    extract::{
        ws::{Message, WebSocketUpgrade},
        Path, State,
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
use tracing::{error, info, trace};
use uuid::{ContextV7, Uuid};
use validator::Validate;

use crate::{
    api::{AppResponse, ValidatedJson, ValidatedQuery},
    data_source::SubscriptionCrud,
    error::AppError,
    jwt::{Scope, User},
    state::AppState,
};

pub(crate) struct NotifierState {
    uuidv7_context: Arc<Mutex<ContextV7>>,
    websockets: Mutex<HashMap<ClientId, mpsc::UnboundedSender<Notification>>>,
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
                    client_name: None,
                    objects: None,
                    skip: 0,
                    limit: i64::MAX,
                },
                &None,
            )
            .await?;

        Ok(Self {
            uuidv7_context: Arc::new(Mutex::new(ContextV7::new())),
            websockets: Mutex::new(HashMap::new()),
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

    // FIXME update retrieve_all implementation when removing this
    if query_params.objects.as_deref().unwrap_or(&[]).len() > 1 {
        let error = "Tried to filter subscriptions by multiple object types. \
            This is not allowed in the current version of openLEADR as the specification \
            is not quite clear about if this should require all or any of the object types \
            to apply to subscriptions. If you have a use case for either option, please \
            open an issue on GitHub.";
        error!("{}", error);
        return Err(AppError::BadRequest(error));
    }

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
    #[validate(length(min = 1, max = 128))]
    pub(crate) client_name: Option<String>,
    #[validate(length(min = 0, max = 6))]
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

pub(crate) async fn notify(
    notifier_state: &NotifierState,
    operation: Operation,
    object: AnyObject,
) {
    let uuid = Uuid::new_v7(uuid::Timestamp::now(
        &*notifier_state.uuidv7_context.lock().await,
    ));

    trace!(id = %object.id(), object = ?object, "notify {operation:?}");

    for subscription in notifier_state.subscriptions.lock().await.values() {
        // FIXME handle object privacy

        for object_operation in &subscription.content.object_operations {
            if object_operation.operations.contains(&operation)
                // FIXME program_id
                && object_operation.objects.contains(&object.kind())
            {
                if let Some(tx) = notifier_state
                    .websockets
                    .lock()
                    .await
                    .get(&subscription.client_id)
                {
                    let _ = tx.send(Notification {
                        id: uuid
                            .to_string()
                            .parse()
                            .expect("uuid should always be a valid identifier"),
                        operation,
                        object: object.clone(),
                    });
                }
            }
        }
    }
}

pub(crate) async fn notifier_get() -> Json<NotifiersResponse> {
    Json(NotifiersResponse { websocket: true })
}

pub(crate) async fn notifier_websocket_get(
    State(notifier_state): State<Arc<NotifierState>>,
    User(user): User,
    ws: WebSocketUpgrade,
) -> Result<Response, AppError> {
    let client_id = user.client_id()?;

    let mut websockets = notifier_state.websockets.lock().await;
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
        notifier_state.websockets.lock().await.remove(&client_id);
    }))
}

#[cfg(test)]
mod test {
    use axum::body::Body;
    use openleadr_wire::{problem::Problem, subscription::Subscription};
    use reqwest::{Method, StatusCode};
    use sqlx::PgPool;

    use crate::{api::test::ApiTest, jwt::Scope};

    #[sqlx::test(fixtures("vens", "users"))]
    async fn empty_object_operations_not_allowed(db: PgPool) {
        let server = ApiTest::new(
            db,
            "ven-1-client-id",
            vec![Scope::WriteSubscriptions, Scope::ReadAll],
        )
        .await;

        let (status, _) = server
            .request::<Problem>(
                Method::POST,
                "/subscriptions",
                Body::from(r#"{"clientName": "ven-1-name", "objectOperations": []}"#),
            )
            .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }

    #[sqlx::test(fixtures("vens", "users"))]
    async fn get_many(db: PgPool) {
        let server = ApiTest::new(
            db,
            "ven-1-client-id",
            vec![Scope::WriteSubscriptions, Scope::ReadAll],
        )
        .await;

        let (status, _) = server
            .request::<Subscription>(
                Method::POST,
                "/subscriptions",
                Body::from(
                    r#"{
                        "clientName": "myClient",
                        "objectOperations": [{
                            "mechanism": "WEBSOCKET",
                            "operations": ["CREATE", "UPDATE"],
                            "objects": ["EVENT", "PROGRAM"]
                        }]
                    }"#,
                ),
            )
            .await;
        assert_eq!(status, StatusCode::CREATED);

        let (status, _) = server
            .request::<Subscription>(
                Method::POST,
                "/subscriptions",
                Body::from(
                    r#"{
                        "clientName": "myClient",
                        "programId": "PROGRAM-100",
                        "objectOperations": [{
                            "mechanism": "WEBSOCKET",
                            "operations": ["CREATE", "UPDATE"],
                            "objects": ["EVENT", "RESOURCE"]
                        }]
                    }"#,
                ),
            )
            .await;
        assert_eq!(status, StatusCode::CREATED);

        let (status, subscriptions) = server
            .request::<Vec<Subscription>>(Method::GET, "/subscriptions", Body::empty())
            .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(subscriptions.len(), 2);

        let (status, subscriptions) = server
            .request::<Vec<Subscription>>(
                Method::GET,
                "/subscriptions?objects=EVENT",
                Body::empty(),
            )
            .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(subscriptions.len(), 2);

        let (status, subscriptions) = server
            .request::<Vec<Subscription>>(
                Method::GET,
                "/subscriptions?objects=RESOURCE",
                Body::empty(),
            )
            .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(subscriptions.len(), 1);

        let (status, subscriptions) = server
            .request::<Vec<Subscription>>(
                Method::GET,
                "/subscriptions?objects=REPORT",
                Body::empty(),
            )
            .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(subscriptions.len(), 0);

        let (status, _) = server
            .request::<Problem>(Method::GET, "/subscriptions?objects=INVALID", Body::empty())
            .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);

        let (status, subscriptions) = server
            .request::<Vec<Subscription>>(
                Method::GET,
                "/subscriptions?programID=PROGRAM-100",
                Body::empty(),
            )
            .await;
        assert_eq!(status, StatusCode::OK);
        // subscriptions without program id filter also match
        assert_eq!(subscriptions.len(), 2);

        let (status, subscriptions) = server
            .request::<Vec<Subscription>>(
                Method::GET,
                "/subscriptions?programID=PROGRAM-999999",
                Body::empty(),
            )
            .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(subscriptions.len(), 2);
    }

    #[sqlx::test(fixtures("vens", "users"))]
    async fn get_many_multiple_object_types_not_allowed(db: PgPool) {
        let server = ApiTest::new(
            db,
            "ven-1-client-id",
            vec![Scope::WriteSubscriptions, Scope::ReadAll],
        )
        .await;

        let (status, _) = server
            .request::<Problem>(
                Method::GET,
                "/subscriptions?objects=PROGRAM&objects=RESOURCE",
                Body::empty(),
            )
            .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }

    // FIXME add edit and delete tests
}
