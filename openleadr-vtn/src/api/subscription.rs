use std::{collections::HashMap, convert::Infallible, sync::Arc, time::Duration};

use axum::{
    Json,
    extract::{Path, State},
    routing::MethodRouter,
};
#[cfg(feature = "experimental-websockets")]
use axum::{
    extract::ws::{Message, WebSocketUpgrade},
    response::Response,
};
use chrono::Utc;
use openleadr_wire::{
    ClientId, Identifier, ObjectType,
    program::ProgramId,
    subscription::{
        AnyObject, MqttNotifierAuthentication, MqttNotifierBindingObject, MqttPushNotification,
        Notification, NotifierOperationsTopics, NotifierTopicsResponse, NotifiersResponse,
        Operation, SerializationType, Subscription, SubscriptionId, SubscriptionRequest,
    },
};
use paho_mqtt::QoS;
use reqwest::StatusCode;
use serde::Deserialize;
use tokio::sync::{Mutex, mpsc};
use tracing::{error, info, trace};
use uuid::{ContextV7, Uuid};
use validator::Validate;

use crate::{
    api::{AppResponse, ValidatedJson, ValidatedQuery},
    data_source::{EventCrud, SubscriptionCrud, VenCrud, VenObjectPrivacy},
    error::AppError,
    jwt::{Claims, Scope, User},
    state::AppState,
};

struct MqttState {
    url: String,
    client: paho_mqtt::AsyncClient,
    topic_prefix: String,
}

pub(crate) struct NotifierState {
    uuidv7_context: Arc<Mutex<ContextV7>>,
    websockets: Mutex<HashMap<ClientId, (mpsc::UnboundedSender<Notification>, Claims)>>,
    subscriptions: Mutex<HashMap<SubscriptionId, Subscription>>,
    mqtt_state: Option<MqttState>,
}

impl NotifierState {
    pub(crate) async fn load_from_storage(
        storage: &dyn SubscriptionCrud,
        mqtt_url: String,
        mqtt_username: String,
        mqtt_password: String,
        mqtt_topic_prefix: String,
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

        let mqtt_client = paho_mqtt::AsyncClient::new(paho_mqtt::CreateOptions::new())?;
        mqtt_client
            .connect(
                paho_mqtt::ConnectOptionsBuilder::new()
                    .server_uris(&[&mqtt_url])
                    .user_name(mqtt_username)
                    .password(mqtt_password)
                    .automatic_reconnect(Duration::from_millis(1), Duration::from_secs(16))
                    .finalize(),
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
            mqtt_state: Some(MqttState {
                url: mqtt_url,
                client: mqtt_client,
                topic_prefix: mqtt_topic_prefix,
            }),
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

    let resources = if user.has_scope(Scope::ReadAll) {
        subscription_source
            .retrieve_all(&query_params, &None)
            .await?
    } else if user.has_scope(Scope::ReadVenObjects) {
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
    let subscription = if user.has_scope(Scope::ReadAll) {
        subscription_source.retrieve(&id, &None).await?
    } else if user.has_scope(Scope::ReadVenObjects) {
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

    let subscription = if user.has_scope(Scope::WriteSubscriptionsVen)
        || user.has_scope(Scope::WriteSubscriptionsBl)
    {
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
    let subscription = if user.has_scope(Scope::WriteSubscriptionsBl) {
        subscription_source.update(&id, update, &None).await?
    } else if user.has_scope(Scope::WriteSubscriptionsVen) {
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
    let subscription = if user.has_scope(Scope::WriteSubscriptionsBl) {
        subscription_source.delete(&id, &None).await?
    } else if user.has_scope(Scope::WriteSubscriptionsVen) {
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

async fn privacy_filter_object(
    object: &AnyObject,
    privacy: &dyn VenObjectPrivacy,
    client_id: &ClientId,
    claims: &Claims,
) -> Option<AnyObject> {
    if claims.has_scope(Scope::ReadAll) {
        return Some(object.clone());
    }

    match object {
        AnyObject::Program(program) => {
            if claims.has_scope(Scope::ReadTargets) {
                let targets = privacy.targets_by_client_id(client_id).await.ok()?;
                let filtered_targets: Vec<_> = program
                    .content
                    .targets
                    .iter()
                    .filter(|t| targets.contains(t))
                    .cloned()
                    .collect();
                if program.content.targets.is_empty() || !filtered_targets.is_empty() {
                    let mut filtered_program = program.clone();
                    filtered_program.content.targets = filtered_targets;
                    Some(AnyObject::Program(filtered_program))
                } else {
                    None
                }
            } else {
                None
            }
        }
        AnyObject::Report(report) => {
            if &report.client_id == client_id && claims.has_scope(Scope::ReadVenObjects) {
                Some(object.clone())
            } else {
                None
            }
        }
        AnyObject::Event(event) => {
            if claims.has_scope(Scope::ReadTargets) {
                let targets = privacy.targets_by_client_id(client_id).await.ok()?;
                let filtered_targets: Vec<_> = event
                    .content
                    .targets
                    .iter()
                    .filter(|t| targets.contains(t))
                    .cloned()
                    .collect();
                if event.content.targets.is_empty() || !filtered_targets.is_empty() {
                    let mut filtered_event = event.clone();
                    filtered_event.content.targets = filtered_targets;
                    Some(AnyObject::Event(filtered_event))
                } else {
                    None
                }
            } else {
                None
            }
        }
        AnyObject::Subscription(subscription) => {
            if &subscription.client_id == client_id && claims.has_scope(Scope::ReadVenObjects) {
                Some(object.clone())
            } else {
                None
            }
        }
        AnyObject::Ven(ven) => {
            if &ven.content.client_id == client_id && claims.has_scope(Scope::ReadVenObjects) {
                Some(object.clone())
            } else {
                None
            }
        }
        AnyObject::Resource(resource) => {
            if &resource.client_id == client_id && claims.has_scope(Scope::ReadVenObjects) {
                Some(object.clone())
            } else {
                None
            }
        }
        AnyObject::ResourceGroup(resourcegroup) => {
            if claims.has_scope(Scope::ReadVenObjects)
                && privacy
                    .resource_group_visible_for_client(client_id, &resourcegroup.id)
                    .await
                    .unwrap_or_default()
            {
                Some(object.clone())
            } else {
                None
            }
        }
    }
}

pub(crate) async fn notify(
    ven_source: &dyn VenCrud,
    event_source: &dyn EventCrud,
    privacy: &dyn VenObjectPrivacy,
    notifier_state: &NotifierState,
    operation: Operation,
    object: AnyObject,
) {
    let uuid: Identifier = Uuid::new_v7(uuid::Timestamp::now(
        &*notifier_state.uuidv7_context.lock().await,
    ))
    .to_string()
    .parse()
    .expect("uuid should always be a valid identifier");

    let event_program_id;
    let target_program_id = match &object {
        AnyObject::Program(program) => Some(&program.id),
        AnyObject::Report(report) => {
            if let Ok(event) = event_source.retrieve(&report.content.event_id, &None).await {
                event_program_id = event.content.program_id;
                Some(&event_program_id)
            } else {
                None
            }
        }
        AnyObject::Event(event) => Some(&event.content.program_id),
        AnyObject::Subscription(_)
        | AnyObject::Ven(_)
        | AnyObject::Resource(_)
        | AnyObject::ResourceGroup(_) => None,
    };

    trace!(id = %object.id(), object = ?object, "notify {operation:?}");

    notify_mqtt(
        ven_source,
        privacy,
        notifier_state,
        Notification {
            id: uuid.clone(),
            operation,
            object: object.clone(),
        },
    )
    .await;

    for subscription in notifier_state.subscriptions.lock().await.values() {
        let program_id = subscription.content.program_id.as_ref();

        for object_operation in &subscription.content.object_operations {
            if !object_operation.operations.contains(&operation)
                || !object_operation.objects.contains(&object.kind())
                || (program_id.is_some() && program_id != target_program_id)
            {
                continue;
            }

            if let Some((tx, claims)) = notifier_state
                .websockets
                .lock()
                .await
                .get(&subscription.client_id)
                && let Some(object) =
                    privacy_filter_object(&object, privacy, &subscription.client_id, claims).await
            {
                let _ = tx.send(Notification {
                    id: uuid.clone(),
                    operation,
                    object: object.clone(),
                });
            }
        }
    }
}

async fn publish_mqtt_push(
    mqtt_state: &MqttState,
    notification: Vec<u8>,
    push_notification: Vec<u8>,
    topic: &str,
) {
    mqtt_state
        .client
        .publish(paho_mqtt::Message::new(
            format!("{}{}", mqtt_state.topic_prefix, topic,),
            notification,
            QoS::AtMostOnce,
        ))
        .await
        .unwrap();
    mqtt_state
        .client
        .publish(paho_mqtt::Message::new(
            format!("{}push/{}", mqtt_state.topic_prefix, topic,),
            push_notification,
            QoS::AtMostOnce,
        ))
        .await
        .unwrap()
}

async fn publish_mqtt_push_by_targets(
    ven_source: &dyn VenCrud,
    privacy: &dyn VenObjectPrivacy,
    mqtt_state: &MqttState,
    notification: &Notification,
    push_notification: Vec<u8>,
    topic: &str,
) {
    if let Ok(vens) = ven_source
        .retrieve_all(
            &super::ven::QueryParams {
                ven_name: None,
                targets: crate::api::TargetQueryParams(None),
                skip: 0,
                limit: i64::MAX,
            },
            &None,
        )
        .await
    {
        for ven in vens {
            if let Some(object) = privacy_filter_object(
                &notification.object,
                privacy,
                &ven.content.client_id,
                &Claims::temporary_claims_for_mqtt_ven(&ven),
            )
            .await
            {
                publish_mqtt_push(
                    mqtt_state,
                    serde_json::to_vec(&Notification {
                        id: notification.id.clone(),
                        operation: notification.operation,
                        object,
                    })
                    .unwrap(),
                    push_notification.clone(),
                    &format!("vens/{}/{}", ven.id, topic),
                )
                .await;
            }
        }
    }
}

async fn notify_mqtt(
    ven_source: &dyn VenCrud,
    privacy: &dyn VenObjectPrivacy,
    notifier_state: &NotifierState,
    notification: Notification,
) {
    let notification_date_time = Utc::now();
    let operation_str = match notification.operation {
        Operation::Create => "create",
        Operation::Update => "update",
        Operation::Delete => "delete",
    };

    if let Some(mqtt_state) = &notifier_state.mqtt_state {
        let mqtt_notification = serde_json::to_vec(&notification).unwrap();
        let mqtt_push_notification = serde_json::to_vec(&MqttPushNotification {
            id: notification.object.id(),
            notification_id: notification.id.clone(),
            object_type: notification.object.kind(),
            operation: notification.operation,
            notification_date_time,
        })
        .unwrap();

        match notification.object {
            AnyObject::Ven(ven) => {
                publish_mqtt_push(
                    mqtt_state,
                    mqtt_notification.clone(),
                    mqtt_push_notification.clone(),
                    &format!("vens/{operation_str}"),
                )
                .await;
                if notification.operation != Operation::Create {
                    publish_mqtt_push(
                        mqtt_state,
                        mqtt_notification,
                        mqtt_push_notification,
                        &format!("vens/{}/{operation_str}", ven.id),
                    )
                    .await;
                }
            }
            AnyObject::Resource(resource) => {
                publish_mqtt_push(
                    mqtt_state,
                    mqtt_notification.clone(),
                    mqtt_push_notification.clone(),
                    &format!("resources/{operation_str}"),
                )
                .await;
                publish_mqtt_push(
                    mqtt_state,
                    mqtt_notification.clone(),
                    mqtt_push_notification.clone(),
                    &format!("vens/{}/resources/{operation_str}", resource.content.ven_id),
                )
                .await;
            }
            AnyObject::ResourceGroup(_) => {
                publish_mqtt_push(
                    mqtt_state,
                    mqtt_notification.clone(),
                    mqtt_push_notification.clone(),
                    &format!("resource_groups/{operation_str}"),
                )
                .await;
                publish_mqtt_push_by_targets(
                    ven_source,
                    privacy,
                    mqtt_state,
                    &notification,
                    mqtt_push_notification,
                    &format!("resource_groups/{operation_str}"),
                )
                .await;
            }
            AnyObject::Program(ref program) => {
                publish_mqtt_push(
                    mqtt_state,
                    mqtt_notification.clone(),
                    mqtt_push_notification.clone(),
                    &format!("programs/{}/{operation_str}", program.id),
                )
                .await;
                publish_mqtt_push(
                    mqtt_state,
                    mqtt_notification.clone(),
                    mqtt_push_notification.clone(),
                    &format!("programs/{operation_str}"),
                )
                .await;
                publish_mqtt_push_by_targets(
                    ven_source,
                    privacy,
                    mqtt_state,
                    &notification,
                    mqtt_push_notification,
                    &format!("programs/{operation_str}"),
                )
                .await;
            }
            AnyObject::Event(ref event) => {
                publish_mqtt_push(
                    mqtt_state,
                    mqtt_notification.clone(),
                    mqtt_push_notification.clone(),
                    &format!(
                        "events/program/{}/{operation_str}",
                        event.content.program_id
                    ),
                )
                .await;
                publish_mqtt_push(
                    mqtt_state,
                    mqtt_notification.clone(),
                    mqtt_push_notification.clone(),
                    &format!("events/{operation_str}"),
                )
                .await;
                publish_mqtt_push_by_targets(
                    ven_source,
                    privacy,
                    mqtt_state,
                    &notification,
                    mqtt_push_notification,
                    &format!("events/{operation_str}"),
                )
                .await;
            }
            AnyObject::Report(_) => {
                publish_mqtt_push(
                    mqtt_state,
                    mqtt_notification.clone(),
                    mqtt_push_notification.clone(),
                    &format!("reports/{operation_str}"),
                )
                .await;
            }
            AnyObject::Subscription(_) => {}
        }
    }
}

pub(crate) async fn notifier_get(
    State(notifier_state): State<Arc<NotifierState>>,
    User(_): User,
) -> Result<Json<NotifiersResponse>, AppError> {
    Ok(Json(NotifiersResponse {
        websocket: cfg!(feature = "experimental-websockets"),
        mqtt: notifier_state
            .mqtt_state
            .as_ref()
            .map(|mqtt_state| MqttNotifierBindingObject {
                uris: vec![mqtt_state.url.clone()],
                serialization: SerializationType::Json,
                authentication: MqttNotifierAuthentication::Oauth2BearerToken {
                    username: "{clientID}".to_owned(),
                },
            }),
        push_mqtt: notifier_state
            .mqtt_state
            .as_ref()
            .map(|mqtt_state| MqttNotifierBindingObject {
                uris: vec![mqtt_state.url.clone()],
                serialization: SerializationType::Json,
                authentication: MqttNotifierAuthentication::Oauth2BearerToken {
                    username: "{clientID}".to_owned(),
                },
            }),
    }))
}

#[cfg(feature = "experimental-websockets")]
pub(crate) async fn notifier_websocket_get(
    State(notifier_state): State<Arc<NotifierState>>,
    User(user): User,
    ws: WebSocketUpgrade,
) -> Result<Response, AppError> {
    let client_id = user.client_id()?;

    let mut websockets = notifier_state.websockets.lock().await;
    if websockets.contains_key(&client_id) {
        // FIXME close existing connection instead
        return Err(AppError::Conflict(
            "websocket connection already open".to_owned(),
            None,
        ));
    }
    let (tx, mut rx) = mpsc::unbounded_channel(); // FIXME use bounded channel
    websockets.insert(client_id.clone(), (tx, user));
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

pub(crate) fn mqtt_notifier() -> axum::Router<AppState> {
    axum::Router::new()
        // Public routes
        .route("/topics/programs", mqtt_route_public("programs/", true))
        .route("/topics/events", mqtt_route_public("events/", true))
        //
        // BL-only routes
        .route("/topics/reports", mqtt_route_bl("reports/", true))
        //.route("/topics/subscriptions", mqtt_route_bl("subscriptions/", true))
        .route("/topics/vens", mqtt_route_bl("vens/", true))
        .route("/topics/resources", mqtt_route_bl("resources/", true))
        .route(
            "/topics/resource_groups",
            mqtt_route_bl("resource_groups/", true),
        )
        .route(
            "/topics/programs/{program_id}",
            mqtt_route_bl_by_program_id("programs/", false),
        )
        .route(
            "/topics/programs/{program_id}/events",
            mqtt_route_bl_by_program_id("events/program/", true),
        )
        //
        // per-VEN routes
        .route("/topics/vens/{ven_id}", mqtt_route_by_ven_id("", false))
        .route(
            "/topics/vens/{ven_id}/events",
            mqtt_route_by_ven_id("events/", true),
        )
        .route(
            "/topics/vens/{ven_id}/programs",
            mqtt_route_by_ven_id("programs/", true),
        )
        .route(
            "/topics/vens/{ven_id}/resources",
            mqtt_route_by_ven_id("resources/", true),
        )
        .route(
            "/topics/vens/{ven_id}/resource_groups",
            mqtt_route_by_ven_id("resource_groups/", true),
        )
}

pub(crate) fn push_mqtt_notifier() -> axum::Router<AppState> {
    axum::Router::new()
        // Public routes
        .route(
            "/topics/programs",
            mqtt_route_public("push/programs/", true),
        )
        .route("/topics/events", mqtt_route_public("push/events/", true))
        //
        // BL-only routes
        .route("/topics/reports", mqtt_route_bl("push/reports/", true))
        //.route("/topics/subscriptions", mqtt_route_bl("push/subscriptions/", true))
        .route("/topics/vens", mqtt_route_bl("push/vens/", true))
        .route("/topics/resources", mqtt_route_bl("push/resources/", true))
        .route(
            "/topics/resource_groups",
            mqtt_route_bl("push/resource_groups/", true),
        )
        .route(
            "/topics/programs/{program_id}",
            mqtt_route_bl_by_program_id("push/programs/", false),
        )
        .route(
            "/topics/programs/{program_id}/events",
            mqtt_route_bl_by_program_id("push/events/program/", true),
        )
        //
        // per-VEN routes
        .route(
            "/topics/vens/{ven_id}",
            mqtt_route_by_ven_id("push/", false),
        )
        .route(
            "/topics/vens/{ven_id}/events",
            mqtt_route_by_ven_id("push/events/", true),
        )
        .route(
            "/topics/vens/{ven_id}/programs",
            mqtt_route_by_ven_id("push/programs/", true),
        )
        .route(
            "/topics/vens/{ven_id}/resources",
            mqtt_route_by_ven_id("push/resources/", true),
        )
        .route(
            "/topics/vens/{ven_id}/resource_groups",
            mqtt_route_by_ven_id("push/resource_groups/", true),
        )
}

fn mqtt_route_public(
    base_topic: &'static str,
    subscribe_create: bool,
) -> MethodRouter<AppState, Infallible> {
    axum::routing::get(
        move |State(notifier_state): State<Arc<NotifierState>>| async move {
            match &notifier_state.mqtt_state {
                Some(mqtt_state) => {
                    let prefix = &mqtt_state.topic_prefix;
                    Ok(Json(NotifierTopicsResponse {
                        topics: NotifierOperationsTopics {
                            create: subscribe_create
                                .then_some(format!("{prefix}{base_topic}create")),
                            update: format!("{prefix}{base_topic}update"),
                            delete: format!("{prefix}{base_topic}delete"),
                            all: Some(format!("{prefix}{base_topic}+")),
                        },
                    }))
                }
                None => Err(AppError::NotFound),
            }
        },
    )
}

fn mqtt_route_bl(
    base_topic: &'static str,
    subscribe_create: bool,
) -> MethodRouter<AppState, Infallible> {
    axum::routing::get(
        move |State(notifier_state): State<Arc<NotifierState>>| async move {
            // We deliberately do not do access control here since these topic
            // paths are completely predictable anyway. Therefore, doing access
            // control can only create the false impression that hiding the
            // names of these topics provides a level of security which is not
            // real.
            match &notifier_state.mqtt_state {
                Some(mqtt_state) => {
                    let prefix = &mqtt_state.topic_prefix;
                    Ok(Json(NotifierTopicsResponse {
                        topics: NotifierOperationsTopics {
                            create: subscribe_create
                                .then_some(format!("{prefix}{base_topic}create")),
                            update: format!("{prefix}{base_topic}update"),
                            delete: format!("{prefix}{base_topic}delete"),
                            all: Some(format!("{prefix}{base_topic}+")),
                        },
                    }))
                }
                None => Err(AppError::NotFound),
            }
        },
    )
}

fn mqtt_route_bl_by_program_id(
    base_topic: &'static str,
    subscribe_create: bool,
) -> MethodRouter<AppState, Infallible> {
    axum::routing::get(
        move |State(notifier_state): State<Arc<NotifierState>>, Path(program_id): Path<String>| async move {
            // We deliberately do not do access control here since these topic
            // paths are completely predictable anyway. Therefore, doing access
            // control can only create the false impression that hiding the
            // names of these topics provides a level of security which is not
            // real.
            match &notifier_state.mqtt_state {
                Some(mqtt_state) => {
                    let prefix = &mqtt_state.topic_prefix;
                    Ok(Json(NotifierTopicsResponse {
                        topics: NotifierOperationsTopics {
                            create: subscribe_create
                                .then_some(format!("{prefix}{base_topic}{program_id}/create")),
                            update: format!("{prefix}{base_topic}{program_id}/update"),
                            delete: format!("{prefix}{base_topic}{program_id}/delete"),
                            all: Some(format!("{prefix}{base_topic}{program_id}/+")),
                        },
                    }))
                }
                None => Err(AppError::NotFound),
            }
        },
    )
}

fn mqtt_route_by_ven_id(
    base_topic: &'static str,
    subscribe_create: bool,
) -> MethodRouter<AppState, Infallible> {
    axum::routing::get(
        move |State(notifier_state): State<Arc<NotifierState>>, Path(ven_id): Path<String>| async move {
            // We deliberately do not do access control here since these topic
            // paths are completely predictable anyway. Therefore, doing access
            // control can only create the false impression that hiding the
            // names of these topics provides a level of security which is not
            // real.
            match &notifier_state.mqtt_state {
                Some(mqtt_state) => {
                    let prefix = &mqtt_state.topic_prefix;
                    Ok(Json(NotifierTopicsResponse {
                        topics: NotifierOperationsTopics {
                            create: subscribe_create
                                .then_some(format!("{prefix}{base_topic}vens/{ven_id}/create")),
                            update: format!("{prefix}{base_topic}vens/{ven_id}/update"),
                            delete: format!("{prefix}{base_topic}vens/{ven_id}/delete"),
                            all: Some(format!("{prefix}{base_topic}vens/{ven_id}/+")),
                        },
                    }))
                }
                None => Err(AppError::NotFound),
            }
        },
    )
}

#[cfg(test)]
mod test {
    use std::{
        collections::{BTreeSet, HashMap},
        sync::Arc,
        time::Duration,
    };

    use async_trait::async_trait;
    use axum::body::Body;
    use chrono::DateTime;
    use openleadr_wire::{
        ClientId, Event, ObjectType, Program, Report, Ven,
        event::{EventId, EventRequest, Priority},
        problem::Problem,
        program::ProgramRequest,
        report::ReportRequest,
        resource::{BlResourceRequest, Resource, ResourceRequest},
        resource_group::ResourceGroupId,
        subscription::{
            AnyObject, MqttPushNotification, Notification, NotificationMechanism, Operation,
            Subscription, SubscriptionObjectOperation, SubscriptionRequest,
        },
        target::Target,
        ven::{BlVenRequest, VenId, VenRequest},
    };
    use paho_mqtt::QoS;
    use reqwest::{Method, StatusCode};
    use sqlx::PgPool;
    use tokio::sync::{Mutex, mpsc::unbounded_channel};
    use uuid::ContextV7;

    use crate::{
        api::{
            self,
            subscription::{NotifierState, notify, privacy_filter_object},
            test::ApiTest,
        },
        data_source::{Crud, EventCrud, VenCrud, VenObjectPrivacy},
        error::AppError,
        jwt::{Claims, Scope},
    };

    struct TestVenObjectPrivacyTargets;

    #[async_trait]
    impl VenObjectPrivacy for TestVenObjectPrivacyTargets {
        async fn targets_by_client_id(
            &self,
            client_id: &ClientId,
        ) -> Result<Vec<Target>, AppError> {
            assert_eq!(client_id, &"test_client_id".parse::<ClientId>().unwrap());
            Ok(vec!["test_target_1".parse().unwrap()])
        }

        async fn resource_group_visible_for_client(
            &self,
            _client_id: &ClientId,
            _resource_group_id: &ResourceGroupId,
        ) -> Result<bool, AppError> {
            unimplemented!()
        }

        async fn ven_id_by_client_id(
            &self,
            _client_id: &ClientId,
        ) -> Result<Option<VenId>, AppError> {
            unimplemented!()
        }
    }

    struct TestVenObjectPrivacyNoCallExpected;

    #[async_trait]
    impl VenObjectPrivacy for TestVenObjectPrivacyNoCallExpected {
        async fn targets_by_client_id(
            &self,
            _client_id: &ClientId,
        ) -> Result<Vec<Target>, AppError> {
            unimplemented!()
        }

        async fn resource_group_visible_for_client(
            &self,
            _client_id: &ClientId,
            _resource_group_id: &ResourceGroupId,
        ) -> Result<bool, AppError> {
            unimplemented!()
        }

        async fn ven_id_by_client_id(
            &self,
            _client_id: &ClientId,
        ) -> Result<Option<VenId>, AppError> {
            unimplemented!()
        }
    }

    struct TestEventCrud;

    #[async_trait]
    impl Crud for TestEventCrud {
        type Type = Event;
        type Id = EventId;
        type NewType = EventRequest;
        type Error = AppError;
        type Filter = api::event::QueryParams;
        type PermissionFilter = Option<ClientId>;

        async fn create(
            &self,
            _new: Self::NewType,
            _permission_filter: &Self::PermissionFilter,
        ) -> Result<Self::Type, Self::Error> {
            unimplemented!()
        }
        async fn retrieve(
            &self,
            _id: &Self::Id,
            _permission_filter: &Self::PermissionFilter,
        ) -> Result<Self::Type, Self::Error> {
            unimplemented!()
        }
        async fn retrieve_all(
            &self,
            _filter: &Self::Filter,
            _permission_filter: &Self::PermissionFilter,
        ) -> Result<Vec<Self::Type>, Self::Error> {
            unimplemented!()
        }
        async fn update(
            &self,
            _id: &Self::Id,
            _new: Self::NewType,
            _permission_filter: &Self::PermissionFilter,
        ) -> Result<Self::Type, Self::Error> {
            unimplemented!()
        }
        async fn delete(
            &self,
            _id: &Self::Id,
            _permission_filter: &Self::PermissionFilter,
        ) -> Result<Self::Type, Self::Error> {
            unimplemented!()
        }
    }

    impl EventCrud for TestEventCrud {}

    struct TestVenCrud;

    #[async_trait]
    impl Crud for TestVenCrud {
        type Type = Ven;
        type Id = VenId;
        type NewType = BlVenRequest;
        type Error = AppError;
        type Filter = api::ven::QueryParams;
        type PermissionFilter = Option<ClientId>;

        async fn create(
            &self,
            _new: Self::NewType,
            _permission_filter: &Self::PermissionFilter,
        ) -> Result<Self::Type, Self::Error> {
            unimplemented!()
        }
        async fn retrieve(
            &self,
            _id: &Self::Id,
            _permission_filter: &Self::PermissionFilter,
        ) -> Result<Self::Type, Self::Error> {
            unimplemented!()
        }
        async fn retrieve_all(
            &self,
            _filter: &Self::Filter,
            _permission_filter: &Self::PermissionFilter,
        ) -> Result<Vec<Self::Type>, Self::Error> {
            unimplemented!()
        }
        async fn update(
            &self,
            _id: &Self::Id,
            _new: Self::NewType,
            _permission_filter: &Self::PermissionFilter,
        ) -> Result<Self::Type, Self::Error> {
            unimplemented!()
        }
        async fn delete(
            &self,
            _id: &Self::Id,
            _permission_filter: &Self::PermissionFilter,
        ) -> Result<Self::Type, Self::Error> {
            unimplemented!()
        }
    }

    impl VenCrud for TestVenCrud {}

    #[tokio::test]
    async fn subscription_filtering() {
        let (test_client_a_tx, mut test_client_a_rx) = unbounded_channel();
        let (test_client_b_tx, mut test_client_b_rx) = unbounded_channel();
        let (test_client_c_tx, mut test_client_c_rx) = unbounded_channel();
        let websockets = HashMap::from([
            (
                "test_client_a".parse().unwrap(),
                (test_client_a_tx, Claims::from_scopes(vec![Scope::ReadAll])),
            ),
            (
                "test_client_id".parse().unwrap(),
                (
                    test_client_b_tx,
                    Claims::from_scopes(vec![Scope::ReadVenObjects, Scope::ReadTargets]),
                ),
            ),
            (
                "test_client_c".parse().unwrap(),
                (test_client_c_tx, Claims::from_scopes(vec![Scope::ReadAll])),
            ),
        ]);

        let subscription_1 = Subscription {
            id: "subscription_1".parse().unwrap(),
            created_date_time: DateTime::from_timestamp_nanos(15_000_000),
            modification_date_time: DateTime::from_timestamp_nanos(15_000_000),
            client_id: "test_client_a".parse().unwrap(),
            content: SubscriptionRequest {
                client_name: "subscription 1".into(),
                program_id: None,
                object_operations: vec![SubscriptionObjectOperation {
                    objects: vec![ObjectType::Event],
                    operations: vec![Operation::Create],
                    mechanism: NotificationMechanism::Websocket,
                    callback_url: None,
                    bearer_token: None,
                }],
            },
        };

        let subscription_2 = Subscription {
            id: "subscription_2".parse().unwrap(),
            created_date_time: DateTime::from_timestamp_nanos(15_000_000),
            modification_date_time: DateTime::from_timestamp_nanos(15_000_000),
            client_id: "test_client_id".parse().unwrap(),
            content: SubscriptionRequest {
                client_name: "subscription 2".into(),
                program_id: None,
                object_operations: vec![SubscriptionObjectOperation {
                    objects: vec![ObjectType::Event],
                    operations: vec![Operation::Create, Operation::Delete],
                    mechanism: NotificationMechanism::Websocket,
                    callback_url: None,
                    bearer_token: None,
                }],
            },
        };

        let subscription_3 = Subscription {
            id: "subscription_3".parse().unwrap(),
            created_date_time: DateTime::from_timestamp_nanos(15_000_000),
            modification_date_time: DateTime::from_timestamp_nanos(15_000_000),
            client_id: "test_client_c".parse().unwrap(),
            content: SubscriptionRequest {
                client_name: "subscription 3".into(),
                program_id: Some("program_2".parse().unwrap()),
                object_operations: vec![SubscriptionObjectOperation {
                    objects: vec![ObjectType::Event],
                    operations: vec![Operation::Create],
                    mechanism: NotificationMechanism::Websocket,
                    callback_url: None,
                    bearer_token: None,
                }],
            },
        };

        let subscriptions = HashMap::from([
            ("subscription_1".parse().unwrap(), subscription_1),
            ("subscription_2".parse().unwrap(), subscription_2),
            ("subscription_3".parse().unwrap(), subscription_3),
        ]);

        let state = NotifierState {
            uuidv7_context: Arc::new(Mutex::new(ContextV7::new())),
            websockets: Mutex::new(websockets),
            subscriptions: Mutex::new(subscriptions),
            mqtt_state: None,
        };

        notify(
            &TestVenCrud,
            &TestEventCrud,
            &TestVenObjectPrivacyTargets,
            &state,
            Operation::Create,
            AnyObject::Event(Event {
                id: "test_event_1".parse().unwrap(),
                created_date_time: DateTime::from_timestamp_nanos(15_000_000),
                modification_date_time: DateTime::from_timestamp_nanos(15_000_000),
                content: EventRequest {
                    program_id: "program_1".parse().unwrap(),
                    event_name: None,
                    duration: None,
                    priority: Priority::MIN,
                    targets: vec!["test_target_1".parse().unwrap()],
                    report_descriptors: None,
                    payload_descriptors: None,
                    interval_period: None,
                    intervals: None,
                },
            }),
        )
        .await;

        assert!(test_client_a_rx.try_recv().is_ok());
        assert!(test_client_a_rx.try_recv().is_err());
        assert!(test_client_b_rx.try_recv().is_ok());
        assert!(test_client_b_rx.try_recv().is_err());
        assert!(test_client_c_rx.try_recv().is_err());

        notify(
            &TestVenCrud,
            &TestEventCrud,
            &TestVenObjectPrivacyTargets,
            &state,
            Operation::Create,
            AnyObject::Event(Event {
                id: "test_event_2".parse().unwrap(),
                created_date_time: DateTime::from_timestamp_nanos(15_000_000),
                modification_date_time: DateTime::from_timestamp_nanos(15_000_000),
                content: EventRequest {
                    program_id: "program_2".parse().unwrap(),
                    event_name: None,
                    duration: None,
                    priority: Priority::MIN,
                    targets: vec!["test_target_2".parse().unwrap()],
                    report_descriptors: None,
                    payload_descriptors: None,
                    interval_period: None,
                    intervals: None,
                },
            }),
        )
        .await;

        assert!(test_client_a_rx.try_recv().is_ok());
        assert!(test_client_a_rx.try_recv().is_err());
        assert!(test_client_b_rx.try_recv().is_err());
        assert!(test_client_c_rx.try_recv().is_ok());
        assert!(test_client_c_rx.try_recv().is_err());

        notify(
            &TestVenCrud,
            &TestEventCrud,
            &TestVenObjectPrivacyTargets,
            &state,
            Operation::Delete,
            AnyObject::Event(Event {
                id: "test_event_1".parse().unwrap(),
                created_date_time: DateTime::from_timestamp_nanos(15_000_000),
                modification_date_time: DateTime::from_timestamp_nanos(15_000_000),
                content: EventRequest {
                    program_id: "program_1".parse().unwrap(),
                    event_name: None,
                    duration: None,
                    priority: Priority::MIN,
                    targets: vec!["test_target_1".parse().unwrap()],
                    report_descriptors: None,
                    payload_descriptors: None,
                    interval_period: None,
                    intervals: None,
                },
            }),
        )
        .await;

        assert!(test_client_a_rx.try_recv().is_err());
        assert!(test_client_b_rx.try_recv().is_ok());
        assert!(test_client_b_rx.try_recv().is_err());
        assert!(test_client_c_rx.try_recv().is_err());
    }

    #[cfg(feature = "experimental-websockets")]
    #[sqlx::test(fixtures("vens", "programs", "events"))]
    async fn websocket_end_to_end(db: PgPool) {
        use futures::StreamExt;
        use tokio_tungstenite::{
            connect_async,
            tungstenite::{ClientRequestBuilder, Message},
        };

        let server = ApiTest::new(
            db,
            "ven-1-client-id",
            vec![
                Scope::WriteReports,
                Scope::WriteSubscriptionsBl,
                Scope::ReadAll,
            ],
        )
        .await;

        let (status, _) = server
            .request::<Subscription>(
                Method::POST,
                "/subscriptions",
                Body::from(r#"{"clientName": "ven-1-name", "programID": "program-1", "objectOperations": [{"objects": ["REPORT"], "operations": ["CREATE"], "mechanism": "WEBHOOK"}]}"#),
            )
            .await;
        assert_eq!(status, StatusCode::CREATED);

        let (token, addr, handle) = server.run().await;
        let request = ClientRequestBuilder::new(
            format!("ws://localhost:{}/notifiers/ws", addr.port())
                .parse()
                .unwrap(),
        )
        .with_header("Authorization", format!("Bearer {token}"));
        let (mut client_socket, _) = connect_async(request).await.unwrap();

        let (status, _) = server
            .request::<Report>(
                Method::POST,
                "/reports",
                Body::from(
                    r#"{"eventID": "event-1", "clientName": "report-1-name", "resources": []}"#,
                ),
            )
            .await;
        assert_eq!(status, StatusCode::CREATED);

        let Message::Text(notification) = client_socket.next().await.unwrap().unwrap() else {
            panic!("Unexpected message type");
        };

        let notification: Notification = serde_json::from_str(&notification).unwrap();

        assert_eq!(notification.operation, Operation::Create);

        client_socket.close(None).await.ok();

        handle.abort();
    }

    #[tokio::test]
    async fn privacy_filter_object_filters_events() {
        let object = AnyObject::Event(Event {
            id: "event_id".parse().unwrap(),
            created_date_time: DateTime::from_timestamp_nanos(15_000_000),
            modification_date_time: DateTime::from_timestamp_nanos(15_000_000),
            content: EventRequest {
                program_id: "program_id".parse().unwrap(),
                event_name: None,
                duration: None,
                priority: Priority::MIN,
                targets: vec![
                    "test_target_1".parse().unwrap(),
                    "test_target_2".parse().unwrap(),
                ],
                report_descriptors: None,
                payload_descriptors: None,
                interval_period: None,
                intervals: None,
            },
        });
        let no_scopes_result = privacy_filter_object(
            &object,
            &TestVenObjectPrivacyTargets,
            &"other_test_client_id".parse().unwrap(),
            &Claims::from_scopes(vec![]),
        )
        .await;
        assert!(no_scopes_result.is_none());
        let ven_scopes_result = privacy_filter_object(
            &object,
            &TestVenObjectPrivacyTargets,
            &"test_client_id".parse().unwrap(),
            &Claims::from_scopes(vec![Scope::ReadTargets]),
        )
        .await;
        let Some(AnyObject::Event(ven_scopes_event)) = ven_scopes_result else {
            panic!("Unexpected result from filter.");
        };
        assert_eq!(
            ven_scopes_event.content.targets,
            vec!["test_target_1".parse().unwrap()]
        );
        let bl_scopes_result = privacy_filter_object(
            &object,
            &TestVenObjectPrivacyTargets,
            &"other_test_client_id".parse().unwrap(),
            &Claims::from_scopes(vec![Scope::ReadAll]),
        )
        .await;
        let Some(AnyObject::Event(bl_scopes_event)) = bl_scopes_result else {
            panic!("Unexpected result from filter.");
        };
        assert_eq!(
            bl_scopes_event.content.targets,
            vec![
                "test_target_1".parse().unwrap(),
                "test_target_2".parse().unwrap()
            ]
        );

        let object = AnyObject::Event(Event {
            id: "event_id".parse().unwrap(),
            created_date_time: DateTime::from_timestamp_nanos(15_000_000),
            modification_date_time: DateTime::from_timestamp_nanos(15_000_000),
            content: EventRequest {
                program_id: "program_id".parse().unwrap(),
                event_name: None,
                duration: None,
                priority: Priority::MIN,
                targets: vec!["test_target_2".parse().unwrap()],
                report_descriptors: None,
                payload_descriptors: None,
                interval_period: None,
                intervals: None,
            },
        });
        let ven_scopes_result = privacy_filter_object(
            &object,
            &TestVenObjectPrivacyTargets,
            &"test_client_id".parse().unwrap(),
            &Claims::from_scopes(vec![Scope::ReadTargets]),
        )
        .await;
        assert!(ven_scopes_result.is_none());
        let bl_scopes_result = privacy_filter_object(
            &object,
            &TestVenObjectPrivacyTargets,
            &"other_test_client_id".parse().unwrap(),
            &Claims::from_scopes(vec![Scope::ReadAll]),
        )
        .await;
        let Some(AnyObject::Event(bl_scopes_event)) = bl_scopes_result else {
            panic!("Unexpected result from filter.");
        };
        assert_eq!(
            bl_scopes_event.content.targets,
            vec!["test_target_2".parse().unwrap()]
        );

        let object = AnyObject::Event(Event {
            id: "event_id".parse().unwrap(),
            created_date_time: DateTime::from_timestamp_nanos(15_000_000),
            modification_date_time: DateTime::from_timestamp_nanos(15_000_000),
            content: EventRequest {
                program_id: "program_id".parse().unwrap(),
                event_name: None,
                duration: None,
                priority: Priority::MIN,
                targets: vec![],
                report_descriptors: None,
                payload_descriptors: None,
                interval_period: None,
                intervals: None,
            },
        });
        let ven_scopes_result = privacy_filter_object(
            &object,
            &TestVenObjectPrivacyTargets,
            &"test_client_id".parse().unwrap(),
            &Claims::from_scopes(vec![Scope::ReadTargets]),
        )
        .await;
        let Some(AnyObject::Event(ven_scopes_event)) = ven_scopes_result else {
            panic!("Unexpected result from filter.");
        };
        assert_eq!(ven_scopes_event.content.targets, vec![]);
        let bl_scopes_result = privacy_filter_object(
            &object,
            &TestVenObjectPrivacyTargets,
            &"other_test_client_id".parse().unwrap(),
            &Claims::from_scopes(vec![Scope::ReadAll]),
        )
        .await;
        let Some(AnyObject::Event(bl_scopes_event)) = bl_scopes_result else {
            panic!("Unexpected result from filter.");
        };
        assert_eq!(bl_scopes_event.content.targets, vec![]);
    }

    #[tokio::test]
    async fn privacy_filter_object_filters_programs() {
        let object = AnyObject::Program(Program {
            id: "program_id".parse().unwrap(),
            created_date_time: DateTime::from_timestamp_nanos(15_000_000),
            modification_date_time: DateTime::from_timestamp_nanos(15_000_000),
            content: ProgramRequest {
                program_name: "Test Program".into(),
                interval_period: None,
                program_descriptions: None,
                payload_descriptors: None,
                attributes: None,
                targets: vec![
                    "test_target_1".parse().unwrap(),
                    "test_target_2".parse().unwrap(),
                ],
            },
        });
        let no_scopes_result = privacy_filter_object(
            &object,
            &TestVenObjectPrivacyTargets,
            &"other_test_client_id".parse().unwrap(),
            &Claims::from_scopes(vec![]),
        )
        .await;
        assert!(no_scopes_result.is_none());
        let ven_scopes_result = privacy_filter_object(
            &object,
            &TestVenObjectPrivacyTargets,
            &"test_client_id".parse().unwrap(),
            &Claims::from_scopes(vec![Scope::ReadTargets]),
        )
        .await;
        let Some(AnyObject::Program(ven_scopes_program)) = ven_scopes_result else {
            panic!("Unexpected result from filter.");
        };
        assert_eq!(
            ven_scopes_program.content.targets,
            vec!["test_target_1".parse().unwrap()]
        );
        let bl_scopes_result = privacy_filter_object(
            &object,
            &TestVenObjectPrivacyTargets,
            &"other_test_client_id".parse().unwrap(),
            &Claims::from_scopes(vec![Scope::ReadAll]),
        )
        .await;
        let Some(AnyObject::Program(bl_scopes_program)) = bl_scopes_result else {
            panic!("Unexpected result from filter.");
        };
        assert_eq!(
            bl_scopes_program.content.targets,
            vec![
                "test_target_1".parse().unwrap(),
                "test_target_2".parse().unwrap()
            ]
        );

        let object = AnyObject::Program(Program {
            id: "program_id".parse().unwrap(),
            created_date_time: DateTime::from_timestamp_nanos(15_000_000),
            modification_date_time: DateTime::from_timestamp_nanos(15_000_000),
            content: ProgramRequest {
                program_name: "Test Program".into(),
                interval_period: None,
                program_descriptions: None,
                payload_descriptors: None,
                attributes: None,
                targets: vec!["test_target_2".parse().unwrap()],
            },
        });
        let ven_scopes_result = privacy_filter_object(
            &object,
            &TestVenObjectPrivacyTargets,
            &"test_client_id".parse().unwrap(),
            &Claims::from_scopes(vec![Scope::ReadTargets]),
        )
        .await;
        assert!(ven_scopes_result.is_none());
        let bl_scopes_result = privacy_filter_object(
            &object,
            &TestVenObjectPrivacyTargets,
            &"other_test_client_id".parse().unwrap(),
            &Claims::from_scopes(vec![Scope::ReadAll]),
        )
        .await;
        let Some(AnyObject::Program(bl_scopes_program)) = bl_scopes_result else {
            panic!("Unexpected result from filter.");
        };
        assert_eq!(
            bl_scopes_program.content.targets,
            vec!["test_target_2".parse().unwrap()]
        );

        let object = AnyObject::Program(Program {
            id: "program_id".parse().unwrap(),
            created_date_time: DateTime::from_timestamp_nanos(15_000_000),
            modification_date_time: DateTime::from_timestamp_nanos(15_000_000),
            content: ProgramRequest {
                program_name: "Test Program".into(),
                interval_period: None,
                program_descriptions: None,
                payload_descriptors: None,
                attributes: None,
                targets: vec![],
            },
        });
        let ven_scopes_result = privacy_filter_object(
            &object,
            &TestVenObjectPrivacyTargets,
            &"test_client_id".parse().unwrap(),
            &Claims::from_scopes(vec![Scope::ReadTargets]),
        )
        .await;
        let Some(AnyObject::Program(ven_scopes_program)) = ven_scopes_result else {
            panic!("Unexpected result from filter.");
        };
        assert_eq!(ven_scopes_program.content.targets, vec![]);
        let bl_scopes_result = privacy_filter_object(
            &object,
            &TestVenObjectPrivacyTargets,
            &"other_test_client_id".parse().unwrap(),
            &Claims::from_scopes(vec![Scope::ReadAll]),
        )
        .await;
        let Some(AnyObject::Program(bl_scopes_program)) = bl_scopes_result else {
            panic!("Unexpected result from filter.");
        };
        assert_eq!(bl_scopes_program.content.targets, vec![]);
    }

    #[tokio::test]
    async fn privacy_filter_object_filters_report() {
        let object = AnyObject::Report(Report {
            id: "report_id".parse().unwrap(),
            created_date_time: DateTime::from_timestamp_nanos(15_000_000),
            modification_date_time: DateTime::from_timestamp_nanos(15_000_000),
            content: ReportRequest {
                event_id: "event_id".parse().unwrap(),
                client_name: "client_reporting_name".into(),
                report_name: None,
                payload_descriptors: None,
                resources: vec![],
            },
            client_id: "test_client_id".parse().unwrap(),
        });
        let no_scope_result = privacy_filter_object(
            &object,
            &TestVenObjectPrivacyNoCallExpected,
            &"test_client_id".parse().unwrap(),
            &Claims::from_scopes(vec![]),
        )
        .await;
        assert!(no_scope_result.is_none());
        let ven_scope_wrong_client_result = privacy_filter_object(
            &object,
            &TestVenObjectPrivacyNoCallExpected,
            &"other_test_client_id".parse().unwrap(),
            &Claims::from_scopes(vec![Scope::ReadVenObjects]),
        )
        .await;
        assert!(ven_scope_wrong_client_result.is_none());
        let ven_scope_result = privacy_filter_object(
            &object,
            &TestVenObjectPrivacyNoCallExpected,
            &"test_client_id".parse().unwrap(),
            &Claims::from_scopes(vec![Scope::ReadVenObjects]),
        )
        .await;
        assert!(ven_scope_result.is_some());
        let bl_scope_result = privacy_filter_object(
            &object,
            &TestVenObjectPrivacyNoCallExpected,
            &"test_client_id".parse().unwrap(),
            &Claims::from_scopes(vec![Scope::ReadAll]),
        )
        .await;
        assert!(bl_scope_result.is_some());
    }

    #[tokio::test]
    async fn privacy_filter_object_filters_resource() {
        let object = AnyObject::Resource(Resource {
            id: "report_id".parse().unwrap(),
            created_date_time: DateTime::from_timestamp_nanos(15_000_000),
            modification_date_time: DateTime::from_timestamp_nanos(15_000_000),
            client_id: "test_client_id".parse().unwrap(),
            content: BlResourceRequest {
                targets: vec!["test_target_2".parse().unwrap()],
                resource_name: "resource_name".into(),
                ven_id: "ven_id".parse().unwrap(),
                attributes: None,
            },
        });
        let no_scope_result = privacy_filter_object(
            &object,
            &TestVenObjectPrivacyNoCallExpected,
            &"test_client_id".parse().unwrap(),
            &Claims::from_scopes(vec![]),
        )
        .await;
        assert!(no_scope_result.is_none());
        let ven_scope_wrong_client_result = privacy_filter_object(
            &object,
            &TestVenObjectPrivacyNoCallExpected,
            &"other_test_client_id".parse().unwrap(),
            &Claims::from_scopes(vec![Scope::ReadVenObjects]),
        )
        .await;
        assert!(ven_scope_wrong_client_result.is_none());
        let ven_scope_result = privacy_filter_object(
            &object,
            &TestVenObjectPrivacyNoCallExpected,
            &"test_client_id".parse().unwrap(),
            &Claims::from_scopes(vec![Scope::ReadVenObjects]),
        )
        .await;
        let Some(AnyObject::Resource(ven_scope_resource)) = ven_scope_result else {
            panic!("Unexpected result from filter.");
        };
        assert_eq!(
            ven_scope_resource.content.targets,
            vec!["test_target_2".parse().unwrap()]
        );
        let bl_scope_result = privacy_filter_object(
            &object,
            &TestVenObjectPrivacyNoCallExpected,
            &"test_client_id".parse().unwrap(),
            &Claims::from_scopes(vec![Scope::ReadAll]),
        )
        .await;
        let Some(AnyObject::Resource(bl_scope_resource)) = bl_scope_result else {
            panic!("Unexpected result from filter.");
        };
        assert_eq!(
            bl_scope_resource.content.targets,
            vec!["test_target_2".parse().unwrap()]
        );
    }

    #[tokio::test]
    async fn privacy_filter_object_filters_subscription() {
        let object = AnyObject::Subscription(Subscription {
            id: "report_id".parse().unwrap(),
            created_date_time: DateTime::from_timestamp_nanos(15_000_000),
            modification_date_time: DateTime::from_timestamp_nanos(15_000_000),
            client_id: "test_client_id".parse().unwrap(),
            content: SubscriptionRequest {
                client_name: "client name".into(),
                program_id: None,
                object_operations: vec![],
            },
        });
        let no_scope_result = privacy_filter_object(
            &object,
            &TestVenObjectPrivacyNoCallExpected,
            &"test_client_id".parse().unwrap(),
            &Claims::from_scopes(vec![]),
        )
        .await;
        assert!(no_scope_result.is_none());
        let ven_scope_wrong_client_result = privacy_filter_object(
            &object,
            &TestVenObjectPrivacyNoCallExpected,
            &"other_test_client_id".parse().unwrap(),
            &Claims::from_scopes(vec![Scope::ReadVenObjects]),
        )
        .await;
        assert!(ven_scope_wrong_client_result.is_none());
        let ven_scope_result = privacy_filter_object(
            &object,
            &TestVenObjectPrivacyNoCallExpected,
            &"test_client_id".parse().unwrap(),
            &Claims::from_scopes(vec![Scope::ReadVenObjects]),
        )
        .await;
        assert!(ven_scope_result.is_some());
        let bl_scope_result = privacy_filter_object(
            &object,
            &TestVenObjectPrivacyNoCallExpected,
            &"test_client_id".parse().unwrap(),
            &Claims::from_scopes(vec![Scope::ReadAll]),
        )
        .await;
        assert!(bl_scope_result.is_some());
    }

    #[tokio::test]
    async fn privacy_filter_object_filters_ven() {
        let object = AnyObject::Ven(Ven {
            id: "report_id".parse().unwrap(),
            created_date_time: DateTime::from_timestamp_nanos(15_000_000),
            modification_date_time: DateTime::from_timestamp_nanos(15_000_000),
            content: BlVenRequest {
                client_id: "test_client_id".parse().unwrap(),
                targets: vec!["test_target_2".parse().unwrap()],
                ven_name: "ven name".into(),
                attributes: None,
            },
        });
        let no_scope_result = privacy_filter_object(
            &object,
            &TestVenObjectPrivacyNoCallExpected,
            &"test_client_id".parse().unwrap(),
            &Claims::from_scopes(vec![]),
        )
        .await;
        assert!(no_scope_result.is_none());
        let ven_scope_wrong_client_result = privacy_filter_object(
            &object,
            &TestVenObjectPrivacyNoCallExpected,
            &"other_test_client_id".parse().unwrap(),
            &Claims::from_scopes(vec![Scope::ReadVenObjects]),
        )
        .await;
        assert!(ven_scope_wrong_client_result.is_none());
        let ven_scope_result = privacy_filter_object(
            &object,
            &TestVenObjectPrivacyNoCallExpected,
            &"test_client_id".parse().unwrap(),
            &Claims::from_scopes(vec![Scope::ReadVenObjects]),
        )
        .await;
        let Some(AnyObject::Ven(ven_scope_ven)) = ven_scope_result else {
            panic!("Unexpected result from filter.");
        };
        assert_eq!(
            ven_scope_ven.content.targets,
            vec!["test_target_2".parse().unwrap()]
        );
        let bl_scope_result = privacy_filter_object(
            &object,
            &TestVenObjectPrivacyNoCallExpected,
            &"test_client_id".parse().unwrap(),
            &Claims::from_scopes(vec![Scope::ReadAll]),
        )
        .await;
        let Some(AnyObject::Ven(bl_scope_ven)) = bl_scope_result else {
            panic!("Unexpected result from filter.");
        };
        assert_eq!(
            bl_scope_ven.content.targets,
            vec!["test_target_2".parse().unwrap()]
        );
    }

    #[sqlx::test(fixtures("vens", "users"))]
    async fn empty_object_operations_not_allowed(db: PgPool) {
        let server = ApiTest::new(
            db,
            "ven-1-client-id",
            vec![Scope::WriteSubscriptionsBl, Scope::ReadAll],
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
            vec![Scope::WriteSubscriptionsBl, Scope::ReadAll],
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
            vec![Scope::WriteSubscriptionsBl, Scope::ReadAll],
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

    #[sqlx::test(fixtures("vens", "users"))]
    async fn mqtt_business_logic(db: PgPool) {
        let server = ApiTest::new(
            db,
            "ven-100-client-id",
            vec![
                Scope::WriteVensBl,
                Scope::WriteSubscriptionsBl,
                Scope::WritePrograms,
                Scope::ReadAll,
            ],
        )
        .await;

        let vtn_config = server.vtn_config();
        let mqtt_client = paho_mqtt::AsyncClient::new(paho_mqtt::CreateOptions::new()).unwrap();
        mqtt_client
            .connect(
                paho_mqtt::ConnectOptionsBuilder::new()
                    .server_uris(&[&vtn_config.mqtt_url.as_ref().unwrap()])
                    .user_name(vtn_config.mqtt_username.as_ref().unwrap())
                    .password(vtn_config.mqtt_password.as_ref().unwrap())
                    .finalize(),
            )
            .await
            .unwrap();
        mqtt_client
            .subscribe(
                format!("{}push/#", vtn_config.mqtt_topic_prefix),
                QoS::ExactlyOnce,
            )
            .await
            .unwrap();
        let mqtt_rx = mqtt_client.start_consuming();

        let expect_msg =
            |id: &str, object_type: ObjectType, operation: Operation, topics: &[&str]| {
                let mut topics = topics.iter().copied().collect::<BTreeSet<&str>>();
                while !topics.is_empty() {
                    let msg = mqtt_rx
                        .recv_timeout(Duration::from_millis(50))
                        .unwrap()
                        .unwrap();
                    if !topics.remove(
                        msg.topic()
                            .strip_prefix(&vtn_config.mqtt_topic_prefix)
                            .unwrap(),
                    ) {
                        panic!("unexpected message {msg:?}");
                    }
                    let msg_data: MqttPushNotification =
                        serde_json::from_slice(msg.payload()).unwrap();
                    assert_eq!(msg_data.id.as_str(), id);
                    assert_eq!(msg_data.object_type, object_type);
                    assert_eq!(msg_data.operation, operation);
                }
                if let Ok(msg) = mqtt_rx.recv_timeout(Duration::from_millis(50)) {
                    panic!("stray message {msg:?}")
                }
            };

        let (status, ven) = server
            .request::<Ven>(
                Method::POST,
                "/vens",
                Body::from(
                    serde_json::to_vec(&VenRequest::BlVenRequest(BlVenRequest {
                        client_id: "ven-100-client-id".parse().unwrap(),
                        targets: vec!["target-1".parse().unwrap()],
                        ven_name: "ven-100".to_owned(),
                        attributes: None,
                    }))
                    .unwrap(),
                ),
            )
            .await;
        assert_eq!(status, StatusCode::CREATED);
        expect_msg(
            ven.id.as_str(),
            ObjectType::Ven,
            Operation::Create,
            &["push/vens/create"],
        );

        let (status, program) = server
            .request::<Program>(
                Method::POST,
                "/programs",
                Body::from(
                    serde_json::to_vec(&ProgramRequest {
                        program_name: "program_name".to_string(),
                        interval_period: None,
                        program_descriptions: None,
                        payload_descriptors: None,
                        attributes: None,
                        targets: vec![],
                    })
                    .unwrap(),
                ),
            )
            .await;
        assert_eq!(status, StatusCode::CREATED);
        expect_msg(
            program.id.as_str(),
            ObjectType::Program,
            Operation::Create,
            &[
                "push/programs/create",
                "push/vens/ven-1/programs/create",
                "push/vens/ven-2/programs/create",
                "push/vens/ven-3/programs/create",
                "push/vens/ven-4/programs/create",
                "push/vens/ven-has-no-targets/programs/create",
                &format!("push/programs/{}/create", program.id),
                &format!("push/vens/{}/programs/create", ven.id),
            ],
        );

        let (status, program) = server
            .request::<Program>(
                Method::POST,
                "/programs",
                Body::from(
                    serde_json::to_vec(&ProgramRequest {
                        program_name: "program_name2".to_string(),
                        interval_period: None,
                        program_descriptions: None,
                        payload_descriptors: None,
                        attributes: None,
                        targets: vec!["target-1".parse().unwrap(), "target-2".parse().unwrap()],
                    })
                    .unwrap(),
                ),
            )
            .await;
        assert_eq!(status, StatusCode::CREATED);
        expect_msg(
            program.id.as_str(),
            ObjectType::Program,
            Operation::Create,
            &[
                "push/programs/create",
                &format!("push/programs/{}/create", program.id),
                &format!("push/vens/{}/programs/create", ven.id),
            ],
        );

        let (status, resource) = server
            .request::<Resource>(
                Method::POST,
                "/resources",
                Body::from(
                    serde_json::to_vec(&ResourceRequest::BlResourceRequest(BlResourceRequest {
                        targets: vec![],
                        resource_name: "my_resource100".to_string(),
                        ven_id: ven.id.clone(),
                        attributes: None,
                    }))
                    .unwrap(),
                ),
            )
            .await;
        assert_eq!(status, StatusCode::CREATED);
        expect_msg(
            resource.id.as_str(),
            ObjectType::Resource,
            Operation::Create,
            &[
                "push/resources/create",
                &format!("push/vens/{}/resources/create", ven.id),
            ],
        );
    }
}
