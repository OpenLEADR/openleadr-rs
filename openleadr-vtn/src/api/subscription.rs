use std::{
    collections::{BTreeSet, HashMap},
    convert::Infallible,
    sync::Arc,
    time::Duration,
};

#[cfg(feature = "experimental-websockets")]
use axum::{
    extract::ws::{Message, WebSocketUpgrade},
    response::Response,
};
use axum::{
    extract::{Path, State},
    routing::MethodRouter,
    Json,
};
use chrono::Utc;
use openleadr_wire::{
    program::ProgramId,
    subscription::{
        AnyObject, MqttNotifierAuthentication, MqttNotifierBindingObject, MqttPushNotification,
        Notification, NotifierOperationsTopics, NotifierTopicsResponse, NotifiersResponse,
        Operation, SerializationType, Subscription, SubscriptionId, SubscriptionRequest,
    },
    ClientId, Identifier, ObjectType,
};
use paho_mqtt::QoS;
use reqwest::StatusCode;
use serde::Deserialize;
use tokio::sync::{mpsc, Mutex};
use tracing::{error, info, trace};
use uuid::{ContextV7, Uuid};
use validator::Validate;

use crate::{
    api::{AppResponse, ValidatedJson, ValidatedQuery},
    data_source::{EventCrud, SubscriptionCrud, VenCrud},
    error::AppError,
    jwt::{Scope, User},
    state::AppState,
};

pub(crate) struct NotifierState {
    uuidv7_context: Arc<Mutex<ContextV7>>,
    websockets: Mutex<HashMap<ClientId, mpsc::UnboundedSender<Notification>>>,
    subscriptions: Mutex<HashMap<SubscriptionId, Subscription>>,
    mqtt_url: String,
    mqtt_client: paho_mqtt::AsyncClient,
    mqtt_topic_prefix: String,
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

        let mqtt_client =
            paho_mqtt::AsyncClient::new(paho_mqtt::CreateOptions::new()).expect("TODO");
        mqtt_client
            .connect(
                paho_mqtt::ConnectOptionsBuilder::new()
                    .server_uris(&[&mqtt_url])
                    .user_name(mqtt_username)
                    .password(mqtt_password)
                    .automatic_reconnect(Duration::from_millis(1), Duration::from_secs(5)) // FIXME is this a sensible interval?
                    .finalize(),
            )
            .await
            .expect("TODO");

        Ok(Self {
            uuidv7_context: Arc::new(Mutex::new(ContextV7::new())),
            websockets: Mutex::new(HashMap::new()),
            subscriptions: Mutex::new(
                subscriptions
                    .into_iter()
                    .map(|subscription| (subscription.id.clone(), subscription))
                    .collect(),
            ),
            mqtt_url,
            mqtt_client,
            mqtt_topic_prefix,
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
    ven_source: &dyn VenCrud,
    event_source: &dyn EventCrud,
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
    let notification_date_time = Utc::now();

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
        AnyObject::Subscription(_) | AnyObject::Ven(_) | AnyObject::Resource(_) => None,
    };

    trace!(id = %object.id(), object = ?object, "notify {operation:?}");

    let operation_str = match operation {
        Operation::Create => "create",
        Operation::Update => "update",
        Operation::Delete => "delete",
    };
    let mqtt_notification = serde_json::to_vec(&Notification {
        id: uuid.clone(),
        operation,
        object: object.clone(),
    })
    .unwrap();
    let mqtt_push_notification = serde_json::to_vec(&MqttPushNotification {
        id: object.id(),
        notification_id: uuid.clone(),
        object_type: object.kind(),
        operation,
        notification_date_time,
    })
    .unwrap();
    macro_rules! publish_mqtt_push {
        ($topic_base:expr) => {{
            notifier_state
                .mqtt_client
                .publish(paho_mqtt::Message::new(
                    format!(
                        "{}{}{operation_str}",
                        notifier_state.mqtt_topic_prefix, $topic_base,
                    ),
                    &*mqtt_notification,
                    QoS::AtMostOnce,
                ))
                .await
                .unwrap();
            notifier_state
                .mqtt_client
                .publish(paho_mqtt::Message::new(
                    format!(
                        "{}push/{}{operation_str}",
                        notifier_state.mqtt_topic_prefix, $topic_base,
                    ),
                    &*mqtt_push_notification,
                    QoS::AtMostOnce,
                ))
                .await
                .unwrap();
        }};
    }

    macro_rules! publish_mqtt_push_by_targets {
        ($kind:literal, $targets:expr) => {
            let mut vens = BTreeSet::new();
            for target in &$targets {
                if let Ok(new_vens) = ven_source
                    .retrieve_all(
                        &super::ven::QueryParams {
                            ven_name: None,
                            targets: crate::api::TargetQueryParams(Some(vec![target.clone()])),
                            skip: 0,
                            limit: i64::MAX,
                        },
                        &None,
                    )
                    .await
                {
                    vens.extend(new_vens.into_iter().map(|ven| ven.id));
                }
            }
            for ven in vens {
                publish_mqtt_push!(format!("vens/{ven}/{}/", $kind));
            }
        };
    }

    match &object {
        AnyObject::Ven(ven) => {
            publish_mqtt_push!("vens/");
            if operation != Operation::Create {
                publish_mqtt_push!(&format!("vens/{}/", ven.id));
            }
        }
        AnyObject::Resource(resource) => {
            publish_mqtt_push!("resources/");
            publish_mqtt_push!(&format!("vens/{}/resources/", resource.content.ven_id));
        }
        AnyObject::Program(program) => {
            publish_mqtt_push!(&format!("programs/{}/", program.id));
            if program.content.targets.is_empty() {
                publish_mqtt_push!("programs/"); // Public event
            } else {
                publish_mqtt_push_by_targets!("programs", program.content.targets);
            }
        }
        AnyObject::Event(event) => {
            publish_mqtt_push!(&format!("events/program/{}/", event.content.program_id));
            if event.content.targets.is_empty() {
                publish_mqtt_push!("events/");
            } else {
                publish_mqtt_push_by_targets!("events", event.content.targets);
            }
        }
        AnyObject::Report(_) => publish_mqtt_push!("reports/"),
        AnyObject::Subscription(_) => {}
    }

    for subscription in notifier_state.subscriptions.lock().await.values() {
        // FIXME handle object privacy

        let program_id = subscription.content.program_id.as_ref();

        for object_operation in &subscription.content.object_operations {
            if !object_operation.operations.contains(&operation)
                || !object_operation.objects.contains(&object.kind())
                || (program_id.is_some() && program_id != target_program_id)
            {
                continue;
            }

            if let Some(tx) = notifier_state
                .websockets
                .lock()
                .await
                .get(&subscription.client_id)
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

pub(crate) async fn notifier_get(
    State(notifier_state): State<Arc<NotifierState>>,
    User(user): User,
) -> Result<Json<NotifiersResponse>, AppError> {
    if !user.scope.contains(Scope::ReadAll) {
        return Err(AppError::Forbidden("Missing 'read_all' scope"));
    }

    Ok(Json(NotifiersResponse {
        websocket: cfg!(feature = "experimental-websockets"),
        mqtt: Some(MqttNotifierBindingObject {
            uris: vec![notifier_state.mqtt_url.clone()],
            serialization: SerializationType::Json,
            authentication: MqttNotifierAuthentication::Oauth2BearerToken {
                username: "{clientID}".to_owned(),
            },
        }),
        push_mqtt: Some(MqttNotifierBindingObject {
            uris: vec![notifier_state.mqtt_url.clone()],
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
    if !user.scope.contains(Scope::ReadAll) {
        return Err(AppError::Forbidden("Missing 'read_all' scope"));
    }

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
}

fn mqtt_route_public(
    base_topic: &'static str,
    subscribe_create: bool,
) -> MethodRouter<AppState, Infallible> {
    axum::routing::get(
        move |State(notifier_state): State<Arc<NotifierState>>| async move {
            let prefix = &notifier_state.mqtt_topic_prefix;
            Json(NotifierTopicsResponse {
                topics: NotifierOperationsTopics {
                    create: subscribe_create.then_some(format!("{prefix}{base_topic}create")),
                    update: format!("{prefix}{base_topic}update"),
                    delete: format!("{prefix}{base_topic}delete"),
                    all: Some(format!("{prefix}{base_topic}+")),
                },
            })
        },
    )
}

fn mqtt_route_bl(
    base_topic: &'static str,
    subscribe_create: bool,
) -> MethodRouter<AppState, Infallible> {
    axum::routing::get(
        move |State(notifier_state): State<Arc<NotifierState>>| async move {
            // FIXME check BL client

            let prefix = &notifier_state.mqtt_topic_prefix;
            Json(NotifierTopicsResponse {
                topics: NotifierOperationsTopics {
                    create: subscribe_create.then_some(format!("{prefix}{base_topic}create")),
                    update: format!("{prefix}{base_topic}update"),
                    delete: format!("{prefix}{base_topic}delete"),
                    all: Some(format!("{prefix}{base_topic}+")),
                },
            })
        },
    )
}

fn mqtt_route_bl_by_program_id(
    base_topic: &'static str,
    subscribe_create: bool,
) -> MethodRouter<AppState, Infallible> {
    axum::routing::get(
        move |State(notifier_state): State<Arc<NotifierState>>, Path(program_id): Path<String>| async move {
            // FIXME check BL client

            let prefix = &notifier_state.mqtt_topic_prefix;
            Json(NotifierTopicsResponse {
                topics: NotifierOperationsTopics {
                    create: subscribe_create
                        .then_some(format!("{prefix}{base_topic}{program_id}/create")),
                    update: format!("{prefix}{base_topic}{program_id}/update"),
                    delete: format!("{prefix}{base_topic}{program_id}/delete"),
                    all: Some(format!("{prefix}{base_topic}{program_id}/+")),
                },
            })
        },
    )
}

fn mqtt_route_by_ven_id(
    base_topic: &'static str,
    subscribe_create: bool,
) -> MethodRouter<AppState, Infallible> {
    axum::routing::get(
        move |State(notifier_state): State<Arc<NotifierState>>, Path(ven_id): Path<String>| async move {
            // FIXME validate that ven matches authenticated user
            let prefix = &notifier_state.mqtt_topic_prefix;
            Json(NotifierTopicsResponse {
                topics: NotifierOperationsTopics {
                    create: subscribe_create
                        .then_some(format!("{prefix}{base_topic}vens/{ven_id}/create")),
                    update: format!("{prefix}{base_topic}vens/{ven_id}/update"),
                    delete: format!("{prefix}{base_topic}vens/{ven_id}/delete"),
                    all: Some(format!("{prefix}{base_topic}vens/{ven_id}/+")),
                },
            })
        },
    )
}

#[cfg(test)]
mod test {
    use std::{collections::BTreeSet, time::Duration};

    use axum::body::Body;
    use openleadr_wire::{
        problem::Problem,
        program::ProgramRequest,
        resource::{BlResourceRequest, Resource, ResourceRequest},
        subscription::{MqttPushNotification, Operation, Subscription},
        ven::{BlVenRequest, VenRequest},
        ObjectType, Program, Ven,
    };
    use paho_mqtt::QoS;
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

    #[sqlx::test(fixtures("vens", "users"))]
    async fn mqtt_business_logic(db: PgPool) {
        let server = ApiTest::new(
            db,
            "ven-100-client-id",
            vec![
                Scope::WriteVensBl,
                Scope::WriteSubscriptions,
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
                let mut topics = topics.into_iter().copied().collect::<BTreeSet<&str>>();
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
                        panic!("{msg}");
                    }
                    let msg_data: MqttPushNotification =
                        serde_json::from_slice(msg.payload()).unwrap();
                    assert_eq!(msg_data.id.as_str(), id);
                    assert_eq!(msg_data.object_type, object_type);
                    assert_eq!(msg_data.operation, operation);
                }
                match mqtt_rx.recv_timeout(Duration::from_millis(50)) {
                    Err(_) => {}
                    Ok(msg) => panic!("stray message {msg:?}"),
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
                &format!("push/programs/{}/create", program.id),
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
