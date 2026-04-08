use std::{collections::HashMap, sync::Arc};

use axum::{
    Json,
    extract::{Path, State},
};
#[cfg(feature = "experimental-websockets")]
use axum::{
    extract::ws::{Message, WebSocketUpgrade},
    response::Response,
};
use openleadr_wire::{
    ClientId, ObjectType,
    program::ProgramId,
    subscription::{
        AnyObject, Notification, NotifiersResponse, Operation, Subscription, SubscriptionId,
        SubscriptionRequest,
    },
};
use reqwest::StatusCode;
use serde::Deserialize;
use tokio::sync::{Mutex, mpsc};
use tracing::{error, info, trace};
use uuid::{ContextV7, Uuid};
use validator::Validate;

use crate::{
    api::{AppResponse, ValidatedJson, ValidatedQuery},
    data_source::{EventCrud, SubscriptionCrud, VenObjectPrivacy},
    error::AppError,
    jwt::{Claims, Scope, User},
    state::AppState,
};

pub(crate) struct NotifierState {
    uuidv7_context: Arc<Mutex<ContextV7>>,
    websockets: Mutex<HashMap<ClientId, (mpsc::UnboundedSender<Notification>, Claims)>>,
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
    event_source: &dyn EventCrud,
    privacy: &dyn VenObjectPrivacy,
    notifier_state: &NotifierState,
    operation: Operation,
    object: AnyObject,
) {
    let uuid = Uuid::new_v7(uuid::Timestamp::now(
        &*notifier_state.uuidv7_context.lock().await,
    ));

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

// Require logging in, but no special permissions beyond that.
pub(crate) async fn notifier_get(User(_): User) -> Result<Json<NotifiersResponse>, AppError> {
    Ok(Json(NotifiersResponse {
        websocket: cfg!(feature = "experimental-websockets"),
        mqtt: None,
        push_mqtt: None,
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

#[cfg(test)]
mod test {
    use std::{collections::HashMap, sync::Arc};

    use async_trait::async_trait;
    use axum::body::Body;
    use chrono::DateTime;
    use openleadr_wire::{
        ClientId, Event, ObjectType, Program, Report, Ven,
        event::{EventId, EventRequest, Priority},
        problem::Problem,
        program::ProgramRequest,
        report::ReportRequest,
        resource::{BlResourceRequest, Resource},
        resource_group::ResourceGroupId,
        subscription::{
            AnyObject, Notification, NotificationMechanism, Operation, Subscription,
            SubscriptionObjectOperation, SubscriptionRequest,
        },
        target::Target,
        ven::{BlVenRequest, VenId},
    };
    use reqwest::{Method, StatusCode};
    use sqlx::PgPool;
    use tokio::sync::{Mutex, mpsc::unbounded_channel};
    use uuid::ContextV7;

    use crate::{
        api::{
            event::QueryParams,
            subscription::{NotifierState, notify, privacy_filter_object},
            test::ApiTest,
        },
        data_source::{Crud, EventCrud, VenObjectPrivacy},
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
        type Filter = QueryParams;
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
        };

        notify(
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
}
