use std::sync::Arc;

use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use serde::Deserialize;
use tracing::{info, trace};
use validator::Validate;

use openleadr_wire::{
    event::{EventContent, EventId},
    program::ProgramId,
    Event,
};

use crate::{
    api::{AppResponse, TargetQueryParams, ValidatedJson, ValidatedQuery},
    data_source::EventCrud,
    error::AppError,
    jwt::{BusinessUser, User},
};

pub async fn get_all(
    State(event_source): State<Arc<dyn EventCrud>>,
    ValidatedQuery(query_params): ValidatedQuery<QueryParams>,
    user: User,
) -> AppResponse<Vec<Event>> {
    trace!(?query_params);

    let events = event_source.retrieve_all(&query_params, &user).await?;
    trace!("retrieved {} events", events.len());

    Ok(Json(events))
}

pub async fn get(
    State(event_source): State<Arc<dyn EventCrud>>,
    Path(id): Path<EventId>,
    user: User,
) -> AppResponse<Event> {
    let event = event_source.retrieve(&id, &user).await?;
    trace!(%event.id, event.event_name=event.content.event_name, "retrieved event");

    Ok(Json(event))
}

pub async fn add(
    State(event_source): State<Arc<dyn EventCrud>>,
    BusinessUser(user): BusinessUser,
    ValidatedJson(new_event): ValidatedJson<EventContent>,
) -> Result<(StatusCode, Json<Event>), AppError> {
    let event = event_source.create(new_event, &User(user)).await?;

    info!(%event.id, event_name=event.content.event_name, "event created");

    Ok((StatusCode::CREATED, Json(event)))
}

pub async fn edit(
    State(event_source): State<Arc<dyn EventCrud>>,
    Path(id): Path<EventId>,
    BusinessUser(user): BusinessUser,
    ValidatedJson(content): ValidatedJson<EventContent>,
) -> AppResponse<Event> {
    let event = event_source.update(&id, content, &User(user)).await?;

    info!(%event.id, event_name=event.content.event_name, "event updated");

    Ok(Json(event))
}

pub async fn delete(
    State(event_source): State<Arc<dyn EventCrud>>,
    Path(id): Path<EventId>,
    BusinessUser(user): BusinessUser,
) -> AppResponse<Event> {
    let event = event_source.delete(&id, &User(user)).await?;
    info!(%event.id, event.event_name=event.content.event_name, "deleted event");
    Ok(Json(event))
}

#[derive(Deserialize, Validate, Debug)]
#[serde(rename_all = "camelCase")]
pub struct QueryParams {
    #[serde(rename = "programID")]
    pub(crate) program_id: Option<ProgramId>,
    #[serde(flatten)]
    #[validate(nested)]
    pub(crate) targets: TargetQueryParams,
    #[serde(default)]
    #[validate(range(min = 0))]
    pub(crate) skip: i64,
    // TODO how to interpret limit = 0 and what is the default?
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
    use crate::{data_source::PostgresStorage, state::AppState};

    use super::*;
    use crate::api::test::*;
    // for `call`, `oneshot`, and `ready`
    use crate::data_source::DataSource;
    // for `collect`
    use crate::jwt::{AuthRole, Claims};
    use axum::{
        body::Body,
        http::{self, Request, Response, StatusCode},
        Router,
    };
    use http_body_util::BodyExt;
    use openleadr_wire::{
        event::{EventInterval, EventPayloadDescriptor, EventType, EventValuesMap, Priority},
        problem::Problem,
        target::{TargetEntry, TargetMap, TargetType},
        values_map::Value,
    };
    use reqwest::Method;
    use sqlx::PgPool;
    use tower::{Service, ServiceExt};

    fn default_event_content() -> EventContent {
        EventContent {
            program_id: ProgramId::new("program-1").unwrap(),
            event_name: Some("event_name".to_string()),
            priority: Priority::MAX,
            report_descriptors: None,
            interval_period: None,
            intervals: vec![EventInterval {
                id: 0,
                interval_period: None,
                payloads: vec![EventValuesMap {
                    value_type: EventType::Price,
                    values: vec![Value::Number(123.4)],
                }],
            }],
            payload_descriptors: None,
            targets: None,
        }
    }

    fn event_request(method: Method, event: Event, token: &str) -> Request<Body> {
        Request::builder()
            .method(method)
            .uri(format!("/events/{}", event.id))
            .header(http::header::AUTHORIZATION, format!("Bearer {}", token))
            .header(http::header::CONTENT_TYPE, mime::APPLICATION_JSON.as_ref())
            .body(Body::from(serde_json::to_vec(&event).unwrap()))
            .unwrap()
    }

    async fn state_with_events(
        new_events: Vec<EventContent>,
        db: PgPool,
    ) -> (AppState, Vec<Event>) {
        let store = PostgresStorage::new(db).unwrap();
        let mut events = Vec::new();

        for event in new_events {
            events.push(
                store
                    .events()
                    .create(event.clone(), &User(Claims::any_business_user()))
                    .await
                    .unwrap(),
            );
            assert_eq!(events[events.len() - 1].content, event)
        }

        (AppState::new(store), events)
    }

    async fn get_help(id: &str, token: &str, app: &mut Router) -> Response<Body> {
        app.oneshot(
            Request::builder()
                .method(http::Method::GET)
                .uri(format!("/events/{}", id))
                .header(http::header::AUTHORIZATION, format!("Bearer {}", token))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap()
    }

    #[sqlx::test(fixtures("programs"))]
    async fn get(db: PgPool) {
        let (state, mut events) = state_with_events(vec![default_event_content()], db).await;
        let event = events.remove(0);
        let token = jwt_test_token(&state, vec![AuthRole::AnyBusiness]);
        let mut app = state.into_router();

        let response = get_help(event.id.as_str(), &token, &mut app).await;

        assert_eq!(response.status(), StatusCode::OK);

        let body = response.into_body().collect().await.unwrap().to_bytes();
        let db_event: Event = serde_json::from_slice(&body).unwrap();

        assert_eq!(event, db_event);
    }

    #[sqlx::test(fixtures("programs"))]
    async fn delete(db: PgPool) {
        let event1 = EventContent {
            program_id: ProgramId::new("program-1").unwrap(),
            event_name: Some("event1".to_string()),
            ..default_event_content()
        };
        let event2 = EventContent {
            program_id: ProgramId::new("program-2").unwrap(),
            event_name: Some("event2".to_string()),
            ..default_event_content()
        };
        let event3 = EventContent {
            program_id: ProgramId::new("program-2").unwrap(),
            event_name: Some("event3".to_string()),
            ..default_event_content()
        };

        let (state, events) = state_with_events(vec![event1, event2.clone(), event3], db).await;
        let token = jwt_test_token(&state, vec![AuthRole::AnyBusiness]);
        let mut app = state.into_router();

        let event_id = events[1].id.clone();

        let request = Request::builder()
            .method(http::Method::DELETE)
            .header(http::header::AUTHORIZATION, format!("Bearer {}", token))
            .uri(format!("/events/{event_id}"))
            .body(Body::empty())
            .unwrap();

        let response = ServiceExt::<Request<Body>>::ready(&mut app)
            .await
            .unwrap()
            .call(request)
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let body = response.into_body().collect().await.unwrap().to_bytes();
        let db_event: Event = serde_json::from_slice(&body).unwrap();

        assert_eq!(event2, db_event.content);

        let response = retrieve_all_with_filter_help(&mut app, "", &token).await;
        assert_eq!(response.status(), StatusCode::OK);

        let body = response.into_body().collect().await.unwrap().to_bytes();
        let programs: Vec<Event> = serde_json::from_slice(&body).unwrap();
        assert_eq!(programs.len(), 2);
    }

    #[sqlx::test(fixtures("programs"))]
    async fn update(db: PgPool) {
        let (state, mut events) = state_with_events(vec![default_event_content()], db).await;
        let event = events.remove(0);
        let token = jwt_test_token(&state, vec![AuthRole::AnyBusiness]);
        let app = state.into_router();

        let response = app
            .oneshot(event_request(http::Method::PUT, event.clone(), &token))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let body = response.into_body().collect().await.unwrap().to_bytes();
        let db_program: Event = serde_json::from_slice(&body).unwrap();

        assert_eq!(event.content, db_program.content);
        assert!(event.modification_date_time < db_program.modification_date_time);
    }

    async fn help_create_event(
        mut app: &mut Router,
        content: &EventContent,
        token: &str,
    ) -> Response<Body> {
        let request = Request::builder()
            .method(http::Method::POST)
            .uri("/events")
            .header(http::header::AUTHORIZATION, format!("Bearer {}", token))
            .header(http::header::CONTENT_TYPE, mime::APPLICATION_JSON.as_ref())
            .body(Body::from(serde_json::to_vec(content).unwrap()))
            .unwrap();

        ServiceExt::<Request<Body>>::ready(&mut app)
            .await
            .unwrap()
            .call(request)
            .await
            .unwrap()
    }

    #[sqlx::test(fixtures("programs"))]
    async fn create_same_name(db: PgPool) {
        let (state, _) = state_with_events(vec![], db).await;
        let token = jwt_test_token(&state, vec![AuthRole::AnyBusiness]);
        let mut app = state.into_router();

        let content = default_event_content();

        let response = help_create_event(&mut app, &content, &token).await;
        assert_eq!(response.status(), StatusCode::CREATED);

        let response = help_create_event(&mut app, &content, &token).await;
        assert_eq!(response.status(), StatusCode::CREATED);
    }

    async fn retrieve_all_with_filter_help(
        app: &mut Router,
        query_params: &str,
        token: &str,
    ) -> Response<Body> {
        let request = Request::builder()
            .method(http::Method::GET)
            .uri(format!("/events?{query_params}"))
            .header(http::header::AUTHORIZATION, format!("Bearer {}", token))
            .header(http::header::CONTENT_TYPE, mime::APPLICATION_JSON.as_ref())
            .body(Body::empty())
            .unwrap();

        ServiceExt::<Request<Body>>::ready(app)
            .await
            .unwrap()
            .call(request)
            .await
            .unwrap()
    }

    #[sqlx::test(fixtures("programs"))]
    async fn retrieve_all_with_filter(db: PgPool) {
        let event1 = EventContent {
            program_id: ProgramId::new("program-1").unwrap(),
            event_name: Some("event1".to_string()),
            targets: Some(TargetMap(vec![TargetEntry {
                label: TargetType::Private("Something".to_string()),
                values: vec!["group-1".to_string()],
            }])),
            ..default_event_content()
        };
        let event2 = EventContent {
            program_id: ProgramId::new("program-2").unwrap(),
            event_name: Some("event2".to_string()),
            targets: Some(TargetMap(vec![TargetEntry {
                label: TargetType::Group,
                values: vec!["group-2".to_string()],
            }])),
            ..default_event_content()
        };
        let event3 = EventContent {
            program_id: ProgramId::new("program-2").unwrap(),
            event_name: Some("event3".to_string()),
            targets: Some(TargetMap(vec![TargetEntry {
                label: TargetType::Group,
                values: vec!["group-1".to_string()],
            }])),
            ..default_event_content()
        };

        let test = ApiTest::new(db, vec![AuthRole::AnyBusiness]);

        for event in vec![event1, event2, event3] {
            let (status, _) = test
                .request::<Event>(
                    Method::POST,
                    "/events",
                    Body::from(serde_json::to_vec(&event).unwrap()),
                )
                .await;
            assert_eq!(status, StatusCode::CREATED);
        }

        // no query params
        let (status, events) = test
            .request::<Vec<Event>>(Method::GET, "/events", Body::empty())
            .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(events.len(), 3);

        // skip
        let (status, events) = test
            .request::<Vec<Event>>(Method::GET, "/events?skip=1", Body::empty())
            .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(events.len(), 2);

        let (status, _) = test
            .request::<Problem>(Method::GET, "/events?skip=-1", Body::empty())
            .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);

        let (status, _) = test
            .request::<Vec<Event>>(Method::GET, "/events?skip=0", Body::empty())
            .await;
        assert_eq!(status, StatusCode::OK);

        // limit
        let (status, events) = test
            .request::<Vec<Event>>(Method::GET, "/events?limit=2", Body::empty())
            .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(events.len(), 2);

        let (status, _) = test
            .request::<Problem>(Method::GET, "/events?limit=-1", Body::empty())
            .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);

        let (status, _) = test
            .request::<Problem>(Method::GET, "/events?limit=0", Body::empty())
            .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);

        // filter by targets
        let (status, _) = test
            .request::<Problem>(Method::GET, "/events?targetType=NONSENSE", Body::empty())
            .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);

        let (status, _) = test
            .request::<Problem>(
                Method::GET,
                "/events?targetType=NONSENSE&targetValues",
                Body::empty(),
            )
            .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);

        let (status, events) = test
            .request::<Vec<Event>>(
                Method::GET,
                "/events?targetType=NONSENSE&targetValues=test",
                Body::empty(),
            )
            .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(events.len(), 0);

        let (status, events) = test
            .request::<Vec<Event>>(
                Method::GET,
                "/events?targetType=GROUP&targetValues=group-1",
                Body::empty(),
            )
            .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(events.len(), 1);

        let (status, events) = test
            .request::<Vec<Event>>(
                Method::GET,
                "/events?targetType=GROUP&targetValues=group-1&targetValues=group-2",
                Body::empty(),
            )
            .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(events.len(), 2);

        let (status, events) = test
            .request::<Vec<Event>>(Method::GET, "/events?programID=program-1", Body::empty())
            .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(events.len(), 1);
    }

    #[ignore = "Depends on https://github.com/oadr3-org/openadr3-vtn-reference-implementation/issues/104"]
    #[sqlx::test]
    async fn name_constraint_validation(db: PgPool) {
        let test = ApiTest::new(db, vec![AuthRole::AnyBusiness]);

        let events = [
            EventContent {
                event_name: Some("".to_string()),
                ..default_event_content()
            },
            EventContent {
                event_name: Some("This is more than 128 characters long and should be rejected This is more than 128 characters long and should be rejected asdfasd".to_string()),
                ..default_event_content()
            },
            EventContent {
                payload_descriptors: Some(vec![
                    EventPayloadDescriptor{
                        payload_type: EventType::Private("".to_string()),
                        units: None,
                        currency: None,
                    }
                ]),
                ..default_event_content()
            },
            EventContent {
                payload_descriptors: Some(vec![
                    EventPayloadDescriptor{
                        payload_type: EventType::Private("This is more than 128 characters long and should be rejected This is more than 128 characters long and should be rejected asdfasd".to_string()),
                        units: None,
                        currency: None,
                    }
                ]),
                ..default_event_content()
            },
        ];

        for event in &events {
            let (status, error) = test
                .request::<Problem>(
                    http::Method::POST,
                    "/events",
                    Body::from(serde_json::to_vec(&event).unwrap()),
                )
                .await;

            assert_eq!(status, StatusCode::BAD_REQUEST);
            assert!(error
                .detail
                .unwrap()
                .contains("outside of allowed range 1..=128"))
        }
    }

    #[sqlx::test(fixtures("programs"))]
    async fn ordered_by_priority(db: PgPool) {
        let test = ApiTest::new(db, vec![AuthRole::AnyBusiness]);

        let events = vec![
            EventContent {
                priority: Priority::MAX,
                ..default_event_content()
            },
            EventContent {
                priority: Priority::MIN,
                ..default_event_content()
            },
            EventContent {
                priority: Priority::new(32),
                ..default_event_content()
            },
            EventContent {
                priority: Priority::new(33),
                ..default_event_content()
            },
            EventContent {
                priority: Priority::UNSPECIFIED,
                ..default_event_content()
            },
            EventContent {
                priority: Priority::new(33),
                ..default_event_content()
            },
        ];

        let mut ids = vec![];
        for event in events {
            ids.push(
                test.request::<Event>(
                    Method::POST,
                    "/events",
                    Body::from(serde_json::to_vec(&event).unwrap()),
                )
                .await
                .1
                .id,
            )
        }

        let expected_order = [0_usize, 2, 5, 3, 4, 1];

        let (_, events) = test
            .request::<Vec<Event>>(Method::GET, "/events", Body::empty())
            .await;
        for (event, expected_pos) in events.into_iter().zip(expected_order) {
            assert_eq!(ids[expected_pos], event.id);
        }
    }

    mod permissions {
        use super::*;

        #[sqlx::test(fixtures("users", "programs", "business", "events"))]
        async fn business_can_write_event_in_own_program_only(db: PgPool) {
            let (state, _) = state_with_events(vec![], db).await;
            let mut app = state.clone().into_router();

            let content = EventContent {
                program_id: "program-3".parse().unwrap(),
                ..default_event_content()
            };

            let token = jwt_test_token(&state, vec![AuthRole::Business("business-1".to_string())]);
            let response = help_create_event(&mut app, &content, &token).await;
            assert_eq!(response.status(), StatusCode::CREATED);

            let token = jwt_test_token(&state, vec![AuthRole::Business("business-2".to_string())]);
            let response = help_create_event(&mut app, &content, &token).await;
            assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

            let token = jwt_test_token(
                &state,
                vec![
                    AuthRole::AnyBusiness,
                    AuthRole::Business("business-2".to_string()),
                ],
            );
            let response = help_create_event(&mut app, &content, &token).await;
            assert_eq!(response.status(), StatusCode::CREATED);

            let token = jwt_test_token(&state, vec![AuthRole::Business("business-2".to_string())]);
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method(http::Method::DELETE)
                        .uri(format!("/events/{}", "event-3"))
                        .header(http::header::AUTHORIZATION, format!("Bearer {}", token))
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

            let token = jwt_test_token(&state, vec![AuthRole::Business("business-2".to_string())]);
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method(http::Method::PUT)
                        .uri(format!("/events/{}", "event-3"))
                        .header(http::header::AUTHORIZATION, format!("Bearer {}", token))
                        .header(http::header::CONTENT_TYPE, mime::APPLICATION_JSON.as_ref())
                        .body(Body::from(serde_json::to_vec(&content).unwrap()))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

            let token = jwt_test_token(&state, vec![AuthRole::Business("business-1".to_string())]);
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method(http::Method::PUT)
                        .uri(format!("/events/{}", "event-3"))
                        .header(http::header::AUTHORIZATION, format!("Bearer {}", token))
                        .header(http::header::CONTENT_TYPE, mime::APPLICATION_JSON.as_ref())
                        .body(Body::from(serde_json::to_vec(&content).unwrap()))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);

            let token = jwt_test_token(&state, vec![AuthRole::Business("business-1".to_string())]);
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method(http::Method::DELETE)
                        .uri(format!("/events/{}", "event-3"))
                        .header(http::header::AUTHORIZATION, format!("Bearer {}", token))
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
        }

        #[sqlx::test(fixtures("users", "programs", "business", "events"))]
        async fn business_can_read_event_in_own_program_only(db: PgPool) {
            let (state, _) = state_with_events(vec![], db).await;
            let mut app = state.clone().into_router();

            let token = jwt_test_token(&state, vec![AuthRole::Business("business-1".to_string())]);
            let response = get_help("event-3", &token, &mut app).await;
            assert_eq!(response.status(), StatusCode::OK);

            let token = jwt_test_token(&state, vec![AuthRole::Business("business-1".to_string())]);
            let response = get_help("event-2", &token, &mut app).await;
            assert_eq!(response.status(), StatusCode::NOT_FOUND);

            let token = jwt_test_token(&state, vec![AuthRole::Business("business-2".to_string())]);
            let response = get_help("event-3", &token, &mut app).await;
            assert_eq!(response.status(), StatusCode::NOT_FOUND);

            let token = jwt_test_token(
                &state,
                vec![
                    AuthRole::VEN("ven-1".parse().unwrap()),
                    AuthRole::Business("business-2".to_string()),
                ],
            );
            let response = get_help("event-3", &token, &mut app).await;
            assert_eq!(response.status(), StatusCode::OK);
        }

        #[sqlx::test(fixtures("users", "programs", "business", "events", "vens", "vens-programs"))]
        async fn vens_can_read_event_in_assigned_program_only(db: PgPool) {
            let (state, _) = state_with_events(vec![], db).await;
            let mut app = state.clone().into_router();

            let token = jwt_test_token(&state, vec![AuthRole::VEN("ven-1".parse().unwrap())]);
            let response = get_help("event-3", &token, &mut app).await;
            assert_eq!(response.status(), StatusCode::OK);

            let token = jwt_test_token(&state, vec![AuthRole::VEN("ven-2".parse().unwrap())]);
            let response = get_help("event-3", &token, &mut app).await;
            assert_eq!(response.status(), StatusCode::NOT_FOUND);

            let token = jwt_test_token(
                &state,
                vec![
                    AuthRole::VEN("ven-2".parse().unwrap()),
                    AuthRole::VEN("ven-1".parse().unwrap()),
                ],
            );
            let response = get_help("event-3", &token, &mut app).await;
            assert_eq!(response.status(), StatusCode::OK);

            let token = jwt_test_token(
                &state,
                vec![
                    AuthRole::VEN("ven-2".parse().unwrap()),
                    AuthRole::Business("business-2".to_string()),
                ],
            );
            let response = get_help("event-3", &token, &mut app).await;
            assert_eq!(response.status(), StatusCode::NOT_FOUND);
        }

        #[sqlx::test(fixtures("users", "programs", "business", "events", "vens", "vens-programs"))]
        async fn vens_event_list_assigned_program_only(db: PgPool) {
            let (state, _) = state_with_events(vec![], db).await;
            let mut app = state.clone().into_router();

            let token = jwt_test_token(&state, vec![AuthRole::VEN("ven-1".parse().unwrap())]);
            let response = retrieve_all_with_filter_help(&mut app, "", &token).await;
            assert_eq!(response.status(), StatusCode::OK);
            let body = response.into_body().collect().await.unwrap().to_bytes();
            let events: Vec<Event> = serde_json::from_slice(&body).unwrap();
            assert_eq!(events.len(), 2);

            let token = jwt_test_token(
                &state,
                vec![
                    AuthRole::VEN("ven-1".parse().unwrap()),
                    AuthRole::VEN("ven-2".parse().unwrap()),
                ],
            );
            let response = retrieve_all_with_filter_help(&mut app, "", &token).await;
            assert_eq!(response.status(), StatusCode::OK);
            let body = response.into_body().collect().await.unwrap().to_bytes();
            let events: Vec<Event> = serde_json::from_slice(&body).unwrap();
            assert_eq!(events.len(), 3);

            // VEN should not be able to filter on other ven names,
            // even if they have a common set of events,
            // as this would leak information about which events the VENs have in common.
            let token = jwt_test_token(&state, vec![AuthRole::VEN("ven-1".parse().unwrap())]);
            let response = retrieve_all_with_filter_help(
                &mut app,
                "targetType=VEN_NAME&targetValues=ven-2-name",
                &token,
            )
            .await;
            assert_eq!(response.status(), StatusCode::OK);
            let body = response.into_body().collect().await.unwrap().to_bytes();
            let events: Vec<Event> = serde_json::from_slice(&body).unwrap();
            assert_eq!(events.len(), 0);
        }

        #[sqlx::test(fixtures("users", "programs", "business", "events", "vens", "vens-programs"))]
        async fn business_can_list_events_in_own_program_only(db: PgPool) {
            let (state, _) = state_with_events(vec![], db).await;
            let mut app = state.clone().into_router();

            let token = jwt_test_token(&state, vec![AuthRole::Business("business-1".to_string())]);
            let response = retrieve_all_with_filter_help(&mut app, "", &token).await;
            assert_eq!(response.status(), StatusCode::OK);
            let body = response.into_body().collect().await.unwrap().to_bytes();
            let events: Vec<Event> = serde_json::from_slice(&body).unwrap();
            assert_eq!(events.len(), 1);

            let token = jwt_test_token(&state, vec![AuthRole::Business("business-1".to_string())]);
            let response =
                retrieve_all_with_filter_help(&mut app, "programID=program-3", &token).await;
            assert_eq!(response.status(), StatusCode::OK);
            let body = response.into_body().collect().await.unwrap().to_bytes();
            let events: Vec<Event> = serde_json::from_slice(&body).unwrap();
            assert_eq!(events.len(), 1);

            let token = jwt_test_token(&state, vec![AuthRole::Business("business-1".to_string())]);
            let response =
                retrieve_all_with_filter_help(&mut app, "programID=program-2", &token).await;
            assert_eq!(response.status(), StatusCode::OK);
            let body = response.into_body().collect().await.unwrap().to_bytes();
            let events: Vec<Event> = serde_json::from_slice(&body).unwrap();
            assert_eq!(events.len(), 0);

            let token = jwt_test_token(&state, vec![AuthRole::Business("business-2".to_string())]);
            let response = retrieve_all_with_filter_help(&mut app, "", &token).await;
            assert_eq!(response.status(), StatusCode::OK);
            let body = response.into_body().collect().await.unwrap().to_bytes();
            let events: Vec<Event> = serde_json::from_slice(&body).unwrap();
            assert_eq!(events.len(), 0);

            let token = jwt_test_token(&state, vec![AuthRole::AnyBusiness]);
            let response = retrieve_all_with_filter_help(&mut app, "", &token).await;
            assert_eq!(response.status(), StatusCode::OK);
            let body = response.into_body().collect().await.unwrap().to_bytes();
            let events: Vec<Event> = serde_json::from_slice(&body).unwrap();
            assert_eq!(events.len(), 3);
        }

        #[sqlx::test(fixtures("users", "programs", "events", "vens", "vens-programs"))]
        async fn ven_cannot_write_event(db: PgPool) {
            let (state, _) = state_with_events(vec![], db).await;
            let mut app = state.clone().into_router();

            let token = jwt_test_token(&state, vec![AuthRole::VEN("ven-1".parse().unwrap())]);
            let response = help_create_event(&mut app, &default_event_content(), &token).await;
            assert_eq!(response.status(), StatusCode::FORBIDDEN);

            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method(http::Method::DELETE)
                        .uri(format!("/events/{}", "event-3"))
                        .header(http::header::AUTHORIZATION, format!("Bearer {}", token))
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::FORBIDDEN);

            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method(http::Method::PUT)
                        .uri(format!("/events/{}", "event-3"))
                        .header(http::header::AUTHORIZATION, format!("Bearer {}", token))
                        .header(http::header::CONTENT_TYPE, mime::APPLICATION_JSON.as_ref())
                        .body(Body::from(
                            serde_json::to_vec(&default_event_content()).unwrap(),
                        ))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::FORBIDDEN);
        }
    }
}
