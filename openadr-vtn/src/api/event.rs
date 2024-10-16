use std::sync::Arc;

use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use serde::Deserialize;
use tracing::{info, trace};
use validator::{Validate, ValidationError};

use openadr_wire::{
    event::{EventContent, EventId},
    program::ProgramId,
    target::TargetLabel,
    Event,
};

use crate::{
    api::{AppResponse, ValidatedJson, ValidatedQuery},
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
#[validate(schema(function = "validate_target_type_value_pair"))]
#[serde(rename_all = "camelCase")]
pub struct QueryParams {
    #[serde(rename = "programID")]
    pub(crate) program_id: Option<ProgramId>,
    pub(crate) target_type: Option<TargetLabel>,
    pub(crate) target_values: Option<Vec<String>>,
    #[serde(default)]
    #[validate(range(min = 0))]
    pub(crate) skip: i64,
    // TODO how to interpret limit = 0 and what is the default?
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

#[cfg(test)]
#[cfg(feature = "live-db-test")]
mod test {
    use crate::{data_source::PostgresStorage, jwt::JwtManager, state::AppState};

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
    use openadr_wire::{
        event::{EventPayloadDescriptor, EventType, Priority},
        problem::Problem,
    };
    use reqwest::Method;
    use sqlx::PgPool;
    use tower::{Service, ServiceExt};

    fn default_event_content() -> EventContent {
        EventContent {
            object_type: None,
            program_id: ProgramId::new("program-1").unwrap(),
            event_name: Some("event_name".to_string()),
            priority: Priority::MAX,
            report_descriptors: None,
            interval_period: None,
            intervals: vec![],
            payload_descriptors: None,
            targets: None,
        }
    }

    fn event_request(method: http::Method, event: Event, token: &str) -> Request<Body> {
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

        (
            AppState::new(store, JwtManager::from_base64_secret("test").unwrap()),
            events,
        )
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

        let (state, _) = state_with_events(vec![event1, event2, event3], db).await;
        let token = jwt_test_token(&state, vec![AuthRole::AnyBusiness]);
        let mut app = state.into_router();

        // no query params
        let response = retrieve_all_with_filter_help(&mut app, "", &token).await;
        assert_eq!(response.status(), StatusCode::OK);

        let body = response.into_body().collect().await.unwrap().to_bytes();
        let programs: Vec<Event> = serde_json::from_slice(&body).unwrap();
        assert_eq!(programs.len(), 3);

        // skip
        let response = retrieve_all_with_filter_help(&mut app, "skip=1", &token).await;
        assert_eq!(response.status(), StatusCode::OK);

        let body = response.into_body().collect().await.unwrap().to_bytes();
        let programs: Vec<Event> = serde_json::from_slice(&body).unwrap();
        assert_eq!(programs.len(), 2);

        let response = retrieve_all_with_filter_help(&mut app, "skip=-1", &token).await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        let response = retrieve_all_with_filter_help(&mut app, "skip=0", &token).await;
        assert_eq!(response.status(), StatusCode::OK);

        // limit
        let response = retrieve_all_with_filter_help(&mut app, "limit=2", &token).await;
        assert_eq!(response.status(), StatusCode::OK);

        let body = response.into_body().collect().await.unwrap().to_bytes();
        let programs: Vec<Event> = serde_json::from_slice(&body).unwrap();
        assert_eq!(programs.len(), 2);

        let response = retrieve_all_with_filter_help(&mut app, "limit=-1", &token).await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        let response = retrieve_all_with_filter_help(&mut app, "limit=0", &token).await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        // program name
        let response = retrieve_all_with_filter_help(&mut app, "targetType=NONSENSE", &token).await;
        assert_eq!(
            response.status(),
            StatusCode::BAD_REQUEST,
            "Do return BAD_REQUEST on empty targetValue"
        );

        let response =
            retrieve_all_with_filter_help(&mut app, "targetType=NONSENSE&targetValues", &token)
                .await;
        assert_eq!(
            response.status(),
            StatusCode::BAD_REQUEST,
            "Do return BAD_REQUEST on empty targetValue"
        );

        let response = retrieve_all_with_filter_help(
            &mut app,
            "targetType=NONSENSE&targetValues=test",
            &token,
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);

        let body = response.into_body().collect().await.unwrap().to_bytes();
        let programs: Vec<Event> = serde_json::from_slice(&body).unwrap();
        assert_eq!(programs.len(), 0);

        let response = retrieve_all_with_filter_help(
            &mut app,
            "targetType=PROGRAM_NAME&targetValues=program-1&targetValues=program-2",
            &token,
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);

        let body = response.into_body().collect().await.unwrap().to_bytes();
        let programs: Vec<Event> = serde_json::from_slice(&body).unwrap();
        assert_eq!(programs.len(), 3);

        let response = retrieve_all_with_filter_help(
            &mut app,
            "targetType=PROGRAM_NAME&targetValues=program-1",
            &token,
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);

        let body = response.into_body().collect().await.unwrap().to_bytes();
        let programs: Vec<Event> = serde_json::from_slice(&body).unwrap();
        assert_eq!(programs.len(), 1);

        let response = retrieve_all_with_filter_help(&mut app, "programID=program-1", &token).await;
        assert_eq!(response.status(), StatusCode::OK);

        let body = response.into_body().collect().await.unwrap().to_bytes();
        let programs: Vec<Event> = serde_json::from_slice(&body).unwrap();
        assert_eq!(programs.len(), 1);
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
            let response = retrieve_all_with_filter_help(
                &mut app,
                "targetType=VEN_NAME&targetValues=ven-1-name",
                &token,
            )
            .await;
            assert_eq!(response.status(), StatusCode::OK);
            let body = response.into_body().collect().await.unwrap().to_bytes();
            let events: Vec<Event> = serde_json::from_slice(&body).unwrap();
            assert_eq!(events.len(), 1);

            let token = jwt_test_token(&state, vec![AuthRole::Business("business-1".to_string())]);
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
