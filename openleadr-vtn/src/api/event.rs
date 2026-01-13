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
    event::{EventId, EventRequest},
    program::ProgramId,
    Event,
};

use crate::{
    api::{AppResponse, TargetQueryParams, ValidatedJson, ValidatedQuery},
    data_source::EventCrud,
    error::AppError,
    jwt::{Scope, User},
};

pub async fn get_all(
    State(event_source): State<Arc<dyn EventCrud>>,
    ValidatedQuery(query_params): ValidatedQuery<QueryParams>,
    User(user): User,
) -> AppResponse<Vec<Event>> {
    trace!(?query_params);
    let events = if user.scope.contains(Scope::ReadAll) {
        event_source.retrieve_all(&query_params, &None).await?
    } else if user.scope.contains(Scope::ReadTargets) {
        event_source
            .retrieve_all(&query_params, &Some(user.client_id()?))
            .await?
    } else {
        return Err(AppError::Forbidden(
            "Missing 'read_all' or 'read_targets' scope",
        ));
    };
    trace!(client_id = user.sub, "retrieved {} events", events.len());

    Ok(Json(events))
}

pub async fn get(
    State(event_source): State<Arc<dyn EventCrud>>,
    Path(id): Path<EventId>,
    User(user): User,
) -> AppResponse<Event> {
    let event = if user.scope.contains(Scope::ReadAll) {
        event_source.retrieve(&id, &None).await?
    } else if user.scope.contains(Scope::ReadTargets) {
        event_source.retrieve(&id, &Some(user.client_id()?)).await?
    } else {
        return Err(AppError::Forbidden(
            "Missing 'read_all' or 'read_targets' scope",
        ));
    };

    trace!(%event.id, event.event_name=event.content.event_name, client_id = user.sub, "retrieved event");

    Ok(Json(event))
}

pub async fn add(
    State(event_source): State<Arc<dyn EventCrud>>,
    User(user): User,
    ValidatedJson(new_event): ValidatedJson<EventRequest>,
) -> Result<(StatusCode, Json<Event>), AppError> {
    if !user.scope.contains(Scope::WriteEvents) {
        return Err(AppError::Forbidden("Missing 'write_events' scope"));
    }

    let event = event_source
        .create(new_event, &Some(user.client_id()?))
        .await?;

    info!(%event.id, event_name=event.content.event_name, client_id = user.sub, "event created");

    Ok((StatusCode::CREATED, Json(event)))
}

pub async fn edit(
    State(event_source): State<Arc<dyn EventCrud>>,
    Path(id): Path<EventId>,
    User(user): User,
    ValidatedJson(content): ValidatedJson<EventRequest>,
) -> AppResponse<Event> {
    if !user.scope.contains(Scope::WriteEvents) {
        return Err(AppError::Forbidden("Missing 'write_events' scope"));
    }

    let event = event_source
        .update(&id, content, &Some(user.client_id()?))
        .await?;

    info!(%event.id, event_name=event.content.event_name, client_id = user.sub, "event updated");

    Ok(Json(event))
}

pub async fn delete(
    State(event_source): State<Arc<dyn EventCrud>>,
    Path(id): Path<EventId>,
    User(user): User,
) -> AppResponse<Event> {
    if !user.scope.contains(Scope::WriteEvents) {
        return Err(AppError::Forbidden("Missing 'write_events' scope"));
    }

    let event = event_source.delete(&id, &Some(user.client_id()?)).await?;
    info!(%event.id, event.event_name=event.content.event_name, client_id = user.sub, "deleted event");
    Ok(Json(event))
}

#[derive(Deserialize, Validate, Debug)]
#[cfg_attr(test, derive(PartialEq, Eq))]
#[serde(rename_all = "camelCase")]
pub struct QueryParams {
    #[serde(rename = "programID")]
    pub(crate) program_id: Option<ProgramId>,
    // #[serde(flatten)]
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
    use std::str::FromStr;

    use super::*;
    use crate::api::test::*;
    // for `call`, `oneshot`, and `ready`
    use crate::data_source::DataSource;
    // for `collect`
    use axum::{
        body::Body,
        http::{self, Request, Response, StatusCode},
        Router,
    };
    use http_body_util::BodyExt;
    use openleadr_wire::{
        event::{EventInterval, EventPayloadDescriptor, EventType, EventValuesMap, Priority},
        problem::Problem,
        target::Target,
        values_map::Value,
    };
    use reqwest::Method;
    use sqlx::PgPool;
    use tower::{Service, ServiceExt};

    #[test]
    fn query_params_deserialization() {
        let query = "skip=1&limit=2&targets=group-1&targets=group-2&programID=program-1";
        let params: QueryParams = serde_html_form::from_str(query).unwrap();
        assert_eq!(
            params,
            QueryParams {
                program_id: Some("program-1".parse().unwrap(),),
                targets: TargetQueryParams(Some(vec![
                    Target::from_str("group-1").unwrap(),
                    Target::from_str("group-2").unwrap()
                ])),
                skip: 1,
                limit: 2,
            }
        );

        let query = "targets=group-1";
        let params: QueryParams = serde_html_form::from_str(query).unwrap();
        assert_eq!(
            params,
            QueryParams {
                targets: TargetQueryParams(Some(vec![Target::from_str("group-1").unwrap(),])),
                ..Default::default()
            }
        )
    }

    fn default_event_content() -> EventRequest {
        EventRequest {
            program_id: ProgramId::new("program-1").unwrap(),
            event_name: Some("event_name".to_string()),
            duration: None,
            priority: Priority::MAX,
            report_descriptors: None,
            interval_period: None,
            intervals: Some(vec![EventInterval {
                id: 0,
                interval_period: None,
                payloads: vec![EventValuesMap {
                    value_type: EventType::Price,
                    values: vec![Value::Number(123.4)],
                }],
            }]),
            payload_descriptors: None,
            targets: vec![],
        }
    }

    fn event_request(method: Method, event: Event, token: &str) -> Request<Body> {
        Request::builder()
            .method(method)
            .uri(format!("/events/{}", event.id))
            .header(http::header::AUTHORIZATION, format!("Bearer {token}"))
            .header(http::header::CONTENT_TYPE, mime::APPLICATION_JSON.as_ref())
            .body(Body::from(serde_json::to_vec(&event).unwrap()))
            .unwrap()
    }

    async fn state_with_events(
        new_events: Vec<EventRequest>,
        db: PgPool,
    ) -> (AppState, Vec<Event>) {
        let store = PostgresStorage::new(db).unwrap();
        let mut events = Vec::new();

        for event in new_events {
            events.push(store.events().create(event.clone(), &None).await.unwrap());
            assert_eq!(events[events.len() - 1].content, event)
        }

        (AppState::new(store).await, events)
    }

    async fn get_help(id: &str, token: &str, app: &mut Router) -> Response<Body> {
        app.oneshot(
            Request::builder()
                .method(Method::GET)
                .uri(format!("/events/{id}"))
                .header(http::header::AUTHORIZATION, format!("Bearer {token}"))
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
        let token = jwt_test_token(&state, "test-client", vec![Scope::ReadAll]);
        let mut app = state.into_router();

        let response = get_help(event.id.as_str(), &token, &mut app).await;

        assert_eq!(response.status(), StatusCode::OK);

        let body = response.into_body().collect().await.unwrap().to_bytes();
        let db_event: Event = serde_json::from_slice(&body).unwrap();

        assert_eq!(event, db_event);
    }

    #[sqlx::test(fixtures("programs"))]
    async fn delete(db: PgPool) {
        let event1 = EventRequest {
            program_id: ProgramId::new("program-1").unwrap(),
            event_name: Some("event1".to_string()),
            ..default_event_content()
        };
        let event2 = EventRequest {
            program_id: ProgramId::new("program-2").unwrap(),
            event_name: Some("event2".to_string()),
            ..default_event_content()
        };
        let event3 = EventRequest {
            program_id: ProgramId::new("program-2").unwrap(),
            event_name: Some("event3".to_string()),
            ..default_event_content()
        };

        let (state, events) = state_with_events(vec![event1, event2.clone(), event3], db).await;
        let token = jwt_test_token(
            &state,
            "test-client",
            vec![Scope::WriteEvents, Scope::ReadAll],
        );
        let mut app = state.into_router();

        let event_id = events[1].id.clone();

        let request = Request::builder()
            .method(Method::DELETE)
            .header(http::header::AUTHORIZATION, format!("Bearer {token}"))
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
        let token = jwt_test_token(
            &state,
            "test-client",
            vec![Scope::WriteEvents, Scope::ReadAll],
        );
        let app = state.into_router();

        let response = app
            .oneshot(event_request(Method::PUT, event.clone(), &token))
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
        content: &EventRequest,
        token: &str,
    ) -> (StatusCode, String) {
        let request = Request::builder()
            .method(Method::POST)
            .uri("/events")
            .header(http::header::AUTHORIZATION, format!("Bearer {token}"))
            .header(http::header::CONTENT_TYPE, mime::APPLICATION_JSON.as_ref())
            .body(Body::from(serde_json::to_vec(content).unwrap()))
            .unwrap();

        let response = ServiceExt::<Request<Body>>::ready(&mut app)
            .await
            .unwrap()
            .call(request)
            .await
            .unwrap();

        let status = response.status();

        let body = String::from_utf8(
            response
                .into_body()
                .collect()
                .await
                .unwrap()
                .to_bytes()
                .to_vec(),
        )
        .unwrap();
        println!("Response body: {}", body);

        (status, body)
    }

    #[sqlx::test(fixtures("programs"))]
    async fn create_same_name(db: PgPool) {
        let (state, _) = state_with_events(vec![], db).await;
        let token = jwt_test_token(
            &state,
            "test-client",
            vec![Scope::WriteEvents, Scope::ReadAll],
        );

        let mut app = state.into_router();

        let content = default_event_content();

        let (status, _) = help_create_event(&mut app, &content, &token).await;
        assert_eq!(status, StatusCode::CREATED);

        let (status, _) = help_create_event(&mut app, &content, &token).await;
        assert_eq!(status, StatusCode::CREATED);
    }

    async fn retrieve_all_with_filter_help(
        app: &mut Router,
        query_params: &str,
        token: &str,
    ) -> Response<Body> {
        let request = Request::builder()
            .method(Method::GET)
            .uri(format!("/events?{query_params}"))
            .header(http::header::AUTHORIZATION, format!("Bearer {token}"))
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

    #[sqlx::test(fixtures("programs", "vens"))]
    async fn retrieve_all_with_filter(db: PgPool) {
        let event1 = EventRequest {
            program_id: ProgramId::new("program-1").unwrap(),
            event_name: Some("event1".to_string()),
            targets: vec![Target::from_str("private-1").unwrap()],
            ..default_event_content()
        };
        let event2 = EventRequest {
            program_id: ProgramId::new("program-2").unwrap(),
            event_name: Some("event2".to_string()),
            targets: vec![
                Target::from_str("group-1").unwrap(),
                Target::from_str("group-2").unwrap(),
            ],
            ..default_event_content()
        };
        let event3 = EventRequest {
            program_id: ProgramId::new("program-2").unwrap(),
            event_name: Some("event3".to_string()),
            targets: vec![Target::from_str("group-1").unwrap()],
            ..default_event_content()
        };

        let test = ApiTest::new(db, "test-client", vec![Scope::WriteEvents, Scope::ReadAll]).await;

        for event in [event1, event2, event3] {
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
        let (status, events) = test
            .request::<Vec<Event>>(Method::GET, "/events?targets=nonsense", Body::empty())
            .await;
        assert_eq!(status, StatusCode::OK);
        dbg!(&events);
        assert_eq!(events.len(), 0);

        let (status, events) = test
            .request::<Vec<Event>>(Method::GET, "/events?targets=group-1", Body::empty())
            .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(events.len(), 2);

        let (status, events) = test
            .request::<Vec<Event>>(
                Method::GET,
                "/events?targets=group-1&targets=group-2",
                Body::empty(),
            )
            .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(events.len(), 1);

        let (status, events) = test
            .request::<Vec<Event>>(Method::GET, "/events?programID=program-1", Body::empty())
            .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(events.len(), 1);
    }

    #[ignore = "Depends on https://github.com/oadr3-org/openadr3-vtn-reference-implementation/issues/104"]
    #[sqlx::test]
    async fn name_constraint_validation(db: PgPool) {
        let test = ApiTest::new(db, "test-client", vec![Scope::WriteEvents, Scope::ReadAll]).await;

        let events = [
            EventRequest {
                event_name: Some("".to_string()),
                ..default_event_content()
            },
            EventRequest {
                event_name: Some("This is more than 128 characters long and should be rejected This is more than 128 characters long and should be rejected asdfasd".to_string()),
                ..default_event_content()
            },
            EventRequest {
                payload_descriptors: Some(vec![
                    EventPayloadDescriptor{
                        payload_type: EventType::Private("".to_string()),
                        units: None,
                        currency: None,
                    }
                ]),
                ..default_event_content()
            },
            EventRequest {
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
                    Method::POST,
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
        let test = ApiTest::new(db, "test-client", vec![Scope::WriteEvents, Scope::ReadAll]).await;

        let events = vec![
            EventRequest {
                priority: Priority::MAX,
                ..default_event_content()
            },
            EventRequest {
                priority: Priority::MIN,
                ..default_event_content()
            },
            EventRequest {
                priority: Priority::new(32),
                ..default_event_content()
            },
            EventRequest {
                priority: Priority::new(33),
                ..default_event_content()
            },
            EventRequest {
                priority: Priority::UNSPECIFIED,
                ..default_event_content()
            },
            EventRequest {
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

        #[sqlx::test(fixtures("users", "programs", "events"))]
        async fn business_can_write_event_in_own_program_only(db: PgPool) {
            let (state, _) = state_with_events(vec![], db).await;
            let mut app = state.clone().into_router();

            let content = EventRequest {
                program_id: "program-3".parse().unwrap(),
                ..default_event_content()
            };

            let token = jwt_test_token(
                &state,
                "test-client",
                vec![Scope::WriteEvents, Scope::ReadAll],
            );
            let (status, _) = help_create_event(&mut app, &content, &token).await;
            assert_eq!(status, StatusCode::CREATED);

            let token = jwt_test_token(
                &state,
                "test-client",
                vec![Scope::WriteEvents, Scope::ReadAll],
            );
            let (status, _) = help_create_event(&mut app, &content, &token).await;
            assert_eq!(status, StatusCode::UNAUTHORIZED);

            let token = jwt_test_token(
                &state,
                "test-client",
                vec![Scope::WriteEvents, Scope::ReadAll],
            );
            let (status, _) = help_create_event(&mut app, &content, &token).await;
            assert_eq!(status, StatusCode::CREATED);

            let token = jwt_test_token(
                &state,
                "test-client",
                vec![Scope::WriteEvents, Scope::ReadAll],
            );
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method(Method::DELETE)
                        .uri(format!("/events/{}", "event-3"))
                        .header(http::header::AUTHORIZATION, format!("Bearer {token}"))
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

            let token = jwt_test_token(
                &state,
                "test-client",
                vec![Scope::WriteEvents, Scope::ReadAll],
            );
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method(Method::PUT)
                        .uri(format!("/events/{}", "event-3"))
                        .header(http::header::AUTHORIZATION, format!("Bearer {token}"))
                        .header(http::header::CONTENT_TYPE, mime::APPLICATION_JSON.as_ref())
                        .body(Body::from(serde_json::to_vec(&content).unwrap()))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

            let token = jwt_test_token(
                &state,
                "test-client",
                vec![Scope::WriteEvents, Scope::ReadAll],
            );
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method(Method::PUT)
                        .uri(format!("/events/{}", "event-3"))
                        .header(http::header::AUTHORIZATION, format!("Bearer {token}"))
                        .header(http::header::CONTENT_TYPE, mime::APPLICATION_JSON.as_ref())
                        .body(Body::from(serde_json::to_vec(&content).unwrap()))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);

            let token = jwt_test_token(
                &state,
                "test-client",
                vec![Scope::WriteEvents, Scope::ReadAll],
            );
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method(Method::DELETE)
                        .uri(format!("/events/{}", "event-3"))
                        .header(http::header::AUTHORIZATION, format!("Bearer {token}"))
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
        }

        #[sqlx::test(fixtures("users", "programs", "events", "vens"))]
        async fn vens_can_read_event_in_assigned_program_only(db: PgPool) {
            // FIXME properly test object privacy
            let (state, _) = state_with_events(vec![], db).await;
            let mut app = state.clone().into_router();

            let token = jwt_test_token(&state, "test-client", vec![Scope::ReadTargets]);
            let response = get_help("event-3", &token, &mut app).await;
            assert_eq!(response.status(), StatusCode::OK);

            let token = jwt_test_token(&state, "test-client", vec![Scope::ReadTargets]);
            let response = get_help("event-3", &token, &mut app).await;
            assert_eq!(response.status(), StatusCode::NOT_FOUND);

            let token = jwt_test_token(&state, "test-client", vec![Scope::ReadTargets]);
            let response = get_help("event-3", &token, &mut app).await;
            assert_eq!(response.status(), StatusCode::OK);

            let token = jwt_test_token(
                &state,
                "ven-2-client-id",
                vec![Scope::WriteEvents, Scope::ReadTargets],
            );
            let response = get_help("event-3", &token, &mut app).await;
            assert_eq!(response.status(), StatusCode::NOT_FOUND);
        }

        #[sqlx::test(fixtures("users", "programs", "events", "vens"))]
        async fn vens_event_list_assigned_program_only(db: PgPool) {
            let (state, _) = state_with_events(vec![], db).await;
            let mut app = state.clone().into_router();

            let token = jwt_test_token(&state, "ven-1-client-id", vec![Scope::ReadTargets]);
            let response = retrieve_all_with_filter_help(&mut app, "", &token).await;
            assert_eq!(response.status(), StatusCode::OK);
            let body = response.into_body().collect().await.unwrap().to_bytes();
            let events: Vec<Event> = serde_json::from_slice(&body).unwrap();
            assert_eq!(events.len(), 2);

            let token = jwt_test_token(&state, "ven-1-client-id", vec![Scope::ReadTargets]);
            let response = retrieve_all_with_filter_help(&mut app, "", &token).await;
            assert_eq!(response.status(), StatusCode::OK);
            let body = response.into_body().collect().await.unwrap().to_bytes();
            let events: Vec<Event> = serde_json::from_slice(&body).unwrap();
            assert_eq!(events.len(), 3);

            // VEN should not be able to filter on other ven names,
            // even if they have a common set of events,
            // as this would leak information about which events the VENs have in common.
            let token = jwt_test_token(&state, "ven-1-client-id", vec![Scope::ReadTargets]);
            let response =
                retrieve_all_with_filter_help(&mut app, "targets=ven-2-name", &token).await;
            assert_eq!(response.status(), StatusCode::OK);
            let body = response.into_body().collect().await.unwrap().to_bytes();
            let events: Vec<Event> = serde_json::from_slice(&body).unwrap();
            assert_eq!(events.len(), 0);
        }

        #[sqlx::test(fixtures("users", "programs", "events", "vens"))]
        async fn ven_cannot_write_event(db: PgPool) {
            let (state, _) = state_with_events(vec![], db).await;
            let mut app = state.clone().into_router();

            let token = jwt_test_token(
                &state,
                "ven-1-client-id",
                vec![Scope::ReadTargets, Scope::WriteVens],
            );
            let (status, _) = help_create_event(&mut app, &default_event_content(), &token).await;
            assert_eq!(status, StatusCode::FORBIDDEN);

            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method(Method::DELETE)
                        .uri(format!("/events/{}", "event-3"))
                        .header(http::header::AUTHORIZATION, format!("Bearer {token}"))
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
                        .method(Method::PUT)
                        .uri(format!("/events/{}", "event-3"))
                        .header(http::header::AUTHORIZATION, format!("Bearer {token}"))
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
