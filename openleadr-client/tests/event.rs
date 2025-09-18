use axum::http::StatusCode;
use openleadr_client::{Error, Filter, PaginationOptions};
use openleadr_wire::{
    event::{EventContent, EventInterval, EventType, EventValuesMap, Priority},
    program::{ProgramContent, ProgramId},
    target::Target,
    values_map::Value,
};
use sqlx::PgPool;

mod common;

fn default_content(program_id: &ProgramId) -> EventContent {
    EventContent {
        program_id: program_id.clone(),
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

#[sqlx::test(fixtures("users"))]
async fn get(db: PgPool) {
    let client = common::setup_program_client("program", db).await;
    let event_content = default_content(client.id());
    let event_client = client.create_event(event_content.clone()).await.unwrap();

    assert_eq!(event_client.content(), &event_content);
}

#[sqlx::test(fixtures("users"))]
async fn delete(db: PgPool) {
    let client = common::setup_program_client("program", db).await;

    let event1 = EventContent {
        event_name: Some("event1".to_string()),
        ..default_content(client.id())
    };
    let event2 = EventContent {
        event_name: Some("event2".to_string()),
        ..default_content(client.id())
    };
    let event3 = EventContent {
        event_name: Some("event3".to_string()),
        ..default_content(client.id())
    };

    let mut ids = vec![];
    for content in [event1.clone(), event2.clone(), event3] {
        ids.push(client.create_event(content).await.unwrap());
    }

    let pagination = PaginationOptions { skip: 0, limit: 3 };
    let mut events = client
        .get_events_request(Filter::none(), pagination)
        .await
        .unwrap();
    assert_eq!(events.len(), 3);
    let event = events.pop().unwrap();
    assert_eq!(event.content(), &event1);
    let event = events.pop().unwrap();
    assert_eq!(event.content(), &event2);

    let removed = event.delete().await.unwrap();
    assert_eq!(removed.content, event2);

    let events = client.get_event_list(Filter::none()).await.unwrap();
    assert_eq!(events.len(), 2);
}

#[sqlx::test(fixtures("users"))]
async fn update(db: PgPool) {
    let client = common::setup_program_client("program", db).await;

    let event1 = EventContent {
        event_name: Some("event1".to_string()),
        ..default_content(client.id())
    };

    let mut event = client.create_event(event1).await.unwrap();
    let creation_date_time = event.modification_date_time();

    let event2 = EventContent {
        event_name: Some("event1".to_string()),
        priority: Priority::MIN,
        ..default_content(client.id())
    };

    *event.content_mut() = event2.clone();
    event.update().await.unwrap();

    assert_eq!(event.content(), &event2);
    assert!(event.modification_date_time() > creation_date_time);
}

#[sqlx::test(fixtures("users"))]
async fn update_same_name(db: PgPool) {
    let client = common::setup_program_client("program", db).await;

    let event1 = EventContent {
        event_name: Some("event1".to_string()),
        ..default_content(client.id())
    };

    let event2 = EventContent {
        event_name: Some("event2".to_string()),
        ..default_content(client.id())
    };

    let _event1 = client.create_event(event1).await.unwrap();
    let mut event2 = client.create_event(event2).await.unwrap();
    let creation_date_time = event2.modification_date_time();

    let content = EventContent {
        event_name: Some("event1".to_string()),
        priority: Priority::MIN,
        ..default_content(client.id())
    };

    // duplicate event names are fine
    *event2.content_mut() = content;
    event2.update().await.unwrap();

    assert!(event2.modification_date_time() > creation_date_time);
}

#[sqlx::test(fixtures("users"))]
async fn create_same_name(db: PgPool) {
    let client = common::setup_program_client("program", db).await;

    let event1 = EventContent {
        event_name: Some("event1".to_string()),
        ..default_content(client.id())
    };

    // duplicate event names are fine
    let _ = client.create_event(event1.clone()).await.unwrap();
    let _ = client.create_event(event1).await.unwrap();
}

#[sqlx::test(fixtures("users"))]
async fn retrieve_all_with_filter(db: PgPool) {
    let client = common::setup_program_client("program1", db).await;

    let event1 = EventContent {
        program_id: client.id().clone(),
        event_name: Some("event1".to_string()),
        ..default_content(client.id())
    };
    let event2 = EventContent {
        program_id: client.id().clone(),
        event_name: Some("event2".to_string()),
        targets: Some(vec![Target::new("group-2").unwrap()]),
        ..default_content(client.id())
    };
    let event3 = EventContent {
        program_id: client.id().clone(),
        event_name: Some("event3".to_string()),
        targets: Some(vec![Target::new("group-1").unwrap()]),
        ..default_content(client.id())
    };
    let event4 = EventContent {
        program_id: client.id().clone(),
        event_name: Some("event4".to_string()),
        targets: Some(vec![
            Target::new("group-1").unwrap(),
            Target::new("group-3").unwrap(),
        ]),
        ..default_content(client.id())
    };

    for content in [event1, event2, event3, event4] {
        let _ = client.create_event(content).await.unwrap();
    }

    let events = client
        .get_events_request(Filter::none(), PaginationOptions { skip: 0, limit: 50 })
        .await
        .unwrap();
    assert_eq!(events.len(), 4);

    // skip
    let events = client
        .get_events_request(Filter::none(), PaginationOptions { skip: 1, limit: 50 })
        .await
        .unwrap();
    assert_eq!(events.len(), 3);

    // limit
    let events = client
        .get_events_request(Filter::none(), PaginationOptions { skip: 0, limit: 2 })
        .await
        .unwrap();
    assert_eq!(events.len(), 2);

    // event name
    let events = client
        .get_events_request(
            Filter::By(&["test"]),
            PaginationOptions { skip: 0, limit: 2 },
        )
        .await
        .unwrap();
    assert_eq!(events.len(), 0);

    let err = client
        .get_events_request(Filter::By(&[""]), PaginationOptions { skip: 0, limit: 2 })
        .await
        .unwrap_err();
    let Error::Problem(problem) = err else {
        unreachable!()
    };
    assert_eq!(
        problem.status,
        StatusCode::BAD_REQUEST,
        "Do return BAD_REQUEST on empty targets"
    );

    let events = client
        .get_events_request(
            Filter::By(&["group-1", "group-2"]),
            PaginationOptions { skip: 0, limit: 50 },
        )
        .await
        .unwrap();
    assert_eq!(events.len(), 3);

    let events = client
        .get_events_request(
            Filter::By(&["group-1", "group-3"]),
            PaginationOptions { skip: 0, limit: 50 },
        )
        .await
        .unwrap();
    assert_eq!(events.len(), 2);

    let events = client
        .get_events_request(
            Filter::By(&["group-2", "group-3"]),
            PaginationOptions { skip: 0, limit: 50 },
        )
        .await
        .unwrap();
    assert_eq!(events.len(), 2);

    let events = client
        .get_events_request(
            Filter::By(&["group-3"]),
            PaginationOptions { skip: 0, limit: 50 },
        )
        .await
        .unwrap();
    assert_eq!(events.len(), 1);

    let events = client
        .get_events_request(
            Filter::By(&["group-1"]),
            PaginationOptions { skip: 0, limit: 50 },
        )
        .await
        .unwrap();
    assert_eq!(events.len(), 2);

    let events = client
        .get_events_request(
            Filter::By(&["group-2"]),
            PaginationOptions { skip: 0, limit: 50 },
        )
        .await
        .unwrap();
    assert_eq!(events.len(), 1);

    let events = client
        .get_events_request(
            Filter::By(&["Not existent"]),
            PaginationOptions { skip: 0, limit: 50 },
        )
        .await
        .unwrap();
    assert_eq!(events.len(), 0);
}

#[sqlx::test(fixtures("users"))]
async fn get_program_events(db: PgPool) {
    let client = common::setup_client(db).await;

    let program1 = client
        .create_program(ProgramContent::new("program1"))
        .await
        .unwrap();
    let program2 = client
        .create_program(ProgramContent::new("program2"))
        .await
        .unwrap();

    let event1 = EventContent {
        event_name: Some("event".to_string()),
        priority: Priority::MAX,
        ..default_content(program1.id())
    };
    let event2 = EventContent {
        event_name: Some("event".to_string()),
        priority: Priority::MIN,
        ..default_content(program2.id())
    };

    program1.create_event(event1.clone()).await.unwrap();
    program2.create_event(event2.clone()).await.unwrap();

    let events1 = program1.get_event_list(Filter::none()).await.unwrap();
    let events2 = program2.get_event_list(Filter::none()).await.unwrap();

    assert_eq!(events1.len(), 1);
    assert_eq!(events2.len(), 1);

    assert_eq!(events1[0].content(), &event1);
    assert_eq!(events2[0].content(), &event2);
}

#[sqlx::test(fixtures("users"))]
async fn filter_constraint_violation(db: PgPool) {
    let client = common::setup_client(db).await;

    let err = client
        .get_events(
            None,
            Filter::none(),
            PaginationOptions { skip: 0, limit: 51 },
        )
        .await
        .unwrap_err();
    let Error::Problem(problem) = err else {
        unreachable!()
    };
    assert_eq!(problem.status, StatusCode::BAD_REQUEST);

    let err = client
        .get_events(
            None,
            Filter::none(),
            PaginationOptions { skip: 0, limit: 0 },
        )
        .await
        .unwrap_err();
    let Error::Problem(problem) = err else {
        unreachable!()
    };
    assert_eq!(problem.status, StatusCode::BAD_REQUEST);
}
