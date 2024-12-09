use crate::common::setup;
use axum::http::StatusCode;
use openleadr_client::{Error, Filter, PaginationOptions};
use openleadr_vtn::jwt::AuthRole;
use openleadr_wire::{
    event::{EventContent, EventInterval, EventType, EventValuesMap, Priority},
    program::{ProgramContent, ProgramId},
    target::{TargetEntry, TargetMap, TargetType},
    values_map::Value,
};
use serial_test::serial;
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

#[tokio::test]
#[serial]
async fn crud() {
    let ctx = setup(AuthRole::AnyBusiness).await;
    let new_program = ProgramContent::new("test-program-name".to_string());
    let program = ctx.create_program(new_program).await.unwrap();

    // Create
    let event_content = EventContent {
        event_name: Some("event1".to_string()),
        ..default_content(program.id())
    };
    let event = program.create_event(event_content.clone()).await.unwrap();
    assert_eq!(event.content().event_name, event_content.event_name);

    // Creating a second event with the same name succeeds
    {
        let event_2 = program.create_event(event_content.clone()).await.unwrap();
        assert_eq!(event_2.content().event_name, event_content.event_name);
        event_2.delete().await.unwrap();
    }

    // Retrieve
    {
        let event_content_2 = EventContent {
            program_id: program.id().clone(),
            event_name: Some("event2".to_string()),
            targets: Some(TargetMap(vec![TargetEntry {
                label: TargetType::Group,
                values: ["Group 2".to_string()],
            }])),
            ..default_content(program.id())
        };
        let event_2 = program.create_event(event_content_2.clone()).await.unwrap();

        let event_content_3 = EventContent {
            program_id: program.id().clone(),
            event_name: Some("event3".to_string()),
            targets: Some(TargetMap(vec![TargetEntry {
                label: TargetType::Group,
                values: ["Group 1".to_string()],
            }])),
            ..default_content(program.id())
        };
        let event_3 = program.create_event(event_content_3.clone()).await.unwrap();

        let events = program
            .get_events_request(Filter::None, PaginationOptions { skip: 0, limit: 50 })
            .await
            .unwrap();
        assert_eq!(events.len(), 3);

        // skip
        let events = program
            .get_events_request(Filter::None, PaginationOptions { skip: 1, limit: 50 })
            .await
            .unwrap();
        assert_eq!(events.len(), 2);

        // limit
        let events = program
            .get_events_request(Filter::None, PaginationOptions { skip: 0, limit: 2 })
            .await
            .unwrap();
        assert_eq!(events.len(), 2);

        // event name
        let events = program
            .get_events_request(
                Filter::By(TargetType::Private("NONSENSE".to_string()), &["test"]),
                PaginationOptions { skip: 0, limit: 2 },
            )
            .await
            .unwrap();
        assert_eq!(events.len(), 0);

        let err = program
            .get_events_request(
                Filter::By(TargetType::Private("NONSENSE".to_string()), &[""]),
                PaginationOptions { skip: 0, limit: 2 },
            )
            .await
            .unwrap_err();

        let Error::Problem(problem) = err else {
            unreachable!()
        };
        assert_eq!(
            problem.status,
            StatusCode::BAD_REQUEST,
            "Do return BAD_REQUEST on empty targetValue"
        );

        let err = program
            .get_events_request(
                Filter::By(TargetType::Private("NONSENSE".to_string()), &[]),
                PaginationOptions { skip: 0, limit: 2 },
            )
            .await
            .unwrap_err();

        let Error::Problem(problem) = err else {
            unreachable!()
        };
        assert_eq!(
            problem.status,
            StatusCode::BAD_REQUEST,
            "Do return BAD_REQUEST on empty targetValue"
        );

        let events = program
            .get_events_request(
                Filter::By(TargetType::Group, &["Group 1", "Group 2"]),
                PaginationOptions { skip: 0, limit: 50 },
            )
            .await
            .unwrap();
        assert_eq!(events.len(), 2);

        let events = program
            .get_events_request(
                Filter::By(TargetType::Group, &["Group 1"]),
                PaginationOptions { skip: 0, limit: 50 },
            )
            .await
            .unwrap();
        assert_eq!(events.len(), 1);

        let events = program
            .get_events_request(
                Filter::By(TargetType::Group, &["Not existent"]),
                PaginationOptions { skip: 0, limit: 50 },
            )
            .await
            .unwrap();
        assert_eq!(events.len(), 0);

        event_2.delete().await.unwrap();
        event_3.delete().await.unwrap();
    }

    // update
    {
        let updated_name = "event1_updated";
        let updated_priority = Priority::MIN;
        let events = program.get_event_list(Filter::None).await.unwrap();
        let mut event = events.into_iter().next().unwrap();
        let creation_date_time = event.modification_date_time();

        event.content_mut().event_name = Some(updated_name.to_string());
        event.content_mut().priority = updated_priority;
        event.content_mut().targets = Some(TargetMap(vec![TargetEntry {
            label: TargetType::Group,
            values: ["Updated Group".to_string()],
        }]));
        event.update().await.unwrap();

        assert_eq!(event.content().event_name, Some(updated_name.to_string()));
        assert_eq!(event.content().priority, updated_priority);

        let updated_events = program
            .get_events_request(
                Filter::By(TargetType::Group, &["Updated Group"]),
                PaginationOptions { skip: 0, limit: 50 },
            )
            .await
            .unwrap();
        assert_eq!(updated_events.len(), 1);

        let updated_event = updated_events.into_iter().next().unwrap();

        assert_eq!(
            updated_event.content().event_name,
            event.content().event_name
        );
        assert_eq!(updated_event.content().priority, updated_priority);
        assert!(updated_event.modification_date_time() > creation_date_time);
    }

    // Cleanup
    event.delete().await.unwrap();
    program.delete().await.unwrap();
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
        .get_events_request(Filter::None, pagination)
        .await
        .unwrap();
    assert_eq!(events.len(), 3);
    let event = events.pop().unwrap();
    assert_eq!(event.content(), &event1);
    let event = events.pop().unwrap();
    assert_eq!(event.content(), &event2);

    let removed = event.delete().await.unwrap();
    assert_eq!(removed.content, event2);

    let events = client.get_event_list(Filter::None).await.unwrap();
    assert_eq!(events.len(), 2);
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

    let events1 = program1.get_event_list(Filter::None).await.unwrap();
    let events2 = program2.get_event_list(Filter::None).await.unwrap();

    assert_eq!(events1.len(), 1);
    assert_eq!(events2.len(), 1);

    assert_eq!(events1[0].content(), &event1);
    assert_eq!(events2[0].content(), &event2);
}

#[sqlx::test(fixtures("users"))]
async fn filter_constraint_violation(db: PgPool) {
    let client = common::setup_client(db).await;

    let err = client
        .get_events(None, Filter::None, PaginationOptions { skip: 0, limit: 51 })
        .await
        .unwrap_err();
    let Error::Problem(problem) = err else {
        unreachable!()
    };
    assert_eq!(problem.status, StatusCode::BAD_REQUEST);

    let err = client
        .get_events(None, Filter::None, PaginationOptions { skip: 0, limit: 0 })
        .await
        .unwrap_err();
    let Error::Problem(problem) = err else {
        unreachable!()
    };
    assert_eq!(problem.status, StatusCode::BAD_REQUEST);
}
