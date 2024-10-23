use axum::http::StatusCode;
use openadr_client::{Error, Filter, PaginationOptions};
use openadr_wire::{
    program::ProgramContent,
    target::{TargetEntry, TargetLabel, TargetMap},
};
use sqlx::PgPool;

mod common;

fn default_content() -> ProgramContent {
    ProgramContent {
        program_name: "program_name".to_string(),
        program_long_name: Some("program_long_name".to_string()),
        retailer_name: Some("retailer_name".to_string()),
        retailer_long_name: Some("retailer_long_name".to_string()),
        program_type: None,
        country: None,
        principal_subdivision: None,
        time_zone_offset: None,
        interval_period: None,
        program_descriptions: None,
        binding_events: None,
        local_price: None,
        payload_descriptors: None,
        targets: None,
    }
}

#[sqlx::test(fixtures("users"))]
async fn get(db: PgPool) {
    let client = common::setup_client(db).await;
    let program_client = client.create_program(default_content()).await.unwrap();

    assert_eq!(program_client.content(), &default_content());
}

#[sqlx::test(fixtures("users"))]
async fn delete(db: PgPool) {
    let client = common::setup_client(db).await;

    let program1 = ProgramContent {
        program_name: "program1".to_string(),
        ..default_content()
    };
    let program2 = ProgramContent {
        program_name: "program2".to_string(),
        ..default_content()
    };
    let program3 = ProgramContent {
        program_name: "program3".to_string(),
        ..default_content()
    };

    let mut ids = vec![];
    for content in [program1, program2.clone(), program3] {
        ids.push(client.create_program(content).await.unwrap());
    }

    let program = client.get_program_by_id(ids[1].id()).await.unwrap();
    assert_eq!(program.content(), &program2);

    let removed = program.delete().await.unwrap();
    assert_eq!(removed.content, program2);

    let programs = client.get_program_list(Filter::None).await.unwrap();
    assert_eq!(programs.len(), 2);
}

#[sqlx::test(fixtures("users"))]
async fn update(db: PgPool) {
    let client = common::setup_client(db).await;

    let program1 = ProgramContent {
        program_name: "program1".to_string(),
        ..default_content()
    };

    let mut program = client.create_program(program1).await.unwrap();
    let creation_date_time = program.modification_date_time();

    let program2 = ProgramContent {
        program_name: "program1".to_string(),
        country: Some("NO".to_string()),
        ..default_content()
    };

    *program.content_mut() = program2.clone();
    program.update().await.unwrap();

    assert_eq!(program.content(), &program2);
    assert!(program.modification_date_time() > creation_date_time);
}

#[sqlx::test(fixtures("users"))]
async fn update_same_name(db: PgPool) {
    let client = common::setup_client(db).await;

    let program1 = ProgramContent {
        program_name: "program1".to_string(),
        ..default_content()
    };

    let program2 = ProgramContent {
        program_name: "program2".to_string(),
        ..default_content()
    };

    let _program1 = client.create_program(program1).await.unwrap();
    let mut program2 = client.create_program(program2).await.unwrap();
    let creation_date_time = program2.modification_date_time();

    let content = ProgramContent {
        program_name: "program1".to_string(),
        country: Some("NO".to_string()),
        ..default_content()
    };

    *program2.content_mut() = content;

    let Error::Problem(problem) = program2.update().await.unwrap_err() else {
        unreachable!()
    };

    assert_eq!(problem.status, StatusCode::CONFLICT);
    assert_eq!(program2.modification_date_time(), creation_date_time);
}

#[sqlx::test(fixtures("users"))]
async fn create_same_name(db: PgPool) {
    let client = common::setup_client(db).await;

    let program1 = ProgramContent {
        program_name: "program1".to_string(),
        ..default_content()
    };

    let _ = client.create_program(program1.clone()).await.unwrap();
    let Error::Problem(problem) = client.create_program(program1).await.unwrap_err() else {
        unreachable!()
    };

    assert_eq!(problem.status, StatusCode::CONFLICT);
}

#[sqlx::test(fixtures("users"))]
async fn retrieve_all_with_filter(db: PgPool) {
    let client = common::setup_client(db).await;

    let program1 = ProgramContent {
        program_name: "program1".to_string(),
        ..default_content()
    };
    let program2 = ProgramContent {
        program_name: "program2".to_string(),
        targets: Some(TargetMap(vec![TargetEntry {
            label: TargetLabel::Group,
            values: ["Group 2".to_string()],
        }])),
        ..default_content()
    };
    let program3 = ProgramContent {
        program_name: "program3".to_string(),
        targets: Some(TargetMap(vec![TargetEntry {
            label: TargetLabel::Group,
            values: ["Group 1".to_string()],
        }])),
        ..default_content()
    };

    for content in [program1, program2, program3] {
        let _ = client.create_program(content).await.unwrap();
    }

    let programs = client
        .get_programs(Filter::None, PaginationOptions { skip: 0, limit: 50 })
        .await
        .unwrap();
    assert_eq!(programs.len(), 3);

    // skip
    let programs = client
        .get_programs(Filter::None, PaginationOptions { skip: 1, limit: 50 })
        .await
        .unwrap();
    assert_eq!(programs.len(), 2);

    // limit
    let programs = client
        .get_programs(Filter::None, PaginationOptions { skip: 0, limit: 2 })
        .await
        .unwrap();
    assert_eq!(programs.len(), 2);

    // program name
    let err = client
        .get_programs(
            Filter::By(TargetLabel::Private("NONSENSE".to_string()), &[]),
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

    let err = client
        .get_programs(
            Filter::By(TargetLabel::Private("NONSENSE".to_string()), &[""]),
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

    let programs = client
        .get_programs(
            Filter::By(TargetLabel::Private("NONSENSE".to_string()), &["test"]),
            PaginationOptions { skip: 0, limit: 50 },
        )
        .await
        .unwrap();
    assert_eq!(programs.len(), 0);

    let programs = client
        .get_programs(
            Filter::By(TargetLabel::Group, &["Group 1", "Group 2"]),
            PaginationOptions { skip: 0, limit: 50 },
        )
        .await
        .unwrap();
    assert_eq!(programs.len(), 2);

    let programs = client
        .get_programs(
            Filter::By(TargetLabel::Group, &["Group 1"]),
            PaginationOptions { skip: 0, limit: 50 },
        )
        .await
        .unwrap();
    assert_eq!(programs.len(), 1);

    let programs = client
        .get_programs(
            Filter::By(TargetLabel::Group, &["Not existent"]),
            PaginationOptions { skip: 0, limit: 50 },
        )
        .await
        .unwrap();
    assert_eq!(programs.len(), 0);
}
