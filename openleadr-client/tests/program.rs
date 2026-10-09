use axum::http::StatusCode;
use openleadr_client::{Error, Filter, PaginationOptions, VirtualEndNode};
use openleadr_wire::{program::ProgramRequest, target::Target};
use serial_test::serial;
use sqlx::PgPool;
use std::str::FromStr;

mod common;

fn default_content() -> ProgramRequest {
    ProgramRequest {
        program_name: "program_name".to_string(),
        interval_period: None,
        program_descriptions: None,
        payload_descriptors: None,
        attributes: None,
        targets: vec![],
    }
}

#[tokio::test]
#[serial]
async fn program_crud() {
    let ctx = common::setup::<openleadr_client::BusinessLogic>(common::AuthRole::Bl).await;
    let original_name = "program-crud-test";
    let updated_name = "program-crud-test-updated";

    // Cleanup resources left behind by an interrupted earlier run without
    // assuming that the VTN is otherwise empty.
    if let Ok(programs) = ctx.get_program_list(Filter::none()).await {
        for program in programs {
            if [original_name, updated_name].contains(&program.content().program_name.as_str()) {
                program.delete().await.unwrap();
            }
        }
    }

    let content = ProgramRequest {
        program_name: original_name.to_string(),
        ..default_content()
    };

    // Create.
    let created = ctx.create_program(content.clone()).await.unwrap();
    assert_eq!(created.content(), &content);

    // A duplicate name is rejected.
    let err = ctx.create_program(content).await.unwrap_err();
    assert!(err.is_conflict());

    // Retrieve by ID without relying on global VTN contents.
    let mut program = ctx.get_program_by_id(created.id()).await.unwrap();
    assert_eq!(program.content(), created.content());

    // Update.
    program.content_mut().program_name = updated_name.to_string();
    program.update().await.unwrap();
    assert_eq!(program.content().program_name, updated_name);

    let updated = ctx.get_program_by_id(program.id()).await.unwrap();
    assert_eq!(updated.content().program_name, updated_name);

    // Delete and verify that the resource is gone.
    let id = program.id().clone();
    program.delete().await.unwrap();
    let err = ctx.get_program_by_id(&id).await.unwrap_err();
    assert!(err.is_not_found());
}

#[tokio::test]
#[serial]
async fn delete() {
    let ctx = common::setup::<openleadr_client::BusinessLogic>(common::AuthRole::Bl).await;
    let names = [
        "program-delete-test-1",
        "program-delete-test-2",
        "program-delete-test-3",
    ];

    // Cleanup only this test's namespace so the case remains valid on a
    // shared or third-party VTN with unrelated programs already present.
    if let Ok(existing) = ctx.get_program_list(Filter::none()).await {
        for program in existing {
            if names.contains(&program.content().program_name.as_str()) {
                program.delete().await.unwrap();
            }
        }
    }

    let mut programs = Vec::new();
    for name in names {
        let content = ProgramRequest {
            program_name: name.to_string(),
            ..default_content()
        };
        programs.push(ctx.create_program(content).await.unwrap());
    }

    let id = programs[1].id().clone();
    let expected = programs[1].content().clone();
    let program = ctx.get_program_by_id(&id).await.unwrap();
    assert_eq!(program.content(), &expected);

    let removed = program.delete().await.unwrap();
    assert_eq!(removed.content, expected);

    let err = ctx.get_program_by_id(&id).await.unwrap_err();
    assert!(err.is_not_found());

    // Explicit cleanup without asserting global collection size.
    programs.remove(2).delete().await.unwrap();
    programs.remove(0).delete().await.unwrap();
}

#[sqlx::test(fixtures("users"))]
async fn update(db: PgPool) {
    let client = common::setup_client::<VirtualEndNode>(db).await;

    let program1 = ProgramRequest {
        program_name: "program1".to_string(),
        ..default_content()
    };

    let mut program = client.create_program(program1).await.unwrap();
    let creation_date_time = program.modification_date_time();

    let program2 = ProgramRequest {
        program_name: "program1".to_string(),
        ..default_content()
    };

    *program.content_mut() = program2.clone();
    program.update().await.unwrap();

    assert_eq!(program.content(), &program2);
    assert!(program.modification_date_time() > creation_date_time);
}

#[tokio::test]
#[serial]
async fn update_same_name() {
    let ctx = common::setup::<openleadr_client::BusinessLogic>(common::AuthRole::Bl).await;
    let first_name = "program-update-conflict-test-1";
    let second_name = "program-update-conflict-test-2";

    // Keep the case independent of unrelated state in a shared VTN.
    if let Ok(existing) = ctx.get_program_list(Filter::none()).await {
        for program in existing {
            if [first_name, second_name].contains(&program.content().program_name.as_str()) {
                program.delete().await.unwrap();
            }
        }
    }

    let first = ctx
        .create_program(ProgramRequest {
            program_name: first_name.to_string(),
            ..default_content()
        })
        .await
        .unwrap();

    let mut second = ctx
        .create_program(ProgramRequest {
            program_name: second_name.to_string(),
            ..default_content()
        })
        .await
        .unwrap();

    let second_id = second.id().clone();
    let before = ctx.get_program_by_id(&second_id).await.unwrap();
    let before_modified = before.modification_date_time();

    second.content_mut().program_name = first_name.to_string();
    let err = second.update().await.unwrap_err();
    assert!(err.is_conflict());

    // The rejected update must not leak into authoritative VTN state.
    let after = ctx.get_program_by_id(&second_id).await.unwrap();
    assert_eq!(after.content().program_name, second_name);
    assert_eq!(after.modification_date_time(), before_modified);

    first.delete().await.unwrap();
    after.delete().await.unwrap();
}

#[sqlx::test(fixtures("users"))]
async fn create_same_name(db: PgPool) {
    let client = common::setup_client::<VirtualEndNode>(db).await;

    let program1 = ProgramRequest {
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
    let client = common::setup_client::<VirtualEndNode>(db).await;

    let program1 = ProgramRequest {
        program_name: "program1".to_string(),
        ..default_content()
    };
    let program2 = ProgramRequest {
        program_name: "program2".to_string(),
        targets: vec![Target::from_str("group-2").unwrap()],
        ..default_content()
    };
    let program3 = ProgramRequest {
        program_name: "program3".to_string(),
        targets: vec![Target::from_str("group-1").unwrap()],
        ..default_content()
    };
    let program4 = ProgramRequest {
        program_name: "program4".to_string(),
        targets: vec![
            Target::from_str("group-1").unwrap(),
            Target::from_str("group-3").unwrap(),
        ],
        ..default_content()
    };

    for content in [program1, program2, program3, program4] {
        let _ = client.create_program(content).await.unwrap();
    }

    let programs = client
        .get_programs(Filter::none(), PaginationOptions { skip: 0, limit: 50 })
        .await
        .unwrap();
    assert_eq!(programs.len(), 4);

    // skip
    let programs = client
        .get_programs(Filter::none(), PaginationOptions { skip: 1, limit: 50 })
        .await
        .unwrap();
    assert_eq!(programs.len(), 3);

    // limit
    let programs = client
        .get_programs(Filter::none(), PaginationOptions { skip: 0, limit: 2 })
        .await
        .unwrap();
    assert_eq!(programs.len(), 2);

    let programs = client
        .get_programs(
            Filter::By(&["test"]),
            PaginationOptions { skip: 0, limit: 50 },
        )
        .await
        .unwrap();
    assert_eq!(programs.len(), 0);

    let programs = client
        .get_programs(
            Filter::By(&["group-1", "group-2"]),
            PaginationOptions { skip: 0, limit: 50 },
        )
        .await
        .unwrap();
    assert_eq!(programs.len(), 3);

    let programs = client
        .get_programs(
            Filter::By(&["group-1", "group-3"]),
            PaginationOptions { skip: 0, limit: 50 },
        )
        .await
        .unwrap();
    assert_eq!(programs.len(), 2);

    let programs = client
        .get_programs(
            Filter::By(&["group-2", "group-3"]),
            PaginationOptions { skip: 0, limit: 50 },
        )
        .await
        .unwrap();
    assert_eq!(programs.len(), 2);

    let programs = client
        .get_programs(
            Filter::By(&["group-3"]),
            PaginationOptions { skip: 0, limit: 50 },
        )
        .await
        .unwrap();
    assert_eq!(programs.len(), 1);

    let programs = client
        .get_programs(
            Filter::By(&["group-1"]),
            PaginationOptions { skip: 0, limit: 50 },
        )
        .await
        .unwrap();
    assert_eq!(programs.len(), 2);

    let programs = client
        .get_programs(
            Filter::By(&["group-2"]),
            PaginationOptions { skip: 0, limit: 50 },
        )
        .await
        .unwrap();
    assert_eq!(programs.len(), 1);

    let programs = client
        .get_programs(
            Filter::By(&["not-existent"]),
            PaginationOptions { skip: 0, limit: 50 },
        )
        .await
        .unwrap();
    assert_eq!(programs.len(), 0);
}
