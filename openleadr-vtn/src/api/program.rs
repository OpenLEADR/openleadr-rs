use std::sync::Arc;

use axum::{
    extract::{Path, State},
    Json,
};
use reqwest::StatusCode;
use serde::Deserialize;
use tracing::{info, trace};
use validator::Validate;

use openleadr_wire::{
    program::{ProgramId, ProgramRequest},
    Program,
};
use openleadr_wire::target::Target;
use crate::{
    api::{AppResponse, ValidatedJson, ValidatedQuery},
    data_source::ProgramCrud,
    error::AppError,
    jwt::{Scope, User},
};

pub async fn get_all(
    State(program_source): State<Arc<dyn ProgramCrud>>,
    ValidatedQuery(query_params): ValidatedQuery<QueryParams>,
    User(user): User,
) -> AppResponse<Vec<Program>> {
    trace!(?query_params);

    let programs = if user.scope.contains(Scope::ReadAll) {
        program_source.retrieve_all(&query_params, &None).await?
    } else if user.scope.contains(Scope::ReadTargets) {
        program_source
            .retrieve_all(&query_params, &Some(user.client_id()?))
            .await?
    } else {
        return Err(AppError::Forbidden(
            "Missing 'read_all' or 'read_targets' scope",
        ));
    };

    trace!(
        client_id = user.sub,
        "retrieved {} programs",
        programs.len()
    );

    Ok(Json(programs))
}

pub async fn get(
    State(program_source): State<Arc<dyn ProgramCrud>>,
    Path(id): Path<ProgramId>,
    User(user): User,
) -> AppResponse<Program> {
    let program = if user.scope.contains(Scope::ReadAll) {
        program_source.retrieve(&id, &None).await?
    } else if user.scope.contains(Scope::ReadTargets) {
        program_source
            .retrieve(&id, &Some(user.client_id()?))
            .await?
    } else {
        return Err(AppError::Forbidden(
            "Missing 'read_all' or 'read_targets' scope",
        ));
    };

    trace!(
        %program.id,
        program.program_name=program.content.program_name,
        client_id = user.sub,
        "program retrieved"
    );

    Ok(Json(program))
}

pub async fn add(
    State(program_source): State<Arc<dyn ProgramCrud>>,
    User(user): User,
    ValidatedJson(new_program): ValidatedJson<ProgramRequest>,
) -> Result<(StatusCode, Json<Program>), AppError> {
    if !user.scope.contains(Scope::WritePrograms) {
        return Err(AppError::Forbidden("Missing 'write_programs' scope"));
    }

    let program = program_source
        .create(new_program, &Some(user.client_id()?))
        .await?;

    info!(
        %program.id,
        program.program_name=program.content.program_name,
        client_id = user.sub,
        "program added"
    );

    Ok((StatusCode::CREATED, Json(program)))
}

pub async fn edit(
    State(program_source): State<Arc<dyn ProgramCrud>>,
    Path(id): Path<ProgramId>,
    User(user): User,
    ValidatedJson(content): ValidatedJson<ProgramRequest>,
) -> AppResponse<Program> {
    if !user.scope.contains(Scope::WritePrograms) {
        return Err(AppError::Forbidden("Missing 'write_programs' scope"));
    }

    let program = program_source
        .update(&id, content, &Some(user.client_id()?))
        .await?;

    info!(
        %program.id,
        program.program_name=program.content.program_name,
        client_id = user.sub,
        "program updated"
    );

    Ok(Json(program))
}

pub async fn delete(
    State(program_source): State<Arc<dyn ProgramCrud>>,
    Path(id): Path<ProgramId>,
    User(user): User,
) -> AppResponse<Program> {
    if !user.scope.contains(Scope::WritePrograms) {
        return Err(AppError::Forbidden("Missing 'write_programs' scope"));
    }

    let program = program_source.delete(&id, &Some(user.client_id()?)).await?;
    info!(%id, client_id = user.sub, "deleted program");
    Ok(Json(program))
}

#[derive(Deserialize, Validate, Debug)]
#[serde(rename_all = "camelCase")]
pub struct QueryParams {
    #[serde(default)]
    pub(crate) targets: Option<Vec<Target>>,
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

#[cfg(test)]
#[cfg(feature = "live-db-test")]
mod test {
    use crate::{data_source::PostgresStorage, state::AppState};
    use std::str::FromStr;

    use crate::api::test::*;

    use super::*;
    use crate::data_source::DataSource;
    use axum::{
        body::Body,
        http::{self, Request, Response, StatusCode},
        Router,
    };
    use http_body_util::BodyExt;
    use reqwest::Method;
    use openleadr_wire::{problem::Problem, target::Target};
    use sqlx::PgPool;
    use tower::{Service, ServiceExt};

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

    fn program_request(
        method: Method,
        program: ProgramRequest,
        id: &str,
        token: &str,
    ) -> Request<Body> {
        Request::builder()
            .method(method)
            .uri(format!("/programs/{id}"))
            .header(http::header::AUTHORIZATION, format!("Bearer {token}"))
            .header(http::header::CONTENT_TYPE, mime::APPLICATION_JSON.as_ref())
            .body(Body::from(serde_json::to_vec(&program).unwrap()))
            .unwrap()
    }

    async fn state_with_programs(
        new_programs: Vec<ProgramRequest>,
        db: PgPool,
    ) -> (AppState, Vec<Program>) {
        let store = PostgresStorage::new(db).unwrap();
        let mut programs = Vec::new();

        for program in new_programs {
            let p = store
                .programs()
                .create(program.clone(), &None)
                .await
                .unwrap();
            assert_eq!(p.content, program);
            programs.push(p);
        }

        (AppState::new(store).await, programs)
    }

    async fn help_get(app: &mut Router, token: &str, id: &str) -> Response<Body> {
        app.oneshot(
            Request::builder()
                .method(Method::GET)
                .uri(format!("/programs/{id}"))
                .header(http::header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap()
    }

    async fn help_get_all(app: &mut Router, token: &str) -> Response<Body> {
        app.oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/programs")
                .header(http::header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
            .await
            .unwrap()
    }

    #[sqlx::test]
    async fn get(db: PgPool) {
        let (state, mut programs) = state_with_programs(vec![default_content()], db).await;
        let program = programs.remove(0);
        let token = jwt_test_token(&state, "test-client", vec![Scope::ReadAll]);
        let mut app = state.into_router();

        let response = help_get(&mut app, &token, program.id.as_str()).await;

        assert_eq!(response.status(), StatusCode::OK);

        let body = response.into_body().collect().await.unwrap().to_bytes();
        let db_program: Program = serde_json::from_slice(&body).unwrap();

        assert_eq!(program, db_program);
    }

    #[sqlx::test]
    async fn get_all(db: PgPool) {
        let mut programs = vec![default_content(), default_content(), default_content()];
        programs[0].program_name = "program0".to_string();
        programs[1].program_name = "program1".to_string();
        programs[2].program_name = "program2".to_string();
        let (state, _) = state_with_programs(programs, db).await;
        let token = jwt_test_token(&state, "test-client", vec![Scope::ReadAll]);
        let mut app = state.into_router();

        let response = help_get_all(&mut app, &token).await;

        assert_eq!(response.status(), StatusCode::OK);

        let body = response.into_body().collect().await.unwrap().to_bytes();
        let db_programs: Vec<Program> = serde_json::from_slice(&body).unwrap();

        assert_eq!(db_programs.len(), 3);
    }

    #[sqlx::test]
    async fn delete(db: PgPool) {
        let mut programs = vec![default_content(), default_content(), default_content()];
        programs[0].program_name = "program0".to_string();
        programs[1].program_name = "program1".to_string();
        programs[2].program_name = "program2".to_string();

        let (state, programs) =
            state_with_programs(programs, db).await;
        let program_id = programs[1].id.clone();
        let token = jwt_test_token(
            &state,
            "test-client",
            vec![Scope::ReadAll, Scope::WritePrograms],
        );
        let mut app = state.into_router();

        let request = Request::builder()
            .method(Method::DELETE)
            .uri(format!("/programs/{program_id}"))
            .header(http::header::AUTHORIZATION, format!("Bearer {token}"))
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
        let db_program: Program = serde_json::from_slice(&body).unwrap();

        assert_eq!(programs[1], db_program);

        let response = retrieve_all_with_filter_help(&mut app, "", &token).await;
        assert_eq!(response.status(), StatusCode::OK);

        let body = response.into_body().collect().await.unwrap().to_bytes();
        let programs: Vec<Program> = serde_json::from_slice(&body).unwrap();
        assert_eq!(programs.len(), 2);
    }

    #[sqlx::test(fixtures("users"))]
    async fn update(db: PgPool) {
        let (state, mut programs) = state_with_programs(vec![default_content()], db).await;
        let program = programs.remove(0);
        let token = jwt_test_token(&state, "test-client", vec![Scope::ReadAll, Scope::WritePrograms]);
        let app = state.into_router();

        let response = app
            .oneshot(program_request(
                Method::PUT,
                program.content.clone(),
                program.id.as_str(),
                &token,
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let body = response.into_body().collect().await.unwrap().to_bytes();
        let db_program: Program = serde_json::from_slice(&body).unwrap();

        assert_eq!(program.content, db_program.content);
        assert!(program.modification_date_time < db_program.modification_date_time);
    }

    #[sqlx::test]
    async fn update_same_name(db: PgPool) {
        let mut programs = vec![default_content(), default_content()];
        programs[0].program_name = "program0".to_string();
        programs[1].program_name = "program1".to_string();

        let (state, mut programs) = state_with_programs(programs, db).await;
        let token = jwt_test_token(&state, "test-client", vec![Scope::ReadAll, Scope::WritePrograms]);
        let app = state.into_router();

        let mut updated = programs.remove(0);
        updated.content.program_name = "program1".to_string();

        // different id, same name
        let response = app
            .oneshot(program_request(
                Method::PUT,
                updated.content,
                updated.id.as_str(),
                &token,
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CONFLICT);
    }

    async fn help_create_program(
        mut app: &mut Router,
        token: &str,
        body: &ProgramRequest,
    ) -> Response<Body> {
        let request = Request::builder()
            .method(Method::POST)
            .uri("/programs")
            .header(http::header::AUTHORIZATION, format!("Bearer {token}"))
            .header(http::header::CONTENT_TYPE, mime::APPLICATION_JSON.as_ref())
            .body(Body::from(serde_json::to_vec(body).unwrap()))
            .unwrap();

        ServiceExt::<Request<Body>>::ready(&mut app)
            .await
            .unwrap()
            .call(request)
            .await
            .unwrap()
    }

    #[sqlx::test]
    async fn create_same_name(db: PgPool) {
        let (state, _) = state_with_programs(vec![], db).await;
        let token = jwt_test_token(
            &state,
            "test-client",
            vec![Scope::ReadAll, Scope::WritePrograms],
        );
        let mut app = state.into_router();

        let response = help_create_program(&mut app, &token, &default_content()).await;
        assert_eq!(response.status(), StatusCode::CREATED);

        let response = help_create_program(&mut app, &token, &default_content()).await;
        assert_eq!(response.status(), StatusCode::CONFLICT);
    }

    #[sqlx::test]
    async fn name_constraint_validation(db: PgPool) {
        let mut programs = vec![default_content(), default_content()];
        programs[0].program_name = "".to_string();
        programs[1].program_name = "This is more than 128 characters long and should be rejected This is more than 128 characters long and should be rejected asdfasd".to_string();

        let test = ApiTest::new(db, "test-client", vec![Scope::ReadAll]).await;

        for program in &programs {
            let (status, error) = test
                .request::<Problem>(
                    Method::POST,
                    "/programs",
                    Body::from(serde_json::to_vec(&program).unwrap()),
                )
                .await;

            assert_eq!(status, StatusCode::BAD_REQUEST);
            let detail = error.detail.unwrap();
            assert!(detail.contains("outside of allowed range 1..=128"));
        }
    }

    async fn retrieve_all_with_filter_help(
        app: &mut Router,
        query_params: &str,
        token: &str,
    ) -> Response<Body> {
        let request = Request::builder()
            .method(Method::GET)
            .uri(format!("/programs?{query_params}"))
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

    #[sqlx::test]
    async fn retrieve_all_with_filter(db: PgPool) {
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

        let (state, _) = state_with_programs(vec![program1, program2, program3], db).await;
        let token = jwt_test_token(&state, "test-client", vec![Scope::ReadAll]);
        let mut app = state.into_router();

        // no query params
        let response = retrieve_all_with_filter_help(&mut app, "", &token).await;
        assert_eq!(response.status(), StatusCode::OK);

        let body = response.into_body().collect().await.unwrap().to_bytes();
        let programs: Vec<Program> = serde_json::from_slice(&body).unwrap();
        assert_eq!(programs.len(), 3);

        // skip
        let response = retrieve_all_with_filter_help(&mut app, "skip=1", &token).await;
        assert_eq!(response.status(), StatusCode::OK);

        let body = response.into_body().collect().await.unwrap().to_bytes();
        let programs: Vec<Program> = serde_json::from_slice(&body).unwrap();
        assert_eq!(programs.len(), 2);

        let response = retrieve_all_with_filter_help(&mut app, "skip=-1", &token).await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        let response = retrieve_all_with_filter_help(&mut app, "skip=0", &token).await;
        assert_eq!(response.status(), StatusCode::OK);

        // limit
        let response = retrieve_all_with_filter_help(&mut app, "limit=2", &token).await;
        assert_eq!(response.status(), StatusCode::OK);

        let body = response.into_body().collect().await.unwrap().to_bytes();
        let programs: Vec<Program> = serde_json::from_slice(&body).unwrap();
        assert_eq!(programs.len(), 2);

        let response = retrieve_all_with_filter_help(&mut app, "limit=-1", &token).await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        let response = retrieve_all_with_filter_help(&mut app, "limit=0", &token).await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        let response = retrieve_all_with_filter_help(&mut app, "targets=NONSENSE", &token).await;
        assert_eq!(response.status(), StatusCode::OK);

        let body = response.into_body().collect().await.unwrap().to_bytes();
        let programs: Vec<Program> = serde_json::from_slice(&body).unwrap();
        assert_eq!(programs.len(), 0);

        let response =
            retrieve_all_with_filter_help(&mut app, "targets=group-1", &token)
                .await;
        assert_eq!(response.status(), StatusCode::OK);

        let body = response.into_body().collect().await.unwrap().to_bytes();
        let programs: Vec<Program> = serde_json::from_slice(&body).unwrap();
        assert_eq!(programs.len(), 1);
    }

    mod permissions {
        use super::*;

        #[sqlx::test]
        async fn can_create_program_with_only_write_scope(db: PgPool) {
            let (state, _) = state_with_programs(vec![], db).await;
            let token = jwt_test_token(&state, "test-client", vec![Scope::WritePrograms]);
            let mut app = state.into_router();

            let response = help_create_program(&mut app, &token, &default_content()).await;
            assert_eq!(response.status(), StatusCode::CREATED);
        }

        #[sqlx::test]
        async fn can_create_program_with_write_scope(db: PgPool) {
            let (state, _) = state_with_programs(vec![], db.clone()).await;
            let token = jwt_test_token(
                &state,
                "test-client",
                vec![Scope::ReadAll, Scope::WritePrograms],
            );

            let mut app = state.into_router();

            let response = help_create_program(&mut app, &token, &default_content()).await;
            assert_eq!(response.status(), StatusCode::CREATED);
        }

        #[sqlx::test]
        async fn cannot_create_program_without_write_scope(db: PgPool) {
            let (state, _) = state_with_programs(vec![], db.clone()).await;
            let token = jwt_test_token(
                &state,
                "test-client",
                Scope::all()
                    .into_iter()
                    .filter(|s| *s != Scope::WritePrograms)
                    .collect(),
            );

            let mut app = state.into_router();

            let response = help_create_program(&mut app, &token, &default_content()).await;
            assert_eq!(response.status(), StatusCode::FORBIDDEN);
        }

        #[sqlx::test(fixtures("vens"))]
        async fn vens_can_read_assigned_programs_only(db: PgPool) {
            let content = ProgramRequest {
                targets: vec!["group-1".parse().unwrap(), "private-value".parse().unwrap()],
                ..default_content()
            };

            let (state, mut programs) = state_with_programs(vec![content], db).await;
            let program = programs.remove(0);
            let mut app = state.clone().into_router();

            let token = jwt_test_token(&state, "ven-1-client-id", vec![Scope::ReadTargets]);
            let response = help_get(&mut app, &token, program.id.as_str()).await;
            assert_eq!(response.status(), StatusCode::OK);

            let token = jwt_test_token(&state, "ven-2-client-id", vec![Scope::ReadTargets]);
            let response = help_get(&mut app, &token, program.id.as_str()).await;
            assert_eq!(response.status(), StatusCode::NOT_FOUND);

            let token = jwt_test_token(&state, "ven-3-client-id", vec![Scope::ReadTargets]);
            let response = help_get(&mut app, &token, program.id.as_str()).await;
            assert_eq!(response.status(), StatusCode::OK);

            let token = jwt_test_token(&state, "ven-4-client-id", vec![Scope::ReadTargets]);
            let response = help_get(&mut app, &token, program.id.as_str()).await;
            assert_eq!(response.status(), StatusCode::NOT_FOUND);
        }

        #[sqlx::test(fixtures("users", "programs", "vens"))]
        async fn retrieve_all_returns_ven_assigned_programs_only(db: PgPool) {
            let (state, _) = state_with_programs(vec![], db).await;
            let mut app = state.clone().into_router();

            let token = jwt_test_token(
                &state,
                "ven-1-client-id".to_string(),
                vec![Scope::ReadTargets],
            );
            let response = retrieve_all_with_filter_help(&mut app, "", &token).await;
            let status = response.status();
            let body = response.into_body().collect().await.unwrap().to_bytes();
            println!("{}", String::from_utf8_lossy(&body));
            assert_eq!(status, StatusCode::OK);
            let programs: Vec<Program> = serde_json::from_slice(&body).unwrap();
            assert_eq!(programs.len(), 2);
            let mut names = programs
                .into_iter()
                .map(|p| p.content.program_name)
                .collect::<Vec<_>>();
            names.sort();
            assert_eq!(names, vec!["program-1", "program-3"]);

            let token = jwt_test_token(
                &state,
                "ven-2-client-id".to_string(),
                vec![Scope::ReadTargets],
            );
            let response = retrieve_all_with_filter_help(&mut app, "", &token).await;
            assert_eq!(response.status(), StatusCode::OK);
            let body = response.into_body().collect().await.unwrap().to_bytes();
            let programs: Vec<Program> = serde_json::from_slice(&body).unwrap();
            assert_eq!(programs.len(), 2);
            let mut names = programs
                .into_iter()
                .map(|p| p.content.program_name)
                .collect::<Vec<_>>();
            names.sort();
            assert_eq!(names, vec!["program-1", "program-2"]);

            let token = jwt_test_token(
                &state,
                "ven-2-client-id".to_string(),
                vec![Scope::ReadTargets],
            );
            let response = retrieve_all_with_filter_help(&mut app, "targets=ven-1", &token).await;
            assert_eq!(response.status(), StatusCode::OK);
            let body = response.into_body().collect().await.unwrap().to_bytes();
            let programs: Vec<Program> = serde_json::from_slice(&body).unwrap();
            assert!(programs.is_empty());
        }

        #[sqlx::test(fixtures("users", "programs", "vens"))]
        async fn ven_cannot_write_program(db: PgPool) {
            let (state, _) = state_with_programs(vec![], db).await;
            let mut app = state.clone().into_router();

            let token = jwt_test_token(
                &state,
                "ven-1-client-id".to_string(),
                vec![Scope::ReadTargets],
            );
            let response = help_create_program(&mut app, &token, &default_content()).await;
            assert_eq!(response.status(), StatusCode::FORBIDDEN);

            let response = app
                .clone()
                .oneshot(program_request(
                    Method::PUT,
                    default_content(),
                    "program-1",
                    &token,
                ))
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::FORBIDDEN);

            app.clone()
                .oneshot(
                    Request::builder()
                        .method(Method::DELETE)
                        .uri(format!("/programs/{}", "program-1"))
                        .header(http::header::AUTHORIZATION, format!("Bearer {token}"))
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::FORBIDDEN);
        }
    }
}
