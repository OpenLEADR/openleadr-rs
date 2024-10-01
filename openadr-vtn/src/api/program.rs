use std::sync::Arc;

use axum::{
    extract::{Path, State},
    Json,
};
use reqwest::StatusCode;
use serde::Deserialize;
use tracing::{info, trace};
use validator::{Validate, ValidationError};

use openadr_wire::{
    program::{ProgramContent, ProgramId},
    target::TargetLabel,
    Program,
};

use crate::{
    api::{AppResponse, ValidatedJson, ValidatedQuery},
    data_source::ProgramCrud,
    error::AppError,
    jwt::{BusinessUser, User},
};
pub async fn get_all(
    State(program_source): State<Arc<dyn ProgramCrud>>,
    ValidatedQuery(query_params): ValidatedQuery<QueryParams>,
    User(user): User,
) -> AppResponse<Vec<Program>> {
    trace!(?query_params);

    let programs = program_source.retrieve_all(&query_params, &user).await?;

    Ok(Json(programs))
}

pub async fn get(
    State(program_source): State<Arc<dyn ProgramCrud>>,
    Path(id): Path<ProgramId>,
    User(user): User,
) -> AppResponse<Program> {
    let program = program_source.retrieve(&id, &user).await?;
    Ok(Json(program))
}

pub async fn add(
    State(program_source): State<Arc<dyn ProgramCrud>>,
    BusinessUser(user): BusinessUser,
    ValidatedJson(new_program): ValidatedJson<ProgramContent>,
) -> Result<(StatusCode, Json<Program>), AppError> {
    let program = program_source.create(new_program, &user).await?;

    Ok((StatusCode::CREATED, Json(program)))
}

pub async fn edit(
    State(program_source): State<Arc<dyn ProgramCrud>>,
    Path(id): Path<ProgramId>,
    BusinessUser(user): BusinessUser,
    ValidatedJson(content): ValidatedJson<ProgramContent>,
) -> AppResponse<Program> {
    let program = program_source.update(&id, content, &user).await?;

    info!(%program.id, program.program_name=program.content.program_name, "program updated");

    Ok(Json(program))
}

pub async fn delete(
    State(program_source): State<Arc<dyn ProgramCrud>>,
    Path(id): Path<ProgramId>,
    BusinessUser(user): BusinessUser,
) -> AppResponse<Program> {
    let program = program_source.delete(&id, &user).await?;
    info!(%id, "deleted program");
    Ok(Json(program))
}

#[derive(Deserialize, Validate, Debug)]
#[validate(schema(function = "validate_target_type_value_pair"))]
#[serde(rename_all = "camelCase")]
pub struct QueryParams {
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

    use crate::api::test::*;

    use super::*;
    // for `collect`
    use crate::{
        data_source::DataSource,
        jwt::{AuthRole, Claims},
    };
    use axum::{
        body::Body,
        http::{self, Request, Response, StatusCode},
        Router,
    };
    use http_body_util::BodyExt;
    use openadr_wire::Event;
    use sqlx::PgPool;
    use tower::{Service, ServiceExt};
    // for `call`, `oneshot`, and `ready`

    fn default_content() -> ProgramContent {
        ProgramContent {
            object_type: None,
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

    fn program_request(
        method: http::Method,
        program: ProgramContent,
        id: &str,
        token: &str,
    ) -> Request<Body> {
        Request::builder()
            .method(method)
            .uri(format!("/programs/{}", id))
            .header(http::header::AUTHORIZATION, format!("Bearer {}", token))
            .header(http::header::CONTENT_TYPE, mime::APPLICATION_JSON.as_ref())
            .body(Body::from(serde_json::to_vec(&program).unwrap()))
            .unwrap()
    }

    async fn state_with_programs(
        new_programs: Vec<ProgramContent>,
        db: PgPool,
    ) -> (AppState, Vec<Program>) {
        let store = PostgresStorage::new(db).unwrap();
        let mut programs = Vec::new();

        for program in new_programs {
            let p = store
                .programs()
                .create(program.clone(), &Claims::any_business_user())
                .await
                .unwrap();
            assert_eq!(p.content, program);
            programs.push(p);
        }

        (
            AppState::new(store, JwtManager::from_base64_secret("test").unwrap()),
            programs,
        )
    }

    async fn get_help(app: &mut Router, token: &str, id: &str) -> Response<Body> {
        app.oneshot(
            Request::builder()
                .method(http::Method::GET)
                .uri(format!("/programs/{}", id))
                .header(http::header::AUTHORIZATION, format!("Bearer {}", token))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap()
    }

    #[sqlx::test(fixtures("users"))]
    async fn get(db: PgPool) {
        let (state, mut programs) = state_with_programs(vec![default_content()], db).await;
        let program = programs.remove(0);
        let token = jwt_test_token(&state, vec![AuthRole::AnyBusiness]);
        let mut app = state.into_router();

        let response = get_help(&mut app, &token, program.id.as_str()).await;

        assert_eq!(response.status(), StatusCode::OK);

        let body = response.into_body().collect().await.unwrap().to_bytes();
        let db_program: Program = serde_json::from_slice(&body).unwrap();

        assert_eq!(program, db_program);
    }

    #[sqlx::test(fixtures("users"))]
    async fn delete(db: PgPool) {
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

        let (state, programs) =
            state_with_programs(vec![program1, program2.clone(), program3], db).await;
        let program_id = programs[1].id.clone();
        let token = jwt_test_token(&state, vec![AuthRole::AnyBusiness]);
        let mut app = state.into_router();

        let request = Request::builder()
            .method(http::Method::DELETE)
            .uri(format!("/programs/{program_id}"))
            .header(http::header::AUTHORIZATION, format!("Bearer {}", token))
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

        assert_eq!(program2, db_program.content);

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
        let token = jwt_test_token(&state, vec![AuthRole::AnyBusiness]);
        let app = state.into_router();

        let response = app
            .oneshot(program_request(
                http::Method::PUT,
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

    #[sqlx::test(fixtures("users"))]
    async fn update_same_name(db: PgPool) {
        let program1 = ProgramContent {
            program_name: "program1".to_string(),
            ..default_content()
        };
        let program2 = ProgramContent {
            program_name: "program2".to_string(),
            ..default_content()
        };

        let (state, mut programs) = state_with_programs(vec![program1, program2], db).await;
        let token = jwt_test_token(&state, vec![AuthRole::AnyBusiness]);
        let app = state.into_router();

        let mut updated = programs.remove(0);
        updated.content.program_name = "program2".to_string();

        // different id, same name
        let response = app
            .oneshot(program_request(
                http::Method::PUT,
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
        body: &ProgramContent,
    ) -> Response<Body> {
        let request = Request::builder()
            .method(http::Method::POST)
            .uri("/programs")
            .header(http::header::AUTHORIZATION, format!("Bearer {}", token))
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

    #[sqlx::test(fixtures("users"))]
    async fn create_same_name(db: PgPool) {
        let (state, _) = state_with_programs(vec![], db).await;
        let token = jwt_test_token(&state, vec![AuthRole::AnyBusiness]);
        let mut app = state.into_router();

        let response = help_create_program(&mut app, &token, &default_content()).await;
        assert_eq!(response.status(), StatusCode::CREATED);

        let response = help_create_program(&mut app, &token, &default_content()).await;
        assert_eq!(response.status(), StatusCode::CONFLICT);
    }

    async fn retrieve_all_with_filter_help(
        app: &mut Router,
        query_params: &str,
        token: &str,
    ) -> Response<Body> {
        let request = Request::builder()
            .method(http::Method::GET)
            .uri(format!("/programs?{query_params}"))
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

    #[sqlx::test(fixtures("users"))]
    async fn retrieve_all_with_filter(db: PgPool) {
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

        let (state, _) = state_with_programs(vec![program1, program2, program3], db).await;
        let token = jwt_test_token(&state, vec![AuthRole::AnyBusiness]);
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
            "targetType=PROGRAM_NAME&targetValues=program1&targetValues=program2",
            &token,
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);

        let body = response.into_body().collect().await.unwrap().to_bytes();
        let programs: Vec<Program> = serde_json::from_slice(&body).unwrap();
        assert_eq!(programs.len(), 2);
    }

    mod permissions {
        use super::*;
        use openadr_wire::target::{TargetEntry, TargetMap};

        #[sqlx::test(fixtures("users", "business"))]
        async fn business_can_create_program(db: PgPool) {
            let (state, _) = state_with_programs(vec![], db).await;
            let token = jwt_test_token(&state, vec![AuthRole::Business("business-1".to_string())]);
            let mut app = state.into_router();

            let response = help_create_program(&mut app, &token, &default_content()).await;
            assert_eq!(response.status(), StatusCode::CREATED);
        }

        #[sqlx::test(fixtures("users", "business"))]
        async fn business_id_must_must_be_unambiguous_create_program(db: PgPool) {
            let (state, _) = state_with_programs(vec![], db.clone()).await;
            let token = jwt_test_token(
                &state,
                vec![
                    AuthRole::Business("business-1".to_string()),
                    AuthRole::Business("business-2".to_string()),
                ],
            );
            let mut app = state.into_router();

            let response = help_create_program(&mut app, &token, &default_content()).await;
            assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        }

        #[sqlx::test(fixtures("users", "business"))]
        async fn businesses_can_read_any_program(db: PgPool) {
            let (state, _) = state_with_programs(vec![], db).await;
            let token = jwt_test_token(&state, vec![AuthRole::Business("business-1".to_string())]);
            let mut app = state.clone().into_router();

            let response = help_create_program(&mut app, &token, &default_content()).await;
            assert_eq!(response.status(), StatusCode::CREATED);

            let body = response.into_body().collect().await.unwrap().to_bytes();
            let program: Program = serde_json::from_slice(&body).unwrap();

            let response = get_help(&mut app, &token, program.id.as_str()).await;
            assert_eq!(response.status(), StatusCode::OK);

            let token = jwt_test_token(&state, vec![AuthRole::Business("business-2".to_string())]);
            let response = get_help(&mut app, &token, program.id.as_str()).await;
            assert_eq!(response.status(), StatusCode::OK);

            let token = jwt_test_token(
                &state,
                vec![
                    AuthRole::Business("business-2".to_string()),
                    AuthRole::Business("business-1".to_string()),
                ],
            );
            let response = get_help(&mut app, &token, program.id.as_str()).await;
            assert_eq!(response.status(), StatusCode::OK);

            let token = jwt_test_token(&state, vec![AuthRole::AnyBusiness]);
            let response = get_help(&mut app, &token, program.id.as_str()).await;
            assert_eq!(response.status(), StatusCode::OK);
        }

        #[sqlx::test(fixtures("users", "business", "programs", "vens"))]
        async fn vens_can_read_assigned_programs_only(db: PgPool) {
            let (state, _) = state_with_programs(vec![], db).await;
            let token = jwt_test_token(&state, vec![AuthRole::Business("business-1".to_string())]);
            let mut app = state.clone().into_router();

            let content = ProgramContent {
                targets: Some(TargetMap(vec![TargetEntry {
                    label: TargetLabel::VENName,
                    values: ["ven-1-name".to_string()],
                }])),
                ..default_content()
            };

            let response = help_create_program(&mut app, &token, &content).await;
            assert_eq!(response.status(), StatusCode::CREATED);

            let body = response.into_body().collect().await.unwrap().to_bytes();
            let program: Program = serde_json::from_slice(&body).unwrap();

            let token = jwt_test_token(&state, vec![AuthRole::VEN("ven-1".parse().unwrap())]);
            let response = get_help(&mut app, &token, program.id.as_str()).await;
            assert_eq!(response.status(), StatusCode::OK);

            let token = jwt_test_token(&state, vec![AuthRole::VEN("ven-2".parse().unwrap())]);
            let response = get_help(&mut app, &token, program.id.as_str()).await;
            assert_eq!(response.status(), StatusCode::NOT_FOUND);

            let token = jwt_test_token(
                &state,
                vec![
                    AuthRole::VEN("ven-2".parse().unwrap()),
                    AuthRole::VEN("ven-1".parse().unwrap()),
                ],
            );
            let response = get_help(&mut app, &token, program.id.as_str()).await;
            assert_eq!(response.status(), StatusCode::OK);
        }

        #[sqlx::test(fixtures("users", "business", "programs", "vens", "vens-programs"))]
        async fn retrieve_all_returns_ven_assigned_programs_only(db: PgPool) {
            let (state, _) = state_with_programs(vec![], db).await;
            let mut app = state.clone().into_router();

            let token = jwt_test_token(&state, vec![AuthRole::VEN("ven-1".parse().unwrap())]);
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
            assert_eq!(names, vec!["program-1", "program-3"]);

            let token = jwt_test_token(&state, vec![AuthRole::VEN("ven-2".parse().unwrap())]);
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

            let token = jwt_test_token(&state, vec![AuthRole::VEN("ven-2".parse().unwrap())]);
            let response = retrieve_all_with_filter_help(
                &mut app,
                "targetType=VEN_NAME&targetValues=ven-1",
                &token,
            )
            .await;
            assert_eq!(response.status(), StatusCode::OK);
            let body = response.into_body().collect().await.unwrap().to_bytes();
            let programs: Vec<Program> = serde_json::from_slice(&body).unwrap();
            assert!(programs.is_empty());

            let token = jwt_test_token(
                &state,
                vec![
                    AuthRole::VEN("ven-2".parse().unwrap()),
                    AuthRole::VEN("ven-1".parse().unwrap()),
                ],
            );
            let response = retrieve_all_with_filter_help(&mut app, "", &token).await;
            assert_eq!(response.status(), StatusCode::OK);
            let body = response.into_body().collect().await.unwrap().to_bytes();
            let programs: Vec<Program> = serde_json::from_slice(&body).unwrap();
            assert_eq!(programs.len(), 3);
            let mut names = programs
                .into_iter()
                .map(|p| p.content.program_name)
                .collect::<Vec<_>>();
            names.sort();
            assert_eq!(names, vec!["program-1", "program-2", "program-3"]);
        }

        #[sqlx::test(fixtures("users", "business", "programs", "vens"))]
        async fn ven_cannot_write_program(db: PgPool) {
            let (state, _) = state_with_programs(vec![], db).await;
            let mut app = state.clone().into_router();

            let token = jwt_test_token(&state, vec![AuthRole::VEN("ven-1".parse().unwrap())]);
            let response = help_create_program(&mut app, &token, &default_content()).await;
            assert_eq!(response.status(), StatusCode::FORBIDDEN);

            let response = app
                .clone()
                .oneshot(program_request(
                    http::Method::PUT,
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
                        .method(http::Method::DELETE)
                        .uri(format!("/programs/{}", "program-1"))
                        .header(http::header::AUTHORIZATION, format!("Bearer {}", token))
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::FORBIDDEN);
        }
    }
}
