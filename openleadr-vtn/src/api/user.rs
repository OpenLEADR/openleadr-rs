use crate::{
    api::{AppResponse, ValidatedJson},
    data_source::{AuthSource, UserDetails},
    error::AppError,
    jwt::{AuthRole, UserManagerUser},
};
use axum::{
    extract::{Path, State},
    Json,
};
use reqwest::StatusCode;
use serde::Deserialize;
#[cfg(test)]
use serde::Serialize;
use std::sync::Arc;
use tracing::{info, trace};
use validator::Validate;

#[derive(Deserialize, Debug, Validate)]
#[cfg_attr(test, derive(Serialize))]
pub struct NewUser {
    reference: String,
    description: Option<String>,
    roles: Vec<AuthRole>,
}

#[derive(Deserialize, Validate)]
#[cfg_attr(test, derive(Serialize, Default))]
pub struct NewCredential {
    client_id: String,
    client_secret: String,
}

pub async fn get_all(
    State(auth_source): State<Arc<dyn AuthSource>>,
    UserManagerUser(_): UserManagerUser,
) -> AppResponse<Vec<UserDetails>> {
    let users = auth_source.get_all_users().await?;

    trace!("received {} users", users.len());
    Ok(Json(users))
}

pub async fn get(
    State(auth_source): State<Arc<dyn AuthSource>>,
    Path(id): Path<String>,
    UserManagerUser(_): UserManagerUser,
) -> AppResponse<UserDetails> {
    let user = auth_source.get_user(&id).await?;
    trace!(user_id = user.id(), "received user");
    Ok(Json(user))
}

pub async fn add_user(
    State(auth_source): State<Arc<dyn AuthSource>>,
    UserManagerUser(_): UserManagerUser,
    ValidatedJson(new_user): ValidatedJson<NewUser>,
) -> Result<(StatusCode, Json<UserDetails>), AppError> {
    let user = auth_source
        .add_user(
            &new_user.reference,
            new_user.description.as_deref(),
            &new_user.roles,
        )
        .await?;
    info!(user_id = user.id(), "created new user");
    Ok((StatusCode::CREATED, Json(user)))
}

pub async fn add_credential(
    State(auth_source): State<Arc<dyn AuthSource>>,
    Path(id): Path<String>,
    UserManagerUser(_): UserManagerUser,
    ValidatedJson(new): ValidatedJson<NewCredential>,
) -> AppResponse<UserDetails> {
    let user = auth_source
        .add_credential(&id, &new.client_id, &new.client_secret)
        .await?;
    info!(
        user_id = id,
        client_id = new.client_id,
        "created new credential for user"
    );
    Ok(Json(user))
}

pub async fn edit(
    State(auth_source): State<Arc<dyn AuthSource>>,
    Path(id): Path<String>,
    UserManagerUser(_): UserManagerUser,
    ValidatedJson(modified): ValidatedJson<NewUser>,
) -> AppResponse<UserDetails> {
    let user = auth_source
        .edit_user(
            &id,
            &modified.reference,
            modified.description.as_deref(),
            &modified.roles,
        )
        .await?;

    info!(user_id = user.id(), "updated user");
    Ok(Json(user))
}

pub async fn delete_user(
    State(auth_source): State<Arc<dyn AuthSource>>,
    Path(id): Path<String>,
    UserManagerUser(_): UserManagerUser,
) -> AppResponse<UserDetails> {
    let user = auth_source.remove_user(&id).await?;
    info!(user_id = user.id(), "deleted user");
    Ok(Json(user))
}

pub async fn delete_credential(
    State(auth_source): State<Arc<dyn AuthSource>>,
    Path((user_id, client_id)): Path<(String, String)>,
    UserManagerUser(_): UserManagerUser,
) -> AppResponse<UserDetails> {
    let user = auth_source.remove_credentials(&user_id, &client_id).await?;
    info!(user_id = user.id(), client_id, "deleted credential");
    Ok(Json(user))
}

#[cfg(test)]
#[cfg(feature = "live-db-test")]
mod test {
    use super::*;
    use crate::api::test::{jwt_test_token, state};
    use axum::{
        body::Body,
        http,
        http::{Request, Response, StatusCode},
        Router,
    };
    use http_body_util::BodyExt;
    use sqlx::PgPool;
    use tower::ServiceExt;

    fn user_1() -> UserDetails {
        UserDetails {
            id: "user-1".to_string(),
            reference: "user-1-ref".to_string(),
            description: Some("desc".to_string()),
            roles: vec![],
            client_ids: vec!["user-1-client-id".to_string()],
            created: "2024-07-25 08:31:10.776000 +00:00".parse().unwrap(),
            modified: "2024-07-25 08:31:10.776000 +00:00".parse().unwrap(),
        }
    }

    fn admin() -> UserDetails {
        UserDetails {
            id: "admin".to_string(),
            reference: "admin-ref".to_string(),
            description: None,
            roles: vec![
                AuthRole::UserManager,
                AuthRole::VenManager,
                AuthRole::AnyBusiness,
            ],
            client_ids: vec!["admin".to_string()],
            created: "2024-07-25 08:31:10.776000 +00:00".parse().unwrap(),
            modified: "2024-07-25 08:31:10.776000 +00:00".parse().unwrap(),
        }
    }

    fn new_user() -> NewUser {
        NewUser {
            reference: "new user reference".to_string(),
            description: Some("Some description".to_string()),
            roles: vec![
                AuthRole::UserManager,
                AuthRole::VenManager,
                AuthRole::AnyBusiness,
            ],
        }
    }

    fn all_roles() -> Vec<AuthRole> {
        vec![
            AuthRole::VEN("ven-1".parse().unwrap()),
            AuthRole::AnyBusiness,
            AuthRole::Business("business-1".parse().unwrap()),
            AuthRole::VenManager,
            AuthRole::UserManager,
        ]
    }

    impl PartialEq<UserDetails> for NewUser {
        fn eq(&self, other: &UserDetails) -> bool {
            let mut self_roles = self.roles.clone();
            self_roles.sort();

            let mut other_roles = other.roles.clone();
            other_roles.sort();

            self.reference == other.reference
                && self.description == other.description
                && self_roles == other_roles
        }
    }

    async fn help_get(app: &mut Router, token: &str, id: &str) -> Response<Body> {
        app.oneshot(
            Request::builder()
                .method(http::Method::GET)
                .uri(format!("/users/{id}"))
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
                .method(http::Method::GET)
                .uri("/users")
                .header(http::header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap()
    }

    async fn help_post<T: Serialize>(
        app: &mut Router,
        token: &str,
        path: &str,
        body: &T,
    ) -> Response<Body> {
        app.oneshot(
            Request::builder()
                .method(http::Method::POST)
                .uri(path)
                .header(http::header::AUTHORIZATION, format!("Bearer {token}"))
                .header(http::header::CONTENT_TYPE, mime::APPLICATION_JSON.as_ref())
                .body(Body::from(serde_json::to_vec(body).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap()
    }

    async fn help_add_user(app: &mut Router, token: &str, user: &NewUser) -> Response<Body> {
        help_post(app, token, "/users", user).await
    }

    async fn help_edit_user(
        app: &mut Router,
        token: &str,
        id: &str,
        user: &NewUser,
    ) -> Response<Body> {
        app.oneshot(
            Request::builder()
                .method(http::Method::PUT)
                .uri(format!("/users/{id}"))
                .header(http::header::AUTHORIZATION, format!("Bearer {token}"))
                .header(http::header::CONTENT_TYPE, mime::APPLICATION_JSON.as_ref())
                .body(Body::from(serde_json::to_vec(&user).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap()
    }

    async fn help_add_credential(
        app: &mut Router,
        token: &str,
        user_id: &str,
        credential: &NewCredential,
    ) -> Response<Body> {
        help_post(app, token, &format!("/users/{user_id}"), credential).await
    }

    async fn help_delete(app: &mut Router, token: &str, path: &str) -> Response<Body> {
        app.oneshot(
            Request::builder()
                .method(http::Method::DELETE)
                .uri(path)
                .header(http::header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap()
    }

    async fn help_login(app: &mut Router, client_id: &str, client_secret: &str) -> Response<Body> {
        app.oneshot(
            Request::builder()
                .method(http::Method::POST)
                .uri("/auth/token")
                .header(
                    http::header::CONTENT_TYPE,
                    mime::APPLICATION_WWW_FORM_URLENCODED.as_ref(),
                )
                .body(Body::from(format!(
                    "client_id={client_id}&client_secret={client_secret}&grant_type=client_credentials",
                )))
                .unwrap(),
        )
        .await
        .unwrap()
    }

    impl UserDetails {
        async fn from(response: Response<Body>) -> Self {
            let body = response.into_body().collect().await.unwrap().to_bytes();
            let mut user: UserDetails = serde_json::from_slice(&body).unwrap();
            user.roles.sort();
            user
        }
    }

    #[sqlx::test(fixtures("users"))]
    async fn get(db: PgPool) {
        let state = state(db).await;
        let token = jwt_test_token(&state, vec![AuthRole::UserManager]);
        let mut app = state.into_router();

        let response = help_get(&mut app, &token, "admin").await;
        assert_eq!(response.status(), StatusCode::OK);

        let user = UserDetails::from(response).await;
        assert_eq!(user, admin());

        let response = help_get(&mut app, &token, "user-1").await;
        assert_eq!(response.status(), StatusCode::OK);

        let user = UserDetails::from(response).await;
        assert_eq!(user, user_1());
    }

    #[sqlx::test(fixtures("users"))]
    async fn all_routes_only_allowed_for_user_manager(db: PgPool) {
        let state = state(db).await;
        let token = jwt_test_token(
            &state,
            vec![
                AuthRole::VEN("123".parse().unwrap()),
                AuthRole::AnyBusiness,
                AuthRole::Business("1234".parse().unwrap()),
                AuthRole::VenManager,
            ],
        );
        let mut app = state.into_router();
        let response = help_get(&mut app, &token, "admin").await;
        assert_eq!(response.status(), StatusCode::FORBIDDEN);

        let response = help_get_all(&mut app, &token).await;
        assert_eq!(response.status(), StatusCode::FORBIDDEN);

        let response = help_add_user(&mut app, &token, &new_user()).await;
        assert_eq!(response.status(), StatusCode::FORBIDDEN);

        let response = help_edit_user(&mut app, &token, "admin", &new_user()).await;
        assert_eq!(response.status(), StatusCode::FORBIDDEN);

        let response = help_add_credential(&mut app, &token, "admin", &Default::default()).await;
        assert_eq!(response.status(), StatusCode::FORBIDDEN);

        let response = help_delete(&mut app, &token, "/users/admin/admin").await;
        assert_eq!(response.status(), StatusCode::FORBIDDEN);

        let response = help_delete(&mut app, &token, "/users/admin").await;
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }

    #[sqlx::test(fixtures("users"))]
    async fn get_all(db: PgPool) {
        let state = state(db).await;
        let token = jwt_test_token(&state, vec![AuthRole::UserManager]);
        let mut app = state.into_router();

        let response = help_get_all(&mut app, &token).await;
        assert_eq!(response.status(), StatusCode::OK);

        let body = response.into_body().collect().await.unwrap().to_bytes();
        let mut users: Vec<UserDetails> = serde_json::from_slice(&body).unwrap();
        users.iter_mut().for_each(|user| user.roles.sort());
        users.sort_by(|a, b| a.id.cmp(&b.id));

        assert_eq!(users, vec![admin(), user_1()]);
    }

    #[sqlx::test(fixtures("users", "vens", "business"))]
    pub async fn add(db: PgPool) {
        let state = state(db).await;
        let token = jwt_test_token(&state, vec![AuthRole::UserManager]);
        let mut app = state.into_router();

        let new_user = NewUser {
            roles: all_roles(),
            ..new_user()
        };

        let response = help_add_user(&mut app, &token, &new_user).await;
        assert_eq!(response.status(), StatusCode::CREATED);

        let user = UserDetails::from(response).await;
        assert_eq!(new_user, user);

        let response = help_get(&mut app, &token, user.id()).await;
        assert_eq!(response.status(), StatusCode::OK);

        let user2 = UserDetails::from(response).await;
        assert_eq!(user2, user);
    }

    #[sqlx::test(fixtures("users", "vens", "business"))]
    async fn edit(db: PgPool) {
        let state = state(db).await;
        let token = jwt_test_token(&state, vec![AuthRole::UserManager]);
        let mut app = state.into_router();

        let new_users = [
            NewUser {
                roles: vec![],
                ..new_user()
            },
            NewUser {
                roles: all_roles(),
                ..new_user()
            },
        ];
        for new_user in new_users {
            let response = help_edit_user(&mut app, &token, "admin", &new_user).await;
            assert_eq!(response.status(), StatusCode::OK);

            let user = UserDetails::from(response).await;
            assert_eq!(new_user, user);

            let response = help_get(&mut app, &token, user.id()).await;
            assert_eq!(response.status(), StatusCode::OK);

            let user2 = UserDetails::from(response).await;
            assert_eq!(user2, user);
        }
    }

    #[sqlx::test(fixtures("users"))]
    async fn add_credential(db: PgPool) {
        let state = state(db).await;
        let token = jwt_test_token(&state, vec![AuthRole::UserManager]);
        let mut app = state.into_router();

        let new_credential = NewCredential {
            client_id: "test".to_string(),
            client_secret: "test".to_string(),
        };

        let response = help_add_credential(&mut app, &token, "admin", &new_credential).await;
        assert_eq!(response.status(), StatusCode::OK);

        let user = UserDetails::from(response).await;
        assert!(user.client_ids.contains(&"test".to_string()));

        let response = help_login(
            &mut app,
            &new_credential.client_id,
            &new_credential.client_secret,
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[sqlx::test(fixtures("users"))]
    async fn delete_credential(db: PgPool) {
        let state = state(db).await;
        let token = jwt_test_token(&state, vec![AuthRole::UserManager]);
        let mut app = state.into_router();

        let response = help_login(&mut app, "admin", "admin").await;
        assert_eq!(response.status(), StatusCode::OK);

        let response = help_delete(&mut app, &token, "/users/admin/admin").await;
        assert_eq!(response.status(), StatusCode::OK);

        let response = help_login(&mut app, "admin", "admin").await;
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[sqlx::test(fixtures("users"))]
    async fn delete_user(db: PgPool) {
        let state = state(db).await;
        let token = jwt_test_token(&state, vec![AuthRole::UserManager]);
        let mut app = state.into_router();

        let response = help_login(&mut app, "admin", "admin").await;
        assert_eq!(response.status(), StatusCode::OK);

        let response = help_delete(&mut app, &token, "/users/admin").await;
        assert_eq!(response.status(), StatusCode::OK);

        let response = help_login(&mut app, "admin", "admin").await;
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }
}
