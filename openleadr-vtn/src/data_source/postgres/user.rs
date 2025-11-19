use crate::{
    data_source::{AuthInfo, AuthSource, UserDetails},
    error::AppError,
    jwt::AuthRole,
};
use argon2::{
    password_hash::{rand_core::OsRng, SaltString},
    Argon2, PasswordHash, PasswordHasher, PasswordVerifier,
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::{Executor, PgConnection, PgPool, Postgres};
use tracing::warn;
use crate::jwt::Scope;

pub struct PgAuthSource {
    db: PgPool,
}

impl From<PgPool> for PgAuthSource {
    fn from(db: PgPool) -> Self {
        Self { db }
    }
}

#[derive(Debug)]
struct IntermediateUser {
    id: String,
    reference: String,
    description: Option<String>,
    client_ids: Option<Vec<String>>,
    created: DateTime<Utc>,
    modified: DateTime<Utc>,
    business_ids: Option<Vec<String>>,
    ven_ids: Option<Vec<String>>,
    is_any_business_user: bool,
    is_user_manager: bool,
    is_ven_manager: bool,
    scopes: Vec<Scope>
}

impl TryFrom<IntermediateUser> for UserDetails {
    type Error = AppError;

    fn try_from(u: IntermediateUser) -> Result<Self, Self::Error> {
        let mut roles = Vec::new();
        if let Some(business_ids) = u.business_ids {
            roles.append(
                &mut business_ids
                    .into_iter()
                    .map(|id| Ok(AuthRole::Business(id.to_string())))
                    .collect::<Result<Vec<_>, AppError>>()?,
            )
        }

        if let Some(ven_ids) = u.ven_ids {
            roles.append(
                &mut ven_ids
                    .into_iter()
                    .map(|id| Ok(AuthRole::VEN(id.parse()?)))
                    .collect::<Result<Vec<_>, AppError>>()?,
            )
        }

        if u.is_user_manager {
            roles.push(AuthRole::UserManager);
        }

        if u.is_ven_manager {
            roles.push(AuthRole::VenManager)
        }

        if u.is_any_business_user {
            roles.push(AuthRole::AnyBusiness)
        }

        Ok(Self {
            id: u.id,
            reference: u.reference,
            description: u.description,
            roles,
            client_ids: u.client_ids.unwrap_or_default(),
            created: u.created,
            modified: u.modified,
        })
    }
}

struct IdAndSecret {
    id: String,
    client_secret: String,
}

#[async_trait]
impl AuthSource for PgAuthSource {
    async fn check_credentials(&self, client_id: &str, client_secret: &str) -> Option<AuthInfo> {
        let db_entry = sqlx::query_as!(
            IdAndSecret,
            r#"
            SELECT id,
                   client_secret
            FROM "user"
                JOIN user_credentials ON user_id = id
            WHERE client_id = $1
            "#,
            client_id,
        )
        .fetch_one(&self.db)
        .await
        .ok()?;

        let parsed_hash = PasswordHash::new(&db_entry.client_secret)
            .inspect_err(|err| warn!("Failed to parse client_secret_hash in DB: {}", err))
            .ok()?;

        Argon2::default()
            .verify_password(client_secret.as_bytes(), &parsed_hash)
            .ok()?;

        let user = Self::get_user(&self.db, &db_entry.id)
            .await
            .inspect_err(|err| warn!(client_id, "error fetching user: {err}"))
            .ok()?;

        Some(AuthInfo {
            client_id: client_id.to_string(),
            roles: user.roles,
        })
    }

    async fn get_user(&self, user_id: &str) -> Result<UserDetails, AppError> {
        Self::get_user(&self.db, user_id).await
    }

    async fn get_all_users(&self) -> Result<Vec<UserDetails>, AppError> {
        sqlx::query_as!(
            IntermediateUser,
            r#"
            SELECT u.id,
                   u.reference,
                   u.description,
                   u.created,
                   u.modified,
                   u.scopes as "scopes:Vec<Scope>",
                   array_agg(DISTINCT c.client_id) FILTER ( WHERE c.client_id IS NOT NULL )     AS client_ids,
                   array_agg(DISTINCT b.business_id) FILTER ( WHERE b.business_id IS NOT NULL ) AS business_ids,
                   array_agg(DISTINCT ven.ven_id) FILTER ( WHERE ven.ven_id IS NOT NULL )       AS ven_ids,
                   ab.user_id IS NOT NULL                                                       AS "is_any_business_user!",
                   um.user_id IS NOT NULL                                                       AS "is_user_manager!",
                   vm.user_id IS NOT NULL                                                       AS "is_ven_manager!"
            FROM "user" u
                     LEFT JOIN user_credentials c ON c.user_id = u.id
                     LEFT JOIN any_business_user ab ON u.id = ab.user_id
                     LEFT JOIN user_business b ON u.id = b.user_id
                     LEFT JOIN user_manager um ON u.id = um.user_id
                     LEFT JOIN user_ven ven ON u.id = ven.user_id
                     LEFT JOIN ven_manager vm ON u.id = vm.user_id
            GROUP BY u.id,
                     u.created,
                     b.user_id,
                     ab.user_id,
                     um.user_id,
                     ven.user_id,
                     vm.user_id
            ORDER BY u.created
            "#,
        )
        .fetch_all(&self.db)
        .await?
        .into_iter()
        .map(TryInto::try_into)
        .collect()
    }

    async fn add_user(
        &self,
        reference: &str,
        description: Option<&str>,
        roles: &[AuthRole],
    ) -> Result<UserDetails, AppError> {
        let mut tx = self.db.begin().await?;

        let user_id = sqlx::query_scalar!(
            r#"
            INSERT INTO "user" (id, reference, description, created, modified)
            VALUES (gen_random_uuid(), $1, $2, now(), now())
            RETURNING id
            "#,
            reference,
            description
        )
        .fetch_one(&mut *tx)
        .await?;

        for role in roles {
            Self::add_role(&mut tx, &user_id, role)
                .await
                .inspect_err(|err| {
                    warn!(
                        "Failed to add role {:?} for new user {:?}: {}",
                        role, user_id, err
                    )
                })?;
        }

        let user = Self::get_user(&mut *tx, &user_id)
            .await
            .inspect_err(|err| warn!("cannot find user just created: {}", err))?;

        tx.commit().await?;
        Ok(user)
    }

    async fn add_credential(
        &self,
        user_id: &str,
        client_id: &str,
        client_secret: &str,
    ) -> Result<UserDetails, AppError> {
        let salt = SaltString::generate(&mut OsRng);

        let argon2 = Argon2::default();
        let hash = argon2
            .hash_password(client_secret.as_bytes(), &salt)?
            .to_string();

        let mut tx = self.db.begin().await?;

        sqlx::query!(
            r#"
            INSERT INTO user_credentials 
                (user_id, client_id, client_secret) 
            VALUES 
                ($1, $2, $3)
            "#,
            user_id,
            client_id,
            &hash
        )
        .execute(&mut *tx)
        .await?;
        let user = Self::get_user(&mut *tx, user_id).await?;
        tx.commit().await?;

        Ok(user)
    }

    async fn remove_credentials(
        &self,
        user_id: &str,
        client_id: &str,
    ) -> Result<UserDetails, AppError> {
        let mut tx = self.db.begin().await?;
        sqlx::query!(
            r#"
            DELETE FROM user_credentials WHERE user_id = $1 AND client_id = $2
            "#,
            user_id,
            client_id
        )
        .execute(&mut *tx)
        .await?;
        let user = Self::get_user(&mut *tx, user_id).await?;
        tx.commit().await?;
        Ok(user)
    }

    async fn remove_user(&self, user_id: &str) -> Result<UserDetails, AppError> {
        let user = Self::get_user(&self.db, user_id).await?;
        sqlx::query!(
            r#"
            DELETE FROM "user" WHERE id = $1
            "#,
            user_id
        )
        .execute(&self.db)
        .await?;

        Ok(user)
    }

    async fn edit_user(
        &self,
        user_id: &str,
        reference: &str,
        description: Option<&str>,
        roles: &[AuthRole],
    ) -> Result<UserDetails, AppError> {
        let mut tx = self.db.begin().await?;

        sqlx::query!(
            r#"
            UPDATE "user" SET
                reference = $2,
                description = $3,
                modified = now()
            WHERE id = $1
            "#,
            user_id,
            reference,
            description
        )
        .execute(&mut *tx)
        .await?;

        Self::delete_all_roles(&mut tx, user_id).await?;

        for role in roles {
            Self::add_role(&mut tx, user_id, role)
                .await
                .inspect_err(|err| {
                    warn!(
                        "Failed to add role {:?} for updated user {:?}: {}",
                        role, user_id, err
                    )
                })?;
        }
        let user = Self::get_user(&mut *tx, user_id)
            .await
            .inspect_err(|err| warn!("cannot find user just updated: {}", err))?;

        tx.commit().await?;
        Ok(user)
    }
}

impl PgAuthSource {
    async fn delete_all_roles(db: &mut PgConnection, user_id: &str) -> Result<(), AppError> {
        sqlx::query!(
            r#"
            DELETE FROM user_ven WHERE user_id = $1 
            "#,
            user_id
        )
        .execute(&mut *db)
        .await?;

        sqlx::query!(
            r#"
            DELETE FROM user_business WHERE user_id = $1 
            "#,
            user_id
        )
        .execute(&mut *db)
        .await?;

        sqlx::query!(
            r#"
            DELETE FROM any_business_user WHERE user_id = $1 
            "#,
            user_id
        )
        .execute(&mut *db)
        .await?;

        sqlx::query!(
            r#"
            DELETE FROM ven_manager WHERE user_id = $1 
            "#,
            user_id
        )
        .execute(&mut *db)
        .await?;

        sqlx::query!(
            r#"
            DELETE FROM user_manager WHERE user_id = $1 
            "#,
            user_id
        )
        .execute(&mut *db)
        .await?;

        Ok(())
    }

    async fn add_role(
        tx: &mut PgConnection,
        user_id: &str,
        role: &AuthRole,
    ) -> Result<(), AppError> {
        match role {
            AuthRole::Business(b_id) => sqlx::query!(
                r#"
                INSERT INTO user_business (user_id, business_id) VALUES ($1, $2)
                "#,
                user_id,
                b_id
            ),
            AuthRole::AnyBusiness => sqlx::query!(
                r#"
                INSERT INTO any_business_user (user_id) VALUES ($1)
                "#,
                user_id
            ),
            AuthRole::VEN(v_id) => sqlx::query!(
                r#"
                INSERT INTO user_ven (user_id, ven_id) VALUES ($1, $2)
                "#,
                user_id,
                v_id.as_str()
            ),
            AuthRole::VenManager => sqlx::query!(
                r#"
                INSERT INTO ven_manager (user_id) VALUES ($1)
                "#,
                user_id
            ),
            AuthRole::UserManager => sqlx::query!(
                r#"
                INSERT INTO user_manager (user_id) VALUES ($1)
                "#,
                user_id
            ),
        }
        .execute(&mut *tx)
        .await?;

        Ok(())
    }

    async fn get_user<'c, E>(db: E, user_id: &str) -> Result<UserDetails, AppError>
    where
        E: Executor<'c, Database = Postgres>,
    {
        sqlx::query_as!(
            IntermediateUser,
            r#"
            SELECT u.id,
                   u.reference,
                   u.description,
                   u.created,
                   u.modified,
                   u.scopes as "scopes:Vec<Scope>",
                   array_agg(DISTINCT c.client_id) FILTER ( WHERE c.client_id IS NOT NULL )     AS client_ids,
                   array_agg(DISTINCT b.business_id) FILTER ( WHERE b.business_id IS NOT NULL ) AS business_ids,
                   array_agg(DISTINCT ven.ven_id) FILTER ( WHERE ven.ven_id IS NOT NULL )       AS ven_ids,
                   ab.user_id IS NOT NULL                                                       AS "is_any_business_user!",
                   um.user_id IS NOT NULL                                                       AS "is_user_manager!",
                   vm.user_id IS NOT NULL                                                       AS "is_ven_manager!"
            FROM "user" u
                     LEFT JOIN user_credentials c ON c.user_id = u.id
                     LEFT JOIN any_business_user ab ON u.id = ab.user_id
                     LEFT JOIN user_business b ON u.id = b.user_id
                     LEFT JOIN user_manager um ON u.id = um.user_id
                     LEFT JOIN user_ven ven ON u.id = ven.user_id
                     LEFT JOIN ven_manager vm ON u.id = vm.user_id
            WHERE u.id = $1
            GROUP BY u.id,
                     b.user_id,
                     ab.user_id,
                     um.user_id,
                     ven.user_id,
                     vm.user_id
            "#,
            user_id
        )
        .fetch_one(db)
        .await?
        .try_into()
    }
}
