use crate::{
    data_source::{AuthInfo, AuthSource, UserDetails},
    error::AppError,
    jwt::Scope,
};
use argon2::{
    password_hash::{rand_core::OsRng, SaltString},
    Argon2, PasswordHash, PasswordHasher, PasswordVerifier,
};
use async_trait::async_trait;
use openleadr_wire::ClientId;
use sqlx::{Executor, PgPool, Postgres};
use tracing::warn;

pub struct PgAuthSource {
    db: PgPool,
}

impl From<PgPool> for PgAuthSource {
    fn from(db: PgPool) -> Self {
        Self { db }
    }
}

#[async_trait]
impl AuthSource for PgAuthSource {
    async fn check_credentials(&self, client_id: &str, client_secret: &str) -> Option<AuthInfo> {
        let db_entry = sqlx::query!(
            r#"
            SELECT id,
                   client_secret,
                   scopes as "scopes:Vec<Scope>"
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

        Some(AuthInfo {
            client_id: client_id.to_string(),
            scope: db_entry.scopes,
        })
    }

    async fn get_user(&self, user_id: &str) -> Result<UserDetails, AppError> {
        Self::get_user(&self.db, user_id).await
    }

    async fn get_all_users(&self) -> Result<Vec<UserDetails>, AppError> {
        Ok(sqlx::query_as!(
            UserDetails,
            r#"
            SELECT u.id,
                   u.reference,
                   u.description,
                   u.created,
                   u.modified,
                   u.scopes as "scope:Vec<Scope>",
                   array_agg(DISTINCT c.client_id) FILTER ( WHERE c.client_id IS NOT NULL ) AS "client_ids!:Vec<ClientId>"
            FROM "user" u
                     LEFT JOIN user_credentials c ON c.user_id = u.id
            GROUP BY u.id,
                     u.created
            ORDER BY u.created
            "#,
        )
        .fetch_all(&self.db)
        .await?)
    }

    async fn add_user(
        &self,
        reference: &str,
        description: Option<&str>,
        scope: &[Scope],
    ) -> Result<UserDetails, AppError> {
        Ok(sqlx::query_as!(
            UserDetails,
            r#"
            INSERT INTO "user" (id, reference, description, scopes, created, modified)
            VALUES (gen_random_uuid(), $1, $2, $3, now(), now())
            RETURNING id, reference, description, scopes as "scope:Vec<Scope>", created, modified, array[]::text[] AS "client_ids!:Vec<ClientId>"
            "#,
            reference,
            description,
            scope as _
        )
        .fetch_one(&self.db)
        .await?
        )
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
        scope: &[Scope],
    ) -> Result<UserDetails, AppError> {
        let mut tx = self.db.begin().await?;

        sqlx::query!(
            r#"
            UPDATE "user" SET
                reference = $2,
                description = $3,
                scopes = $4,
                modified = now()
            WHERE id = $1
            "#,
            user_id,
            reference,
            description,
            scope as _
        )
        .execute(&mut *tx)
        .await?;

        let user = Self::get_user(&mut *tx, user_id)
            .await
            .inspect_err(|err| warn!("cannot find user just updated: {}", err))?;

        tx.commit().await?;
        Ok(user)
    }
}

impl PgAuthSource {
    async fn get_user<'c, E>(db: E, user_id: &str) -> Result<UserDetails, AppError>
    where
        E: Executor<'c, Database = Postgres>,
    {
        Ok(sqlx::query_as!(
            UserDetails,
            r#"
            SELECT u.id,
                   u.reference,
                   u.description,
                   u.created,
                   u.modified,
                   u.scopes as "scope:Vec<Scope>",
                   array_agg(DISTINCT c.client_id) FILTER ( WHERE c.client_id IS NOT NULL ) AS "client_ids!:Vec<ClientId>"
            FROM "user" u
                     LEFT JOIN user_credentials c ON c.user_id = u.id
            WHERE u.id = $1
            GROUP BY u.id
            "#,
            user_id
        )
        .fetch_one(db)
        .await?)
    }
}
