use crate::{
    api::subscription::QueryParams,
    data_source::{Crud, SubscriptionCrud},
    error::AppError,
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use openleadr_wire::{
    subscription::{Subscription, SubscriptionId, SubscriptionRequest},
    ClientId,
};
use sqlx::PgPool;
use tracing::{error, trace, warn};

impl SubscriptionCrud for PgSubscriptionStorage {}

pub(crate) struct PgSubscriptionStorage {
    db: PgPool,
}

impl From<PgPool> for PgSubscriptionStorage {
    fn from(db: PgPool) -> Self {
        Self { db }
    }
}

#[derive(Debug)]
pub(crate) struct PostgresSubscription {
    id: String,
    created_date_time: DateTime<Utc>,
    modification_date_time: DateTime<Utc>,
    client_name: String,
    program_id: Option<String>,
    object_operations: serde_json::Value,
    //targets: Vec<Target>,
}

impl TryFrom<PostgresSubscription> for Subscription {
    type Error = AppError;

    #[tracing::instrument(name = "TryFrom<PostgresSubscription> for Subscription")]
    fn try_from(value: PostgresSubscription) -> Result<Self, Self::Error> {
        let object_operations = serde_json::from_value(value.object_operations)
            .inspect_err(|err| {
                error!(
                    ?err,
                    "Failed to deserialize JSON from DB to `Vec<SubscriptionObjectOperation>`"
                )
            })
            .map_err(AppError::SerdeJsonInternalServerError)?;

        Ok(Self {
            id: value.id.parse()?,
            created_date_time: value.created_date_time,
            modification_date_time: value.modification_date_time,
            content: SubscriptionRequest {
                client_name: value.client_name,
                program_id: value
                    .program_id
                    .map(|program_id| program_id.parse())
                    .transpose()?,
                object_operations,
            },
        })
    }
}

#[async_trait]
impl Crud for PgSubscriptionStorage {
    type Type = Subscription;
    type Id = SubscriptionId;
    type NewType = SubscriptionRequest;
    type Error = AppError;
    type Filter = QueryParams;
    type PermissionFilter = Option<ClientId>;

    async fn create(
        &self,
        new: Self::NewType,
        client_id: &Self::PermissionFilter,
    ) -> Result<Self::Type, Self::Error> {
        let subscription: Subscription = sqlx::query_as!(
            PostgresSubscription,
            r#"
            INSERT INTO subscription (
                id,
                created_date_time,
                modification_date_time,
                client_id,
                client_name,
                program_id,
                object_operations
            )
            VALUES (gen_random_uuid(), now(), now(), $1, $2::text, $3, $4)
            RETURNING
                id,
                created_date_time,
                modification_date_time,
                client_name,
                program_id,
                object_operations
            "#,
            client_id
                .as_ref()
                .expect("subscription create requires client id")
                .as_str(),
            new.client_name,
            new.program_id.as_ref().map(|id| id.as_str()),
            serde_json::to_value(new.object_operations).map_err(AppError::SerdeJsonBadRequest)?,
        )
        .fetch_one(&self.db)
        .await?
        .try_into()?;

        Ok(subscription)
    }

    async fn retrieve(
        &self,
        id: &Self::Id,
        client_id: &Self::PermissionFilter,
    ) -> Result<Self::Type, Self::Error> {
        let subscription = sqlx::query_as!(
            PostgresSubscription,
            r#"
            SELECT
                id,
                created_date_time,
                modification_date_time,
                client_name,
                program_id,
                object_operations
            FROM subscription
            WHERE id = $1
              AND ($2::text IS NULL OR client_id = $2)
            "#,
            id.as_str(),
            client_id as _
        )
        .fetch_one(&self.db)
        .await?
        .try_into()?;

        Ok(subscription)
    }

    async fn retrieve_all(
        &self,
        filter: &Self::Filter,
        client_id: &Self::PermissionFilter,
    ) -> Result<Vec<Self::Type>, Self::Error> {
        let res = sqlx::query_as!(
            PostgresSubscription,
            r#"
            SELECT
                id,
                created_date_time,
                modification_date_time,
                client_name,
                program_id,
                object_operations
            FROM subscription
            WHERE ($1::text IS NULL OR client_id = $1)
            ORDER BY created_date_time
            OFFSET $2 LIMIT $3
            "#,
            client_id as _,
            filter.skip,
            filter.limit,
        )
        .fetch_all(&self.db)
        .await?
        .into_iter()
        .map(TryInto::try_into)
        .collect::<Result<Vec<_>, _>>()?;

        trace!("retrieved {} subscriptions", res.len());

        Ok(res)
    }

    async fn update(
        &self,
        id: &Self::Id,
        new: Self::NewType,
        client_id: &Self::PermissionFilter,
    ) -> Result<Self::Type, Self::Error> {
        let subscription: Subscription = sqlx::query_as!(
            PostgresSubscription,
            r#"
            UPDATE subscription
            SET modification_date_time = now(),
                client_name = $2,
                program_id = $3,
                object_operations = $4
            WHERE id = $1
              AND ($5::text IS NULL OR client_id = $5)
            RETURNING
                id,
                created_date_time,
                modification_date_time,
                client_name,
                program_id,
                object_operations
            "#,
            id.as_str(),
            new.client_name,
            new.program_id.as_ref().map(|id| id.as_str()),
            serde_json::to_value(&new.object_operations).map_err(AppError::SerdeJsonBadRequest)?,
            client_id as _
        )
        .fetch_one(&self.db)
        .await?
        .try_into()?;

        Ok(subscription)
    }

    async fn delete(
        &self,
        id: &Self::Id,
        client_id: &Self::PermissionFilter,
    ) -> Result<Self::Type, Self::Error> {
        Ok(sqlx::query_as!(
            PostgresSubscription,
            r#"
            DELETE FROM subscription
            WHERE id = $1
              AND ($2::text IS NULL OR client_id = $2)
            RETURNING
                id,
                created_date_time,
                modification_date_time,
                client_name,
                program_id,
                object_operations
            "#,
            id.as_str(),
            client_id as _
        )
        .fetch_one(&self.db)
        .await?
        .try_into()?)
    }
}

#[cfg(test)]
#[cfg(feature = "live-db-test")]
mod test {
    use crate::{
        api::subscription::QueryParams,
        data_source::{postgres::subscription::PgSubscriptionStorage, Crud},
    };
    use sqlx::PgPool;

    impl Default for QueryParams {
        fn default() -> Self {
            Self {
                program_id: None,
                objects: vec![],
                skip: 0,
                limit: 50,
            }
        }
    }

    #[sqlx::test(fixtures("users", "vens", "resources", "subscriptions"))] // FIXME remove unnecessary fixtures
    async fn retrieve_all(db: PgPool) {
        let repo = PgSubscriptionStorage::from(db.clone());

        let subscription = repo
            .retrieve_all(
                &QueryParams::default(),
                &Some("ven-client-client-id".parse().unwrap()),
            )
            .await
            .unwrap();
        assert_eq!(subscription.len(), 2);

        // Ensure a client cannot see subscriptions of another client
        let subscription = repo
            .retrieve_all(
                &QueryParams::default(),
                &Some("ven-client2-client-id".parse().unwrap()),
            )
            .await
            .unwrap();
        assert_eq!(subscription.len(), 0);
    }
}
