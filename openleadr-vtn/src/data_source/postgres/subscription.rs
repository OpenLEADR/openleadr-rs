use crate::{
    api::subscription::QueryParams,
    data_source::{postgres::to_json_value, Crud, SubscriptionCrud},
    error::AppError,
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use openleadr_wire::{
    subscription::{Subscription, SubscriptionId, SubscriptionRequest},
    target::Target,
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
    client_id: ClientId,
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
        let resource: Subscription = sqlx::query_as!(
            PostgresSubscription,
            r#"
            INSERT INTO resource (
                id,
                created_date_time,
                modification_date_time,
                resource_name,
                ven_id,
                attributes,
                targets
            )
            VALUES (gen_random_uuid(), now(), now(), $1, $2, $3, $4)
            RETURNING
                id,
                created_date_time,
                modification_date_time,
                resource_name,
                ven_id,
                attributes,
                targets as "targets:Vec<Target>"
            "#,
            new.resource_name,
            new.ven_id.as_str(),
            to_json_value(new.attributes)?,
            new.targets as _,
        )
        .fetch_one(&self.db)
        .await?
        .try_into()?;

        Ok(resource)
    }

    async fn retrieve(
        &self,
        id: &Self::Id,
        client_id: &Self::PermissionFilter,
    ) -> Result<Self::Type, Self::Error> {
        let resource = sqlx::query_as!(
            PostgresSubscription,
            r#"
            SELECT
                r.id,
                r.created_date_time,
                r.modification_date_time,
                r.resource_name,
                r.ven_id,
                r.attributes,
                r.targets as "targets:Vec<Target>"
            FROM resource r
                JOIN ven v on r.ven_id = v.id
            WHERE r.id = $1
              AND ($2::text IS NULL OR v.client_id = $2)
            "#,
            id.as_str(),
            client_id as _
        )
        .fetch_one(&self.db)
        .await?
        .try_into()?;

        Ok(resource)
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
                r.id,
                r.created_date_time,
                r.modification_date_time,
                r.resource_name,
                r.ven_id,
                r.attributes,
                r.targets as "targets:Vec<Target>"
            FROM resource r
                JOIN ven v on r.ven_id = v.id
            WHERE ($1::text IS NULL OR r.ven_id = $1)
                AND ($2::text IS NULL OR r.resource_name = $2)
                AND ($3::text[] IS NULL OR r.targets @> $3)
                AND ($4::text IS NULL OR v.client_id = $4)
            ORDER BY r.created_date_time
            OFFSET $5 LIMIT $6
            "#,
            filter.ven_id as _,
            filter.resource_name,
            filter.targets.as_deref() as _,
            client_id as _,
            filter.skip,
            filter.limit,
        )
        .fetch_all(&self.db)
        .await?
        .into_iter()
        .map(TryInto::try_into)
        .collect::<Result<Vec<_>, _>>()?;

        trace!("retrieved {} resources", res.len());

        Ok(res)
    }

    async fn update(
        &self,
        id: &Self::Id,
        new: Self::NewType,
        client_id: &Self::PermissionFilter,
    ) -> Result<Self::Type, Self::Error> {
        let mut tx = self.db.begin().await?;

        let old_ven_id = sqlx::query_scalar!(
            r#"
            SELECT ven_id FROM resource WHERE id = $1
            "#,
            id.as_str()
        )
        .fetch_one(&mut *tx)
        .await?;

        if old_ven_id != new.ven_id.as_str() {
            let error = "Tried to update `ven_id` of resource. \
            This is not allowed in the current version of openLEADR as the specification is not quite \
            clear about if that should be allowed. If you disagree with that interpretation, please open \
            an issue on GitHub.";
            error!(resource_id = id.as_str(), "{}", error);
            return Err(Self::Error::BadRequest(error));
        }

        let resource: Subscription = sqlx::query_as!(
            PostgresSubscription,
            r#"
            UPDATE resource r
            SET modification_date_time = now(),
                resource_name = $2,
                attributes = $3,
                targets = $4
            FROM ven v
            WHERE r.ven_id = v.id
              AND r.id = $1
              AND ($5::text IS NULL OR v.client_id = $5)
            RETURNING
                r.id,
                r.created_date_time,
                r.modification_date_time,
                r.resource_name,
                r.ven_id,
                r.attributes,
                r.targets as "targets:Vec<Target>"
            "#,
            id.as_str(),
            new.resource_name,
            to_json_value(new.attributes)?,
            new.targets as _,
            client_id as _
        )
        .fetch_one(&mut *tx)
        .await?
        .try_into()?;

        tx.commit().await?;

        Ok(resource)
    }

    async fn delete(
        &self,
        id: &Self::Id,
        client_id: &Self::PermissionFilter,
    ) -> Result<Self::Type, Self::Error> {
        Ok(sqlx::query_as!(
            PostgresSubscription,
            r#"
            DELETE FROM resource r
                   USING ven v
            WHERE r.ven_id = v.id
              AND r.id = $1
              AND ($2::text IS NULL OR v.client_id = $2)
            RETURNING
                r.id,
                r.created_date_time,
                r.modification_date_time,
                r.resource_name,
                r.ven_id,
                r.attributes,
                r.targets as "targets:Vec<Target>"
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

    impl QueryParams {
        fn program_id(program_id: &str) -> QueryParams {
            Self {
                program_id: Some(program_id.parse().unwrap()),
                ..Self::default()
            }
        }
    }

    #[sqlx::test(fixtures("users", "vens", "resources", "subscriptions"))] // FIXME remove unnecessary fixtures
    async fn retrieve_all(db: PgPool) {
        let repo = PgSubscriptionStorage::from(db.clone());

        let subscription = repo
            .retrieve_all(
                &QueryParams::program_id("program-1"),
                &Some("ven-1-client-id".parse().unwrap()),
            )
            .await
            .unwrap();
        assert_eq!(subscription.len(), 2);

        let subscription = repo
            .retrieve_all(
                &QueryParams::program("program-2"),
                &Some("ven-2-client-id".parse().unwrap()),
            )
            .await
            .unwrap();
        assert_eq!(subscription.len(), 3);

        let filters = QueryParams {
            resource_name: Some("resource-1-name".to_string()),
            ven_id: Some("ven-1".parse().unwrap()),
            ..Default::default()
        };

        let resources = repo
            .retrieve_all(&filters, &Some("ven-1-client-id".parse().unwrap()))
            .await
            .unwrap();
        assert_eq!(resources.len(), 1);
        assert_eq!(resources[0].content.resource_name, "resource-1-name");

        // Ensure a client cannot see resources of another client
        let resources = repo
            .retrieve_all(
                &QueryParams::program_id("program-2"),
                &Some("ven-1-client-id".parse().unwrap()),
            )
            .await
            .unwrap();
        assert_eq!(resources.len(), 0);
    }
}
