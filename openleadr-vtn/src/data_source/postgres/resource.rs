use crate::{
    api::resource::QueryParams,
    data_source::{postgres::to_json_value, Crud, ResourceCrud},
    error::AppError,
    jwt::User,
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use openleadr_wire::{
    resource::{BlResourceRequest, Resource, ResourceId, ResourceRequest},
    target::Target,
};
use sqlx::PgPool;
use tracing::{error, trace, warn};

#[async_trait]
impl ResourceCrud for PgResourceStorage {}

pub(crate) struct PgResourceStorage {
    db: PgPool,
}

impl From<PgPool> for PgResourceStorage {
    fn from(db: PgPool) -> Self {
        Self { db }
    }
}

#[derive(Debug)]
pub(crate) struct PostgresResource {
    id: String,
    created_date_time: DateTime<Utc>,
    modification_date_time: DateTime<Utc>,
    resource_name: String,
    attributes: Option<serde_json::Value>,
    targets: Vec<Target>,
    client_id: String,
    ven_id: String,
}

impl TryFrom<PostgresResource> for Resource {
    type Error = AppError;

    #[tracing::instrument(name = "TryFrom<PostgresResource> for Resource")]
    fn try_from(value: PostgresResource) -> Result<Self, Self::Error> {
        let attributes = match value.attributes {
            None => None,
            Some(t) => serde_json::from_value(t)
                .inspect_err(|err| {
                    error!(
                        ?err,
                        "Failed to deserialize JSON from DB to `Vec<ValuesMap>`"
                    )
                })
                .map_err(AppError::SerdeJsonInternalServerError)?,
        };

        Ok(Self {
            id: value.id.parse()?,
            created_date_time: value.created_date_time,
            modification_date_time: value.modification_date_time,
            content: BlResourceRequest {
                client_id: value.client_id.parse()?,
                resource_name: value.resource_name,
                ven_id: value.ven_id.parse()?,
                attributes,
                targets: value.targets,
            },
        })
    }
}

#[async_trait]
impl Crud for PgResourceStorage {
    type Type = Resource;
    type Id = ResourceId;
    type NewType = ResourceRequest;
    type Error = AppError;
    type Filter = QueryParams;
    type PermissionFilter = User;

    async fn create(
        &self,
        new: Self::NewType,
        user: &Self::PermissionFilter,
    ) -> Result<Self::Type, Self::Error> {
        // FIXME object privacy
        let _user = user;

        let resource: Resource = sqlx::query_as!(
            PostgresResource,
            r#"
            INSERT INTO resource (
                id,
                created_date_time,
                modification_date_time,
                resource_name,
                ven_id,
                attributes,
                targets,
                client_id
            )
            VALUES (gen_random_uuid(), now(), now(), $1, $2, $3, $4, $5)
            RETURNING
                id,
                created_date_time,
                modification_date_time,
                resource_name,
                ven_id,
                attributes,
                targets as "targets:Vec<Target>",
                client_id
            "#,
            new.resource_name(),
            new.ven_id().as_str(),
            to_json_value(new.attributes())?,
            new.targets() as &[Target],
            // FIXME object privacy
            new.client_id()
                .map(|id| id.as_str())
                .unwrap_or("FIXME-object-privacy")
        )
        .fetch_one(&self.db)
        .await?
        .try_into()?;

        Ok(resource)
    }

    async fn retrieve(
        &self,
        id: &Self::Id,
        user: &Self::PermissionFilter,
    ) -> Result<Self::Type, Self::Error> {
        // Fixme object privacy
        let _user = user;

        let resource = sqlx::query_as!(
            PostgresResource,
            r#"
            SELECT
                id,
                created_date_time,
                modification_date_time,
                resource_name,
                ven_id,
                attributes,
                targets as "targets:Vec<Target>",
                client_id
            FROM resource
            WHERE id = $1
            "#,
            id.as_str(),
        )
        .fetch_one(&self.db)
        .await?
        .try_into()?;

        Ok(resource)
    }

    async fn retrieve_all(
        &self,
        filter: &Self::Filter,
        user: &Self::PermissionFilter,
    ) -> Result<Vec<Self::Type>, Self::Error> {
        // Fixme object privacy
        let _user = user;

        let res = sqlx::query_as!(
            PostgresResource,
            r#"
            SELECT DISTINCT
                r.id AS "id!",
                r.created_date_time AS "created_date_time!",
                r.modification_date_time AS "modification_date_time!",
                r.resource_name AS "resource_name!",
                r.ven_id AS "ven_id!",
                r.attributes,
                r.targets as "targets:Vec<Target>",
                r.client_id
            FROM resource r
            WHERE ($1::text IS NULL OR r.ven_id = $1)
                AND ($2::text IS NULL OR r.resource_name = $2)
                AND ($3::text[] IS NULL OR r.targets && $3)
            ORDER BY r.created_date_time
            OFFSET $4 LIMIT $5
            "#,
            filter.ven_id.as_ref().map(|id| id.to_string()),
            filter.resource_name,
            filter.targets.targets.as_deref(),
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
        _user: &Self::PermissionFilter,
    ) -> Result<Self::Type, Self::Error> {
        let mut tx = self.db.begin().await?;

        let old = sqlx::query!(
            r#"
            SELECT ven_id, client_id FROM resource WHERE id = $1
            "#,
            id.as_str()
        )
        .fetch_one(&mut *tx)
        .await?;

        if old.ven_id != new.ven_id().as_str() {
            let error = "Tried to update `ven_id` of resource. \
            This is not allowed in the current version of openLEADR as the specification is not quite \
            clear about if that should be allowed. If you disagree with that interpretation, please open \
            an issue on GitHub.";
            error!(resource_id = id.as_str(), "{}", error);
            return Err(Self::Error::BadRequest(error));
        }

        if let Some(new_client_id) = new.client_id() {
            if old.client_id != new_client_id.as_str() {
                let error = "Tried to update `client_id` of resource. \
                This is not allowed in the current version of openLEADR as the specification is not quite \
                clear about if that should be allowed. If you disagree with that interpretation, please open \
                an issue on GitHub.";
                error!(resource_id = id.as_str(), "{}", error);
                return Err(Self::Error::BadRequest(error));
            }
        }

        let resource: Resource = sqlx::query_as!(
            PostgresResource,
            r#"
            UPDATE resource
            SET modification_date_time = now(),
                resource_name = $2,
                attributes = $3,
                targets = $4
            WHERE id = $1
            RETURNING
                id,
                created_date_time,
                modification_date_time,
                resource_name,
                ven_id,
                attributes,
                targets as "targets:Vec<Target>",
                client_id
            "#,
            id.as_str(),
            new.resource_name(),
            to_json_value(new.attributes())?,
            new.targets() as &[Target],
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
        _user: &Self::PermissionFilter,
    ) -> Result<Self::Type, Self::Error> {
        Ok(sqlx::query_as!(
            PostgresResource,
            r#"
            DELETE FROM resource r
            WHERE r.id = $1
            RETURNING
                id,
                created_date_time,
                modification_date_time,
                resource_name,
                ven_id,
                attributes,
                targets as "targets:Vec<Target>",
                client_id
            "#,
            id.as_str(),
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
        api::{resource::QueryParams, TargetQueryParams},
        data_source::{postgres::resource::PgResourceStorage, Crud},
        jwt::{AuthRole, User},
    };
    use sqlx::PgPool;

    impl Default for QueryParams {
        fn default() -> Self {
            Self {
                resource_name: None,
                ven_id: None,
                targets: TargetQueryParams { targets: None },
                skip: 0,
                limit: 50,
            }
        }
    }

    impl QueryParams {
        fn ven_id(ven_id: &str) -> QueryParams {
            Self {
                ven_id: Some(ven_id.parse().unwrap()),
                ..Self::default()
            }
        }
    }

    #[sqlx::test(fixtures("users", "vens", "resources"))]
    async fn retrieve_all(db: PgPool) {
        let repo = PgResourceStorage::from(db.clone());
        let user = User(crate::jwt::Claims::new(vec![AuthRole::VenManager]));

        let resources = repo
            .retrieve_all(&QueryParams::ven_id("ven-1"), &user)
            .await
            .unwrap();
        assert_eq!(resources.len(), 2);

        let resources = repo
            .retrieve_all(&QueryParams::ven_id("ven-2"), &user)
            .await
            .unwrap();
        assert_eq!(resources.len(), 3);

        let filters = QueryParams {
            resource_name: Some("resource-1-name".to_string()),
            ven_id: Some("ven-1".parse().unwrap()),
            ..Default::default()
        };

        let resources = repo.retrieve_all(&filters, &user).await.unwrap();
        assert_eq!(resources.len(), 1);
        assert_eq!(resources[0].content.resource_name, "resource-1-name");
    }
}
