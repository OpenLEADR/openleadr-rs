use crate::{
    api::resource::QueryParams,
    data_source::{postgres::to_json_value, ResourceCrud, VenScopedCrud},
    error::AppError,
    jwt::User,
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use openleadr_wire::{
    resource::{Resource, ResourceContent, ResourceId},
    target::TargetEntry,
    ven::VenId,
};
use sqlx::PgPool;
use tracing::{error, trace};

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
    ven_id: String,
    attributes: Option<serde_json::Value>,
    targets: Option<serde_json::Value>,
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
                        "Failed to deserialize JSON from DB to `Vec<PayloadDescriptor>`"
                    )
                })
                .map_err(AppError::SerdeJsonInternalServerError)?,
        };
        let targets = match value.targets {
            None => None,
            Some(t) => serde_json::from_value(t)
                .inspect_err(|err| {
                    error!(?err, "Failed to deserialize JSON from DB to `TargetMap`")
                })
                .map_err(AppError::SerdeJsonInternalServerError)?,
        };

        Ok(Self {
            id: value.id.parse()?,
            created_date_time: value.created_date_time,
            modification_date_time: value.modification_date_time,
            ven_id: value.ven_id.parse()?,
            content: ResourceContent {
                resource_name: value.resource_name,
                attributes,
                targets,
            },
        })
    }
}

#[async_trait]
impl VenScopedCrud for PgResourceStorage {
    type Type = Resource;
    type Id = ResourceId;
    type NewType = ResourceContent;
    type Error = AppError;
    type Filter = QueryParams;
    type PermissionFilter = User;

    async fn create(
        &self,
        new: Self::NewType,
        ven_id: VenId,
        _user: &Self::PermissionFilter,
    ) -> Result<Self::Type, Self::Error> {
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
                targets
            )
            VALUES (gen_random_uuid(), now(), now(), $1, $2, $3, $4)
            RETURNING *
            "#,
            new.resource_name,
            ven_id.as_str(),
            to_json_value(new.attributes)?,
            to_json_value(new.targets)?,
        )
        .fetch_one(&self.db)
        .await?
        .try_into()?;

        Ok(resource)
    }

    async fn retrieve(
        &self,
        id: &Self::Id,
        ven_id: VenId,
        _user: &Self::PermissionFilter,
    ) -> Result<Self::Type, Self::Error> {
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
                targets
            FROM resource
            WHERE id = $1 AND ven_id = $2
            "#,
            id.as_str(),
            ven_id.as_str(),
        )
        .fetch_one(&self.db)
        .await?
        .try_into()?;

        Ok(resource)
    }

    async fn retrieve_all(
        &self,
        ven_id: VenId,
        filter: &Self::Filter,
        _user: &Self::PermissionFilter,
    ) -> Result<Vec<Self::Type>, Self::Error> {
        let target: Option<TargetEntry> = filter.targets.clone().into();
        let target_values = target.as_ref().map(|t| t.values.clone());

        let res = sqlx::query_as!(
            PostgresResource,
            r#"
            SELECT
                r.id AS "id!", 
                r.created_date_time AS "created_date_time!", 
                r.modification_date_time AS "modification_date_time!",
                r.resource_name AS "resource_name!",
                r.ven_id AS "ven_id!",
                r.attributes,
                r.targets
            FROM resource r
              LEFT JOIN LATERAL ( 
                  
                    SELECT targets.r_id,
                           (t ->> 'type' = $3) AND
                           (t -> 'values' ?| $4) AS target_test
                    FROM (SELECT resource.id                            AS r_id,
                                 jsonb_array_elements(resource.targets) AS t
                          FROM resource) AS targets
                  
                   )
                  ON r.id = r_id
            WHERE r.ven_id = $1
                AND ($2::text IS NULL OR r.resource_name = $2)
                AND ($3 IS NULL OR $4 IS NULL OR target_test)
            ORDER BY r.created_date_time
            OFFSET $5 LIMIT $6
            "#,
            ven_id.as_str(),
            filter.resource_name,
            target.as_ref().map(|t| t.label.as_str()),
            target_values.as_deref(),
            filter.skip,
            filter.limit,
        )
        .fetch_all(&self.db)
        .await?
        .into_iter()
        .map(TryInto::try_into)
        .collect::<Result<Vec<_>, _>>()?;

        trace!(
            ven_id = ven_id.as_str(),
            "retrieved {} resources",
            res.len()
        );

        Ok(res)
    }

    async fn update(
        &self,
        id: &Self::Id,
        ven_id: VenId,
        new: Self::NewType,
        _user: &Self::PermissionFilter,
    ) -> Result<Self::Type, Self::Error> {
        let resource: Resource = sqlx::query_as!(
            PostgresResource,
            r#"
            UPDATE resource
            SET modification_date_time = now(),
                resource_name = $3,
                ven_id = $4,
                attributes = $5,
                targets = $6
            WHERE id = $1 AND ven_id = $2
            RETURNING *
            "#,
            id.as_str(),
            ven_id.as_str(),
            new.resource_name,
            ven_id.as_str(),
            to_json_value(new.attributes)?,
            to_json_value(new.targets)?
        )
        .fetch_one(&self.db)
        .await?
        .try_into()?;

        Ok(resource)
    }

    async fn delete(
        &self,
        id: &Self::Id,
        ven_id: VenId,
        _user: &Self::PermissionFilter,
    ) -> Result<Self::Type, Self::Error> {
        Ok(sqlx::query_as!(
            PostgresResource,
            r#"
            DELETE FROM resource r
            WHERE r.id = $1 AND r.ven_id = $2
            RETURNING r.*
            "#,
            id.as_str(),
            ven_id.as_str(),
        )
        .fetch_one(&self.db)
        .await?
        .try_into()?)
    }
}

impl PgResourceStorage {
    pub(crate) async fn retrieve_by_ven(
        db: &PgPool,
        ven_id: &VenId,
    ) -> Result<Vec<Resource>, AppError> {
        sqlx::query_as!(
            PostgresResource,
            r#"
            SELECT
                id,
                created_date_time,
                modification_date_time,
                resource_name,
                ven_id,
                attributes,
                targets
            FROM resource
            WHERE ven_id = $1
            "#,
            ven_id.as_str(),
        )
        .fetch_all(db)
        .await?
        .into_iter()
        .map(TryInto::try_into)
        .collect::<Result<_, _>>()
    }

    pub(crate) async fn retrieve_by_vens(
        db: &PgPool,
        ven_ids: &[String],
    ) -> Result<Vec<Resource>, AppError> {
        sqlx::query_as!(
            PostgresResource,
            r#"
            SELECT
                id,
                created_date_time,
                modification_date_time,
                resource_name,
                ven_id,
                attributes,
                targets
            FROM resource
            WHERE ven_id = ANY($1)
            "#,
            ven_ids,
        )
        .fetch_all(db)
        .await?
        .into_iter()
        .map(TryInto::try_into)
        .collect::<Result<_, _>>()
    }
}

#[cfg(test)]
#[cfg(feature = "live-db-test")]
mod test {
    use crate::{
        api::{resource::QueryParams, TargetQueryParams},
        data_source::{postgres::resource::PgResourceStorage, VenScopedCrud},
        jwt::{AuthRole, User},
    };
    use sqlx::PgPool;

    impl Default for QueryParams {
        fn default() -> Self {
            Self {
                resource_name: None,
                targets: TargetQueryParams {
                    target_type: None,
                    values: None,
                },
                skip: 0,
                limit: 50,
            }
        }
    }

    #[sqlx::test(fixtures("users", "vens", "resources"))]
    async fn retrieve_all(db: PgPool) {
        let repo = PgResourceStorage::from(db.clone());
        let user = User(crate::jwt::Claims::new(vec![AuthRole::VenManager]));

        let resources = repo
            .retrieve_all("ven-1".parse().unwrap(), &Default::default(), &user)
            .await
            .unwrap();
        assert_eq!(resources.len(), 2);

        let resources = repo
            .retrieve_all("ven-2".parse().unwrap(), &Default::default(), &user)
            .await
            .unwrap();
        assert_eq!(resources.len(), 3);

        let filters = QueryParams {
            resource_name: Some("resource-1-name".to_string()),
            ..Default::default()
        };

        let resources = repo
            .retrieve_all("ven-1".parse().unwrap(), &filters, &user)
            .await
            .unwrap();
        assert_eq!(resources.len(), 1);
        assert_eq!(resources[0].content.resource_name, "resource-1-name");
    }
}
