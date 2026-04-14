use crate::{
    api::resource_group::QueryParams,
    data_source::{postgres::to_json_value, Crud, ResourceGroupCrud},
    error::AppError,
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use openleadr_wire::{
    resource::ResourceId,
    resource_group::{BlResourceGroupRequest, ResourceGroup, ResourceGroupChild, ResourceGroupId},
    target::Target,
    ClientId,
};
use sqlx::PgPool;
use tracing::{error, trace, warn};

impl ResourceGroupCrud for PgResourceGroupStorage {}

pub(crate) struct PgResourceGroupStorage {
    db: PgPool,
}

impl From<PgPool> for PgResourceGroupStorage {
    fn from(db: PgPool) -> Self {
        Self { db }
    }
}

#[derive(Debug)]
pub(crate) struct PostgresResourceGroup {
    id: String,
    created_date_time: DateTime<Utc>,
    modification_date_time: DateTime<Utc>,
    resource_group_name: String,
    attributes: Option<serde_json::Value>,
    targets: Vec<Target>,
}

impl TryFrom<PostgresResourceGroup> for ResourceGroup {
    type Error = AppError;

    #[tracing::instrument(name = "TryFrom<PostgresResourceGroup> for ResourceGroup")]
    fn try_from(value: PostgresResourceGroup) -> Result<Self, Self::Error> {
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
            content: BlResourceGroupRequest {
                resource_group_name: value.resource_group_name,
                attributes,
                targets: value.targets,
                children: vec![],
            },
        })
    }
}

impl PgResourceGroupStorage {
    async fn get_resource_group_children(
        &self,
        rg: &ResourceGroup,
    ) -> Result<Vec<ResourceGroupChild>, <PgResourceGroupStorage as Crud>::Error> {
        let mut rg_children: Vec<_> = sqlx::query!(
            r#"
                SELECT rg_child_rg_id FROM rg_child_rg
                WHERE rg_parent_rg_id = $1
            "#,
            rg.id.as_str()
        )
        .fetch_all(&self.db)
        .await?
        .into_iter()
        .map(|r| {
            // TODO: should this use expect?
            let rg_id = ResourceGroupId::new(&r.rg_child_rg_id).expect(&format!(
                "{} is not a valid ResourceGroupId",
                r.rg_child_rg_id
            ));
            ResourceGroupChild::ResourceGroup(rg_id)
        })
        .collect();

        let ven_children = sqlx::query!(
            r#"
                SELECT rg_child_ven_resource_id FROM rg_child_ven_resource
                WHERE rg_parent_rg_id = $1
            "#,
            rg.id.as_str()
        )
        .fetch_all(&self.db)
        .await?
        .into_iter()
        .map(|r| {
            let ven_id = ResourceId::new(&r.rg_child_ven_resource_id).expect(&format!(
                "{} is not a valid ResourceId",
                r.rg_child_ven_resource_id
            ));
            ResourceGroupChild::VenResource(ven_id)
        });

        rg_children.extend(ven_children);
        Ok(rg_children)
    }

    async fn insert_resource_group_children(
        &self,
        rg_id: &ResourceGroupId,
        children: &[ResourceGroupChild],
    ) -> Result<(), <PgResourceGroupStorage as Crud>::Error> {
        let (rg_children, ven_children) = children.iter().fold(
            (Vec::new(), Vec::new()),
            |(mut rg_children, mut ven_children), child| match child {
                ResourceGroupChild::ResourceGroup(id) => {
                    rg_children.push(id.to_string());
                    (rg_children, ven_children)
                }
                ResourceGroupChild::VenResource(id) => {
                    ven_children.push(id.to_string());
                    (rg_children, ven_children)
                }
            },
        );

        sqlx::query!(
            r#"
                INSERT INTO rg_child_rg (rg_parent_rg_id, rg_child_rg_id)
                SELECT $1, child FROM UNNEST($2::text[]) as u(child)
            "#,
            rg_id.as_str(),
            &rg_children
        )
        .execute(&self.db)
        .await?;

        sqlx::query!(
            r#"
                INSERT INTO rg_child_ven_resource (rg_parent_rg_id, rg_child_ven_resource_id)
                SELECT $1, child FROM UNNEST ($2::text[]) as u(child)
            "#,
            rg_id.as_str(),
            &ven_children
        )
        .execute(&self.db)
        .await?;

        Ok(())
    }
}

#[async_trait]
impl Crud for PgResourceGroupStorage {
    type Type = ResourceGroup;
    type Id = ResourceGroupId;
    type NewType = BlResourceGroupRequest;
    type Error = AppError;
    type Filter = QueryParams;
    type PermissionFilter = Option<ClientId>;

    async fn create(
        &self,
        new: Self::NewType,
        _client_id: &Self::PermissionFilter,
    ) -> Result<Self::Type, Self::Error> {
        // Add children to PgResourceGroup even though this does not get stored in that table
        // Or: Add children via separate function, bypassing the try_into interface
        //
        // Option 2 sounds better, but maybe there's a hidden third option
        // This does separate the creation into two distinct steps

        let mut resource_group: ResourceGroup = sqlx::query_as!(
            PostgresResourceGroup,
            r#"
            INSERT INTO resource_group (
                id, created_date_time, modification_date_time, resource_group_name, attributes, targets
            )
            VALUES (
                gen_random_uuid()::text, now(), now(), $1, $2, $3)
            RETURNING
                id,
                created_date_time,
                modification_date_time,
                resource_group_name,
                attributes,
                targets as "targets:Vec<Target>"
            "#,
            new.resource_group_name,
            to_json_value(new.attributes)?,
            new.targets as _,
        )
        .fetch_one(&self.db)
        .await?
        .try_into()?;

        self.insert_resource_group_children(&resource_group.id, &new.children)
            .await?;

        resource_group.content.children.extend(new.children);
        Ok(resource_group)
    }

    async fn retrieve(
        &self,
        id: &Self::Id,
        // TODO: Does client_id still make sense? Check with other retrieve funcs
        _client_id: &Self::PermissionFilter,
    ) -> Result<Self::Type, Self::Error> {
        let mut resource_group: ResourceGroup = sqlx::query_as!(
            PostgresResourceGroup,
            r#"
            SELECT
                rg.id,
                rg.created_date_time,
                rg.modification_date_time,
                rg.resource_group_name,
                rg.attributes,
                rg.targets as "targets:Vec<Target>"
            FROM resource_group rg
            WHERE rg.id = $1
            "#,
            id.as_str(),
        )
        .fetch_one(&self.db)
        .await?
        .try_into()?;

        resource_group
            .content
            .children
            .extend(self.get_resource_group_children(&resource_group).await?);

        Ok(resource_group)
    }

    async fn retrieve_all(
        &self,
        filter: &Self::Filter,
        _client_id: &Self::PermissionFilter,
    ) -> Result<Vec<Self::Type>, Self::Error> {
        let mut rgs = sqlx::query_as!(
            PostgresResourceGroup,
            r#"
            SELECT
                rg.id,
                rg.created_date_time,
                rg.modification_date_time,
                rg.resource_group_name,
                rg.attributes,
                rg.targets as "targets:Vec<Target>"
            FROM resource_group rg
            WHERE ($1::text IS NULL OR rg.resource_group_name = $1)
                AND ($2::text[] IS NULL OR rg.targets @> $2)
            ORDER BY rg.created_date_time
            OFFSET $3 LIMIT $4
            "#,
            filter.resource_group_name,
            filter.targets.as_deref() as _,
            filter.skip,
            filter.limit,
        )
        .fetch_all(&self.db)
        .await?
        .into_iter()
        .map(TryInto::try_into)
        .collect::<Result<Vec<ResourceGroup>, _>>()?;

        for rg in rgs.iter_mut() {
            rg.content
                .children
                .extend(self.get_resource_group_children(&rg).await?);
        }

        trace!("retrieved {} resources", rgs.len());

        Ok(rgs)
    }

    async fn update(
        &self,
        id: &Self::Id,
        new: Self::NewType,
        _client_id: &Self::PermissionFilter,
    ) -> Result<Self::Type, Self::Error> {
        let resource_group: ResourceGroup = sqlx::query_as!(
            PostgresResourceGroup,
            r#"
            UPDATE resource_group rg
            SET modification_date_time = now(),
                resource_group_name = $2,
                attributes = $3,
                targets = $4
            WHERE rg.id = $1
            RETURNING
                rg.id,
                rg.created_date_time,
                rg.modification_date_time,
                rg.resource_group_name,
                rg.attributes,
                rg.targets as "targets:Vec<Target>"
            "#,
            id.as_str(),
            new.resource_group_name,
            to_json_value(new.attributes)?,
            new.targets as _,
        )
        .fetch_one(&self.db)
        .await?
        .try_into()?;

        // TODO: Is this safe?
        sqlx::query!(
            r#"
                DELETE FROM rg_child_ven_resource WHERE rg_parent_rg_id = $1
            "#,
            id.as_str()
        )
        .execute(&self.db)
        .await?;

        self.insert_resource_group_children(id, &new.children)
            .await?;

        Ok(resource_group)
    }

    async fn delete(
        &self,
        id: &Self::Id,
        _client_id: &Self::PermissionFilter,
    ) -> Result<Self::Type, Self::Error> {
        // TODO: Does this cascade and thus remove all entries in rg_child_ven_resource and
        // rg_child_rg with this parent rg id
        Ok(sqlx::query_as!(
            PostgresResourceGroup,
            r#"
            DELETE FROM resource_group rg
            WHERE rg.id = $1
            RETURNING
                rg.id,
                rg.created_date_time,
                rg.modification_date_time,
                rg.resource_group_name,
                rg.attributes,
                rg.targets as "targets:Vec<Target>"
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
        api::{resource_group::QueryParams, TargetQueryParams},
        data_source::{postgres::resource_group::PgResourceGroupStorage, Crud},
    };
    use sqlx::PgPool;

    impl Default for QueryParams {
        fn default() -> Self {
            Self {
                resource_group_name: None,
                targets: TargetQueryParams(None),
                skip: 0,
                limit: 50,
            }
        }
    }

    #[sqlx::test(fixtures("users", "vens", "resources"))]
    async fn retrieve_all(db: PgPool) {
        let repo = PgResourceGroupStorage::from(db.clone());

        let resources = repo
            .retrieve_all(
                &QueryParams::default(),
                &Some("ven-1-client-id".parse().unwrap()),
            )
            .await
            .unwrap();
        assert_eq!(resources.len(), 2);

        let resources = repo
            .retrieve_all(
                &QueryParams::default(),
                &Some("ven-2-client-id".parse().unwrap()),
            )
            .await
            .unwrap();
        assert_eq!(resources.len(), 3);

        let filters = QueryParams {
            resource_group_name: Some("resource-1-name".to_string()),
            ..Default::default()
        };

        let resources = repo
            .retrieve_all(&filters, &Some("ven-1-client-id".parse().unwrap()))
            .await
            .unwrap();
        assert_eq!(resources.len(), 1);
        assert_eq!(resources[0].content.resource_group_name, "resource-1-name");

        // Ensure a client cannot see resources of another client
    }
}
