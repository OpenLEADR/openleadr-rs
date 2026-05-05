use crate::{
    api::resource_group::QueryParams,
    data_source::{postgres::to_json_value, Crud, ResourceGroupCrud},
    error::AppError,
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use openleadr_wire::{
    resource_group::{BlResourceGroupRequest, ResourceGroup, ResourceGroupChild, ResourceGroupId},
    target::Target,
    ClientId,
};
use sqlx::{PgPool, Postgres, Transaction};
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

async fn get_rg_children(
    tx: &mut Transaction<'_, Postgres>,
    rg_id: &ResourceGroupId,
    client_id: &Option<ClientId>,
) -> Result<Vec<ResourceGroupChild>, <PgResourceGroupStorage as Crud>::Error> {
    let mut rg_children: Vec<_> = sqlx::query!(
        r#"
        WITH RECURSIVE rg_family(root, id) AS NOT MATERIALIZED (
            SELECT id, id FROM resource_group
            UNION
            SELECT fam.root, child.rg_child_rg_id
            FROM rg_child_rg AS child
            JOIN rg_family AS fam ON fam.id = child.rg_parent_rg_id
        )

        SELECT id
        FROM (
            SELECT root, id
            FROM rg_family
            WHERE root = $1
              AND (
                  -- business logic
                  $2::text IS NULL

                  -- client logic (child rg visible if any
                  -- descendant resource is owned by client id)
                  OR EXISTS (
                      SELECT r.id
                      FROM resource r
                      INNER JOIN rg_child_ven_resource AS rcvr
                          ON rcvr.rg_child_ven_resource_id = r.id
                      INNER JOIN rg_family AS rg_fam
                          ON rg_fam.id = rcvr.rg_parent_rg_id
                      WHERE r.ven_id = (SELECT v.id FROM ven v WHERE v.client_id = $2)
                        AND rg_fam.root = $1
                  )
              )

            INTERSECT

            SELECT rg_parent_rg_id, rg_child_rg_id FROM rg_child_rg
        )
        "#,
        rg_id.as_str(),
        client_id as _
    )
    .fetch_all(tx.as_mut())
    .await?
    .into_iter()
    .filter_map(|r| {
        r.id.and_then(|id| id.parse().ok().map(ResourceGroupChild::ResourceGroup))
    })
    .collect();

    let ven_children = sqlx::query!(
        r#"
        SELECT rg_child.rg_child_ven_resource_id
        FROM rg_child_ven_resource AS rg_child
            INNER JOIN resource ON rg_child.rg_child_ven_resource_id = resource.id
            INNER JOIN ven ON ven_id = ven.id

        WHERE rg_child.rg_parent_rg_id = $1
            AND (client_id = $2 OR $2::text IS NULL)
        "#,
        rg_id.as_str(),
        client_id as _
    )
    .fetch_all(tx.as_mut())
    .await?
    .into_iter()
    .map(|r| {
        r.rg_child_ven_resource_id
            .parse()
            .map(ResourceGroupChild::VenResource)
    })
    .collect::<Result<Vec<_>, _>>()?;

    rg_children.extend(ven_children);
    Ok(rg_children)
}

async fn insert_resource_group_children(
    tx: &mut Transaction<'_, Postgres>,
    rg: &mut ResourceGroup,
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

    let rg_children: Vec<_> = sqlx::query!(
        r#"
        INSERT INTO rg_child_rg (rg_parent_rg_id, rg_child_rg_id)
        SELECT $1, u.child FROM UNNEST($2::text[]) as u(child)
        RETURNING
            rg_child_rg_id
        "#,
        rg.id.as_str(),
        &rg_children
    )
    .fetch_all(tx.as_mut())
    .await?
    .into_iter()
    .map(|child| {
        child
            .rg_child_rg_id
            .parse()
            .map(ResourceGroupChild::ResourceGroup)
    })
    .collect::<Result<_, _>>()?;
    rg.content.children.extend(rg_children);

    let ven_children: Vec<_> = sqlx::query!(
        r#"
        INSERT INTO rg_child_ven_resource (rg_parent_rg_id, rg_child_ven_resource_id)
        SELECT $1, child FROM UNNEST ($2::text[]) as u(child)
        RETURNING
            rg_child_ven_resource_id
        "#,
        rg.id.as_str(),
        &ven_children
    )
    .fetch_all(tx.as_mut())
    .await?
    .into_iter()
    .map(|child| {
        child
            .rg_child_ven_resource_id
            .parse()
            .map(ResourceGroupChild::VenResource)
    })
    .collect::<Result<_, _>>()?;
    rg.content.children.extend(ven_children);

    Ok(())
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
        let mut tx = self.db.begin().await?;

        let mut resource_group: ResourceGroup = sqlx::query_as!(
            PostgresResourceGroup,
            r#"
            INSERT INTO resource_group
            VALUES (gen_random_uuid()::text, now(), now(), $1, $2, $3)
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
        .fetch_one(tx.as_mut())
        .await?
        .try_into()?;

        insert_resource_group_children(&mut tx, &mut resource_group, &new.children).await?;

        tx.commit().await?;
        Ok(resource_group)
    }

    async fn retrieve(
        &self,
        id: &Self::Id,
        client_id: &Self::PermissionFilter,
    ) -> Result<Self::Type, Self::Error> {
        let mut tx = self.db.begin().await?;

        let mut resource_group: ResourceGroup = sqlx::query_as!(
            PostgresResourceGroup,
            r#"
                -- view of all resource group (grand)children of a resource group
                WITH RECURSIVE rg_family(root, id) AS NOT MATERIALIZED (
                    SELECT id, id FROM resource_group
                    UNION
                    SELECT fam.root, child.rg_child_rg_id
                    FROM rg_child_rg AS child
                    JOIN rg_family AS fam ON fam.id = child.rg_parent_rg_id
                )

                SELECT
                    rg.id,
                    rg.created_date_time,
                    rg.modification_date_time,
                    rg.resource_group_name,
                    rg.attributes,
                    rg.targets as "targets:Vec<Target>"
                FROM resource_group rg
                WHERE rg.id = $1

                AND (
                    -- If client_id is null, it is a business logic request
                    $2::text IS NULL

                    -- Otherwise, for a VEN, the resource group should only be visible if there
                    -- is at least 1 VEN resource (grand) child, with matching client_id.
                    OR EXISTS (
                        SELECT r.id FROM resource r
                        INNER JOIN rg_child_ven_resource rcvr
                            ON rcvr.rg_child_ven_resource_id = r.id
                        INNER JOIN rg_family rg_fam
                            ON rg_fam.id = rcvr.rg_parent_rg_id
                        WHERE r.ven_id = (SELECT v.id FROM ven v WHERE v.client_id = $2)
                          AND rg_fam.root = $1

                    )
                )
                "#,
            id.as_str(),
            client_id as _
        )
        .fetch_one(tx.as_mut())
        .await?
        .try_into()?;

        resource_group
            .content
            .children
            .extend(get_rg_children(&mut tx, &resource_group.id, client_id).await?);

        tx.commit().await?;

        Ok(resource_group)
    }

    async fn retrieve_all(
        &self,
        filter: &Self::Filter,
        client_id: &Self::PermissionFilter,
    ) -> Result<Vec<Self::Type>, Self::Error> {
        let mut tx = self.db.begin().await?;

        let mut rgs = sqlx::query_as!(
            PostgresResourceGroup,
            r#"
            -- view of all resource group (grand)children of a resource group
            WITH RECURSIVE rg_family(root, id) AS NOT MATERIALIZED (
                SELECT id, id FROM resource_group
                UNION
                SELECT fam.root, child.rg_child_rg_id
                FROM rg_child_rg AS child
                JOIN rg_family AS fam ON fam.id = child.rg_parent_rg_id
            )

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

            AND (
                -- If client_id is null, it is a business logic request
                $3::text IS NULL

                    -- Otherwise, for a VEN, the resource group should only be visible if there
                    -- is at least 1 VEN resource (grand) child, with matching ven_id.
                  OR EXISTS (
                      SELECT r.id
                      FROM resource r
                      INNER JOIN rg_child_ven_resource AS rcvr
                          ON rcvr.rg_child_ven_resource_id = r.id
                      INNER JOIN rg_family AS rg_fam
                          ON rg_fam.id = rcvr.rg_parent_rg_id
                      WHERE r.ven_id = (SELECT v.id FROM ven v WHERE v.client_id = $3)
                        AND rg_fam.root = rg.id
                  )
            )

            ORDER BY rg.created_date_time
            OFFSET $4 LIMIT $5
            "#,
            filter.resource_group_name,
            filter.targets.as_deref() as _,
            client_id as _,
            filter.skip,
            filter.limit,
        )
        .fetch_all(tx.as_mut())
        .await?
        .into_iter()
        .map(TryInto::try_into)
        .collect::<Result<Vec<ResourceGroup>, _>>()?;

        for rg in rgs.iter_mut() {
            rg.content
                .children
                .extend(get_rg_children(&mut tx, &rg.id, client_id).await?);
        }

        trace!("retrieved {} resource groups", rgs.len());
        tx.commit().await?;
        Ok(rgs)
    }

    async fn update(
        &self,
        id: &Self::Id,
        new: Self::NewType,
        _client_id: &Self::PermissionFilter,
    ) -> Result<Self::Type, Self::Error> {
        let mut tx = self.db.begin().await?;

        let mut resource_group: ResourceGroup = sqlx::query_as!(
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
        .fetch_one(tx.as_mut())
        .await?
        .try_into()?;

        sqlx::query!(
            r#"
            DELETE FROM rg_child_rg rg_child_ven_resource WHERE rg_parent_rg_id = $1
            "#,
            id.as_str()
        )
        .execute(tx.as_mut())
        .await?;

        insert_resource_group_children(&mut tx, &mut resource_group, &new.children).await?;
        tx.commit().await?;

        Ok(resource_group)
    }

    async fn delete(
        &self,
        id: &Self::Id,
        client_id: &Self::PermissionFilter,
    ) -> Result<Self::Type, Self::Error> {
        let mut tx = self.db.begin().await?;

        let children = get_rg_children(&mut tx, id, client_id).await?;
        let mut resource_group: ResourceGroup = sqlx::query_as!(
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
        .fetch_one(tx.as_mut())
        .await?
        .try_into()?;

        resource_group.content.children.extend(children);

        sqlx::query!(
            r#"
            DELETE FROM rg_child_rg rg_child_ven_resource WHERE rg_parent_rg_id = $1
            "#,
            id.as_str()
        )
        .execute(tx.as_mut())
        .await?;

        tx.commit().await?;
        Ok(resource_group)
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

    #[sqlx::test(fixtures("users", "vens", "resources", "resource_groups"))]
    async fn retrieve_all(db: PgPool) {
        let repo = PgResourceGroupStorage::from(db.clone());

        let resource_groups = repo
            .retrieve_all(&QueryParams::default(), &None)
            .await
            .unwrap();
        assert_eq!(resource_groups.len(), 5);

        let resource_groups = repo
            .retrieve_all(
                &QueryParams::default(),
                &Some("ven-2-client-id".parse().unwrap()),
            )
            .await
            .unwrap();
        assert_eq!(resource_groups.len(), 2);

        let filters = QueryParams {
            resource_group_name: Some("resource-group-1-name".to_string()),
            ..QueryParams::default()
        };
        let resource_groups = repo.retrieve_all(&filters, &None).await.unwrap();
        assert_eq!(resource_groups.len(), 1);

        let filters = QueryParams {
            targets: TargetQueryParams(Some(vec!["group-1".parse().unwrap()])),
            ..QueryParams::default()
        };
        let resource_groups = repo.retrieve_all(&filters, &None).await.unwrap();
        assert_eq!(resource_groups.len(), 2);

        let filters = QueryParams {
            targets: TargetQueryParams(Some(vec![
                "group-1".parse().unwrap(),
                "somewhere-in-the-nowhere".parse().unwrap(),
            ])),
            ..QueryParams::default()
        };
        let resource_groups = repo.retrieve_all(&filters, &None).await.unwrap();
        assert_eq!(resource_groups.len(), 1);

        let filters = QueryParams {
            limit: 4,
            ..QueryParams::default()
        };
        let resource_groups = repo.retrieve_all(&filters, &None).await.unwrap();
        assert_eq!(resource_groups.len(), 4);
    }
}
