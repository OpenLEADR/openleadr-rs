use crate::{
    api::resource_group::QueryParams,
    data_source::{Crud, ResourceGroupCrud, postgres::to_json_value},
    error::AppError,
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use openleadr_wire::{
    ClientId,
    resource_group::{BlResourceGroupRequest, ResourceGroup, ResourceGroupChild, ResourceGroupId},
    target::Target,
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
    child_groups: Vec<String>,
    child_resources: Vec<String>,
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

        let mut children =
            Vec::with_capacity(value.child_groups.len() + value.child_resources.len());
        for id in value.child_groups {
            children.push(ResourceGroupChild::ResourceGroup(id.parse()?));
        }
        for id in value.child_resources {
            children.push(ResourceGroupChild::VenResource(id.parse()?));
        }

        Ok(Self {
            id: value.id.parse()?,
            created_date_time: value.created_date_time,
            modification_date_time: value.modification_date_time,
            content: BlResourceGroupRequest {
                resource_group_name: value.resource_group_name,
                attributes,
                targets: value.targets,
                children,
            },
        })
    }
}

/// Resource groups with their children, in one statement, so that a group and its children
/// always come from the same snapshot.
///
/// A VEN client sees the groups of the `rg_visible` view, only the children that it sees
/// itself, and only its own resources. The BL (`client_id` is `None`) sees everything.
async fn select_resource_groups(
    db: &PgPool,
    id: Option<&ResourceGroupId>,
    name: Option<&str>,
    targets: &[Target],
    client_id: &Option<ClientId>,
    skip: i64,
    limit: i64,
) -> Result<Vec<ResourceGroup>, AppError> {
    sqlx::query_as!(
        PostgresResourceGroup,
        r#"
        WITH visible AS (
            SELECT rg_id FROM rg_visible WHERE client_id = $4
            UNION ALL
            SELECT id FROM resource_group WHERE $4::text IS NULL
        )
        SELECT
            rg.id,
            rg.created_date_time,
            rg.modification_date_time,
            rg.resource_group_name,
            rg.attributes,
            rg.targets AS "targets:Vec<Target>",
            ARRAY(
                SELECT DISTINCT child.rg_child_rg_id
                FROM rg_child_rg child
                WHERE child.rg_parent_rg_id = rg.id
                  AND child.rg_child_rg_id IN (SELECT rg_id FROM visible)
            ) AS "child_groups!",
            ARRAY(
                SELECT child.rg_child_ven_resource_id
                FROM rg_child_ven_resource child
                WHERE child.rg_parent_rg_id = rg.id
                  AND ($4::text IS NULL OR EXISTS (
                      SELECT 1 FROM resource r
                      WHERE r.id = child.rg_child_ven_resource_id
                        AND r.ven_id = (SELECT id FROM ven WHERE client_id = $4)
                  ))
            ) AS "child_resources!"
        FROM resource_group rg
        WHERE ($1::text IS NULL OR rg.id = $1)
          AND ($2::text IS NULL OR rg.resource_group_name = $2)
          AND (array_length($3::text[], 1) IS NULL OR rg.targets && $3)
          AND rg.id IN (SELECT rg_id FROM visible)
        -- id breaks ties, so paging with OFFSET doesn't skip or repeat groups
        ORDER BY rg.created_date_time, rg.id
        OFFSET $5 LIMIT $6
        "#,
        id.map(ResourceGroupId::as_str),
        name,
        targets as _,
        client_id as _,
        skip,
        limit,
    )
    .fetch_all(db)
    .await?
    .into_iter()
    .map(TryInto::try_into)
    .collect()
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
                targets as "targets:Vec<Target>",
                '{}'::text[] AS "child_groups!",
                '{}'::text[] AS "child_resources!"
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
        select_resource_groups(&self.db, Some(id), None, &[], client_id, 0, 1)
            .await?
            .pop()
            .ok_or(AppError::NotFound)
    }

    async fn retrieve_all(
        &self,
        filter: &Self::Filter,
        client_id: &Self::PermissionFilter,
    ) -> Result<Vec<Self::Type>, Self::Error> {
        let rgs = select_resource_groups(
            &self.db,
            None,
            filter.resource_group_name.as_deref(),
            filter.targets.as_deref(),
            client_id,
            filter.skip,
            filter.limit,
        )
        .await?;
        trace!("retrieved {} resource groups", rgs.len());
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
                rg.targets as "targets:Vec<Target>",
                '{}'::text[] AS "child_groups!",
                '{}'::text[] AS "child_resources!"
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
            r#"DELETE FROM rg_child_rg WHERE rg_parent_rg_id = $1"#,
            id.as_str()
        )
        .execute(tx.as_mut())
        .await?;

        sqlx::query!(
            r#"DELETE FROM rg_child_ven_resource WHERE rg_parent_rg_id = $1"#,
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
        _client_id: &Self::PermissionFilter,
    ) -> Result<Self::Type, Self::Error> {
        let mut tx = self.db.begin().await?;

        // Lock the group first: a concurrent update holds the same row lock while it replaces
        // the children, and the next statement then sees the children of its commit.
        sqlx::query!(
            "SELECT id FROM resource_group WHERE id = $1 FOR UPDATE",
            id.as_str()
        )
        .fetch_one(tx.as_mut())
        .await?;

        // The foreign key cascade would remove the children without returning them.
        let resource_group = sqlx::query_as!(
            PostgresResourceGroup,
            r#"
            WITH child_groups AS (
                DELETE FROM rg_child_rg WHERE rg_parent_rg_id = $1 RETURNING rg_child_rg_id
            ), child_resources AS (
                DELETE FROM rg_child_ven_resource WHERE rg_parent_rg_id = $1
                RETURNING rg_child_ven_resource_id
            )
            DELETE FROM resource_group rg
            WHERE rg.id = $1
            RETURNING
                rg.id,
                rg.created_date_time,
                rg.modification_date_time,
                rg.resource_group_name,
                rg.attributes,
                rg.targets as "targets:Vec<Target>",
                ARRAY(SELECT DISTINCT rg_child_rg_id FROM child_groups) AS "child_groups!",
                ARRAY(SELECT rg_child_ven_resource_id FROM child_resources) AS "child_resources!"
            "#,
            id.as_str(),
        )
        .fetch_one(tx.as_mut())
        .await?
        .try_into()?;

        tx.commit().await?;
        Ok(resource_group)
    }
}

#[cfg(test)]
#[cfg(feature = "live-db-test")]
mod test {
    use crate::{
        api::{TargetQueryParams, resource_group::QueryParams},
        data_source::{Crud, postgres::resource_group::PgResourceGroupStorage},
    };
    use openleadr_wire::{
        ClientId,
        resource_group::{ResourceGroup, ResourceGroupChild},
    };
    use sqlx::PgPool;
    use std::collections::BTreeMap;

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
        assert_eq!(resource_groups.len(), 2);

        let filters = QueryParams {
            limit: 4,
            ..QueryParams::default()
        };
        let resource_groups = repo.retrieve_all(&filters, &None).await.unwrap();
        assert_eq!(resource_groups.len(), 4);
    }

    fn child_names(children: &[ResourceGroupChild]) -> Vec<String> {
        let mut names: Vec<String> = children
            .iter()
            .map(|child| match child {
                ResourceGroupChild::ResourceGroup(id) => format!("rg:{}", id.as_str()),
                ResourceGroupChild::VenResource(id) => format!("res:{}", id.as_str()),
            })
            .collect();
        names.sort();
        names
    }

    /// Children per group as seen by `client_id` through `retrieve_all`, checked against
    /// `retrieve` for each group.
    async fn children_by_group(
        repo: &PgResourceGroupStorage,
        client_id: &Option<ClientId>,
    ) -> BTreeMap<String, Vec<String>> {
        let rgs: Vec<ResourceGroup> = repo
            .retrieve_all(&QueryParams::default(), client_id)
            .await
            .unwrap();
        let mut by_group = BTreeMap::new();
        for rg in rgs {
            let single = repo.retrieve(&rg.id, client_id).await.unwrap();
            assert_eq!(
                child_names(&single.content.children),
                child_names(&rg.content.children),
                "retrieve and retrieve_all disagree on children of {}",
                rg.id.as_str()
            );
            by_group.insert(rg.id.as_str().to_owned(), child_names(&rg.content.children));
        }
        by_group
    }

    fn expected(groups: &[(&str, &[&str])]) -> BTreeMap<String, Vec<String>> {
        groups
            .iter()
            .map(|(id, children)| {
                let mut children: Vec<String> = children.iter().map(|c| c.to_string()).collect();
                children.sort();
                (id.to_string(), children)
            })
            .collect()
    }

    #[sqlx::test(fixtures("users", "vens", "resources", "resource_groups"))]
    async fn children_for_bl(db: PgPool) {
        let repo = PgResourceGroupStorage::from(db);

        assert_eq!(
            children_by_group(&repo, &None).await,
            expected(&[
                (
                    "resource-group-1",
                    &["rg:resource-group-2", "rg:resource-group-3"]
                ),
                (
                    "resource-group-2",
                    &["rg:resource-group-4", "res:resource-1"]
                ),
                (
                    "resource-group-3",
                    &[
                        "res:resource-1",
                        "res:resource-2",
                        "res:resource-3",
                        "res:resource-4"
                    ]
                ),
                ("resource-group-4", &[]),
                ("ouroboros", &["rg:ouroboros"]),
            ])
        );
    }

    #[sqlx::test(fixtures("users", "vens", "resources", "resource_groups"))]
    async fn children_for_ven(db: PgPool) {
        let repo = PgResourceGroupStorage::from(db);

        // ven-1 owns resource-1 (in groups 2 and 3) and resource-3 (in group 3). Group 4 is a
        // child of group 2 but holds none of its resources, so it is hidden.
        assert_eq!(
            children_by_group(&repo, &Some("ven-1-client-id".parse().unwrap())).await,
            expected(&[
                (
                    "resource-group-1",
                    &["rg:resource-group-2", "rg:resource-group-3"]
                ),
                ("resource-group-2", &["res:resource-1"]),
                ("resource-group-3", &["res:resource-1", "res:resource-3"]),
            ])
        );

        // ven-2 only has resources in group 3, so group 2 is hidden, also as a child of group 1.
        assert_eq!(
            children_by_group(&repo, &Some("ven-2-client-id".parse().unwrap())).await,
            expected(&[
                ("resource-group-1", &["rg:resource-group-3"]),
                ("resource-group-3", &["res:resource-2", "res:resource-4"]),
            ])
        );

        // ven-3 owns no resources and sees nothing.
        let ven_3 = Some("ven-3-client-id".parse().unwrap());
        assert!(children_by_group(&repo, &ven_3).await.is_empty());

        for hidden in ["resource-group-2", "resource-group-4", "ouroboros"] {
            assert!(
                repo.retrieve(
                    &hidden.parse().unwrap(),
                    &Some("ven-2-client-id".parse().unwrap())
                )
                .await
                .is_err(),
                "{hidden} must be hidden from ven-2"
            );
        }
    }

    #[sqlx::test(fixtures("users", "vens", "resources", "resource_groups"))]
    async fn visibility_through_nesting_and_cycles(db: PgPool) {
        // Put a ven-2 resource two levels down (1 -> 2 -> 4), and close the loop 4 -> 1.
        sqlx::raw_sql(
            "INSERT INTO rg_child_ven_resource VALUES ('resource-group-4', 'resource-5');
             INSERT INTO rg_child_rg VALUES ('resource-group-4', 'resource-group-1');",
        )
        .execute(&db)
        .await
        .unwrap();
        let repo = PgResourceGroupStorage::from(db);

        assert_eq!(
            children_by_group(&repo, &Some("ven-2-client-id".parse().unwrap())).await,
            expected(&[
                (
                    "resource-group-1",
                    &["rg:resource-group-2", "rg:resource-group-3"]
                ),
                ("resource-group-2", &["rg:resource-group-4"]),
                ("resource-group-3", &["res:resource-2", "res:resource-4"]),
                (
                    "resource-group-4",
                    &["rg:resource-group-1", "res:resource-5"]
                ),
            ])
        );
        let ven_3 = Some("ven-3-client-id".parse().unwrap());
        assert!(children_by_group(&repo, &ven_3).await.is_empty());
    }

    #[sqlx::test(fixtures("users", "vens", "resources", "resource_groups"))]
    async fn paging_is_stable_with_equal_timestamps(db: PgPool) {
        // All fixture groups share created_date_time; paging must still visit each one once.
        let repo = PgResourceGroupStorage::from(db);
        let mut seen = Vec::new();
        for skip in 0..5 {
            let page = repo
                .retrieve_all(
                    &QueryParams {
                        skip,
                        limit: 1,
                        ..QueryParams::default()
                    },
                    &None,
                )
                .await
                .unwrap();
            assert_eq!(page.len(), 1);
            seen.push(page[0].id.as_str().to_owned());
        }
        assert_eq!(
            seen,
            [
                "ouroboros",
                "resource-group-1",
                "resource-group-2",
                "resource-group-3",
                "resource-group-4"
            ]
        );
    }

    #[sqlx::test(fixtures("users", "vens", "resources", "resource_groups"))]
    async fn delete_returns_the_children_it_removed(db: PgPool) {
        let repo = PgResourceGroupStorage::from(db.clone());

        let deleted = repo
            .delete(&"resource-group-2".parse().unwrap(), &None)
            .await
            .unwrap();
        assert_eq!(
            child_names(&deleted.content.children),
            ["res:resource-1", "rg:resource-group-4"]
        );
        let left_over: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM rg_child_rg WHERE rg_parent_rg_id = 'resource-group-2'",
        )
        .fetch_one(&db)
        .await
        .unwrap();
        assert_eq!(left_over, 0);

        let deleted = repo
            .delete(&"ouroboros".parse().unwrap(), &None)
            .await
            .unwrap();
        assert_eq!(child_names(&deleted.content.children), ["rg:ouroboros"]);
    }
}
