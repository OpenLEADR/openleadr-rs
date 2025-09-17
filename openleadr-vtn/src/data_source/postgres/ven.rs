use crate::{
    api::ven::QueryParams,
    data_source::{
        postgres::{resource::PgResourceStorage, to_json_value},
        Crud, VenCrud, VenPermissions,
    },
    error::AppError,
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use openleadr_wire::{
    resource::Resource,
    ven::{Ven, VenContent, VenId},
};
use sqlx::PgPool;
use std::collections::{hash_map::Entry, HashMap};
use tracing::{error, trace};

#[async_trait]
impl VenCrud for PgVenStorage {}

pub(crate) struct PgVenStorage {
    db: PgPool,
}

impl From<PgPool> for PgVenStorage {
    fn from(db: PgPool) -> Self {
        Self { db }
    }
}

#[derive(Debug)]
struct PostgresVen {
    id: String,
    created_date_time: DateTime<Utc>,
    modification_date_time: DateTime<Utc>,
    ven_name: String,
    attributes: Option<serde_json::Value>,
    targets: Option<serde_json::Value>,
}

impl PostgresVen {
    #[tracing::instrument]
    fn try_into_ven_with_resources(
        self,
        resources: Option<Vec<Resource>>,
    ) -> Result<Ven, AppError> {
        let attributes = match self.attributes {
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
        let targets = match self.targets {
            None => None,
            Some(t) => serde_json::from_value(t)
                .inspect_err(|err| {
                    error!(?err, "Failed to deserialize JSON from DB to `TargetMap`")
                })
                .map_err(AppError::SerdeJsonInternalServerError)?,
        };

        Ok(Ven {
            id: self.id.parse()?,
            created_date_time: self.created_date_time,
            modification_date_time: self.modification_date_time,
            content: VenContent::new(self.ven_name, attributes, targets, resources),
        })
    }
}

#[async_trait]
impl Crud for PgVenStorage {
    type Type = Ven;
    type Id = VenId;
    type NewType = VenContent;
    type Error = AppError;
    type Filter = QueryParams;
    type PermissionFilter = VenPermissions;

    async fn create(
        &self,
        new: Self::NewType,
        _user: &Self::PermissionFilter,
    ) -> Result<Self::Type, Self::Error> {
        let ven: Ven = sqlx::query_as!(
            PostgresVen,
            r#"
            INSERT INTO ven (
                id,
                created_date_time,
                modification_date_time,
                ven_name,
                attributes,
                targets
            )
            VALUES (gen_random_uuid(), now(), now(), $1, $2, $3)
            RETURNING *
            "#,
            new.ven_name,
            to_json_value(new.attributes)?,
            to_json_value(new.targets)?,
        )
        .fetch_one(&self.db)
        .await?
        .try_into_ven_with_resources(None)?;

        trace!(ven_id = ven.id.as_str(), "created ven");

        Ok(ven)
    }

    async fn retrieve(
        &self,
        id: &Self::Id,
        permissions: &Self::PermissionFilter,
    ) -> Result<Self::Type, Self::Error> {
        let ids = permissions.as_value();

        let resources = PgResourceStorage::retrieve_by_ven(&self.db, id).await?;
        let resources = if resources.is_empty() {
            None
        } else {
            Some(resources)
        };

        let ven: Ven = sqlx::query_as!(
            PostgresVen,
            r#"
            SELECT *
            FROM ven
            WHERE id = $1
            AND ($2::text[] IS NULL OR id = ANY($2))
            "#,
            id.as_str(),
            ids.as_deref(),
        )
        .fetch_one(&self.db)
        .await?
        .try_into_ven_with_resources(resources)?;

        trace!(ven_id = ven.id.as_str(), "retrieved ven");

        Ok(ven)
    }

    async fn retrieve_all(
        &self,
        filter: &Self::Filter,
        permissions: &Self::PermissionFilter,
    ) -> Result<Vec<Self::Type>, Self::Error> {
        let ids = permissions.as_value();

        let pg_vens: Vec<PostgresVen> = sqlx::query_as!(
            PostgresVen,
            r#"
            SELECT DISTINCT
                v.id AS "id!",
                v.created_date_time AS "created_date_time!",
                v.modification_date_time AS "modification_date_time!",
                v.ven_name AS "ven_name!",
                v.attributes,
                v.targets
            FROM ven v
              LEFT JOIN resource r ON r.ven_id = v.id
            WHERE ($1::text IS NULL OR v.ven_name = $1)
              AND ($2::text[] IS NULL OR v.targets ?| $2)
              AND ($3::text[] IS NULL OR v.id = ANY($3))
            ORDER BY v.created_date_time DESC
            OFFSET $4 LIMIT $5
            "#,
            filter.ven_name,
            filter.targets.targets.as_deref(),
            ids.as_deref(),
            filter.skip,
            filter.limit,
        )
        .fetch_all(&self.db)
        .await?;

        let ven_ids: Vec<String> = pg_vens.iter().map(|v| v.id.to_string()).collect();
        let resources = PgResourceStorage::retrieve_by_vens(&self.db, &ven_ids).await?;

        let mut resources_map = resources.into_iter().fold(
            HashMap::new(),
            |mut map: HashMap<String, Vec<Resource>>, resource| {
                match map.entry(resource.ven_id.to_string()) {
                    Entry::Occupied(mut e) => e.get_mut().push(resource),
                    Entry::Vacant(e) => {
                        e.insert(vec![resource]);
                    }
                }
                map
            },
        );

        let vens = pg_vens
            .into_iter()
            .map(|ven| {
                let id = ven.id.to_string();
                ven.try_into_ven_with_resources(resources_map.remove(&id))
            })
            .collect::<Result<Vec<_>, AppError>>()?;

        trace!("retrieved {} ven(s)", vens.len());

        Ok(vens)
    }

    async fn update(
        &self,
        id: &Self::Id,
        new: Self::NewType,
        _user: &Self::PermissionFilter,
    ) -> Result<Self::Type, Self::Error> {
        let resources = PgResourceStorage::retrieve_by_ven(&self.db, id).await?;
        let resources = if resources.is_empty() {
            None
        } else {
            Some(resources)
        };

        let ven: Ven = sqlx::query_as!(
            PostgresVen,
            r#"
            UPDATE ven
            SET modification_date_time = now(),
                ven_name = $2,
                attributes = $3,
                targets = $4
            WHERE id = $1
            RETURNING *
            "#,
            id.as_str(),
            new.ven_name,
            to_json_value(new.attributes)?,
            to_json_value(new.targets)?
        )
        .fetch_one(&self.db)
        .await?
        .try_into_ven_with_resources(resources)?;

        trace!(ven_id = id.as_str(), "updated ven");

        Ok(ven)
    }

    async fn delete(
        &self,
        id: &Self::Id,
        _user: &Self::PermissionFilter,
    ) -> Result<Self::Type, Self::Error> {
        if !PgResourceStorage::retrieve_by_ven(&self.db, id)
            .await?
            .is_empty()
        {
            Err(AppError::Forbidden(
                "Cannot delete VEN with associated resources",
            ))?
        }

        let ven: Ven = sqlx::query_as!(
            PostgresVen,
            r#"
            DELETE FROM ven
            WHERE id = $1
            RETURNING *
            "#,
            id.as_str(),
        )
        .fetch_one(&self.db)
        .await?
        .try_into_ven_with_resources(None)?;

        trace!(ven_id = id.as_str(), "deleted ven");

        Ok(ven)
    }
}

#[cfg(test)]
#[cfg(feature = "live-db-test")]
mod tests {
    use crate::{
        api::{ven::QueryParams, TargetQueryParams},
        data_source::{postgres::ven::PgVenStorage, Crud},
        error::AppError,
    };
    use openleadr_wire::{
        target::Target,
        ven::{Ven, VenContent},
    };
    use sqlx::PgPool;

    impl Default for QueryParams {
        fn default() -> Self {
            Self {
                ven_name: None,
                targets: TargetQueryParams { targets: None },
                skip: 0,
                limit: 50,
            }
        }
    }

    fn ven_1() -> Ven {
        Ven {
            id: "ven-1".parse().unwrap(),
            created_date_time: "2024-07-25 08:31:10.776000 +00:00".parse().unwrap(),
            modification_date_time: "2024-07-25 08:31:10.776000 +00:00".parse().unwrap(),
            content: VenContent::new(
                "ven-1-name".to_string(),
                None,
                Some(vec![
                    Target::new("group-1").unwrap(),
                    Target::new("private-value").unwrap(),
                ]),
                None,
            ),
        }
    }

    fn ven_2() -> Ven {
        Ven {
            id: "ven-2".parse().unwrap(),
            created_date_time: "2024-07-25 08:31:10.776000 +00:00".parse().unwrap(),
            modification_date_time: "2024-07-25 08:31:10.776000 +00:00".parse().unwrap(),
            content: VenContent::new("ven-2-name".to_string(), None, None, None),
        }
    }

    mod get_all {
        use crate::data_source::postgres::ven::{PgVenStorage, VenPermissions};

        use super::*;

        #[sqlx::test(fixtures("users", "vens"))]
        async fn default_get_all(db: PgPool) {
            let repo: PgVenStorage = db.into();
            let mut vens = repo
                .retrieve_all(&Default::default(), &VenPermissions::AllAllowed)
                .await
                .unwrap();
            assert_eq!(vens.len(), 2);
            vens.sort_by(|a, b| a.id.as_str().cmp(b.id.as_str()));
            assert_eq!(vens, vec![ven_1(), ven_2()]);
        }

        #[sqlx::test(fixtures("users", "vens"))]
        async fn limit_get_all(db: PgPool) {
            let repo: PgVenStorage = db.into();
            let vens = repo
                .retrieve_all(
                    &QueryParams {
                        limit: 1,
                        ..Default::default()
                    },
                    &VenPermissions::AllAllowed,
                )
                .await
                .unwrap();
            assert_eq!(vens.len(), 1);
        }

        #[sqlx::test(fixtures("users", "vens"))]
        async fn skip_get_all(db: PgPool) {
            let repo: PgVenStorage = db.into();
            let vens = repo
                .retrieve_all(
                    &QueryParams {
                        skip: 1,
                        ..Default::default()
                    },
                    &VenPermissions::AllAllowed,
                )
                .await
                .unwrap();
            assert_eq!(vens.len(), 1);

            let vens = repo
                .retrieve_all(
                    &QueryParams {
                        skip: 2,
                        ..Default::default()
                    },
                    &VenPermissions::AllAllowed,
                )
                .await
                .unwrap();
            assert_eq!(vens.len(), 0);
        }

        #[sqlx::test(fixtures("users", "vens"))]
        async fn filter_target_get_all(db: PgPool) {
            let repo: PgVenStorage = db.into();

            let vens = repo
                .retrieve_all(
                    &QueryParams {
                        targets: TargetQueryParams {
                            targets: Some(vec!["group-1".to_string()]),
                        },
                        ..Default::default()
                    },
                    &VenPermissions::AllAllowed,
                )
                .await
                .unwrap();
            assert_eq!(vens.len(), 1);

            let vens = repo
                .retrieve_all(
                    &QueryParams {
                        targets: TargetQueryParams {
                            targets: Some(vec!["not-existent".to_string()]),
                        },
                        ..Default::default()
                    },
                    &VenPermissions::AllAllowed,
                )
                .await
                .unwrap();
            assert_eq!(vens.len(), 0);

            let vens = repo
                .retrieve_all(
                    &QueryParams {
                        ven_name: Some("ven-2-name".to_string()),
                        ..Default::default()
                    },
                    &VenPermissions::AllAllowed,
                )
                .await
                .unwrap();
            assert_eq!(vens.len(), 1);
            assert_eq!(vens, vec![ven_2()]);

            let vens = repo
                .retrieve_all(
                    &QueryParams {
                        ven_name: Some("ven-not-existent".to_string()),
                        ..Default::default()
                    },
                    &VenPermissions::AllAllowed,
                )
                .await
                .unwrap();
            assert_eq!(vens.len(), 0);
        }
    }

    mod get {
        use crate::data_source::postgres::ven::VenPermissions;

        use super::*;

        #[sqlx::test(fixtures("users", "vens"))]
        async fn get_existing(db: PgPool) {
            let repo: PgVenStorage = db.into();

            let ven = repo
                .retrieve(&"ven-1".parse().unwrap(), &VenPermissions::AllAllowed)
                .await
                .unwrap();
            assert_eq!(ven, ven_1());
        }

        #[sqlx::test(fixtures("users", "vens"))]
        async fn get_not_existent(db: PgPool) {
            let repo: PgVenStorage = db.into();
            let ven = repo
                .retrieve(
                    &"ven-not-existent".parse().unwrap(),
                    &VenPermissions::AllAllowed,
                )
                .await;

            assert!(matches!(ven, Err(AppError::NotFound)));
        }
    }

    mod add {
        use crate::data_source::postgres::ven::VenPermissions;

        use super::*;
        use chrono::{Duration, Utc};

        #[sqlx::test]
        async fn add(db: PgPool) {
            let repo: PgVenStorage = db.into();

            let ven = repo
                .create(ven_1().content, &VenPermissions::AllAllowed)
                .await
                .unwrap();
            assert!(ven.created_date_time < Utc::now() + Duration::minutes(10));
            assert!(ven.created_date_time > Utc::now() - Duration::minutes(10));
            assert!(ven.modification_date_time < Utc::now() + Duration::minutes(10));
            assert!(ven.modification_date_time > Utc::now() - Duration::minutes(10));
        }

        #[sqlx::test(fixtures("users", "vens"))]
        async fn add_existing_name(db: PgPool) {
            let repo: PgVenStorage = db.into();

            let ven = repo
                .create(ven_1().content, &VenPermissions::AllAllowed)
                .await;
            assert!(matches!(ven, Err(AppError::Conflict(_, _))));
        }
    }

    mod modify {
        use crate::data_source::postgres::ven::VenPermissions;

        use super::*;
        use chrono::{DateTime, Duration, Utc};

        #[sqlx::test(fixtures("users", "vens"))]
        async fn updates_modify_time(db: PgPool) {
            let repo: PgVenStorage = db.into();
            let ven = repo
                .update(
                    &"ven-1".parse().unwrap(),
                    ven_1().content,
                    &VenPermissions::AllAllowed,
                )
                .await
                .unwrap();

            assert_eq!(ven.content, ven_1().content);
            assert_eq!(
                ven.created_date_time,
                "2024-07-25 08:31:10.776000 +00:00"
                    .parse::<DateTime<Utc>>()
                    .unwrap()
            );
            assert!(ven.modification_date_time < Utc::now() + Duration::minutes(10));
            assert!(ven.modification_date_time > Utc::now() - Duration::minutes(10));
        }

        #[sqlx::test(fixtures("users", "vens"))]
        async fn update(db: PgPool) {
            let repo: PgVenStorage = db.into();
            let mut updated = ven_2().content;
            updated.ven_name = "updated_name".parse().unwrap();

            let ven = repo
                .update(
                    &"ven-1".parse().unwrap(),
                    updated.clone(),
                    &VenPermissions::AllAllowed,
                )
                .await
                .unwrap();

            assert_eq!(ven.content, updated);
            let ven = repo
                .retrieve(&"ven-1".parse().unwrap(), &VenPermissions::AllAllowed)
                .await
                .unwrap();
            assert_eq!(ven.content, updated);
        }
    }

    mod delete {
        use crate::data_source::postgres::ven::VenPermissions;

        use super::*;

        #[sqlx::test(fixtures("users", "vens"))]
        async fn delete_existing(db: PgPool) {
            let repo: PgVenStorage = db.into();
            let ven = repo
                .delete(&"ven-1".parse().unwrap(), &VenPermissions::AllAllowed)
                .await
                .unwrap();
            assert_eq!(ven, ven_1());

            let ven = repo
                .retrieve(&"ven-1".parse().unwrap(), &VenPermissions::AllAllowed)
                .await;
            assert!(matches!(ven, Err(AppError::NotFound)));

            let ven = repo
                .retrieve(&"ven-2".parse().unwrap(), &VenPermissions::AllAllowed)
                .await
                .unwrap();
            assert_eq!(ven, ven_2());
        }

        #[sqlx::test(fixtures("users", "vens"))]
        async fn delete_not_existing(db: PgPool) {
            let repo: PgVenStorage = db.into();
            let ven = repo
                .delete(
                    &"ven-not-existing".parse().unwrap(),
                    &VenPermissions::AllAllowed,
                )
                .await;
            assert!(matches!(ven, Err(AppError::NotFound)));
        }
    }
}
