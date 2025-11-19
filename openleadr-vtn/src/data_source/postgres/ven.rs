use crate::{
    api::ven::QueryParams,
    data_source::{postgres::to_json_value, Crud, VenCrud, VenObjectPrivacy, VenPermissions},
    error::AppError,
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use openleadr_wire::{
    target::Target,
    ven::{BlVenRequest, Ven, VenId, VenRequest},
    ClientId,
};
use sqlx::PgPool;
use std::collections::BTreeSet;
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
    targets: Vec<Target>,
    client_id: String,
}

impl TryFrom<PostgresVen> for Ven {
    type Error = AppError;

    #[tracing::instrument(name = "TryFrom<PostgresVen> for Ven")]
    fn try_from(pg: PostgresVen) -> Result<Self, Self::Error> {
        let attributes = match pg.attributes {
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

        Ok(Ven {
            id: pg.id.parse()?,
            created_date_time: pg.created_date_time,
            modification_date_time: pg.modification_date_time,
            content: BlVenRequest::new(pg.client_id.parse()?, pg.ven_name, attributes, pg.targets),
        })
    }
}

#[async_trait]
impl Crud for PgVenStorage {
    type Type = Ven;
    type Id = VenId;
    type NewType = VenRequest;
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
                targets,
                client_id
            )
            VALUES (gen_random_uuid(), now(), now(), $1, $2, $3, $4)
            RETURNING
                id,
                created_date_time,
                modification_date_time,
                ven_name,
                attributes,
                targets as "targets:Vec<Target>",
                client_id
            "#,
            new.ven_name(),
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

        trace!(ven_id = ven.id.as_str(), "created ven");

        Ok(ven)
    }

    async fn retrieve(
        &self,
        id: &Self::Id,
        permissions: &Self::PermissionFilter,
    ) -> Result<Self::Type, Self::Error> {
        let ids = permissions.as_value();

        let ven: Ven = sqlx::query_as!(
            PostgresVen,
            r#"
            SELECT
                id,
                created_date_time,
                modification_date_time,
                ven_name,
                attributes,
                targets as "targets:Vec<Target>",
                client_id
            FROM ven
            WHERE id = $1
            AND ($2::text[] IS NULL OR id = ANY($2))
            "#,
            id.as_str(),
            ids.as_deref(),
        )
        .fetch_one(&self.db)
        .await?
        .try_into()?;

        trace!(ven_id = ven.id.as_str(), "retrieved ven");

        Ok(ven)
    }

    async fn retrieve_all(
        &self,
        filter: &Self::Filter,
        permissions: &Self::PermissionFilter,
    ) -> Result<Vec<Self::Type>, Self::Error> {
        let ids = permissions.as_value();

        let vens = sqlx::query_as!(
            PostgresVen,
            r#"
            SELECT DISTINCT
                v.id AS "id!",
                v.created_date_time AS "created_date_time!",
                v.modification_date_time AS "modification_date_time!",
                v.ven_name AS "ven_name!",
                v.attributes,
                v.targets as "targets:Vec<Target>",
                v.client_id
            FROM ven v
              LEFT JOIN resource r ON r.ven_id = v.id
            WHERE ($1::text IS NULL OR v.ven_name = $1)
              AND ($2::text[] IS NULL OR v.targets && $2)
              AND ($3::text[] IS NULL OR v.id = ANY($3))
            ORDER BY v.created_date_time DESC
            OFFSET $4 LIMIT $5
            "#,
            filter.ven_name,
            filter.targets.as_deref() as &[Target],
            ids.as_deref(),
            filter.skip,
            filter.limit,
        )
        .fetch_all(&self.db)
        .await?
        .into_iter()
        .map(|ven| ven.try_into())
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
        let mut tx = self.db.begin().await?;

        let old = sqlx::query!(
            r#"
            SELECT client_id FROM ven WHERE id = $1
            "#,
            id.as_str()
        )
        .fetch_one(&mut *tx)
        .await?;

        if let Some(new_client_id) = new.client_id() {
            if old.client_id != new_client_id.as_str() {
                let error = "Tried to update `client_id` of VEN. \
                This is not allowed in the current version of openLEADR as the specification is not quite \
                clear about if that should be allowed. If you disagree with that interpretation, please open \
                an issue on GitHub.";
                error!(ven_id = id.as_str(), "{}", error);
                return Err(Self::Error::BadRequest(error));
            }
        }

        let ven: Ven = sqlx::query_as!(
            PostgresVen,
            r#"
            UPDATE ven
            SET modification_date_time = now(),
                ven_name = $2,
                attributes = $3,
                targets = $4
            WHERE id = $1
            RETURNING
                id,
                modification_date_time,
                created_date_time,
                ven_name,
                attributes,
                targets as "targets:Vec<Target>",
                client_id
            "#,
            id.as_str(),
            new.ven_name(),
            to_json_value(new.attributes())?,
            new.targets() as &[Target],
        )
        .fetch_one(&mut *tx)
        .await?
        .try_into()?;

        tx.commit().await?;
        trace!(ven_id = id.as_str(), "updated ven");

        Ok(ven)
    }

    async fn delete(
        &self,
        id: &Self::Id,
        _user: &Self::PermissionFilter,
    ) -> Result<Self::Type, Self::Error> {
        let mut tx = self.db.begin().await?;

        let resource_id = sqlx::query_scalar!(
            r#"
            SELECT id FROM resource WHERE ven_id = $1 LIMIT 1
            "#,
            id.as_str()
        )
        .fetch_optional(&mut *tx)
        .await?;

        if resource_id.is_some() {
            Err(AppError::Forbidden(
                "Cannot delete VEN with associated resources",
            ))?
        }

        let ven: Ven = sqlx::query_as!(
            PostgresVen,
            r#"
            DELETE FROM ven
            WHERE id = $1
            RETURNING
                id,
                created_date_time,
                modification_date_time,
                ven_name,
                attributes,
                targets as "targets:Vec<Target>",
                client_id
            "#,
            id.as_str(),
        )
        .fetch_one(&mut *tx)
        .await?
        .try_into()?;

        tx.commit().await?;

        trace!(ven_id = id.as_str(), "deleted ven");

        Ok(ven)
    }
}

#[async_trait]
impl VenObjectPrivacy for PgVenStorage {
    async fn targets_by_client_id(
        &self,
        client_id: ClientId,
    ) -> Result<BTreeSet<Target>, AppError> {
        Ok(sqlx::query_scalar!(
            r#"
            SELECT targets FROM ven WHERE client_id = $1
            "#,
            client_id.as_str()
        )
        .fetch_one(&self.db)
        .await?
        .into_iter()
        .map(|id| id.parse())
        .collect::<Result<BTreeSet<_>, _>>()?)
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
        ven::{BlVenRequest, Ven},
    };
    use sqlx::PgPool;
    use std::str::FromStr;

    impl Default for QueryParams {
        fn default() -> Self {
            Self {
                ven_name: None,
                targets: TargetQueryParams(None),
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
            content: BlVenRequest {
                client_id: "ven-1-client-id".parse().unwrap(),
                targets: vec![
                    Target::from_str("group-1").unwrap(),
                    Target::from_str("private-value").unwrap(),
                ],
                ven_name: "ven-1-name".to_string(),
                attributes: None,
            },
        }
    }

    fn ven_2() -> Ven {
        Ven {
            id: "ven-2".parse().unwrap(),
            created_date_time: "2024-07-25 08:31:10.776000 +00:00".parse().unwrap(),
            modification_date_time: "2024-07-25 08:31:10.776000 +00:00".parse().unwrap(),
            content: BlVenRequest::new(
                "ven-2-client-id".parse().unwrap(),
                "ven-2-name".to_string(),
                None,
                vec![],
            ),
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
                        targets: TargetQueryParams(Some(vec!["group-1".parse().unwrap()])),
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
                        targets: TargetQueryParams(Some(vec!["not-existent".parse().unwrap()])),
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
        use openleadr_wire::ven::VenRequest;

        #[sqlx::test]
        async fn add(db: PgPool) {
            let repo: PgVenStorage = db.into();

            let ven = repo
                .create(
                    VenRequest::BlVenRequest(ven_1().content),
                    &VenPermissions::AllAllowed,
                )
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
                .create(
                    VenRequest::BlVenRequest(ven_1().content),
                    &VenPermissions::AllAllowed,
                )
                .await;
            assert!(matches!(ven, Err(AppError::Conflict(_, _))));
        }
    }

    mod modify {
        use crate::data_source::postgres::ven::VenPermissions;

        use super::*;
        use chrono::{DateTime, Duration, Utc};
        use openleadr_wire::ven::VenRequest;

        #[sqlx::test(fixtures("users", "vens"))]
        async fn updates_modify_time(db: PgPool) {
            let repo: PgVenStorage = db.into();
            let ven = repo
                .update(
                    &"ven-1".parse().unwrap(),
                    VenRequest::BlVenRequest(ven_1().content),
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
            updated.client_id = ven_1().content.client_id;
            updated.ven_name = "updated_name".parse().unwrap();

            let ven = repo
                .update(
                    &"ven-1".parse().unwrap(),
                    VenRequest::BlVenRequest(updated.clone()),
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
