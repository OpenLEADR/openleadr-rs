#[cfg(feature = "internal-oauth")]
use crate::data_source::{postgres::user::PgAuthSource, AuthSource};

use super::{Migrate, VenObjectPrivacy};
use crate::{
    data_source::{
        postgres::{
            event::PgEventStorage, program::PgProgramStorage, report::PgReportStorage,
            ven::PgVenStorage,
        },
        DataSource, EventCrud, ProgramCrud, ReportCrud, ResourceCrud, VenCrud,
    },
    error::AppError,
};
use async_trait::async_trait;
use dotenvy::dotenv;
use openleadr_wire::{target::Target, ClientId};
use resource::PgResourceStorage;
use serde::Serialize;
use sqlx::{migrate::MigrateError, postgres::PgPoolOptions, PgPool};
use std::sync::Arc;
use tracing::{error, info};

mod event;
mod program;
mod report;
mod resource;
#[cfg(feature = "internal-oauth")]
mod user;
mod ven;

#[derive(Clone)]
pub struct PostgresStorage {
    db: PgPool,
}

impl DataSource for PostgresStorage {
    fn programs(&self) -> Arc<dyn ProgramCrud> {
        Arc::<PgProgramStorage>::new(self.db.clone().into())
    }

    fn reports(&self) -> Arc<dyn ReportCrud> {
        Arc::<PgReportStorage>::new(self.db.clone().into())
    }

    fn events(&self) -> Arc<dyn EventCrud> {
        Arc::<PgEventStorage>::new(self.db.clone().into())
    }

    fn vens(&self) -> Arc<dyn VenCrud> {
        Arc::<PgVenStorage>::new(self.db.clone().into())
    }

    fn ven_object_privacy(&self) -> Arc<dyn VenObjectPrivacy> {
        Arc::<PgVenStorage>::new(self.db.clone().into())
    }

    fn resources(&self) -> Arc<dyn ResourceCrud> {
        Arc::<PgResourceStorage>::new(self.db.clone().into())
    }

    #[cfg(feature = "internal-oauth")]
    fn auth(&self) -> Arc<dyn AuthSource> {
        Arc::<PgAuthSource>::new(self.db.clone().into())
    }

    /// Verify the connection pool is open and has at least one connection
    fn connection_active(&self) -> bool {
        !self.db.is_closed() && self.db.size() > 0
    }
}

#[async_trait]
impl Migrate for PostgresStorage {
    async fn migrate(&self) -> Result<(), MigrateError> {
        sqlx::migrate!("./migrations").run(&self.db).await
    }
}

impl PostgresStorage {
    pub fn new(db: PgPool) -> Result<Self, sqlx::Error> {
        Ok(Self { db })
    }

    pub async fn from_env() -> Result<Self, sqlx::Error> {
        dotenv().ok();
        let db_url = std::env::var("DATABASE_URL")
            .expect("Missing DATABASE_URL env var even though the 'postgres' feature is active");

        let db = PgPoolOptions::new()
            .min_connections(1)
            .connect(&db_url)
            .await?;

        let connect_options = db.connect_options();
        let safe_db_url = format!(
            "{}:{}/{}",
            connect_options.get_host(),
            connect_options.get_port(),
            connect_options.get_database().unwrap_or_default()
        );

        Self::new(db)
            .inspect_err(|err| error!(?err, "could not connect to Postgres database"))
            .inspect(|_| {
                info!(
                    "Successfully connected to Postgres backend at {}",
                    safe_db_url
                )
            })
    }
}

fn to_json_value<T: Serialize>(v: Option<T>) -> Result<Option<serde_json::Value>, AppError> {
    v.map(|v| serde_json::to_value(v).map_err(AppError::SerdeJsonBadRequest))
        .transpose()
}

/// Returns the targets of the VEN associated with the given `client_id` and it's resources.
/// If the VEN does not exist, returns an empty vector.
async fn get_ven_targets(
    db: PgPool,
    client_id: &ClientId,
) -> Result<Vec<Target>, AppError> {
    let ven_store: PgVenStorage = db.into();
    match ven_store.targets_by_client_id(client_id).await {
        Ok(t) => Ok(t),
        // Cite from OpenADR Spec 3.1.1 Definition.md, "VEN created object privacy":
        //      4. If a VEN object is not found, return objects that do not have targets and do not proceed to step 5.
        //      [...]
        //      6. If the union of the targets of the VEN and its resources is empty, return objects that do not have targets and do not proceed to step 7.
        Err(AppError::NotFound) => Ok(Vec::new()),
        Err(err) => Err(err),
    }
}
