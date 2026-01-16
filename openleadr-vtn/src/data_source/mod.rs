#[cfg(feature = "postgres")]
mod postgres;

use crate::{error::AppError, jwt::Scope};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use openleadr_wire::{
    event::{EventId, EventRequest},
    program::{ProgramId, ProgramRequest},
    report::{ReportId, ReportRequest},
    resource::{BlResourceRequest, Resource, ResourceId},
    target::Target,
    ven::{BlVenRequest, Ven, VenId},
    ClientId, Event, Program, Report,
};
#[cfg(feature = "postgres")]
pub use postgres::PostgresStorage;
use serde::{Deserialize, Serialize};
use sqlx::migrate::MigrateError;
use std::sync::Arc;

#[async_trait]
pub trait VenObjectPrivacy: Send + Sync + 'static {
    /// Returns the union of the VEN object [`targets`](field@BlVenRequest::targets) with all resources [`targets`](field@BlResourceRequest::targets) of resources that belong to the VEN.
    async fn targets_by_client_id(&self, client_id: &ClientId) -> Result<Vec<Target>, AppError>;
}

#[async_trait]
pub trait Crud: Send + Sync + 'static {
    type Type;
    type Id;
    type NewType;
    type Error;
    type Filter;
    type PermissionFilter;

    async fn create(
        &self,
        new: Self::NewType,
        permission_filter: &Self::PermissionFilter,
    ) -> Result<Self::Type, Self::Error>;
    async fn retrieve(
        &self,
        id: &Self::Id,
        permission_filter: &Self::PermissionFilter,
    ) -> Result<Self::Type, Self::Error>;
    async fn retrieve_all(
        &self,
        filter: &Self::Filter,
        permission_filter: &Self::PermissionFilter,
    ) -> Result<Vec<Self::Type>, Self::Error>;
    async fn update(
        &self,
        id: &Self::Id,
        new: Self::NewType,
        permission_filter: &Self::PermissionFilter,
    ) -> Result<Self::Type, Self::Error>;
    async fn delete(
        &self,
        id: &Self::Id,
        permission_filter: &Self::PermissionFilter,
    ) -> Result<Self::Type, Self::Error>;
}

pub trait ProgramCrud:
    Crud<
    Type = Program,
    Id = ProgramId,
    NewType = ProgramRequest,
    Error = AppError,
    Filter = crate::api::program::QueryParams,
    PermissionFilter = Option<ClientId>,
>
{
}
pub trait ReportCrud:
    Crud<
    Type = Report,
    Id = ReportId,
    NewType = ReportRequest,
    Error = AppError,
    Filter = crate::api::report::QueryParams,
    PermissionFilter = Option<ClientId>,
>
{
}
pub trait EventCrud:
    Crud<
    Type = Event,
    Id = EventId,
    NewType = EventRequest,
    Error = AppError,
    Filter = crate::api::event::QueryParams,
    PermissionFilter = Option<ClientId>,
>
{
}

pub trait VenCrud:
    Crud<
    Type = Ven,
    Id = VenId,
    NewType = BlVenRequest,
    Error = AppError,
    Filter = crate::api::ven::QueryParams,
    PermissionFilter = Option<ClientId>,
>
{
}

pub trait ResourceCrud:
    Crud<
    Type = Resource,
    Id = ResourceId,
    NewType = BlResourceRequest,
    Error = AppError,
    Filter = crate::api::resource::QueryParams,
    PermissionFilter = Option<ClientId>,
>
{
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct UserDetails {
    pub(crate) id: String,
    pub(crate) reference: String,
    pub(crate) description: Option<String>,
    pub(crate) scope: Vec<Scope>,
    pub(crate) client_ids: Vec<ClientId>,
    #[serde(with = "openleadr_wire::serde_rfc3339")]
    pub(crate) created: DateTime<Utc>,
    #[serde(with = "openleadr_wire::serde_rfc3339")]
    pub(crate) modified: DateTime<Utc>,
}

impl UserDetails {
    pub fn id(&self) -> &str {
        &self.id
    }
}

#[async_trait]
#[cfg(feature = "internal-oauth")]
pub trait AuthSource: Send + Sync + 'static {
    async fn check_credentials(&self, client_id: &str, client_secret: &str) -> Option<AuthInfo>;
    async fn get_user(&self, user_id: &str) -> Result<UserDetails, AppError>;
    async fn get_all_users(&self) -> Result<Vec<UserDetails>, AppError>;
    async fn add_user(
        &self,
        reference: &str,
        description: Option<&str>,
        scope: &[Scope],
    ) -> Result<UserDetails, AppError>;
    async fn add_credential(
        &self,
        user_id: &str,
        client_id: &str,
        client_secret: &str,
    ) -> Result<UserDetails, AppError>;
    async fn remove_credentials(
        &self,
        user_id: &str,
        client_id: &str,
    ) -> Result<UserDetails, AppError>;
    async fn remove_user(&self, user_id: &str) -> Result<UserDetails, AppError>;
    async fn edit_user(
        &self,
        user_id: &str,
        reference: &str,
        description: Option<&str>,
        scope: &[Scope],
    ) -> Result<UserDetails, AppError>;
}

pub trait DataSource: Send + Sync + 'static {
    fn programs(&self) -> Arc<dyn ProgramCrud>;
    fn reports(&self) -> Arc<dyn ReportCrud>;
    fn events(&self) -> Arc<dyn EventCrud>;
    fn vens(&self) -> Arc<dyn VenCrud>;
    fn ven_object_privacy(&self) -> Arc<dyn VenObjectPrivacy>;
    fn resources(&self) -> Arc<dyn ResourceCrud>;
    #[cfg(feature = "internal-oauth")]
    fn auth(&self) -> Arc<dyn AuthSource>;
    fn connection_active(&self) -> bool;
}

#[async_trait]
pub trait Migrate {
    async fn migrate(&self) -> Result<(), MigrateError>;
}

#[derive(Debug, Clone)]
#[cfg(feature = "internal-oauth")]
pub struct AuthInfo {
    pub(crate) client_id: String,
    pub(crate) scope: Vec<Scope>,
}
