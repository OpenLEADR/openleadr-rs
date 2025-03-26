#[cfg(feature = "postgres")]
mod postgres;

use crate::{
    error::AppError,
    jwt::{AuthRole, Claims, User},
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use openleadr_wire::{
    event::{EventContent, EventId},
    program::{ProgramContent, ProgramId},
    report::{ReportContent, ReportId},
    resource::{Resource, ResourceContent, ResourceId},
    ven::{Ven, VenContent, VenId},
    Event, Program, Report,
};
#[cfg(feature = "postgres")]
pub use postgres::PostgresStorage;
use serde::{Deserialize, Serialize};
use sqlx::migrate::MigrateError;
use std::sync::Arc;

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

#[async_trait]
pub trait VenScopedCrud: Send + Sync + 'static {
    type Type;
    type Id;
    type NewType;
    type Error;
    type Filter;
    type PermissionFilter;

    async fn create(
        &self,
        new: Self::NewType,
        ven_id: VenId,
        permission_filter: &Self::PermissionFilter,
    ) -> Result<Self::Type, Self::Error>;
    async fn retrieve(
        &self,
        id: &Self::Id,
        ven_id: VenId,
        permission_filter: &Self::PermissionFilter,
    ) -> Result<Self::Type, Self::Error>;
    async fn retrieve_all(
        &self,
        ven_id: VenId,
        filter: &Self::Filter,
        permission_filter: &Self::PermissionFilter,
    ) -> Result<Vec<Self::Type>, Self::Error>;
    async fn update(
        &self,
        id: &Self::Id,
        ven_id: VenId,
        new: Self::NewType,
        permission_filter: &Self::PermissionFilter,
    ) -> Result<Self::Type, Self::Error>;
    async fn delete(
        &self,
        id: &Self::Id,
        ven_id: VenId,
        permission_filter: &Self::PermissionFilter,
    ) -> Result<Self::Type, Self::Error>;
}

pub trait ProgramCrud:
    Crud<
    Type = Program,
    Id = ProgramId,
    NewType = ProgramContent,
    Error = AppError,
    Filter = crate::api::program::QueryParams,
    PermissionFilter = User,
>
{
}
pub trait ReportCrud:
    Crud<
    Type = Report,
    Id = ReportId,
    NewType = ReportContent,
    Error = AppError,
    Filter = crate::api::report::QueryParams,
    PermissionFilter = User,
>
{
}
pub trait EventCrud:
    Crud<
    Type = Event,
    Id = EventId,
    NewType = EventContent,
    Error = AppError,
    Filter = crate::api::event::QueryParams,
    PermissionFilter = User,
>
{
}

pub enum VenPermissions {
    AllAllowed,
    Specific(Vec<VenId>),
}

impl VenPermissions {
    pub fn as_value(&self) -> Option<Vec<String>> {
        match self {
            VenPermissions::AllAllowed => None,
            VenPermissions::Specific(ids) => {
                Some(ids.iter().map(|id| id.to_string()).collect::<Vec<_>>())
            }
        }
    }
}

impl TryFrom<Claims> for VenPermissions {
    type Error = AppError;

    fn try_from(claims: Claims) -> Result<Self, Self::Error> {
        if claims.is_ven_manager() {
            Ok(VenPermissions::AllAllowed)
        } else if claims.is_ven() {
            Ok(VenPermissions::Specific(claims.ven_ids()))
        } else {
            Err(AppError::Forbidden(
                "User not authorized to access this vens",
            ))
        }
    }
}

pub trait VenCrud:
    Crud<
    Type = Ven,
    Id = VenId,
    NewType = VenContent,
    Error = AppError,
    Filter = crate::api::ven::QueryParams,
    PermissionFilter = VenPermissions,
>
{
}

pub trait ResourceCrud:
    VenScopedCrud<
    Type = Resource,
    Id = ResourceId,
    NewType = ResourceContent,
    Error = AppError,
    Filter = crate::api::resource::QueryParams,
    PermissionFilter = User,
>
{
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct UserDetails {
    pub(crate) id: String,
    pub(crate) reference: String,
    pub(crate) description: Option<String>,
    pub(crate) roles: Vec<AuthRole>,
    pub(crate) client_ids: Vec<String>,
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
pub trait AuthSource: Send + Sync + 'static {
    #[cfg(feature = "internal-oauth")]
    async fn check_credentials(&self, client_id: &str, client_secret: &str) -> Option<AuthInfo>;
    async fn get_user(&self, user_id: &str) -> Result<UserDetails, AppError>;
    async fn get_all_users(&self) -> Result<Vec<UserDetails>, AppError>;
    async fn add_user(
        &self,
        reference: &str,
        description: Option<&str>,
        roles: &[AuthRole],
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
        roles: &[AuthRole],
    ) -> Result<UserDetails, AppError>;
}

pub trait DataSource: Send + Sync + 'static {
    fn programs(&self) -> Arc<dyn ProgramCrud>;
    fn reports(&self) -> Arc<dyn ReportCrud>;
    fn events(&self) -> Arc<dyn EventCrud>;
    fn vens(&self) -> Arc<dyn VenCrud>;
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
    pub(crate) roles: Vec<AuthRole>,
}
