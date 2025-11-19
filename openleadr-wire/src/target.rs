//! Types to filter resources

use crate::{Identifier};
use derive_more::FromStr;
use serde::{Deserialize, Serialize};
use std::fmt::Display;

/// User generated target string.
#[derive(
    Clone, Debug, PartialEq, Serialize, Deserialize, Hash, Eq, FromStr, sqlx::Type, PartialOrd, Ord
)]
#[sqlx(transparent)]
pub struct Target(pub(crate) Identifier);

impl Display for Target {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Target {
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}
