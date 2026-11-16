use std::{collections::HashMap, sync::Arc};

use openleadr_wire::{program::ProgramId, subscription::Operation, ven::VenId};
use tokio::sync::Mutex;

struct MqttSubscriptionTable {
    subscriptions: Arc<Mutex<HashMap<MqttSubscription, usize>>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum MqttSubscriptionTarget {
    Programs,
    Program(ProgramId),
    Events,
    ProgramEvents(ProgramId),
    Reports,
    Vens,
    Ven(VenId),
    Resources,
    ResourceGroups,
    VenEvents(VenId),
    VenPrograms(VenId),
    VenResources(VenId),
    VenResourceGroups(VenId),
}

/// A subscription made with one of the `subscribe_*` methods of [`MqttClient`].
///
/// Pass it to [`MqttClient::unsubscribe`] to remove the subscription again.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct MqttSubscription {
    target: MqttSubscriptionTarget,
    operation_filter: Option<Operation>,
}

impl MqttSubscription {
    pub fn new(target: MqttSubscriptionTarget, operation_filter: Option<Operation>) -> Self {
        Self {
            target,
            operation_filter,
        }
    }
}

impl MqttSubscriptionTarget {
    pub(crate) fn endpoint(&self) -> String {
        let path = match self {
            Self::Programs => "programs".to_string(),
            Self::Program(program_id) => format!("programs/{program_id}"),
            Self::Events => "events".to_string(),
            Self::ProgramEvents(program_id) => format!("programs/{program_id}/events"),
            Self::Reports => "reports".to_string(),
            Self::Vens => "vens".to_string(),
            Self::Ven(ven_id) => format!("vens/{ven_id}"),
            Self::Resources => "resources".to_string(),
            Self::ResourceGroups => "resource_groups".to_string(),
            Self::VenEvents(ven_id) => format!("vens/{ven_id}/events"),
            Self::VenPrograms(ven_id) => format!("vens/{ven_id}/programs"),
            Self::VenResources(ven_id) => format!("vens/{ven_id}/resources"),
            Self::VenResourceGroups(ven_id) => format!("vens/{ven_id}/resource_groups"),
        };
        format!("notifiers/mqtt/topics/{path}")
    }
}
