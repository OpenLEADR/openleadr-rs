use std::sync::Arc;

use openleadr_wire::{program::ProgramId, subscription::Operation, ven::VenId};

use crate::{ClientKind, MqttClient};

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
#[derive(Debug)]
pub struct MqttSubscription<K: ClientKind> {
    topic: String,
    target: MqttSubscriptionTarget,
    operation_filter: Option<Operation>,
    state: Arc<MqttClient<K>>,
}

impl<K: ClientKind> MqttSubscription<K> {
    pub fn new(
        topic: String,
        target: MqttSubscriptionTarget,
        operation_filter: Option<Operation>,
        state: Arc<MqttClient<K>>,
    ) -> Self {
        Self {
            topic,
            target,
            operation_filter,
            state,
        }
    }

    pub fn topic(&self) -> &str {
        &self.topic
    }
}

impl<K: ClientKind> Drop for MqttSubscription<K> {
    fn drop(&mut self) {
        // The mutex can only be poisoned from an earlier panic. It is ok for us
        // to propagate that to all the threads.
        let mut subscriptions = self.state.subscriptions.lock().unwrap();
        // The topic will always be present because of the refcounting.
        let refcount = subscriptions.get_mut(&self.topic).unwrap();
        // An underflowing refcount should never happen. If it does, something
        // is seriously wrong and aborting is the best solution.
        *refcount = refcount.checked_sub(1).unwrap();
        if *refcount == 0 {
            subscriptions.remove(&self.topic);
            self.state.mqtt_client.unsubscribe(&self.topic);
        }
    }
}

impl<K: ClientKind> Clone for MqttSubscription<K> {
    fn clone(&self) -> Self {
        // The mutex can only be poisoned from an earlier panic. It is ok for us
        // to propagate that to all the threads.
        let mut subscriptions = self.state.subscriptions.lock().unwrap();
        // The topic will always be present because of the refcounting.
        let refcount = subscriptions.get_mut(&self.topic).unwrap();
        // An overflowing reference count is an error condition from which we
        // cannot reasonably recover. A panic here is the best solution.
        *refcount = refcount.checked_add(1).unwrap();
        Self {
            target: self.target.clone(),
            topic: self.topic.clone(),
            operation_filter: self.operation_filter,
            state: self.state.clone(),
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
