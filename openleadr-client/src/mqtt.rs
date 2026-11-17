use std::{
    collections::HashMap,
    sync::{Arc, Mutex, MutexGuard},
};

use openleadr_wire::{
    program::ProgramId,
    subscription::{Notification, Operation},
    ven::VenId,
};
use paho_mqtt::TopicFilter;
use tokio::sync::broadcast;

use crate::{Error, error::Result};

const MQTT_NOTIFICATION_BUFFER: usize = 128;

pub(crate) struct TopicEntry {
    pub(crate) refcount: usize,
    filter: TopicFilter,
    sender: broadcast::Sender<Option<paho_mqtt::Message>>,
}

impl TopicEntry {
    pub(crate) fn new(topic: &str) -> Self {
        Self {
            refcount: 0,
            filter: TopicFilter::new_unchecked(topic),
            sender: broadcast::channel(MQTT_NOTIFICATION_BUFFER).0,
        }
    }

    pub(crate) fn receiver(&self) -> broadcast::Receiver<Option<paho_mqtt::Message>> {
        self.sender.subscribe()
    }
}

pub(crate) struct MqttSubscriptionRegistry {
    pub(crate) mqtt_client: paho_mqtt::AsyncClient,
    subscriptions: Mutex<HashMap<String, TopicEntry>>,
}

impl MqttSubscriptionRegistry {
    pub(crate) fn new(mqtt_client: paho_mqtt::AsyncClient) -> Self {
        Self {
            mqtt_client,
            subscriptions: Mutex::default(),
        }
    }

    pub(crate) fn subscriptions(&self) -> MutexGuard<'_, HashMap<String, TopicEntry>> {
        // The mutex can only be poisoned from an earlier panic. It is ok for
        // us to propagate that to all the threads.
        self.subscriptions.lock().unwrap()
    }

    pub(crate) fn dispatch(&self, message: Option<paho_mqtt::Message>) {
        for entry in self.subscriptions().values() {
            if message
                .as_ref()
                // None indicates a dropped connection, which is sent to all
                // subscribers
                .is_none_or(|message| entry.filter.is_match(message.topic()))
            {
                // Sending only fails without receivers, i.e., when nobody is
                // interested in the message.
                let _ = entry.sender.send(message.clone());
            }
        }
    }
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

/// A subscription made with one of the `subscribe_*` methods of
/// [`MqttClient`](crate::MqttClient). Receive its notifications with
/// [`recv`](Self::recv).
///
/// Dropping the subscription unsubscribes from the topic, unless another
/// subscription to the same topic (e.g., a clone) is still alive.
pub struct MqttSubscription {
    topic: String,
    target: MqttSubscriptionTarget,
    operation_filter: Option<Operation>,
    receiver: broadcast::Receiver<Option<paho_mqtt::Message>>,
    registry: Arc<MqttSubscriptionRegistry>,
}

impl MqttSubscription {
    pub(crate) fn new(
        topic: String,
        target: MqttSubscriptionTarget,
        operation_filter: Option<Operation>,
        receiver: broadcast::Receiver<Option<paho_mqtt::Message>>,
        registry: Arc<MqttSubscriptionRegistry>,
    ) -> Self {
        Self {
            topic,
            target,
            operation_filter,
            receiver,
            registry,
        }
    }

    /// The MQTT topic this subscription is for. May contain wildcards.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Waits for the next notification of this subscription.
    ///
    /// Every subscription, including every clone, receives every notification
    /// that arrives after it was created.
    ///
    /// The VTN may publish one change on several topics (e.g., a targeted event
    /// on both the [`subscribe_events`](crate::MqttClient::subscribe_events)
    /// and the
    /// [`subscribe_ven_events`](crate::MqttClient::subscribe_ven_events)
    /// topic), so different subscriptions can receive the same change. Use
    /// [`Notification::id`] to deduplicate across subscriptions.
    ///
    /// # Errors
    ///
    /// Errors do not end the subscription; calling `recv` again continues with
    /// the next notification.
    ///
    /// - [`Error::Serde`] if a message is not a valid [`Notification`].
    /// - [`Error::MqttNotificationsLagged`] if this subscription fell behind
    ///   and notifications were dropped.
    /// - [`Error::Mqtt`] with [`paho_mqtt::Error::Disconnected`] if the
    ///   connection to the broker was lost. No notifications arrive after this;
    ///   reconnect with [`Client::connect_mqtt`](crate::Client::connect_mqtt)
    ///   and subscribe again.
    pub async fn recv(&mut self) -> Result<Notification> {
        match self.receiver.recv().await {
            Ok(Some(message)) => serde_json::from_slice(message.payload()).map_err(Into::into),
            Ok(None) => Err(Error::Mqtt(paho_mqtt::Error::Disconnected)),
            Err(broadcast::error::RecvError::Lagged(skipped)) => {
                Err(Error::MqttNotificationsLagged(skipped))
            }
            // Cannot happen, as the sender lives in the registry for as long
            // as this subscription is alive.
            Err(broadcast::error::RecvError::Closed) => {
                Err(Error::Mqtt(paho_mqtt::Error::Disconnected))
            }
        }
    }
}

impl Drop for MqttSubscription {
    fn drop(&mut self) {
        let mut subscriptions = self.registry.subscriptions();

        // The subcription should always be present because of the refcounting.
        let entry = subscriptions.get_mut(&self.topic).unwrap();

        // An underflowing refcount should never happen. If it does, something
        // is seriously wrong and aborting is the best solution.
        entry.refcount = entry.refcount.checked_sub(1).unwrap();

        if entry.refcount == 0 {
            subscriptions.remove(&self.topic);
            drop(self.registry.mqtt_client.unsubscribe(&self.topic));
        }
    }
}

impl Clone for MqttSubscription {
    fn clone(&self) -> Self {
        let mut subscriptions = self.registry.subscriptions();

        // The subcription should always be present because of the refcounting.
        let entry = subscriptions.get_mut(&self.topic).unwrap();

        // An overflowing reference count is an error condition from which we
        // cannot reasonably recover. A panic here is the best solution.
        entry.refcount = entry.refcount.checked_add(1).unwrap();

        Self::new(
            self.topic.clone(),
            self.target.clone(),
            self.operation_filter,
            entry.receiver(),
            self.registry.clone(),
        )
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
