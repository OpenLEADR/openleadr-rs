use std::{
    collections::HashMap,
    marker::PhantomData,
    sync::{Arc, Mutex, MutexGuard},
};

use openleadr_wire::{
    Event, Identifier, ObjectType, Program, Report, Ven,
    program::ProgramId,
    resource::Resource,
    resource_group::ResourceGroup,
    subscription::{AnyObject, Notification, Operation},
    ven::VenId,
};
use paho_mqtt::TopicFilter;
use tokio::sync::broadcast;

use crate::{Error, error::Result, sealed::SealedOadrObject};

const MQTT_NOTIFICATION_BUFFER: usize = 128;

mod sealed {}

/// An object that an [`MqttSubscription`] can receive notifications about.
///
/// This trait is sealed: it is implemented for [`Program`], [`Event`],
/// [`Report`], [`Ven`], [`Resource`] and [`ResourceGroup`] only.
pub trait OadrObject: SealedOadrObject {}

macro_rules! impl_oadr_object {
    ($($ty:ident),*) => {$(
        impl SealedOadrObject for $ty {
            const OBJECT_TYPE: ObjectType = ObjectType::$ty;

            fn from_any(object: AnyObject) -> Option<Self> {
                match object {
                    AnyObject::$ty(inner) => Some(inner),
                    _ => None,
                }
            }
        }
        impl OadrObject for $ty {}
    )*};
}
impl_oadr_object!(Program, Event, Report, Ven, Resource, ResourceGroup);

/// A notification received by an [`MqttSubscription`] about an object of type `O`.
///
/// To act on the object, turn it into a client with e.g. [`Client::program_client`] or
/// [`MqttClient::event_client`].
#[derive(Clone, Debug, PartialEq)]
pub struct MqttNotification<O> {
    /// The ID of the operation that triggered this notification. The same change can arrive on
    /// several subscriptions with the same ID, so it can be used to deduplicate them.
    pub id: Identifier,
    /// The operation on the object that triggered the notification.
    pub operation: Operation,
    /// The object the notification is about. For [`Operation::Delete`] this is the object as it was
    /// right before its deletion.
    pub object: O,
}

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

#[derive(Default)]
pub(crate) struct MqttSubscriptionRegistry {
    subscriptions: Mutex<HashMap<String, TopicEntry>>,
}

impl MqttSubscriptionRegistry {
    pub(crate) fn subscriptions(&self) -> MutexGuard<'_, HashMap<String, TopicEntry>> {
        // The mutex can only be poisoned from an earlier panic. It is ok for
        // us to propagate that to all the threads.
        self.subscriptions.lock().unwrap()
    }

    pub(crate) fn dispatch(&self, message: Option<paho_mqtt::Message>) {
        for (topic, entry) in self.subscriptions().iter() {
            if message
                .as_ref()
                // None indicates a dropped connection, which is sent to all
                // subscribers
                .is_some_and(|message| !entry.filter.is_match(message.topic()))
            {
                continue;
            }

            if entry.sender.send(message.clone()).is_err() {
                // Sending should never fail
                tracing::error!("MQTT subscription to '{topic}' without receivers");
            }
        }
    }
}

#[derive(Debug)]
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
pub struct MqttSubscription<O> {
    topic: String,
    receiver: broadcast::Receiver<Option<paho_mqtt::Message>>,
    registry: Arc<MqttSubscriptionRegistry>,
    mqtt_client: paho_mqtt::AsyncClient,
    // Produces `O`s but never holds one
    object: PhantomData<fn() -> O>,
}

impl<O> MqttSubscription<O> {
    pub(crate) fn new(
        topic: &str,
        receiver: broadcast::Receiver<Option<paho_mqtt::Message>>,
        registry: Arc<MqttSubscriptionRegistry>,
        mqtt_client: paho_mqtt::AsyncClient,
    ) -> Self {
        Self {
            topic: topic.to_string(),
            receiver,
            registry,
            mqtt_client,
            object: PhantomData,
        }
    }

    /// The MQTT topic this subscription is for. May contain wildcards.
    pub fn topic(&self) -> &str {
        &self.topic
    }
}

impl<O: OadrObject> MqttSubscription<O> {
    /// Waits for the next notification of this subscription.
    ///
    /// Every subscription, including every clone, receives every notification for its topic that
    /// arrives after it was created.
    ///
    /// The VTN may publish one change on several topics (e.g., a targeted event on both the
    /// [`subscribe_events`](crate::MqttClient::subscribe_events) and the
    /// [`subscribe_ven_events`](crate::MqttClient::subscribe_ven_events) topic), so different
    /// subscriptions can receive the same change. Use [`MqttNotification::id`] to deduplicate
    /// across subscriptions.
    ///
    /// # Errors
    ///
    /// Errors do not end the subscription; calling `recv` again continues with
    /// the next notification.
    ///
    /// - [`Error::Serde`] if a message is not a valid [`Notification`].
    /// - [`Error::MqttIncorrectObjectType`] if a notification is about another
    ///   kind of object than this subscription is for.
    /// - [`Error::MqttNotificationsLagged`] if this subscription fell behind
    ///   and notifications were dropped.
    /// - [`Error::Mqtt`] with [`paho_mqtt::Error::Disconnected`] if the
    ///   connection to the broker was lost. No notifications arrive after this;
    ///   reconnect with [`Client::connect_mqtt`](crate::Client::connect_mqtt)
    ///   and subscribe again.
    ///
    /// # Examples
    ///
    /// `recv` is cancel safe, so it can be used in e.g. [`tokio::select!`]:
    ///
    /// ```no_run
    /// # use openleadr_client::{BusinessLogic, Client, ClientCredentials};
    /// # use openleadr_wire::subscription::Operation;
    /// # async fn example() -> Result<(), openleadr_client::Error> {
    /// let credentials =
    ///     ClientCredentials::new("client_id".to_string(), "client_secret".to_string());
    /// let client = Client::<BusinessLogic>::with_url(
    ///     "https://your-vtn.com".parse().unwrap(),
    ///     Some(credentials),
    /// );
    /// let mqtt_client = client.connect_mqtt().await?;
    /// let mut vens = mqtt_client.subscribe_vens(Some(Operation::Create)).await?;
    /// let mut reports = mqtt_client.subscribe_reports(None).await?;
    /// loop {
    ///     tokio::select! {
    ///         ven = vens.recv() => {
    ///             // handle the VEN notification
    ///         }
    ///         report = reports.recv() => {
    ///             // handle the report notification
    ///         }
    ///     }
    /// }
    /// # }
    /// ```
    pub async fn recv(&mut self) -> Result<MqttNotification<O>> {
        match self.receiver.recv().await {
            Ok(Some(message)) => {
                let Notification {
                    id,
                    operation,
                    object,
                } = serde_json::from_slice(message.payload()).map_err(Error::Serde)?;

                let actual = object.kind();
                let object = O::from_any(object).ok_or(Error::MqttIncorrectObjectType {
                    expected: O::OBJECT_TYPE,
                    actual,
                })?;

                Ok(MqttNotification {
                    id,
                    operation,
                    object,
                })
            }
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

impl<O> Drop for MqttSubscription<O> {
    fn drop(&mut self) {
        let mut subscriptions = self.registry.subscriptions();

        // The subcription should always be present because of the refcounting.
        let entry = subscriptions.get_mut(&self.topic).unwrap();

        // An underflowing refcount should never happen. If it does, something
        // is seriously wrong and aborting is the best solution.
        entry.refcount = entry.refcount.checked_sub(1).unwrap();

        if entry.refcount == 0 {
            subscriptions.remove(&self.topic);
            self.mqtt_client.unsubscribe(&self.topic);
        }
    }
}

impl<O> Clone for MqttSubscription<O> {
    fn clone(&self) -> Self {
        let mut subscriptions = self.registry.subscriptions();

        // The subcription should always be present because of the refcounting.
        let entry = subscriptions.get_mut(&self.topic).unwrap();

        // An overflowing reference count is an error condition from which we
        // cannot reasonably recover. A panic here is the best solution.
        entry.refcount = entry.refcount.checked_add(1).unwrap();

        Self::new(
            &self.topic,
            entry.receiver(),
            self.registry.clone(),
            self.mqtt_client.clone(),
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

#[cfg(test)]
mod tests {
    use openleadr_wire::Program;
    use paho_mqtt::{CreateOptionsBuilder, PersistenceType, QoS};

    use super::*;
    use crate::{Client, MqttClient, VirtualEndNode};

    /// An `MqttClient` whose paho client never connects. Subscribe and
    /// unsubscribe requests fail immediately, which leaves the refcounting
    /// unaffected.
    fn mqtt_client() -> MqttClient<VirtualEndNode> {
        let client = Client::with_url("https://vtn.example.com".parse().unwrap(), None);
        let mqtt_client = CreateOptionsBuilder::new()
            .server_uri("mqtt://localhost:1")
            .persistence(PersistenceType::None)
            .create_client()
            .unwrap();

        MqttClient {
            client_ref: client.client_ref,
            mqtt_client,
            registry: Arc::default(),
        }
    }

    fn subscribe<O: OadrObject>(
        client: &MqttClient<VirtualEndNode>,
        topic: &str,
    ) -> MqttSubscription<O> {
        client.subscribe_topic(topic).1
    }

    /// `None` if the topic has no entry, i.e., is not subscribed to.
    fn refcount(client: &MqttClient<VirtualEndNode>, topic: &str) -> Option<usize> {
        client
            .registry
            .subscriptions()
            .get(topic)
            .map(|entry| entry.refcount)
    }

    #[test]
    fn subscriptions_to_one_topic_share_an_entry() {
        let client = mqtt_client();

        let first = subscribe::<Program>(&client, "programs/+");
        assert_eq!(refcount(&client, "programs/+"), Some(1));

        let second = subscribe::<Program>(&client, "programs/+");
        assert_eq!(refcount(&client, "programs/+"), Some(2));
        assert_eq!(client.registry.subscriptions().len(), 1);

        drop(first);
        assert_eq!(refcount(&client, "programs/+"), Some(1));

        drop(second);
        assert_eq!(refcount(&client, "programs/+"), None);
    }

    #[test]
    fn clone_takes_a_reference() {
        let client = mqtt_client();

        let original = subscribe::<Program>(&client, "programs/+");
        let clone = original.clone();
        assert_eq!(refcount(&client, "programs/+"), Some(2));

        drop(original);
        assert_eq!(refcount(&client, "programs/+"), Some(1));

        drop(clone);
        assert_eq!(refcount(&client, "programs/+"), None);
    }

    #[test]
    fn topics_are_counted_independently() {
        let client = mqtt_client();

        let creates = subscribe::<Program>(&client, "programs/create");
        let deletes = subscribe::<Program>(&client, "programs/delete");
        assert_eq!(refcount(&client, "programs/create"), Some(1));
        assert_eq!(refcount(&client, "programs/delete"), Some(1));

        drop(creates);
        assert_eq!(refcount(&client, "programs/create"), None);
        assert_eq!(refcount(&client, "programs/delete"), Some(1));

        drop(deletes);
        assert!(client.registry.subscriptions().is_empty());
    }

    #[test]
    fn resubscribing_after_last_drop_starts_a_new_entry() {
        let client = mqtt_client();

        drop(subscribe::<Program>(&client, "programs/+"));
        assert_eq!(refcount(&client, "programs/+"), None);

        let _subscription = subscribe::<Program>(&client, "programs/+");
        assert_eq!(refcount(&client, "programs/+"), Some(1));
    }

    #[test]
    fn subscription_keeps_receiving_after_clone_is_dropped() {
        let client = mqtt_client();

        let mut original = subscribe::<Program>(&client, "programs/+");
        drop(original.clone());

        client.registry.dispatch(Some(paho_mqtt::Message::new(
            "programs/create",
            "{}",
            QoS::AtMostOnce,
        )));

        let message = original.receiver.try_recv().unwrap().unwrap();
        assert_eq!(message.topic(), "programs/create");
    }
}
