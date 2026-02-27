use std::{
    fmt::{Display, Formatter},
    str::FromStr,
};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, skip_serializing_none};
use validator::Validate;

use crate::{
    program::ProgramId, resource::Resource, Event, Identifier, IdentifierError, ObjectType,
    Program, Report, Ven,
};

/// Server provided representation of subscription
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Validate)]
#[serde(rename_all = "camelCase")]
pub struct Subscription {
    /// URL safe VTN assigned object ID.
    pub id: SubscriptionId,
    /// datetime in ISO 8601 format
    #[serde(with = "crate::serde_rfc3339")]
    pub created_date_time: DateTime<Utc>,
    /// datetime in ISO 8601 format
    #[serde(with = "crate::serde_rfc3339")]
    pub modification_date_time: DateTime<Utc>,
    #[serde(flatten)]
    #[validate(nested)]
    pub content: SubscriptionRequest,
}

/// An object created by a client to receive notification of operations on objects.
/// Clients may subscribe to be notified when a type of object is created,
/// updated, or deleted.
#[skip_serializing_none]
#[serde_as]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Validate)]
#[serde(rename_all = "camelCase")]
pub struct SubscriptionRequest {
    /// User generated identifier, may be VEN identifier provisioned out-of-band.
    pub client_name: String,

    /// ID attribute of the program object this subscription is associated with.
    #[serde(rename = "programID")]
    pub program_id: Option<ProgramId>,

    /// list of objects and operations to subscribe to.
    pub object_operations: Vec<SubscriptionObjectOperation>,
    // /// A list of target objects. Used by server to filter notifications.
    // #[serde(default)]
    // #[serde_as(deserialize_as = "DefaultOnNull")]
    // pub targets: Vec<Target>,
}

#[skip_serializing_none]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Validate)]
#[serde(rename_all = "camelCase")]
pub struct SubscriptionObjectOperation {
    /// list of objects to subscribe to.
    pub objects: Vec<ObjectType>,

    /// list of operations to subscribe to.
    pub operations: Vec<Operation>,

    /// The transport mechanism used to deliver the notification
    #[serde(default)]
    pub mechanism: NotificationMechanism,

    /// User provided webhook URL. Required if `mechanism` is "WEBHOOK"
    pub callback_url: Option<String>,

    /// User provided token.
    /// To avoid custom integrations, callback endpoints
    /// should accept the provided bearer token to authenticate VTN requests.
    pub bearer_token: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum Operation {
    Create,
    Update,
    Delete,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Default)]
#[serde(rename_all = "UPPERCASE")]
pub enum NotificationMechanism {
    #[default]
    Webhook,
    Websocket,
}

/// URL safe VTN assigned object ID
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Hash, Eq)]
pub struct SubscriptionId(pub(crate) Identifier);

impl Display for SubscriptionId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl SubscriptionId {
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl FromStr for SubscriptionId {
    type Err = IdentifierError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(s.parse()?))
    }
}

///  VTN generated object included in request to subscription callbackUrl.
#[skip_serializing_none]
#[serde_as]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Validate)]
#[serde(rename_all = "camelCase")]
pub struct Notification {
    /// A unique ID of the operation that triggered this notification.
    /// Used to acknowledge receiving a notification over websockets and to allow a VEN to deduplicate notifications.
    /// A duplication could happend due to multiple subscriptions of (partly) overlying operations, possibly over different
    /// notification mechanisms, or if a webhook call returns an error code and gets therefore retried, for example.
    ///
    /// Note that this an ID of the operation (create, update, ...) that triggerd this notification. This means,
    /// multiple subscribers can get the same ID, possibly over different notification channels (webhook, websocket, MQTT).
    ///
    /// The exact structure of this ID is up to the implementation, but using a counter or UUID is RECOMMENDED.
    pub id: Identifier,

    /// the operation on on object that triggered the notification.
    pub operation: Operation,

    /// the object that is the subject of the notification.
    #[serde(flatten)]
    pub object: AnyObject,
    // /// A list of targets.
    // #[serde(default)]
    // #[serde_as(deserialize_as = "DefaultOnNull")]
    // pub targets: Vec<Target>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "objectType", content = "object", rename_all = "UPPERCASE")]
pub enum AnyObject {
    Program(Program),
    Report(Report),
    Event(Event),
    Subscription(Subscription),
    Ven(Ven),
    Resource(Resource),
}

impl AnyObject {
    pub fn id(&self) -> Identifier {
        match self {
            AnyObject::Program(program) => program.id.0.clone(),
            AnyObject::Report(report) => report.id.0.clone(),
            AnyObject::Event(event) => event.id.0.clone(),
            AnyObject::Subscription(subscription) => subscription.id.0.clone(),
            AnyObject::Ven(ven) => ven.id.0.clone(),
            AnyObject::Resource(resource) => resource.id.0.clone(),
        }
    }
}

/// Provides details of each notifier binding supported
#[skip_serializing_none]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Validate)]
#[serde(rename_all = "UPPERCASE")]
pub struct NotifiersResponse {
    pub websocket: bool,
}

#[cfg(test)]
mod tests {
    use crate::program::ProgramRequest;

    use super::*;

    #[test]
    fn parse_subscription_request() {
        let example = r#"{
  "clientName": "myClient",
  "programID": "44",
  "objectOperations": [
    {
      "callbackUrl": "https://myserver.com/event_callbacks",
      "operations": [
        "CREATE",
        "UPDATE"
      ],
      "objects": [
        "EVENT"
      ]
    },
    {
      "callbackUrl": "https://myserver.com/program_callbacks",
      "operations": [
        "CREATE",
        "UPDATE"
      ],
      "objects": [
        "PROGRAM"
      ]
    }
  ]
}"#;
        assert_eq!(
            serde_json::from_str::<SubscriptionRequest>(example).unwrap(),
            SubscriptionRequest {
                client_name: "myClient".to_owned(),
                program_id: Some("44".parse().unwrap()),
                object_operations: vec![
                    SubscriptionObjectOperation {
                        objects: vec![ObjectType::Event],
                        operations: vec![Operation::Create, Operation::Update],
                        mechanism: NotificationMechanism::Webhook,
                        callback_url: Some("https://myserver.com/event_callbacks".to_owned()),
                        bearer_token: None,
                    },
                    SubscriptionObjectOperation {
                        objects: vec![ObjectType::Program],
                        operations: vec![Operation::Create, Operation::Update],
                        mechanism: NotificationMechanism::Webhook,
                        callback_url: Some("https://myserver.com/program_callbacks".to_owned()),
                        bearer_token: None,
                    }
                ],
                // targets: vec![],
            }
        );
    }

    #[test]
    fn parse_notification() {
        let example = r#"{
  "id": "100",
  "objectType": "PROGRAM",
  "operation": "UPDATE",
  "object": {
    "bindingEvents": false,
    "createdDateTime": "2023-06-15T15:51:29.000Z",
    "modificationDateTime": "2023-06-15T15:51:29.000Z",
    "id": "0",
    "localPrice": false,
    "objectType": "PROGRAM",
    "programName": "myProgram"
  }
}"#;
        assert_eq!(
            serde_json::from_str::<Notification>(example).unwrap(),
            Notification {
                id: "100".parse().unwrap(),
                operation: Operation::Update,
                object: AnyObject::Program(Program {
                    id: "0".parse().unwrap(),
                    created_date_time: "2023-06-15T15:51:29.000Z".parse().unwrap(),
                    modification_date_time: "2023-06-15T15:51:29.000Z".parse().unwrap(),
                    content: ProgramRequest {
                        program_name: "myProgram".to_owned(),
                        interval_period: None,
                        program_descriptions: None,
                        payload_descriptors: None,
                        attributes: None,
                        targets: vec![],
                    }
                }),
                // targets: vec![],
            }
        );
    }
}
