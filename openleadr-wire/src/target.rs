//! Types to filter resources

use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct TargetMap(pub Vec<TargetEntry>);

// TODO: Handle strong typing of values
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TargetEntry {
    #[serde(rename = "type")]
    pub label: TargetType,
    pub values: [String; 1],
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Debug)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TargetType {
    /// A Power Service Location is a utility named specific location in
    /// geography or the distribution system, usually the point of service to a
    /// customer site.
    PowerServiceLocation,
    /// A Service Area is a utility named geographic region.
    ServiceArea,
    /// Targeting a specific group (string).
    Group,
    /// Targeting a specific resource (string).
    ResourceName,
    /// Targeting a specific VEN (string).
    #[serde(rename = "VEN_NAME")]
    VENName,
    /// Targeting a specific event (string).
    EventName,
    /// Targeting a specific program (string).
    ProgramName,
    /// An application specific privately defined target.
    #[serde(untagged)]
    #[serde(deserialize_with = "crate::string_within_range_inclusive::<1, 128, _>")]
    Private(String),
}

impl TargetType {
    pub fn as_str(&self) -> &str {
        match self {
            TargetType::PowerServiceLocation => "POWER_SERVICE_LOCATION",
            TargetType::ServiceArea => "SERVICE_AREA",
            TargetType::Group => "GROUP",
            TargetType::ResourceName => "RESOURCE_NAME",
            TargetType::VENName => "VEN_NAME",
            TargetType::EventName => "EVENT_NAME",
            TargetType::ProgramName => "PROGRAM_NAME",
            TargetType::Private(s) => s.as_str(),
        }
    }
}

impl Display for TargetType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_target_serialization() {
        assert_eq!(
            serde_json::to_string(&TargetType::EventName).unwrap(),
            r#""EVENT_NAME""#
        );
        assert_eq!(
            serde_json::to_string(&TargetType::Private(String::from("something else"))).unwrap(),
            r#""something else""#
        );
        assert_eq!(
            serde_json::from_str::<TargetType>(r#""VEN_NAME""#).unwrap(),
            TargetType::VENName
        );
        assert_eq!(
            serde_json::from_str::<TargetType>(r#""something else""#).unwrap(),
            TargetType::Private(String::from("something else"))
        );
    }
}
