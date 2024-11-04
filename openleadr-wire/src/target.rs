//! Types to filter resources

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct TargetMap(pub Vec<TargetEntry>);

// TODO: Handle strong typing of values
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TargetEntry {
    #[serde(rename = "type")]
    pub label: TargetLabel,
    pub values: [String; 1],
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Debug, JsonSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TargetLabel {
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

impl TargetLabel {
    pub fn as_str(&self) -> &str {
        match self {
            TargetLabel::PowerServiceLocation => "POWER_SERVICE_LOCATION",
            TargetLabel::ServiceArea => "SERVICE_AREA",
            TargetLabel::Group => "GROUP",
            TargetLabel::ResourceName => "RESOURCE_NAME",
            TargetLabel::VENName => "VEN_NAME",
            TargetLabel::EventName => "EVENT_NAME",
            TargetLabel::ProgramName => "PROGRAM_NAME",
            TargetLabel::Private(s) => s.as_str(),
        }
    }
}

impl Display for TargetLabel {
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
            serde_json::to_string(&TargetLabel::EventName).unwrap(),
            r#""EVENT_NAME""#
        );
        assert_eq!(
            serde_json::to_string(&TargetLabel::Private(String::from("something else"))).unwrap(),
            r#""something else""#
        );
        assert_eq!(
            serde_json::from_str::<TargetLabel>(r#""VEN_NAME""#).unwrap(),
            TargetLabel::VENName
        );
        assert_eq!(
            serde_json::from_str::<TargetLabel>(r#""something else""#).unwrap(),
            TargetLabel::Private(String::from("something else"))
        );
    }
}
