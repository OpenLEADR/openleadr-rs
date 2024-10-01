//! Helper types to realize type values relations

use serde::{Deserialize, Serialize};

/// ValuesMap : Represents one or more values associated with a type. E.g. a type of PRICE contains a single float value.

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ValuesMap {
    /// Enumerated or private string signifying the nature of values. E.G. \"PRICE\" indicates value is to be interpreted as a currency.
    #[serde(rename = "type")]
    pub value_type: ValueType,
    /// A list of data points. Most often a singular value such as a price.
    pub values: Vec<Value>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ValueType(
    #[serde(deserialize_with = "crate::string_within_range_inclusive::<1, 128, _>")] pub String,
);

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Value {
    Integer(i64),
    Number(f64),
    Boolean(bool),
    Point(Point),
    String(String),
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Integer(s), Self::Integer(o)) => s == o,
            (Self::Boolean(s), Self::Boolean(o)) => s == o,
            (Self::Point(s), Self::Point(o)) => s == o,
            (Self::String(s), Self::String(o)) => s == o,
            (Self::Number(s), Self::Number(o)) if s.is_nan() && o.is_nan() => true,
            (Self::Number(s), Self::Number(o)) => s == o,
            _ => false,
        }
    }
}

impl Eq for Value {}

#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct Point {
    /// A value on an x axis.
    pub x: f32,
    /// A value on a y axis.
    pub y: f32,
}

impl Point {
    pub fn new(x: f32, y: f32) -> Self {
        Self { x, y }
    }
}
