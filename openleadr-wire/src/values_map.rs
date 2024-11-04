//! Helper types to realize type values relations

use serde::{Deserialize, Serialize};
use utoipa::{
    __dev::ComposeSchema, // TODO I'm probably using utoipa incorrectly, but a bound asked for this hidden trait.
    openapi::{
        schema::{AnyOf, Array, ArrayItems, Object, Ref, Schema, SchemaType, Type},
        RefOr
    },
    PartialSchema, ToSchema
};

/// ValuesMap : Represents one or more values associated with a type. E.g. a type of PRICE contains a single float value.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ValuesMap {
    /// Enumerated or private string signifying the nature of values. E.G. \"PRICE\" indicates value is to be interpreted as a currency.
    #[serde(rename = "type")]
    pub value_type: ValueType,
    /// A list of data points. Most often a singular value such as a price.
    pub values: Vec<Value>,
}

impl PartialSchema for ValuesMap {
    fn schema() -> RefOr<Schema> {
        let value_array_schema = {
            let array_items = ArrayItems::RefOrSchema(
                Box::new(Value::schema())
            );
            let example = serde_json::Value::Array(vec![
                serde_json::Value::Number(
                    serde_json::Number::from_f64(0.17)
                        .expect("The float above must not be infinite or NaN because these are not valid JSON numbers.")
                )
            ]);
            #[allow(deprecated)] // The example keyword was deprecated in OpenAPI 3.1.0 but is used by the OpenADR 3.0.1 spec.
            Array::builder()
                .items(array_items)
                .description(Some("A list of data points. Most often a singular value such as a price."))
                .example(Some(example))
                .build()
        };
        RefOr::T(Schema::Object(Object::builder()
            .property("type", ValueType::schema())
            .property("values", value_array_schema)
            .build()
        ))
    }
}

impl ToSchema for ValuesMap {
    fn schemas(schemas: &mut Vec<(String, RefOr<Schema>)>) {
        schemas.push((Point::name().into(), Point::schema()));
        <Point as ToSchema>::schemas(schemas);
    }
}

/// Enumerated or private string signifying the nature of values. E.G. \"PRICE\" indicates value is to be interpreted as a currency.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct ValueType(
    #[serde(deserialize_with = "crate::string_within_range_inclusive::<1, 128, _>")] pub String,
);

/// A list of data points. Most often a singular value such as a price.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Value {
    Integer(i64),
    Number(f64),
    Boolean(bool),
    Point(Point),
    String(String)
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

impl ComposeSchema for Value {
    fn compose(_: Vec<RefOr<Schema>>) -> RefOr<Schema> {
        let point_ref = Ref::builder()
            .ref_location_from_schema_name(Point::name())
            .build();
        let integer_schema = Object::builder()
            .schema_type(SchemaType::Type(Type::Integer))
            .build();
        let number_schema = Object::builder()
            .schema_type(SchemaType::Type(Type::Number))
            .build();
        RefOr::T(Schema::AnyOf(AnyOf::builder()
            .item(integer_schema)
            .item(number_schema)
            .item(bool::schema())
            .item(point_ref)
            .item(String::schema())
            .build()
        ))
    }
}

impl ToSchema for Value {
    fn schemas(schemas: &mut Vec<(String, RefOr<Schema>)>) {
        schemas.push((Point::name().into(), Point::schema()));
        <Point as ToSchema>::schemas(schemas);
    }
}

/// A pair of floats typically used as a point on a 2 dimensional grid.
#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize, ToSchema)]
pub struct Point {
    /// A value on an x axis.
    #[schema(example = 1.0, nullable = true, default = json!(null))]
    pub x: f32,
    /// A value on a y axis.
    #[schema(example = 2.0, nullable = true, default = json!(null))]
    pub y: f32,
}

impl Point {
    pub fn new(x: f32, y: f32) -> Self {
        Self { x, y }
    }
}
