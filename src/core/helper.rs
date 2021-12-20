use arrow::datatypes::{DataType, Field, Schema};
use std::sync::Arc;

use super::data_type::ArrowType;
use crate::logical_plan::expression::*;

/// Helper function for generating an Arrow schema from a vector of tuples.
///
/// # Overview
/// Each tuple in the input corresponds to one field in the schema, and
/// each tuple contains the following fields:
/// ```
/// (name, datatype, nullable)
/// ```
///
/// # Example
/// ```
/// let schema = schema(vec![
///   ("col1", ArrowType::Int64, false),
///   ("col2", ArrowType::Int64, false),
/// ]);
/// ```
pub fn schema(fields: Vec<(&str, ArrowType, bool)>) -> Arc<Schema> {
    Arc::new(Schema::new(
        fields
            .iter()
            .map(move |(n, t, z)| Field::new(n, DataType::from(*t), z.clone()))
            .collect(),
    ))
}

/// Generate a column expression from a column name.
pub fn col(name: &str) -> Arc<ColumnExpression> {
    Arc::new(ColumnExpression::new(String::from(name)))
}

/// Helper trait for converting values into literal expressions.
/// :TODO: Implement for more types than string/i64.
pub trait IntoLit {
    fn into_lit(&self) -> Arc<dyn LogicalExpression>;
}

impl IntoLit for i64 {
    fn into_lit(&self) -> Arc<dyn LogicalExpression> {
        Arc::new(LiteralI64Expression::new(*self))
    }
}

impl IntoLit for &str {
    fn into_lit(&self) -> Arc<dyn LogicalExpression> {
        Arc::new(LiteralStringExpression::new(String::from(*self)))
    }
}

/// Generate a literal expression from a value reference that implements the `IntoLit` trait.
pub fn lit(val: &impl IntoLit) -> Arc<dyn LogicalExpression> {
    val.into_lit()
}

/// Generate a boolean expression that evaluates to true when the inputs are equal.
pub fn eq(l: Arc<dyn LogicalExpression>, r: Arc<dyn LogicalExpression>) -> Arc<BooleanExpression> {
    Arc::new(BooleanExpression::eq(l, r))
}

/// Generate a boolean expression that evaluates to true when the inputs are not equal.
pub fn neq(l: Arc<dyn LogicalExpression>, r: Arc<dyn LogicalExpression>) -> Arc<BooleanExpression> {
    Arc::new(BooleanExpression::neq(l, r))
}

/// Generate a boolean expression that evaluates to true when the left input is greater than the right.
pub fn gt(l: Arc<dyn LogicalExpression>, r: Arc<dyn LogicalExpression>) -> Arc<BooleanExpression> {
    Arc::new(BooleanExpression::gt(l, r))
}

/// Generate a boolean expression that evaluates to true when the left input is less than the right.
pub fn lt(l: Arc<dyn LogicalExpression>, r: Arc<dyn LogicalExpression>) -> Arc<BooleanExpression> {
    Arc::new(BooleanExpression::lt(l, r))
}

/// Generate a boolean expression that evaluates to true when the left input is greater than or equal to the right.
pub fn gteq(
    l: Arc<dyn LogicalExpression>,
    r: Arc<dyn LogicalExpression>,
) -> Arc<BooleanExpression> {
    Arc::new(BooleanExpression::gteq(l, r))
}

/// Generate a boolean expression that evaluates to true when the left input is less than or equal to the right.
pub fn lteq(
    l: Arc<dyn LogicalExpression>,
    r: Arc<dyn LogicalExpression>,
) -> Arc<BooleanExpression> {
    Arc::new(BooleanExpression::lteq(l, r))
}

/// Generate a boolean expression that evaluates to true when both inputs are "truthy".
pub fn and(l: Arc<dyn LogicalExpression>, r: Arc<dyn LogicalExpression>) -> Arc<BooleanExpression> {
    Arc::new(BooleanExpression::neq(l, r))
}

/// Generate a boolean expression that evaluates to true when at least one of the input is "truthy".
pub fn or(l: Arc<dyn LogicalExpression>, r: Arc<dyn LogicalExpression>) -> Arc<BooleanExpression> {
    Arc::new(BooleanExpression::neq(l, r))
}

/// Generate a math expression that sums the inputs.
pub fn add(l: Arc<dyn LogicalExpression>, r: Arc<dyn LogicalExpression>) -> Arc<MathExpression> {
    Arc::new(MathExpression::add(l, r))
}

/// Generate a math expression that subtracts the inputs.
pub fn subtract(
    l: Arc<dyn LogicalExpression>,
    r: Arc<dyn LogicalExpression>,
) -> Arc<MathExpression> {
    Arc::new(MathExpression::subtract(l, r))
}

/// Generate a math expression that multiplies the inputs.
pub fn multiply(
    l: Arc<dyn LogicalExpression>,
    r: Arc<dyn LogicalExpression>,
) -> Arc<MathExpression> {
    Arc::new(MathExpression::multiply(l, r))
}

/// Generate a math expression that divides the inputs.
pub fn divide(l: Arc<dyn LogicalExpression>, r: Arc<dyn LogicalExpression>) -> Arc<MathExpression> {
    Arc::new(MathExpression::divide(l, r))
}

/// Generate a math expression that takes the modulus of the first input with the second.
pub fn modulus(
    l: Arc<dyn LogicalExpression>,
    r: Arc<dyn LogicalExpression>,
) -> Arc<MathExpression> {
    Arc::new(MathExpression::modulus(l, r))
}

/// Generate an aggregate expression that sums the input.
pub fn sum(input: Arc<dyn LogicalExpression>) -> Arc<AggregateExpression> {
    Arc::new(AggregateExpression::sum(input))
}

/// Generate an aggregate expression that takes the min of the input.
pub fn min(input: Arc<dyn LogicalExpression>) -> Arc<AggregateExpression> {
    Arc::new(AggregateExpression::min(input))
}

/// Generate an aggregate expression that takes the max of the input.
pub fn max(input: Arc<dyn LogicalExpression>) -> Arc<AggregateExpression> {
    Arc::new(AggregateExpression::max(input))
}

/// Generate an aggregate expression that averages the input.
pub fn avg(input: Arc<dyn LogicalExpression>) -> Arc<AggregateExpression> {
    Arc::new(AggregateExpression::avg(input))
}
