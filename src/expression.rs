use crate::logical_plan::LogicalPlan;

use arrow::datatypes::{DataType, Field};
use std::{panic, sync::Arc};

pub trait LogicalExpression {
    fn to_field(&self, input: Arc<dyn LogicalPlan>) -> Field;
    fn to_string(&self) -> String;
}

pub struct ColumnExpression {
    name: String,
}

impl LogicalExpression for ColumnExpression {
    fn to_field(&self, input: Arc<dyn LogicalPlan>) -> Field {
        let schema = input.schema();
        let matching_fields: Vec<&Field> = schema
            .fields()
            .iter()
            .filter(|f| *f.name() == self.name)
            .collect();

        if matching_fields.len() != 1 {
            panic!("Unknown field");
        }

        (*matching_fields[0]).clone()
    }

    fn to_string(&self) -> String {
        self.name.clone()
    }
}

impl ColumnExpression {
    pub fn new(name: String) -> ColumnExpression {
        ColumnExpression { name }
    }
}

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
pub struct LiteralStringExpression {
    val: String,
}

impl LogicalExpression for LiteralStringExpression {
    fn to_field(&self, _: Arc<dyn LogicalPlan>) -> Field {
        Field::new(self.val.as_str(), DataType::Utf8, false)
    }

    fn to_string(&self) -> String {
        format!("'${}'", self.val)
    }
}

impl LiteralStringExpression {
    pub fn new(val: String) -> LiteralStringExpression {
        LiteralStringExpression { val }
    }
}

pub struct LiteralI64Expression {
    val: i64,
}

impl LogicalExpression for LiteralI64Expression {
    fn to_field(&self, _: Arc<dyn LogicalPlan>) -> Field {
        Field::new(format!("{}", self.val).as_str(), DataType::Int64, false)
    }

    fn to_string(&self) -> String {
        format!("{}", self.val)
    }
}

impl LiteralI64Expression {
    pub fn new(val: i64) -> LiteralI64Expression {
        LiteralI64Expression { val }
    }
}

pub trait BinaryExpression: LogicalExpression {
    fn name(&self) -> String;
    fn op(&self) -> String;
    fn l(&self) -> Arc<dyn LogicalExpression>;
    fn r(&self) -> Arc<dyn LogicalExpression>;
}

pub struct BooleanExpression {
    name: String,
    op: String,
    l: Arc<dyn LogicalExpression>,
    r: Arc<dyn LogicalExpression>,
}

impl LogicalExpression for BooleanExpression {
    fn to_field(&self, _: Arc<dyn LogicalPlan>) -> Field {
        Field::new(format!("{}", self.name).as_str(), DataType::Boolean, false)
    }

    fn to_string(&self) -> String {
        format!(
            "{}{}{}",
            self.l().to_string(),
            self.op(),
            self.r().to_string()
        )
    }
}

impl BinaryExpression for BooleanExpression {
    fn name(&self) -> String {
        self.name.clone()
    }
    fn op(&self) -> String {
        self.op.clone()
    }
    fn l(&self) -> Arc<dyn LogicalExpression> {
        Arc::clone(&self.l)
    }
    fn r(&self) -> Arc<dyn LogicalExpression> {
        Arc::clone(&self.r)
    }
}

impl BooleanExpression {
    pub fn eq(l: Arc<dyn LogicalExpression>, r: Arc<dyn LogicalExpression>) -> BooleanExpression {
        BooleanExpression {
            name: "eq".to_owned(),
            op: "=".to_owned(),
            l,
            r,
        }
    }
    pub fn neq(l: Arc<dyn LogicalExpression>, r: Arc<dyn LogicalExpression>) -> BooleanExpression {
        BooleanExpression {
            name: "neq".to_owned(),
            op: "!=".to_owned(),
            l,
            r,
        }
    }
    pub fn gt(l: Arc<dyn LogicalExpression>, r: Arc<dyn LogicalExpression>) -> BooleanExpression {
        BooleanExpression {
            name: "gt".to_owned(),
            op: ">".to_owned(),
            l,
            r,
        }
    }
    pub fn lt(l: Arc<dyn LogicalExpression>, r: Arc<dyn LogicalExpression>) -> BooleanExpression {
        BooleanExpression {
            name: "lt".to_owned(),
            op: "<".to_owned(),
            l,
            r,
        }
    }
    pub fn gteq(l: Arc<dyn LogicalExpression>, r: Arc<dyn LogicalExpression>) -> BooleanExpression {
        BooleanExpression {
            name: "gteq".to_owned(),
            op: ">=".to_owned(),
            l,
            r,
        }
    }
    pub fn lteq(l: Arc<dyn LogicalExpression>, r: Arc<dyn LogicalExpression>) -> BooleanExpression {
        BooleanExpression {
            name: "lteq".to_owned(),
            op: "<=".to_owned(),
            l,
            r,
        }
    }
    pub fn and(l: Arc<dyn LogicalExpression>, r: Arc<dyn LogicalExpression>) -> BooleanExpression {
        BooleanExpression {
            name: "and".to_owned(),
            op: "AND".to_owned(),
            l,
            r,
        }
    }
    pub fn or(l: Arc<dyn LogicalExpression>, r: Arc<dyn LogicalExpression>) -> BooleanExpression {
        BooleanExpression {
            name: "or".to_owned(),
            op: "OR".to_owned(),
            l,
            r,
        }
    }
}

pub struct MathExpression {
    name: String,
    op: String,
    l: Arc<dyn LogicalExpression>,
    r: Arc<dyn LogicalExpression>,
}

impl LogicalExpression for MathExpression {
    fn to_field(&self, input: Arc<dyn LogicalPlan>) -> Field {
        Field::new(
            format!("{}", self.name).as_str(),
            self.l.to_field(input).data_type().clone(),
            false,
        )
    }

    fn to_string(&self) -> String {
        format!(
            "{} {} {}",
            self.l().to_string(),
            self.op(),
            self.r().to_string()
        )
    }
}

impl BinaryExpression for MathExpression {
    fn name(&self) -> String {
        self.name.clone()
    }
    fn op(&self) -> String {
        self.op.clone()
    }
    fn l(&self) -> Arc<dyn LogicalExpression> {
        Arc::clone(&self.l)
    }
    fn r(&self) -> Arc<dyn LogicalExpression> {
        Arc::clone(&self.r)
    }
}

impl MathExpression {
    fn add(l: Arc<dyn LogicalExpression>, r: Arc<dyn LogicalExpression>) -> BooleanExpression {
        BooleanExpression {
            name: "add".to_owned(),
            op: "+".to_owned(),
            l,
            r,
        }
    }
    fn subtract(l: Arc<dyn LogicalExpression>, r: Arc<dyn LogicalExpression>) -> BooleanExpression {
        BooleanExpression {
            name: "subtract".to_owned(),
            op: "-".to_owned(),
            l,
            r,
        }
    }
    fn multiply(l: Arc<dyn LogicalExpression>, r: Arc<dyn LogicalExpression>) -> BooleanExpression {
        BooleanExpression {
            name: "mult".to_owned(),
            op: "*".to_owned(),
            l,
            r,
        }
    }
    fn divide(l: Arc<dyn LogicalExpression>, r: Arc<dyn LogicalExpression>) -> BooleanExpression {
        BooleanExpression {
            name: "div".to_owned(),
            op: "/".to_owned(),
            l,
            r,
        }
    }
    fn modulus(l: Arc<dyn LogicalExpression>, r: Arc<dyn LogicalExpression>) -> BooleanExpression {
        BooleanExpression {
            name: "modulus".to_owned(),
            op: "%".to_owned(),
            l,
            r,
        }
    }
}

pub struct AggregateExpression {
    name: String,
    expr: Arc<dyn LogicalExpression>,
}

impl LogicalExpression for AggregateExpression {
    fn to_field(&self, input: Arc<dyn LogicalPlan>) -> Field {
        Field::new(
            self.name.clone().as_str(),
            self.expr.to_field(input).data_type().clone(),
            false,
        )
    }

    fn to_string(&self) -> String {
        format!("{}({})", self.name, self.expr.to_string())
    }
}

impl AggregateExpression {
    fn sum(input: Arc<dyn LogicalExpression>) -> AggregateExpression {
        AggregateExpression {
            name: "SUM".to_owned(),
            expr: input,
        }
    }
    fn min(input: Arc<dyn LogicalExpression>) -> AggregateExpression {
        AggregateExpression {
            name: "MIN".to_owned(),
            expr: input,
        }
    }
    fn max(input: Arc<dyn LogicalExpression>) -> AggregateExpression {
        AggregateExpression {
            name: "MAX".to_owned(),
            expr: input,
        }
    }
    fn avg(input: Arc<dyn LogicalExpression>) -> AggregateExpression {
        AggregateExpression {
            name: "AVG".to_owned(),
            expr: input,
        }
    }
}
