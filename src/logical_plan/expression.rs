use super::*;

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

pub struct LiteralStringExpression {
    val: String,
}

impl LogicalExpression for LiteralStringExpression {
    fn to_field(&self, _: Arc<dyn LogicalPlan>) -> Field {
        Field::new(self.val.as_str(), DataType::Utf8, false)
    }

    fn to_string(&self) -> String {
        format!("'{}'", self.val)
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
    pub fn add(l: Arc<dyn LogicalExpression>, r: Arc<dyn LogicalExpression>) -> MathExpression {
        MathExpression {
            name: "add".to_owned(),
            op: "+".to_owned(),
            l,
            r,
        }
    }
    pub fn subtract(
        l: Arc<dyn LogicalExpression>,
        r: Arc<dyn LogicalExpression>,
    ) -> MathExpression {
        MathExpression {
            name: "subtract".to_owned(),
            op: "-".to_owned(),
            l,
            r,
        }
    }
    pub fn multiply(
        l: Arc<dyn LogicalExpression>,
        r: Arc<dyn LogicalExpression>,
    ) -> MathExpression {
        MathExpression {
            name: "mult".to_owned(),
            op: "*".to_owned(),
            l,
            r,
        }
    }
    pub fn divide(l: Arc<dyn LogicalExpression>, r: Arc<dyn LogicalExpression>) -> MathExpression {
        MathExpression {
            name: "div".to_owned(),
            op: "/".to_owned(),
            l,
            r,
        }
    }
    pub fn modulus(l: Arc<dyn LogicalExpression>, r: Arc<dyn LogicalExpression>) -> MathExpression {
        MathExpression {
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
    pub fn sum(input: Arc<dyn LogicalExpression>) -> AggregateExpression {
        AggregateExpression {
            name: "sum".to_owned(),
            expr: input,
        }
    }
    pub fn min(input: Arc<dyn LogicalExpression>) -> AggregateExpression {
        AggregateExpression {
            name: "min".to_owned(),
            expr: input,
        }
    }
    pub fn max(input: Arc<dyn LogicalExpression>) -> AggregateExpression {
        AggregateExpression {
            name: "max".to_owned(),
            expr: input,
        }
    }
    pub fn avg(input: Arc<dyn LogicalExpression>) -> AggregateExpression {
        AggregateExpression {
            name: "avg".to_owned(),
            expr: input,
        }
    }
}
