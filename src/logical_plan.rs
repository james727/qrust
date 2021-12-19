use crate::data_source::DataSource;
use crate::expression::{AggregateExpression, LogicalExpression};
use arrow::datatypes::{Field, Schema};
use std::fmt;
use std::sync::Arc;

pub trait LogicalPlan {
    fn schema(&self) -> Arc<Schema>;
    fn children(&self) -> Vec<Arc<dyn LogicalPlan>>;
    fn to_string(&self) -> String;

    fn format_helper(&self, indent: usize) -> String {
        let mut builder = String::from("");
        for _ in 0..indent {
            builder.push_str("  ");
        }
        builder.push_str(&self.to_string());
        builder.push_str("\n");
        for child in self.children() {
            builder.push_str(&child.format_helper(indent + 1));
        }
        builder
    }

    fn format(&self) -> String {
        self.format_helper(0)
    }
}

impl fmt::Display for dyn LogicalPlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.format())
    }
}

pub struct Scan {
    path: String,
    schema: Arc<Schema>,
    datasource: Box<dyn DataSource>,
    projection: Vec<String>,
}

impl LogicalPlan for Scan {
    fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.schema)
    }

    fn children(&self) -> Vec<Arc<dyn LogicalPlan>> {
        vec![]
    }

    fn to_string(&self) -> String {
        if self.projection.len() == 0 {
            format!("Scan: {}, projection=None", self.path)
        } else {
            format!("Scan: {}, projection={:?}", self.path, self.projection)
        }
    }
}

impl Scan {
    pub fn new(
        path: String,
        schema: Arc<Schema>,
        datasource: Box<dyn DataSource>,
        projection: Vec<String>,
    ) -> Scan {
        Scan {
            path,
            schema,
            datasource,
            projection,
        }
    }
}

pub struct Projection {
    input: Arc<dyn LogicalPlan>,
    expr: Vec<Arc<dyn LogicalExpression>>,
}

impl LogicalPlan for Projection {
    fn schema(&self) -> Arc<Schema> {
        Arc::new(Schema::new(
            self.expr
                .iter()
                .map(|e| e.to_field(Arc::clone(&self.input)))
                .collect(),
        ))
    }

    fn children(&self) -> Vec<Arc<dyn LogicalPlan>> {
        vec![Arc::clone(&self.input)]
    }

    fn to_string(&self) -> String {
        format!(
            "Projection: {}",
            self.expr
                .iter()
                .map(|e| e.to_string())
                .collect::<Vec<String>>()
                .join(", ")
        )
    }
}

impl Projection {
    pub fn new(input: Arc<dyn LogicalPlan>, expr: Vec<Arc<dyn LogicalExpression>>) -> Projection {
        Projection { input, expr }
    }
}

pub struct Selection {
    input: Arc<dyn LogicalPlan>,
    expr: Arc<dyn LogicalExpression>,
}

impl LogicalPlan for Selection {
    fn schema(&self) -> Arc<Schema> {
        self.input.schema()
    }

    fn children(&self) -> Vec<Arc<dyn LogicalPlan>> {
        vec![Arc::clone(&self.input)]
    }

    fn to_string(&self) -> String {
        format!("Filter: {}", self.expr.to_string())
    }
}

impl Selection {
    pub fn new(input: Arc<dyn LogicalPlan>, expr: Arc<dyn LogicalExpression>) -> Selection {
        Selection { input, expr }
    }
}

pub struct Aggregate {
    input: Arc<dyn LogicalPlan>,
    groupexpr: Vec<Arc<dyn LogicalExpression>>,
    aggregateexpr: Vec<Arc<AggregateExpression>>,
}

impl LogicalPlan for Aggregate {
    fn schema(&self) -> Arc<Schema> {
        let mut fields: Vec<Field> = self
            .groupexpr
            .iter()
            .map(|g| g.to_field(Arc::clone(&self.input)))
            .collect();
        fields.extend(
            self.aggregateexpr
                .iter()
                .map(|g| g.to_field(Arc::clone(&self.input))),
        );
        Arc::new(Schema::new(fields))
    }

    fn children(&self) -> Vec<Arc<dyn LogicalPlan>> {
        vec![Arc::clone(&self.input)]
    }

    fn to_string(&self) -> String {
        format!(
            "Aggregate: groupExpr={}, aggregateExpr={}",
            self.groupexpr
                .iter()
                .map(|g| g.to_string())
                .collect::<Vec<String>>()
                .join(", "),
            self.aggregateexpr
                .iter()
                .map(|g| g.to_string())
                .collect::<Vec<String>>()
                .join(", ")
        )
    }
}

impl Aggregate {
    pub fn new(
        input: Arc<dyn LogicalPlan>,
        groupexpr: Vec<Arc<dyn LogicalExpression>>,
        aggregateexpr: Vec<Arc<AggregateExpression>>,
    ) -> Aggregate {
        Aggregate {
            input,
            groupexpr,
            aggregateexpr,
        }
    }
}
