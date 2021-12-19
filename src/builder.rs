pub mod builder {
    use arrow::datatypes::{DataType, Field, Schema};

    use crate::expression::{BooleanExpression, ColumnExpression, IntoLit, LogicalExpression};
    use std::sync::Arc;

    pub fn schema(fields: Vec<(&str, DataType, bool)>) -> Arc<Schema> {
        Arc::new(Schema::new(
            fields
                .iter()
                .map(move |(n, t, z)| Field::new(n, t.clone(), z.clone()))
                .collect(),
        ))
    }

    pub fn col(name: &str) -> Arc<ColumnExpression> {
        Arc::new(ColumnExpression::new(String::from(name)))
    }

    pub fn lit(val: &impl IntoLit) -> Arc<dyn LogicalExpression> {
        val.into_lit()
    }

    pub fn eq(
        l: Arc<dyn LogicalExpression>,
        r: Arc<dyn LogicalExpression>,
    ) -> Box<BooleanExpression> {
        Box::new(BooleanExpression::eq(l, r))
    }
}
