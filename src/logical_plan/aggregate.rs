use arrow::datatypes::Field;

use super::expression::*;
use super::*;

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
