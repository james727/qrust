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

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;

    use super::*;
    use crate::core::{execution_context::ExecutionContext, helper::*};

    #[test]
    fn test_aggregate() {
        let ctx = ExecutionContext::new();
        let input = ctx.csv(
            schema(vec![
                ("abc", DataType::Utf8, false),
                ("values", DataType::Int64, false),
            ]),
            "path.csv",
        );
        let group: Vec<Arc<dyn LogicalExpression>> = vec![col("abc")];
        let agg = vec![sum(col("values"))];
        let expr = Aggregate::new(input.plan(), group, agg);

        assert_eq!(
            expr.to_string().as_str(),
            "Aggregate: groupExpr=abc, aggregateExpr=sum(values)"
        );

        assert_eq!(
            expr.schema(),
            schema(vec![
                ("abc", DataType::Utf8, false),
                ("sum", DataType::Int64, false),
            ])
        )
    }
}
