use std::sync::Arc;

use arrow::datatypes::Schema;

use crate::logical_plan::aggregate::*;
use crate::logical_plan::expression::*;
use crate::logical_plan::projection::*;
use crate::logical_plan::selection::*;
use crate::logical_plan::*;

pub struct DataFrame {
    plan: Arc<dyn LogicalPlan>,
}

impl DataFrame {
    pub fn new(plan: Arc<dyn LogicalPlan>) -> DataFrame {
        DataFrame { plan }
    }

    pub fn select(&self, expr: Vec<Arc<dyn LogicalExpression>>) -> DataFrame {
        DataFrame {
            plan: Arc::new(Projection::new(Arc::clone(&self.plan), expr)),
        }
    }

    pub fn filter(&self, expr: Arc<dyn LogicalExpression>) -> DataFrame {
        DataFrame {
            plan: Arc::new(Selection::new(Arc::clone(&self.plan), expr)),
        }
    }

    pub fn aggregate(
        &self,
        group_by: Vec<Arc<dyn LogicalExpression>>,
        aggregate: Vec<Arc<AggregateExpression>>,
    ) -> DataFrame {
        DataFrame {
            plan: Arc::new(Aggregate::new(Arc::clone(&self.plan), group_by, aggregate)),
        }
    }

    pub fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.plan.schema())
    }

    pub fn plan(&self) -> Arc<dyn LogicalPlan> {
        Arc::clone(&self.plan)
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;

    use crate::builder::*;
    use crate::context::ExecutionContext;

    use super::*;

    fn generate_df() -> Arc<DataFrame> {
        let context = ExecutionContext::new();
        let schema = schema(vec![
            ("column1", DataType::Int64, false),
            ("column2", DataType::Int64, false),
            ("column3", DataType::Int64, false),
        ]);
        let path = "test.csv";
        Arc::new(context.csv(Arc::clone(&schema), path))
    }

    fn check_plan(df: DataFrame, plan: &str) {
        fn normalize_plan(p: &str) -> Vec<&str> {
            p.trim()
                .split("\n")
                .map(|s| s.trim())
                .collect::<Vec<&str>>()
        }
        assert_eq!(
            normalize_plan(df.plan().format().as_str()),
            normalize_plan(plan)
        );
    }

    #[test]
    fn data_frame_init() {
        let df = generate_df();

        assert_eq!(
            df.schema(),
            schema(vec![
                ("column1", DataType::Int64, false),
                ("column2", DataType::Int64, false),
                ("column3", DataType::Int64, false),
            ])
        );

        assert_eq!(df.plan().format().trim(), "Scan: test.csv, projection=None");
    }

    #[test]
    fn data_frame_select() {
        let df = generate_df().select(vec![col("column1"), col("column3")]);

        assert_eq!(
            df.schema(),
            schema(vec![
                ("column1", DataType::Int64, false),
                ("column3", DataType::Int64, false),
            ])
        );

        check_plan(
            df,
            "Projection: column1, column3
                    Scan: test.csv, projection=None",
        );
    }

    #[test]
    fn data_frame_filter() {
        let df = generate_df().filter(eq(col("column1"), lit(&"abc")));

        assert_eq!(
            df.schema(),
            schema(vec![
                ("column1", DataType::Int64, false),
                ("column2", DataType::Int64, false),
                ("column3", DataType::Int64, false),
            ])
        );

        check_plan(
            df,
            "Filter: column1='abc'
                    Scan: test.csv, projection=None",
        );
    }

    #[test]
    fn data_frame_aggregate() {
        let df = generate_df().aggregate(vec![col("column1")], vec![sum(col("column3"))]);

        assert_eq!(
            df.schema(),
            schema(vec![
                ("column1", DataType::Int64, false),
                ("sum", DataType::Int64, false),
            ])
        );

        check_plan(
            df,
            "Aggregate: groupExpr=column1, aggregateExpr=sum(column3)
                    Scan: test.csv, projection=None",
        );
    }
}
