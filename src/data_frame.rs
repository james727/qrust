use std::sync::Arc;

use arrow::datatypes::Schema;

use crate::{
    expression::{AggregateExpression, LogicalExpression},
    logical_plan::{Aggregate, LogicalPlan, Projection, Selection},
};

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

    pub fn filter(&self, expr: Box<dyn LogicalExpression>) -> DataFrame {
        DataFrame {
            plan: Arc::new(Selection::new(Arc::clone(&self.plan), expr)),
        }
    }

    pub fn aggregate(
        &self,
        group_by: Vec<Box<dyn LogicalExpression>>,
        aggregate: Vec<Box<AggregateExpression>>,
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
