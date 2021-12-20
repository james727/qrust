use std::sync::Arc;

use super::expression::*;
use super::*;

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
