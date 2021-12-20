use super::expression::*;
use super::*;

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
