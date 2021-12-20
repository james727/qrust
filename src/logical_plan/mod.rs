use arrow::datatypes::Schema;
use std::{fmt, sync::Arc};

pub mod aggregate;
pub mod expression;
pub mod projection;
pub mod scan;
pub mod selection;

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
