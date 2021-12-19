use std::sync::Arc;

use arrow::datatypes::Schema;

use crate::{data_frame::DataFrame, data_source::CsvDataSource, logical_plan::Scan};
pub struct ExecutionContext {}

impl ExecutionContext {
    pub fn new() -> ExecutionContext {
        ExecutionContext {}
    }

    pub fn csv(&self, schema: Arc<Schema>, path: String) -> DataFrame {
        let source = CsvDataSource::new(Arc::clone(&schema), path.clone());
        DataFrame::new(Arc::new(Scan::new(
            path,
            Arc::clone(&schema),
            Box::new(source),
            vec![],
        )))
    }
}
