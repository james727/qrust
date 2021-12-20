use arrow::datatypes::Schema;
use std::sync::Arc;

use super::data_frame::DataFrame;
use super::data_source::CsvDataSource;
use crate::logical_plan::scan::*;

pub struct ExecutionContext {}

impl ExecutionContext {
    pub fn new() -> ExecutionContext {
        ExecutionContext {}
    }

    pub fn csv(&self, schema: Arc<Schema>, path: &str) -> DataFrame {
        let source = CsvDataSource::new(Arc::clone(&schema), String::from(path));
        DataFrame::new(Arc::new(Scan::new(
            String::from(path),
            Arc::clone(&schema),
            Box::new(source),
            vec![],
        )))
    }
}
