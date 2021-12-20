use super::*;
use crate::core::data_source::DataSource;

pub struct Scan {
    path: String,
    schema: Arc<Schema>,
    datasource: Box<dyn DataSource>,
    projection: Vec<String>,
}

impl LogicalPlan for Scan {
    fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.schema)
    }

    fn children(&self) -> Vec<Arc<dyn LogicalPlan>> {
        vec![]
    }

    fn to_string(&self) -> String {
        if self.projection.len() == 0 {
            format!("Scan: {}, projection=None", self.path)
        } else {
            format!("Scan: {}, projection={:?}", self.path, self.projection)
        }
    }
}

impl Scan {
    pub fn new(
        path: String,
        schema: Arc<Schema>,
        datasource: Box<dyn DataSource>,
        projection: Vec<String>,
    ) -> Scan {
        Scan {
            path,
            schema,
            datasource,
            projection,
        }
    }
}
