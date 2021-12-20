use std::error::Error;
use std::fs::File;
use std::sync::Arc;

use arrow::csv;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;

pub trait DataSource {
    fn schema(&self) -> Arc<Schema>;
    fn scan(&self, projection: Vec<String>) -> Result<Vec<RecordBatch>, Box<dyn Error>>;
}

pub struct CsvDataSource {
    schema: Arc<Schema>,
    path: String,
}

impl DataSource for CsvDataSource {
    fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.schema)
    }

    // :TODO: Return an iterator instead of parsing the entire file into memory.
    fn scan(&self, projection: Vec<String>) -> Result<Vec<RecordBatch>, Box<dyn Error>> {
        // Convert the projection string input into a Vec<usize>, where each element
        // corresponds to the index of the relevant column in the schema.
        let proj: Vec<usize> = self
            .schema
            .fields()
            .iter()
            .enumerate()
            .filter(|(_, f)| projection.contains(f.name()))
            .map(|(i, _)| i)
            .collect();

        // Build the CSV reader and iterate over the resulting record batches.
        let file = File::open(self.path.clone())?;
        let header = false;
        let csv = csv::Reader::new(
            file,
            Arc::clone(&self.schema),
            header,
            None,
            1024,
            None,
            Some(proj),
        );

        let mut out: Vec<RecordBatch> = vec![];
        for result in csv {
            out.push(result?);
        }

        Ok(out)
    }
}

impl CsvDataSource {
    pub fn new(schema: Arc<Schema>, path: String) -> CsvDataSource {
        CsvDataSource { schema, path }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::{
        array::Int64Array,
        datatypes::{DataType, Field, Schema},
    };
    use std::io::Write;

    #[test]
    fn csv_source() {
        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("input.csv");
        let mut file = File::create(file_path).unwrap();

        writeln!(file, "123,456,654").unwrap();
        writeln!(file, "789,101,987").unwrap();

        let schema = Schema::new(vec![
            Field::new("column1", DataType::Int64, false),
            Field::new("column2", DataType::Int64, false),
            Field::new("column3", DataType::Int64, false),
        ]);

        let source = CsvDataSource {
            schema: Arc::new(schema),
            path: dir
                .path()
                .join("input.csv")
                .into_os_string()
                .into_string()
                .unwrap(),
        };

        let batches = source
            .scan(vec!["column1".to_string(), "column3".to_string()])
            .unwrap();
        assert_eq!(batches.len(), 1);

        let batch = &batches[0];
        assert_eq!(batch.num_columns(), 2);
        assert_eq!(batch.num_rows(), 2);

        // Check values
        let c1 = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(c1.values(), &[123, 789]);

        // Since we selected c3 in the projection, the second column of the result set
        // should contain the contents of column3 in the input.
        let c2 = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(c2.values(), &[654, 987]);
    }
}
