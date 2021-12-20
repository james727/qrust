use arrow::datatypes::DataType;

use crate::core::execution_context::*;
use crate::core::helper::*;

mod core;
mod logical_plan;

fn main() {
    let context = ExecutionContext::new();

    let schema = schema(vec![
        ("column1", DataType::Int64, false),
        ("column2", DataType::Int64, false),
        ("column3", DataType::Int64, false),
    ]);
    let path = "test.csv";

    let df = context
        .csv(schema, path)
        .filter(eq(col("column1"), lit(&123)))
        .select(vec![col("column1"), col("column3")]);

    println!("{}", df.plan().format());
}
