# Qrust
Qrust is an in-memory query engine inspired by https://github.com/apache/arrow-datafusion. It currently supports using a dataframe API for reading from CSV files.

This is a toy project I put together to learn Rust. It's not intended for production use. If you need something like this for anything important, just use Datafusion instead.

# Example
```rust

fn main() {
    // Create a context for running queries.
    let context = ExecutionContext::new();

    // Define the source file and schema.
    let schema = schema(vec![
        ("column1", DataType::Int64, false),
        ("column2", DataType::Int64, false),
        ("column3", DataType::Int64, false),
    ]);
    let path = "test.csv";

    // Construct a dataframe, filter it, and select.
    let df = context
        .csv(schema, path)
        .filter(eq(col("column1"), lit(&123)))
        .select(vec![col("column1"), col("column3")]);

    // Print the logical plan for this operation. This will print:
    //  Projection: column1, column3
    //    Filter: column1=123
    //      Scan: test.csv, projection=None
    println!("{}", df.plan().format());
}
```
