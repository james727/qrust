use arrow::datatypes::DataType;

/// Supported data types for Qrust.
/// TODO: Implement more.
#[derive(Copy, Clone)]
pub enum ArrowType {
    StringType,
    Int64Type,
}

impl From<ArrowType> for DataType {
    fn from(t: ArrowType) -> DataType {
        match t {
            ArrowType::StringType => DataType::Utf8,
            ArrowType::Int64Type => DataType::Int64,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_string() {
        let t = ArrowType::StringType;
        assert_eq!(DataType::Utf8, DataType::from(t));
    }

    #[test]
    fn test_i64() {
        let t = ArrowType::Int64Type;
        assert_eq!(DataType::Int64, DataType::from(t));
    }
}
