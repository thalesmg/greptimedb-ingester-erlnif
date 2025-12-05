use greptimedb_ingester::api::v1::ColumnDataType;
use greptimedb_ingester::Value;
use rustler::Term;

macro_rules! match_term_to_value {
    ($val:expr, $dtype:expr, [ $( ($variant:ident, $rust_type:ty) ),* ]) => {
        match $dtype {
            $(
                ColumnDataType::$variant => {
                    if let Ok(v) = $val.decode::<$rust_type>() {
                        Ok(Value::$variant(v))
                    } else {
                        Ok(Value::Null)
                    }
                }
            )*
            _ => Ok(Value::Null),
        }
    };
}

pub fn term_to_value(val: &Term, dtype: ColumnDataType) -> rustler::NifResult<Value> {
    match_term_to_value!(val, dtype, [
        (Boolean, bool),
        (Int8, i8),
        (Int16, i16),
        (Int32, i32),
        (Int64, i64),
        (Uint8, u8),
        (Uint16, u16),
        (Uint32, u32),
        (Uint64, u64),
        (Float32, f32),
        (Float64, f64),
        (String, String),
        (Binary, Vec<u8>),
        (Date, i32),
        (Datetime, i64),
        (TimestampSecond, i64),
        (TimestampMillisecond, i64),
        (TimestampMicrosecond, i64),
        (TimestampNanosecond, i64)
    ])
}
