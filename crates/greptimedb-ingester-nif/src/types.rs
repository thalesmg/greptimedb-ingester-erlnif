use greptimedb_ingester::api::v1::value::ValueData;
use greptimedb_ingester::api::v1::{ColumnDataType, Value as ProtoValue};
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

macro_rules! generate_term_to_proto_value_match {
    (
        $val:expr,
        $dtype:expr,
        $( ($column_data_type:ident, $rust_type:ty, $proto_value_data_variant:ident $(, $cast_type:ty)? ) ),*
    ) => {
        match $dtype {
            $(
                ColumnDataType::$column_data_type => {
                    if let Ok(v) = $val.decode::<$rust_type>() {
                        Some(ValueData::$proto_value_data_variant(
                            v $(as $cast_type)?
                        ))
                    } else {
                        None
                    }
                }
            )*
            _ => None,
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

pub fn term_to_proto_value(val: &Term, dtype: ColumnDataType) -> rustler::NifResult<ProtoValue> {
    let value_data = generate_term_to_proto_value_match!(
        val,
        dtype,
        (Boolean, bool, BoolValue),
        (Int8, i8, I8Value, i32),
        (Int16, i16, I16Value, i32),
        (Int32, i32, I32Value),
        (Int64, i64, I64Value),
        (Uint8, u8, U8Value, u32),
        (Uint16, u16, U16Value, u32),
        (Uint32, u32, U32Value),
        (Uint64, u64, U64Value),
        (Float32, f32, F32Value),
        (Float64, f64, F64Value),
        (String, String, StringValue),
        (Binary, Vec<u8>, BinaryValue),
        (Date, i32, DateValue),
        (Datetime, i64, DatetimeValue),
        (TimestampSecond, i64, TimestampSecondValue),
        (TimestampMillisecond, i64, TimestampMillisecondValue),
        (TimestampMicrosecond, i64, TimestampMicrosecondValue),
        (TimestampNanosecond, i64, TimestampNanosecondValue)
    );
    Ok(ProtoValue { value_data })
}
