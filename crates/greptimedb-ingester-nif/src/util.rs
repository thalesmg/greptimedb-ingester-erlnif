use crate::atoms;
use crate::types;
use greptimedb_ingester::api::v1::{ColumnDataType, SemanticType};
use greptimedb_ingester::{Row, Rows, TableSchema, Value};
use rustler::{Encoder, Term};

pub fn terms_to_rows<'a>(
    table_schema: &TableSchema,
    rows_term: Vec<Term<'a>>,
) -> rustler::NifResult<Rows> {
    let column_schemas = table_schema.columns();
    let mut greptime_rows = Rows::new(column_schemas, rows_term.len(), 1024)
        .map_err(|e| rustler::Error::Term(Box::new(e.to_string())))?;

    if rows_term.is_empty() {
        return Ok(greptime_rows);
    }

    let env = rows_term[0].get_env();

    // Pre-compute keys and metadata for columns to avoid repetitive encoding/decoding
    let col_meta: Vec<(SemanticType, Term<'a>, ColumnDataType)> = column_schemas
        .iter()
        .map(|c| (c.semantic_type, c.name.encode(env), c.data_type))
        .collect();

    // Pre-compute static atoms and binary keys
    let atom_fields = atoms::fields().to_term(env);
    let bin_fields = "fields".encode(env);
    let atom_tags = atoms::tags().to_term(env);
    let bin_tags = "tags".encode(env);
    let atom_timestamp = atoms::timestamp().to_term(env);
    let bin_timestamp = "timestamp".encode(env);
    let atom_ts = atoms::ts().to_term(env);
    let bin_ts = "ts".encode(env);

    for row_term in rows_term {
        // Retrieve sub-maps directly from the row term (atom or binary key)
        let fields_term = row_term
            .map_get(atom_fields)
            .ok()
            .or_else(|| row_term.map_get(bin_fields).ok());
        let tags_term = row_term
            .map_get(atom_tags)
            .ok()
            .or_else(|| row_term.map_get(bin_tags).ok());

        // Timestamp can be under "timestamp" or "ts" (atom or binary)
        let ts_term = row_term
            .map_get(atom_timestamp)
            .ok()
            .or_else(|| row_term.map_get(bin_timestamp).ok())
            .or_else(|| row_term.map_get(atom_ts).ok())
            .or_else(|| row_term.map_get(bin_ts).ok());

        let mut values = Vec::with_capacity(col_meta.len());

        for (semantic, key_term, dtype) in &col_meta {
            let val_term = match semantic {
                SemanticType::Field => fields_term.and_then(|map| map.map_get(*key_term).ok()),
                SemanticType::Tag => tags_term.and_then(|map| map.map_get(*key_term).ok()),
                SemanticType::Timestamp => ts_term,
            };

            let val = if let Some(t) = val_term {
                types::term_to_value(&t, *dtype)?
            } else {
                Value::Null
            };
            values.push(val);
        }

        greptime_rows
            .add_row(Row::from_values(values))
            .map_err(|e| rustler::Error::Term(Box::new(e.to_string())))?;
    }
    Ok(greptime_rows)
}
