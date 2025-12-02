use greptime_proto::v1::auth_header::AuthScheme;
use greptime_proto::v1::Basic;
use greptimedb_ingester::api::v1::{ColumnDataType, SemanticType};
use greptimedb_ingester::client::Client;
use greptimedb_ingester::database::Database;
use greptimedb_ingester::{
    BulkInserter, BulkWriteOptions, CompressionType, Row, Rows, TableSchema, Value,
};
use lazy_static::lazy_static;
use rustler::{Encoder, Env, NifResult, ResourceArc, Term, TermType};
use std::collections::HashMap;
use std::time::Duration;
use tokio::runtime::Runtime;

mod atoms;

lazy_static! {
    static ref RUNTIME: Runtime = Runtime::new().unwrap();
}

pub struct GreptimeResource {
    pub db: Database,
    pub client: Client,
    pub auth: Option<AuthScheme>,
}

#[allow(non_local_definitions)]
fn load(env: Env, _info: Term) -> bool {
    _ = rustler::resource!(GreptimeResource, env);
    true
}

use greptimedb_ingester::channel_manager::ClientTlsOption;

#[rustler::nif(schedule = "DirtyIo")]
fn connect(opts: Term) -> NifResult<Term> {
    let env = opts.get_env();

    let endpoints_term: Term = opts.map_get(atoms::endpoints().to_term(env))?;
    let dbname_term: Term = opts.map_get(atoms::dbname().to_term(env))?;

    let endpoints: Vec<String> = endpoints_term.decode()?;
    let dbname: String = dbname_term.decode()?;

    let client = if let Ok(tls_term) = opts.map_get(atoms::tls().to_term(env)) {
        if tls_term.decode::<bool>()? {
            let server_ca_cert_path = opts
                .map_get(atoms::ca_cert().to_term(env))
                .and_then(|t| t.decode())
                .unwrap_or_default();
            let client_cert_path = opts
                .map_get(atoms::client_cert().to_term(env))
                .and_then(|t| t.decode())
                .unwrap_or_default();
            let client_key_path = opts
                .map_get(atoms::client_key().to_term(env))
                .and_then(|t| t.decode())
                .unwrap_or_default();

            let tls_option = ClientTlsOption {
                server_ca_cert_path,
                client_cert_path,
                client_key_path,
            };

            match Client::with_tls_and_urls(endpoints, tls_option) {
                Ok(c) => c,
                Err(e) => return Ok((atoms::error(), e.to_string()).encode(env)),
            }
        } else {
            Client::with_urls(endpoints)
        }
    } else {
        Client::with_urls(endpoints)
    };

    let mut db = Database::new_with_dbname(dbname, client.clone());
    let mut auth = None;

    if let Ok(username_term) = opts.map_get(atoms::username().to_term(env)) {
        if let Ok(password_term) = opts.map_get(atoms::password().to_term(env)) {
            let username: String = username_term.decode()?;
            let password: String = password_term.decode()?;
            let auth_scheme = AuthScheme::Basic(Basic { username, password });
            db.set_auth(auth_scheme.clone());
            auth = Some(auth_scheme);
        }
    }

    let resource = ResourceArc::new(GreptimeResource { db, client, auth });
    Ok((atoms::ok(), resource).encode(env))
}

#[rustler::nif(schedule = "DirtyIo")]
fn execute(env: Env, resource: ResourceArc<GreptimeResource>, sql: String) -> NifResult<Term> {
    let db = &resource.db;

    let result = RUNTIME.block_on(async {
        use futures::StreamExt;
        match db.query(&sql).await {
            Ok(mut stream) => {
                let mut rows = Vec::new();
                while let Some(batch_res) = stream.next().await {
                    match batch_res {
                        Ok(batch) => {
                            rows.push(format!("{batch:?}"));
                        }
                        Err(e) => return Err(e),
                    }
                }
                Ok(rows)
            }
            Err(e) => Err(e),
        }
    });

    match result {
        Ok(rows) => Ok((atoms::ok(), rows).encode(env)),
        Err(e) => Ok((atoms::error(), e.to_string()).encode(env)),
    }
}

#[rustler::nif(schedule = "DirtyIo")]
fn insert<'a>(
    env: Env<'a>,
    resource: ResourceArc<GreptimeResource>,
    table: String,
    rows_term: Vec<Term<'a>>,
) -> NifResult<Term<'a>> {
    if rows_term.is_empty() {
        return Ok((atoms::ok(), 0).encode(env));
    }

    // 1. Infer Schema from the first row
    let first_row = rows_term[0];
    let map: HashMap<String, Term> = first_row.decode()?;

    let mut column_names = Vec::new();
    let mut table_template = TableSchema::builder()
        .name(&table)
        .build()
        .map_err(|e| rustler::Error::Term(Box::new(e.to_string())))?;

    // We need a stable order for columns
    let mut sorted_keys: Vec<String> = map.keys().cloned().collect();
    sorted_keys.sort();

    for key in &sorted_keys {
        let val = map.get(key).unwrap();
        let (dtype, semantic_type) = match val.get_type() {
            TermType::Integer => (ColumnDataType::Int64, SemanticType::Field),
            TermType::Float => (ColumnDataType::Float64, SemanticType::Field),
            TermType::Binary => (ColumnDataType::String, SemanticType::Tag), // Default string to Tag for now, naive inference
            TermType::Atom => {
                if let Ok(_b) = val.decode::<bool>() {
                    (ColumnDataType::Boolean, SemanticType::Field)
                } else {
                    (ColumnDataType::String, SemanticType::Tag)
                }
            }
            _ => (ColumnDataType::String, SemanticType::Field), // Fallback
        };

        // Special case: 'ts' or 'timestamp' is Timestamp
        let (final_dtype, final_semantic) = if key == "ts" || key == "timestamp" {
            (
                ColumnDataType::TimestampMillisecond,
                SemanticType::Timestamp,
            )
        } else {
            (dtype, semantic_type)
        };

        column_names.push(key.clone());

        match final_semantic {
            SemanticType::Timestamp => {
                table_template = table_template.add_timestamp(key, final_dtype);
            }
            SemanticType::Tag => {
                table_template = table_template.add_tag(key, final_dtype);
            }
            SemanticType::Field => {
                table_template = table_template.add_field(key, final_dtype);
            }
        }
    }

    // 2. Construct Rows
    let column_schemas = table_template.columns();
    let mut greptime_rows = Rows::new(column_schemas, rows_term.len(), 1024)
        .map_err(|e| rustler::Error::Term(Box::new(e.to_string())))?;

    for row_term in rows_term {
        let row_map: HashMap<String, Term> = row_term.decode()?;
        let mut values = Vec::new();

        for col_name in &column_names {
            if let Some(val_term) = row_map.get(col_name) {
                let val = if let Ok(i) = val_term.decode::<i64>() {
                    // Check if it's the timestamp column
                    if col_name == "ts" || col_name == "timestamp" {
                        Value::TimestampMillisecond(i)
                    } else {
                        Value::Int64(i)
                    }
                } else if let Ok(f) = val_term.decode::<f64>() {
                    Value::Float64(f)
                } else if let Ok(s) = val_term.decode::<String>() {
                    Value::String(s)
                } else if let Ok(b) = val_term.decode::<bool>() {
                    Value::Boolean(b)
                } else {
                    Value::Null
                };

                values.push(val);
            } else {
                // Missing value, push null
                values.push(Value::Null);
            }
        }
        greptime_rows
            .add_row(Row::from_values(values))
            .map_err(|e| rustler::Error::Term(Box::new(e.to_string())))?;
    }

    let result: Result<u32, String> = RUNTIME.block_on(async {
        let mut bulk_inserter = BulkInserter::new(resource.client.clone(), resource.db.dbname());
        if let Some(auth) = &resource.auth {
            bulk_inserter.set_auth(auth.clone());
        }

        let mut writer = bulk_inserter
            .create_bulk_stream_writer(
                &table_template,
                Some(
                    BulkWriteOptions::default()
                        .with_compression(CompressionType::Zstd)
                        .with_timeout(Duration::from_secs(30)),
                ),
            )
            .await
            .map_err(|e| e.to_string())?;

        let response = writer
            .write_rows(greptime_rows)
            .await
            .map_err(|e| e.to_string())?;
        writer.finish().await.map_err(|e| e.to_string())?;

        Ok(response.affected_rows() as u32)
    });

    match result {
        Ok(affected) => Ok((atoms::ok(), affected).encode(env)),
        Err(e) => Ok((atoms::error(), e).encode(env)),
    }
}

rustler::init!("greptimedb_rs_nif", load = load);