# GreptimeDB Ingester Erlang Library (NIF)

High-performance Erlang client for [GreptimeDB](https://greptime.com/), built on top of the official Rust Ingester SDK using NIFs (Native Implemented Functions). This library is designed to offer superior performance by leveraging the Rust SDK's efficiency while providing a friendly Erlang API.

## Features

- **High Performance**: Direct binding to the Rust Ingester SDK.
- **Async & Sync API**: Flexible API supporting both blocking calls and non-blocking async callbacks.
- **Stream Ingestion**: efficient streaming support for high-throughput data ingestion.
- **Connection Pooling**: Robust connection management handled by the underlying Rust implementation.
- **SQL Execution**: Execute DDL and SQL queries directly.
- **Full Type Support**: Supports all GreptimeDB data types including Int8-64, Float32/64, Boolean, String, Binary, Date, Datetime, and Timestamps with various precisions.

## Installation

Add `greptimedb_rs` to your `rebar.config` dependencies:

```erlang
{deps, [
    {greptimedb_rs, {git, "https://github.com/emqx/greptimedb-ingester-erlnif", {branch, "main"}}}
]}.
```

## Basic Usage

### 1. Start the Application

Ensure the application is started before using it:

```erlang
application:ensure_all_started(greptimedb_rs).
```

### 2. Connect to GreptimeDB

Initialize the client with your GreptimeDB endpoint configuration.

**Basic Connection:**

```erlang
Opts = #{
    endpoints => [<<"127.0.0.1:4001">>], % List of gRPC endpoints
    dbname => <<"public">>,
    pool_size => 8,       % Optional: Number of backend connections
    pool_type => random   % Optional: 'random' or 'round_robin'
},
{ok, Client} = greptimedb_rs:start_client(Opts).
```

**Connection with Authentication:**

```erlang
Opts = #{
    endpoints => [<<"127.0.0.1:4001">>],
    dbname => <<"public">>,
    username => <<"greptime_user">>,
    password => <<"greptime_pwd">>
},
{ok, Client} = greptimedb_rs:start_client(Opts).
```

**Connection with TLS:**

```erlang
Opts = #{
    endpoints => [<<"127.0.0.1:4001">>],
    dbname => <<"public">>,
    tls => true,
    % Optional: Paths to certificates
    ca_cert => <<"/path/to/ca.crt">>,
    client_cert => <<"/path/to/client.crt">>,
    client_key => <<"/path/to/client.key">>
},
{ok, Client} = greptimedb_rs:start_client(Opts).
```

### 3. Prepare Data

Rows are represented as maps containing `timestamp`, `tags` (optional), and `fields`. Keys can be atoms or binaries.

```erlang
Row = #{
    timestamp => os:system_time(millisecond),
    tags => #{
        <<"host">> => <<"server-01">>,
        <<"region">> => <<"us-west">>
    },
    fields => #{
        <<"cpu_usage">> => 85.5,
        <<"memory_usage">> => 1024,
        <<"is_active">> => true
    }
}.
```

### 4. Insert Data

#### Synchronous Insert
Blocks the calling process until the insert is confirmed.

```erlang
Table = <<"system_metrics">>,
Rows = [Row], % Insert a list of rows
{ok, AffectedRows} = greptimedb_rs:insert(Client, Table, Rows).
```

#### Asynchronous Insert
Returns immediately. The provided callback is executed upon completion.

```erlang
% Define a callback function
% Callback signature: fun(CallbackArgs, Result)
Callback = {fun(Ref, Result) ->
    io:format("Insert finished for ~p: ~p~n", [Ref, Result])
end, [make_ref()]},

ok = greptimedb_rs:insert_async(Client, Table, Rows, Callback).
```

## Streaming Usage

Streaming is recommended for high-volume data ingestion. It establishes a persistent stream to the server.

### 1. Start a Stream
Initialize a stream. The schema is automatically inferred from the `FirstRow` provided (or fetched from the server if the table exists).

```erlang
{ok, Stream} = greptimedb_rs:stream_start(Client, Table, Row).
```

### 2. Write to Stream

```erlang
% Synchronous Write
ok = greptimedb_rs:stream_write(Stream, Rows).

% Asynchronous Write
ok = greptimedb_rs:stream_write_async(Stream, Rows, Callback).
```

### 3. Close Stream
Always close the stream to flush any buffered data and release resources.

```erlang
greptimedb_rs:stream_close(Stream).
```

## Executing SQL

You can execute SQL statements (like `CREATE TABLE`, `DROP TABLE`, or `SELECT`) using the `query/2` function.

**DDL (Data Definition Language):**

```erlang
Sql = <<"CREATE TABLE IF NOT EXISTS system_metrics (
    ts TIMESTAMP TIME INDEX,
    host STRING,
    cpu_usage DOUBLE,
    PRIMARY KEY(host)
) ENGINE=mito">>,

{ok, _} = greptimedb_rs:query(Client, Sql).
```

**DQL (Data Query Language):**

```erlang
Query = <<"SELECT * FROM system_metrics LIMIT 10">>,
{ok, Result} = greptimedb_rs:query(Client, Query).
```

## Supported Data Types

The library supports automatic mapping from Erlang terms to GreptimeDB types based on the table schema.

| GreptimeDB Type  | Erlang Type                                 | Example                   |
|:-----------------|:--------------------------------------------|:--------------------------|
| `String`         | Binary / String                             | `<<"hello">>` / `"hello"` |
| `Boolean`        | Boolean                                     | `true`, `false`           |
| `Int8/16/32/64`  | Integer                                     | `123`, `-456`             |
| `UInt8/16/32/64` | Integer                                     | `123`                     |
| `Float32/64`     | Float                                       | `123.45`                  |
| `Binary`         | Binary                                      | `<<1, 2, 3>>`             |
| `Date`           | Integer (Days since epoch)                  | `19700`                   |
| `Datetime`       | Integer (Milliseconds since epoch)          | `1678888888000`           |
| `Timestamp`      | Integer (Units depend on column definition) | `1678888888000`           |

## Cleanup

Stop the client to close all connections.

```erlang
greptimedb_rs:stop_client(Client).
```
