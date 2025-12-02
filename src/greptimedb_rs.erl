-module(greptimedb_rs).

-include("greptimedb_rs.hrl").

%% Connection APIs
-export([
    start_client/1,
    stop_client/1
]).

%% Query APIs
-export([
    query/2,
    query_async/3,

    insert_bulk/3,
    insert_bulk_async/4
]).

-spec start_client(opts()) -> {ok, client()} | {error, reason()}.
start_client(Opts) ->
    {ok, Pid} = greptimedb_rs_sock:start(),
    case greptimedb_rs_sock:sync_command(Pid, connect, [Opts]) of
        {ok, _Ref} ->
            {ok, Pid};
        {error, _} = Err ->
            stop_client(Pid),
            Err
    end.

-spec stop_client(client()) -> ok.
stop_client(Client) ->
    greptimedb_rs_sock:stop(Client).

-spec query(client(), sql()) -> {ok, result()} | {error, reason()}.
query(Client, Sql) ->
    greptimedb_rs_sock:sync_command(Client, execute, [Sql]).

-spec query_async(client(), sql(), callback()) -> ok.
query_async(Client, Sql, ResultCallback) ->
    async(Client, execute, [Sql], ResultCallback).

-spec insert_bulk(client(), binary(), [map()]) -> {ok, integer()} | {error, reason()}.
insert_bulk(Client, Table, Rows) ->
    greptimedb_rs_sock:sync_command(Client, insert, [Table, Rows]).

-spec insert_bulk_async(client(), binary(), [map()], callback()) -> ok.
insert_bulk_async(Client, Table, Rows, ResultCallback) ->
    async(Client, insert, [Table, Rows], ResultCallback).

async(Client, Cmd, Args, ResultCallback) ->
    _ = erlang:send(Client, ?ASYNC_REQ(Cmd, Args, ResultCallback)),
    ok.
