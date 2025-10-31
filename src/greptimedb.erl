-module(greptimedb).

-include("greptimedb.hrl").

-export([
    start_client/1,
    stop_client/1,
    write/3,
    query/2,
    async_write/4,
    async_query/3
]).

-spec start_client(opts()) -> {ok, client()} | {error, reason()}.
start_client(Opts) ->
    {ok, Pid} = greptimedb_sock:start(),
    case greptimedb_sock:sync_command(Pid, connect, [Opts]) of
        {ok, _Ref} ->
            {ok, Pid};
        {error, _} = Err ->
            stop_client(Pid),
            Err
    end.

-spec stop_client(client()) -> ok.
stop_client(Client) ->
    greptimedb_sock:stop(Client).

-spec write(client(), binary(), [map()]) -> {ok, integer()} | {error, reason()}.
write(Client, Table, Rows) ->
    greptimedb_sock:sync_command(Client, insert, [Table, Rows]).

-spec query(client(), sql()) -> {ok, result()} | {error, reason()}.
query(Client, Sql) ->
    greptimedb_sock:sync_command(Client, execute, [Sql]).

-spec async_write(client(), binary(), [map()], callback()) -> ok.
async_write(Client, Table, Rows, ResultCallback) ->
    async(Client, insert, [Table, Rows], ResultCallback).

-spec async_query(client(), sql(), callback()) -> ok.
async_query(Client, Sql, ResultCallback) ->
    async(Client, execute, [Sql], ResultCallback).

async(Client, Cmd, Args, ResultCallback) ->
    _ = erlang:send(Client, ?ASYNC_REQ(Cmd, Args, ResultCallback)),
    ok.
