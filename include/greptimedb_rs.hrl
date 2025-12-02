-type opts() :: #{
    endpoints := [binary()],
    dbname := binary(),
    username => binary(),
    password => binary(),
    tls => boolean(),
    ca_cert => binary(),
    client_cert => binary(),
    client_key => binary()
}.
-type client() :: pid().
-type client_ref() :: reference().
-type sql() :: binary().
-type result() :: term().
-type reason() :: binary().

-type command() :: connect | execute | insert.
-type args() :: term().
-type callback() :: {function(), list()}.

-define(NIF_MODULE, greptimedb_rs_nif).
-define(REQ(Func, Args), {sync, Func, Args}).
-define(ASYNC_REQ(Func, Args, Callback), {async, Func, Args, Callback}).
