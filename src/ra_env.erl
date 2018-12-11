-module(ra_env).

-export([
         data_dir/0,
         server_data_dir/1,
         wal_data_dir/0
         ]).

-export_type([
              ]).

data_dir() ->
    DataDir = case application:get_env(ra, data_dir) of
        {ok, Dir} ->
            Dir;
        undefined ->
            {ok, Cwd} = file:get_cwd(),
            Cwd
    end,
    Node = ra_lib:to_list(node()),
    filename:join(DataDir, Node).

server_data_dir(UId) ->
    Me = ra_lib:to_list(UId),
    filename:join(data_dir(), Me).

wal_data_dir() ->
    %% allows the wal director to be overridden or fall back to the default
    %% data directory
    case application:get_env(ra, wal_data_dir) of
        {ok, Dir} -> Dir;
        _ ->
            data_dir()
    end.
