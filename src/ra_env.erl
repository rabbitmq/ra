-module(ra_env).

-export([
         data_dir/0,
         server_data_dir/1,
         wal_data_dir/0
         ]).

-export_type([
              ]).

data_dir() ->
    case application:get_env(ra, data_dir) of
        {ok, Dir} ->
            Node = ra_lib:to_list(node()),
            filename:join(Dir, Node);
        undefined ->
            exit({ra_missing_config, data_dir})
    end.

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
