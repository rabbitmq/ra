-module(ra_env).

-export([
         data_dir/0,
         data_dir/1,
         wal_data_dir/0
         ]).

-export_type([
              ]).

data_dir() ->
    case application:get_env(ra, data_dir) of
        {ok, Dir} -> Dir;
        undefined ->
            exit({ra_missing_config, data_dir})
    end.

data_dir(Me0) ->
    Me = ra_lib:to_list(Me0),
    filename:join(data_dir(), Me).

wal_data_dir() ->
    % TODO: review - this assumes noone will ever name a ra_node "_wal".
    Def = filename:join(data_dir(), "_wal"),
    application:get_env(ra, wal_data_dir, Def).
