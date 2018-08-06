-module(ra_log_snapshot).

-export([
         write/2,
         read/1
         ]).

-include("ra.hrl").

-define(MAGIC, "RASN").

-type file_err() :: file:posix() | badarg | terminated | system_limit.
-type state() :: {ra_index(), ra_term(), ra_cluster_nodes(), term()}.

-export_type([state/0]).

-spec write(file:filename(), state()) ->
    ok | {error, file_err()}.
write(File, Snapshot) ->
    file:write_file(File, [<<?MAGIC>>, term_to_binary(Snapshot)]).


-spec read(file:filename()) ->
    {ok, state()} | {error, invalid_format | file_err()}.
read(File) ->
    case file:read_file(File) of
        {ok, <<?MAGIC, Data/binary>>} ->
            {ok, binary_to_term(Data)};
        {ok, _} ->
            {error, invalid_format};
        {error, _} = Err ->
            Err
    end.


-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.
