-module(coordination_SUITE).

-compile(export_all).

-export([
         ]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

%%%===================================================================
%%% Common Test callbacks
%%%===================================================================

all() ->
    [
     {group, tests}
    ].


all_tests() ->
    [
     start_stop_restart_delete_on_remote
    ].

groups() ->
    [
     {tests, [], all_tests()}
    ].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(_Group, Config) ->
    Config.

end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(TestCase, Config) ->
    DataDir = filename:join(?config(priv_dir, Config), TestCase),
    [{data_dir, DataDir} | Config].

end_per_testcase(_TestCase, _Config) ->
    ok.

%%%===================================================================
%%% Test cases
%%%===================================================================

start_stop_restart_delete_on_remote(Config) ->
    PrivDir = ?config(data_dir, Config),
    S1Dir = filename:join(PrivDir, s1),
    {ok, S1} = start_slave(s1, S1Dir),
    % ensure application is started
    _ = rpc:call(S1, application, ensure_all_started, [ra]),
    UId = <<"c1">>,
    NodeId = {c1, S1},
    Conf = #{cluster_id => c1,
             id => NodeId,
             uid => UId,
             initial_nodes => [{c1, S1}],
             log_module => ra_log_file,
             log_init_args => #{uid => UId},
             machine => {module, ra_fifo, #{}}},
    ok = ra:start_node(Conf),
    ok = ra:trigger_election(NodeId),
    % idempotency
    ok = ra:start_node(Conf),
    ok = ra:stop_node(NodeId),
    ok = ra:restart_node(NodeId),
    % idempotency
    ok = ra:restart_node(NodeId),
    ok = ra:stop_node(NodeId),
    % idempotency
    ok = ra:stop_node(NodeId),
    ok = ra:delete_node(NodeId),
    % idempotency
    ok = ra:delete_node(NodeId),
    timer:sleep(500),
    ok.

%% Utility

node_setup(DataDir) ->
    LogFile = filename:join([DataDir, atom_to_list(node()), "ra.log"]),
    SaslFile = filename:join([DataDir, atom_to_list(node()), "ra_sasl.log"]),
    application:load(sasl),
    application:set_env(sasl, sasl_error_logger, {file, SaslFile}),
    application:stop(sasl),
    application:start(sasl),
    filelib:ensure_dir(LogFile),
    _ = error_logger:logfile({open, LogFile}),
    _ = error_logger:tty(false),
    ok.

get_current_host() ->
    {ok, H} = inet:gethostname(),
    list_to_atom(H).

make_node_name(N) ->
    {ok, H} = inet:gethostname(),
    list_to_atom(lists:flatten(io_lib:format("~s@~s", [N, H]))).

search_paths() ->
    Ld = code:lib_dir(),
    lists:filter(fun (P) -> string:prefix(P, Ld) =:= nomatch end,
                 code:get_path()).

start_slave(N, Dir0) ->
    Host = get_current_host(),
    Dir = "'\"" ++ Dir0 ++ "\"'",
    Pa = string:join(["-pa" | search_paths()] ++ ["-s ra -ra data_dir", Dir], " "),
    ct:pal("starting slave node with ~s~n", [Pa]),
    slave:start_link(Host, N, Pa).
