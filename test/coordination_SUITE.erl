-module(coordination_SUITE).

-compile(export_all).

-export([
         ]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(info, true).

%%%===================================================================
%%% Common Test callbacks
%%%===================================================================

all() ->
    [
     {group, tests}
    ].


all_tests() ->
    [
     start_stop_restart_delete_on_remote,
     start_cluster,
     start_or_restart_cluster,
     delete_one_server_cluster,
     delete_two_server_cluster,
     delete_three_server_cluster,
     start_cluster_majority,
     start_cluster_minority
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
    [{data_dir, DataDir}, {cluster_id, TestCase} | Config].

end_per_testcase(_TestCase, _Config) ->
    ok.

%%%===================================================================
%%% Test cases
%%%===================================================================

conf({Name, _Node} = NodeId, Nodes) ->
    UId = atom_to_binary(Name, utf8),
    #{cluster_id => c1,
      id => NodeId,
      uid => UId,
      initial_members => Nodes,
      log_init_args => #{uid => UId},
      machine => {module, ?MODULE, #{}}}.

start_stop_restart_delete_on_remote(Config) ->
    PrivDir = ?config(data_dir, Config),
    S1 = start_slave(s1, PrivDir),
    % ensure application is started
    NodeId = {c1, S1},
    Conf = conf(NodeId, [NodeId]),
    ok = ra:start_server(Conf),
    ok = ra:trigger_election(NodeId),
    % idempotency
    {error, {already_started, _}} = ra:start_server(Conf),
    ok = ra:stop_server(NodeId),
    ok = ra:restart_server(NodeId),
    % idempotency
    {error, {already_started, _}} = ra:restart_server(NodeId),
    ok = ra:stop_server(NodeId),
    % idempotency
    ok = ra:stop_server(NodeId),
    ok = ra:delete_server(NodeId),
    % idempotency
    {error, _} = ra:delete_server(NodeId),
    timer:sleep(500),
    slave:stop(S1),
    ok.

start_cluster(Config) ->
    PrivDir = ?config(data_dir, Config),
    ClusterId = ?config(cluster_id, Config),
    NodeIds = [{ClusterId, start_slave(N, PrivDir)} || N <- [s1,s2,s3]],
    Machine = {module, ?MODULE, #{}},
    {ok, Started, []} = ra:start_cluster(ClusterId, Machine, NodeIds),
    % assert all were said to be started
    [] = Started -- NodeIds,
    % assert all nodes are actually started
    PingResults = [{pong, _} = ra_server_proc:ping(N, 500) || N <- NodeIds],
    % assert one node is leader
    ?assert(lists:any(fun ({pong, S}) -> S =:= leader end, PingResults)),
    [ok = slave:stop(S) || {_, S} <- NodeIds],
    ok.

start_or_restart_cluster(Config) ->
    PrivDir = ?config(data_dir, Config),
    ClusterId = ?config(cluster_id, Config),
    NodeIds = [{ClusterId, start_slave(N, PrivDir)} || N <- [s1,s2,s3]],
    Machine = {module, ?MODULE, #{}},
    %% this should start
    {ok, Started, []} = ra:start_or_restart_cluster(ClusterId, Machine,
                                                    NodeIds),
    % assert all were said to be started
    [] = Started -- NodeIds,
    % assert all nodes are actually started
    PingResults = [{pong, _} = ra_server_proc:ping(N, 500) || N <- NodeIds],
    % assert one node is leader
    ?assert(lists:any(fun ({pong, S}) -> S =:= leader end, PingResults)),
    % timer:sleep(1000),
    [ok = slave:stop(S) || {_, S} <- NodeIds],
    NodeIds = [{ClusterId, start_slave(N, PrivDir)} || N <- [s1,s2,s3]],
    %% this should restart
    {ok, Started2, []} = ra:start_or_restart_cluster(ClusterId, Machine,
                                                     NodeIds),
    [] = Started2 -- NodeIds,
    timer:sleep(1000),
    PingResults2 = [{pong, _} = ra_server_proc:ping(N, 500) || N <- NodeIds],
    % assert one node is leader
    ?assert(lists:any(fun ({pong, S}) -> S =:= leader end, PingResults2)),
    [ok = slave:stop(S) || {_, S} <- NodeIds],
    ok.

delete_one_server_cluster(Config) ->
    PrivDir = ?config(data_dir, Config),
    ClusterId = ?config(cluster_id, Config),
    NodeIds = [{ClusterId, start_slave(N, PrivDir)} || N <- [s1]],
    Machine = {module, ?MODULE, #{}},
    {ok, _, []} = ra:start_cluster(ClusterId, Machine, NodeIds),
    [{_, Node}] = NodeIds,
    UId = rpc:call(Node, ra_directory, uid_of, [ClusterId]),
    false = undefined =:= UId,
    {ok, _} = ra:delete_cluster(NodeIds),
    timer:sleep(250),
    S1DataDir = rpc:call(Node, ra_env, data_dir, []),
    Wc = filename:join([S1DataDir, "*"]),
    [] = [F || F <- filelib:wildcard(Wc), filelib:is_dir(F)],
    {error, _} = ra_server_proc:ping(hd(NodeIds), 50),
    % assert all nodes are actually started
    [ok = slave:stop(S) || {_, S} <- NodeIds],
    % restart node
    NodeIds = [{ClusterId, start_slave(N, PrivDir)} || N <- [s1]],
    receive
        Anything ->
            ct:pal("got weird message ~p~n", [Anything]),
            exit({unexpected, Anything})
    after 250 ->
              ok
    end,
    %% validate there is no data
    Files = [F || F <- filelib:wildcard(Wc), filelib:is_dir(F)],
    undefined = rpc:call(Node, ra_directory, uid_of, [ClusterId]),
    undefined = rpc:call(Node, ra_log_meta, fetch, [UId, current_term]),
    ct:pal("Files  ~p~n", [Files]),
    [] = Files,
    [ok = slave:stop(S) || {_, S} <- NodeIds],
    ok.

delete_two_server_cluster(Config) ->
    PrivDir = ?config(data_dir, Config),
    ClusterId = ?config(cluster_id, Config),
    NodeIds = [{ClusterId, start_slave(N, PrivDir)} || N <- [s1,s2]],
    Machine = {module, ?MODULE, #{}},
    {ok, _, []} = ra:start_cluster(ClusterId, Machine, NodeIds),
    {ok, _} = ra:delete_cluster(NodeIds),
    timer:sleep(250),
    {error, _} = ra_server_proc:ping(hd(tl(NodeIds)), 50),
    {error, _} = ra_server_proc:ping(hd(NodeIds), 50),
    % assert all nodes are actually started
    [ok = slave:stop(S) || {_, S} <- NodeIds],
    receive
        Anything ->
            ct:pal("got wierd message ~p~n", [Anything]),
            exit({unexpected, Anything})
    after 250 ->
              ok
    end,
    ok.

delete_three_server_cluster(Config) ->
    PrivDir = ?config(data_dir, Config),
    ClusterId = ?config(cluster_id, Config),
    NodeIds = [{ClusterId, start_slave(N, PrivDir)} || N <- [s1,s2,s3]],
    Machine = {module, ?MODULE, #{}},
    {ok, _, []} = ra:start_cluster(ClusterId, Machine, NodeIds),
    {ok, _} = ra:delete_cluster(NodeIds),
    timer:sleep(250),
    {error, _} = ra_server_proc:ping(hd(tl(NodeIds)), 50),
    {error, _} = ra_server_proc:ping(hd(NodeIds), 50),
    % assert all nodes are actually started
    [ok = slave:stop(S) || {_, S} <- NodeIds],
    ok.

start_cluster_majority(Config) ->
    PrivDir = ?config(data_dir, Config),
    ClusterId = ?config(cluster_id, Config),
    NodeIds0 = [{ClusterId, start_slave(N, PrivDir)} || N <- [s1,s2]],
    % s3 isn't available
    S3 = make_node_name(s3),
    NodeIds = NodeIds0 ++ [{ClusterId, S3}],
    Machine = {module, ?MODULE, #{}},
    {ok, Started, NotStarted} =
        ra:start_cluster(ClusterId, Machine, NodeIds),
    % assert  two were started
    ?assertEqual(2,  length(Started)),
    ?assertEqual(1,  length(NotStarted)),
    % assert all started are actually started
    PingResults = [{pong, _} = ra_server_proc:ping(N, 500) || N <- Started],
    % assert one node is leader
    ?assert(lists:any(fun ({pong, S}) -> S =:= leader end, PingResults)),
    [ok = slave:stop(S) || {_, S} <- NodeIds0],
    ok.

start_cluster_minority(Config) ->
    PrivDir = ?config(data_dir, Config),
    ClusterId = ?config(cluster_id, Config),
    NodeIds0 = [{ClusterId, start_slave(N, PrivDir)} || N <- [s1]],
    % s3 isn't available
    S2 = make_node_name(s2),
    S3 = make_node_name(s3),
    NodeIds = NodeIds0 ++ [{ClusterId, S2}, {ClusterId, S3}],
    Machine = {module, ?MODULE, #{}},
    {error, cluster_not_formed} =
        ra:start_cluster(ClusterId, Machine, NodeIds),
    % assert none is started
    [{error, _} = ra_server_proc:ping(N, 50) || N <- NodeIds],
    [ok = slave:stop(S) || {_, S} <- NodeIds0],
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

start_slave(N, PrivDir) ->
    Dir0 = filename:join(PrivDir, N),
    Host = get_current_host(),
    Dir = "'\"" ++ Dir0 ++ "\"'",
    Pa = string:join(["-pa" | search_paths()] ++ ["-s ra -ra data_dir", Dir], " "),
    ct:pal("starting slave node with ~s~n", [Pa]),
    {ok, S} = slave:start_link(Host, N, Pa),
    _ = rpc:call(S, ra, start, []),
    S.

%% ra_machine impl

init(_) ->
    {#{}, []}.

apply(_Meta, _Cmd, Effects, State) ->
    {State, Effects}.
