%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2017-2025 Broadcom. All Rights Reserved. The term Broadcom refers to Broadcom Inc. and/or its subsidiaries.
%%
-module(ra_system_SUITE).

-compile(nowarn_export_all).
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
     start_cluster,
     start_clusters_in_systems,
     restart_system,
     ra_overview,
     ra_overview_not_started,
     stop_system
    ].

groups() ->
    [
     {tests, [], all_tests()}
    ].

init_per_suite(Config) ->
    %% as we're not starting the ra application and we want the logs
    ra_env:configure_logger(logger),
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(_Group, Config) ->
    Config.

end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(TestCase, Config) ->
    DataDir = filename:join(?config(priv_dir, Config), TestCase),
    [{data_dir, DataDir}, {cluster_name, TestCase} | Config].

end_per_testcase(_TestCase, _Config) ->
    ok.

%%%===================================================================
%%% Test cases
%%%===================================================================

conf({Name, _Node} = NodeId, Nodes) ->
    UId = atom_to_binary(Name, utf8),
    #{cluster_name => c1,
      id => NodeId,
      uid => UId,
      initial_members => Nodes,
      log_init_args => #{uid => UId},
      machine => {module, ?MODULE, #{}}}.

start_cluster(Config) ->
    Sys = ?FUNCTION_NAME,
    DataDir = ?config(data_dir, Config),
    ClusterName = ?config(cluster_name, Config),
    Peers = [start_peer(DataDir) || _ <- lists:seq(1, 3)],
    ServerIds = [{ClusterName, S} || {S, _P} <- Peers],
    Nodes = lists:map(fun ({_, N}) -> N end, ServerIds),
    Machine = {module, ?MODULE, #{}},
    %% the system hasn't been started yet
    {error, cluster_not_formed} = ra:start_cluster(Sys, ClusterName, Machine,
                                                   ServerIds),
    %% start system on all nodes
    [ok = start_system_on(Sys, N, DataDir) || N <- Nodes],

    {ok, Started, []} = ra:start_cluster(Sys, ClusterName, Machine, ServerIds),
    % assert all were said to be started
    [] = Started -- ServerIds,
    % assert all nodes are actually started
    PingResults = [{pong, _} = ra_server_proc:ping(S, 500) || S <- ServerIds],
    % assert one node is leader
    ?assert(lists:any(fun ({pong, S}) -> S =:= leader end, PingResults)),
    stop_peers(Peers),
    ok.

start_clusters_in_systems(Config) ->
    Sys1 = system_1,
    Sys2 = system_2,
    DataDir = ?config(data_dir, Config),
    ClusterName1 = start_clusters_in_systems_1,
    ClusterName2 = start_clusters_in_systems_2,
    Peers = [start_peer(DataDir) || _ <- lists:seq(1, 3)],
    Servers1 = [{ClusterName1, S} || {S, _P} <- Peers],
    Servers2 = [{ClusterName2, S} || {S, _P} <- Peers],
    Machine = {module, ?MODULE, #{}},
    %% the system hasn't been started yet
    {error, cluster_not_formed} = ra:start_cluster(Sys1, ClusterName1, Machine,
                                                   Servers1),
    %% start system on all nodes
    [ok = start_system_on(Sys1, S, DataDir) || {S, _P} <- Peers],
    [ok = start_system_on(Sys2, S, DataDir) || {S, _P} <- Peers],

    {ok, Started1, []} = ra:start_cluster(Sys1, ClusterName1, Machine, Servers1),
    [] = Started1 -- Servers1,
    {ok, Started2, []} = ra:start_cluster(Sys2, ClusterName2, Machine, Servers2),
    [] = Started2 -- Servers2,
    stop_peers(Peers),
    ok.

restart_system(Config) ->
    Sys = ?FUNCTION_NAME,
    DataDir = ?config(data_dir, Config),
    ClusterName = ?config(cluster_name, Config),
    Peers = [start_peer(DataDir) || _ <- lists:seq(1, 3)],
    ServerIds = [{ClusterName, S} || {S, _P} <- Peers],
    Nodes = lists:map(fun ({_, N}) -> N end, ServerIds),
    Machine = {module, ?MODULE, #{}},
    %% the system hasn't been started yet
    {error, cluster_not_formed} = ra:start_cluster(Sys, ClusterName, Machine,
                                                   ServerIds),
    %% start system on all nodes
    [ok = start_system_on(Sys, N, DataDir) || N <- Nodes],

    {ok, Started, []} = ra:start_cluster(Sys, ClusterName, Machine, ServerIds),
    % assert all were said to be started
    [] = Started -- ServerIds,
    %% stop all
    [rpc:call(N, application, stop, [ra]) || N <- Nodes],
    [rpc:call(N, application, ensure_all_started, [ra]) || N <- Nodes],
    [ok = start_system_on(Sys, N, DataDir) || N <- Nodes],
    {ok, Started2, []} = ra:start_cluster(Sys, ClusterName, Machine, ServerIds),
    [] = Started2 -- ServerIds,
    stop_peers(Peers),
    ok.

ra_overview(Config) ->
    DataDir = ?config(data_dir, Config),
    SysCfg = #{name => system_name,
               data_dir => DataDir,
               names => ra_system:derive_names(system_name)},
    application:ensure_all_started(ra),
    ra_system:start(SysCfg),
    Overview = ra:overview(system_name),
    ?assert(is_map(Overview)),
    ?assert(maps:is_key(servers, Overview)),
    ok.

ra_overview_not_started(_Config) ->
    ?assertEqual(ra:overview(unstarted_system), system_not_started).

stop_system(Config) ->
    Sys = ?FUNCTION_NAME,
    DataDir = ?config(data_dir, Config),
    SysCfg = #{name => Sys,
               data_dir => DataDir,
               names => ra_system:derive_names(Sys)},

    _ = application:stop(ra),

    ?assertEqual(undefined, erlang:whereis(ra_systems_sup)),
    ?assertEqual(ok, ra_system:stop(Sys)),

    {ok, _} = application:ensure_all_started(ra),

    ?assertEqual(undefined, ra_system:fetch(Sys)),
    ?assertEqual(ok, ra_system:stop(Sys)),
    ?assertEqual(undefined, ra_system:fetch(Sys)),

    ?assertMatch({ok, _}, ra_system:start(SysCfg)),
    ?assertMatch(#{name := Sys}, ra_system:fetch(Sys)),

    ?assertEqual(ok, ra_system:stop(Sys)),
    ?assertEqual(undefined, ra_system:fetch(Sys)),

    ok.

%% Utility

node_setup(DataDir) ->
    ok = ra_lib:make_dir(DataDir),
    LogFile = filename:join([DataDir, atom_to_list(node()), "ra.log"]),
    SaslFile = filename:join([DataDir, atom_to_list(node()), "ra_sasl.log"]),
    application:load(sasl),
    application:set_env(sasl, sasl_error_logger, {file, SaslFile}),
    application:stop(sasl),
    application:start(sasl),
    _ = error_logger:logfile({open, LogFile}),
    _ = error_logger:tty(false),
    ok.

get_current_host() ->
    NodeStr = atom_to_list(node()),
    Host = re:replace(NodeStr, "^[^@]+@", "", [{return, list}]),
    list_to_atom(Host).

make_node_name(N) ->
    H = get_current_host(),
    list_to_atom(lists:flatten(io_lib:format("~s@~s", [N, H]))).

search_paths() ->
    Ld = code:lib_dir(),
    lists:filter(fun (P) -> string:prefix(P, Ld) =:= nomatch end,
                 code:get_path()).

start_peer(PrivDir) ->
    Name = ?CT_PEER_NAME(),
    Dir0 = filename:join(PrivDir, Name),
    Dir = "'" ++ Dir0 ++ "'",
    Pa = filename:dirname(code:which(ra)),
    Args = ["-pa", Pa, "-ra", "data_dir", Dir],
    ct:pal("starting child node ~ts for node ~ts~n", [Name, Args]),
    {ok, P, S} = ?CT_PEER(#{name => Name, args => Args}),
    {ok, _} = rpc:call(S, application, ensure_all_started, [ra]),
    {S, P}.

stop_peers(Peers) ->
    [peer:stop(P) || {_S, P} <- Peers].

flush() ->
    receive
        Any ->
            ct:pal("flush ~p", [Any]),
            flush()
    after 0 ->
              ok
    end.

%% ra_machine impl

init(_) ->
    {#{}, []}.

apply(_Meta, {send_local_msg, Pid, Opts}, State) ->
    {State, ok, [{send_msg, Pid, {local_msg, node()}, Opts}]};
apply(#{index := Idx}, {do_local_log, SenderPid, Opts}, State) ->
    Eff = {log, [Idx],
           fun([{do_local_log, Pid, _}]) ->
                   [{send_msg, Pid, {local_msg, node()}, Opts}]
           end,
           {local, node(SenderPid)}},
    {State, ok, [Eff]};
apply(_Meta, _Cmd, State) ->
    {State, []}.

start_system_on(Sys, Node, BaseDir) ->
    Dir = filename:join([BaseDir, Node, Sys]),

    SysCfg = #{name => Sys,
               data_dir => Dir,
               names => ra_system:derive_names(Sys)},
    {ok, _} = rpc:call(Node, ra_system, start, [SysCfg]),
    ok.
