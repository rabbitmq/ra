%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2017-2020 VMware, Inc. or its affiliates.  All rights reserved.
%%
-module(ra_system_SUITE).

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
     start_cluster,
     start_clusters_in_systems,
     restart_system
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
    ServerIds = [{ClusterName, start_child_node(N, DataDir)}
                 || N <- [s1, s2, s3]],
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
    [ok = slave:stop(N) || N <- Nodes],
    ok.

start_clusters_in_systems(Config) ->
    Sys1 = system_1,
    Sys2 = system_2,
    DataDir = ?config(data_dir, Config),
    ClusterName1 = start_clusters_in_systems_1,
    ClusterName2 = start_clusters_in_systems_2,
    Nodes = [start_child_node(N, DataDir) || N <- [s1, s2, s3]],
    Servers1 = [{ClusterName1, N} || N <- Nodes],
    Servers2 = [{ClusterName2, N} || N <- Nodes],
    Machine = {module, ?MODULE, #{}},
    %% the system hasn't been started yet
    {error, cluster_not_formed} = ra:start_cluster(Sys1, ClusterName1, Machine,
                                                   Servers1),
    %% start system on all nodes
    [ok = start_system_on(Sys1, N, DataDir) || N <- Nodes],
    [ok = start_system_on(Sys2, N, DataDir) || N <- Nodes],

    {ok, Started1, []} = ra:start_cluster(Sys1, ClusterName1, Machine, Servers1),
    [] = Started1 -- Servers1,
    {ok, Started2, []} = ra:start_cluster(Sys2, ClusterName2, Machine, Servers2),
    [] = Started2 -- Servers2,
    ok.

restart_system(Config) ->
    Sys = ?FUNCTION_NAME,
    DataDir = ?config(data_dir, Config),
    ClusterName = ?config(cluster_name, Config),
    ServerIds = [{ClusterName, start_child_node(N, DataDir)}
                 || N <- [s1, s2, s3]],
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

start_child_node(N, _PrivDir) ->
    Host = get_current_host(),
    Pa = string:join(["-pa" | search_paths()], " "),
    ct:pal("starting child node with ~s on host ~s for node ~s~n", [Pa, Host, node()]),
    {ok, S} = slave:start_link(Host, N, Pa),
    _ = rpc:call(S, application, ensure_all_started, [ra]),
    S.

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
