%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term Broadcom refers to Broadcom Inc. and/or its subsidiaries.
%%
-module(coordination_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-export([
         ]).

-include_lib("src/ra.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(SYS, default).

%%%===================================================================
%%% Common Test callbacks
%%%===================================================================

all() ->
    [
     {group, tests}
    ].


all_tests() ->
    [
     nonvoter_catches_up,
     nonvoter_catches_up_after_restart,
     nonvoter_catches_up_after_leader_restart,
     start_stop_restart_delete_on_remote,
     start_cluster,
     start_or_restart_cluster,
     delete_one_server_cluster,
     delete_two_server_cluster,
     delete_three_server_cluster,
     delete_three_server_cluster_parallel,
     start_cluster_majority,
     start_cluster_minority,
     grow_cluster,
     shrink_cluster_with_snapshot,
     send_local_msg,
     local_log_effect,
     leaderboard,
     bench,
     disconnected_node_catches_up,
     key_metrics,
     recover_from_checkpoint,
     segment_writer_or_wal_crash_follower,
     segment_writer_or_wal_crash_leader,
     server_recovery_strategy,
     stopped_wal_causes_leader_change_registered,
     stopped_wal_causes_leader_change_mfa
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

start_stop_restart_delete_on_remote(Config) ->
    PrivDir = ?config(data_dir, Config),
    S1 = start_follower(s1, PrivDir),
    % ensure application is started
    ServerId = {c1, S1},
    Conf = conf(ServerId, [ServerId]),
    ok = ra:start_server(?SYS, Conf),
    ok = ra:trigger_election(ServerId),
    % idempotency
    {error, {already_started, _}} = ra:start_server(?SYS, Conf),
    ok = ra:stop_server(?SYS, ServerId),
    ok = ra:restart_server(?SYS, ServerId),
    % idempotency
    {error, {already_started, _}} = ra:restart_server(?SYS, ServerId),
    ok = ra:stop_server(?SYS, ServerId),
    % idempotency
    ok = ra:stop_server(?SYS, ServerId),
    ok = ra:force_delete_server(?SYS, ServerId),
    % idempotency
    ok = ra:force_delete_server(?SYS, ServerId),
    stop_nodes([ServerId]),
    slave:stop(S1),
    ok.

start_cluster(Config) ->
    PrivDir = ?config(data_dir, Config),
    ClusterName = ?config(cluster_name, Config),
    ServerIds = [{ClusterName, start_follower(N, PrivDir)} || N <- [s1,s2,s3,s4,s5]],
    Machine = {module, ?MODULE, #{}},
    {ok, Started, []} = ra:start_cluster(?SYS, ClusterName, Machine, ServerIds),
    % assert all were said to be started
    [] = Started -- ServerIds,
    ra:members(hd(Started)),
    % assert all nodes are actually started
    PingResults = [{pong, _} = ra_server_proc:ping(N, 500) || N <- ServerIds],
    % assert one node is leader
    ?assert(lists:any(fun ({pong, S}) -> S =:= leader end, PingResults)),
    stop_nodes(ServerIds),
    ok.

start_or_restart_cluster(Config) ->
    PrivDir = ?config(data_dir, Config),
    ClusterName = ?config(cluster_name, Config),
    ServerIds = [{ClusterName, start_follower(N, PrivDir)} || N <- [s1,s2,s3]],
    Machine = {module, ?MODULE, #{}},
    %% this should start
    {ok, Started, []} = ra:start_or_restart_cluster(?SYS, ClusterName, Machine,
                                                    ServerIds),
    % assert all were said to be started
    [] = Started -- ServerIds,
    % assert all nodes are actually started
    PingResults = [{pong, _} = ra_server_proc:ping(N, 500) || N <- ServerIds],
    % assert one node is leader
    ?assert(lists:any(fun ({pong, S}) -> S =:= leader end, PingResults)),
    [ok = slave:stop(S) || {_, S} <- ServerIds],
    ServerIds = [{ClusterName, start_follower(N, PrivDir)} || N <- [s1,s2,s3]],
    %% this should restart
    {ok, Started2, []} = ra:start_or_restart_cluster(?SYS, ClusterName, Machine,
                                                     ServerIds),
    [] = Started2 -- ServerIds,
    timer:sleep(1000),
    PingResults2 = [{pong, _} = ra_server_proc:ping(N, 500) || N <- ServerIds],
    % assert one node is leader
    ?assert(lists:any(fun ({pong, S}) -> S =:= leader end, PingResults2)),
    stop_nodes(ServerIds),
    ok.

delete_one_server_cluster(Config) ->
    PrivDir = ?config(data_dir, Config),
    ClusterName = ?config(cluster_name, Config),
    ServerIds = [{ClusterName, start_follower(N, PrivDir)} || N <- [s1]],
    Machine = {module, ?MODULE, #{}},
    {ok, _, []} = ra:start_cluster(?SYS, ClusterName, Machine, ServerIds),
    [{_, Node}] = ServerIds,
    UId = rpc:call(Node, ra_directory, uid_of, [ClusterName]),
    false = undefined =:= UId,
    {ok, _} = ra:delete_cluster(ServerIds),
    timer:sleep(250),
    S1DataDir = rpc:call(Node, ra_env, data_dir, []),
    Wc = filename:join([S1DataDir, "*"]),
    [] = [F || F <- filelib:wildcard(Wc), filelib:is_dir(F)],
    {error, _} = ra_server_proc:ping(hd(ServerIds), 50),
    % assert all nodes are actually started
    [ok = slave:stop(S) || {_, S} <- ServerIds],
    % restart node
    ServerIds = [{ClusterName, start_follower(N, PrivDir)} || N <- [s1]],
    receive
        Anything ->
            ct:pal("got weird message ~p", [Anything]),
            exit({unexpected, Anything})
    after 250 ->
              ok
    end,
    %% validate there is no data
    Files = [F || F <- filelib:wildcard(Wc), filelib:is_dir(F)],
    undefined = rpc:call(Node, ra_directory, uid_of, [?SYS, ClusterName]),
    undefined = rpc:call(Node, ra_log_meta, fetch, [ra_log_meta, UId, current_term]),
    ct:pal("Files  ~p", [Files]),
    [] = Files,
    stop_nodes(ServerIds),
    ok.

delete_two_server_cluster(Config) ->
    PrivDir = ?config(data_dir, Config),
    ClusterName = ?config(cluster_name, Config),
    ServerIds = [{ClusterName, start_follower(N, PrivDir)} || N <- [s1,s2]],
    Machine = {module, ?MODULE, #{}},
    {ok, _, []} = ra:start_cluster(?SYS, ClusterName, Machine, ServerIds),
    {ok, _} = ra:delete_cluster(ServerIds),
    await_condition(
      fun () ->
              lists:all(
                fun ({Name, Node}) ->
                        undefined == erpc:call(Node, erlang, whereis, [Name])
                end, ServerIds)
      end, 100),
    stop_nodes(ServerIds),
    receive
        Anything ->
            ct:pal("got weird message ~p", [Anything]),
            exit({unexpected, Anything})
    after 250 ->
              ok
    end,
    ok.

delete_three_server_cluster(Config) ->
    PrivDir = ?config(data_dir, Config),
    ClusterName = ?config(cluster_name, Config),
    ServerIds = [{ClusterName, start_follower(N, PrivDir)} || N <- [s1,s2,s3]],
    Machine = {module, ?MODULE, #{}},
    {ok, _, []} = ra:start_cluster(?SYS, ClusterName, Machine, ServerIds),
    {ok, _} = ra:delete_cluster(ServerIds),
    await_condition(
      fun () ->
              lists:all(
                fun ({Name, Node}) ->
                        undefined == erpc:call(Node, erlang, whereis, [Name])
                end, ServerIds)
      end, 100),
    stop_nodes(ServerIds),
    ok.

delete_three_server_cluster_parallel(Config) ->
    PrivDir = ?config(data_dir, Config),
    ClusterName = ?config(cluster_name, Config),
    ServerIds = [{ClusterName, start_follower(N, PrivDir)} || N <- [s1,s2,s3]],
    Machine = {module, ?MODULE, #{}},
    {ok, _, []} = ra:start_cluster(?SYS, ClusterName, Machine, ServerIds),
    %% spawn a delete command to try cause it to commit more than
    %% one delete command
    spawn(fun () -> {ok, _} = ra:delete_cluster(ServerIds) end),
    spawn(fun () -> {ok, _} = ra:delete_cluster(ServerIds) end),
    {ok, _} = ra:delete_cluster(ServerIds),
    await_condition(
      fun () ->
              lists:all(
                fun ({Name, Node}) ->
                        undefined == erpc:call(Node, erlang, whereis, [Name])
                end, ServerIds)
      end, 100),
    [begin
         true = rpc:call(S, ?MODULE, check_sup, [])
     end || {_, S} <- ServerIds],
    % assert all nodes are actually started
    stop_nodes(ServerIds),
    ok.

check_sup() ->
    [] == supervisor:which_children(ra_server_sup_sup).

start_cluster_majority(Config) ->
    PrivDir = ?config(data_dir, Config),
    ClusterName = ?config(cluster_name, Config),
    ServerIds0 = [{ClusterName, start_follower(N, PrivDir)} || N <- [s1,s2]],
    % s3 isn't available
    S3 = make_node_name(s3),
    NodeIds = ServerIds0 ++ [{ClusterName, S3}],
    Machine = {module, ?MODULE, #{}},
    {ok, Started, NotStarted} =
        ra:start_cluster(?SYS, ClusterName, Machine, NodeIds),
    % assert  two were started
    ?assertEqual(2,  length(Started)),
    ?assertEqual(1,  length(NotStarted)),
    % assert all started are actually started
    PingResults = [{pong, _} = ra_server_proc:ping(N, 500) || N <- Started],
    % assert one node is leader
    ?assert(lists:any(fun ({pong, S}) -> S =:= leader end, PingResults)),
    stop_nodes(ServerIds0),
    ok.

start_cluster_minority(Config) ->
    PrivDir = ?config(data_dir, Config),
    ClusterName = ?config(cluster_name, Config),
    ServerIds0 = [{ClusterName, start_follower(N, PrivDir)} || N <- [s1]],
    % s3 isn't available
    S2 = make_node_name(s2),
    S3 = make_node_name(s3),
    NodeIds = ServerIds0 ++ [{ClusterName, S2}, {ClusterName, S3}],
    Machine = {module, ?MODULE, #{}},
    {error, cluster_not_formed} =
        ra:start_cluster(?SYS, ClusterName, Machine, NodeIds),
    % assert none is started
    [{error, _} = ra_server_proc:ping(N, 50) || N <- NodeIds],
    stop_nodes(ServerIds0),
    ok.

grow_cluster(Config) ->
    PrivDir = ?config(data_dir, Config),
    ClusterName = ?config(cluster_name, Config),
    [{_, ANode} = A,
     {_, BNode} = B,
     {_, CNode} = C] =
    ServerIds = [{ClusterName, start_follower(N, PrivDir)} || N <- [s1,s2,s3]],
    Machine = {module, ?MODULE, #{}},
    {ok, [A], []} = ra:start_cluster(?SYS, ClusterName, Machine, [A]),

    ok = ra:start_server(?SYS, ClusterName, B, Machine, [A]),
    _ = ra:members(A),
    {ok, _, _} = ra:add_member(A, B),
    {ok, _, _} = ra:process_command(A, banana),

    [A, B] = rpc:call(BNode, ra_leaderboard, lookup_members, [ClusterName]),
    [A, B] = rpc:call(ANode, ra_leaderboard, lookup_members, [ClusterName]),

    ok = ra:start_server(?SYS, ClusterName, C, Machine, [A, B]),
    {ok, _, _} = ra:add_member(A, C),
    {ok, _, _} = ra:process_command(A, banana),
    {ok, _, L1} = ra:members(A),
    [A, B, C] = rpc:call(ANode, ra_leaderboard, lookup_members, [ClusterName]),
    L1 = rpc:call(ANode, ra_leaderboard, lookup_leader, [ClusterName]),

    await_condition(
      fun () ->
              [A, B, C] == rpc:call(BNode, ra_leaderboard, lookup_members, [ClusterName]) andalso
              L1 == rpc:call(BNode, ra_leaderboard, lookup_leader, [ClusterName])
      end, 20),
    await_condition(
      fun () ->
              [A, B, C] == rpc:call(CNode, ra_leaderboard, lookup_members, [ClusterName]) andalso
              L1 == rpc:call(CNode, ra_leaderboard, lookup_leader, [ClusterName])
      end, 20),

    ok = ra:leave_and_delete_server(?SYS, A, A),
    %% wait for B to process the cluster change
    await_condition(
      fun () ->
              [B, C] == rpc:call(CNode, ra_leaderboard, lookup_members, [ClusterName])
      end, 20),
    {ok, _, L2} = ra:process_command(B, banana),

    %% check members
    [B, C] = rpc:call(BNode, ra_leaderboard, lookup_members, [ClusterName]),
    [B, C] = rpc:call(CNode, ra_leaderboard, lookup_members, [ClusterName]),
    undefined = rpc:call(ANode, ra_leaderboard, lookup_members, [ClusterName]),
    %% check leader
    L2 = rpc:call(CNode, ra_leaderboard, lookup_leader, [ClusterName]),
    L2 = rpc:call(BNode, ra_leaderboard, lookup_leader, [ClusterName]),
    undefined = rpc:call(ANode, ra_leaderboard, lookup_leader, [ClusterName]),


    ok = ra:leave_and_delete_server(?SYS, B, B),
    {ok, _, _} = ra:process_command(C, banana),
    %% check members
    [C] = rpc:call(CNode, ra_leaderboard, lookup_members, [ClusterName]),
    undefined = rpc:call(ANode, ra_leaderboard, lookup_members, [ClusterName]),
    undefined = rpc:call(BNode, ra_leaderboard, lookup_members, [ClusterName]),
    %% check leader
    C = rpc:call(CNode, ra_leaderboard, lookup_leader, [ClusterName]),
    undefined = rpc:call(ANode, ra_leaderboard, lookup_leader, [ClusterName]),
    undefined = rpc:call(BNode, ra_leaderboard, lookup_leader, [ClusterName]),

    stop_nodes(ServerIds),
    ok.

shrink_cluster_with_snapshot(Config) ->
    %% this test removes leaders to ensure the remaining cluster can
    %% resume activity ok
    PrivDir = ?config(data_dir, Config),
    ClusterName = ?config(cluster_name, Config),
    Peers = start_peers([s1,s2,s3], PrivDir),
    ServerIds = server_ids(ClusterName, Peers),
    [A, B, C] = ServerIds,

    Machine = {module, ?MODULE, #{}},
    {ok, _, []} = ra:start_cluster(?SYS, ClusterName, Machine, ServerIds),
    {ok, _, Leader1} = ra:members(ServerIds),

    %% run some activity to create a snapshot
    [_ = ra:process_command(Leader1, {banana, I})
      || I <- lists:seq(1, 5000)],

    Fun = fun F(L0) ->
                  {ok, _, L} = ra:process_command(L0, banana),
                  F(L)
          end,
    Pid = spawn(fun () -> Fun(Leader1) end),
    timer:sleep(100),

    exit(Pid, kill),
    {ok, _, _} = ra:remove_member(Leader1, Leader1),


    timer:sleep(500),

    {ok, _, Leader2} = ra:members(ServerIds),

    ct:pal("old leader ~p, new leader ~p", [Leader1, Leader2]),
    {ok, O, _} = ra:member_overview(Leader2),
    ct:pal("overview2 ~p", [O]),
    stop_peers(Peers),
    ?assertMatch(#{cluster_change_permitted := true}, O),
    ok.

send_local_msg(Config) ->
    PrivDir = ?config(data_dir, Config),
    ClusterName = ?config(cluster_name, Config),
    ServerIds = [A, B, NonVoter] = [{ClusterName, start_follower(N, PrivDir)}
                                    || N <- [s1,s2,s3]],
    NodeIds = [A, B],
    Machine = {module, ?MODULE, #{}},
    {ok, Started, []} = ra:start_cluster(?SYS, ClusterName, Machine, NodeIds),
    % assert all were said to be started
    [] = Started -- NodeIds,
    % add permanent non-voter
    {ok, _, Leader} = ra:members(hd(NodeIds)),
    {ok, _, _} = ra:process_command(Leader, banana),
    New = #{id => NonVoter,
            membership => non_voter,
            uid => <<"test">>},
    {ok, _, _} = ra:add_member(A, New),
    ok = ra:start_server(?SYS, ClusterName, New, Machine, NodeIds),
    %% select a non-leader node to spawn on
    [{_, N} | _] = lists:delete(Leader, NodeIds),
    test_local_msg(Leader, N, N, send_local_msg, local),
    test_local_msg(Leader, N, N, send_local_msg, [local, ra_event]),
    test_local_msg(Leader, N, N, send_local_msg, [local, cast]),
    test_local_msg(Leader, N, N, send_local_msg, [local, cast, ra_event]),
    {_, LeaderNode} = Leader,
    %% test the same but for a local pid (non-member)
    test_local_msg(Leader, node(), LeaderNode, send_local_msg, local),
    test_local_msg(Leader, node(), LeaderNode, send_local_msg, [local, ra_event]),
    test_local_msg(Leader, node(), LeaderNode, send_local_msg, [local, cast]),
    test_local_msg(Leader, node(), LeaderNode, send_local_msg, [local, cast, ra_event]),
    %% same for non-voter
    {_, NonVoterNode} = NonVoter,
    test_local_msg(Leader, NonVoterNode, LeaderNode, send_local_msg, local),
    test_local_msg(Leader, NonVoterNode, LeaderNode, send_local_msg, [local, ra_event]),
    test_local_msg(Leader, NonVoterNode, LeaderNode, send_local_msg, [local, cast]),
    test_local_msg(Leader, NonVoterNode, LeaderNode, send_local_msg, [local, cast, ra_event]),
    stop_nodes(ServerIds),
    ok.

local_log_effect(Config) ->
    PrivDir = ?config(data_dir, Config),
    ClusterName = ?config(cluster_name, Config),
    ServerIds = [A, B, NonVoter] = [{ClusterName, start_follower(N, PrivDir)}
                                    || N <- [s1,s2,s3]],
    NodeIds = [A, B],
    Machine = {module, ?MODULE, #{}},
    {ok, Started, []} = ra:start_cluster(?SYS, ClusterName, Machine, NodeIds),
    % assert all were said to be started
    [] = Started -- NodeIds,
    % add permanent non-voter
    {ok, _, Leader} = ra:members(hd(NodeIds)),
    {ok, _, _} = ra:process_command(Leader, banana),
    New = #{id => NonVoter,
            membership => non_voter,
            uid => <<"test">>},
    {ok, _, _} = ra:add_member(A, New),
    ok = ra:start_server(?SYS, ClusterName, New, Machine, NodeIds),
    %% select a non-leader node to spawn on
    [{_, N} | _] = lists:delete(Leader, NodeIds),
    test_local_msg(Leader, N, N, do_local_log, local),
    test_local_msg(Leader, N, N, do_local_log, [local, ra_event]),
    test_local_msg(Leader, N, N, do_local_log, [local, cast]),
    test_local_msg(Leader, N, N, do_local_log, [local, cast, ra_event]),
    %% test the same but for a local pid (non-member)
    {_, LeaderNode} = Leader,
    test_local_msg(Leader, node(), LeaderNode, do_local_log, local),
    test_local_msg(Leader, node(), LeaderNode, do_local_log, [local, ra_event]),
    test_local_msg(Leader, node(), LeaderNode, do_local_log, [local, cast]),
    test_local_msg(Leader, node(), LeaderNode, do_local_log, [local, cast, ra_event]),
    %% same for non-voter
    {_, NonVoterNode} = NonVoter,
    test_local_msg(Leader, NonVoterNode, LeaderNode, do_local_log, local),
    test_local_msg(Leader, NonVoterNode, LeaderNode, do_local_log, [local, ra_event]),
    test_local_msg(Leader, NonVoterNode, LeaderNode, do_local_log, [local, cast]),
    test_local_msg(Leader, NonVoterNode, LeaderNode, do_local_log, [local, cast, ra_event]),
    stop_nodes(ServerIds),
    ok.

disconnected_node_catches_up(Config) ->
    PrivDir = ?config(data_dir, Config),
    ClusterName = ?config(cluster_name, Config),
    ServerIds = [{ClusterName, start_follower(N, PrivDir)} || N <- [s1,s2,s3]],
    Machine = {module, ?MODULE, #{}},
    {ok, Started, []} = ra:start_cluster(?SYS, ClusterName, Machine, ServerIds),
    {ok, _, Leader} = ra:members(hd(Started)),

    [{_, DownServerNode} = DownServerId, _] = Started -- [Leader],
    %% the ra_directory DETS table has a 500ms autosave configuration
    timer:sleep(1000),

    ok = slave:stop(DownServerNode),

    ct:pal("Nodes ~p", [nodes()]),
    [
     ok = ra:pipeline_command(Leader, N, no_correlation, normal)
     || N <- lists:seq(1, 10000)],
    {ok, _, _} = ra:process_command(Leader, banana),

    %% wait for leader to take a snapshot
    await_condition(
      fun () ->
              {ok, #{log := #{snapshot_index := SI}}, _} =
                  ra:member_overview(Leader),
              SI /= undefined
      end, 20),

    DownServerNodeName =
        case atom_to_binary(DownServerNode) of
            <<Tag:2/binary, _/binary>> -> binary_to_atom(Tag, utf8)
        end,


    DownNode = start_follower(DownServerNodeName, PrivDir),

    Self = self(),
    SPid = erlang:spawn(DownNode,
                     fun () ->
                             erlang:register(snapshot_installed_proc, self()),
                             receive
                                 {snapshot_installed, _Meta} = Evt ->
                                     Self ! Evt,
                                     ok
                             after 10000 ->
                                       ok
                             end
                     end),
    await_condition(
      fun () ->
              ok == ra:restart_server(?SYS, DownServerId)
      end, 100),

    %% wait for snapshot on restarted server
    await_condition(
      fun () ->
              {ok, #{log := #{snapshot_index := SI}}, _} =
                  ra:member_overview(DownServerId),
              SI /= undefined
      end, 200),

    receive
        {snapshot_installed, Meta} ->
            ct:pal("snapshot installed receive ~p", [Meta]),
            ok
    after 10000 ->
              erlang:exit(SPid, kill),
              ct:fail("snapshot_installed not received"),
              ok
    end,

    stop_nodes(ServerIds),
    ok.

nonvoter_catches_up(Config) ->
    PrivDir = ?config(data_dir, Config),
    ClusterName = ?config(cluster_name, Config),
    [A, B, C = {Group, NodeC}] = ServerIds = [{ClusterName, start_follower(N, PrivDir)} || N <- [s1,s2,s3]],
    Machine = {module, ?MODULE, #{}},
    {ok, Started, []} = ra:start_cluster(?SYS, ClusterName, Machine, [A, B]),
    {ok, _, Leader} = ra:members(hd(Started)),

    [ok = ra:pipeline_command(Leader, N, no_correlation, normal)
     || N <- lists:seq(1, 10000)],
    {ok, _, _} = ra:process_command(Leader, banana),

    New = #{id => C, membership => promotable, uid => <<"test">>},
    {ok, _, _} = ra:add_member(A, New),
    ok = ra:start_server(?SYS, ClusterName, New, Machine, [A, B]),
    ?assertMatch(#{Group := #{membership := promotable}},
                 rpc:call(NodeC, ra_directory, overview, [?SYS])),
    ?assertMatch(#{membership := promotable},
                 ra:key_metrics(C)),
    ?assertMatch({ok, #{membership := promotable}, _},
                 ra:member_overview(C)),

    await_condition(
      fun () ->
          {ok, #{membership := M}, _} = ra:member_overview(C),
          M == voter
      end, 200),
    ?assertMatch(#{Group := #{membership := voter}},
                 rpc:call(NodeC, ra_directory, overview, [?SYS])),
    ?assertMatch(#{membership := voter},
                 ra:key_metrics(C)),

    stop_nodes(ServerIds),
    ok.

nonvoter_catches_up_after_restart(Config) ->
    PrivDir = ?config(data_dir, Config),
    ClusterName = ?config(cluster_name, Config),
    [A, B, C = {Group, NodeC}] = ServerIds = [{ClusterName, start_follower(N, PrivDir)} || N <- [s1,s2,s3]],
    Machine = {module, ?MODULE, #{}},
    {ok, Started, []} = ra:start_cluster(?SYS, ClusterName, Machine, [A, B]),
    {ok, _, Leader} = ra:members(hd(Started)),

    [ok = ra:pipeline_command(Leader, N, no_correlation, normal)
     || N <- lists:seq(1, 10000)],
    {ok, _, _} = ra:process_command(Leader, banana),

    New = #{id => C, membership => promotable, uid => <<"test">>},
    {ok, _, _} = ra:add_member(A, New),
    ok = ra:start_server(?SYS, ClusterName, New, Machine, [A, B]),
    ?assertMatch(#{Group := #{membership := promotable}},
                 rpc:call(NodeC, ra_directory, overview, [?SYS])),
    ?assertMatch(#{membership := promotable},
                 ra:key_metrics(C)),
    ?assertMatch({ok, #{membership := promotable}, _},
                 ra:member_overview(C)),
    ok = ra:stop_server(?SYS, C),
    ok = ra:restart_server(?SYS, C),

    await_condition(
      fun () ->
          {ok, #{membership := M}, _} = ra:member_overview(C),
          M == voter
      end, 200),
    ?assertMatch(#{Group := #{membership := voter}},
                 rpc:call(NodeC, ra_directory, overview, [?SYS])),
    ?assertMatch(#{membership := voter},
                 ra:key_metrics(C)),

    stop_nodes(ServerIds),
    ok.

nonvoter_catches_up_after_leader_restart(Config) ->
    PrivDir = ?config(data_dir, Config),
    ClusterName = ?config(cluster_name, Config),
    [A, B, C = {Group, NodeC}] = ServerIds = [{ClusterName, start_follower(N, PrivDir)} || N <- [s1,s2,s3]],
    Machine = {module, ?MODULE, #{}},
    {ok, Started, []} = ra:start_cluster(?SYS, ClusterName, Machine, [A, B]),
    {ok, _, Leader} = ra:members(hd(Started)),

    [ok = ra:pipeline_command(Leader, N, no_correlation, normal)
     || N <- lists:seq(1, 10000)],
    {ok, _, _} = ra:process_command(Leader, banana),

    New = #{id => C, membership => promotable, uid => <<"test">>},
    {ok, _, _} = ra:add_member(A, New),
    ok = ra:start_server(?SYS, ClusterName, New, Machine, [A, B]),
    ?assertMatch(#{Group := #{membership := promotable}},
                 rpc:call(NodeC, ra_directory, overview, [?SYS])),
    ?assertMatch(#{membership := promotable},
                 ra:key_metrics(C)),
    ?assertMatch({ok, #{membership := promotable}, _},
                 ra:member_overview(C)),
    ok = ra:stop_server(?SYS, Leader),
    ok = ra:restart_server(?SYS, Leader),

    await_condition(
      fun () ->
          {ok, #{membership := M}, _} = ra:member_overview(C),
          M == voter
      end, 200),
    ?assertMatch(#{Group := #{membership := voter}},
                 rpc:call(NodeC, ra_directory, overview, [?SYS])),
    ?assertMatch(#{membership := voter},
                 ra:key_metrics(C)),

    stop_nodes(ServerIds),
    ok.

key_metrics(Config) ->
    PrivDir = ?config(data_dir, Config),
    ClusterName = ?config(cluster_name, Config),
    ServerIds = [{ClusterName, start_follower(N, PrivDir)} || N <- [s1,s2,s3]],
    Machine = {module, ?MODULE, #{}},
    {ok, Started, []} = ra:start_cluster(?SYS, ClusterName, Machine, ServerIds),
    {ok, _, Leader} = ra:members(hd(Started)),

    Data = crypto:strong_rand_bytes(1024),
    [begin
         ok = ra:pipeline_command(Leader, {data, Data})
     end || _ <- lists:seq(1, 10000)],
    {ok, _, _} = ra:process_command(Leader, {data, Data}),

    timer:sleep(100),
    TestId  = lists:last(Started),
    ok = ra:stop_server(?SYS, TestId),
    StoppedMetrics = ra:key_metrics(TestId),
    ct:pal("StoppedMetrics  ~p", [StoppedMetrics]),
    ?assertMatch(#{state := noproc,
                   last_applied := LA,
                   last_written_index := LW,
                   commit_index := CI}
                   when LA > 0 andalso
                        LW > 0 andalso
                        CI > 0,
                 StoppedMetrics),
    ok = ra:restart_server(?SYS, TestId),
    {ok, _, _} = ra:process_command(Leader, {data, Data}),
    await_condition(
      fun () ->
              Metrics = ra:key_metrics(TestId),
              ct:pal("FollowerMetrics  ~p", [Metrics]),
              follower == maps:get(state, Metrics)
      end, 200),
    [begin
         M = ra:key_metrics(S),
         ct:pal("Metrics ~p", [M]),
         ?assertMatch(#{state := _,
                        last_applied := LA,
                        last_written_index := LW,
                        commit_index := CI}
                        when LA > 0 andalso
                             LW > 0 andalso
                             CI > 0, M)
     end
     || S <- Started],

    stop_nodes(ServerIds),
    ok.


leaderboard(Config) ->
    PrivDir = ?config(data_dir, Config),
    ClusterName = ?config(cluster_name, Config),
    ServerIds = [{ClusterName, start_follower(N, PrivDir)} || N <- [s1,s2,s3]],
    Machine = {module, ?MODULE, #{}},
    {ok, Started, []} = ra:start_cluster(?SYS, ClusterName, Machine, ServerIds),
    % assert all were said to be started
    [] = Started -- ServerIds,
    %% synchronously get leader
    {ok, _, Leader} = ra:members(hd(Started)),

    %% assert leaderboard has correct leader on all nodes
    await_condition(
      fun () ->
              lists:all(fun (B) -> B end,
                        [begin
                             L = rpc:call(N, ra_leaderboard, lookup_leader, [ClusterName]),
                             ct:pal("~w has ~w as leader expected ~w", [N, L, Leader]),
                             Leader == L
                         end || {_, N} <- ServerIds])
      end, 100),

    NextLeader = hd(lists:delete(Leader, Started)),
    ok = ra:transfer_leadership(Leader, NextLeader),
    {ok, _, NewLeader} = ra:members(hd(Started)),

    await_condition(
      fun () ->
              lists:all(fun (B) -> B end,
                        [begin
                             L = rpc:call(N, ra_leaderboard, lookup_leader, [ClusterName]),
                             ct:pal("~w has ~w as leader expected ~w", [N, L, Leader]),
                             NewLeader == L
                         end || {_, N} <- ServerIds])
      end, 100),

    stop_nodes(ServerIds),
    ok.

bench(Config) ->
    %% exercises the large message handling code
    PrivDir = ?config(data_dir, Config),
    Nodes = [start_follower(N, PrivDir) || N <- [s1,s2,s3]],
    ok = ra_bench:run(#{name => ?FUNCTION_NAME,
                        seconds => 10,
                        target => 500,
                        degree => 3,
                        data_size => 256 * 1000,
                        nodes => Nodes}),

    stop_nodes(Nodes),
    %% clean up
    ra_lib:recursive_delete(PrivDir),
    ok.

recover_from_checkpoint(Config) ->
    PrivDir = ?config(data_dir, Config),
    ClusterName = ?config(cluster_name, Config),
    ServerNames = [s1, s2, s3],
    ServerIds = [{ClusterName, start_follower(N, PrivDir)} || N <- ServerNames],
    Configs = [begin
                   UId = atom_to_binary(Name, utf8),
                   #{cluster_name => ClusterName,
                     id => NodeId,
                     uid => UId,
                     initial_members => ServerIds,
                     machine => {module, ?MODULE, #{}},
                     log_init_args => #{uid => UId,
                                        min_checkpoint_interval => 3,
                                        min_snapshot_interval => 5}}
               end || {Name, _Node} = NodeId <- ServerIds],
    {ok, Started, []} = ra:start_cluster(?SYS, Configs),
    {ok, _, Leader} = ra:members(hd(Started)),
    [Follower1, Follower2] = ServerIds -- [Leader],

    %% Send five commands to trigger a snapshot.
    [ok = ra:pipeline_command(Leader, N, no_correlation, normal)
     || N <- lists:seq(1, 6)],
    await_condition(
      fun () ->
              {ok, #{log := #{snapshot_index := LeaderIdx}}, _} =
                  ra:member_overview(Leader),
              {ok, #{log := #{snapshot_index := Follower1Idx}}, _} =
                  ra:member_overview(Follower1),
              {ok, #{log := #{snapshot_index := Follower2Idx}}, _} =
                  ra:member_overview(Follower2),
              LeaderIdx =:= 6 andalso Follower1Idx =:= 6 andalso
                Follower2Idx =:= 6
      end, 20),

    %% Trigger a checkpoint.
    {ok, _, _} = ra:process_command(Leader, checkpoint),
    await_condition(
      fun () ->
              {ok, #{log := #{latest_checkpoint_index := LeaderIdx}}, _} =
                  ra:member_overview(Leader),
              {ok, #{log := #{latest_checkpoint_index := Follower1Idx}}, _} =
                  ra:member_overview(Follower1),
              {ok, #{log := #{latest_checkpoint_index := Follower2Idx}}, _} =
                  ra:member_overview(Follower2),
              LeaderIdx =:= 8 andalso Follower1Idx =:= 8 andalso
                Follower2Idx =:= 8
      end, 20),
    CounterKeys = [
                   checkpoint_bytes_written,
                   checkpoint_index,
                   checkpoints,
                   checkpoints_written,
                   checkpoints_promoted
                  ],
    [begin
         ?assertMatch(
            #{
              checkpoint_bytes_written := B,
              checkpoint_index := 8,
              checkpoints := 1,
              checkpoints_written := 1,
              checkpoints_promoted := 0
             } when B > 0,
                    ct_rpc:call(N, ra_counters, counters,
                                [ServerId, CounterKeys]))
     end || {_, N} = ServerId <- ServerIds],


    %% Restart the servers
    [ok = ra:stop_server(?SYS, ServerId) || ServerId <- ServerIds],
    [ok = ra:restart_server(?SYS, ServerId) || ServerId <- ServerIds],

    %% All servers should have recovered from their checkpoints since the
    %% checkpoint has a higher index than the snapshot.
    [{ok, {_CurrentIdx, _CheckpointIdx = 8}, _Leader} =
       ra:local_query(ServerId, fun(State) ->
                                        maps:get(checkpoint_index, State,
                                                 undefined)
                                end) || ServerId <- ServerIds],

    %% Promote the checkpoint into a snapshot.
    {ok, _, _} = ra:process_command(Leader, promote_checkpoint),
    await_condition(
      fun () ->
              {ok, #{log := #{snapshot_index := LeaderIdx}}, _} =
                  ra:member_overview(Leader),
              {ok, #{log := #{snapshot_index := Follower1Idx}}, _} =
                  ra:member_overview(Follower1),
              {ok, #{log := #{snapshot_index := Follower2Idx}}, _} =
                  ra:member_overview(Follower2),
              LeaderIdx =:= 8 andalso Follower1Idx =:= 8 andalso
                Follower2Idx =:= 8
      end, 20),

    [begin
         ?assertMatch(
            #{
              checkpoint_bytes_written := B,
              checkpoint_index := 8,
              checkpoints := 1,
              checkpoints_written := 1,
              checkpoints_promoted := 1
             } when B > 0,
                    ct_rpc:call(N, ra_counters, counters,
                                [ServerId, CounterKeys]))
     end || {_, N} = ServerId <- ServerIds],
    %% Restart the servers: the servers should be able to recover from the
    %% snapshot which was promoted from a checkpoint.
    [ok = ra:stop_server(?SYS, ServerId) || ServerId <- ServerIds],
    [ok = ra:restart_server(?SYS, ServerId) || ServerId <- ServerIds],
    [{ok, {_CurrentIdx, _CheckpointIdx = 8}, _Leader} =
       ra:local_query(ServerId, fun(State) ->
                                        maps:get(checkpoint_index, State,
                                                 undefined)
                                end) || ServerId <- ServerIds],

    stop_nodes(ServerIds),
    ok.

segment_writer_or_wal_crash_follower(Config) ->
    %% this test crashes the segment writer for a follower node whilst the
    %% ra cluster is active and receiving and replicating commands.
    %% it tests the segment writer and wal is able to recover without the
    %% follower crashing.
    %% Finally we stop and restart the follower to make sure it can recover
    %% correactly and that the log data contains no missing entries
    PrivDir = ?config(data_dir, Config),
    ClusterName = ?config(cluster_name, Config),
    ServerNames = [s1, s2, s3],
    ServerIds = [{ClusterName, start_follower(N, PrivDir)} || N <- ServerNames],
    Configs = [begin
                   UId = atom_to_binary(Name, utf8),
                   #{cluster_name => ClusterName,
                     id => NodeId,
                     uid => UId,
                     initial_members => ServerIds,
                     machine => {module, ?MODULE, #{}},
                     log_init_args => #{uid => UId,
                                        min_snapshot_interval => 5}}
               end || {Name, _Node} = NodeId <- ServerIds],
    {ok, Started, []} = ra:start_cluster(?SYS, Configs),

    {ok, _, Leader} = ra:members(hd(Started)),
    [{FollowerName, FollowerNode} = Follower, _] = lists:delete(Leader, Started),

    Data = crypto:strong_rand_bytes(2_000),
    WriterFun = fun Recur() ->
                          {ok, _, _} = ra:process_command(Leader, {?FUNCTION_NAME, Data}),
                          receive
                              stop ->
                                  ok
                          after 1 ->
                                    Recur()
                          end
                end,
    FollowerPid = ct_rpc:call(FollowerNode, erlang, whereis, [FollowerName]),
    ?assert(is_pid(FollowerPid)),

    AwaitReplicated = fun () ->
                              LastIdxs =
                              [begin
                                   {ok, #{current_term := T,
                                          log := #{last_index := L,
                                                   cache_size := 0}}, _} =
                                   ra:member_overview(S),
                                   {T, L}
                               end || {_, _N} = S <- ServerIds],
                              1 == length(lists:usort(LastIdxs))
                      end,
    [begin
         ct:pal("running iteration ~b", [I]),

         WriterPid = spawn(WriterFun),
         timer:sleep(rand:uniform(500) + 5_000),

         case I rem 2 == 0 of
             true ->
                 ct:pal("killing segment writer"),
                 _ = ct_rpc:call(FollowerNode, ra_log_wal, force_rollover, [ra_log_wal]),
                 timer:sleep(10),
                 Pid = ct_rpc:call(FollowerNode, erlang, whereis, [ra_log_segment_writer]),
                 true = ct_rpc:call(FollowerNode, erlang, exit, [Pid, kill]);
             false ->
                 ct:pal("killing wal"),
                 Pid = ct_rpc:call(FollowerNode, erlang, whereis, [ra_log_wal]),
                 true = ct_rpc:call(FollowerNode, erlang, exit, [Pid, kill])
         end,


         timer:sleep(1000),
         WriterPid ! stop,
         await_condition(fun () -> not is_process_alive(WriterPid) end, 1000),

         %% assert stuff
         await_condition(AwaitReplicated, 100),
         %% follower hasn't crashed
         ?assertEqual(FollowerPid, ct_rpc:call(FollowerNode, erlang, whereis,
                                               [FollowerName]))
     end || I <- lists:seq(1, 10)],

    %% stop and restart the follower
    ok = ra:stop_server(Follower),
    ok = ra:restart_server(Follower),

    await_condition(AwaitReplicated, 100),

    _ = ct_rpc:call(FollowerNode, ra_log_wal, force_rollover, [ra_log_wal]),

    ok = ra:stop_server(Follower),
    ok = ra:restart_server(Follower),

    await_condition(AwaitReplicated, 100),

    stop_nodes(ServerIds),
    ok.

segment_writer_or_wal_crash_leader(Config) ->
    %% This test crashes the segment writer for a follower node whilst the
    %% ra cluster is active and receiving and replicating commands.
    %% It tests the segment writer and wal are able to recover without the
    %% follower crashing.
    %% Finally we stop and restart the follower to make sure it can recover
    %% correctly and that the log data does not miss any entries
    PrivDir = ?config(data_dir, Config),
    ClusterName = ?config(cluster_name, Config),
    ServerNames = [s1, s2, s3],
    ServerIds = [{ClusterName, start_follower(N, PrivDir)} || N <- ServerNames],
    Configs = [begin
                   UId = atom_to_binary(Name, utf8),
                   #{cluster_name => ClusterName,
                     id => NodeId,
                     uid => UId,
                     initial_members => ServerIds,
                     machine => {module, ?MODULE, #{}},
                     log_init_args => #{uid => UId,
                                        min_snapshot_interval => 5}}
               end || {Name, _Node} = NodeId <- ServerIds],
    {ok, Started, []} = ra:start_cluster(?SYS, Configs),


    {ok, _, Leader} = ra:members(hd(Started)),
    % [{FollowerName, FollowerNode} = Follower, _] = lists:delete(Leader, Started),
    {LeaderName, LeaderNode} = Leader,

    Data = crypto:strong_rand_bytes(2_000),
    WriterFun = fun Recur(Leader_) ->
                        NextLeader =
                          case ra:process_command(Leader_, {?FUNCTION_NAME, Data}, 1000) of
                              {timeout, _} ->
                                  timer:sleep(1000),
                                  Leader_;
                              {error, _} ->
                                  timer:sleep(1000),
                                  Leader_;
                              {ok, _, L} ->
                                  L
                          end,
                          receive
                              stop ->
                                  ok
                          after 1 ->
                                    Recur(NextLeader)
                          end
                end,
    LeaderPid = ct_rpc:call(LeaderNode, erlang, whereis, [LeaderName]),
    ?assert(is_pid(LeaderPid)),

    AwaitReplicated = fun () ->
                              LastIdxs =
                              [begin
                                   {ok, #{current_term := T,
                                          log := #{last_index := L,
                                                   last_written_index_term := {L, _}}},
                                    _} =
                                   ra:member_overview(S),
                                   {T, L}
                               end || {_, _N} = S <- ServerIds],
                              1 == length(lists:usort(LastIdxs))
                      end,
    [begin
         ct:pal("running iteration ~b", [I]),

         WriterPid = spawn_link(fun () -> WriterFun(Leader) end),
         timer:sleep(rand:uniform(500) + 5_000),

         case I rem 2 == 0 of
             true ->
                 ct:pal("killing segment writer"),
                 _ = ct_rpc:call(LeaderNode, ra_log_wal, force_rollover, [ra_log_wal]),
                 timer:sleep(10),
                 Pid = ct_rpc:call(LeaderNode, erlang, whereis, [ra_log_segment_writer]),
                 true = ct_rpc:call(LeaderNode, erlang, exit, [Pid, kill]);
             false ->
                 ct:pal("killing wal"),
                 Pid = ct_rpc:call(LeaderNode, erlang, whereis, [ra_log_wal]),
                 true = ct_rpc:call(LeaderNode, erlang, exit, [Pid, kill])
         end,


         timer:sleep(1000),
         WriterPid ! stop,
         await_condition(fun () -> not is_process_alive(WriterPid) end, 1000),

         %% assert stuff
         await_condition(AwaitReplicated, 100),
         ?assertMatch({ok, #{log := #{cache_size := 0}}, _},
                      ra:member_overview(Leader)),
         %% follower hasn't crashed
         ?assertEqual(LeaderPid, ct_rpc:call(LeaderNode, erlang, whereis,
                                               [LeaderName]))
     end || I <- lists:seq(1, 10)],

    %% stop and restart the follower
    ok = ra:stop_server(Leader),
    ok = ra:restart_server(Leader),

    await_condition(AwaitReplicated, 100),

    _ = ct_rpc:call(LeaderNode, ra_log_wal, force_rollover, [ra_log_wal]),

    ok = ra:stop_server(Leader),
    ok = ra:restart_server(Leader),

    await_condition(AwaitReplicated, 100),

    stop_nodes(ServerIds),
    ok.

server_recovery_strategy(Config) ->
    PrivDir = ?config(data_dir, Config),
    ClusterName = ?config(cluster_name, Config),
    ServerNames = [s1, s2, s3],
    SysCfg = #{server_recovery_strategy => registered},
    ServerIds = [{ClusterName, start_follower(N, PrivDir, SysCfg)}
                 || N <- ServerNames],
    Configs = [begin
                   UId = atom_to_binary(Name, utf8),
                   #{cluster_name => ClusterName,
                     id => NodeId,
                     uid => UId,
                     initial_members => ServerIds,
                     machine => {module, ?MODULE, #{}},
                     log_init_args => #{uid => UId,
                                        min_snapshot_interval => 5}}
               end || {Name, _Node} = NodeId <- ServerIds],
    {ok, Started, []} = ra:start_cluster(?SYS, Configs),

    {ok, _, {LeaderName, LeaderNode}} = ra:members(hd(Started)),

    LeaderPid = ct_rpc:call(LeaderNode, erlang, whereis, [LeaderName]),
    %% killing this process will take the system down
    RaLogEtsPid  = ct_rpc:call(LeaderNode, erlang, whereis, [ra_log_ets]),
    true = ct_rpc:call(LeaderNode, erlang, exit, [RaLogEtsPid, kill]),

    await_condition(
      fun () ->
              Pid = ct_rpc:call(LeaderNode, erlang, whereis, [LeaderName]),
              Pid =/= undefined andalso Pid =/= LeaderPid
      end, 100),

    timer:sleep(100),

    stop_nodes(ServerIds),
    ok.

stopped_wal_causes_leader_change_registered(Config) ->
    stopped_wal_causes_leader_change(Config, registered).

stopped_wal_causes_leader_change_mfa(Config) ->
    stopped_wal_causes_leader_change(Config, {?MODULE, server_recover_function, []}).

stopped_wal_causes_leader_change(Config, RecoverStrat) ->
    PrivDir = ?config(data_dir, Config),
    ClusterName = ?config(cluster_name, Config),
    ServerNames = [s1, s2, s3],
    SysCfg = #{server_recovery_strategy => RecoverStrat},
    ServerIds = [{ClusterName, start_follower(N, PrivDir, SysCfg)}
                 || N <- ServerNames],
    Configs = [begin
                   UId = atom_to_binary(Name, utf8),
                   #{cluster_name => ClusterName,
                     id => NodeId,
                     uid => UId,
                     initial_members => ServerIds,
                     machine => {module, ?MODULE, #{}},
                     log_init_args => #{uid => UId,
                                        min_snapshot_interval => 5}}
               end || {Name, _Node} = NodeId <- ServerIds],
    {ok, Started, []} = ra:start_cluster(?SYS, Configs),


    {ok, _, Leader} = ra:members(hd(Started)),
    [Follower, _] = lists:delete(Leader, Started),
    {LeaderName, LeaderNode} = Leader,

    Data = crypto:strong_rand_bytes(2_000),
    WriterFun = fun Recur(Leader_) ->
                        NextLeader =
                          case ra:process_command(Leader_, {?FUNCTION_NAME, Data}, 1000) of
                              {timeout, _} ->
                                  timer:sleep(1000),
                                  Leader_;
                              {error, _} ->
                                  timer:sleep(1000),
                                  Leader_;
                              {ok, _, L} ->
                                  L
                          end,
                          receive
                              stop ->
                                  ok
                          after 1 ->
                                    Recur(NextLeader)
                          end
                end,
    LeaderPid = ct_rpc:call(LeaderNode, erlang, whereis, [LeaderName]),
    ?assert(is_pid(LeaderPid)),

    AwaitReplicated = fun () ->
                              LastIdxs =
                              [begin
                                   {ok, #{current_term := T,
                                          log := #{last_index := L}}, _} =
                                   ra:member_overview(S),
                                   {T, L}
                               end || {_, _N} = S <- ServerIds],
                              1 == length(lists:usort(LastIdxs))
                      end,

    _WriterPid = spawn_link(fun () -> WriterFun(Leader) end),
    timer:sleep(2000),

    %% kill the wal until the system crashes and the current member is terminated
    %% and another leader is elected
    #{term := Term} = ra:key_metrics(Follower),
    await_condition(fun () ->
                            WalPid = ct_rpc:call(LeaderNode, erlang, whereis,
                                                 [ra_log_wal]),
                            true = ct_rpc:call(LeaderNode, erlang, exit,
                                               [WalPid, kill]),
                            #{term := T} = ra:key_metrics(Follower),
                            T > Term andalso
                            (begin
                                 P = ct_rpc:call(LeaderNode, erlang, whereis, [LeaderName]),%                      [ra_log_wal]),
                                 is_pid(P) andalso P =/= LeaderPid
                             end)
                    end, 200),
    await_condition(AwaitReplicated, 100),
    stop_nodes(ServerIds),
    ok.

%% Utility

test_local_msg(Leader, ReceiverNode, ExpectedSenderNode, CmdTag, Opts0) ->
    Opts = case Opts0 of
               local -> [local];
               _ -> lists:sort(Opts0)
           end,
    Self = self(),
    ReceiveFun = fun () ->
                         erlang:register(receiver_proc, self()),
                         receive
                             {'$gen_cast', {local_msg, Node}} ->
                                 %% assert options match received message
                                 %% structure
                                 [cast, local] = Opts,
                                 Self ! {got_it, Node};
                             {local_msg, Node} ->
                                 [local] = Opts,
                                 Self ! {got_it, Node};
                             {ra_event, _, {machine, {local_msg, Node}}} ->
                                 [local, ra_event] = Opts,
                                 Self ! {got_it, Node};
                             {'$gen_cast',
                              {ra_event, _, {machine, {local_msg, Node}}}} ->
                                 [cast, local, ra_event] = Opts,
                                 Self ! {got_it, Node};
                             Msg ->
                                 Self ! {unexpected_msg, Msg}
                         after 2000 ->
                                   exit(blah)
                         end
                 end,
    ReceivePid = spawn(ReceiverNode, ReceiveFun),
    ra:pipeline_command(Leader, {CmdTag, ReceivePid, Opts0}),
    %% the leader should send local deliveries if there is no local member
    receive
        {got_it, ExpectedSenderNode} -> ok
    after 3000 ->
              flush(),
              exit(got_it_timeout)
    end,

    _ = spawn(ReceiverNode, ReceiveFun),
    ra:pipeline_command(Leader, {send_local_msg, {receiver_proc, ReceiverNode},
                                 Opts0}),
    %% the leader should send local deliveries if there is no local member
    receive
        {got_it, ExpectedSenderNode} -> ok
    after 3000 ->
              flush(),
              exit(got_it_timeout2)
    end,
    flush(),
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

start_follower(N, PrivDir) ->
    start_follower(N, PrivDir, #{}).

start_follower(N, PrivDir, SysCfg) ->
    Dir0 = filename:join(PrivDir, N),
    Dir = "'\"" ++ Dir0 ++ "\"'",
    Host = get_current_host(),
    Pa = string:join(["-pa" | search_paths()] ++ ["-ra data_dir", Dir], " "),
    ct:pal("starting child node with ~ts on host ~ts for node ~ts",
           [Pa, Host, node()]),
    {ok, S} = slave:start_link(Host, N, Pa),
    ct:pal("started child node ~s", [S]),
    ok = ct_rpc:call(S, ?MODULE, node_setup, [Dir0]),
    % ok = ct_rpc:call(S, logger, set_primary_config,
    %                  [level, all]),
    _ = ct_cover:add_nodes([S]),
    {ok, _} = ct_rpc:call(S, ?MODULE, ra_start, [[], SysCfg]),
    S.

ra_start(Params, SysCfg) when is_map(SysCfg) ->
    _ = application:stop(ra),
    _ = application:load(ra),
    [ok = application:set_env(ra, Param, Value)
     || {Param, Value} <- Params],
    {ok, _} = application:ensure_all_started(ra),
    Cfg = maps:merge(SysCfg, ra_system:default_config()),
    ra_system:start(Cfg).

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
    #{}.

apply(_Meta, {send_local_msg, Pid, Opts}, State) ->
    {State, ok, [{send_msg, Pid, {local_msg, node()}, Opts}]};
apply(#{index := Idx}, {do_local_log, SenderPid, Opts}, State) ->
    Eff = {log, [Idx],
           fun([{do_local_log, Pid, _}]) ->
                   [{send_msg, Pid, {local_msg, node()}, Opts}]
           end,
           {local, node(SenderPid)}},
    {State, ok, [Eff]};
apply(#{index := _Idx}, {data, _}, State) ->
    {State, ok, []};
apply(#{index := Idx}, checkpoint, State) ->
    %% Generally machines should save their state without any modifications
    %% but we slightly modify the machine state we save in the checkpoint here
    %% so that we can tell when we've recovered from a checkpoint rather than
    %% a snapshot.
    CheckpointState = maps:put(checkpoint_index, Idx, State),
    {State, ok, [{checkpoint, Idx, CheckpointState}]};
apply(#{index := Idx}, promote_checkpoint, State) ->
    {State, ok, [{release_cursor, Idx}]};
apply(#{index := _Idx}, {segment_writer_or_wal_crash_leader, _}, State) ->
    {State, ok, []};
apply(#{index := _Idx}, {segment_writer_or_wal_crash_follower, _}, State) ->
    {State, ok, []};
apply(#{index := Idx}, _Cmd, State) ->
    {State, ok, [{release_cursor, Idx, State}]}.

snapshot_installed(#{machine_version := _,
                     index := Idx,
                     term := _,
                     cluster := Cluster} = Meta,
                   _State,
                   #{machine_version := _,
                     index := OldIdx,
                     term := _,
                     cluster := OldCluster} = _OldMeta,
                   _OldState)
  when is_map(OldCluster) andalso
       is_map(Cluster) andalso
       Idx > OldIdx ->
    case whereis(snapshot_installed_proc) of
        undefined ->
            [];
        Pid ->
            [{send_msg, Pid, {snapshot_installed, Meta}}]
    end.

node_setup(DataDir) ->
    ok = ra_lib:make_dir(DataDir),
    % NodeDir = filename:join(DataDir, atom_to_list(node())),
    % ok = ra_lib:make_dir(DataDir),
    LogFile = filename:join(DataDir, "ra.log"),
    SaslFile = filename:join(DataDir, "ra_sasl.log"),
    logger:set_primary_config(level, debug),
    Config = #{config => #{file => LogFile}},
    logger:add_handler(ra_handler, logger_std_h, Config),
    application:load(sasl),
    application:set_env(sasl, sasl_error_logger, {file, SaslFile}),
    application:stop(sasl),
    application:start(sasl),
    _ = error_logger:tty(false),
    ok.

await_condition(_Fun, 0) ->
    ct:fail(condition_did_not_materialise);
await_condition(Fun, Attempts) ->
    case catch Fun() of
        true -> ok;
        _Reason ->
            % ct:pal("await_condition retry with ~p", [Reason]),
            timer:sleep(100),
            await_condition(Fun, Attempts - 1)
    end.

server_recover_function(System) ->
    Regd = ra_directory:list_registered(System),
    ?INFO("~s: ra system '~ts' num servers ~b",
          [?MODULE, System, length(Regd)]),
    [begin
         case ra:restart_server(System, {N, node()}) of
             ok ->
                 ok;
             Err ->
                 ?WARN("~s: ra:restart_server/2 failed with ~p",
                       [?MODULE, Err]),
                 ok
         end
     end || {N, _Uid} <- Regd],
    ok.

stop_nodes([{_, _} | _ ] = ServerIds) ->
    stop_nodes([S || {_, S} <- ServerIds]);
stop_nodes(Nodes) ->
    _ = ct_cover:remove_nodes(Nodes),
    [ok = slave:stop(S) || S <- Nodes],
    ok.
