%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2017-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%
-module(coordination_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-export([
         ]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(info, true).
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
     start_stop_restart_delete_on_remote,
     start_cluster,
     start_or_restart_cluster,
     delete_one_server_cluster,
     delete_two_server_cluster,
     delete_three_server_cluster,
     delete_three_server_cluster_parallel,
     start_cluster_majority,
     start_cluster_minority,
     send_local_msg,
     local_log_effect,
     leaderboard,
     bench
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
    NodeId = {c1, S1},
    Conf = conf(NodeId, [NodeId]),
    ok = ra:start_server(?SYS, Conf),
    ok = ra:trigger_election(NodeId),
    % idempotency
    {error, {already_started, _}} = ra:start_server(?SYS, Conf),
    ok = ra:stop_server(?SYS, NodeId),
    ok = ra:restart_server(?SYS, NodeId),
    % idempotency
    {error, {already_started, _}} = ra:restart_server(?SYS, NodeId),
    ok = ra:stop_server(?SYS, NodeId),
    % idempotency
    ok = ra:stop_server(?SYS, NodeId),
    ok = ra:force_delete_server(?SYS, NodeId),
    % idempotency
    ok = ra:force_delete_server(?SYS, NodeId),
    timer:sleep(500),
    slave:stop(S1),
    ok.

start_cluster(Config) ->
    PrivDir = ?config(data_dir, Config),
    ClusterName = ?config(cluster_name, Config),
    NodeIds = [{ClusterName, start_follower(N, PrivDir)} || N <- [s1,s2,s3]],
    Machine = {module, ?MODULE, #{}},
    {ok, Started, []} = ra:start_cluster(?SYS, ClusterName, Machine, NodeIds),
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
    ClusterName = ?config(cluster_name, Config),
    NodeIds = [{ClusterName, start_follower(N, PrivDir)} || N <- [s1,s2,s3]],
    Machine = {module, ?MODULE, #{}},
    %% this should start
    {ok, Started, []} = ra:start_or_restart_cluster(?SYS, ClusterName, Machine,
                                                    NodeIds),
    % assert all were said to be started
    [] = Started -- NodeIds,
    % assert all nodes are actually started
    PingResults = [{pong, _} = ra_server_proc:ping(N, 500) || N <- NodeIds],
    % assert one node is leader
    ?assert(lists:any(fun ({pong, S}) -> S =:= leader end, PingResults)),
    % timer:sleep(1000),
    [ok = slave:stop(S) || {_, S} <- NodeIds],
    NodeIds = [{ClusterName, start_follower(N, PrivDir)} || N <- [s1,s2,s3]],
    %% this should restart
    {ok, Started2, []} = ra:start_or_restart_cluster(?SYS, ClusterName, Machine,
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
    ClusterName = ?config(cluster_name, Config),
    NodeIds = [{ClusterName, start_follower(N, PrivDir)} || N <- [s1]],
    Machine = {module, ?MODULE, #{}},
    {ok, _, []} = ra:start_cluster(?SYS, ClusterName, Machine, NodeIds),
    [{_, Node}] = NodeIds,
    UId = rpc:call(Node, ra_directory, uid_of, [ClusterName]),
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
    NodeIds = [{ClusterName, start_follower(N, PrivDir)} || N <- [s1]],
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
    [ok = slave:stop(S) || {_, S} <- NodeIds],
    ok.

delete_two_server_cluster(Config) ->
    PrivDir = ?config(data_dir, Config),
    ClusterName = ?config(cluster_name, Config),
    NodeIds = [{ClusterName, start_follower(N, PrivDir)} || N <- [s1,s2]],
    Machine = {module, ?MODULE, #{}},
    {ok, _, []} = ra:start_cluster(?SYS, ClusterName, Machine, NodeIds),
    {ok, _} = ra:delete_cluster(NodeIds),
    timer:sleep(1000),
    {error, _} = ra_server_proc:ping(hd(tl(NodeIds)), 50),
    {error, _} = ra_server_proc:ping(hd(NodeIds), 50),
    % assert all nodes are actually started
    [ok = slave:stop(S) || {_, S} <- NodeIds],
    receive
        Anything ->
            ct:pal("got wierd message ~p", [Anything]),
            exit({unexpected, Anything})
    after 250 ->
              ok
    end,
    ok.

delete_three_server_cluster(Config) ->
    PrivDir = ?config(data_dir, Config),
    ClusterName = ?config(cluster_name, Config),
    NodeIds = [{ClusterName, start_follower(N, PrivDir)} || N <- [s1,s2,s3]],
    Machine = {module, ?MODULE, #{}},
    {ok, _, []} = ra:start_cluster(?SYS, ClusterName, Machine, NodeIds),
    {ok, _} = ra:delete_cluster(NodeIds),
    timer:sleep(250),
    {error, _} = ra_server_proc:ping(hd(tl(NodeIds)), 50),
    {error, _} = ra_server_proc:ping(hd(NodeIds), 50),
    % assert all nodes are actually started
    [ok = slave:stop(S) || {_, S} <- NodeIds],
    ok.

delete_three_server_cluster_parallel(Config) ->
    PrivDir = ?config(data_dir, Config),
    ClusterName = ?config(cluster_name, Config),
    NodeIds = [{ClusterName, start_follower(N, PrivDir)} || N <- [s1,s2,s3]],
    Machine = {module, ?MODULE, #{}},
    {ok, _, []} = ra:start_cluster(?SYS, ClusterName, Machine, NodeIds),
    %% spawn a delete command to try cause it to commit more than
    %% one delete command
    spawn(fun () -> {ok, _} = ra:delete_cluster(NodeIds) end),
    spawn(fun () -> {ok, _} = ra:delete_cluster(NodeIds) end),
    {ok, _} = ra:delete_cluster(NodeIds),
    timer:sleep(250),
    {error, _} = ra_server_proc:ping(hd(tl(NodeIds)), 50),
    {error, _} = ra_server_proc:ping(hd(NodeIds), 50),
    [begin
         true = rpc:call(S, ?MODULE, check_sup, [])
     end || {_, S} <- NodeIds],
    % assert all nodes are actually started
    [ok = slave:stop(S) || {_, S} <- NodeIds],
    ok.

check_sup() ->
    [] == supervisor:which_children(ra_server_sup_sup).

start_cluster_majority(Config) ->
    PrivDir = ?config(data_dir, Config),
    ClusterName = ?config(cluster_name, Config),
    NodeIds0 = [{ClusterName, start_follower(N, PrivDir)} || N <- [s1,s2]],
    % s3 isn't available
    S3 = make_node_name(s3),
    NodeIds = NodeIds0 ++ [{ClusterName, S3}],
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
    [ok = slave:stop(S) || {_, S} <- NodeIds0],
    ok.

start_cluster_minority(Config) ->
    PrivDir = ?config(data_dir, Config),
    ClusterName = ?config(cluster_name, Config),
    NodeIds0 = [{ClusterName, start_follower(N, PrivDir)} || N <- [s1]],
    % s3 isn't available
    S2 = make_node_name(s2),
    S3 = make_node_name(s3),
    NodeIds = NodeIds0 ++ [{ClusterName, S2}, {ClusterName, S3}],
    Machine = {module, ?MODULE, #{}},
    {error, cluster_not_formed} =
        ra:start_cluster(?SYS, ClusterName, Machine, NodeIds),
    % assert none is started
    [{error, _} = ra_server_proc:ping(N, 50) || N <- NodeIds],
    [ok = slave:stop(S) || {_, S} <- NodeIds0],
    ok.

send_local_msg(Config) ->
    PrivDir = ?config(data_dir, Config),
    ClusterName = ?config(cluster_name, Config),
    NodeIds = [{ClusterName, start_follower(N, PrivDir)} || N <- [s1,s2,s3]],
    Machine = {module, ?MODULE, #{}},
    {ok, Started, []} = ra:start_cluster(?SYS, ClusterName, Machine, NodeIds),
    % assert all were said to be started
    [] = Started -- NodeIds,
    %% spawn a receiver process on one node
    {ok, _, Leader} = ra:members(hd(NodeIds)),
    %% select a non-leader node to spawn on
    [{_, N} | _] = lists:delete(Leader, NodeIds),
    test_local_msg(Leader, N, N, send_local_msg, local),
    test_local_msg(Leader, N, N, send_local_msg, [local, ra_event]),
    test_local_msg(Leader, N, N, send_local_msg, [local, cast]),
    test_local_msg(Leader, N, N, send_local_msg, [local, cast, ra_event]),
    {_, LeaderNode} = Leader,
    test_local_msg(Leader, node(), LeaderNode, send_local_msg, local),
    test_local_msg(Leader, node(), LeaderNode, send_local_msg, [local, ra_event]),
    test_local_msg(Leader, node(), LeaderNode, send_local_msg, [local, cast]),
    test_local_msg(Leader, node(), LeaderNode, send_local_msg, [local, cast, ra_event]),
    %% test the same but for a local pid (non-member)
    [ok = slave:stop(S) || {_, S} <- NodeIds],
    ok.

local_log_effect(Config) ->
    PrivDir = ?config(data_dir, Config),
    ClusterName = ?config(cluster_name, Config),
    NodeIds = [{ClusterName, start_follower(N, PrivDir)} || N <- [s1,s2,s3]],
    Machine = {module, ?MODULE, #{}},
    {ok, Started, []} = ra:start_cluster(?SYS, ClusterName, Machine, NodeIds),
    % assert all were said to be started
    [] = Started -- NodeIds,
    %% spawn a receiver process on one node
    {ok, _, Leader} = ra:members(hd(NodeIds)),
    %% select a non-leader node to spawn on
    [{_, N} | _] = lists:delete(Leader, NodeIds),
    test_local_msg(Leader, N, N, do_local_log, local),
    test_local_msg(Leader, N, N, do_local_log, [local, ra_event]),
    test_local_msg(Leader, N, N, do_local_log, [local, cast]),
    test_local_msg(Leader, N, N, do_local_log, [local, cast, ra_event]),
    {_, LeaderNode} = Leader,
    test_local_msg(Leader, node(), LeaderNode, do_local_log, local),
    test_local_msg(Leader, node(), LeaderNode, do_local_log, [local, ra_event]),
    test_local_msg(Leader, node(), LeaderNode, do_local_log, [local, cast]),
    test_local_msg(Leader, node(), LeaderNode, do_local_log, [local, cast, ra_event]),
    %% test the same but for a local pid (non-member)
    [ok = slave:stop(S) || {_, S} <- NodeIds],
    ok.

leaderboard(Config) ->
    PrivDir = ?config(data_dir, Config),
    ClusterName = ?config(cluster_name, Config),
    NodeIds = [{ClusterName, start_follower(N, PrivDir)} || N <- [s1,s2,s3]],
    Machine = {module, ?MODULE, #{}},
    {ok, Started, []} = ra:start_cluster(?SYS, ClusterName, Machine, NodeIds),
    % assert all were said to be started
    [] = Started -- NodeIds,
    %% synchronously get leader
    {ok, _, Leader} = ra:members(hd(Started)),

    timer:sleep(500),
    %% assert leaderboard has correct leader on all nodes
    [begin
         L = rpc:call(N, ra_leaderboard, lookup_leader, [ClusterName]),
         ct:pal("~w has ~w as leader expected ~w", [N, L, Leader]),
         ?assertEqual(Leader, L)
     end || {_, N} <- NodeIds],

    NextLeader = hd(lists:delete(Leader, Started)),
    ok = ra:transfer_leadership(Leader, NextLeader),
    {ok, _, NewLeader} = ra:members(hd(Started)),

    timer:sleep(500),
    [begin
         L = rpc:call(N, ra_leaderboard, lookup_leader, [ClusterName]),
         ct:pal("~w has ~w as leader expected ~w", [N, L, NewLeader]),
         ?assertEqual(NewLeader, L)
     end || {_, N} <- NodeIds],

    [ok = slave:stop(S) || {_, S} <- NodeIds],
    ok.

bench(Config) ->
    %% exercies the large message handling code
    PrivDir = ?config(data_dir, Config),
    Nodes = [start_follower(N, PrivDir) || N <- [s1,s2,s3]],
    ok = ra_bench:run(#{name => ?FUNCTION_NAME,
                        seconds => 10,
                        target => 500,
                        degree => 3,
                        data_size => 256 * 1000,
                        nodes => Nodes}),
    [begin
         ok = slave:stop(N)
     end || N <- Nodes],
    %% clean up
    ra_lib:recursive_delete(PrivDir),
    ok.

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

start_follower(N, PrivDir) ->
    Dir0 = filename:join(PrivDir, N),
    Dir = "'\"" ++ Dir0 ++ "\"'",
    Host = get_current_host(),
    Pa = string:join(["-pa" | search_paths()] ++ ["-s ra -ra data_dir", Dir], " "),
    ct:pal("starting secondary node with ~s on host ~s for node ~s", [Pa, Host, node()]),
    {ok, S} = slave:start_link(Host, N, Pa),
    _ = rpc:call(S, ra, start, []),
    ok = ct_rpc:call(S, logger, set_primary_config,
                     [level, all]),
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

