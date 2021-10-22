%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2017-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%
-module(ra_SUITE).

-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include("src/ra.hrl").

-define(PROCESS_COMMAND_TIMEOUT, 6000).
-define(SYS, default).

all() ->
    [
     {group, tests}
    ].

all_tests() ->
    [
     single_server_processes_command,
     pipeline_commands,
     stop_server_idemp,
     minority,
     start_servers,
     server_recovery,
     process_command,
     pipeline_command,
     pipeline_command_reject,
     pipeline_command_2_forwards_to_leader,
     local_query,
     local_query_boom,
     local_query_stale,
     members,
     consistent_query,
     consistent_query_stale,
     server_catches_up,
     snapshot_installation,
     snapshot_installation_with_call_crash,
     add_member,
     queue_example,
     ramp_up_and_ramp_down,
     start_and_join_then_leave_and_terminate,
     leader_steps_down_after_replicating_new_cluster,
     stop_leader_and_wait_for_elections,
     follower_catchup,
     post_partition_liveness,
     all_metrics_are_integers,
     transfer_leadership
    ].

groups() ->
    [
     {tests, [], all_tests()}
    ].

suite() -> [{timetrap, {seconds, 120}}].

init_per_suite(Config) ->
    ok = logger:set_primary_config(level, all),
    Config.

end_per_suite(Config) ->
    application:stop(ra),
    Config.

restart_ra(DataDir) ->
    ok = application:set_env(ra, segment_max_entries, 128),
    {ok, _} = ra:start_in(DataDir),
    ok.

init_per_group(_G, Config) ->
    PrivDir = ?config(priv_dir, Config),
    DataDir = filename:join([PrivDir, "data"]),
    ok = restart_ra(DataDir),
    % ok = logger:set_application_level(ra, all),
    Config.

end_per_group(_, Config) ->
    Config.

init_per_testcase(TestCase, Config) ->
    [{test_name, ra_lib:to_list(TestCase)} | Config].

end_per_testcase(_TestCase, Config) ->
    ra_server_sup_sup:remove_all(default),
    Config.

single_server_processes_command(Config) ->
    % ok = logger:set_primary_config(level, all),
    Name = ?config(test_name, Config),
    N1 = nth_server_name(Config, 1),
    ok = ra:start_server(default, Name, N1, add_machine(), []),
    ok = ra:trigger_election(N1),
    % index is 2 as leaders commit a no-op entry on becoming leaders
    {ok, 5, _} = ra:process_command(N1, 5, 2000),
    {ok, 10, _} = ra:process_command(N1, 5, 2000),
    terminate_cluster([N1]).

pipeline_commands(Config) ->
    Name = ?config(test_name, Config),
    N1 = nth_server_name(Config, 1),
    ok = ra:start_server(default, Name, N1, add_machine(), []),
    ok = ra:trigger_election(N1),
    _ = ra:members(N1),
    C1 = make_ref(),
    C2 = make_ref(),
    % index is 2 as leaders commit a no-op entry on becoming leaders
    ok = ra:pipeline_command(N1, 5, C1, normal),
    ok = ra:pipeline_command(N1, 5, C2, normal),
    [{C1, 5}, {C2, 10}] = gather_applied([], 125),
    terminate_cluster([N1]).

stop_server_idemp(Config) ->
    Name = ?config(test_name, Config),
    N1 = nth_server_name(Config, 1),
    ok = ra:start_server(default, Name, N1, add_machine(), []),
    ok = ra:trigger_election(N1),
    timer:sleep(100),
    ok = ra:stop_server(?SYS, N1),
    % should not raise exception
    ok = ra:stop_server(?SYS, N1),
    {error, nodedown} = ra:stop_server(?SYS, {element(1, N1), random@node}),
    ok.

leader_steps_down_after_replicating_new_cluster(Config) ->
    N1 = nth_server_name(Config, 1),
    N2 = nth_server_name(Config, 2),
    N3 = nth_server_name(Config, 3),
    ok = new_server(N1, Config),
    ok = ra:trigger_election(N1),
    Leader = issue_op(N1, 5),
    validate_state_on_node(Leader, 5),
    ok = start_and_join(N1, N2),
    Leader = issue_op(Leader, 5),
    validate_state_on_node(N1, 10),
    ok = start_and_join(Leader, N3),
    Leader = issue_op(Leader, 5),
    validate_state_on_node(Leader, 15),
    % allow N3 some time to catch up
    timer:sleep(100),
    % Remove the leader node.
    % The leader should replicate the new cluster config
    % before stepping down, then shut itself down.
    ok = remove_member(Leader),
    timer:sleep(500),
    % we no longer can issue any new commands to this decommissioned leader
    {error, noproc} = ra:process_command(Leader, 5, 2000),
    % but we can do that to the remaining cluster members
    _ = issue_op(N2, 5),
    validate_state_on_node(N2, 20),
    terminate_cluster([N2, N3]).


start_and_join_then_leave_and_terminate(Config) ->
    N1 = nth_server_name(Config, 1),
    N2 = nth_server_name(Config, 2),
    % form a cluster
    ok = new_server(N1, Config),
    ok = ra:trigger_election(N1),
    _ = issue_op(N1, 5),
    validate_state_on_node(N1, 5),
    ok = start_and_join(N1, N2),
    _ = issue_op(N2, 5),
    validate_state_on_node(N2, 10),
    % safe server removal
    ok = ra:leave_and_terminate(default, N1, N2),
    validate_state_on_node(N1, 10),
    terminate_cluster([N1]),
    ok.

ramp_up_and_ramp_down(Config) ->
    N1 = nth_server_name(Config, 1),
    N2 = nth_server_name(Config, 2),
    N3 = nth_server_name(Config, 3),
    ok = new_server(N1, Config),
    ok = ra:trigger_election(N1),
    _ = issue_op(N1, 5),
    validate_state_on_node(N1, 5),

    ok = start_and_join(N1, N2),
    _ = issue_op(N2, 5),
    validate_state_on_node(N2, 10),

    ok = start_and_join(N1, N3),
    _ = issue_op(N3, 5),
    validate_state_on_node(N3, 15),

    ok = ra:leave_and_terminate(default, N3, N3),
    _ = issue_op(N2, 5),
    validate_state_on_node(N2, 20),

    % This is dangerous territory.
    % For the cluster change, we need a quorum from the server that is to be removed.
    % if we stop the server before removing it from the cluster
    % configuration this cluster becomes non-functional.
    ok = remove_member(N2),
    % A longish sleep here simulates a server that has been removed but not
    % shut down and thus may start issuing request_vote_rpcs.
    timer:sleep(1000),
    ok = stop_server(N2),
    _ = issue_op(N1, 5),
    validate_state_on_node(N1, 25),
    %% Stop and restart to ensure membership changes can be recovered.
    ok = stop_server(N1),
    ok = ra:restart_server(?SYS, N1),
    _ = issue_op(N1, 5),
    validate_state_on_node(N1, 30),
    terminate_cluster([N1]).

minority(Config) ->
    Name = ?config(test_name, Config),
    N1 = nth_server_name(Config, 1),
    N2 = nth_server_name(Config, 2),
    N3 = nth_server_name(Config, 3),
    ok = ra:start_server(default, Name, N1, add_machine(), [N2, N3]),
    ok = ra:trigger_election(N1),
    {timeout, _} = ra:process_command(N1, 5, 100),
    terminate_cluster([N1]).

start_servers(Config) ->
    Name = ?config(test_name, Config),
    % suite unique server names
    N1 = nth_server_name(Config, 1),
    N2 = nth_server_name(Config, 2),
    N3 = nth_server_name(Config, 3),
    % start the first server and wait a bit
    ok = ra:start_server(default, Name, N1, add_machine(),
                         [N2, N3]),
    % start second server
    ok = ra:start_server(default, Name, N2, add_machine(),
                         [N1, N3]),
    % trigger election
    ok = ra:trigger_election(N1, ?DEFAULT_TIMEOUT),
    % a consensus command tells us there is a functioning cluster
    {ok, _, _Leader} = ra:process_command(N1, 5,
                                          ?PROCESS_COMMAND_TIMEOUT),
    % start the 3rd server and issue another command
    ok = ra:start_server(default, Name, N3, add_machine(),
                         [N1, N2]),
    timer:sleep(100),
    % issue command - this is likely to preceed teh rpc timeout so the node
    % then should stash the command until a leader is known
    {ok, _, Leader} = ra:process_command(N3, 5,
                                         ?PROCESS_COMMAND_TIMEOUT),
    % shut down non leader
    Target = case Leader of
                 N1 -> N2;
                 _ -> N1
             end,
    gen_statem:stop(Target, normal, 2000),
    %% simpel check to ensure overview at least doesn't crash
    ra:overview(?SYS),
    % issue command to confirm n3 joined the cluster successfully
    {ok, _, _} = ra:process_command(N3, 5, ?PROCESS_COMMAND_TIMEOUT),
    terminate_cluster([N1, N2, N3] -- [element(1, Target)]).


server_recovery(Config) ->
    N1 = nth_server_name(Config, 1),
    N2 = nth_server_name(Config, 2),
    N3 = nth_server_name(Config, 3),

    Name = ?config(test_name, Config),
    % start the first server and wait a bit
    ok = ra:start_server(default, Name, N1, add_machine(), [N2, N3]),
    ok = ra_server_proc:trigger_election(N1, ?DEFAULT_TIMEOUT),
    % start second server
    ok = ra:start_server(default, Name, N2, add_machine(), [N1, N3]),
    % a consensus command tells us there is a functioning 2 node cluster
    {ok, _, Leader} = ra:process_command(N2, 5, ?PROCESS_COMMAND_TIMEOUT),
    % stop leader to trigger restart
    proc_lib:stop(Leader, bad_thing, 5000),
    timer:sleep(1000),
    N = case Leader of
            N1 -> N2;
            _ -> N1
        end,
    % issue command
    {ok, _, _Leader} = ra:process_command(N, 5, ?PROCESS_COMMAND_TIMEOUT),
    terminate_cluster([N1, N2]).

process_command(Config) ->
    [A, _B, _C] = Cluster =
        start_local_cluster(3, ?config(test_name, Config),
                            {simple, fun erlang:'+'/2, 9}),
        {ok, 14, _Leader} = ra:process_command(A, 5, ?PROCESS_COMMAND_TIMEOUT),
    terminate_cluster(Cluster).

pipeline_command(Config) ->
    [A, _B, _C] = Cluster =
        start_local_cluster(3, ?config(test_name, Config),
                            {simple, fun erlang:'+'/2, 9}),
    {ok, _, Leader} = ra:members(A),
    Correlation = make_ref(),
    ok = ra:pipeline_command(Leader, 5, Correlation),
    receive
        {ra_event, _, {applied, [{Correlation, 14}]}} -> ok
    after 2000 ->
              exit(consensus_timeout)
    end,
    terminate_cluster(Cluster).

pipeline_command_reject(Config) ->
    [A, _, _C] = Cluster =
        start_local_cluster(3, ?config(test_name, Config),
                            {simple, fun erlang:'+'/2, 9}),
    {ok, _, Leader} = ra:members(A),
    Followers = Cluster -- [Leader],
    Correlation = make_ref(),
    Target = hd(Followers),
    ok = ra:pipeline_command(Target, 5, Correlation),
    receive
        {ra_event, _, {rejected, {not_leader, Leader, Correlation}}} -> ok
    after 2000 ->
              exit(ra_event_timeout)
    end,
    terminate_cluster(Cluster).

pipeline_command_2_forwards_to_leader(Config) ->
    [A, B, C] = Cluster =
        start_local_cluster(3, ?config(test_name, Config),
                            {simple, fun erlang:'+'/2, 0}),
    % cast to each server - command should be forwarded
    ok = ra:pipeline_command(A, 5),
    ok = ra:pipeline_command(B, 5),
    ok = ra:pipeline_command(C, 5),
    timer:sleep(50),
    {ok, _, _} = ra:consistent_query(A, fun (X) -> X end),
    terminate_cluster(Cluster).

local_query(Config) ->
    [A, B, _C] = Cluster = start_local_cluster(3, ?config(test_name, Config),
                                               {simple, fun erlang:'+'/2, 9}),
    {ok, {_, 9}, _} = ra:local_query(B, fun(S) -> S end),
    {ok, _, Leader} = ra:process_command(A, 5,
                                                 ?PROCESS_COMMAND_TIMEOUT),
    {ok, {_, 14}, _} = ra:local_query(Leader, fun(S) -> S end),
    terminate_cluster(Cluster).

local_query_boom(Config) ->
    [A, B, _C] = Cluster = start_local_cluster(3, ?config(test_name, Config),
                                               {simple, fun erlang:'+'/2, 9}),
    {error, _} = ra:local_query(B, fun(_) -> exit(boom) end),
    {ok, _, Leader} = ra:process_command(A, 5, ?PROCESS_COMMAND_TIMEOUT),
    {ok, {_, 14}, _} = ra:local_query(Leader, fun(S) -> S end),
    {timeout, Leader} = ra:local_query(Leader, fun(_) -> timer:sleep(200) end, 100),
    terminate_cluster(Cluster).

local_query_stale(Config) ->
    [A, B, _C] = Cluster = start_local_cluster(3, ?config(test_name, Config),
                                               add_machine()),
    {ok, {_, 0}, _} = ra:local_query(B, fun(S) -> S end),
    {ok, _, Leader} = ra:process_command(A, 5, ?PROCESS_COMMAND_TIMEOUT),
    {ok, {_, 5}, _} = ra:local_query(Leader, fun(S) -> S end),

    NonLeader = hd([Node || Node <- [A,B], Node =/= Leader]),
    ra:stop_server(?SYS, NonLeader),

    Correlation = make_ref(),
    [ra:pipeline_command(Leader, 1, Correlation) || _ <- lists:seq(1, 5000)],

    wait_for_applied({Correlation, 5005}),

    ra:restart_server(?SYS, NonLeader),

    {ok, {_, NonLeaderV}, _} = ra:local_query(NonLeader, fun(S) -> S end, 100000),
    {ok, {_, LeaderV}, _} = ra:local_query(Leader, fun(S) -> S end),
    ct:pal("LeaderV ~p~n NonLeaderV ~p", [LeaderV, NonLeaderV]),
    ?assertNotMatch(LeaderV, NonLeaderV),
    terminate_cluster(Cluster).

consistent_query_stale(Config) ->
    [A, B, _C] = Cluster = start_local_cluster(3, ?config(test_name, Config),
                                               add_machine()),
    {ok, 0, _} = ra:consistent_query(B, fun(S) -> S end),
    {ok, _, Leader} = ra:process_command(A, 5, ?PROCESS_COMMAND_TIMEOUT),
    {ok, 5, _} = ra:consistent_query(Leader, fun(S) -> S end),

    NonLeader = hd([Node || Node <- [A,B], Node =/= Leader]),
    ra:stop_server(?SYS, NonLeader),

    Correlation = make_ref(),
    [ra:pipeline_command(Leader, 1, Correlation) || _ <- lists:seq(1, 5000)],

    wait_for_applied({Correlation, 5005}),

    ra:restart_server(?SYS, NonLeader),

    {ok, NonLeaderV, _} = ra:consistent_query(NonLeader, fun(S) -> S end),
    {ok, LeaderV, _} = ra:consistent_query(Leader, fun(S) -> S end),
    ct:pal("LeaderV ~p~n NonLeaderV ~p", [LeaderV, NonLeaderV]),
    ?assertMatch(LeaderV, NonLeaderV),
    {ok, {{Index, _}, _}, _} = ra:local_query(Leader, fun(S) -> S end),
    {ok, V, _} = ra:consistent_query(NonLeader, fun(S) -> S end),
    {ok, V, _} = ra:consistent_query(Leader, fun(S) -> S end),
    {ok, {{IndexAfter, _}, _}, _} = ra:local_query(Leader, fun(S) -> S end),
    ?assertMatch(Index, IndexAfter),
    terminate_cluster(Cluster).

all_metrics_are_integers(Config) ->
    % ok = logger:set_primary_config(level, all),
    Name = ?config(test_name, Config),
    N1 = nth_server_name(Config, 1),
    ok = ra:start_server(default, Name, N1, add_machine(), []),
    ok = ra:trigger_election(N1),
    {ok, 5, _} = ra:process_command(N1, 5, 2000),
    [{_, M1, M2, M3, M4, M5, M6}] = ets:lookup(ra_metrics, element(1, N1)),
    ?assert(lists:all(fun(I) -> is_integer(I) end, [M1, M2, M3, M4, M5, M6])),
    terminate_cluster([N1]).

wait_for_applied(Msg) ->
    receive {ra_event, _, {applied, Applied}} ->
        case lists:member(Msg, Applied) of
            true  -> ok;
            false -> wait_for_applied(Msg)
        end
    after 10000 ->
        error({timeout_waiting_for_applied, Msg})
    end.

members(Config) ->
    Cluster = start_local_cluster(3, ?config(test_name, Config),
                                  {simple, fun erlang:'+'/2, 9}),
    {ok, _, Leader} = ra:process_command(hd(Cluster), 5,
                                         ?PROCESS_COMMAND_TIMEOUT),
    {ok, Cluster, Leader} = ra:members(Leader),
    terminate_cluster(Cluster).

consistent_query(Config) ->
    [A, _, _]  = Cluster = start_local_cluster(3, ?config(test_name, Config),
                                               add_machine()),
    {ok, _, Leader} = ra:process_command(A, 9,
                                         ?PROCESS_COMMAND_TIMEOUT),
    {ok, 9, Leader} = ra:consistent_query(A, fun(S) -> S end),
    {ok, 14, _} = ra:process_command(Leader, 5,
                                    ?PROCESS_COMMAND_TIMEOUT),
    {ok, 14, Leader} = ra:consistent_query(A, fun(S) -> S end),
    terminate_cluster(Cluster).

add_member(Config) ->
    Name = ?config(test_name, Config),
    [A, _B] = Cluster = start_local_cluster(2, Name, add_machine()),
    {ok, _, Leader} = ra:process_command(A, 9),
    C = {ra_server:name(Name, "3"), node()},
    ok = ra:start_server(default, Name, C, add_machine(), Cluster),
    {ok, _, _Leader} = ra:add_member(Leader, C),
    {ok, 9, Leader} = ra:consistent_query(C, fun(S) -> S end),
    terminate_cluster([C | Cluster]).

server_catches_up(Config) ->
    N1 = nth_server_name(Config, 1),
    N2 = nth_server_name(Config, 2),
    N3 = nth_server_name(Config, 3),
    Name = ?config(test_name, Config),
    InitialNodes = [N1, N2],
    %%TODO look into cluster changes WITH INVALID NAMES!!!

    Mac = {module, ra_queue, #{}},
    % start two servers
    ok = ra:start_server(default, Name, N1, Mac, InitialNodes),
    ok = ra:start_server(default, Name, N2, Mac, InitialNodes),
    ok = ra:trigger_election(N1),
    DecSink = spawn(fun () -> receive marker_pattern -> ok end end),
    {ok, _, Leader} = ra:process_command(N1, {enq, banana}),
    ok = ra:pipeline_command(Leader, {deq, DecSink}),
    {ok, _, Leader} = ra:process_command(Leader, {enq, apple},
                                         ?PROCESS_COMMAND_TIMEOUT),

    ok = ra:start_server(default, Name, N3, Mac, InitialNodes),
    {ok, _, _Leader} = ra:add_member(Leader, N3),
    timer:sleep(1000),
    % at this point the servers should be caught up
    {ok, {_, Res}, _} = ra:local_query(N1, fun ra_lib:id/1),
    {ok, {_, Res}, _} = ra:local_query(N2, fun ra_lib:id/1),
    {ok, {_, Res}, _} = ra:local_query(N3, fun ra_lib:id/1),
    % check that the message isn't delivered multiple times
    terminate_cluster([N3 | InitialNodes]).

snapshot_installation(Config) ->
    N1 = nth_server_name(Config, 1),
    N2 = nth_server_name(Config, 2),
    N3 = nth_server_name(Config, 3),
    Name = ?config(test_name, Config),
    Servers = [N1, N2, N3],
    Mac = {module, ra_queue, #{}},
    % start two servers
    {ok, [Leader0, _, Down], []}  = ra:start_cluster(default, Name, Mac, Servers),
    ok = ra:stop_server(?SYS, Down),
    {ok, _, Leader} = ra:members(Leader0),
    %% process enough commands to trigger two snapshots, ra will snapshot
    %% every ~4000 log entries or so by default
    [begin
         ok = ra:pipeline_command(Leader, {enq, N}, no_correlation, normal),
         ok = ra:pipeline_command(Leader, deq, no_correlation, normal)
     end || N <- lists:seq(1, 2500)],

    {ok, _, _} = ra:process_command(Leader, deq),
    [begin
         ok = ra:pipeline_command(Leader, {enq, N}, no_correlation, normal),
         ok = ra:pipeline_command(Leader, deq, no_correlation, normal)
     end || N <- lists:seq(2500, 6000)],
    {ok, _, _} = ra:process_command(Leader, deq),

    N1Dir = ra_env:server_data_dir(default, ra_directory:uid_of(default, N1)),
    N2Dir = ra_env:server_data_dir(default, ra_directory:uid_of(default, N2)),
    N3Dir = ra_env:server_data_dir(default, ra_directory:uid_of(default, N3)),

    %% start the down node again, catchup should involve sending a snapshot
    ok = ra:restart_server(?SYS, Down),

    %% assert all contains snapshots
    TryFun = fun(Dir) ->
                     length(filelib:wildcard(
                              filename:join([Dir, "snapshots", "*"]))) > 0
             end,
    ?assert(try_n_times(fun () -> TryFun(N2Dir) end, 20)),
    ?assert(try_n_times(fun () -> TryFun(N3Dir) end, 20)),
    ?assert(try_n_times(fun () -> TryFun(N1Dir) end, 20)),

    % then do some more
    [begin
         ok = ra:pipeline_command(Leader, {enq, N}, no_correlation, normal),
         ok = ra:pipeline_command(Leader, deq, no_correlation, normal)
     end || N <- lists:seq(6000, 7000)],
    {ok, _, _} = ra:process_command(Leader, deq),

    %% check snapshot was taken by leader
    ?assert(try_n_times(
              fun () ->
                      {ok, {N1Idx, _}, _} = ra:local_query(N1, fun ra_lib:id/1),
                      {ok, {N2Idx, _}, _} = ra:local_query(N2, fun ra_lib:id/1),
                      {ok, {N3Idx, _}, _} = ra:local_query(N3, fun ra_lib:id/1),
                      (N1Idx == N2Idx) and (N1Idx == N3Idx)
              end, 20)),
    ok.

snapshot_installation_with_call_crash(Config) ->
    N1 = nth_server_name(Config, 1),
    N2 = nth_server_name(Config, 2),
    N3 = nth_server_name(Config, 3),
    Name = ?config(test_name, Config),
    Servers = [N1, N2, N3],
    Mac = {module, ra_queue, #{}},
    meck:new(gen_statem, [unstick, passthrough]),

    % start two servers
    {ok, [Leader0, _, Down], []}  = ra:start_cluster(default, Name, Mac, Servers),
    ok = ra:stop_server(?SYS, Down),
    {ok, _, Leader} = ra:members(Leader0),
    %% process enough commands to trigger two snapshots, ra will snapshot
    %% every ~4000 log entries or so by default
    [begin
         ok = ra:pipeline_command(Leader, {enq, N}, no_correlation, normal),
         ok = ra:pipeline_command(Leader, deq, no_correlation, normal)
     end || N <- lists:seq(1, 2500)],

    {ok, _, _} = ra:process_command(Leader, deq),

    meck:expect(gen_statem, call, fun (_,  #install_snapshot_rpc{}, _) ->
                                          exit(timeout);
                                      (A, B, C) ->
                                          meck:passthrough([A, B, C])
                                  end),
    %% start the down node again, catchup should involve sending a snapshot
    ok = ra:restart_server(?SYS, Down),

    timer:sleep(2500),
    meck:unload(gen_statem),

    ?assert(try_n_times(
              fun () ->
                      {ok, {N1Idx, _}, _} = ra:local_query(N1, fun ra_lib:id/1),
                      {ok, {N2Idx, _}, _} = ra:local_query(N2, fun ra_lib:id/1),
                      {ok, {N3Idx, _}, _} = ra:local_query(N3, fun ra_lib:id/1),
                      (N1Idx == N2Idx) and (N1Idx == N3Idx)
              end, 20)),
    ok.


try_n_times(_Fun, 0) ->
    false;
try_n_times(Fun, N) ->
    case Fun() of
        true -> true;
        false ->
            timer:sleep(250),
            try_n_times(Fun, N -1)
    end.



stop_leader_and_wait_for_elections(Config) ->
    Name = ?config(test_name, Config),
    Members = [{n1, node()}, {n2, node()}, {n3, node()}],
    {ok, _, _} = ra:start_cluster(default, Name, add_machine(), Members),
    % issue command
    {ok, _, Leader} = ra:process_command({n3, node()}, 5),
    % shut down the leader
    gen_statem:stop(Leader, normal, 2000),
    timer:sleep(1000),
    % issue command to confirm a new leader is elected
    [Serv | _] = Rem = Members -- [Leader],
    {ok, _, NewLeader} = ra:process_command(Serv, 5),
    true = (NewLeader =/= Leader),
    terminate_cluster(Rem).

queue_example(Config) ->
    Self = self(),
    [A, _B, _C] = Cluster = start_local_cluster(3, ?config(test_name, Config),
                                                {module, ra_queue, #{}}),

    {ok, _, Leader} = ra:process_command(A, {enq, test_msg}),
    {ok, _, _} = ra:process_command(Leader, {deq, Self}),
    await_msg_or_fail(test_msg, apply_timeout),
    % check that the message isn't delivered multiple times
    receive
        {ra_queue, _, test_msg} ->
            exit(double_delivery)
    after 500 -> ok
    end,
    terminate_cluster(Cluster).

contains(Match, Entries) ->
    lists:any(fun({_, _, {_, _, Value, _}}) when Value == Match ->
                      true;
                 (_) ->
                      false
              end, Entries).

follower_catchup(Config) ->

    % ok = logger:set_primary_config(level, all),
    meck:new(ra_server_proc, [passthrough]),
    meck:expect(ra_server_proc, send_rpc,
                fun(P, #append_entries_rpc{entries = Entries} = T, S) ->
                        case contains(500, Entries) of
                            true ->
                                ct:pal("dropped 500"),
                                ok;
                            false ->
                                meck:passthrough([P, T, S])
                        end;
                   (P, T, S) ->
                        meck:passthrough([P, T, S])
                end),
    Name = ?config(test_name, Config),
    % suite unique server names
    N1 = nth_server_name(Config, 1),
    N2 = nth_server_name(Config, 2),
    % start the first server and wait a bit
    Conf = fun (NodeId, NodeIds, UId) ->
               #{cluster_name => Name,
                 id => NodeId,
                 uid => UId,
                 initial_members => NodeIds,
                 log_init_args => #{uid => UId},
                 machine => add_machine(),
                 await_condition_timeout => 1000}
           end,
    ok = ra:start_server(?SYS, Conf(N1, [N2], <<"N1">>)),
    % start second servern
    ok = ra:start_server(?SYS, Conf(N2, [N1], <<"N2">>)),
    ok = ra:trigger_election(N1),
    _ = ra:members(N1),
    % a consensus command tells us there is a functioning cluster
    {ok, _, Leader} = ra:process_command(N1, 5,
                                         ?PROCESS_COMMAND_TIMEOUT),
    Corr = make_ref(),
    % issue command - this will be lost
    ok = ra:pipeline_command(N1, 500, Corr),
    % issue next command
    ok = ra:pipeline_command(N1, 501),
    [Follower] = [N1, N2] -- [Leader],
    receive
        {ra_event, _, {applied, [{Corr, _}]}} ->
            exit(unexpected_consensus)
    after 1000 ->
              wait_for_gen_statem_status(Follower, await_condition, 30000)
    end,
    meck:unload(),
    % we wait for the condition to time out - then the follower will re-issue
    % the aer with the original condition which should trigger a re-wind of of
    % the next_index and a subsequent resend of missing entries
    receive
        {ra_event, _, {applied, [{Corr, _}]}} ->
              wait_for_gen_statem_status(Follower, follower, 30000),
              ok
    after 6000 ->
              flush(),
              exit(consensus_not_achieved)
    end,
    terminate_cluster([N1, N2]).

flush() ->
    receive
        Any ->
            ct:pal("flush ~p", [Any]),
            flush()
    after 0 ->
              ok
    end.

post_partition_liveness(Config) ->
    meck:new(ra_server_proc, [passthrough]),
    Name = ?config(test_name, Config),
    % suite unique servef names
    N1 = nth_server_name(Config, 1),
    N2 = nth_server_name(Config, 2),
    {ok, [_, _], _}  = ra:start_cluster(default, Name, add_machine(), [N1, N2]),
    {ok, _, Leader}  = ra:members(N1),

    % simulate partition
    meck:expect(ra_server_proc, send_rpc, fun(_, _, _) -> ok end),
    Corr = make_ref(),
    % send an entry that will not be replicated
    ok = ra:pipeline_command(Leader, 500, Corr),
    % assert we don't achieve consensus
    receive
        {ra_event, _, {applied, [{Corr, _}]}} ->
            exit(unexpected_consensus)
    after 1000 ->
              ok
    end,
    % heal partition
    meck:unload(),
    % assert consensus completes after some time
    receive
        {ra_event, _, {applied, [{Corr, _}]}} ->
            ok
    after 6500 ->
            exit(consensus_timeout)
    end,
    ok.

transfer_leadership(Config) ->
    Name = ?config(test_name, Config),
    Members = [{n1, node()}, {n2, node()}, {n3, node()}],
    {ok, _, _} = ra:start_cluster(default, Name, add_machine(), Members),
    % issue a command
    {ok, _, Leader} = ra:process_command({n3, node()}, 5),
    % transfer leadership
    [NextInLine | _] = Members -- [Leader],
    ct:pal("Transferring leadership from ~p to ~p", [Leader, NextInLine]),
    ok = ra:transfer_leadership(Leader, NextInLine),
    {ok, _, NewLeader} = ra:process_command(NextInLine, 5),
    ?assertEqual(NewLeader, NextInLine),
    ?assertEqual(already_leader, ra:transfer_leadership(NewLeader, NewLeader)),
    ?assertEqual({error, unknown_member}, ra:transfer_leadership(NewLeader, {unknown, node()})),
    terminate_cluster(Members).

get_gen_statem_status(Ref) ->
    {_, _, _, Items} = sys:get_status(Ref),
    proplists:get_value(raft_state, lists:last(Items)).

wait_for_gen_statem_status(Ref, ExpectedStatus, Timeout)
  when Timeout >= 0 ->
    case get_gen_statem_status(Ref) of
        ExpectedStatus ->
            ok;
        OtherStatus when Timeout >= 0 ->
            timer:sleep(500),
            wait_for_gen_statem_status(Ref, ExpectedStatus, Timeout - 500);
        OtherStatus ->
            exit({unexpected_gen_statem_status, OtherStatus})
    end.

% implements a simple queue machine
queue_apply({enqueue, Msg}, State =#{queue := Q0, pending_dequeues := []}) ->
    Q = queue:in(Msg, Q0),
    State#{queue => Q};
queue_apply({enqueue, Msg}, State = #{queue := Q0,
                                      pending_dequeues := [Next | Rest]}) ->
    Q1 = queue:in(Msg, Q0),
    {{value, Item}, Q} = queue:out(Q1),
    {State#{queue => Q, pending_dequeues => Rest}, [{send_msg, Next, Item}]};
queue_apply({dequeue, For}, State = #{queue := Q0, pending_dequeues := []}) ->
    case queue:out(Q0) of
        {empty, Q} ->
            State#{queue => Q, pending_dequeues => [For]};
        {{value, Item}, Q} ->
            {State#{queue => Q}, [{send_msg, For, Item}]}
    end;
queue_apply({dequeue, For},
            State = #{queue := Q0,
                      pending_dequeues := [Next | Rest] = Pending}) ->
    case queue:out(Q0) of
        {empty, Q} ->
            State#{queue => Q, pending_dequeues => Pending ++ [For]};
        {{value, Item}, Q} ->
            {State#{queue => Q, pending_dequeues => Rest ++ [For]},
             [{send_msg, Next, Item}]}
    end.


await_msg_or_fail(Msg, ExitWith) ->
    receive
        Msg -> ok
    after 3000 ->
              exit(ExitWith)
    end.

terminate_cluster(Nodes) ->
    [ra:stop_server(?SYS, P) || P <- Nodes].

new_server(Name, Config) ->
    ClusterName = ?config(test_name, Config),
    ok = ra:start_server(default, ClusterName, Name, add_machine(), []),
    ok.

stop_server(Name) ->
    ok = ra:stop_server(?SYS, Name),
    ok.

add_member(Ref, New) ->
    {ok, _IdxTerm, _Leader} = ra:add_member(Ref, New),
    ok.

start_and_join({ClusterName, _} = ServerRef, {_, _} = New) ->
    {ok, _, _} = ra:add_member(ServerRef, New),
    ok = ra:start_server(default, ClusterName, New, add_machine(), [ServerRef]),
    ok.

start_local_cluster(Num, Name, Machine) ->
    Nodes = [{ra_server:name(Name, integer_to_list(N)), node()}
             || N <- lists:seq(1, Num)],

    {ok, _, Failed} = ra:start_cluster(default, Name, Machine, Nodes),
    ?assert(length(Failed) == 0),
    Nodes.

remove_member(Name) ->
    {ok, _IdxTerm, _Leader} = ra:remove_member(Name, Name),
    ok.

issue_op(Name, Op) ->
    {ok, _, Leader} = ra:process_command(Name, Op, 2000),
    Leader.

validate_state_on_node(Name, Expected) ->
    {ok, Expected, _} = ra:consistent_query(Name, fun(X) -> X end).

dump(T) ->
    ct:pal("DUMP: ~p", [T]),
    T.

nth_server_name(Config, N) when is_integer(N) ->
    {ra_server:name(?config(test_name, Config), erlang:integer_to_list(N)),
     node()}.

add_machine() ->
    {module, ?MODULE, #{}}.

%% machine impl
init(_) -> 0.
apply(_Meta, Num, State) ->
    {Num + State, Num + State}.

gather_applied([], Timeout) ->
    %% have a longer timeout first
    %% as we assume we expect to receive at least one ra_event
    receive
        {ra_event, _Leader, {applied, Corrs}} ->
            gather_applied(Corrs, Timeout)
    after 2000 ->
              exit(ra_event_timeout)
    end;
gather_applied(Acc, Timeout) ->
    receive
        {ra_event, _Leader, {applied, Corrs}} ->
            gather_applied(Acc ++ Corrs, Timeout)
    after Timeout ->
              Acc
    end.

