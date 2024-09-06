%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term Broadcom refers to Broadcom Inc. and/or its subsidiaries.
%%
-module(ra_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include("src/ra.hrl").

-define(PROCESS_COMMAND_TIMEOUT, 6000).
-define(SYS, default).

%% The dialyzer catches that the given reply mode is not included in the
%% `ra_server:command_reply_mode()' type:
-dialyzer({nowarn_function, [process_command_with_unknown_reply_mode/1]}).

%% The following testcases simulate an erroneous or unsupported call that is
%% outside of the spec.
-dialyzer({nowarn_function, [unknown_leader_call/1,
                             unknown_local_call/1]}).

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
     process_command_reply_from_local,
     process_command_reply_from_member,
     process_command_with_unknown_reply_mode,
     pipeline_command,
     pipeline_command_reject,
     pipeline_command_2_forwards_to_leader,
     local_query,
     local_query_boom,
     local_query_stale,
     local_query_with_condition_option,
     members,
     members_info,
     consistent_query,
     consistent_query_after_restart,
     consistent_query_minority,
     consistent_query_leader_change,
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
     {testcase, send_command_to_follower_during_election,
      [{repeat_until_fail, 50}]},
     follower_catchup,
     post_partition_liveness,
     all_metrics_are_integers,
     transfer_leadership,
     transfer_leadership_two_node,
     new_nonvoter_knows_its_status,
     voter_gets_promoted_consistent_leader,
     voter_gets_promoted_new_leader,
     unknown_leader_call,
     unknown_local_call
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
    %% here we are asserting on the order of received
    %% correlations
    [{C1, 5}, {C2, 10}] = gather_applied([], 125),

    %% correlations are transient so ok to reuse refs
    ok = ra:pipeline_command(N1, 5, C1, low),
    ok = ra:pipeline_command(N1, 5, C2, low),
    %% here we are asserting on the order of received
    %% correlations
    [{C1, 15}, {C2, 20}] = gather_applied([], 125),
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
    % issue command - this is likely to precede the rpc timeout so the node
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

process_command_reply_from_local(Config) ->
    PrivDir = filename:join(?config(priv_dir, Config),
                            ?config(test_name, Config)),
    Options = #{reply_from => local,
                timeout => ?PROCESS_COMMAND_TIMEOUT},

    Cluster = start_remote_cluster(3, PrivDir, local_command_cluster,
                                   add_machine()),
    {ok, _, Leader} = ra:members(hd(Cluster)),
    {_, FollowerNode} = Follower =
        hd([Member || Member <- Cluster, Member =/= Leader]),

    %% The leader will reply if no node in the cluster is local to the caller.
    {ok, 5, _} = ra:process_command(Leader, 5, Options),

    %% The reply will come from the follower.
    {ok, 10, _} = rpc:call(FollowerNode,
                           ra, process_command, [Leader, 5, Options]),

    ct:pal("stopping member: ~p", [Follower]),
    ra:stop_server(?SYS, Follower),

    %% The server is stopped so the command is not handled.
    ?assertEqual({error, noproc},
                 ra:process_command(Follower, 5, Options)),
    {ok, {_, 10}, _} = ra:leader_query(Leader, fun(State) -> State end),

    %% The local member can't reply to the command request since it is stopped.
    ?assertMatch({timeout, _},
                 rpc:call(FollowerNode,
                          ra, process_command, [Leader, 5, Options])),

    terminate_cluster(Cluster).

process_command_reply_from_member(Config) ->
    [A, _B, _C] = Cluster =
        start_local_cluster(3, ?config(test_name, Config),
                            {simple, fun erlang:'+'/2, 9}),

    {ok, _, Leader} = ra:members(A),
    Follower = hd([Member || Member <- Cluster, Member =/= Leader]),
    Options = #{reply_from => {member, Follower},
                timeout => ?PROCESS_COMMAND_TIMEOUT},

    {ok, 14, _Leader} = ra:process_command(A, 5, Options),

    ct:pal("stopping member: ~p", [Follower]),
    ra:stop_server(?SYS, Follower),

    %% The process is no longer alive so the command is not handled.
    ?assertEqual({error, noproc}, ra:process_command(Follower, 5, Options)),
    {ok, {_, 14}, _} = ra:leader_query(Leader, fun(State) -> State end),

    %% The command is successfully handled on the leader but the member is
    %% not available to reply to the caller.
    ?assertMatch({timeout, _}, ra:process_command(Leader, 5, Options)),
    {ok, {_, 19}, _} = ra:leader_query(Leader, fun(State) -> State end),

    %% If the given member is not part of the cluster then the reply is
    %% performed by the leader.
    {ok, 24, _} =
        ra:process_command(Leader, 5,
                           #{reply_from => {member, {does_not_exist, node()}},
                             timeout => ?PROCESS_COMMAND_TIMEOUT}),

    terminate_cluster(Cluster).

process_command_with_unknown_reply_mode(Config) ->
    [A, _B, _C] = Cluster =
        start_local_cluster(3, ?config(test_name, Config),
                            {simple, fun erlang:'+'/2, 9}),
    Command = 5,
    ReplyMode = bad_reply_mode,
    RaCommand = {'$usr', Command, ReplyMode},
    ?assertEqual({error, {invalid_reply_mode, ReplyMode}},
                 ra_server_proc:command(A, RaCommand,
                                        ?PROCESS_COMMAND_TIMEOUT)),
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
              flush(),
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

local_query_with_condition_option() ->
    [{timetrap, {minutes, 1}}].

local_query_with_condition_option(Config) ->
    [A, _B, _C] = Cluster = start_local_cluster(3, ?config(test_name, Config),
                                                {simple, fun erlang:'+'/2, 9}),
    try
        %% Get the leader and deduce a follower.
        {ok, _, Leader} = ra:process_command(A, 5, ?PROCESS_COMMAND_TIMEOUT),
        [Follower | _] = Cluster -- [Leader],
        ct:pal(
          "Leader:   ~0p (~p)~n"
          "Follower: ~0p (~p)",
          [Leader, erlang:whereis(element(1, Leader)),
           Follower, erlang:whereis(element(1, Follower))]),

        %% Get the last applied index.
        QueryFun = fun(S) -> S end,
        {ok, {{Idx, Term}, 14}, _} = ra:local_query(Leader, QueryFun),
        ct:pal("Currently applied index on leader: {~b, ~b}", [Idx, Term]),

        %% Query using the already applied index.
        IdxTerm = {Idx, Term},
        ct:pal(
          "Query on leader with idxterm ~0p; expecting success",
          [IdxTerm]),
        ?assertMatch(
           {ok, {_, 14}, _},
           ra:local_query(
             Leader, QueryFun, #{condition => {applied, IdxTerm}})),
        ct:pal(
          "Query on follower with idxterm ~0p; expecting success",
          [IdxTerm]),
        ?assertMatch(
           {ok, {_, 14}, _},
           ra:local_query(
             Follower, QueryFun, #{condition => {applied, IdxTerm}})),

        %% Query using the next index; this should time out.
        IdxTerm1 = {Idx + 1, Term},
        Timeout = 2000,
        ct:pal(
          "Query on leader with idxterm ~0p; expecting timeout", [IdxTerm1]),
        ?assertMatch(
           {timeout, _},
           ra:local_query(
             Leader, QueryFun,
             #{condition => {applied, IdxTerm1},
               timeout => Timeout})),
        ct:pal(
          "Query on follower with idxterm ~0p; expecting timeout", [IdxTerm1]),
        ?assertMatch(
           {timeout, _},
           ra:local_query(
             Follower, QueryFun,
             #{condition => {applied, IdxTerm1},
               timeout => Timeout})),

        %% Submit a command through the follower.
        %%
        %% The following queries will be executed, as well as the previous ones
        %% which timed out from the caller's point of view. They were still in
        %% the Ra servers' state. The answer to these queries will go to
        %% /dev/null because their alias is inactive.
        ok = ra:pipeline_command(Follower, 3, no_correlation, normal),

        %% Query using the next index; we should get an answer. The command
        %% might be applied already before we submit the query though.
        ct:pal(
          "Query on leader with idxterm ~0p; expecting success",
          [IdxTerm1]),
        ?assertMatch(
           {ok, {_, 17}, _},
           ra:local_query(
             Leader, QueryFun, #{condition => {applied, IdxTerm1}})),
        ct:pal(
          "Query on follower with idxterm ~0p; expecting success",
          [IdxTerm1]),
        ?assertMatch(
           {ok, {_, 17}, _},
           ra:local_query(
             Follower, QueryFun, #{condition => {applied, IdxTerm1}})),

        %% Query using the next next index; this should time out. This ensures
        %% that that index wasn't already applied, invalidating the rest of the
        %% test case.
        IdxTerm2 = {Idx + 2, Term},
        ct:pal(
          "Query on leader with idxterm ~0p; expecting timeout",
          [IdxTerm2]),
        ?assertMatch(
           {timeout, _},
           ra:local_query(
             Leader, QueryFun,
             #{condition => {applied, IdxTerm2},
               timeout => Timeout})),
        ct:pal(
          "Query on follower with idxterm ~0p; expecting timeout",
          [IdxTerm2]),
        ?assertMatch(
           {timeout, _},
           ra:local_query(
             Follower, QueryFun,
             #{condition => {applied, IdxTerm2},
               timeout => Timeout})),

        %% Query using the next next index; we should get an answer. This time,
        %% we ensure that the query is submitted before the next command.
        %%
        %% The following queries will be executed, as well as the previous ones
        %% which timed out from the caller's point of view. They were still in
        %% the Ra servers' state. The answer to these queries will go to
        %% /dev/null because their alias is inactive.
        Parent = self(),
        _Pid = spawn_link(
                 fun() ->
                         ct:pal(
                           "Query on leader with idxterm ~0p; "
                           "expecting success", [IdxTerm2]),
                         ?assertMatch(
                            {ok, {_, 19}, _},
                            ra:local_query(
                              Leader, QueryFun,
                              #{condition => {applied, IdxTerm2}})),
                         ct:pal(
                           "Query on follower with idxterm ~0p; "
                           "expecting success", [IdxTerm2]),
                         ?assertMatch(
                            {ok, {_, 19}, _},
                            ra:local_query(
                              Follower, QueryFun,
                              #{condition => {applied, IdxTerm2}})),
                         Parent ! done,
                         erlang:unlink(Parent)
                 end),

        %% Submit a command through the follower.
        ok = ra:pipeline_command(Follower, 2, no_correlation, normal),
        receive
            done -> ok
        end
    after
        terminate_cluster(Cluster)
    end.

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

members_info(Config) ->
    Name = ?config(test_name, Config),
    [A = {_, Host}, B] = InitNodes = start_local_cluster(2, Name, add_machine()),
    {ok, _, Leader} = ra:process_command(A, 9),
    [Follower] = InitNodes -- [Leader],
    CSpec =  #{id => {CName = ra_server:name(Name, "3"), node()},
               uid => <<"3">>,
               membership => promotable},
    C =  {CName, Host},
    ok = ra:start_server(default, Name, CSpec, add_machine(), InitNodes),
    {ok, _, _} = ra:add_member(Leader, CSpec),
    {ok, 9, Leader} = ra:consistent_query(C, fun(S) -> S end),
    timer:sleep(100),
    ?assertMatch({ok,
                  #{Follower := #{status := normal,
                                  query_index := QI,
                                  next_index := NI,
                                  match_index := MI,
                                  commit_index_sent := _,
                                  voter_status := #{membership := voter}},
                    Leader := #{status := normal,
                                query_index := QI,
                                next_index := NI,
                                match_index := MI,
                                voter_status := #{membership := voter}},
                    C := #{status := normal,
                           query_index := _,
                           next_index := NI,
                           commit_index_sent := MI,
                           voter_status := #{membership := CMemb,
                                             target := _}}},
                  Leader}
                   when CMemb == promotable orelse CMemb == voter,
                        ra:members_info(Follower)),
    ?assertMatch({ok,
                  #{A := #{},
                    B := #{},
                    C := #{query_index := _,
                           match_index := _,
                           voter_status := #{membership := _}}},
                  C}, ra:members_info({local, C})),
    terminate_cluster([A, B, C]).

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

new_value(A, _) ->
    A.

consistent_query_after_restart(Config) ->
    DataDir = filename:join([?config(priv_dir, Config), "data"]),
    [A, B]  = Cluster = start_local_cluster(2, ?config(test_name, Config),
                                            {simple, fun ?MODULE:new_value/2, 0}),
    %% this test occasionally reproduces a stale read bug after a restart
    %% NB: occasionally....
    [begin
         {ok, _, _} = ra:process_command(A, N, ?PROCESS_COMMAND_TIMEOUT),
         application:stop(ra),
         restart_ra(DataDir),
         ok = ra:restart_server(A),
         ok = ra:restart_server(B),
         ?assertMatch({ok, N, _}, ra:consistent_query(A, fun(S) -> S end))
     end || N <- lists:seq(1, 30)],

    terminate_cluster(Cluster),
    ok.

consistent_query_minority(Config) ->
    [A, _, _]  = Cluster = start_local_cluster(3, ?config(test_name, Config),
                                               add_machine()),
    {ok, _, Leader} = ra:process_command(A, 9,
                                         ?PROCESS_COMMAND_TIMEOUT),
    [F1, F2] = Cluster -- [Leader],
    ra:stop_server(F1),
    ra:stop_server(F2),

    {timeout, _} = ra:consistent_query(Leader, fun(S) -> S end),
    %% restart after a short sleep so that quorum is restored whilst the next
    %% query is executing
    _ = spawn(fun() ->
                      timer:sleep(1000),
                      ra:restart_server(F1),
                      ok
              end),
    {ok, 9, _} = ra:consistent_query(Leader, fun(S) -> S end, 10000),
    {ok, 9, _} = ra:consistent_query(Leader, fun(S) -> S end),
    _ = terminate_cluster(Cluster),
    ok.


consistent_query_leader_change(Config) ->
    %% this test reproduces a scenario that could cause a stale
    %% read to be returned from `ra:consistent_query/2`
    ClusterName = ?config(test_name, Config),
    [A, B, C, D, E] = Cluster = start_local_cluster(5, ClusterName,
                                                    add_machine()),
    ok = ra:transfer_leadership(A, A),
    {ok, _, A} = ra:process_command(A, 9, ?PROCESS_COMMAND_TIMEOUT),
    %% do two consistent queries, this will put query_index == 2 everywhere
    {ok, 9, A} = ra:consistent_query(A, fun(S) -> S end),
    ok = ra:stop_server(E),
    {ok, 9, A} = ra:consistent_query(A, fun(S) -> S end),
    %% restart B
    ok = ra:stop_server(B),
    ok = ra:restart_server(B),
    %% Wait for B to recover and catch up.
    {ok, #{log := #{last_written_index_term := CurrentIdxTerm}}, _} =
      ra:member_overview(A),
    await_condition(
      fun() ->
              {ok, #{log := #{last_written_index_term := IdxTermB}}, _} =
                ra:member_overview(B),
              IdxTermB =:= CurrentIdxTerm
      end, 20),
    %% B's query_index is now 0
    %% Make B leader
    ok = ra:transfer_leadership(A, B),
    await_condition(
      fun() ->
              ra_leaderboard:lookup_leader(ClusterName) =:= B
      end, 20),
    %% restart E
    ok = ra:restart_server(E),
    {ok, 9, B} = ra:consistent_query(B, fun(S) -> S end),

    ok = ra:stop_server(A),
    ok = ra:stop_server(C),
    ok = ra:stop_server(D),

    %% there is no quorum now so this should time out
    case ra:consistent_query(B, fun(S) -> S end, 500) of
        {timeout, _} ->
            ok;
        {ok, _, _} ->
            ct:fail("consistent query should time out"),
            ok
    end,
    ok = ra:restart_server(A),
    ok = ra:restart_server(C),
    ok = ra:restart_server(D),
    {ok, 9, _} = ra:consistent_query(A, fun(S) -> S end),
    {ok, 9, _} = ra:consistent_query(B, fun(S) -> S end),
    {ok, 9, _} = ra:consistent_query(C, fun(S) -> S end),
    {ok, 9, _} = ra:consistent_query(D, fun(S) -> S end),
    {ok, 9, _} = ra:consistent_query(E, fun(S) -> S end),

    terminate_cluster(Cluster),
    ok.

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
    ra_env:configure_logger(logger),
    ok = logger:set_primary_config(level, debug),
    LogFile = filename:join(?config(priv_dir, Config), "ra.log"),
    SaslFile = filename:join(?config(priv_dir, Config), "ra_sasl.log"),
    logger:add_handler(ra_handler, logger_std_h, #{config => #{file => LogFile}}),
    application:load(sasl),
    application:set_env(sasl, sasl_error_logger, {file, SaslFile}),
    application:stop(sasl),
    application:start(sasl),
    _ = error_logger:tty(false),
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
    ?assert(try_n_times(fun () -> TryFun(N2Dir) end, 100)),
    ?assert(try_n_times(fun () -> TryFun(N3Dir) end, 100)),
    ?assert(try_n_times(fun () -> TryFun(N1Dir) end, 100)),

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

    meck:new(ra_server, [passthrough]),
    meck:expect(ra_server, handle_follower,
                fun (#install_snapshot_rpc{}, _) ->
                        exit(timeout);
                    (A, B) ->
                        meck:passthrough([A, B])
                end),
    %% start the down node again, catchup should involve sending a snapshot
    ok = ra:restart_server(?SYS, Down),

    timer:sleep(2500),
    meck:unload(ra_server),

    ?assert(try_n_times(
              fun () ->
                      {ok, {N1Idx, _}, _} = ra:local_query(N1, fun ra_lib:id/1),
                      {ok, {N2Idx, _}, _} = ra:local_query(N2, fun ra_lib:id/1),
                      {ok, {N3Idx, _}, _} = ra:local_query(N3, fun ra_lib:id/1),
                      (N1Idx == N2Idx) and (N1Idx == N3Idx)
              end, 200)),
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

send_command_to_follower_during_election() ->
    [{timetrap, {seconds, 20}}].

send_command_to_follower_during_election(Config) ->
    ok = logger:set_primary_config(level, debug),
    ok = logger:update_handler_config(default, #{filter_default => log}),

    Name = ?config(test_name, Config),
    Members = [{n1, node()}, {n2, node()}, {n3, node()}],
    {ok, _, _} = ra:start_cluster(default, Name, add_machine(), Members),
    % issue one command to query leader
    {ok, _, Leader} = ra:process_command({n3, node()}, 5),
    Followers = Members -- [Leader],
    ct:pal("Leader = ~p~nFollowers = ~p", [Leader, Followers]),
    [Follower, _] = Followers,

    % shut down the leader
    % first we ensure that the follower we will use later at least has
    % learnt about the first leader
    ra:members(Follower),

    gen_statem:stop(Leader, normal, 2000),
    % issue command to confirm a new leader is elected
    NewLeader = wait_for_leader(Leader, Follower, 5, 5000),
    ?assertNotEqual(Leader, NewLeader),
    terminate_cluster(Followers).

wait_for_leader(OldLeader, Follower, Command, Timeout) ->
    case ra:process_command(Follower, Command, Timeout) of
        {ok, _, NewLeader} ->
            NewLeader;
        {timeout, Follower} ->
            wait_for_leader(OldLeader, Follower, Command, Timeout);
        {timeout, OldLeader} ->
            ct:pal(
              "ERROR: Follower ~0p redirected command to exited "
              "leader on ~0p",
              [Follower, OldLeader]),
            exit({bad_redirect_from_follower_to_exited_leader,
                  #{follower => Follower,
                    exited_leader => OldLeader}});
        {error, noproc} ->
            ct:pal(
              "ERROR: Follower ~0p (probably) redirected command "
              "to exited leader on ~0p",
              [Follower, OldLeader]),
            exit({bad_redirect_from_follower_to_exited_leader,
                  #{follower => Follower,
                    exited_leader => OldLeader}})
    end.

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

transfer_leadership_two_node(Config) ->
    Name = ?config(test_name, Config),
    Members = [{n1, node()}, {n2, node()}],
    {ok, _, _} = ra:start_cluster(default, Name, add_machine(), Members),
    % issue a command
    {ok, _, Leader} = ra:process_command({n2, node()}, 5),
    % transfer leadership
    [NextInLine | _] = Members -- [Leader],
    ct:pal("Transferring leadership from ~p to ~p", [Leader, NextInLine]),
    ok = ra:transfer_leadership(Leader, NextInLine),
    {ok, _, NewLeader} = ra:process_command(NextInLine, 5),
    ?assertEqual(NewLeader, NextInLine),
    ?assertEqual(already_leader, ra:transfer_leadership(NewLeader, NewLeader)),
    ?assertEqual({error, unknown_member}, ra:transfer_leadership(NewLeader, {unknown, node()})),
    terminate_cluster(Members).

new_nonvoter_knows_its_status(Config) ->
    Name = ?config(test_name, Config),
    [N1, N2] = [{n1, node()}, {n2, node()}],
    {ok, _, _} = ra:start_cluster(default, Name, add_machine(), [N1]),
    _ = issue_op(N1, 1),
    validate_state_on_node(N1, 1),

    % grow
    ok = start_and_join_nonvoter(N1, N2),

    % n2 had no time to catch up
    % in server state
    {ok, #{uid := T, membership := promotable}, _} = ra:member_overview(N2),
    {ok,
     #{cluster := #{N2 := #{voter_status := #{membership := promotable,
                                              target := 3,
                                              uid := T}}}},
     _} = ra:member_overview(N1),
    % in ets
    #{servers := #{n1 := #{membership := voter},
                   n2 := #{membership := promotable}}} = ra:overview(?SYS),
    ok.

voter_gets_promoted_consistent_leader(Config) ->
    N1 = nth_server_name(Config, 1),
    N2 = nth_server_name(Config, 2),
    N3 = nth_server_name(Config, 3),

    {ok, _, _}  = ra:start_cluster(default, ?config(test_name, Config), add_machine(), [N1]),
    _ = issue_op(N1, 1),
    validate_state_on_node(N1, 1),

    % grow 1
    ok = start_and_join_nonvoter(N1, N2),
    _ = issue_op(N2, 1),
    validate_state_on_node(N2, 2),

    % grow 2
    ok = start_and_join_nonvoter(N1, N3),
    _ = issue_op(N3, 1),
    validate_state_on_node(N3, 3),

    % all are voters after catch-up
    timer:sleep(100),
    All = [N1, N2, N3],
    % in server state
    lists:map(fun(O) -> ?assertEqual(All, voters(O)) end, overviews(N1)),
    % in ets
    #{servers := Servers} = ra:overview(?SYS),
    lists:map(fun({Name, _}) -> #{Name := #{membership := voter}} = Servers end, All),
    ok.

voter_gets_promoted_new_leader(Config) ->
    N1 = nth_server_name(Config, 1),
    N2 = nth_server_name(Config, 2),
    N3 = nth_server_name(Config, 3),

    {ok, [Leader, _Second], []}  = ra:start_cluster(default, ?config(test_name, Config), add_machine(), [N1, N2]),
    _ = issue_op(N1, 1),
    validate_state_on_node(N1, 1),

    % grow with leadership change
    ok = start_and_join_nonvoter(N1, N3),
    ra:transfer_leadership(Leader, _Second),
    _ = issue_op(N3, 1),
    validate_state_on_node(N3, 2),

    % all are voters after catch-up
    timer:sleep(100),
    All = [N1, N2, N3],
    % in server state
    lists:map(fun(O) -> ?assertEqual(All, voters(O)) end, overviews(N1)),
    % in ets
    #{servers := Servers} = ra:overview(?SYS),
    lists:map(fun({Name, _}) -> #{Name := #{membership := voter}} = Servers end, All),
    ok.

unknown_leader_call(Config) ->
    [A, _B, _C] = Cluster = start_local_cluster(3, ?config(test_name, Config),
                                                {simple, fun erlang:'+'/2, 9}),
    try
        %% Query the leader and deduce a follower.
        {ok, _, Leader} = ra:process_command(A, 5, ?PROCESS_COMMAND_TIMEOUT),
        [Follower | _] = Cluster -- [Leader],
        ct:pal("Leader:   ~0p~nFollower: ~0p", [Leader, Follower]),

        Call = unknown_call,
        ?assertEqual(
           {error, {unsupported_call, Call}},
           ra_server_proc:leader_call(Leader, Call, ?DEFAULT_TIMEOUT)),
        ?assertEqual(
           {error, {unsupported_call, Call}},
           ra_server_proc:leader_call(Follower, Call, ?DEFAULT_TIMEOUT))
    after
        terminate_cluster(Cluster)
    end.

unknown_local_call(Config) ->
    [A, _B, _C] = Cluster = start_local_cluster(3, ?config(test_name, Config),
                                                {simple, fun erlang:'+'/2, 9}),
    try
        %% Query the leader and deduce a follower.
        {ok, _, Leader} = ra:process_command(A, 5, ?PROCESS_COMMAND_TIMEOUT),
        [Follower | _] = Cluster -- [Leader],
        ct:pal("Leader:   ~0p~nFollower: ~0p", [Leader, Follower]),

        Call = unknown_call,
        ?assertEqual(
           {error, {unsupported_call, Call}},
           ra_server_proc:local_call(Leader, Call, ?DEFAULT_TIMEOUT)),
        ?assertEqual(
           {error, {unsupported_call, Call}},
           ra_server_proc:local_call(Follower, Call, ?DEFAULT_TIMEOUT))
    after
        terminate_cluster(Cluster)
    end.

get_gen_statem_status(Ref) ->
    {_, _, _, Misc} = sys:get_status(Ref),
    [{_, State}] = [S || {data, [{"State", S}]} <- lists:last(Misc)],
    proplists:get_value(raft_state, State).

wait_for_gen_statem_status(Ref, ExpectedStatus, Timeout)
  when Timeout >= 0 ->
    case get_gen_statem_status(Ref) of
        ExpectedStatus ->
            ok;
        _OtherStatus when Timeout >= 0 ->
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

start_and_join_nonvoter({ClusterName, _} = ServerRef, {_, _} = New) ->
    UId = ra:new_uid(ra_lib:to_binary(ClusterName)),
    Server = #{id => New, membership => promotable, uid => UId},
    {ok, _, _} = ra:add_member(ServerRef, Server),
    ok = ra:start_server(default, ClusterName, Server, add_machine(), [ServerRef]),
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

overviews(Node) ->
    {ok, Members, _From} = ra:members(Node),
    [ra:member_overview(P) || {_, _} = P <- Members].

voters({ok, #{cluster := Peers}, _} = _Overview) ->
    [Id || {Id, Status} <- maps:to_list(Peers), maps:get(membership, Status, voter) == voter].

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
              flush(),
              exit(ra_event_timeout)
    end;
gather_applied(Acc, Timeout) ->
    receive
        {ra_event, _Leader, {applied, Corrs}} ->
            gather_applied(Acc ++ Corrs, Timeout)
    after Timeout ->
              Acc
    end.

start_remote_cluster(Num, PrivDir, ClusterName, Machine) ->
    Nodes = [begin
                 Name = "node" ++ erlang:integer_to_list(N),
                 Node = start_peer(Name, PrivDir),
                 {ClusterName, Node}
             end || N <- lists:seq(1, Num)],
    {ok, _, Failed} = ra:start_cluster(default, ClusterName, Machine, Nodes),
    ?assertEqual([], Failed),
    Nodes.

start_peer(Name, PrivDir) ->
    Dir0 = filename:join(PrivDir, Name),
    Dir = "'\"" ++ Dir0 ++ "\"'",
    Host = get_current_host(),
    Pa = string:join(["-pa" | search_paths()] ++ ["-s ra -ra data_dir", Dir],
                     " "),
    ct:pal("starting peer node ~ts on host ~s for node ~ts with ~ts",
           [Name, Host, node(), Pa]),
    {ok, S} = slave:start_link(Host, Name, Pa),
    _ = rpc:call(S, ra, start, []),
    ok = ct_rpc:call(S, logger, set_primary_config,
                     [level, all]),
    S.

get_current_host() ->
    NodeStr = atom_to_list(node()),
    Host = re:replace(NodeStr, "^[^@]+@", "", [{return, list}]),
    list_to_atom(Host).

search_paths() ->
    Ld = code:lib_dir(),
    lists:filter(fun (P) -> string:prefix(P, Ld) =:= nomatch end,
                 code:get_path()).

await_condition(_Fun, 0) ->
    exit(condition_did_not_materialise);
await_condition(Fun, Attempts) ->
    case catch Fun() of
        true -> ok;
        _ ->
            timer:sleep(100),
            await_condition(Fun, Attempts - 1)
    end.
