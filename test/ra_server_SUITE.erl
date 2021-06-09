%% This Source Code Form is subject to the terms of the Mozilla Public
%%
%% Copyright (c) 2017-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%
-module(ra_server_SUITE).

-compile(export_all).

-include("src/ra.hrl").
-include("src/ra_server.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

all() ->
    [
     init_test,
     recover_restores_cluster_changes,
     election_timeout,
     follower_aer_term_mismatch,
     follower_aer_term_mismatch_snapshot,
     follower_handles_append_entries_rpc,
     candidate_handles_append_entries_rpc,
     append_entries_reply_success,
     append_entries_reply_no_success,
     follower_request_vote,
     follower_pre_vote,
     pre_vote_receives_pre_vote,
     request_vote_rpc_with_lower_term,
     leader_does_not_abdicate_to_unknown_peer,
     higher_term_detected,
     pre_vote_election,
     pre_vote_election_reverts,
     leader_receives_pre_vote,
     candidate_election,
     is_new,
     command,
     leader_noop_operation_enables_cluster_change,
     leader_noop_increments_machine_version,
     follower_machine_version,
     leader_server_join,
     leader_server_leave,
     leader_is_removed,
     follower_cluster_change,
     leader_applies_new_cluster,
     leader_appends_cluster_change_then_steps_before_applying_it,
     leader_receives_install_snapshot_rpc,
     follower_installs_snapshot,
     follower_receives_stale_snapshot,
     receive_snapshot_timeout,
     snapshotted_follower_received_append_entries,
     leader_received_append_entries_reply_with_stale_last_index,
     leader_receives_install_snapshot_result,
     leader_replies_to_append_entries_rpc_with_lower_term,
     follower_aer_1,
     follower_aer_2,
     follower_aer_3,
     follower_aer_4,
     follower_aer_5,
     follower_catchup_condition,
     wal_down_condition,
     update_release_cursor,

     follower_heartbeat,
     follower_heartbeat_reply,
     candidate_heartbeat,
     candidate_heartbeat_reply,
     pre_vote_heartbeat,
     pre_vote_heartbeat_reply,

     leader_heartbeat,
     leader_heartbeat_reply_same_term,
     leader_heartbeat_reply_node_size_5,
     leader_heartbeat_reply_lower_term,
     leader_heartbeat_reply_higher_term,
     leader_consistent_query_delay,
     leader_consistent_query,
     await_condition_heartbeat_dropped,
     await_condition_heartbeat_reply_dropped,
     receive_snapshot_heartbeat_dropped,
     receive_snapshot_heartbeat_reply_dropped,

     handle_down
    ].

-define(MACFUN, fun (E, _) -> E end).
-define(N1, {n1, node()}).
-define(N2, {n2, node()}).
-define(N3, {n3, node()}).
-define(N4, {n4, node()}).
-define(N5, {n5, node()}).

groups() ->
    [ {tests, [], all()} ].

init_per_suite(Config) ->
    ok = logger:set_primary_config(level, all),
    Config.

end_per_suite(Config) ->
    Config.

init_per_testcase(TestCase, Config) ->
    ok = logger:set_primary_config(level, all),
    ok = setup_log(),
    [{test_case, TestCase} | Config].

end_per_testcase(_TestCase, Config) ->
    meck:unload(),
    Config.

id(X) -> X.

ra_server_init(Conf) ->
    ra_server:recover(ra_server:init(Conf)).

setup_log() ->
    ok = meck:new(ra_log, []),
    ok = meck:new(ra_snapshot, [passthrough]),
    ok = meck:new(ra_machine, [passthrough]),
    meck:expect(ra_log, init, fun(C) -> ra_log_memory:init(C) end),
    meck:expect(ra_log_meta, store, fun (_, U, K, V) ->
                                            put({U, K}, V), ok
                                    end),
    meck:expect(ra_log_meta, store_sync, fun (_, U, K, V) ->
                                                 put({U, K}, V), ok
                                         end),
    meck:expect(ra_log_meta, fetch, fun(_, U, K) ->
                                            get({U, K})
                                    end),
    meck:expect(ra_log_meta, fetch, fun (_, U, K, D) ->
                                            ra_lib:default(get({U, K}), D)
                                    end),
    meck:expect(ra_snapshot, begin_accept,
                fun(_Meta, SS) ->
                        {ok, SS}
                end),
    meck:expect(ra_snapshot, accept_chunk,
                fun(_Data, _OutOf, _Flag, SS) ->
                        {ok, SS}
                end),
    meck:expect(ra_snapshot, abort_accept, fun(SS) -> SS end),
    meck:expect(ra_snapshot, accepting, fun(_SS) -> undefined end),
    meck:expect(ra_log, snapshot_state, fun (_) -> snap_state end),
    meck:expect(ra_log, set_snapshot_state, fun (_, Log) -> Log end),
    meck:expect(ra_log, install_snapshot, fun (_, _, Log) -> {Log, []} end),
    meck:expect(ra_log, recover_snapshot, fun ra_log_memory:recover_snapshot/1),
    meck:expect(ra_log, snapshot_index_term, fun ra_log_memory:snapshot_index_term/1),
    meck:expect(ra_log, take, fun ra_log_memory:take/3),
    meck:expect(ra_log, release_resources, fun ra_log_memory:release_resources/3),
    meck:expect(ra_log, append_sync,
                fun({Idx, Term, _} = E, L) ->
                        L1 = ra_log_memory:append(E, L),
                        {LX, _} = ra_log_memory:handle_event({written, {Idx, Idx, Term}}, L1),
                        LX
                end),
    meck:expect(ra_log, write_config, fun ra_log_memory:write_config/2),
    meck:expect(ra_log, next_index, fun ra_log_memory:next_index/1),
    meck:expect(ra_log, append, fun ra_log_memory:append/2),
    meck:expect(ra_log, write, fun ra_log_memory:write/2),
    meck:expect(ra_log, handle_event, fun ra_log_memory:handle_event/2),
    meck:expect(ra_log, last_written, fun ra_log_memory:last_written/1),
    meck:expect(ra_log, last_index_term, fun ra_log_memory:last_index_term/1),
    meck:expect(ra_log, set_last_index, fun ra_log_memory:set_last_index/2),
    meck:expect(ra_log, fetch_term, fun ra_log_memory:fetch_term/2),
    meck:expect(ra_log, exists,
                fun ({Idx, Term}, L) ->
                        case ra_log_memory:fetch_term(Idx, L) of
                            {Term, Log} -> {true, Log};
                            {_, Log} -> {false, Log}
                        end
                end),
    meck:expect(ra_log, update_release_cursor,
                fun ra_log_memory:update_release_cursor/5),
    ok.

init_test(_Config) ->
    #{cluster := Cluster,
      current_term := CurrentTerm,
      log := Log0} = Base = base_state(3, ?FUNCTION_NAME),
    Id = ra_server:id(Base),
    UId = ra_server:uid(Base),

    % ensure it is written to the log
    InitConf = #{cluster_name => init,
                 id => Id,
                 uid => UId,
                 log_init_args => #{uid => <<>>},
                 machine => {module, ?FUNCTION_NAME, #{}},
                 initial_members => []}, % init without known peers
    % new
    #{current_term := 0,
      voted_for := undefined} = ra_server_init(InitConf),
    % previous data
    ok = ra_log_meta:store(ra_log_meta, UId, voted_for, some_server),
    ok = ra_log_meta:store(ra_log_meta, UId, current_term, CurrentTerm),
    meck:expect(ra_log, init, fun (_) -> Log0 end),
    #{current_term := 5,
      voted_for := some_server} = ra_server_init(InitConf),
    % snapshot
    SnapshotMeta = #{index => 3, term => 5,
                     cluster => maps:keys(Cluster),
                     machine_version => 1},
    SnapshotData = "hi1+2+3",
    {LogS, _, _} = ra_log_memory:install_snapshot(SnapshotMeta, SnapshotData, Log0),
    meck:expect(ra_log, init, fun (_) -> LogS end),
    #{current_term := 5,
      commit_index := 3,
      machine_state := "hi1+2+3",
      cluster := ClusterOut,
      voted_for := some_server} = ra_server_init(InitConf),
    ?assertEqual(maps:keys(Cluster), maps:keys(ClusterOut)),
    ok.

recover_restores_cluster_changes(_Config) ->
    N1 = ?N1, N2 = ?N2,
    InitConf = #{cluster_name => ?FUNCTION_NAME,
                 id => N1,
                 uid => <<"n1">>,
                 log_init_args => #{uid => <<>>},
                 machine => {simple, fun erlang:'+'/2, 0},
                 initial_members => []}, % init without known peers
    % new
    {leader, State00, _} =
    ra_server:handle_candidate(#request_vote_result{term = 1,
                                                    vote_granted = true},
                               (ra_server_init(InitConf))#{votes => 0,
                                                           current_term => 1,
                                                           voted_for => N1}),
    {leader, State0 = #{cluster := Cluster0,
                        current_term := 1}, _} =
        ra_server:handle_leader({command, {noop, meta(), 0}}, State00),
    ?assert(maps:size(Cluster0) =:= 1),
    {leader, State, _} = ra_server:handle_leader(written_evt({1, 1, 1}), State0),
    ?assertMatch(#{current_term := 1}, State),
    ?assertMatch(#{cluster_change_permitted := true}, State),

    % n2 joins
    {leader, #{cluster := Cluster,
               log := Log0}, _} =
        ra_server:handle_leader({command, {'$ra_join', meta(),
                                           N2, await_consensus}}, State),
    ?assertEqual(2, maps:size(Cluster)),
    % intercept ra_log:init call to simulate persisted log data
    % ok = meck:new(ra_log, [passthrough]),
    meck:expect(ra_log, init, fun (_) -> Log0 end),
    meck:expect(ra_log_meta, fetch,
                fun (_, _, last_applied, 0) ->
                        element(1, ra_log:last_index_term(Log0));
                    (_, _, _, Def) ->
                        Def
                end),

    #{cluster := #{?N1 := _, N2 := _}} = ra_server_init(InitConf),
    ok.

election_timeout(_Config) ->
    N1 = ?N1, N2 = ?N2, N3 = ?N3,
    State = base_state(3, ?FUNCTION_NAME),
    Msg = election_timeout,
    % follower
    {pre_vote, #{current_term := 5, votes := 0,
                 pre_vote_token := Token},
     [{next_event, cast, #pre_vote_result{term = 5, token = Token,
                                          vote_granted = true}},
      {send_vote_requests,
       [{N2, #pre_vote_rpc{term = 5, token = Token,
                           last_log_index = 3,
                           last_log_term = 5,
                           candidate_id = N1}},
        {N3, _}]}]} =
        ra_server:handle_follower(Msg, State),

    % pre_vote
    {pre_vote, #{current_term := 5, votes := 0,
                 pre_vote_token := Token1},
     [{next_event, cast, #pre_vote_result{term = 5, token = Token1,
                                          vote_granted = true}},
      {send_vote_requests,
       [{N2, #pre_vote_rpc{term = 5, token = Token1,
                           last_log_index = 3,
                           last_log_term = 5,
                           candidate_id = N1}},
        {N3, _}]}]} =
        ra_server:handle_pre_vote(Msg, State),

    %% assert tokens are not the same
    ?assertNotEqual(Token, Token1),

    % candidate
    VoteRpc = #request_vote_rpc{term = 6, candidate_id = N1,
                                last_log_index = 3, last_log_term = 5},
    VoteForSelfEvent = {next_event, cast,
                        #request_vote_result{term = 6, vote_granted = true}},
    {candidate, #{current_term := 6, votes := 0},
     [VoteForSelfEvent, {send_vote_requests, [{N2, VoteRpc}, {N3, VoteRpc}]}]} =
        ra_server:handle_candidate(Msg, State),
    ok.

follower_aer_1(_Config) ->
    % Scenario 1
    N1 = ?N1,
    Self = N1,

    % AER with index [1], commit = 0, commit_index = 0
    Init = empty_state(3, n1),
    AER1 = #append_entries_rpc{term = 1, leader_id = N1, prev_log_index = 0,
                               prev_log_term = 0, leader_commit = 0,
                               entries = [entry(1, 1, one)]},
    {follower, State1 = #{leader_id := N1, current_term := 1,
                          commit_index := 0, last_applied := 0}, _} =
    ra_server:handle_follower(AER1, Init),

    % AER with index [2], leader_commit = 1, commit_index = 1
    AER2 = #append_entries_rpc{term = 1, leader_id = N1, prev_log_index = 1,
                               prev_log_term = 1, leader_commit = 1,
                               entries = [entry(2, 1, two)]},
    {follower, State2 = #{leader_id := N1, current_term := 1,
                          commit_index := 1, last_applied := 1,
                          %% validate entry was applied to machine
                          machine_state := {simple, _, one}},
     _} = ra_server:handle_follower(AER2, State1),

    % {written, 1} -> last_applied: 1 - replies with last_index = 1, next_index = 3
    {follower, State3 = #{leader_id := N1, current_term := 1,
                          commit_index := 1, last_applied := 1,
                          machine_state := {simple, _, one}},
     [{cast, N1, {Self, #append_entries_reply{next_index = 3,
                                              last_term = 1,
                                              last_index = 1}}}]}
        = ra_server:handle_follower({ra_log_event, {written, {1, 1, 1}}}, State2),

    % AER with index [3], commit = 3 -> commit_index = 3
    AER3 = #append_entries_rpc{term = 1, leader_id = N1, prev_log_index = 2,
                               prev_log_term = 1, leader_commit = 3,
                               entries = [entry(3, 1, tre)]},
    {follower, State4 = #{leader_id := N1, current_term := 1,
                          commit_index := 3, last_applied := 3,
                          machine_state := {simple, _, tre}},
     _} = ra_server:handle_follower(AER3, State3),

    % {written, 2} -> last_applied: 3, commit_index = 3 reply = 2, next_index = 4
    {follower, State5 = #{leader_id := N1, current_term := 1,
                          commit_index := 3, last_applied := 3,
                          machine_state := {_, _, tre}},
     [{cast, N1, {Self, #append_entries_reply{next_index = 4,
                                              last_term = 1,
                                              last_index = 2}}}]}
        = ra_server:handle_follower({ra_log_event, {written, {2, 2, 1}}}, State4),

    ok = logger:set_primary_config(level, all),
    % AER with index [] -> last_applied: 2 - replies with last_index = 2,
        % next_index = 4
    % empty AER before {written, 3} is received
    AER4 = #append_entries_rpc{term = 1, leader_id = N1, prev_log_index = 3,
                               prev_log_term = 1, leader_commit = 3,
                               entries = []},
    {follower, State6 = #{leader_id := N1, current_term := 1,
                          commit_index := 3, last_applied := 3,
                          machine_state := {simple, _, tre}},
     [{cast, N1, {Self, #append_entries_reply{next_index = 4,
                                              last_term = 1,
                                              last_index = 2}}} | _]}
        = ra_server:handle_follower(AER4, State5),

    % {written, 3} -> commit_index = 3, last_applied = 3 : reply last_index = 3
    {follower, #{leader_id := N1, current_term := 1,
                 commit_index := 3, last_applied := 3,
                 machine_state := {_, _, tre}},
     [{cast, N1, {Self, #append_entries_reply{next_index = 4,
                                              last_term = 1,
                                              last_index = 3}}}]}
        = ra_server:handle_follower({ra_log_event, {written, {3, 3, 1}}}, State6),
    ok.

follower_aer_2(_Config) ->
    N1 = ?N1, N2 = ?N2,
    % Scenario 2
    % empty AER applies previously replicated entry
    % AER with index [1], leader_commit = 0
    Init = empty_state(3, n2),
    AER1 = #append_entries_rpc{term = 1, leader_id = N1, prev_log_index = 0,
                               prev_log_term = 0, leader_commit = 0,
                               entries = [entry(1, 1, one)]},
    {follower, State1 = #{leader_id := N1, current_term := 1,
                          commit_index := 0, last_applied := 0},
     _} = ra_server:handle_follower(AER1, Init),

    % {written, 1} -> last_applied: 0, reply: last_applied = 1, next_index = 2
    {follower, State2 = #{leader_id := N1, current_term := 1,
                          commit_index := 0, last_applied := 0,
                          machine_state := {simple, _, <<>>}},
     [{cast, N1, {N2, #append_entries_reply{next_index = 2,
                                            last_term = 1,
                                            last_index = 1}}}]}
        = ra_server:handle_follower({ra_log_event, {written, {1, 1, 1}}}, State1),

    % AER with index [], leader_commit = 1 -> last_applied: 1, reply: last_index = 1, next_index = 2
    AER2 = #append_entries_rpc{term = 1, leader_id = N1, prev_log_index = 1,
                               prev_log_term = 1, leader_commit = 1,
                               entries = []},
    {follower, #{leader_id := N1, current_term := 1,
                 commit_index := 1, last_applied := 1,
                 machine_state := {simple, _, one}},
     _} = ra_server:handle_follower(AER2, State2),
    ok.

follower_aer_3(_Config) ->
    N1 = ?N1, N2 = ?N2,
    % Scenario 3
    % AER with index [1], commit_index = 1
    Init = empty_state(3, n2),
    AER1 = #append_entries_rpc{term = 1, leader_id = N1, prev_log_index = 0,
                               prev_log_term = 0, leader_commit = 1,
                               entries = [entry(1, 1, one)]},
    {follower, State1 = #{leader_id := N1, current_term := 1,
                          commit_index := 1, last_applied := 1},
     _} = ra_server:handle_follower(AER1, Init),
    % {written, 1} -> last_applied: 1 - reply: last_index = 1, next_index = 2
    {follower, State2 = #{leader_id := N1, current_term := 1,
                          commit_index := 1, last_applied := 1,
                          machine_state := {simple, _, one}},
     [{cast, N1, {N2, #append_entries_reply{next_index = 2,
                                            last_term = 1,
                                            last_index = 1}}}]}
        = ra_server:handle_follower({ra_log_event, {written, {1, 1, 1}}}, State1),
    % AER with index [3] -> last_applied = 1 - reply(false):
    % last_index, 1, next_index = 2
    AER2 = #append_entries_rpc{term = 1, leader_id = N1, prev_log_index = 2,
                               prev_log_term = 1, leader_commit = 3,
                               entries = [entry(3, 1, tre)]},
    {await_condition, State3 = #{leader_id := N1, current_term := 1,
                                 commit_index := 3, last_applied := 1},
     [{cast, N1, {N2, #append_entries_reply{next_index = 2,
                                            success = false,
                                            last_term = 1,
                                            last_index = 1}}},
      {record_leader_msg, N1}]}
        = ra_server:handle_follower(AER2, State2),
    % AER with index [2,3,4], commit_index = 3 -> commit_index = 3
    AER3 = #append_entries_rpc{term = 1, leader_id = N1, prev_log_index = 1,
                               prev_log_term = 1, leader_commit = 3,
                               entries = [
                                          entry(2, 1, two),
                                          entry(3, 1, tre),
                                          entry(4, 1, for)
                                         ]},
    {follower, State4 = #{leader_id := N1, current_term := 1,
                          commit_index := 3, last_applied := 3}, _}
    = ra_server:handle_follower(AER3, State3),
    % {written, 4} -> last_applied: 3 - reply: last_index = 4, next_index = 5
    {follower, State5 = #{leader_id := N1, current_term := 1,
                          commit_index := 3, last_applied := 3,
                          machine_state := {_, _, tre}},
     [{cast, N1, {N2, #append_entries_reply{next_index = 5,
                                            success = true,
                                            last_term = 1,
                                            last_index = 4}}} | _]}
    = ra_server:handle_follower({ra_log_event, {written, {4, 4, 1}}}, State4),

    % AER with index [2,3,4], commit_index = 4
    % async failed AER reverted back leader's next_index for follower
    % however an updated commit index is now known
    AER4 = #append_entries_rpc{term = 1, leader_id = N1, prev_log_index = 1,
                               prev_log_term = 1, leader_commit = 4,
                               entries = [
                                          entry(2, 1, two),
                                          entry(3, 1, tre),
                                          entry(4, 1, for)
                                         ]},
    {follower, #{leader_id := N1, current_term := 1, commit_index := 4,
                 last_applied := 4, machine_state := {_, _, for}}, _}
    = ra_server:handle_follower(AER4, State5),
    % TODO: scenario where the batch is partially already seen and partiall not
    % + increments commit_index
    ok.

follower_aer_4(_Config) ->
    N1 = ?N1, N2 = ?N2,
    % Scenario 4 - commit index
    % AER with index [1,2,3,4], commit_index = 10 -> commit_index = 4,
    % last_applied = 4
    % follower catching up scenario
    Init = empty_state(3, n2),
    AER1 = #append_entries_rpc{term = 1, leader_id = N1, prev_log_index = 0,
                               prev_log_term = 0, leader_commit = 10,
                               entries = [
                                          entry(1, 1, one),
                                          entry(2, 1, two),
                                          entry(3, 1, tre),
                                          entry(4, 1, for)
                                         ]},
    {follower, State1 = #{leader_id := N1, current_term := 1,
                          commit_index := 10, last_applied := 4},
     _} = ra_server:handle_follower(AER1, Init),
    % {written, 4} -> last_applied = 4, commit_index = 4
    {follower, _State2 = #{leader_id := N1, current_term := 1,
                           commit_index := 10, last_applied := 4,
                           machine_state := {_, _, for}},
     [{cast, N1, {N2, #append_entries_reply{next_index = 5,
                                            last_term = 1,
                                            last_index = 4}}}]}
        = ra_server:handle_follower({ra_log_event, {written, {4, 4, 1}}}, State1),
    % AER with index [5], commit_index = 10 -> last_applied = 4, commit_index = 5
    ok.

follower_aer_5(_Config) ->
    N1 = ?N1, N5 = ?N5,
    %% Scenario
    %% Leader with smaller log is elected and sends empty aer
    %% Follower should truncate it's log and reply with an appropriate
    %% next index
    Init = empty_state(3, n2),
    AER1 = #append_entries_rpc{term = 1, leader_id = N1, prev_log_index = 0,
                               prev_log_term = 0, leader_commit = 10,
                               entries = [
                                          entry(1, 1, one),
                                          entry(2, 1, two),
                                          entry(3, 1, tre),
                                          entry(4, 1, for)
                                         ]},
    %% set up follower state
    {follower, State00, _} = ra_server:handle_follower(AER1, Init),
    %% TODO also test when written even occurs after
    {follower, State0, _} = ra_server:handle_follower(
                              {ra_log_event, {written, {4, 4, 1}}}, State00),
    % now an AER from another leader in a higher term is received
    % This is what the leader sends immedately before committing it;s noop
    AER2 = #append_entries_rpc{term = 2, leader_id = N5, prev_log_index = 3,
                               prev_log_term = 1, leader_commit = 3,
                               entries = []},
    {follower, _State1, Effects} = ra_server:handle_follower(AER2, State0),
    {cast, N5, {_, M}} = hd(Effects),
    ?assertMatch(#append_entries_reply{next_index = 4,
                                       last_term = 1,
                                       last_index = 3}, M),
    % ct:pal("Effects ~p~n State: ~p", [Effects, State1]),
    ok.



follower_aer_term_mismatch(_Config) ->
    State = (base_state(3, ?FUNCTION_NAME))#{last_applied => 2,
                                             commit_index => 3},
    AE = #append_entries_rpc{term = 6,
                             leader_id = ?N1,
                             prev_log_index = 3,
                             prev_log_term = 6, % higher log term
                             leader_commit = 3},

    % term mismatch scenario follower has index 3 but for different term
    % rewinds back to last_applied + 1 as next index and enters await condition
    {await_condition, #{condition := _},
     [{_, _, {_, Reply}} | _]} = ra_server:handle_follower(AE, State),
    ?assertMatch(#append_entries_reply{term = 6,
                                       success = false,
                                       next_index = 3,
                                       last_index = 2,
                                       last_term = 3}, Reply),
                 ok.

follower_aer_term_mismatch_snapshot(_Config) ->
    %% case when we have to revert all the way back to a snapshot
    State0 = (base_state(3, ?FUNCTION_NAME))#{last_applied => 3,
                                              commit_index => 3
                                             },

    Log0 = maps:get(log, State0),
    Meta = #{index => 3,
             term => 5,
             cluster => [],
             machine_version => 1},
    Data = <<"hi3">>,
    {Log, _, _} = ra_log_memory:install_snapshot(Meta, Data, Log0),
    State = maps:put(log, Log, State0),

    AE = #append_entries_rpc{term = 6,
                             leader_id = ?N1,
                             prev_log_index = 3,
                             prev_log_term = 6, % higher log term
                             leader_commit = 3},

    % term mismatch scenario follower has index 3 but for different term
    % rewinds back to last_applied + 1 as next index and enters await condition
    {await_condition, #{condition := _},
     [{_, _, {_, Reply}} | _]} = ra_server:handle_follower(AE, State),

    ?assertMatch(#append_entries_reply{term = 6,
                                       success = false,
                                       next_index = 4,
                                       last_index = 3,
                                       last_term = 5}, Reply),
                 ok.

follower_handles_append_entries_rpc(_Config) ->
    N1 = ?N1,
    State = (base_state(3, ?FUNCTION_NAME))#{commit_index => 1},
    EmptyAE = #append_entries_rpc{term = 5,
                                  leader_id = ?N1,
                                  prev_log_index = 3,
                                  prev_log_term = 5,
                                  leader_commit = 3},


    % success case - everything is up to date leader id got updated
    {follower, #{leader_id := N1, current_term := 5}, _} =
        ra_server:handle_follower(EmptyAE, State),

    % success case when leader term is higher
    % reply term should be updated
    % replies for empty rpcs are sent immediately
   {follower, #{leader_id := N1, current_term := 6} = _State1,
        [{cast, N1, {N1, #append_entries_reply{term = 6, success = true,
                                               next_index = 4, last_index = 3,
                                               last_term = 5}}} | _]}
       = ra_server:handle_follower(EmptyAE#append_entries_rpc{term = 6}, State),

    % reply false if term < current_term (5.1)
    {follower, _, [{cast, N1, {N1, #append_entries_reply{term = 5, success = false}}}]}
        = ra_server:handle_follower(EmptyAE#append_entries_rpc{term = 4}, State),

    % reply false if log doesn't contain a term matching entry at prev_log_index
    {await_condition, _,
     [{cast, N1, {N1, #append_entries_reply{term = 5, success = false}}},
      %% it was still a valid leader message only that we need to back up a bit
      {record_leader_msg, _}]}
        = ra_server:handle_follower(EmptyAE#append_entries_rpc{prev_log_index = 4},
                                    State),
    % there is an entry but not with a macthing term
    {await_condition, _, [{cast, N1, {N1, #append_entries_reply{term = 5, success = false}}},
                          {record_leader_msg, _}]}
        = ra_server:handle_follower(EmptyAE#append_entries_rpc{prev_log_term = 4},
                                  State),

    % truncate/overwrite if a existing entry conflicts (diff term) with
    % a new one (5.3)
    AE = #append_entries_rpc{term = 5, leader_id = ?N1,
                             prev_log_index = 1, prev_log_term = 1,
                             leader_commit = 2,
                             entries = [{2, 4, {'$usr', meta(), <<"hi">>,
                                                after_log_append}}]},

    {follower,  #{log := Log},
     [{cast, N1, {N1, #append_entries_reply{term = 5, success = true,
                                            next_index = 3, last_index = 2,
                                            last_term = 4}}}]}
    = begin
          {follower, Inter3, _} =
              ra_server:handle_follower(AE, State#{last_applied => 1}),
          ra_server:handle_follower({ra_log_event, {written, {2, 2, 4}}}, Inter3)
      end,
    [{0, 0, undefined},
     {1, 1, _}, {2, 4, _}] = ra_log_memory:to_list(Log),

    % append new entries not in the log
    % if leader_commit > the last entry received ensure last_applied does not
    % match commit_index
    % ExpectedLogEntry = usr(<<"hi4">>),
    {follower, #{commit_index := 5, last_applied := 4,
                 machine_state := <<"hi4">>},
     [{cast, N1, {N1, #append_entries_reply{term = 5, success = true,
                                            last_index = 4,
                                            last_term = 5}}}]}
    = begin
          {follower, Inter4, _}
        = ra_server:handle_follower(
            EmptyAE#append_entries_rpc{entries = [{4, 5, usr(<<"hi4">>)}],
                                       leader_commit = 5},
            State#{commit_index => 1, last_applied => 1,
                   machine_state => <<"hi1">>}),
          ra_server:handle_follower(written_evt({4, 4, 5}), Inter4)
      end,
    ok.

follower_catchup_condition(_Config) ->
    N1 = ?N1,
    State0 = (base_state(3, ?FUNCTION_NAME))#{commit_index => 1},
    EmptyAE = #append_entries_rpc{term = 5,
                                  leader_id = N1,
                                  prev_log_index = 3,
                                  prev_log_term = 5,
                                  leader_commit = 3},

    % from follower to await condition
    {await_condition, State = #{condition := _}, _} =
    ra_server:handle_follower(EmptyAE#append_entries_rpc{term = 5,
                                                         prev_log_index = 4},
                              State0),

    % append entry with a lower leader term should not enter await condition
    % even if prev_log_index is higher than last index
    {follower, _, [_]}
    = ra_server:handle_follower(EmptyAE#append_entries_rpc{term = 4,
                                                           prev_log_index = 4},
                                State),

    % append entry when prev log index exists but the term is different should
    % not also enter await condition as it then rewinds and request resends
    % of all log entries since last known commit index
    {await_condition, _, [_, _]}
    = ra_server:handle_follower(EmptyAE#append_entries_rpc{term = 6,
                                                           prev_log_term = 4,
                                                           prev_log_index = 3},
                                State),

    % append entry when term is ok but there is a gap should remain in await
    % condition we do not want to send a reply here
    {await_condition, _, []} =
    ra_server:handle_await_condition(EmptyAE#append_entries_rpc{term = 5,
                                                                prev_log_index = 4},
                                     State),

    % success case - it transitions back to follower state
    {follower, _, [{next_event, EmptyAE}]} =
        ra_server:handle_await_condition(EmptyAE, State),


    ISRpc = #install_snapshot_rpc{term = 99, leader_id = N1,
                                  chunk_state = {1, last},
                                  meta = #{index => 99,
                                           term => 99,
                                           cluster => [],
                                           machine_version => 0},
                                  data = []},
    {follower, State, [_NextEvent]} =
        ra_server:handle_await_condition(ISRpc, State),

    {await_condition, State, []} =
        ra_server:handle_await_condition({ra_log_event, {written, {99, 99, 99}}}, State),

    Msg = #request_vote_rpc{candidate_id = ?N2, term = 6, last_log_index = 3,
                            last_log_term = 5},
    {follower, State, [{next_event, Msg}]} =
        ra_server:handle_await_condition(Msg, State),
    {follower, _, [{cast, N1, {N1, #append_entries_reply{success = false,
                                                         next_index = 4}}},
                   {record_leader_msg, _}]}
    = ra_server:handle_await_condition(await_condition_timeout, State),

    {pre_vote, _, _} = ra_server:handle_await_condition(election_timeout, State),
    ok.


wal_down_condition(_Config) ->
    N1 = ?N1,
    State0 = (base_state(3, ?FUNCTION_NAME))#{commit_index => 1},
    EmptyAE = #append_entries_rpc{term = 5,
                                  leader_id = N1,
                                  prev_log_index = 3,
                                  prev_log_term = 5,
                                  leader_commit = 3},

    % meck:new(ra_log, [passthrough]),
    meck:expect(ra_log, write, fun (_Es, _L) -> {error, wal_down} end),
    meck:expect(ra_log, can_write, fun (_L) -> false end),

    % ra log fails
    {await_condition, State = #{condition := _}, [{record_leader_msg, _}]}
    = ra_server:handle_follower(EmptyAE#append_entries_rpc{entries = [{4, 5, yo}]}, State0),

    % stay in await condition as ra_log_wal is not available
    {await_condition, State = #{condition := _}, []}
    = ra_server:handle_await_condition(EmptyAE#append_entries_rpc{entries = [{4, 5, yo}]}, State),

    meck:expect(ra_log, can_write, fun (_L) -> true end),
    % exit condition
    {follower, _State, [_]}
    = ra_server:handle_await_condition(EmptyAE#append_entries_rpc{entries = [{4, 5, yo}]}, State),
    ok.

update_release_cursor(_Config) ->
    State00 = #{cfg := Cfg} = (base_state(3, ?FUNCTION_NAME)),
    meck:expect(?FUNCTION_NAME, version, fun () -> 1 end),
    State0 = State00#{cfg => Cfg#cfg{machine_versions = [{1, 1}, {0, 0}]}},
    %% need to match on something for this macro
    ?assertMatch({#{cfg := #cfg{machine_version = 0}}, []},
                 ra_server:update_release_cursor(2, some_state, State0)),
    ok.

candidate_handles_append_entries_rpc(_Config) ->
    N1 = ?N1,
    State = (base_state(3, ?FUNCTION_NAME))#{commit_index => 1},
    EmptyAE = #append_entries_rpc{term = 4,
                                  leader_id = ?N1,
                                  prev_log_index = 3,
                                  prev_log_term = 5,
                                  leader_commit = 3},
    % Lower term
    {candidate, _,
     [{cast, N1, {N1, #append_entries_reply{term = 5, success = false,
                                            last_index = 3, last_term = 5}}}]}
    = ra_server:handle_candidate(EmptyAE, State),
    ok.

append_entries_reply_success(_Config) ->

    N1 = ?N1, N2 = ?N2, N3 = ?N3,
    Cluster = #{N1 => new_peer_with(#{next_index => 5, match_index => 4}),
                N2 => new_peer_with(#{next_index => 1, match_index => 0,
                                      commit_index_sent => 3}),
                N3 => new_peer_with(#{next_index => 2, match_index => 1})},
    State = (base_state(3, ?FUNCTION_NAME))#{commit_index => 1,
                             last_applied => 1,
                             cluster => Cluster,
                             machine_state => <<"hi1">>},
    Msg = {N2, #append_entries_reply{term = 5, success = true,
                                     next_index = 4,
                                     last_index = 3, last_term = 5}},
    % update match index
    {leader, #{cluster := #{N2 := #{next_index := 4, match_index := 3}},
               commit_index := 3,
               last_applied := 3,
               machine_state := <<"hi3">>},
     [{aux, eval},
      {send_rpc, N3,
       #append_entries_rpc{term = 5, leader_id = N1,
                           prev_log_index = 1,
                           prev_log_term = 1,
                           leader_commit = 3,
                           entries = [{2, 3, {'$usr', _, <<"hi2">>, _}},
                                      {3, 5, {'$usr', _, <<"hi3">>, _}}]}
      }]} = ra_server:handle_leader(Msg, State),

    Msg1 = {N2, #append_entries_reply{term = 7, success = true,
                                      next_index = 4,
                                      last_index = 3, last_term = 5}},
    {leader, #{cluster := #{N2 := #{next_index := 4,
                                             match_index := 3}},
               commit_index := 1,
               last_applied := 1,
               current_term := 7,
               machine_state := <<"hi1">>}, _} =
        ra_server:handle_leader(Msg1, State#{current_term := 7}),
    ok.

append_entries_reply_no_success(_Config) ->
    N1 = ?N1, N2 = ?N2, N3 = ?N3,
    % decrement next_index for peer if success = false
    Cluster = #{N1 => new_peer(),
                N2 => new_peer_with(#{next_index => 3, match_index => 0}),
                N3 => new_peer_with(#{next_index => 2, match_index => 1,
                                      commit_index_sent => 1})},
    State = (base_state(3, ?FUNCTION_NAME))#{commit_index => 1,
                             last_applied => 1,
                             cluster => Cluster,
                             machine_state => <<"hi1">>},
    % n2 has only seen index 1
    Msg = {N2, #append_entries_reply{term = 5, success = false, next_index = 2,
                                     last_index = 1, last_term = 1}},
    % new peers state is updated
    {leader, #{cluster := #{N2 := #{next_index := 4, match_index := 1}},
               commit_index := 1,
               last_applied := 1,
               machine_state := <<"hi1">>},
     [{send_rpc, N3,
       #append_entries_rpc{term = 5, leader_id = N1,
                           prev_log_index = 1,
                           prev_log_term = 1,
                           leader_commit = 1,
                           entries = [{2, 3, {'$usr', _, <<"hi2">>, _}},
                                      {3, 5, {'$usr', _, <<"hi3">>, _}}]}},
        {send_rpc, N2, _}
      ]} = ra_server:handle_leader(Msg, State),
    ok.

follower_request_vote(_Config) ->
    N2 = ?N2, N3 = ?N3,
    State = base_state(3, ?FUNCTION_NAME),
    Msg = #request_vote_rpc{candidate_id = ?N2, term = 6, last_log_index = 3,
                            last_log_term = 5},
    % success
    {follower, #{voted_for := N2, current_term := 6} = State1,
     [{reply, #request_vote_result{term = 6, vote_granted = true}}]} =
    ra_server:handle_follower(Msg, State),

    % we can vote again for the same candidate and term
    {follower, #{voted_for := N2, current_term := 6},
     [{reply, #request_vote_result{term = 6, vote_granted = true}}]} =
    ra_server:handle_follower(Msg, State1),

    % but not for a different candidate
    {follower, #{voted_for := N2, current_term := 6},
     [{reply, #request_vote_result{term = 6, vote_granted = false}}]} =
    ra_server:handle_follower(Msg#request_vote_rpc{candidate_id = N3}, State1),

    % fail due to lower term
    {follower, #{current_term := 5},
     [{reply, #request_vote_result{term = 5, vote_granted = false}}]} =
    ra_server:handle_follower(Msg#request_vote_rpc{term = 4}, State),

     % Raft determines which of two logs is more up-to-date by comparing the
     % index and term of the last entries in the logs. If the logs have last
     % entries with different terms, then the log with the later term is more
     % up-to-date. If the logs end with the same term, then whichever log is
     % longer is more up-to-date. (§5.4.1)

     % reject when candidate last log entry has a lower term
     % still update current term if incoming is higher
    {follower, #{current_term := 6},
     [{reply, #request_vote_result{term = 6, vote_granted = false}}]} =
    ra_server:handle_follower(Msg#request_vote_rpc{last_log_term = 4},
                            State),

    % grant vote when candidate last log entry has same term but is longer
    {follower, #{current_term := 6},
     [{reply, #request_vote_result{term = 6, vote_granted = true}}]} =
    ra_server:handle_follower(Msg#request_vote_rpc{last_log_index = 4},
                            State),
     ok.

follower_pre_vote(_Config) ->
    State = base_state(3, ?FUNCTION_NAME),
    Term = 5,
    Token = make_ref(),
    Msg = #pre_vote_rpc{candidate_id = ?N2, term = Term, last_log_index = 3,
                        machine_version = 0,
                        token = Token, last_log_term = 5},
    % success
    {follower, #{current_term := Term},
     [{reply, #pre_vote_result{term = Term, token = Token,
                               vote_granted = true}}]} =
        ra_server:handle_follower(Msg, State),

    % disallow pre votes from higher protocol version
    ?assertMatch(
       {follower, _,
        [{reply, #pre_vote_result{term = Term, token = Token,
                                  vote_granted = false}}]},
       ra_server:handle_follower(Msg#pre_vote_rpc{version = ?RA_PROTO_VERSION + 1},
                                 State)),

    % but still allow from a lower protocol version
    {follower, _,
     [{reply, #pre_vote_result{term = Term, token = Token,
                               vote_granted = true}}]} =
    ra_server:handle_follower(Msg#pre_vote_rpc{version = ?RA_PROTO_VERSION - 1},
                              State),

    % disallow pre votes from higher machine version
    {follower, _,
     [{reply, #pre_vote_result{term = Term, token = Token,
                               vote_granted = false}} | _]} =
        ra_server:handle_follower(Msg#pre_vote_rpc{machine_version = 99},
                                  State),

    Cfg = maps:get(cfg, State),

    % disallow votes from a lower machine version when the effective machine
    % version is higher
    {follower, _,
     [{reply, #pre_vote_result{term = Term, token = Token,
                               vote_granted = false}} | _]} =
    ra_server:handle_follower(Msg#pre_vote_rpc{machine_version = 0},
                              State#{cfg => Cfg#cfg{effective_machine_version = 1,
                                                    machine_version = 1}}),

    % allow votes from a lower machine version when the effective machine
    % version is lower too
    {follower, _,
     [{reply, #pre_vote_result{term = Term, token = Token,
                               vote_granted = true}} | _]} =
    ra_server:handle_follower(Msg#pre_vote_rpc{machine_version = 0},
                              State#{cfg => Cfg#cfg{effective_machine_version = 0,
                                                    machine_version = 1}}),

    % allow votes for the same machine version
    {follower, _,
     [{reply, #pre_vote_result{term = Term, token = Token,
                               vote_granted = true}}]} =
    ra_server:handle_follower(Msg#pre_vote_rpc{machine_version = 2},
                              State#{cfg => Cfg#cfg{machine_version = 2}}),

    % fail due to lower term
    % return failure and immediately enter pre_vote phase as there are
    {follower, #{current_term := 5},
     [{reply, #pre_vote_result{term = 5, token = Token,
                               vote_granted = false}}]} =
    ra_server:handle_follower(Msg#pre_vote_rpc{term = 4}, State),

    % when candidate last log entry has a lower term
    % the current server is a better candidate and thus
    % requests that an election timeout is started
    {follower, #{current_term := 6},
     [start_election_timeout]} =
    ra_server:handle_follower(Msg#pre_vote_rpc{last_log_term = 4,
                                               term = 6},
                              State),

    % grant vote when candidate last log entry has same term but is longer
    {follower, #{current_term := 5},
     [{reply, #pre_vote_result{term = 5, token = Token,
                               vote_granted = true}}]} =
    ra_server:handle_follower(Msg#pre_vote_rpc{last_log_index = 4},
                              State),
    ok.

pre_vote_receives_pre_vote(_Config) ->
    State = base_state(3, ?FUNCTION_NAME),
    Term = 5,
    Token = make_ref(),
    Msg = #pre_vote_rpc{candidate_id = ?N2, term = Term, last_log_index = 3,
                        machine_version = 0,
                        token = Token, last_log_term = 5},
    % success - pre vote still returns other pre vote requests
    % else we could have a dead-lock with a two process cluster with
    % both peers in pre-vote state
    {pre_vote, #{current_term := Term},
     [{reply, #pre_vote_result{term = Term, token = Token,
                               vote_granted = true}}]} =
        ra_server:handle_pre_vote(Msg, State),
    ok.

request_vote_rpc_with_lower_term(_Config) ->
    N1 = ?N1,
    State = (base_state(3, ?FUNCTION_NAME))#{current_term => 6,
                             voted_for => N1},
    Msg = #request_vote_rpc{candidate_id = ?N2, term = 5, last_log_index = 3,
                            last_log_term = 5},
    % term is lower than candidate term
    {candidate, #{voted_for := N1, current_term := 6},
     [{reply, #request_vote_result{term = 6, vote_granted = false}}]} =
         ra_server:handle_candidate(Msg, State),
    % term is lower than candidate term
    {leader, #{current_term := 6},
     [{reply, #request_vote_result{term = 6, vote_granted = false}}]} =
         ra_server:handle_leader(Msg, State).

leader_does_not_abdicate_to_unknown_peer(_Config) ->
    State = base_state(3, ?FUNCTION_NAME),
    Unk = {unknown_peer, node()},
    Vote = #request_vote_rpc{candidate_id = Unk,
                             term = 6,
                             last_log_index = 3,
                             last_log_term = 5},
    {leader, State, []} = ra_server:handle_leader(Vote, State),

    AEReply = {Unk, #append_entries_reply{term = 6,
                                          success = false,
                                          next_index = 4,
                                          last_index = 3,
                                          last_term = 5}},
    {leader, State, []} = ra_server:handle_leader(AEReply , State),
    IRS = #install_snapshot_result{term = 6, last_index = 0, last_term = 0},
    {leader, State, []} = ra_server:handle_leader({Unk, IRS}, State),
    ok.


leader_replies_to_append_entries_rpc_with_lower_term(_Config) ->
    N3 = ?N3,
    State = #{cfg := #cfg{id = Id},
              current_term := CTerm} = base_state(3, ?FUNCTION_NAME),
    AERpc = #append_entries_rpc{term = CTerm - 1,
                                leader_id = N3,
                                prev_log_index = 3,
                                prev_log_term = 5,
                                leader_commit = 3},
    ?assertMatch(
       {leader, _,
        [{cast, N3,
          {Id, #append_entries_reply{term = CTerm, success = false}}}]},
       ra_server:handle_leader(AERpc, State)),

    ok.

higher_term_detected(_Config) ->
    N2 = ?N2, N3 = ?N3,
    % Any message received with a higher term should result in the
    % server reverting to follower state
    State = #{log := Log} = base_state(3, ?FUNCTION_NAME),
    IncomingTerm = 6,
    AEReply = {N2, #append_entries_reply{term = IncomingTerm, success = false,
                                         next_index = 4,
                                         last_index = 3, last_term = 5}},
    Log1 = ra_log_memory:write_meta_f(current_term, IncomingTerm, Log),
    _ = ra_log_memory:write_meta_f(voted_for, undefined, Log1),
    {follower, #{current_term := IncomingTerm, leader_id := undefined}, []} =
        ra_server:handle_leader(AEReply, State),
    {follower, #{current_term := IncomingTerm}, []} =
        ra_server:handle_follower(AEReply, State),
    {follower, #{current_term := IncomingTerm}, []}
        = ra_server:handle_candidate(AEReply, State),

    AERpc = #append_entries_rpc{term = IncomingTerm, leader_id = N3,
                                prev_log_index = 3, prev_log_term = 5,
                                leader_commit = 3},
    {follower, #{current_term := IncomingTerm, leader_id := undefined},
     [{next_event, AERpc}]} = ra_server:handle_leader(AERpc, State),
    {follower, #{current_term := IncomingTerm},
     [{next_event, AERpc}]} = ra_server:handle_candidate(AERpc, State),
    % follower will handle this properly and is tested elsewhere

    Vote = #request_vote_rpc{candidate_id = ?N2, term = 6, last_log_index = 3,
                             last_log_term = 5},
    {follower, #{current_term := IncomingTerm,
                 leader_id := undefined,
                 log := ExpectLog},
     [{next_event, Vote}]} = ra_server:handle_leader(Vote, State),
    {follower, #{current_term := IncomingTerm,
                 log := ExpectLog},
     [{next_event, Vote}]} = ra_server:handle_candidate(Vote, State),

    IRS = #install_snapshot_result{term = IncomingTerm,
                                   last_term = 0,
                                   last_index = 0},
    {follower, #{current_term := IncomingTerm, leader_id := undefined},
     _} = ra_server:handle_leader({N2, IRS}, State),
    ok.

leader_noop_operation_enables_cluster_change(_Config) ->
    N2 = ?N2,
    State00 = (base_state(3, ?FUNCTION_NAME))#{cluster_change_permitted => false},
    {leader, #{cluster_change_permitted := false} = State0, _Effects} =
        ra_server:handle_leader({command, {noop, meta(), 0}}, State00),
    {leader, State, _} = ra_server:handle_leader({ra_log_event, {written, {4, 4, 5}}}, State0),
    AEReply = {N2, #append_entries_reply{term = 5, success = true,
                                         next_index = 5,
                                         last_index = 4, last_term = 5}},
    % noop consensus
    {leader, #{cluster_change_permitted := true}, _} =
        ra_server:handle_leader(AEReply, State),
    ok.

leader_noop_increments_machine_version(_Config) ->
    N2 = ?N2,
    Mod = ?FUNCTION_NAME,
    MacVer = 2,
    OldMacVer = 1,
    Base = base_state(3, ?FUNCTION_NAME),
    Cfg = maps:get(cfg, Base),
    State00 = Base#{cluster_change_permitted => false,
                    cfg => Cfg#cfg{machine_version = MacVer,
                                   effective_machine_version = OldMacVer}},
    ModV2 = leader_noop_increments_machine_version_v2,
    meck:new(ModV2, [non_strict]),
    meck:expect(Mod, version, fun () -> MacVer end),
    meck:expect(Mod, which_module, fun (1) -> Mod;
                                       (2) -> ModV2
                                   end),
    {leader, #{cfg := #cfg{effective_machine_version = OldMacVer,
                           effective_machine_module = Mod}} = State0,
     _Effects} = ra_server:handle_leader({command, {noop, meta(), MacVer}},
                                         State00),
    %% new machine version is applied
    {leader, State1, _} =
        ra_server:handle_leader({ra_log_event, {written, {4, 4, 5}}}, State0),

    meck:expect(ModV2, apply,
                fun (_, {machine_version, 1, 2}, State) ->
                        {State, ok};
                    (_, Cmd, _) ->
                        {Cmd, ok}
                end),

    %% AER reply confirms consensus of noop operation
    AEReply = {N2, #append_entries_reply{term = 5, success = true,
                                         next_index = 5,
                                         last_index = 4, last_term = 5}},
    % noop consensus
    {leader, #{cfg := #cfg{effective_machine_version = MacVer,
                           effective_machine_module = ModV2}}, _} =
        ra_server:handle_leader(AEReply, State1),

    ?assert(meck:called(ModV2, apply, ['_', {machine_version, 1, 2}, '_'])),
    ok.

follower_machine_version(_Config) ->
    MacVer = 1,
    %
    State00 = base_state(3, ?FUNCTION_NAME),
    %% follower with lower machine version is adviced of higher machine version
    Aer = #append_entries_rpc{entries = [{4, 5, {noop, meta(), MacVer}},
                                         {5, 5, usr(new_state)}],
                              term = 5, leader_id = ?N1,
                              prev_log_index = 3,
                              prev_log_term = 5,
                              leader_commit = 5},
    {follower, #{cfg := #cfg{machine_version = 0,
                             effective_machine_version = 1},
                 last_applied := 3,
                 commit_index := 5} = State0, _} =
        ra_server:handle_follower(Aer, State00),
    %% new effective machine version is detected that is lower than available
    %% machine version
    %% last_applied is not updated we simply "peek" at the noop command to
    %% learn the next machine version to update it
    {follower, #{cfg := #cfg{machine_version = 0,
                             effective_machine_version = 1},
                 last_applied := 3,
                 commit_index := 5,
                 log := _Log} = _State1, _Effects} =
    ra_server:handle_follower({ra_log_event, {written, {4, 5, 5}}}, State0),

    %% TODO: validate append entries reply effect

    %% TODO: simulate that follower is updated from this state
    %% ra_server_init
    ok.

leader_server_join(_Config) ->
    N1 = ?N1, N2 = ?N2, N3 = ?N3, N4 = ?N4,
    OldCluster = #{N1 => new_peer_with(#{next_index => 4, match_index => 3}),
                   N2 => new_peer_with(#{next_index => 4, match_index => 3}),
                   N3 => new_peer_with(#{next_index => 4, match_index => 3})},
    State0 = (base_state(3, ?FUNCTION_NAME))#{cluster => OldCluster},
    % raft servers should switch to the new configuration after log append
    % and further cluster changes should be disallowed
    {leader, #{cluster := #{N1 := _, N2 := _, N3 := _, N4 := _},
               cluster_change_permitted := false} = _State1, Effects} =
        ra_server:handle_leader({command, {'$ra_join', meta(),
                                           N4, await_consensus}}, State0),
    [
     {send_rpc, N4,
      #append_entries_rpc{entries =
                          [_, _, _, {4, 5, {'$ra_cluster_change', _,
                                            #{N1 := _, N2 := _,
                                              N3 := _, N4 := _},
                                            await_consensus}}]}},
     {send_rpc, N3,
      #append_entries_rpc{entries =
                          [{4, 5, {'$ra_cluster_change', _,
                                   #{N1 := _, N2 := _, N3 := _, N4 := _},
                                   await_consensus}}],
                          term = 5, leader_id = N1,
                          prev_log_index = 3,
                          prev_log_term = 5,
                          leader_commit = 3}},
     {send_rpc, N2,
      #append_entries_rpc{entries =
                          [{4, 5, {'$ra_cluster_change', _,
                                   #{N1 := _, N2 := _, N3 := _, N4 := _},
                                   await_consensus}}],
                          term = 5, leader_id = N1,
                          prev_log_index = 3,
                          prev_log_term = 5,
                          leader_commit = 3}}
     | _] = Effects,
    ok.

leader_server_leave(_Config) ->
    N1 = ?N1, N2 = ?N2, N3 = ?N3, N4 = ?N4,
    OldCluster = #{N1 => new_peer_with(#{next_index => 4, match_index => 3}),
                   N2 => new_peer_with(#{next_index => 4, match_index => 3}),
                   N3 => new_peer_with(#{next_index => 4, match_index => 3}),
                   N4 => new_peer_with(#{next_index => 1, match_index => 0})},
    State = (base_state(3, ?FUNCTION_NAME))#{cluster => OldCluster},
    % raft servers should switch to the new configuration after log append
    {leader, #{cluster := #{N1 := _, N2 := _, N3 := _}},
     [{send_rpc, N3, RpcN3}, {send_rpc, N2, RpcN2} | _]} =
        ra_server:handle_leader({command, {'$ra_leave', meta(), ?N4, await_consensus}},
                                State),
    % the leaving server is no longer included
    #append_entries_rpc{term = 5, leader_id = N1,
                        prev_log_index = 3,
                        prev_log_term = 5,
                        leader_commit = 3,
                        entries = [{4, 5, {'$ra_cluster_change', _,
                                           #{N1 := _, N2 := _, N3 := _},
                                           await_consensus}}]} = RpcN3,
    #append_entries_rpc{term = 5, leader_id = N1,
                        prev_log_index = 3,
                        prev_log_term = 5,
                        leader_commit = 3,
                        entries = [{4, 5, {'$ra_cluster_change', _,
                                           #{N1 := _, N2 := _, N3 := _},
                                           await_consensus}}]} = RpcN2,
    ok.

leader_is_removed(_Config) ->
    N1 = ?N1, N2 = ?N2, N3 = ?N3, N4 = ?N4,
    OldCluster = #{N1 => new_peer_with(#{next_index => 4, match_index => 3}),
                   N2 => new_peer_with(#{next_index => 4, match_index => 3}),
                   N3 => new_peer_with(#{next_index => 4, match_index => 3}),
                   N4 => new_peer_with(#{next_index => 1, match_index => 0})},
    State = (base_state(3, ?FUNCTION_NAME))#{cluster => OldCluster},

    {leader, State1, _} =
        ra_server:handle_leader({command, {'$ra_leave', meta(), ?N1, await_consensus}},
                              State),
    {leader, State1b, _} =
        ra_server:handle_leader(written_evt({4, 4, 5}), State1),

    % replies coming in
    AEReply = #append_entries_reply{term = 5, success = true, next_index = 5,
                                    last_index = 4, last_term = 5},
    {leader, State2, _} = ra_server:handle_leader({N2, AEReply}, State1b),
    % after committing the new entry the leader steps down
    {stop, #{commit_index := 4}, _} = ra_server:handle_leader({N3, AEReply}, State2),
    ok.

follower_cluster_change(_Config) ->
    N1 = ?N1, N2 = ?N2, N3 = ?N3, N4 = ?N4,
    OldCluster = #{N1 => new_peer_with(#{next_index => 4, match_index => 3}),
                   N2 => new_peer_with(#{next_index => 4, match_index => 3}),
                   N3 => new_peer_with(#{next_index => 4, match_index => 3})},
    Base = base_state(3, ?FUNCTION_NAME),
    Cfg = maps:get(cfg, Base),
    State = Base#{cfg => Cfg#cfg{id = N2,
                                 uid = <<"n2">>,
                                 log_id = "n2"},
                  cluster => OldCluster},
    NewCluster = #{N1 => new_peer_with(#{next_index => 4, match_index => 3}),
                   N2 => new_peer_with(#{next_index => 4, match_index => 3}),
                   N3 => new_peer_with(#{next_index => 4, match_index => 3}),
                   N4 => new_peer_with(#{next_index => 1})},
    JoinEntry = {4, 5, {'$ra_cluster_change', meta(), NewCluster, await_consensus}},
    AE = #append_entries_rpc{term = 5, leader_id = N1,
                             prev_log_index = 3,
                             prev_log_term = 5,
                             leader_commit = 3,
                             entries = [JoinEntry]},
    {follower, #{cluster := #{N1 := _, N2 := _,
                              N3 := _, N4 := _},
                 cluster_index_term := {4, 5}},
     [{cast, N1, {N2, #append_entries_reply{}}}]} =
        begin
            {follower, Int, _} = ra_server:handle_follower(AE, State),
            ra_server:handle_follower(written_evt({4, 4, 5}), Int)
        end,

    ok.

written_evt(E) ->
    {ra_log_event, {written, E}}.

leader_applies_new_cluster(_Config) ->
    N1 = ?N1, N2 = ?N2, N3 = ?N3, N4 = ?N4,
    OldCluster = #{N1 => new_peer_with(#{next_index => 4, match_index => 3}),
                   N2 => new_peer_with(#{next_index => 4, match_index => 3}),
                   N3 => new_peer_with(#{next_index => 4, match_index => 3})},

    State = (base_state(3, ?FUNCTION_NAME))#{cluster => OldCluster},
    Command = {command, {'$ra_join', meta(), N4, await_consensus}},
    % cluster records index and term it was applied to determine whether it has
    % been applied
    {leader, #{cluster_index_term := {4, 5},
               cluster := #{N1 := _, N2 := _,
                            N3 := _, N4 := _} } = State1, _} =
        ra_server:handle_leader(Command, State),
    {leader, State2, _} =
        ra_server:handle_leader(written_evt({4, 4, 5}), State1),

    ?assert(not maps:get(cluster_change_permitted, State2)),

    % replies coming in
    AEReply = #append_entries_reply{term = 5, success = true,
                                    next_index = 5,
                                    last_index = 4, last_term = 5},
    % leader does not yet have consensus as will need at least 3 votes
    {leader, State3 = #{commit_index := 3,

                        cluster_change_permitted := false,
                        cluster_index_term := {4, 5},
                        cluster := #{N2 := #{next_index := 5,
                                             match_index := 4}}},
     _} = ra_server:handle_leader({N2, AEReply}, State2#{votes => 1}),

    % leader has consensus
    {leader, _State4 = #{commit_index := 4,
                         cluster_change_permitted := true,
                         cluster := #{N3 := #{next_index := 5,
                                              match_index := 4}}},
     _Effects} = ra_server:handle_leader({N3, AEReply}, State3),
    ok.

leader_appends_cluster_change_then_steps_before_applying_it(_Config) ->
    N1 = ?N1, N2 = ?N2, N3 = ?N3, N4 = ?N4,
    OldCluster = #{N1 => new_peer_with(#{next_index => 4, match_index => 3}),
                   N2 => new_peer_with(#{next_index => 4, match_index => 3}),
                   N3 => new_peer_with(#{next_index => 4, match_index => 3})},

    State = (base_state(3, ?FUNCTION_NAME))#{cluster => OldCluster},
    Command = {command, {'$ra_join', meta(), N4, await_consensus}},
    % cluster records index and term it was applied to determine whether it has
    % been applied
    {leader, #{cluster_index_term := {4, 5},
               cluster := #{N1 := _, N2 := _,
                            N3 := _, N4 := _}} = State1, _} =
    ra_server:handle_leader(Command, State),

    % leader has committed the entry but n2 and n3 have not yet seen it and
    % n2 has been elected leader and is replicating a different entry for
    % index 4 with a higher term
    AE = #append_entries_rpc{entries = [{4, 6, {noop, meta(), 1}}],
                             leader_id = N2,
                             term = 6,
                             prev_log_index = 3,
                             prev_log_term = 5,
                             leader_commit = 3},
    {follower, #{cluster := Cluster}, _} =
        ra_server:handle_follower(AE, State1),
    % assert n1 has switched back to the old cluster config
    #{N1 := _, N2 := _, N3 := _} = Cluster,
    3 = maps:size(Cluster),
    ok.

is_new(_Config) ->
    Id = some_id,
    Args = #{cluster_name => Id,
             id => {ra, node()},
             uid => <<"ra">>,
             initial_members => [],
             log_init_args => #{uid => <<>>},
             machine => {simple, fun erlang:'+'/2, 0}},
    NewState = ra_server:init(Args),
    {leader, State, _} = ra_server:handle_leader(usr_cmd(1), NewState),
    false = ra_server:is_new(State),
    NewState = ra_server:init(Args),
    true = ra_server:is_new(NewState),
    ok.

command(_Config) ->
    N1 = ?N1, N2 = ?N2, N3 = ?N3,
    State = base_state(3, ?FUNCTION_NAME),
    Meta = meta(),
    Cmd = {'$usr', Meta, <<"hi4">>, after_log_append},
    AE = #append_entries_rpc{entries = [{4, 5, Cmd}],
                             leader_id = N1,
                             term = 5,
                             prev_log_index = 3,
                             prev_log_term = 5,
                             leader_commit = 3
                            },
    From = maps:get(from, Meta),
    {leader, _, [{reply, From, {wrap_reply, {4, 5}}},
                 {send_rpc, N3, AE}, {send_rpc, N2, AE} |
                 _]} =
        ra_server:handle_leader({command, Cmd}, State),
    ok.

candidate_election(_Config) ->
    N2 = ?N2, N3 = ?N3, N4 = ?N4, N5 = ?N5,
    State = (base_state(5, ?FUNCTION_NAME))#{current_term => 6, votes => 1},
    Reply = #request_vote_result{term = 6, vote_granted = true},
    {candidate, #{votes := 2} = State1, []}
        = ra_server:handle_candidate(Reply, State),
    % denied
    NegResult = #request_vote_result{term = 6, vote_granted = false},
    {candidate, #{votes := 2}, []}
        = ra_server:handle_candidate(NegResult, State1),
    % newer term should make candidate stand down
    HighTermResult = #request_vote_result{term = 7, vote_granted = false},
    {follower, #{current_term := 7}, []}
        = ra_server:handle_candidate(HighTermResult, State1),

    MacVer = 1,
    meck:expect(ra_machine, version, fun (_) -> MacVer end),

    % quorum has been achieved - candidate becomes leader
    PeerState = new_peer_with(#{next_index => 3+1, % leaders last log index + 1
                                match_index => 0}), % initd to 0
    % when candidate becomes leader the next operation should be a noop
    % and all peers should be initialised with the appropriate state
    % Also rpcs for all members should be issued
    {leader, #{cluster := #{N2 := PeerState,
                            N3 := PeerState,
                            N4 := PeerState,
                            N5 := PeerState}},
     [
      {next_event, cast, {command, {noop, _, MacVer}}},
      {send_rpc, _, _},
      {send_rpc, _, _},
      {send_rpc, _, _},
      {send_rpc, _, _}
     ]} = ra_server:handle_candidate(Reply, State1).

pre_vote_election(_Config) ->
    Token = make_ref(),
    State = (base_state(5, ?FUNCTION_NAME))#{votes => 1,
                             pre_vote_token => Token},
    Reply = #pre_vote_result{term = 5, token = Token, vote_granted = true},
    {pre_vote, #{votes := 2} = State1, []}
        = ra_server:handle_pre_vote(Reply, State),

    %% different token is ignored
    {pre_vote, #{votes := 1}, []}
        = ra_server:handle_pre_vote(Reply#pre_vote_result{token = make_ref()},
                                  State),
    % denied
    NegResult = #pre_vote_result{term = 5, token = Token,
                                 vote_granted = false},
    {pre_vote, #{votes := 2}, []}
        = ra_server:handle_pre_vote(NegResult, State1),

    % newer term should make pre_vote revert to follower
    HighTermResult = #pre_vote_result{term = 6, token = Token,
                                      vote_granted = false},
    {follower, #{current_term := 6, votes := 0}, []}
        = ra_server:handle_pre_vote(HighTermResult, State1),

    % quorum has been achieved - pre_vote becomes candidate
    {candidate,
     #{current_term := 6}, _} = ra_server:handle_pre_vote(Reply, State1).

pre_vote_election_reverts(_Config) ->
    N2 = ?N2,
    Token = make_ref(),
    State = (base_state(5, ?FUNCTION_NAME))#{votes => 1,
                             pre_vote_token => Token},
    % request vote with higher term
    VoteRpc = #request_vote_rpc{term = 6, candidate_id = N2,
                                last_log_index = 3, last_log_term = 5},
    {follower, #{current_term := 6, votes := 0}, [{next_event, VoteRpc}]}
        = ra_server:handle_pre_vote(VoteRpc, State),
    %  append entries rpc with equal term
    AE = #append_entries_rpc{term = 5, leader_id = ?N2, prev_log_index = 3,
                             prev_log_term = 5, leader_commit = 3},
    {follower, #{current_term := 5, votes := 0}, [{next_event, AE}]}
        = ra_server:handle_pre_vote(AE, State),
    % and higher term
    AETerm6 = AE#append_entries_rpc{term = 6},
    {follower, #{current_term := 6, votes := 0}, [{next_event, AETerm6}]}
        = ra_server:handle_pre_vote(AETerm6, State),

    % install snapshot rpc
    ISR = #install_snapshot_rpc{term = 5, leader_id = N2,
                                meta = #{index => 3,
                                         term => 5,
                                         cluster => [],
                                         machine_version => 0},
                                chunk_state = {1, last},
                                data = []},
    {follower, #{current_term := 5, votes := 0}, [{next_event, ISR}]}
        = ra_server:handle_pre_vote(ISR, State),
    ok.

leader_receives_pre_vote(_Config) ->
    % leader should emit rpcs to all nodes immediately upon receiving
    % an pre_vote_rpc to put upstart followers back in their place
    Token = make_ref(),
    State = (base_state(5, ?FUNCTION_NAME))#{votes => 1},
    PreVoteRpc = #pre_vote_rpc{term = 5, candidate_id = ?N1,
                               token = Token,
                               machine_version = 0,
                               last_log_index = 3, last_log_term = 5},
    {leader, #{}, [
                   {send_rpc, _, _},
                   {send_rpc, _, _},
                   {send_rpc, _, _},
                   {send_rpc, _, _}
                   | _]}
        = ra_server:handle_leader(PreVoteRpc, State),
    % leader abdicates for higher term
    {follower, #{current_term := 6}, _}
        = ra_server:handle_leader(PreVoteRpc#pre_vote_rpc{term = 6}, State),
    ok.

leader_receives_install_snapshot_rpc(_Config) ->
    % leader step down when receiving an install snapshot rpc with a higher
    % term
    State  = #{current_term := Term,
               last_applied := Idx} = (base_state(5, ?FUNCTION_NAME))#{votes => 1},
    ISRpc = #install_snapshot_rpc{term = Term + 1, leader_id = ?N5,
                                  meta = #{index => Idx,
                                           term => Term,
                                           cluster => [],
                                           machine_version => 0},
                                  chunk_state = {1, last},
                                  data = []},
    {follower, #{}, [{next_event, ISRpc}]}
        = ra_server:handle_leader(ISRpc, State),
    % leader ignores lower term
    {leader, State, _}
        = ra_server:handle_leader(ISRpc#install_snapshot_rpc{term = Term - 1},
                                State),
    ok.

follower_installs_snapshot(_Config) ->
    N1 = ?N1, N2 = ?N2, N3 = ?N3,
    #{N3 := {_, FState = #{cluster := Config}, _}}
    = init_servers([N1, N2, N3], {module, ra_queue, #{}}),
    LastTerm = 1, % snapshot term
    Term = 2, % leader term
    Idx = 3,
    ISRpc = #install_snapshot_rpc{term = Term, leader_id = N1,
                                  meta = snap_meta(Idx, LastTerm,
                                                   maps:keys(Config)),
                                  chunk_state = {1, last},
                                  data = []},
    {receive_snapshot, FState1,
     [{next_event, ISRpc}, {record_leader_msg, _}]} =
        ra_server:handle_follower(ISRpc, FState),

    meck:expect(ra_log, recover_snapshot,
                fun (_) ->
                        {#{index => Idx,
                           term => Term,
                           cluster => maps:keys(Config),
                           machine_version => 0},
                         []}
                end),

    {follower, #{current_term := Term,
                 commit_index := Idx,
                 last_applied := Idx,
                 cluster := Config,
                 machine_state := [],
                 leader_id := N1},
     [{reply, #install_snapshot_result{}}]} =
        ra_server:handle_receive_snapshot(ISRpc, FState1),

    ok.

follower_receives_stale_snapshot(_Config) ->
    N1 = ?N1, N2 = ?N2, N3 = ?N3,
    #{N3 := {_, FState0 = #{cluster := Config,
                            current_term := CurTerm}, _}}
    = init_servers([N1, N2, N3], {module, ra_queue, #{}}),
    FState = FState0#{last_applied => 3},
    LastTerm = 1, % snapshot term
    Idx = 2,
    ISRpc = #install_snapshot_rpc{term = CurTerm, leader_id = N1,
                                  meta = snap_meta(Idx, LastTerm,
                                                   maps:keys(Config)),
                                  chunk_state = {1, last},
                                  data = []},
    %% this should be a rare occurence, rather than implement a special
    %% protocol at this point the server just replies
    {follower, _, _} =
        ra_server:handle_follower(ISRpc, FState),
    ok.

receive_snapshot_timeout(_Config) ->
    N1 = ?N1, N2 = ?N2, N3 = ?N3,
    #{N3 := {_, FState0 = #{cluster := Config,
                            current_term := CurTerm}, _}}
    = init_servers([N1, N2, N3], {module, ra_queue, #{}}),
    FState = FState0#{last_applied => 3},
    LastTerm = 1, % snapshot term
    Idx = 6,
    ISRpc = #install_snapshot_rpc{term = CurTerm, leader_id = N1,
                                  meta = snap_meta(Idx, LastTerm,
                                                   maps:keys(Config)),
                                  chunk_state = {1, last},
                                  data = []},
    {receive_snapshot, FState1,
     [{next_event, ISRpc}, {record_leader_msg, _}]} =
        ra_server:handle_follower(ISRpc, FState),

    %% revert back to follower on timeout
    {follower, #{log := Log}, _}
    = ra_server:handle_receive_snapshot(receive_snapshot_timeout, FState1),
    %% snapshot should be aborted
    SS = ra_log:snapshot_state(Log),
    undefined = ra_snapshot:accepting(SS),
    ok.

snapshotted_follower_received_append_entries(_Config) ->
    N1 = ?N1, N2 = ?N2, N3 = ?N3,
    #{N3 := {_, FState0 = #{cluster := Config}, _}} =
        init_servers([N1, N2, N3], {module, ra_queue, #{}}),
    LastTerm = 1, % snapshot term
    Term = 2, % leader term
    Idx = 3,
    meck:expect(ra_log, recover_snapshot,
                fun (_) ->
                        {#{index => Idx,
                           term => Term,
                           cluster => maps:keys(Config),
                           machine_version => 0},
                         []}
                end),
    ISRpc = #install_snapshot_rpc{term = Term, leader_id = N1,
                                  meta = snap_meta(Idx, LastTerm,
                                                   maps:keys(Config)),
                                  chunk_state = {1, last},
                                  data = []},
    {follower, FState1, _} = ra_server:handle_receive_snapshot(ISRpc, FState0),

    meck:expect(ra_log, snapshot_index_term,
                fun (_) -> {Idx, LastTerm} end),
    %% mock the ra_log write to return ok for index 4 as this is the next
    %% expected index after the snapshot
    meck:expect(ra_log, write,
                fun ([{4, _, _}], Log) -> {ok, Log} end),
    Cmd = usr({enc, banana}),
    AER = #append_entries_rpc{entries = [{4, 2, Cmd}],
                              leader_id = N1,
                              term = Term,
                              prev_log_index = 3, % snapshot index
                              prev_log_term = 1,
                              leader_commit = 4 % entry is already committed
                             },
    {follower, _FState, [{cast, N1, {N3, #append_entries_reply{success = true}}}]} =
        begin
            {follower, Int, _} = ra_server:handle_follower(AER, FState1),
            ra_server:handle_follower(written_evt({4, 4, 2}), Int)
        end,
    ok.

leader_received_append_entries_reply_with_stale_last_index(_Config) ->
    N1 = ?N1, N2 = ?N2, N3 = ?N3,
    Term = 2,
    N2NextIndex = 3,
    Log = lists:foldl(fun(E, L) ->
                              ra_log:append_sync(E, L)
                      end, ra_log:init(#{system_config => ra_system:default_config(),
                                         uid => <<>>}),
                      [{1, 1, {noop, meta(), 1}},
                       {2, 2, {'$usr', meta(), {enq, apple}, after_log_append}},
                       {3, 5, {2, {'$usr', meta(), {enq, pear}, after_log_append}}}]),
    Cfg = #cfg{id = N1,
               uid = <<"n1">>,
               log_id = <<"n1">>,
               metrics_key = n1,
               machine = {machine, ra_machine_simple,
                            #{simple_fun => ?MACFUN,
                              initial_state => <<>>}}, % just keep last applied value
               machine_version = 0,
               machine_versions = [{0, 0}],
               effective_machine_version = 0,
               effective_machine_module = ra_machine_simple,
               system_config = ra_system:default_config()
              },
    Leader0 = #{cfg => Cfg,
                cluster =>
                #{N1 => new_peer_with(#{match_index => 0}), % current leader in term 2
                  N2 => new_peer_with(#{match_index => 0,
                                        next_index => N2NextIndex}), % stale peer - previous leader
                  N3 => new_peer_with(#{match_index => 3,
                                        next_index => 4,
                                        commit_index_sent => 3})}, % uptodate peer
                cluster_change_permitted => true,
                cluster_index_term => {0,0},
                commit_index => 3,
                current_term => Term,
                last_applied => 4,
                log => Log,
                machine_state => [{4,apple}],
                query_index => 0,
                queries_waiting_heartbeats => queue:new(),
                pending_consistent_queries => []},
    AER = #append_entries_reply{success = false,
                                term = Term,
                                next_index = 3,
                                last_index = 2, % refer to stale entry
                                last_term = 1}, % in previous term
    % should decrement next_index for n2
    % ExpectedN2NextIndex = 2,
    {leader, #{cluster := #{N2 := #{next_index := 4}}},
     [{send_rpc, N2, #append_entries_rpc{entries = [{2, _, _}, {3, _, _}]}}]}
       = ra_server:handle_leader({N2, AER}, Leader0),
    ok.


leader_receives_install_snapshot_result(_Config) ->
    N1 = ?N1, N2 = ?N2, N3 = ?N3,
    MacVer = {0, 1, ?MODULE},
    % should update peer next_index
    Term = 1,
    Log0 = lists:foldl(fun(E, L) ->
                              ra_log:append_sync(E, L)
                      end, ra_log:init(#{system_config => ra_system:default_config(),
                                         uid => <<>>}),
                      [{1, 1, {noop, meta(), MacVer}},
                       {2, 1, {noop, meta(), MacVer}},
                       {3, 1, {'$usr', meta(), {enq,apple}, after_log_append}},
                       {4, 1, {'$usr', meta(), {enq,pear}, after_log_append}}]),
    mock_machine(?FUNCTION_NAME),
    Cfg = #cfg{id = ?N1,
               uid = <<"n1">>,
               log_id = <<"n1">>,
               metrics_key = n1,
               machine = {machine, ?FUNCTION_NAME, #{}},
               machine_version = 0,
               machine_versions = [{0, 0}],
               effective_machine_version = 1,
               effective_machine_module = ra_machine_simple,
               system_config = ra_system:default_config()
              },
    Leader = #{cfg => Cfg,
               cluster =>
               #{N1 => new_peer_with(#{match_index => 0}),
                 N2 => new_peer_with(#{match_index => 4, next_index => 5,
                                       commit_index_sent => 4}),
                 N3 => new_peer_with(#{match_index => 0, next_index => 1})},
               cluster_change_permitted => true,
               cluster_index_term => {0,0},
               commit_index => 4,
               current_term => Term,
               last_applied => 4,
               log => Log0,
               machine_state => [{4,apple}],
               query_index => 0,
               queries_waiting_heartbeats => queue:new(),
               pending_consistent_queries => []},
    ISR = #install_snapshot_result{term = Term,
                                   last_index = 2,
                                   last_term = 1},
    {leader, #{cluster := #{N3 := #{match_index := 2,
                                    commit_index_sent := 4,
                                    next_index := 5}}},
     Effects} = ra_server:handle_leader({N3, ISR}, Leader),
    ?assert(lists:any(fun({send_rpc, {n3, _},
                           #append_entries_rpc{entries = [{3, _, _},
                                                          {4, _, _}]}}) ->
                              true;
                         (_) -> false end, Effects)),
    ok.

follower_heartbeat(_Config) ->
    State = base_state(3, ?FUNCTION_NAME),
    #{current_term := Term,
      query_index := QIndex,
      cluster := Cluster,
      cfg := #cfg{id = Id},
      leader_id := LeaderId} = State,
    #{Id := _} = Cluster,
    NewQueryIndex = QIndex + 1,
    LowerTerm = Term - 1,
    Heartbeat = #heartbeat_rpc{query_index = NewQueryIndex,
                               leader_id = ?N1,
                               term = Term},

    %% Return lower term. No changes in state
    {follower,
     State,
     [{cast, LeaderId, {Id, #heartbeat_reply{term = Term,
                                             query_index = QIndex}}}]}
        = ra_server:handle_follower(Heartbeat#heartbeat_rpc{term = LowerTerm}, State),

    %% Reply to the same term. Update query index
    {follower,
     #{query_index := NewQueryIndex},
     [{cast,
       LeaderId,
       {Id, #heartbeat_reply{term = Term,
                             query_index = NewQueryIndex}}}]}
        = ra_server:handle_follower(Heartbeat#heartbeat_rpc{term = Term}, State),

    %% Reply to a higher term. Update follower term.
    NewTerm = Term + 1,
    {follower,
     #{query_index := NewQueryIndex,
       current_term := NewTerm,
       voted_for := undefined},
     [{cast,
       LeaderId,
       {Id, #heartbeat_reply{term = NewTerm,
                             query_index = NewQueryIndex}}}]}
        = ra_server:handle_follower(Heartbeat#heartbeat_rpc{term = NewTerm}, State).

follower_heartbeat_reply(_Config) ->
    State = base_state(3, ?FUNCTION_NAME),
    #{current_term := Term, leader_id := LeaderId} = State,
    HeartbeatReply = #heartbeat_reply{term = Term, query_index = 2},

    %% Ignore lower or same term
    {follower, State, []}
        = ra_server:handle_follower({LeaderId, HeartbeatReply}, State),
    {follower, State, []}
        = ra_server:handle_follower({LeaderId, HeartbeatReply#heartbeat_reply{term = Term - 1}}, State),

    %% Update term if the term is higher
    NewTerm = Term + 1,
    {follower, #{current_term := NewTerm,
                 voted_for := undefined}, []}
        = ra_server:handle_follower({LeaderId,
                                     HeartbeatReply#heartbeat_reply{term = NewTerm}}, State).

candidate_heartbeat(_Config) ->
    N1 = ?N1,
    State = base_state(3, ?FUNCTION_NAME),
    #{current_term := Term,
      leader_id := LeaderId,
      cfg := #cfg{id = Id},
      query_index := QueryIndex} = State,
    NewQueryIndex = QueryIndex + 1,
    Heartbeat = #heartbeat_rpc{query_index = NewQueryIndex,
                               term = Term,
                               leader_id = N1},

    %% Same term heartbeat is delayed and changes to follower
    {follower, State, [{next_event, Heartbeat}]}
        = ra_server:handle_candidate(Heartbeat, State),

    %% Higher term updates the candidate term and changes to follower
    NewTerm = Term + 1,
    HeartbeatHigherTerm = Heartbeat#heartbeat_rpc{term = NewTerm},
    {follower, #{current_term := NewTerm,
                 voted_for := undefined},
     [{next_event, HeartbeatHigherTerm}]}
        = ra_server:handle_candidate(HeartbeatHigherTerm, State),

    %% Lower term does not change state
    LowTerm = Term - 1,
    HeartbeatLowTerm = Heartbeat#heartbeat_rpc{term = LowTerm},
    {candidate, State,
     [{cast,
       LeaderId,
       {Id, #heartbeat_reply{term = Term,
                             query_index = QueryIndex}}}]}
        = ra_server:handle_candidate(HeartbeatLowTerm, State).

candidate_heartbeat_reply(_Config) ->
    State = base_state(3, ?FUNCTION_NAME),
    #{current_term := Term} = State,

    HeartbeatReply = #heartbeat_reply{term = Term, query_index = 2},
    %% Same term is ignored
    {candidate, State, []}
        = ra_server:handle_candidate({{no_peer, node()}, HeartbeatReply}, State),

    %% Lower term is ignored
    {candidate, State, []}
        = ra_server:handle_candidate({{no_peer, node()}, HeartbeatReply#heartbeat_reply{term = Term - 1}}, State),

    %% Higher term updates term and changes to follower
    NewTerm = Term + 1,
    {follower, #{current_term := NewTerm,
                 voted_for := undefined}, []}
        = ra_server:handle_candidate({{no_peer, node()}, HeartbeatReply#heartbeat_reply{term = NewTerm}}, State).

pre_vote_heartbeat(_Config) ->
    State = (base_state(3, ?FUNCTION_NAME))#{votes => 1},
    #{current_term := Term,
      query_index := QueryIndex,
      leader_id := LeaderId,
      cfg := #cfg{id = Id}} = State,
    NewQueryIndex = QueryIndex + 1,
    Heartbeat = #heartbeat_rpc{query_index = NewQueryIndex,
                               term = Term,
                               leader_id = ?N1},

    StateWithoutVotes = State#{votes => 0},
    %% Same term changes to follower and resets votes
    {follower, StateWithoutVotes, [{next_event, Heartbeat}]}
        = ra_server:handle_pre_vote(Heartbeat, State),

    %% Higher term updates term, changes to follower and resets votes
    NewTerm = Term + 1,
    HeartbeatHigherTerm = Heartbeat#heartbeat_rpc{term = NewTerm},
    {follower, #{votes := 0,
                 current_term := NewTerm,
                 voted_for := undefined},
     [{next_event, HeartbeatHigherTerm}]}
        = ra_server:handle_pre_vote(HeartbeatHigherTerm, State),

    %% Lower term does not change state
    LowTerm = Term - 1,
    {pre_vote, State,
     [{cast, LeaderId,
       {Id, #heartbeat_reply{term = Term,
                             query_index = QueryIndex}}}]}
        = ra_server:handle_pre_vote(Heartbeat#heartbeat_rpc{term = LowTerm},
                                    State).

pre_vote_heartbeat_reply(_Config) ->
    State = base_state(3, ?FUNCTION_NAME),
    #{current_term := Term} = State,

    HeartbeatReply = #heartbeat_reply{term = Term,
                                      query_index = 2},

    %% Heartbeat reply with same term is ignored
    {pre_vote, State, []}
        = ra_server:handle_pre_vote({{no_peer, node()}, HeartbeatReply}, State),

    %% Heartbeat reply with lower term is ignored
    {pre_vote, State, []}
        = ra_server:handle_pre_vote(
            {{no_peer, node()}, HeartbeatReply#heartbeat_reply{term = Term - 1}},
            State),

    %% Heartbeat reply with higher term updates the term and resets to follower
    NewTerm = Term + 1,
    {follower, #{votes := 0,
                 current_term := NewTerm,
                 voted_for := undefined}, []}
        = ra_server:handle_pre_vote(
            {{no_peer, node()}, HeartbeatReply#heartbeat_reply{term = NewTerm}},
            State).

leader_heartbeat(_Config) ->
    State = base_state(3, ?FUNCTION_NAME),
    #{current_term := Term,
      leader_id := LeaderId,
      cfg := #cfg{id = Id},
      query_index := QueryIndex} = State,
    NewQueryIndex = QueryIndex + 1,
    Heartbeat = #heartbeat_rpc{query_index = NewQueryIndex,
                               term = Term,
                               leader_id = LeaderId},

    %% Same term throws an error
    try
        ra_server:handle_leader(Heartbeat, State),
        error(expected_exit)
    catch exit:leader_saw_heartbeat_rpc_in_same_term ->
        ok
    end,

    %% Higher term updates term and changes to follower
    NewTerm = Term + 1,
    HeartbeatHigherTerm = Heartbeat#heartbeat_rpc{term = NewTerm},
    StateWithHigherTerm = set_peer_query_index(
                                State#{current_term => NewTerm,
                                       leader_id => undefined,
                                       voted_for => undefined},
                                Id, 0),
    {follower, StateWithHigherTerm, [{next_event, HeartbeatHigherTerm}]}
        = ra_server:handle_leader(HeartbeatHigherTerm, State),

    %% Lower term does not change state
    LowTerm = Term - 1,
    HeartbeatLowTerm = Heartbeat#heartbeat_rpc{term = LowTerm},
    {leader, State,
     [{cast,
       LeaderId,
       {Id, #heartbeat_reply{term = Term,
                             query_index = QueryIndex}}}]}
        = ra_server:handle_leader(HeartbeatLowTerm, State).

leader_heartbeat_reply_node_size_5(_Config) ->
    N3 = ?N3,
    BaseState = base_state(5, ?FUNCTION_NAME),
    #{current_term := Term,
      cfg := #cfg{id = Id},
      commit_index := CommitIndex} = BaseState,
    QueryIndex = 2,
    QueryRef1 = {from1, fun(_) -> query_result1 end, CommitIndex},
    %% Increase self query index to cover more cases
    ReplyingPeerId = ?N2,
    HeartbeatReply = #heartbeat_reply{term = Term,
                                      query_index = QueryIndex},
    WaitingQuery = queue:in({QueryIndex, QueryRef1}, queue:new()),
    State0 = set_peer_query_index(BaseState#{query_index => QueryIndex + 1,
                                             queries_waiting_heartbeats => WaitingQuery},
                                  Id, QueryIndex + 1),

    {leader, State, []}
        = ra_server:handle_leader({ReplyingPeerId, HeartbeatReply}, State0),

    {leader, _, [{reply, from1, {ok, query_result1, Id}}]}
        = ra_server:handle_leader({N3, HeartbeatReply}, State),
    ok.

leader_heartbeat_reply_same_term(_Config) ->
    BaseState = base_state(3, ?FUNCTION_NAME),
    #{current_term := Term,
      cfg := #cfg{id = Id},
      commit_index := CommitIndex} = BaseState,
    QueryIndex = 2,
    QueryRef1 = {from1, fun(_) -> query_result1 end, CommitIndex},
    QueryRef2 = {from2, fun(_) -> query_result2 end, CommitIndex - 1},
    %% Increase self query index to cover more cases
    State = set_peer_query_index(BaseState#{query_index => QueryIndex + 1},
                                 Id, QueryIndex + 1),

    ReplyingPeerId = ?N2,
    HeartbeatReply = #heartbeat_reply{term = Term,
                                      query_index = QueryIndex},

    %% Reply updates query index for peer
    StateWithQueryIndexForPeer = set_peer_query_index(State, ReplyingPeerId,
                                                      QueryIndex),

    {leader, StateWithQueryIndexForPeer, []}
        = ra_server:handle_leader({ReplyingPeerId, HeartbeatReply}, State),

    WaitingQuery = queue:in({QueryIndex, QueryRef1}, queue:new()),
    StateWithQuery = State#{queries_waiting_heartbeats => WaitingQuery},

    %% Reply is ignored if peer is not known
    {leader, StateWithQuery, []}
        = ra_server:handle_leader({{no_peer, node()}, HeartbeatReply}, StateWithQuery),

    %% Reply updates query index but does not apply higher index queries
    LowerQueryIndex = QueryIndex - 1,
    StateWithQueryIndexForPeerAndQueries = set_peer_query_index(StateWithQuery, ReplyingPeerId, LowerQueryIndex),

    {leader, StateWithQueryIndexForPeerAndQueries, []}
        = ra_server:handle_leader(
            {ReplyingPeerId,
             HeartbeatReply#heartbeat_reply{query_index = LowerQueryIndex}},
            StateWithQuery),

    %% Reply applies a query if there is a consensus
    %% A single reply in 3 node cluster is a consensus
    {leader, StateWithQueryIndexForPeer,
     [{reply, from1, {ok, query_result1, Id}}]}
        = ra_server:handle_leader({ReplyingPeerId, HeartbeatReply},
                                  StateWithQuery),

    % %% Reply does not apply a query if there is no consensus
    % %% Set own query_index to lower value to emulate that.
    % %% This will not happen in normal operation, but for three nodes
    % %% it's hard to emulate. For five nodes it is easy.
    % StateWithLowQueryIndex = set_peer_query_index(StateWithQuery, n3,
    %                                               QueryIndex - 1),

    % StateWithLowQueryIndexAndQueryIndexForPeer =
    %     set_peer_query_index(StateWithLowQueryIndex, ReplyingPeerId, QueryIndex),

    % {leader, StateWithLowQueryIndexAndQueryIndexForPeer, []}
    %     = ra_server:handle_leader({ReplyingPeerId, HeartbeatReply},
    %                               StateWithLowQueryIndex),

    HighQueryIndex = QueryIndex + 1,

    WaitingQueries = queue:in({HighQueryIndex, QueryRef2}, WaitingQuery),
    StateWithTwoQueries = State#{queries_waiting_heartbeats => WaitingQueries},

    StateWithHighQueryIndexForPeer = set_peer_query_index(State, ReplyingPeerId, HighQueryIndex),

    {_, WaitingQuery2} = queue:out(WaitingQueries),
    StateWithSecondQuery = StateWithQueryIndexForPeer#{queries_waiting_heartbeats => WaitingQuery2},

    %% Apply single query out of 2 if there is a consensus for some
    {leader, StateWithSecondQuery, [{reply, from1, {ok, query_result1, Id}}]}
        = ra_server:handle_leader({ReplyingPeerId, HeartbeatReply}, StateWithTwoQueries),

    %% Apply multiple queries if there is consensus for all
    HighIndexReply = HeartbeatReply#heartbeat_reply{query_index = HighQueryIndex},
    {leader, StateWithHighQueryIndexForPeer,
     [{reply, from2, {ok, query_result2, Id}},
      {reply, from1, {ok, query_result1, Id}}]}
        = ra_server:handle_leader({ReplyingPeerId, HighIndexReply}, StateWithTwoQueries),
    ok.

leader_consistent_query_delay(_Config) ->

    N2 = ?N2, N3 = ?N3,
    State = (base_state(3, ?FUNCTION_NAME))#{cluster_change_permitted => false},
    #{commit_index := CommitIndex,
      query_index := QueryIndex,
      current_term := Term,
      cfg := #cfg{id = Id}} = State,

    %% If cluster changes are not permitted - delay the heartbeats
    Fun = fun(_) -> query_result end,
    StateWithPending = State#{pending_consistent_queries => [{from, Fun, CommitIndex}]},
    {leader, StateWithPending, []}
        = ra_server:handle_leader({consistent_query, from, Fun}, State),

    %% Pending stack together
    %% Order does not matter here, btw.
    StateWithMorePending =
        State#{pending_consistent_queries => [{from1, Fun, CommitIndex},
                                              {from, Fun, CommitIndex}]},
    {leader, StateWithMorePending, []}
        = ra_server:handle_leader({consistent_query, from1, Fun}, StateWithPending),


    QueryIndex1 = QueryIndex + 1,
    QueryIndex2 = QueryIndex + 2,
    HeartBeatRpc1 = #heartbeat_rpc{term = Term,
                                   query_index = QueryIndex1,
                                   leader_id = Id},
    HeartBeatRpc2 = #heartbeat_rpc{term = Term,
                                   query_index = QueryIndex2,
                                   leader_id = Id},
    %% Technically, order should not matter here.
    %% In theory these queries may have the same query index
    WaitingQueries = queue:in({QueryIndex2, {from, Fun, CommitIndex}},
                              queue:in({QueryIndex1, {from1, Fun, CommitIndex}}, queue:new())),

    %% Send heartbeats as soon as cluster changes permitted
    {leader, #{cluster_change_permitted := true,
               queries_waiting_heartbeats := WaitingQueries,
               query_index := QueryIndex2},
    %% There can be more effects.
     [{send_rpc, N2, HeartBeatRpc2},
      {send_rpc, N3, HeartBeatRpc2},
      {send_rpc, N2, HeartBeatRpc1},
      {send_rpc, N3, HeartBeatRpc1}
      | _]} =
        enable_cluster_change(StateWithMorePending).

leader_consistent_query(_Config) ->
    N2 = ?N2, N3 = ?N3,
    State = base_state(3, ?FUNCTION_NAME),
    #{commit_index := CommitIndex,
      query_index := QueryIndex,
      current_term := Term,
      cfg := #cfg{id = Id}} = State,

    Fun = fun(_) -> query_result end,
    Query1 = {from1, Fun, CommitIndex},
    Query2 = {from2, Fun, CommitIndex},
    QueryIndex1 = QueryIndex + 1,
    QueryIndex2 = QueryIndex1 + 1,
    WaitingQuery = queue:in({QueryIndex1, Query1}, queue:new()),
    WaitingQueries = queue:in({QueryIndex2, Query2}, WaitingQuery),

    HeartBeatRpc1 = #heartbeat_rpc{term = Term,
                                   query_index = QueryIndex1,
                                   leader_id = Id},
    HeartBeatRpc2 = #heartbeat_rpc{term = Term,
                                   query_index = QueryIndex2,
                                   leader_id = Id},

    %% Queue the query and create heatbeat effects
    {leader, #{query_index := QueryIndex1,
               queries_waiting_heartbeats := WaitingQuery} = StateWithQuery,
     [{send_rpc, N2, HeartBeatRpc1},
      {send_rpc, N3, HeartBeatRpc1}]} =
        ra_server:handle_leader({consistent_query, from1, Fun}, State),

    {leader, #{query_index := QueryIndex2,
               queries_waiting_heartbeats := WaitingQueries},
     [{send_rpc, N2, HeartBeatRpc2},
      {send_rpc, N3, HeartBeatRpc2}]} =
        ra_server:handle_leader({consistent_query, from2, Fun}, StateWithQuery).

enable_cluster_change(State0) ->
    {leader, #{cluster_change_permitted := false} = State1, _Effects} =
        ra_server:handle_leader({command, {noop, meta(), 0}}, State0),
    {leader, State, _} = ra_server:handle_leader({ra_log_event, {written, {4, 4, 5}}}, State1),
    AEReply = {?N2, #append_entries_reply{term = 5, success = true,
                                          next_index = 5,
                                          last_index = 4, last_term = 5}},
    % noop consensus
    {leader, #{cluster_change_permitted := true}, _} =
        ra_server:handle_leader(AEReply, State).

await_condition_heartbeat_dropped(_Config) ->
    State = (base_state(3, ?FUNCTION_NAME))#{condition => fun(_,S) -> {false, S} end},
    #{current_term := Term,
      query_index := QueryIndex,
      cfg := #cfg{id = Id}} = State,

    Heartbeat = #heartbeat_rpc{term = Term,
                               query_index = QueryIndex,
                               leader_id = Id},
    {await_condition, State, []} =
        ra_server:handle_await_condition(Heartbeat, State),
    %% Term does not matter
    {await_condition, State, []} =
        ra_server:handle_await_condition(Heartbeat#heartbeat_rpc{term = Term + 1},
                                         State),
    {await_condition, State, []} =
        ra_server:handle_await_condition(Heartbeat#heartbeat_rpc{term = Term - 1},
                                         State).

await_condition_heartbeat_reply_dropped(_Config) ->
    State = (base_state(3, ?FUNCTION_NAME))#{condition => fun(_,S) -> {false, S} end},
    #{current_term := Term,
      query_index := QueryIndex} = State,

    HeartbeatReply = #heartbeat_reply{term = Term,
                                 query_index = QueryIndex},
    {await_condition, State, []} =
        ra_server:handle_await_condition({?N2, HeartbeatReply}, State),
    %% Term does not matter
    {await_condition, State, []} =
        ra_server:handle_await_condition({?N2, HeartbeatReply#heartbeat_reply{term = Term + 1}},
                                         State),
    {await_condition, State, []} =
        ra_server:handle_await_condition({?N2, HeartbeatReply#heartbeat_reply{term = Term - 1}},
                                         State).

receive_snapshot_heartbeat_dropped(_Config) ->
    State = base_state(3, ?FUNCTION_NAME),
    #{current_term := Term,
      query_index := QueryIndex,
      cfg := #cfg{id = Id}} = State,

    Heartbeat = #heartbeat_rpc{term = Term,
                               query_index = QueryIndex,
                               leader_id = Id},
    {receive_snapshot, State, []} =
        ra_server:handle_receive_snapshot(Heartbeat, State),
    %% Term does not matter
    {receive_snapshot, State, []} =
        ra_server:handle_receive_snapshot(Heartbeat#heartbeat_rpc{term = Term + 1},
                                          State),
    {receive_snapshot, State, []} =
        ra_server:handle_receive_snapshot(Heartbeat#heartbeat_rpc{term = Term - 1},
                                          State).

receive_snapshot_heartbeat_reply_dropped(_config) ->
    State = base_state(3, ?FUNCTION_NAME),
    #{current_term := Term,
      query_index := QueryIndex} = State,

    HeartbeatReply = #heartbeat_reply{term = Term,
                                      query_index = QueryIndex},
    {receive_snapshot, State, []} =
        ra_server:handle_receive_snapshot(HeartbeatReply, State),
    %% Term does not matter
    {receive_snapshot, State, []} =
        ra_server:handle_receive_snapshot(HeartbeatReply#heartbeat_reply{term = Term + 1},
                                          State),
    {receive_snapshot, State, []} =
        ra_server:handle_receive_snapshot(HeartbeatReply#heartbeat_reply{term = Term - 1},
                                          State).

handle_down(_config) ->
    State0 = base_state(3, ?FUNCTION_NAME),
    %% this should commit a command
    {leader, #{log := Log} =  State, _} =
        ra_server:handle_down(leader, machine, self(), noproc, State0),
    ?assertEqual({4, 5}, ra_log:last_index_term(Log)),
    %% this should be ignored as may happen if state machine doesn't demonitor
    %% on state changes
    {follower, State, []} =
        ra_server:handle_down(follower, machine, self(), noproc, State),

    ok.

set_peer_query_index(State, PeerId, QueryIndex) ->
    #{cluster := Cluster} = State,
    #{PeerId := Peer} = Cluster,
    State#{cluster := Cluster#{PeerId => Peer#{query_index => QueryIndex}}}.

leader_heartbeat_reply_lower_term(_Config) ->
    State = base_state(3, ?FUNCTION_NAME),
    #{current_term := Term,
      query_index := QueryIndex} = State,
    HeartbeatReply = #heartbeat_reply{term = Term - 1,
                                      query_index = QueryIndex},

    %% Lower term replies are ignored
    ReplyingPeerId = ?N2,
    {leader, State, []} =
        ra_server:handle_leader({ReplyingPeerId, HeartbeatReply}, State),

    %% Ignores query index
    ReplyWithHigherIndex = HeartbeatReply#heartbeat_reply{query_index = QueryIndex + 1},
    {leader, State, []} =
        ra_server:handle_leader({ReplyingPeerId, ReplyWithHigherIndex}, State).

leader_heartbeat_reply_higher_term(_Config) ->
    State = base_state(3, ?FUNCTION_NAME),
    #{current_term := Term,
      query_index := QueryIndex} = State,
    NewTerm = Term + 1,
    HeartbeatReply = #heartbeat_reply{term = NewTerm,
                                      query_index = QueryIndex},
    ReplyingPeerId = ?N2,

    %% Higher term is an error
    StateWithNewTerm = State#{current_term => NewTerm,
                              voted_for => undefined,
                              leader_id => undefined},
    {follower, StateWithNewTerm, []} =
        ra_server:handle_leader({ReplyingPeerId, HeartbeatReply}, State),

    %% Ignores query index
    ReplyWithHigherIndex = HeartbeatReply#heartbeat_reply{query_index = QueryIndex + 1},
    {follower, StateWithNewTerm, []} =
        ra_server:handle_leader({ReplyingPeerId, ReplyWithHigherIndex}, State).

% %%% helpers

init_servers(ServerIds, Machine) ->
    lists:foldl(fun (ServerId, Acc) ->
                        Args = #{cluster_name => some_id,
                                 id => ServerId,
                                 uid => atom_to_binary(element(1, ServerId), utf8),
                                 initial_members => ServerIds,
                                 log_init_args => #{uid => <<>>},
                                 machine => Machine},
                        Acc#{ServerId => {follower, ra_server_init(Args), []}}
                end, #{}, ServerIds).

list(L) when is_list(L) -> L;
list(L) -> [L].

entry(Idx, Term, Data) ->
    {Idx, Term, {'$usr', meta(), Data, after_log_append}}.

empty_state(NumServers, Id) ->
    Servers = lists:foldl(fun(N, Acc) ->
                                [{list_to_atom("n" ++ integer_to_list(N)), node()}
                                 | Acc]
                        end, [], lists:seq(1, NumServers)),
    ra_server_init(#{cluster_name => someid,
                     id => {Id, node()},
                     uid => atom_to_binary(Id, utf8),
                     initial_members => Servers,
                     log_init_args => #{uid => <<>>},
                     machine => {simple, fun (E, _) -> E end, <<>>}}). % just keep last applied value

base_state(NumServers, MacMod) ->
    Log0 = lists:foldl(fun(E, L) ->
                               ra_log:append(E, L)
                       end, ra_log:init(#{system_config => ra_system:default_config(),
                                          uid => <<>>}),
                       [{1, 1, usr(<<"hi1">>)},
                        {2, 3, usr(<<"hi2">>)},
                        {3, 5, usr(<<"hi3">>)}]),
    {Log, _} = ra_log:handle_event({written, {1, 3, 5}}, Log0),

    Servers = lists:foldl(fun(N, Acc) ->
                                Name = {list_to_atom("n" ++ integer_to_list(N)), node()},
                                Acc#{Name =>
                                     new_peer_with(#{next_index => 4,
                                                     match_index => 3})}
                        end, #{}, lists:seq(1, NumServers)),
    mock_machine(MacMod),
    Cfg = #cfg{id = ?N1,
               uid = <<"n1">>,
               log_id = <<"n1">>,
               metrics_key = n1,
               machine = {machine, MacMod, #{}}, % just keep last applied value
               machine_version = 0,
               machine_versions = [{0, 0}],
               effective_machine_version = 0,
               effective_machine_module = MacMod,
               system_config = ra_system:default_config()
              },
    #{cfg => Cfg,
      leader_id => ?N1,
      cluster => Servers,
      cluster_index_term => {0, 0},
      cluster_change_permitted => true,
      machine_state => <<"hi3">>, % last entry has been applied
      current_term => 5,
      commit_index => 3,
      last_applied => 3,
      log => Log,
      query_index => 0,
      queries_waiting_heartbeats => queue:new(),
      pending_consistent_queries => []}.

mock_machine(Mod) ->
    meck:new(Mod, [non_strict]),
    meck:expect(Mod, init, fun (_) -> init_state end),
    %% just keep the latest command as the state
    meck:expect(Mod, apply, fun (_, Cmd, _) -> {Cmd, ok} end),
    ok.

usr_cmd(Data) ->
    {command, usr(Data)}.

usr(Data) ->
    {'$usr', meta(), Data, after_log_append}.

meta() ->
    #{from => {self(), make_ref()},
      ts => os:system_time(millisecond)}.

dump(T) ->
    ct:pal("DUMP: ~p", [T]),
    T.

new_peer() ->
    #{next_index => 1,
      match_index => 0,
      query_index => 0,
      commit_index_sent => 0,
      status => normal}.

new_peer_with(Map) ->
    maps:merge(new_peer(), Map).

snap_meta(Idx, Term) ->
    snap_meta(Idx, Term, []).

snap_meta(Idx, Term, Cluster) ->
    #{index => Idx,
      term => Term,
      cluster => Cluster,
      machine_version => 0}.

