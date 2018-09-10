-module(ra_node_SUITE).

-compile(export_all).

-include("ra.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

all() ->
    [
     init,
     init_restores_cluster_changes,
     election_timeout,
     follower_aer_term_mismatch,
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
     consistent_query,
     leader_noop_operation_enables_cluster_change,
     leader_node_join,
     leader_node_leave,
     leader_is_removed,
     follower_cluster_change,
     leader_applies_new_cluster,
     leader_appends_cluster_change_then_steps_before_applying_it,
     leader_receives_install_snapshot_rpc,
     follower_installs_snapshot,
     snapshotted_follower_received_append_entries,
     leader_received_append_entries_reply_with_stale_last_index,
     leader_receives_install_snapshot_result,
     follower_aer_1,
     follower_aer_2,
     follower_aer_3,
     follower_aer_4,
     follower_aer_5,
     follower_catchup_condition,
     wal_down_condition
    ].

-define(MACFUN, fun (E, _) -> E end).

groups() ->
    [ {tests, [], all()} ].

init_per_suite(Config) ->
    Config.

end_per_suite(Config) ->
    Config.

% init_per_group(_, Config) ->
%     PrivDir = ?config(priv_dir, Config),
%     _ = application:load(ra),
%     ok = application:set_env(ra, data_dir, PrivDir),
%     application:ensure_all_started(ra),
%     application:ensure_all_started(lg),
%     Config.

% end_per_group(_, Config) ->
%     _ = application:stop(ra),
%     Config.

init_per_testcase(TestCase, Config) ->
    ok = setup_log(),
    [{test_case, TestCase} | Config].

end_per_testcase(_TestCase, Config) ->
    meck:unload(),
    Config.

id(X) -> X.

ra_node_init(Conf) ->
    ra_node:recover(element(1, ra_node:init(Conf))).

setup_log() ->
    ok = meck:new(ra_log, []),
    meck:expect(ra_log, init, fun(C) -> ra_log_memory:init(C) end),
    meck:expect(ra_log, write_meta, fun ra_log_memory:write_meta/3),
    meck:expect(ra_log, write_meta,
                fun (K, V, L, _) ->
                        ra_log_memory:write_meta(K, V, L)
                end),
    meck:expect(ra_log, read_meta, fun ra_log_memory:read_meta/2),
    meck:expect(ra_log, read_meta, fun (K, L, D) ->
                                           ra_lib:default(
                                             ra_log_memory:read_meta(K, L), D)
                                   end),
    meck:expect(ra_log, read_snapshot, fun ra_log_memory:read_snapshot/1),
    meck:expect(ra_log, snapshot_index_term, fun ra_log_memory: snapshot_index_term/1),
    meck:expect(ra_log, install_snapshot, fun ra_log_memory:install_snapshot/2),
    meck:expect(ra_log, take, fun ra_log_memory:take/3),
    meck:expect(ra_log, release_resources, fun ra_log_memory:release_resources/2),
    meck:expect(ra_log, append_sync,
                fun({Idx, Term, _} = E, L) ->
                        L1 = ra_log_memory:append(E, L),
                        ra_log_memory:handle_event({written, {Idx, Idx, Term}}, L1)
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
    ok.

init(_Config) ->
    #{id := Id,
      uid := UId,
      cluster := Cluster,
      current_term := CurrentTerm,
      log := Log0} = base_state(3),
    % ensure it is written to the log
    InitConf = #{cluster_id => init,
                 id => Id,
                 uid => UId,
                 log_init_args => #{data_dir => "", uid => <<>>},
                 machine => {simple, ?MACFUN, <<"hi3">>},
                 initial_nodes => []}, % init without known peers
    % new
    #{current_term := 0,
      voted_for := undefined} = ra_node_init(InitConf),
    % previous data
    Log1 = ra_log_memory:write_meta_f(voted_for, some_node, Log0),
    Log = ra_log_memory:write_meta_f(current_term, CurrentTerm, Log1),
    meck:expect(ra_log, init, fun (_) -> Log end),
    #{current_term := 5,
      voted_for := some_node} = ra_node_init(InitConf),
    % snapshot
    Snapshot = {3, 5, maps:keys(Cluster), {simple, ?MACFUN, "hi1+2+3"}},
    LogS = ra_log_memory:install_snapshot(Snapshot, Log),
    meck:expect(ra_log, init, fun (_) -> LogS end),
    #{current_term := 5,
      commit_index := 3,
      machine_state := {simple, _, "hi1+2+3"},
      cluster := ClusterOut,
      voted_for := some_node} = ra_node_init(InitConf),
    ?assertEqual(maps:keys(Cluster), maps:keys(ClusterOut)),
    ok.

init_restores_cluster_changes(_Config) ->
    InitConf = #{cluster_id => init_restores_cluster_changes,
                 id => n1,
                 uid => <<"n1">>,
                 log_init_args => #{data_dir => "", uid => <<>>},
                 machine => {simple, fun erlang:'+'/2, 0},
                 initial_nodes => []}, % init without known peers
    % new
    {leader, State00, _} =
        ra_node:handle_candidate(#request_vote_result{term = 1,
                                                      vote_granted = true},
                                 (ra_node_init(InitConf))#{votes => 0,
                                                           current_term => 1,
                                                           voted_for => n1}),
    {leader, State0 = #{cluster := Cluster0}, _} =
        ra_node:handle_leader({command, noop}, State00),
    {leader, State, _} = ra_node:handle_leader(written_evt({1, 1, 1}), State0),
    ?assert(maps:size(Cluster0) =:= 1),

    % n2 joins
    {leader, #{cluster := Cluster,
               log := Log0}, _} =
        ra_node:handle_leader({command, {'$ra_join', meta(),
                                         n2, await_consensus}}, State),
    ?assert(maps:size(Cluster) =:= 2),
    % intercept ra_log:init call to simulate persisted log data
    % ok = meck:new(ra_log, [passthrough]),
    meck:expect(ra_log, init, fun (_) -> Log0 end),

    #{cluster := #{n1 := _, n2 := _}} = ra_node_init(InitConf),
    ok.

election_timeout(_Config) ->
    State = base_state(3),
    Msg = election_timeout,
    %
    % follower
    {pre_vote, #{current_term := 5, votes := 0,
                 pre_vote_token := Token},
     [{next_event, cast, #pre_vote_result{term = 5, token = Token,
                                          vote_granted = true}},
      {send_vote_requests,
       [{n2, #pre_vote_rpc{term = 5, token = Token,
                           last_log_index = 3,
                           last_log_term = 5,
                           candidate_id = n1}},
        {n3, _}]}]} =
        ra_node:handle_follower(Msg, State),

    % pre_vote
    {pre_vote, #{current_term := 5, votes := 0,
                 pre_vote_token := Token1},
     [{next_event, cast, #pre_vote_result{term = 5, token = Token1,
                                          vote_granted = true}},
      {send_vote_requests,
       [{n2, #pre_vote_rpc{term = 5, token = Token1,
                           last_log_index = 3,
                           last_log_term = 5,
                           candidate_id = n1}},
        {n3, _}]}]} =
        ra_node:handle_pre_vote(Msg, State),

    %% assert tokens are not the same
    ?assertNotEqual(Token, Token1),

    % candidate
    VoteRpc = #request_vote_rpc{term = 6, candidate_id = n1,
                                last_log_index = 3, last_log_term = 5},
    VoteForSelfEvent = {next_event, cast,
                        #request_vote_result{term = 6, vote_granted = true}},
    {candidate, #{current_term := 6, votes := 0},
     [VoteForSelfEvent, {send_vote_requests, [{n2, VoteRpc}, {n3, VoteRpc}]}]} =
        ra_node:handle_candidate(Msg, State),
    ok.

follower_aer_1(_Config) ->
    % Scenario 1
    Self = n1,

    % AER with index [1], commit = 0, commit_index = 0
    Init = empty_state(3, Self),
    AER1 = #append_entries_rpc{term = 1, leader_id = n1, prev_log_index = 0,
                               prev_log_term = 0, leader_commit = 0,
                               entries = [entry(1, 1, one)]},
    {follower, State1 = #{leader_id := n1, current_term := 1,
                          commit_index := 0, last_applied := 0}, _} =
    ra_node:handle_follower(AER1, Init),

    % AER with index [2], leader_commit = 1, commit_index = 1
    AER2 = #append_entries_rpc{term = 1, leader_id = n1, prev_log_index = 1,
                               prev_log_term = 1, leader_commit = 1,
                               entries = [entry(2, 1, two)]},
    {follower, State2 = #{leader_id := n1, current_term := 1,
                          commit_index := 1, last_applied := 0,
                          machine_state := {simple, _, <<>>}},
     _} = ra_node:handle_follower(AER2, State1),

    % {written, 1} -> last_applied: 1 - replies with last_index = 1, next_index = 3
    {follower, State3 = #{leader_id := n1, current_term := 1,
                          commit_index := 1, last_applied := 1,
                          machine_state := {simple, _, one}},
     [{cast, n1, {Self, #append_entries_reply{next_index = 3,
                                              last_term = 1,
                                              last_index = 1}}}, _]}
        = ra_node:handle_follower({ra_log_event, {written, {1, 1, 1}}}, State2),

    % AER with index [3], commit = 3 -> commit_index = 3
    AER3 = #append_entries_rpc{term = 1, leader_id = n1, prev_log_index = 2,
                               prev_log_term = 1, leader_commit = 3,
                               entries = [entry(3, 1, tre)]},
    {follower, State4 = #{leader_id := n1, current_term := 1,
                          commit_index := 3, last_applied := 1,
                          machine_state := {simple, _, one}},
     _} = ra_node:handle_follower(AER3, State3),

    % {written, 2} -> last_applied: 2, commit_index = 3 reply = 2, next_index = 4
    {follower, State5 = #{leader_id := n1, current_term := 1,
                          commit_index := 3, last_applied := 2,
                          machine_state := {_, _, two}},
     [{cast, n1, {Self, #append_entries_reply{next_index = 4,
                                              last_term = 1,
                                              last_index = 2}}}, _]}
        = ra_node:handle_follower({ra_log_event, {written, {2, 2, 1}}}, State4),

    % AER with index [] -> last_applied: 2 - replies with last_index = 2,
        % next_index = 4
    % empty AER before {written, 3} is received
    AER4 = #append_entries_rpc{term = 1, leader_id = n1, prev_log_index = 3,
                               prev_log_term = 1, leader_commit = 3,
                               entries = []},
    {follower, State6 = #{leader_id := n1, current_term := 1,
                          commit_index := 3, last_applied := 2,
                          machine_state := {simple, _, two}},
     [{cast, n1, {Self, #append_entries_reply{next_index = 4,
                                              last_term = 1,
                                              last_index = 2}}} | _]}
        = ra_node:handle_follower(AER4, State5),

    % {written, 3} -> commit_index = 3, last_applied = 3 : reply last_index = 3
    {follower, #{leader_id := n1, current_term := 1,
                 commit_index := 3, last_applied := 3,
                 machine_state := {_, _, tre}},
     [{cast, n1, {Self, #append_entries_reply{next_index = 4,
                                              last_term = 1,
                                              last_index = 3}}}, _]}
        = ra_node:handle_follower({ra_log_event, {written, {3, 3, 1}}}, State6),
    ok.

follower_aer_2(_Config) ->
    % Scenario 2
    % empty AER applies previously replicated entry
    % AER with index [1], leader_commit = 0
    Init = empty_state(3, n2),
    AER1 = #append_entries_rpc{term = 1, leader_id = n1, prev_log_index = 0,
                               prev_log_term = 0, leader_commit = 0,
                               entries = [entry(1, 1, one)]},
    {follower, State1 = #{leader_id := n1, current_term := 1,
                          commit_index := 0, last_applied := 0},
     _} = ra_node:handle_follower(AER1, Init),

    % {written, 1} -> last_applied: 0, reply: last_applied = 1, next_index = 2
    {follower, State2 = #{leader_id := n1, current_term := 1,
                          commit_index := 0, last_applied := 0,
                          machine_state := {simple, _, <<>>}},
     [{cast, n1, {n2, #append_entries_reply{next_index = 2,
                                            last_term = 1,
                                            last_index = 1}}}, _]}
        = ra_node:handle_follower({ra_log_event, {written, {1, 1, 1}}}, State1),

    % AER with index [], leader_commit = 1 -> last_applied: 1, reply: last_index = 1, next_index = 2
    AER2 = #append_entries_rpc{term = 1, leader_id = n1, prev_log_index = 1,
                               prev_log_term = 1, leader_commit = 1,
                               entries = []},
    {follower, #{leader_id := n1, current_term := 1,
                 commit_index := 1, last_applied := 1,
                 machine_state := {simple, _, one}},
     _} = ra_node:handle_follower(AER2, State2),
    ok.

follower_aer_3(_Config) ->
    % Scenario 3
    % AER with index [1], commit_index = 1
    Init = empty_state(3, n2),
    AER1 = #append_entries_rpc{term = 1, leader_id = n1, prev_log_index = 0,
                               prev_log_term = 0, leader_commit = 1,
                               entries = [entry(1, 1, one)]},
    {follower, State1 = #{leader_id := n1, current_term := 1,
                          commit_index := 1, last_applied := 0},
     _} = ra_node:handle_follower(AER1, Init),
    % {written, 1} -> last_applied: 1 - reply: last_index = 1, next_index = 2
    {follower, State2 = #{leader_id := n1, current_term := 1,
                          commit_index := 1, last_applied := 1,
                          machine_state := {simple, _, one}},
     [{cast, n1, {n2, #append_entries_reply{next_index = 2,
                                            last_term = 1,
                                            last_index = 1}}}, _]}
        = ra_node:handle_follower({ra_log_event, {written, {1, 1, 1}}}, State1),
    % AER with index [3] -> last_applied = 1 - reply(false): last_index, 1, next_index = 2
    AER2 = #append_entries_rpc{term = 1, leader_id = n1, prev_log_index = 2,
                               prev_log_term = 1, leader_commit = 3,
                               entries = [entry(3, 1, tre)]},
    {await_condition, State3 = #{leader_id := n1, current_term := 1,
                          commit_index := 1, last_applied := 1},
     [{cast, n1, {n2, #append_entries_reply{next_index = 2,
                                            success = false,
                                            last_term = 1,
                                            last_index = 1}}}]}
        = ra_node:handle_follower(AER2, State2),
    % AER with index [2,3,4], commit_index = 3 -> commit_index = 3
    AER3 = #append_entries_rpc{term = 1, leader_id = n1, prev_log_index = 1,
                               prev_log_term = 1, leader_commit = 3,
                               entries = [
                                          entry(2, 1, two),
                                          entry(3, 1, tre),
                                          entry(4, 1, for)
                                         ]},
    {follower, State4 = #{leader_id := n1, current_term := 1,
                          commit_index := 3, last_applied := 1}, _}
    = ra_node:handle_follower(AER3, State3),
    % {written, 4} -> last_applied: 3 - reply: last_index = 4, next_index = 5
    {follower, State5 = #{leader_id := n1, current_term := 1,
                          commit_index := 3, last_applied := 3,
                          machine_state := {_, _, tre}},
     [{cast, n1, {n2, #append_entries_reply{next_index = 5,
                                            success = true,
                                            last_term = 1,
                                            last_index = 4}}} | _]}
    = ra_node:handle_follower({ra_log_event, {written, {4, 4, 1}}}, State4),

    % AER with index [2,3,4], commit_index = 4
    % async failed AER reverted back leader's next_index for follower
    % however an updated commit index is now known
    AER4 = #append_entries_rpc{term = 1, leader_id = n1, prev_log_index = 1,
                               prev_log_term = 1, leader_commit = 4,
                               entries = [
                                          entry(2, 1, two),
                                          entry(3, 1, tre),
                                          entry(4, 1, for)
                                         ]},
    {follower, #{leader_id := n1, current_term := 1, commit_index := 4,
                 last_applied := 4, machine_state := {_, _, for}}, _}
    = ra_node:handle_follower(AER4, State5),
    % TODO: scenario where the batch is partially already seen and partiall not
    % + increments commit_index
    ok.

follower_aer_4(_Config) ->
    % Scenario 4 - commit index
    % AER with index [1,2,3,4], commit_index = 10 -> commit_index = 4, last_applied = 0
    % follower catching up scenario
    Init = empty_state(3, n2),
    AER1 = #append_entries_rpc{term = 1, leader_id = n1, prev_log_index = 0,
                               prev_log_term = 0, leader_commit = 10,
                               entries = [
                                          entry(1, 1, one),
                                          entry(2, 1, two),
                                          entry(3, 1, tre),
                                          entry(4, 1, for)
                                         ]},
    {follower, State1 = #{leader_id := n1, current_term := 1,
                          commit_index := 4, last_applied := 0},
     _} = ra_node:handle_follower(AER1, Init),
    % {written, 4} -> last_applied = 4, commit_index = 4
    {follower, _State2 = #{leader_id := n1, current_term := 1,
                           commit_index := 4, last_applied := 4,
                           machine_state := {_, _, for}},
     [{cast, n1, {n2, #append_entries_reply{next_index = 5,
                                            last_term = 1,
                                            last_index = 4}}}, _]}
        = ra_node:handle_follower({ra_log_event, {written, {4, 4, 1}}}, State1),
    % AER with index [5], commit_index = 10 -> last_applied = 4, commit_index = 5
    ok.

follower_aer_5(_Config) ->
    %% Scenario
    %% Leader with smaller log is elected and sends empty aer
    %% Follower should truncate it's log and reply with an appropriate
    %% next index
    Init = empty_state(3, n2),
    AER1 = #append_entries_rpc{term = 1, leader_id = n1, prev_log_index = 0,
                               prev_log_term = 0, leader_commit = 10,
                               entries = [
                                          entry(1, 1, one),
                                          entry(2, 1, two),
                                          entry(3, 1, tre),
                                          entry(4, 1, for)
                                         ]},
    %% set up follower state
    {follower, State00, _} = ra_node:handle_follower(AER1, Init),
    %% TODO also test when written even occurs after
    {follower, State0, _} = ra_node:handle_follower(
                              {ra_log_event, {written, {4, 4, 1}}}, State00),
    % now an AER from another leader in a higher term is received
    % This is what the leader sends immedately before committing it;s noop
    AER2 = #append_entries_rpc{term = 2, leader_id = n5, prev_log_index = 3,
                               prev_log_term = 1, leader_commit = 3,
                               entries = []},
    {follower, _State1, Effects} = ra_node:handle_follower(AER2, State0),
    {cast, n5, {_, M}} = hd(Effects),
    ?assertMatch(#append_entries_reply{next_index = 4,
                                       last_term = 1,
                                       last_index = 3}, M),
    % ct:pal("Effects ~p~n State: ~p", [Effects, State1]),
    ok.



follower_aer_term_mismatch(_Config) ->
    State = (base_state(3))#{commit_index => 2},
    AE = #append_entries_rpc{term = 6,
                             leader_id = n1,
                             prev_log_index = 3,
                             prev_log_term = 6, % higher log term
                             leader_commit = 3},

    % term mismatch scenario follower has index 3 but for different term
    % rewinds back to commit index + 1 as next index and entres await condition
    {await_condition, #{condition := _},
     [{_, _, {_, Reply}} | _]} = ra_node:handle_follower(AE, State),
    ?assertMatch(#append_entries_reply{term = 6,
                                       success = false,
                                       next_index = 3,
                                       last_index = 2,
                                       last_term = 3}, Reply),
                 ok.

follower_handles_append_entries_rpc(_Config) ->
    State = (base_state(3))#{commit_index => 1},
    EmptyAE = #append_entries_rpc{term = 5,
                                  leader_id = n1,
                                  prev_log_index = 3,
                                  prev_log_term = 5,
                                  leader_commit = 3},


    % success case - everything is up to date leader id got updated
    {follower, #{leader_id := n1, current_term := 5}, _} =
        ra_node:handle_follower(EmptyAE, State),

    % success case when leader term is higher
    % reply term should be updated
    {follower, #{leader_id := n1, current_term := 6},
     [{cast, n1, {n1, #append_entries_reply{term = 6, success = true,
                                            next_index = 4, last_index = 3,
                                            last_term = 5}}}, _Metrics]}
          = ra_node:handle_follower(EmptyAE#append_entries_rpc{term = 6}, State),

    % reply false if term < current_term (5.1)
    {follower, _, [{cast, n1, {n1, #append_entries_reply{term = 5, success = false}}}]}
        = ra_node:handle_follower(EmptyAE#append_entries_rpc{term = 4}, State),

    % reply false if log doesn't contain a term matching entry at prev_log_index
    {await_condition, _, [{cast, n1, {n1, #append_entries_reply{term = 5, success = false}}}]}
        = ra_node:handle_follower(EmptyAE#append_entries_rpc{prev_log_index = 4},
                                  State),
    % there is an entry but not with a macthing term
    {await_condition, _, [{cast, n1, {n1, #append_entries_reply{term = 5, success = false}}}]}
        = ra_node:handle_follower(EmptyAE#append_entries_rpc{prev_log_term = 4},
                                  State),

    % truncate/overwrite if a existing entry conflicts (diff term) with
    % a new one (5.3)
    AE = #append_entries_rpc{term = 5, leader_id = n1,
                             prev_log_index = 1, prev_log_term = 1,
                             leader_commit = 2,
                             entries = [{2, 4, {'$usr', meta(), <<"hi">>,
                                                after_log_append}}]},

    {follower,  #{log := Log},
     [{cast, n1, {n1, #append_entries_reply{term = 5, success = true,
                                            next_index = 3, last_index = 2,
                                            last_term = 4}}}, _Metric0]}
    = begin
          {follower, Inter3, _} =
              ra_node:handle_follower(AE, State#{last_applied => 1}),
          ra_node:handle_follower({ra_log_event, {written, {2, 2, 4}}}, Inter3)
      end,
    [{0, 0, undefined},
     {1, 1, _}, {2, 4, _}] = ra_log_memory:to_list(Log),

    % append new entries not in the log
    % if leader_commit > the last entry received ensure last_applied does not
    % match commit_index
    % ExpectedLogEntry = usr(<<"hi4">>),
    {follower, #{commit_index := 4, last_applied := 4,
                 machine_state := {_, _, <<"hi4">>}},
     [{cast, n1, {n1, #append_entries_reply{term = 5, success = true,
                                            last_index = 4,
                                            last_term = 5}}}, _Metrics1]}
    = begin
          {follower, Inter4, _}
        = ra_node:handle_follower(
            EmptyAE#append_entries_rpc{entries = [{4, 5, usr(<<"hi4">>)}],
                                       leader_commit = 5},
            State#{commit_index => 1, last_applied => 1,
                   machine_state => {simple, ?MACFUN, <<"hi1">>}}),
          ra_node:handle_follower(written_evt({4, 4, 5}), Inter4)
      end,
    ok.

follower_catchup_condition(_Config) ->
    State0 = (base_state(3))#{commit_index => 1},
    EmptyAE = #append_entries_rpc{term = 5,
                                  leader_id = n1,
                                  prev_log_index = 3,
                                  prev_log_term = 5,
                                  leader_commit = 3},

    % from follower to await condition
    {await_condition, State = #{condition := _}, [_AppendEntryReply]} =
        ra_node:handle_follower(EmptyAE#append_entries_rpc{term = 5,
                                                           prev_log_index = 4},
                                State0),

    % append entry with a lower leader term should not enter await condition
    % even if prev_log_index is higher than last index
    {follower, _, [_]}
    = ra_node:handle_follower(EmptyAE#append_entries_rpc{term = 4,
                                                         prev_log_index = 4},
                              State),

    % append entry when prev log index exists but the term is different should
    % not also enter await condition as it then rewinds and request resends
    % of all log entries since last known commit index
    {await_condition, _, [_]}
    = ra_node:handle_follower(EmptyAE#append_entries_rpc{term = 6,
                                                         prev_log_term = 4,
                                                         prev_log_index = 3},
                              State),

    % append entry when term is ok but there is a gap should remain in await
    % condition we do not want to send a reply here
    {await_condition, _, []} =
    ra_node:handle_await_condition(EmptyAE#append_entries_rpc{term = 5,
                                                              prev_log_index = 4},
                                   State),

    % success case - it transitions back to follower state
    {follower, _, [{next_event, cast, EmptyAE}]} =
        ra_node:handle_await_condition(EmptyAE, State),


    ISRpc = #install_snapshot_rpc{term = 99, leader_id = n1,
                                  last_index = 99, last_term = 99,
                                  last_config = [], data = []},
    {follower, State, [_NextEvent]} =
        ra_node:handle_await_condition(ISRpc, State),

    {await_condition, State, []} =
        ra_node:handle_await_condition({ra_log_event, {written, {99, 99, 99}}}, State),

    Msg = #request_vote_rpc{candidate_id = n2, term = 6, last_log_index = 3,
                            last_log_term = 5},
    {follower, State, [{next_event, cast, Msg}]} =
        ra_node:handle_await_condition(Msg, State),
    {follower, _, [{cast, n1, {n1, #append_entries_reply{success = false,
                                                         next_index = 4}}}]}
    = ra_node:handle_await_condition(await_condition_timeout, State),

    {pre_vote, _, _} = ra_node:handle_await_condition(election_timeout, State).


wal_down_condition(_Config) ->
    State0 = (base_state(3))#{commit_index => 1},
    EmptyAE = #append_entries_rpc{term = 5,
                                  leader_id = n1,
                                  prev_log_index = 3,
                                  prev_log_term = 5,
                                  leader_commit = 3},

    % meck:new(ra_log, [passthrough]),
    meck:expect(ra_log, write, fun (_Es, _L) -> {error, wal_down} end),
    meck:expect(ra_log, can_write, fun (_L) -> false end),

    % ra log fails
    {await_condition, State = #{condition := _}, []}
    = ra_node:handle_follower(EmptyAE#append_entries_rpc{entries = [{4, 5, yo}]}, State0),

    % stay in await condition as ra_log_wal is not available
    {await_condition, State = #{condition := _}, []}
    = ra_node:handle_await_condition(EmptyAE#append_entries_rpc{entries = [{4, 5, yo}]}, State),

    meck:expect(ra_log, can_write, fun (_L) -> true end),
    % exit condition
    {follower, _State, [_]}
    = ra_node:handle_await_condition(EmptyAE#append_entries_rpc{entries = [{4, 5, yo}]}, State),
    ok.

candidate_handles_append_entries_rpc(_Config) ->
    State = (base_state(3))#{commit_index => 1},
    EmptyAE = #append_entries_rpc{term = 4,
                                  leader_id = n1,
                                  prev_log_index = 3,
                                  prev_log_term = 5,
                                  leader_commit = 3},
    % Lower term
    {candidate, _,
     [{cast, n1, {n1, #append_entries_reply{term = 5, success = false,
                                            last_index = 3, last_term = 5}}}]}
    = ra_node:handle_candidate(EmptyAE, State),
    ok.

append_entries_reply_success(_Config) ->
    Cluster = #{n1 => new_peer_with(#{next_index => 5, match_index => 4}),
                n2 => new_peer_with(#{next_index => 1, match_index => 0,
                                      commit_index => 3}),
                n3 => new_peer_with(#{next_index => 2, match_index => 1})},
    State = (base_state(3))#{commit_index => 1,
                             last_applied => 1,
                             cluster => Cluster,
                             machine_state => {simple, ?MACFUN, <<"hi1">>}},
    Msg = {n2, #append_entries_reply{term = 5, success = true,
                                     next_index = 4,
                                     last_index = 3, last_term = 5}},
    % update match index
    {leader, #{cluster := #{n2 := #{next_index := 4, match_index := 3}},
               commit_index := 3,
               last_applied := 3,
               machine_state := {simple, _, <<"hi3">>}},
     [{send_rpcs,
         [{n3, #append_entries_rpc{term = 5, leader_id = n1,
                                   prev_log_index = 1,
                                   prev_log_term = 1,
                                   leader_commit = 3,
                                   entries = [{2, 3, {'$usr', _, <<"hi2">>, _}},
                                              {3, 5, {'$usr', _, <<"hi3">>, _}}]}}
         ]}, _Metrics]} = ra_node:handle_leader(Msg, State),

    Msg1 = {n2, #append_entries_reply{term = 7, success = true,
                                      next_index = 4,
                                      last_index = 3, last_term = 5}},
    {leader, #{cluster := #{n2 := #{next_index := 4,
                                             match_index := 3}},
               commit_index := 1,
               last_applied := 1,
               current_term := 7,
               machine_state := {_, _, <<"hi1">>}}, _} =
        ra_node:handle_leader(Msg1, State#{current_term := 7}),
    ok.

append_entries_reply_no_success(_Config) ->
    % decrement next_index for peer if success = false
    Cluster = #{n1 => #{},
                n2 => new_peer_with(#{next_index => 3, match_index => 0}),
                n3 => new_peer_with(#{next_index => 2, match_index => 1,
                                      commit_index => 1})},
    State = (base_state(3))#{commit_index => 1,
                             last_applied => 1,
                             cluster => Cluster,
                             machine_state => {simple, ?MACFUN, <<"hi1">>}},
    % n2 has only seen index 1
    Msg = {n2, #append_entries_reply{term = 5, success = false, next_index = 2,
                                     last_index = 1, last_term = 1}},
    % new peers state is updated
    {leader, #{cluster := #{n2 := #{next_index := 4, match_index := 1}},
               commit_index := 1,
               last_applied := 1,
               machine_state := {simple, _, <<"hi1">>}},
     [{send_rpcs,
       [{n3, #append_entries_rpc{term = 5, leader_id = n1,
                                 prev_log_index = 1,
                                 prev_log_term = 1,
                                 leader_commit = 1,
                                 entries = [{2, 3, {'$usr', _, <<"hi2">>, _}},
                                            {3, 5, {'$usr', _, <<"hi3">>, _}}]}},
        {n2, _}]}]} =
        ra_node:handle_leader(Msg, State),
    ok.

follower_request_vote(_Config) ->
    State = base_state(3),
    Msg = #request_vote_rpc{candidate_id = n2, term = 6, last_log_index = 3,
                            last_log_term = 5},
    % success
    {follower, #{voted_for := n2, current_term := 6} = State1,
     [{reply, #request_vote_result{term = 6, vote_granted = true}}]} =
    ra_node:handle_follower(Msg, State),

    % we can vote again for the same candidate and term
    {follower, #{voted_for := n2, current_term := 6},
     [{reply, #request_vote_result{term = 6, vote_granted = true}}]} =
    ra_node:handle_follower(Msg, State1),

    % but not for a different candidate
    {follower, #{voted_for := n2, current_term := 6},
     [{reply, #request_vote_result{term = 6, vote_granted = false}}]} =
    ra_node:handle_follower(Msg#request_vote_rpc{candidate_id = n3}, State1),

    % fail due to lower term
    {follower, #{current_term := 5},
     [{reply, #request_vote_result{term = 5, vote_granted = false}}]} =
    ra_node:handle_follower(Msg#request_vote_rpc{term = 4}, State),

     % Raft determines which of two logs is more up-to-date by comparing the
     % index and term of the last entries in the logs. If the logs have last
     % entries with different terms, then the log with the later term is more
     % up-to-date. If the logs end with the same term, then whichever log is
     % longer is more up-to-date. (§5.4.1)

     % reject when candidate last log entry has a lower term
     % still update current term if incoming is higher
    {follower, #{current_term := 6},
     [{reply, #request_vote_result{term = 6, vote_granted = false}}]} =
    ra_node:handle_follower(Msg#request_vote_rpc{last_log_term = 4},
                            State),

    % grant vote when candidate last log entry has same term but is longer
    {follower, #{current_term := 6},
     [{reply, #request_vote_result{term = 6, vote_granted = true}}]} =
    ra_node:handle_follower(Msg#request_vote_rpc{last_log_index = 4},
                            State),
     ok.

follower_pre_vote(_Config) ->
    State = base_state(3),
    Term = 5,
    Token = make_ref(),
    Msg = #pre_vote_rpc{candidate_id = n2, term = Term, last_log_index = 3,
                        token = Token, last_log_term = 5},
    % success
    {follower, #{current_term := Term},
     [{reply, #pre_vote_result{term = Term, token = Token,
                               vote_granted = true}}]} =
        ra_node:handle_follower(Msg, State),

    % disallow pre votes from higher protocol version
    {follower, #{current_term := Term},
     [{reply, #pre_vote_result{term = Term, token = Token,
                               vote_granted = false}}]} =
        ra_node:handle_follower(Msg#pre_vote_rpc{version = ?RA_PROTO_VERSION+1},
                                State),

    % but still allow from a lower protocol version
        %
    {follower, #{current_term := Term},
     [{reply, #pre_vote_result{term = Term, token = Token,
                               vote_granted = true}}]} =
        ra_node:handle_follower(Msg#pre_vote_rpc{version = ?RA_PROTO_VERSION - 1},
                                State),
    % fail due to lower term
    {follower, #{current_term := 5},
     [{reply, #pre_vote_result{term = 5, token = Token,
                               vote_granted = false}}]} =
    ra_node:handle_follower(Msg#pre_vote_rpc{term = 4}, State),

     % reject when candidate last log entry has a lower term
     % still update current term if incoming is higher
    {follower, #{current_term := 6},
     [{reply, #pre_vote_result{term = 6, token = Token,
                               vote_granted = false}}]} =
    ra_node:handle_follower(Msg#pre_vote_rpc{last_log_term = 4,
                                             term = 6},
                            State),

    % grant vote when candidate last log entry has same term but is longer
    {follower, #{current_term := 5},
     [{reply, #pre_vote_result{term = 5, token = Token,
                               vote_granted = true}}]} =
    ra_node:handle_follower(Msg#pre_vote_rpc{last_log_index = 4},
                            State),
     ok.

pre_vote_receives_pre_vote(_Config) ->
    State = base_state(3),
    Term = 5,
    Token = make_ref(),
    Msg = #pre_vote_rpc{candidate_id = n2, term = Term, last_log_index = 3,
                        token = Token, last_log_term = 5},
    % success - pre vote still returns other pre vote requests
    % else we could have a dead-lock with a two process cluster with
    % both peers in pre-vote state
    {pre_vote, #{current_term := Term},
     [{reply, #pre_vote_result{term = Term, token = Token,
                               vote_granted = true}}]} =
        ra_node:handle_pre_vote(Msg, State),
    ok.

request_vote_rpc_with_lower_term(_Config) ->
    State = (base_state(3))#{current_term => 6,
                             voted_for => n1},
    Msg = #request_vote_rpc{candidate_id = n2, term = 5, last_log_index = 3,
                            last_log_term = 5},
    % term is lower than candidate term
    {candidate, #{voted_for := n1, current_term := 6},
     [{reply, #request_vote_result{term = 6, vote_granted = false}}]} =
         ra_node:handle_candidate(Msg, State),
    % term is lower than candidate term
    {leader, #{current_term := 6},
     [{reply, #request_vote_result{term = 6, vote_granted = false}}]} =
         ra_node:handle_leader(Msg, State).

leader_does_not_abdicate_to_unknown_peer(_Config) ->
    State = base_state(3),
    Vote = #request_vote_rpc{candidate_id = uknown_peer, term = 6,
                             last_log_index = 3,
                             last_log_term = 5},
    {leader, State, []} = ra_node:handle_leader(Vote, State),

    AEReply = {unknown_peer, #append_entries_reply{term = 6, success = false,
                                                   next_index = 4,
                                                   last_index = 3,
                                                   last_term = 5}},
    {leader, State, []} = ra_node:handle_leader(AEReply , State),
    IRS = #install_snapshot_result{term = 6, last_index = 0, last_term = 0},
    {leader, State, []} = ra_node:handle_leader({unknown_peer, IRS}, State),
    ok.

higher_term_detected(_Config) ->
    % Any message received with a higher term should result in the
    % node reverting to follower state
    State = #{log := Log} = base_state(3),
    IncomingTerm = 6,
    AEReply = {n2, #append_entries_reply{term = IncomingTerm, success = false,
                                         next_index = 4,
                                         last_index = 3, last_term = 5}},
    Log1 = ra_log_memory:write_meta_f(current_term, IncomingTerm, Log),
    _ = ra_log_memory:write_meta_f(voted_for, undefined, Log1),
    {follower, #{current_term := IncomingTerm}, []} =
        ra_node:handle_leader(AEReply, State),
    {follower, #{current_term := IncomingTerm}, []} =
        ra_node:handle_follower(AEReply, State),
    {follower, #{current_term := IncomingTerm}, []}
        = ra_node:handle_candidate(AEReply, State),

    AERpc = #append_entries_rpc{term = IncomingTerm, leader_id = n3,
                                prev_log_index = 3, prev_log_term = 5,
                                leader_commit = 3},
    {follower, #{current_term := IncomingTerm},
     [{next_event, AERpc}]} = ra_node:handle_leader(AERpc, State),
    {follower, #{current_term := IncomingTerm},
     [{next_event, AERpc}]} = ra_node:handle_candidate(AERpc, State),
    % follower will handle this properly and is tested elsewhere

    Vote = #request_vote_rpc{candidate_id = n2, term = 6, last_log_index = 3,
                             last_log_term = 5},
    {follower, #{current_term := IncomingTerm,
                 log := ExpectLog},
     [{next_event, Vote}]} = ra_node:handle_leader(Vote, State),
    {follower, #{current_term := IncomingTerm,
                 log := ExpectLog},
     [{next_event, Vote}]} = ra_node:handle_candidate(Vote, State),

    IRS = #install_snapshot_result{term = IncomingTerm,
                                   last_term = 0,
                                   last_index = 0},
    {follower, #{current_term := IncomingTerm },
     _} = ra_node:handle_leader({n2, IRS}, State),
    ok.

consistent_query(_Config) ->
    Cluster = #{n1 => #{next_index => 5, match_index => 3},
                n2 => #{next_index => 4, match_index => 3},
                n3 => #{next_index => 4, match_index => 3}},
    State = (base_state(3))#{cluster => Cluster},
    {leader, State0, _} =
        ra_node:handle_leader({command, {'$ra_query', meta(),
                                         fun id/1, await_consensus}}, State),
    % ct:pal("next ~p", [Next]),
    {leader, State1, _} = ra_node:handle_leader({ra_log_event, {written, {4, 4, 5}}}, State0),
    AEReply = {n2, #append_entries_reply{term = 5, success = true,
                                         next_index = 5,
                                         last_index = 4, last_term = 5}},
    {leader, _State2, Effects} = ra_node:handle_leader(AEReply, State1),
    % ct:pal("Effects ~p", [Effects]),
    ?assert(lists:any(fun({reply, _, {machine_reply, {{4, 5}, <<"hi3">>}}}) ->
                              true;
                         (_) -> false
                      end, Effects)),
    ok.

leader_noop_operation_enables_cluster_change(_Config) ->
    State00 = (base_state(3))#{cluster_change_permitted => false},
    {leader, #{cluster_change_permitted := false} = State0, _Effects} =
        ra_node:handle_leader({command, noop}, State00),
    {leader, State, _} = ra_node:handle_leader({ra_log_event, {written, {4, 4, 5}}}, State0),
    AEReply = {n2, #append_entries_reply{term = 5, success = true,
                                         next_index = 5,
                                         last_index = 4, last_term = 5}},
    % noop consensus
    {leader, #{cluster_change_permitted := true}, _} =
        ra_node:handle_leader(AEReply, State),
    ok.

leader_node_join(_Config) ->
    OldCluster = #{n1 => new_peer_with(#{next_index => 4, match_index => 3}),
                   n2 => new_peer_with(#{next_index => 4, match_index => 3}),
                   n3 => new_peer_with(#{next_index => 4, match_index => 3})},
    State0 = (base_state(3))#{cluster => OldCluster},
    % raft nodes should switch to the new configuration after log append
    % and further cluster changes should be disallowed
    {leader, #{cluster := #{n1 := _, n2 := _, n3 := _, n4 := _},
               cluster_change_permitted := false} = _State1, Effects} =
        ra_node:handle_leader({command, {'$ra_join', meta(),
                                         n4, await_consensus}}, State0),
    % {leader, State, Effects} = ra_node:handle_leader({written, 4}, State1),
    [{send_rpcs,
      [{n4, #append_entries_rpc{entries =
                                [_, _, _, {4, 5, {'$ra_cluster_change', _,
                                                  #{n1 := _, n2 := _,
                                                    n3 := _, n4 := _},
                                                  await_consensus}}]}},
       {n3, #append_entries_rpc{entries =
                                [{4, 5, {'$ra_cluster_change', _,
                                         #{n1 := _, n2 := _, n3 := _, n4 := _},
                                         await_consensus}}],
                                term = 5, leader_id = n1,
                                prev_log_index = 3,
                                prev_log_term = 5,
                                leader_commit = 3}},
       {n2, #append_entries_rpc{entries =
                                [{4, 5, {'$ra_cluster_change', _,
                                         #{n1 := _, n2 := _, n3 := _, n4 := _},
                                         await_consensus}}],
                                term = 5, leader_id = n1,
                                prev_log_index = 3,
                                prev_log_term = 5,
                                leader_commit = 3}}]},
     _] = Effects,
    ok.

leader_node_leave(_Config) ->
    OldCluster = #{n1 => new_peer_with(#{next_index => 4, match_index => 3}),
                   n2 => new_peer_with(#{next_index => 4, match_index => 3}),
                   n3 => new_peer_with(#{next_index => 4, match_index => 3}),
                   n4 => new_peer_with(#{next_index => 1, match_index => 0})},
    State = (base_state(3))#{cluster => OldCluster},
    % raft nodes should switch to the new configuration after log append
    {leader, #{cluster := #{n1 := _, n2 := _, n3 := _}},
     [{send_rpcs, [N3, N2]} | _]} =
        ra_node:handle_leader({command, {'$ra_leave', meta(), n4, await_consensus}},
                              State),
    % the leaving node is no longer included
    {n3, #append_entries_rpc{term = 5, leader_id = n1,
                             prev_log_index = 3,
                             prev_log_term = 5,
                             leader_commit = 3,
                             entries = [{4, 5, {'$ra_cluster_change', _,
                                                #{n1 := _, n2 := _, n3 := _},
                                                await_consensus}}]}} = N3,
    {n2, #append_entries_rpc{term = 5, leader_id = n1,
                             prev_log_index = 3,
                             prev_log_term = 5,
                             leader_commit = 3,
                             entries = [{4, 5, {'$ra_cluster_change', _,
                                                #{n1 := _, n2 := _, n3 := _},
                                                await_consensus}}]}} = N2,
    ok.

leader_is_removed(_Config) ->
    OldCluster = #{n1 => #{next_index => 4, match_index => 3},
                   n2 => #{next_index => 4, match_index => 3},
                   n3 => #{next_index => 4, match_index => 3},
                   n4 => #{next_index => 1, match_index => 0}},
    State = (base_state(3))#{cluster => OldCluster},

    {leader, State1, _} =
        ra_node:handle_leader({command, {'$ra_leave', meta(), n1, await_consensus}},
                              State),
    {leader, State1b, _} =
        ra_node:handle_leader(written_evt({4, 4, 5}), State1),

    % replies coming in
    AEReply = #append_entries_reply{term = 5, success = true, next_index = 5,
                                    last_index = 4, last_term = 5},
    {leader, State2, _} = ra_node:handle_leader({n2, AEReply}, State1b),
    % after committing the new entry the leader steps down
    {stop, #{commit_index := 4}, _} = ra_node:handle_leader({n3, AEReply}, State2),
    ok.

follower_cluster_change(_Config) ->
    OldCluster = #{n1 => #{next_index => 4, match_index => 3},
                   n2 => #{next_index => 4, match_index => 3},
                   n3 => #{next_index => 4, match_index => 3}},
    State = (base_state(3))#{id => n2,
                             cluster => OldCluster},
    NewCluster = #{n1 => #{next_index => 4, match_index => 3},
                   n2 => #{next_index => 4, match_index => 3},
                   n3 => #{next_index => 4, match_index => 3},
                   n4 => #{next_index => 1}},
    JoinEntry = {4, 5, {'$ra_cluster_change', meta(), NewCluster, await_consensus}},
    AE = #append_entries_rpc{term = 5, leader_id = n1,
                             prev_log_index = 3,
                             prev_log_term = 5,
                             leader_commit = 3,
                             entries = [JoinEntry]},
    {follower, #{cluster := #{n1 := _, n2 := _,
                              n3 := _, n4 := _},
                 cluster_index_term := {4, 5}},
     [{cast, n1, {n2, #append_entries_reply{}}}, _Metrics]} =
        begin
            {follower, Int, _} = ra_node:handle_follower(AE, State),
            ra_node:handle_follower(written_evt({4, 4, 5}), Int)
        end,

    ok.

written_evt(E) ->
    {ra_log_event, {written, E}}.

leader_applies_new_cluster(_Config) ->
    OldCluster = #{n1 => #{next_index => 4, match_index => 3},
                   n2 => #{next_index => 4, match_index => 3},
                   n3 => #{next_index => 4, match_index => 3}},

    State = (base_state(3))#{id => n1, cluster => OldCluster},
    Command = {command, {'$ra_join', meta(), n4, await_consensus}},
    % cluster records index and term it was applied to determine whether it has
    % been applied
    {leader, #{cluster_index_term := {4, 5},
               cluster := #{n1 := _, n2 := _,
                            n3 := _, n4 := _} } = State1, _} =
        ra_node:handle_leader(Command, State),
    {leader, State1b, _} =
        ra_node:handle_leader(written_evt({4, 4, 5}), State1),

    Command2 = {command, {'$ra_join', meta(), n5, await_consensus}},
    % additional cluster change commands are not applied whilst
    % cluster change is being committed
    {leader, #{cluster_index_term := {4, 5},
               cluster := #{n1 := _, n2 := _,
                            n3 := _, n4 := _},
               pending_cluster_changes := [_]} = State2, _} =
        ra_node:handle_leader(Command2, State1b),


    % replies coming in
    AEReply = #append_entries_reply{term = 5, success = true,
                                    next_index = 5,
                                    last_index = 4, last_term = 5},
    % leader does not yet have consensus as will need at least 3 votes
    {leader, State3 = #{commit_index := 3,
                        cluster_index_term := {4, 5},
                        cluster := #{n2 := #{next_index := 5,
                                             match_index := 4}}},
     _} = ra_node:handle_leader({n2, AEReply}, State2#{votes => 1}),

    % leader has consensus
    {leader, _State4 = #{commit_index := 4,
                         cluster := #{n3 := #{next_index := 5,
                                              match_index := 4}}},
     Effects} = ra_node:handle_leader({n3, AEReply}, State3),

    % the pending cluster change can now be processed as the
    % next event
    ?assert(lists:any(fun({next_event, {call, _}, {command, _} = C2}) ->
                              C2 =:= Command2;
                         (_) -> false
                      end, Effects)),

    ok.

leader_appends_cluster_change_then_steps_before_applying_it(_Config) ->
    OldCluster = #{n1 => #{next_index => 4, match_index => 3},
                   n2 => #{next_index => 4, match_index => 3},
                   n3 => #{next_index => 4, match_index => 3}},

    State = (base_state(3))#{id => n1, cluster => OldCluster},
    Command = {command, {'$ra_join', meta(), n4, await_consensus}},
    % cluster records index and term it was applied to determine whether it has
    % been applied
    {leader, #{cluster_index_term := {4, 5},
               cluster := #{n1 := _, n2 := _,
                            n3 := _, n4 := _}} = State1, _} = ra_node:handle_leader(
                                                       Command, State),

    % leader has committed the entry but n2 and n3 have not yet seen it and
    % n2 has been elected leader and is replicating a different entry for
    % index 4 with a higher term
    AE = #append_entries_rpc{entries = [{4, 6, noop}],
                             leader_id = n2,
                             term = 6,
                             prev_log_index = 3,
                             prev_log_term = 5,
                             leader_commit = 3},
    {follower, #{cluster := Cluster}, _} = ra_node:handle_follower(AE, State1),
    % assert n1 has switched back to the old cluster config
    #{n1 := _, n2 := _, n3 := _} = Cluster,
    3 = maps:size(Cluster),
    ok.

is_new(_Config) ->
    Args = #{cluster_id => some_id,
             id => {ra, node()},
             uid => <<"ra">>,
             initial_nodes => [],
             log_init_args => #{data_dir => "", uid => <<>>},
             machine => {simple, fun erlang:'+'/2, 0}},
    {NewState, _} = ra_node:init(Args),
    {leader, State, _} = ra_node:handle_leader(usr_cmd(1), NewState),
    false = ra_node:is_new(State),
    {NewState, _} = ra_node:init(Args),
    true = ra_node:is_new(NewState),
    ok.


command(_Config) ->
    State = base_state(3),
    Meta = meta(),
    Cmd = {'$usr', Meta, <<"hi4">>, after_log_append},
    AE = #append_entries_rpc{entries = [{4, 5, Cmd}],
                             leader_id = n1,
                             term = 5,
                             prev_log_index = 3,
                             prev_log_term = 5,
                             leader_commit = 3
                            },
    From = maps:get(from, Meta),
    {leader, _, [{reply, From, {4, 5}},
                 {send_rpcs, [{n3, AE}, {n2, AE}]} |
                 _]} =
        ra_node:handle_leader({command, Cmd}, State),
    ok.

candidate_election(_Config) ->
    State = (base_state(5))#{current_term => 6, votes => 1},
    Reply = #request_vote_result{term = 6, vote_granted = true},
    {candidate, #{votes := 2} = State1, []}
        = ra_node:handle_candidate(Reply, State),
    % denied
    NegResult = #request_vote_result{term = 6, vote_granted = false},
    {candidate, #{votes := 2}, []}
        = ra_node:handle_candidate(NegResult, State1),
    % newer term should make candidate stand down
    HighTermResult = #request_vote_result{term = 7, vote_granted = false},
    {follower, #{current_term := 7}, []}
        = ra_node:handle_candidate(HighTermResult, State1),

    % quorum has been achieved - candidate becomes leader
    PeerState = new_peer_with(#{next_index => 3+1, % leaders last log index + 1
                                match_index => 0}), % initd to 0
    % when candidate becomes leader the next operation should be a noop
    % and all peers should be initialised with the appropriate state
    {leader, #{cluster := #{n2 := PeerState,
                            n3 := PeerState,
                            n4 := PeerState,
                            n5 := PeerState}},
     [{send_rpcs, _}, {next_event, cast, {command, noop}}]}
        = ra_node:handle_candidate(Reply, State1).

pre_vote_election(_Config) ->
    Token = make_ref(),
    State = (base_state(5))#{votes => 1,
                             pre_vote_token => Token},
    Reply = #pre_vote_result{term = 5, token = Token, vote_granted = true},
    {pre_vote, #{votes := 2} = State1, []}
        = ra_node:handle_pre_vote(Reply, State),

    %% different token is ignored
    {pre_vote, #{votes := 1}, []}
        = ra_node:handle_pre_vote(Reply#pre_vote_result{token = make_ref()},
                                  State),
    % denied
    NegResult = #pre_vote_result{term = 5, token = Token,
                                 vote_granted = false},
    {pre_vote, #{votes := 2}, []}
        = ra_node:handle_pre_vote(NegResult, State1),

    % newer term should make pre_vote revert to follower
    HighTermResult = #pre_vote_result{term = 6, token = Token,
                                      vote_granted = false},
    {follower, #{current_term := 6, votes := 0}, []}
        = ra_node:handle_pre_vote(HighTermResult, State1),

    % quorum has been achieved - pre_vote becomes candidate
    {candidate,
     #{current_term := 6}, _} = ra_node:handle_pre_vote(Reply, State1).

pre_vote_election_reverts(_Config) ->
    Token = make_ref(),
    State = (base_state(5))#{votes => 1,
                             pre_vote_token => Token},
    % request vote with higher term
    VoteRpc = #request_vote_rpc{term = 6, candidate_id = n2,
                                last_log_index = 3, last_log_term = 5},
    {follower, #{current_term := 6, votes := 0}, [{next_event, VoteRpc}]}
        = ra_node:handle_pre_vote(VoteRpc, State),
    %  append entries rpc with equal term
    AE = #append_entries_rpc{term = 5, leader_id = n2, prev_log_index = 3,
                             prev_log_term = 5, leader_commit = 3},
    {follower, #{current_term := 5, votes := 0}, [{next_event, AE}]}
        = ra_node:handle_pre_vote(AE, State),
    % and higher term
    AETerm6 = AE#append_entries_rpc{term = 6},
    {follower, #{current_term := 6, votes := 0}, [{next_event, AETerm6}]}
        = ra_node:handle_pre_vote(AETerm6, State),

    % install snapshot rpc
    ISR = #install_snapshot_rpc{term = 5, leader_id = n2,
                                last_index = 3, last_term = 5,
                                last_config = [], data = []},
    {follower, #{current_term := 5, votes := 0}, [{next_event, ISR}]}
        = ra_node:handle_pre_vote(ISR, State),
    ok.

leader_receives_pre_vote(_Config) ->
    % leader should reply immediately with append entries if it receives
    % a pre_vote
    Token = make_ref(),
    State = (base_state(5))#{votes => 1},
    PreVoteRpc = #pre_vote_rpc{term = 5, candidate_id = n1,
                               token = Token,
                               last_log_index = 3, last_log_term = 5},
    {leader, #{}, [{send_rpcs, _}]}
        = ra_node:handle_leader(PreVoteRpc, State),
    % leader abdicates for higher term
    {follower, #{current_term := 6}, _}
        = ra_node:handle_leader(PreVoteRpc#pre_vote_rpc{term = 6}, State),
    ok.

leader_receives_install_snapshot_rpc(_Config) ->
    % leader step down when receiving an install snapshot rpc with a higher
    % term
    State  = #{current_term := Term,
               last_applied := Idx} = (base_state(5))#{votes => 1},
    ISRpc = #install_snapshot_rpc{term = Term + 1, leader_id = n5,
                                  last_index = Idx, last_term = Term,
                                  last_config = [], data = []},
    {follower, #{}, [{next_event, ISRpc}]}
        = ra_node:handle_leader(ISRpc, State),
    % leader ignores lower term
    {leader, State, _}
        = ra_node:handle_leader(ISRpc#install_snapshot_rpc{term = Term - 1},
                                State),
    ok.

follower_installs_snapshot(_Config) ->
    #{n3 := {_, FState = #{cluster := Config}, _}}
    = init_nodes([n1, n2, n3], {module, ra_queue, #{}}),
    LastTerm = 1, % snapshot term
    Term = 2, % leader term
    Idx = 3,
    ISRpc = #install_snapshot_rpc{term = Term, leader_id = n1,
                                  last_index = Idx, last_term = LastTerm,
                                  last_config = maps:keys(Config), data = []},
    {follower, #{current_term := Term,
                 commit_index := Idx,
                 last_applied := Idx,
                 cluster := Config,
                 machine_state := [],
                 leader_id := n1},
     [{cast, _, {_, #install_snapshot_result{}}}]} =
    ra_node:handle_follower(ISRpc, FState),
    ok.

snapshotted_follower_received_append_entries(_Config) ->
    #{n3 := {_, FState0 = #{cluster := Config}, _}} =
        init_nodes([n1, n2, n3], {module, ra_queue, #{}}),
    LastTerm = 1, % snapshot term
    Term = 2, % leader term
    Idx = 3,
    ISRpc = #install_snapshot_rpc{term = Term, leader_id = n1,
                                  last_index = Idx, last_term = LastTerm,
                                  last_config = maps:keys(Config), data = []},
    {follower, FState1, _} = ra_node:handle_follower(ISRpc, FState0),

    Cmd = usr({enc, banana}),
    AER = #append_entries_rpc{entries = [{4, 2, Cmd}],
                              leader_id = n1,
                              term = Term,
                              prev_log_index = 3, % snapshot index
                              prev_log_term = 1,
                              leader_commit = 4 % entry is already committed
                             },
    {follower, _FState, [{cast, n1, {n3, #append_entries_reply{success = true}}},
                         _Metrics]} =
        begin
            {follower, Int, _} = ra_node:handle_follower(AER, FState1),
            ra_node:handle_follower(written_evt({4, 4, 2}), Int)
        end,
    ok.

leader_received_append_entries_reply_with_stale_last_index(_Config) ->
    Term = 2,
    N2NextIndex = 3,
    Log = lists:foldl(fun(E, L) ->
                              ra_log:append_sync(E, L)
                      end, ra_log:init(#{data_dir => "", uid => <<>>}),
                      [{1, 1, noop},
                       {2, 2, {'$usr', meta(), {enq, apple}, after_log_append}},
                       {3, 5, {2, {'$usr', meta(), {enq, pear}, after_log_append}}}]),
    Leader0 = #{cluster =>
                #{n1 => new_peer_with(#{match_index => 0}), % current leader in term 2
                  n2 => new_peer_with(#{match_index => 0,
                                        next_index => N2NextIndex}), % stale peer - previous leader
                  n3 => new_peer_with(#{match_index => 3,
                                        next_index => 4,
                                        commit_index => 3})}, % uptodate peer
                cluster_change_permitted => true,
                cluster_index_term => {0,0},
                commit_index => 3,
                current_term => Term,
                id => n1,
                last_applied => 4,
                log => Log,
                machine_state => [{4,apple}],
                pending_cluster_changes => []},
    AER = #append_entries_reply{success = false,
                                term = Term,
                                next_index = 3,
                                last_index = 2, % refer to stale entry
                                last_term = 1}, % in previous term
    % should decrement next_index for n2
    % ExpectedN2NextIndex = 2,
    {leader, #{cluster := #{n2 := #{next_index := 4}}},
     [{send_rpcs,
       [{n2, #append_entries_rpc{entries = [{2, _, _}, {3, _, _}]}}]}]}
       = ra_node:handle_leader({n2, AER}, Leader0),
    ok.


leader_receives_install_snapshot_result(_Config) ->
    % should update peer next_index
    Term = 1,
    Log0 = lists:foldl(fun(E, L) ->
                              ra_log:append_sync(E, L)
                      end, ra_log:init(#{data_dir => "", uid => <<>>}),
                      [{1, 1, noop}, {2, 1, noop},
                       {3, 1, {'$usr', meta(), {enq,apple}, after_log_append}},
                       {4, 1, {'$usr', meta(), {enq,pear}, after_log_append}}]),
    Log = ra_log:install_snapshot({2,1, [n1, n2, n3], []}, Log0),
    Leader = #{cluster =>
               #{n1 => new_peer_with(#{match_index => 0}),
                 n2 => new_peer_with(#{match_index => 4, next_index => 5,
                                       commit_index => 4}),
                 n3 => new_peer_with(#{match_index => 0, next_index => 1})},
               cluster_change_permitted => true,
               cluster_index_term => {0,0},
               commit_index => 4,
               current_term => Term,
               id => n1,
               uid => <<"n1">>,
               last_applied => 4,
               log => Log,
               machine_state => [{4,apple}],
               pending_cluster_changes => []},
    ISR = #install_snapshot_result{term = Term,
                                   last_index = 2,
                                   last_term = 1},
    {leader, #{cluster := #{n3 := #{match_index := 2,
                                    commit_index := 4,
                                    next_index := 5}}},
     [{send_rpcs, Rpcs}]} = ra_node:handle_leader({n3, ISR}, Leader),
    ?assert(lists:any(fun({n3,
                           #append_entries_rpc{entries = [{3, _, _},
                                                          {4, _, _}]}}) ->
                              true;
                         (_) -> false end, Rpcs)),
    ok.

% %%% helpers

init_nodes(NodeIds, Machine) ->
    lists:foldl(fun (NodeId, Acc) ->
                        Args = #{cluster_id => some_id,
                                 id => NodeId,
                                 uid => atom_to_binary(NodeId, utf8),
                                 initial_nodes => NodeIds,
                                 log_init_args => #{data_dir => "", uid => <<>>},
                                 machine => Machine},
                        Acc#{NodeId => {follower, ra_node_init(Args), []}}
                end, #{}, NodeIds).

list(L) when is_list(L) -> L;
list(L) -> [L].

entry(Idx, Term, Data) ->
    {Idx, Term, {'$usr', meta(), Data, after_log_append}}.

empty_state(NumNodes, Id) ->
    Nodes = lists:foldl(fun(N, Acc) ->
                                [list_to_atom("n" ++ integer_to_list(N)) | Acc]
                        end, [], lists:seq(1, NumNodes)),
    ra_node_init(#{cluster_id => someid,
                   id => Id,
                   uid => atom_to_binary(Id, utf8),
                   initial_nodes => Nodes,
                   log_init_args => #{data_dir => "", uid => <<>>},
                   machine => {simple, fun (E, _) -> E end, <<>>}}). % just keep last applied value

base_state(NumNodes) ->
    Log0 = lists:foldl(fun(E, L) ->
                              ra_log_memory:append(E, L)
                      end, ra_log_memory:init(#{data_dir => "", uid => <<>>}),
                      [{1, 1, usr(<<"hi1">>)},
                       {2, 3, usr(<<"hi2">>)},
                       {3, 5, usr(<<"hi3">>)}]),
    Log = ra_log_memory:handle_event({written, {1, 3, 5}}, Log0),

    Nodes = lists:foldl(fun(N, Acc) ->
                                Name = list_to_atom("n" ++ integer_to_list(N)),
                                Acc#{Name =>
                                     new_peer_with(#{next_index => 4,
                                                     match_index => 3})}
                        end, #{}, lists:seq(1, NumNodes)),
    MacFun = fun (E, _) -> E end,
    #{id => n1,
      uid => <<"n1">>,
      leader_id => n1,
      cluster => Nodes,
      cluster_index_term => {0, 0},
      cluster_change_permitted => true,
      pending_cluster_changes => [],
      current_term => 5,
      commit_index => 3,
      last_applied => 3,
      machine => {machine, ra_machine_simple,
                  #{simple_fun => MacFun,
                    initial_state => <<>>}}, % just keep last applied value
      machine_state => {simple, MacFun, <<"hi3">>}, % last entry has been applied
      log => Log}.

usr_cmd(Data) ->
    {command, usr(Data)}.

usr(Data) ->
    {'$usr', meta(), Data, after_log_append}.

meta() ->
    #{from => {self(), make_ref()}}.

dump(T) ->
    ct:pal("DUMP: ~p~n", [T]),
    T.

new_peer() ->
    #{next_index => 1,
      match_index => 0,
      commit_index => 0}.

new_peer_with(Map) ->
    maps:merge(new_peer(), Map).
