-module(ra_server_SUITE).

-compile(export_all).

-include("ra.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

all() ->
    [
     init_test,
     recover_restores_cluster_changes,
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
     update_release_cursor
    ].

-define(MACFUN, fun (E, _) -> E end).

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
    meck:expect(ra_log_meta, store, fun (U, K, V) ->
                                            put({U, K}, V), ok
                                    end),
    meck:expect(ra_log_meta, store_sync, fun (U, K, V) ->
                                                 put({U, K}, V), ok
                                         end),
    meck:expect(ra_log_meta, fetch, fun(U, K) ->
                                            get({U, K})
                                    end),
    meck:expect(ra_log_meta, fetch, fun (U, K, D) ->
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
    meck:expect(ra_log, install_snapshot, fun (_, _, Log) -> Log end),
    meck:expect(ra_log, recover_snapshot, fun ra_log_memory:recover_snapshot/1),
    meck:expect(ra_log, snapshot_index_term, fun ra_log_memory: snapshot_index_term/1),
    meck:expect(ra_log, take, fun ra_log_memory:take/3),
    meck:expect(ra_log, release_resources, fun ra_log_memory:release_resources/2),
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
    #{id := Id,
      uid := UId,
      cluster := Cluster,
      current_term := CurrentTerm,
      log := Log0} = base_state(3, ?FUNCTION_NAME),
    % ensure it is written to the log
    InitConf = #{cluster_name => init,
                 id => Id,
                 uid => UId,
                 log_init_args => #{data_dir => "", uid => <<>>},
                 machine => {module, ?FUNCTION_NAME, #{}},
                 initial_members => []}, % init without known peers
    % new
    #{current_term := 0,
      voted_for := undefined} = ra_server_init(InitConf),
    % previous data
    ok = ra_log_meta:store(UId, voted_for, some_server),
    ok = ra_log_meta:store(UId, current_term, CurrentTerm),
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
    InitConf = #{cluster_name => ?FUNCTION_NAME,
                 id => n1,
                 uid => <<"n1">>,
                 log_init_args => #{data_dir => "", uid => <<>>},
                 machine => {simple, fun erlang:'+'/2, 0},
                 initial_members => []}, % init without known peers
    % new
    {leader, State00, _} =
    ra_server:handle_candidate(#request_vote_result{term = 1,
                                                    vote_granted = true},
                               (ra_server_init(InitConf))#{votes => 0,
                                                           current_term => 1,
                                                           voted_for => n1}),
    {leader, State0 = #{cluster := Cluster0}, _} =
        ra_server:handle_leader({command, {noop, meta(), 0}}, State00),
    {leader, State, _} = ra_server:handle_leader(written_evt({1, 1, 1}), State0),
    ?assert(maps:size(Cluster0) =:= 1),

    % n2 joins
    {leader, #{cluster := Cluster,
               log := Log0}, _} =
        ra_server:handle_leader({command, {'$ra_join', meta(),
                                         n2, await_consensus}}, State),
    ?assert(maps:size(Cluster) =:= 2),
    % intercept ra_log:init call to simulate persisted log data
    % ok = meck:new(ra_log, [passthrough]),
    meck:expect(ra_log, init, fun (_) -> Log0 end),
    meck:expect(ra_log_meta, fetch, fun (_, last_applied, 0) ->
                                            element(1, ra_log:last_index_term(Log0));
                                        (_, _, Def) ->
                                            Def
                                    end),

    #{cluster := #{n1 := _, n2 := _}} = ra_server_init(InitConf),
    ok.

election_timeout(_Config) ->
    State = base_state(3, ?FUNCTION_NAME),
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
        ra_server:handle_follower(Msg, State),

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
        ra_server:handle_pre_vote(Msg, State),

    %% assert tokens are not the same
    ?assertNotEqual(Token, Token1),

    % candidate
    VoteRpc = #request_vote_rpc{term = 6, candidate_id = n1,
                                last_log_index = 3, last_log_term = 5},
    VoteForSelfEvent = {next_event, cast,
                        #request_vote_result{term = 6, vote_granted = true}},
    {candidate, #{current_term := 6, votes := 0},
     [VoteForSelfEvent, {send_vote_requests, [{n2, VoteRpc}, {n3, VoteRpc}]}]} =
        ra_server:handle_candidate(Msg, State),
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
    ra_server:handle_follower(AER1, Init),

    % AER with index [2], leader_commit = 1, commit_index = 1
    AER2 = #append_entries_rpc{term = 1, leader_id = n1, prev_log_index = 1,
                               prev_log_term = 1, leader_commit = 1,
                               entries = [entry(2, 1, two)]},
    {follower, State2 = #{leader_id := n1, current_term := 1,
                          commit_index := 1, last_applied := 0,
                          machine_state := {simple, _, <<>>}},
     _} = ra_server:handle_follower(AER2, State1),

    % {written, 1} -> last_applied: 1 - replies with last_index = 1, next_index = 3
    {follower, State3 = #{leader_id := n1, current_term := 1,
                          commit_index := 1, last_applied := 1,
                          machine_state := {simple, _, one}},
     [{cast, n1, {Self, #append_entries_reply{next_index = 3,
                                              last_term = 1,
                                              last_index = 1}}}, _]}
        = ra_server:handle_follower({ra_log_event, {written, {1, 1, 1}}}, State2),

    % AER with index [3], commit = 3 -> commit_index = 3
    AER3 = #append_entries_rpc{term = 1, leader_id = n1, prev_log_index = 2,
                               prev_log_term = 1, leader_commit = 3,
                               entries = [entry(3, 1, tre)]},
    {follower, State4 = #{leader_id := n1, current_term := 1,
                          commit_index := 3, last_applied := 1,
                          machine_state := {simple, _, one}},
     _} = ra_server:handle_follower(AER3, State3),

    % {written, 2} -> last_applied: 2, commit_index = 3 reply = 2, next_index = 4
    {follower, State5 = #{leader_id := n1, current_term := 1,
                          commit_index := 3, last_applied := 2,
                          machine_state := {_, _, two}},
     [{cast, n1, {Self, #append_entries_reply{next_index = 4,
                                              last_term = 1,
                                              last_index = 2}}}, _]}
        = ra_server:handle_follower({ra_log_event, {written, {2, 2, 1}}}, State4),

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
        = ra_server:handle_follower(AER4, State5),

    % {written, 3} -> commit_index = 3, last_applied = 3 : reply last_index = 3
    {follower, #{leader_id := n1, current_term := 1,
                 commit_index := 3, last_applied := 3,
                 machine_state := {_, _, tre}},
     [{cast, n1, {Self, #append_entries_reply{next_index = 4,
                                              last_term = 1,
                                              last_index = 3}}}, _]}
        = ra_server:handle_follower({ra_log_event, {written, {3, 3, 1}}}, State6),
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
     _} = ra_server:handle_follower(AER1, Init),

    % {written, 1} -> last_applied: 0, reply: last_applied = 1, next_index = 2
    {follower, State2 = #{leader_id := n1, current_term := 1,
                          commit_index := 0, last_applied := 0,
                          machine_state := {simple, _, <<>>}},
     [{cast, n1, {n2, #append_entries_reply{next_index = 2,
                                            last_term = 1,
                                            last_index = 1}}}, _]}
        = ra_server:handle_follower({ra_log_event, {written, {1, 1, 1}}}, State1),

    % AER with index [], leader_commit = 1 -> last_applied: 1, reply: last_index = 1, next_index = 2
    AER2 = #append_entries_rpc{term = 1, leader_id = n1, prev_log_index = 1,
                               prev_log_term = 1, leader_commit = 1,
                               entries = []},
    {follower, #{leader_id := n1, current_term := 1,
                 commit_index := 1, last_applied := 1,
                 machine_state := {simple, _, one}},
     _} = ra_server:handle_follower(AER2, State2),
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
     _} = ra_server:handle_follower(AER1, Init),
    % {written, 1} -> last_applied: 1 - reply: last_index = 1, next_index = 2
    {follower, State2 = #{leader_id := n1, current_term := 1,
                          commit_index := 1, last_applied := 1,
                          machine_state := {simple, _, one}},
     [{cast, n1, {n2, #append_entries_reply{next_index = 2,
                                            last_term = 1,
                                            last_index = 1}}}, _]}
        = ra_server:handle_follower({ra_log_event, {written, {1, 1, 1}}}, State1),
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
        = ra_server:handle_follower(AER2, State2),
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
    = ra_server:handle_follower(AER3, State3),
    % {written, 4} -> last_applied: 3 - reply: last_index = 4, next_index = 5
    {follower, State5 = #{leader_id := n1, current_term := 1,
                          commit_index := 3, last_applied := 3,
                          machine_state := {_, _, tre}},
     [{cast, n1, {n2, #append_entries_reply{next_index = 5,
                                            success = true,
                                            last_term = 1,
                                            last_index = 4}}} | _]}
    = ra_server:handle_follower({ra_log_event, {written, {4, 4, 1}}}, State4),

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
    = ra_server:handle_follower(AER4, State5),
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
     _} = ra_server:handle_follower(AER1, Init),
    % {written, 4} -> last_applied = 4, commit_index = 4
    {follower, _State2 = #{leader_id := n1, current_term := 1,
                           commit_index := 4, last_applied := 4,
                           machine_state := {_, _, for}},
     [{cast, n1, {n2, #append_entries_reply{next_index = 5,
                                            last_term = 1,
                                            last_index = 4}}}, _]}
        = ra_server:handle_follower({ra_log_event, {written, {4, 4, 1}}}, State1),
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
    {follower, State00, _} = ra_server:handle_follower(AER1, Init),
    %% TODO also test when written even occurs after
    {follower, State0, _} = ra_server:handle_follower(
                              {ra_log_event, {written, {4, 4, 1}}}, State00),
    % now an AER from another leader in a higher term is received
    % This is what the leader sends immedately before committing it;s noop
    AER2 = #append_entries_rpc{term = 2, leader_id = n5, prev_log_index = 3,
                               prev_log_term = 1, leader_commit = 3,
                               entries = []},
    {follower, _State1, Effects} = ra_server:handle_follower(AER2, State0),
    {cast, n5, {_, M}} = hd(Effects),
    ?assertMatch(#append_entries_reply{next_index = 4,
                                       last_term = 1,
                                       last_index = 3}, M),
    % ct:pal("Effects ~p~n State: ~p", [Effects, State1]),
    ok.



follower_aer_term_mismatch(_Config) ->
    State = (base_state(3, ?FUNCTION_NAME))#{commit_index => 2},
    AE = #append_entries_rpc{term = 6,
                             leader_id = n1,
                             prev_log_index = 3,
                             prev_log_term = 6, % higher log term
                             leader_commit = 3},

    % term mismatch scenario follower has index 3 but for different term
    % rewinds back to commit index + 1 as next index and entres await condition
    {await_condition, #{condition := _},
     [{_, _, {_, Reply}} | _]} = ra_server:handle_follower(AE, State),
    ?assertMatch(#append_entries_reply{term = 6,
                                       success = false,
                                       next_index = 3,
                                       last_index = 2,
                                       last_term = 3}, Reply),
                 ok.

follower_handles_append_entries_rpc(_Config) ->
    State = (base_state(3, ?FUNCTION_NAME))#{commit_index => 1},
    EmptyAE = #append_entries_rpc{term = 5,
                                  leader_id = n1,
                                  prev_log_index = 3,
                                  prev_log_term = 5,
                                  leader_commit = 3},


    % success case - everything is up to date leader id got updated
    {follower, #{leader_id := n1, current_term := 5}, _} =
        ra_server:handle_follower(EmptyAE, State),

    % success case when leader term is higher
    % reply term should be updated
    {follower, #{leader_id := n1, current_term := 6},
     [{cast, n1, {n1, #append_entries_reply{term = 6, success = true,
                                            next_index = 4, last_index = 3,
                                            last_term = 5}}}, _Metrics]}
          = ra_server:handle_follower(EmptyAE#append_entries_rpc{term = 6}, State),

    % reply false if term < current_term (5.1)
    {follower, _, [{cast, n1, {n1, #append_entries_reply{term = 5, success = false}}}]}
        = ra_server:handle_follower(EmptyAE#append_entries_rpc{term = 4}, State),

    % reply false if log doesn't contain a term matching entry at prev_log_index
    {await_condition, _, [{cast, n1, {n1, #append_entries_reply{term = 5, success = false}}}]}
        = ra_server:handle_follower(EmptyAE#append_entries_rpc{prev_log_index = 4},
                                  State),
    % there is an entry but not with a macthing term
    {await_condition, _, [{cast, n1, {n1, #append_entries_reply{term = 5, success = false}}}]}
        = ra_server:handle_follower(EmptyAE#append_entries_rpc{prev_log_term = 4},
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
              ra_server:handle_follower(AE, State#{last_applied => 1}),
          ra_server:handle_follower({ra_log_event, {written, {2, 2, 4}}}, Inter3)
      end,
    [{0, 0, undefined},
     {1, 1, _}, {2, 4, _}] = ra_log_memory:to_list(Log),

    % append new entries not in the log
    % if leader_commit > the last entry received ensure last_applied does not
    % match commit_index
    % ExpectedLogEntry = usr(<<"hi4">>),
    {follower, #{commit_index := 4, last_applied := 4,
                 machine_state := <<"hi4">>},
     [{cast, n1, {n1, #append_entries_reply{term = 5, success = true,
                                            last_index = 4,
                                            last_term = 5}}}, _Metrics1]}
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
    State0 = (base_state(3, ?FUNCTION_NAME))#{commit_index => 1},
    EmptyAE = #append_entries_rpc{term = 5,
                                  leader_id = n1,
                                  prev_log_index = 3,
                                  prev_log_term = 5,
                                  leader_commit = 3},

    % from follower to await condition
    {await_condition, State = #{condition := _}, [_AppendEntryReply]} =
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
    {await_condition, _, [_]}
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


    ISRpc = #install_snapshot_rpc{term = 99, leader_id = n1,
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

    Msg = #request_vote_rpc{candidate_id = n2, term = 6, last_log_index = 3,
                            last_log_term = 5},
    {follower, State, [{next_event, cast, Msg}]} =
        ra_server:handle_await_condition(Msg, State),
    {follower, _, [{cast, n1, {n1, #append_entries_reply{success = false,
                                                         next_index = 4}}}]}
    = ra_server:handle_await_condition(await_condition_timeout, State),

    {pre_vote, _, _} = ra_server:handle_await_condition(election_timeout, State).


wal_down_condition(_Config) ->
    State0 = (base_state(3, ?FUNCTION_NAME))#{commit_index => 1},
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
    State00 = (base_state(3, ?FUNCTION_NAME)),
    meck:expect(?FUNCTION_NAME, version, fun () -> 1 end),
    State0 = State00#{machine_versions => [{1, 1}, {0, 0}]},
    %% need to match on something for this macro
    ?assertMatch({#{machine_version := 0}, []},
                 ra_server:update_release_cursor(2, some_state, State0)),
    ok.

candidate_handles_append_entries_rpc(_Config) ->
    State = (base_state(3, ?FUNCTION_NAME))#{commit_index => 1},
    EmptyAE = #append_entries_rpc{term = 4,
                                  leader_id = n1,
                                  prev_log_index = 3,
                                  prev_log_term = 5,
                                  leader_commit = 3},
    % Lower term
    {candidate, _,
     [{cast, n1, {n1, #append_entries_reply{term = 5, success = false,
                                            last_index = 3, last_term = 5}}}]}
    = ra_server:handle_candidate(EmptyAE, State),
    ok.

append_entries_reply_success(_Config) ->
    Cluster = #{n1 => new_peer_with(#{next_index => 5, match_index => 4}),
                n2 => new_peer_with(#{next_index => 1, match_index => 0,
                                      commit_index_sent => 3}),
                n3 => new_peer_with(#{next_index => 2, match_index => 1})},
    State = (base_state(3, ?FUNCTION_NAME))#{commit_index => 1,
                             last_applied => 1,
                             cluster => Cluster,
                             machine_state => <<"hi1">>},
    Msg = {n2, #append_entries_reply{term = 5, success = true,
                                     next_index = 4,
                                     last_index = 3, last_term = 5}},
    % update match index
    {leader, #{cluster := #{n2 := #{next_index := 4, match_index := 3}},
               commit_index := 3,
               last_applied := 3,
               machine_state := <<"hi3">>},
     [{send_rpc, n3,
       #append_entries_rpc{term = 5, leader_id = n1,
                           prev_log_index = 1,
                           prev_log_term = 1,
                           leader_commit = 3,
                           entries = [{2, 3, {'$usr', _, <<"hi2">>, _}},
                                      {3, 5, {'$usr', _, <<"hi3">>, _}}]}
      }, _Metrics]} = ra_server:handle_leader(Msg, State),

    Msg1 = {n2, #append_entries_reply{term = 7, success = true,
                                      next_index = 4,
                                      last_index = 3, last_term = 5}},
    {leader, #{cluster := #{n2 := #{next_index := 4,
                                             match_index := 3}},
               commit_index := 1,
               last_applied := 1,
               current_term := 7,
               machine_state := <<"hi1">>}, _} =
        ra_server:handle_leader(Msg1, State#{current_term := 7}),
    ok.

append_entries_reply_no_success(_Config) ->
    % decrement next_index for peer if success = false
    Cluster = #{n1 => new_peer(),
                n2 => new_peer_with(#{next_index => 3, match_index => 0}),
                n3 => new_peer_with(#{next_index => 2, match_index => 1,
                                      commit_index_sent => 1})},
    State = (base_state(3, ?FUNCTION_NAME))#{commit_index => 1,
                             last_applied => 1,
                             cluster => Cluster,
                             machine_state => <<"hi1">>},
    % n2 has only seen index 1
    Msg = {n2, #append_entries_reply{term = 5, success = false, next_index = 2,
                                     last_index = 1, last_term = 1}},
    % new peers state is updated
    {leader, #{cluster := #{n2 := #{next_index := 4, match_index := 1}},
               commit_index := 1,
               last_applied := 1,
               machine_state := <<"hi1">>},
     [{send_rpc, n3,
       #append_entries_rpc{term = 5, leader_id = n1,
                           prev_log_index = 1,
                           prev_log_term = 1,
                           leader_commit = 1,
                           entries = [{2, 3, {'$usr', _, <<"hi2">>, _}},
                                      {3, 5, {'$usr', _, <<"hi3">>, _}}]}},
        {send_rpc, n2, _}
      ]} = ra_server:handle_leader(Msg, State),
    ok.

follower_request_vote(_Config) ->
    State = base_state(3, ?FUNCTION_NAME),
    Msg = #request_vote_rpc{candidate_id = n2, term = 6, last_log_index = 3,
                            last_log_term = 5},
    % success
    {follower, #{voted_for := n2, current_term := 6} = State1,
     [{reply, #request_vote_result{term = 6, vote_granted = true}}]} =
    ra_server:handle_follower(Msg, State),

    % we can vote again for the same candidate and term
    {follower, #{voted_for := n2, current_term := 6},
     [{reply, #request_vote_result{term = 6, vote_granted = true}}]} =
    ra_server:handle_follower(Msg, State1),

    % but not for a different candidate
    {follower, #{voted_for := n2, current_term := 6},
     [{reply, #request_vote_result{term = 6, vote_granted = false}}]} =
    ra_server:handle_follower(Msg#request_vote_rpc{candidate_id = n3}, State1),

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
    Msg = #pre_vote_rpc{candidate_id = n2, term = Term, last_log_index = 3,
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
       ra_server:handle_follower(Msg#pre_vote_rpc{version = ?RA_PROTO_VERSION+1},
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
                               vote_granted = false}}]} =
        ra_server:handle_follower(Msg#pre_vote_rpc{machine_version = 99},
                                  State),

    % disallow votes from a lower machine version
    {follower, _,
     [{reply, #pre_vote_result{term = Term, token = Token,
                               vote_granted = false}}]} =
    ra_server:handle_follower(Msg#pre_vote_rpc{machine_version = 1},
                              State#{machine_version => 2}),

    % allow votes for the same machine version
    {follower, _,
     [{reply, #pre_vote_result{term = Term, token = Token,
                               vote_granted = true}}]} =
    ra_server:handle_follower(Msg#pre_vote_rpc{machine_version = 2},
                              State#{machine_version => 2}),

    % fail due to lower term
    % return failure and immediately enter pre_vote phase as there are
    {follower, #{current_term := 5},
     [{reply, #pre_vote_result{term = 5, token = Token,
                               vote_granted = false}}]} =
    ra_server:handle_follower(Msg#pre_vote_rpc{term = 4}, State),

    % when candidate last log entry has a lower term
    % the current server is a better candidate and thus
    % immedately enters pre_vote state
    {pre_vote, #{current_term := 6},
     [{next_event, cast, #pre_vote_result{term = 6, token = _,
                                          vote_granted = true}},
      {send_vote_requests, _}]} =
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
    Msg = #pre_vote_rpc{candidate_id = n2, term = Term, last_log_index = 3,
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
    State = (base_state(3, ?FUNCTION_NAME))#{current_term => 6,
                             voted_for => n1},
    Msg = #request_vote_rpc{candidate_id = n2, term = 5, last_log_index = 3,
                            last_log_term = 5},
    % term is lower than candidate term
    {candidate, #{voted_for := n1, current_term := 6},
     [{reply, #request_vote_result{term = 6, vote_granted = false}}]} =
         ra_server:handle_candidate(Msg, State),
    % term is lower than candidate term
    {leader, #{current_term := 6},
     [{reply, #request_vote_result{term = 6, vote_granted = false}}]} =
         ra_server:handle_leader(Msg, State).

leader_does_not_abdicate_to_unknown_peer(_Config) ->
    State = base_state(3, ?FUNCTION_NAME),
    Vote = #request_vote_rpc{candidate_id = uknown_peer, term = 6,
                             last_log_index = 3,
                             last_log_term = 5},
    {leader, State, []} = ra_server:handle_leader(Vote, State),

    AEReply = {unknown_peer, #append_entries_reply{term = 6, success = false,
                                                   next_index = 4,
                                                   last_index = 3,
                                                   last_term = 5}},
    {leader, State, []} = ra_server:handle_leader(AEReply , State),
    IRS = #install_snapshot_result{term = 6, last_index = 0, last_term = 0},
    {leader, State, []} = ra_server:handle_leader({unknown_peer, IRS}, State),
    ok.


leader_replies_to_append_entries_rpc_with_lower_term(_Config) ->
    State = #{id := Id,
              current_term := CTerm} = base_state(3, ?FUNCTION_NAME),
    AERpc = #append_entries_rpc{term = CTerm - 1,
                                leader_id = n3,
                                prev_log_index = 3,
                                prev_log_term = 5,
                                leader_commit = 3},
    ?assertMatch(
       {leader, _,
        [{cast, n3,
          {Id, #append_entries_reply{term = CTerm, success = false}}}]},
       ra_server:handle_leader(AERpc, State)),

    ok.

higher_term_detected(_Config) ->
    % Any message received with a higher term should result in the
    % server reverting to follower state
    State = #{log := Log} = base_state(3, ?FUNCTION_NAME),
    IncomingTerm = 6,
    AEReply = {n2, #append_entries_reply{term = IncomingTerm, success = false,
                                         next_index = 4,
                                         last_index = 3, last_term = 5}},
    Log1 = ra_log_memory:write_meta_f(current_term, IncomingTerm, Log),
    _ = ra_log_memory:write_meta_f(voted_for, undefined, Log1),
    {follower, #{current_term := IncomingTerm}, []} =
        ra_server:handle_leader(AEReply, State),
    {follower, #{current_term := IncomingTerm}, []} =
        ra_server:handle_follower(AEReply, State),
    {follower, #{current_term := IncomingTerm}, []}
        = ra_server:handle_candidate(AEReply, State),

    AERpc = #append_entries_rpc{term = IncomingTerm, leader_id = n3,
                                prev_log_index = 3, prev_log_term = 5,
                                leader_commit = 3},
    {follower, #{current_term := IncomingTerm},
     [{next_event, AERpc}]} = ra_server:handle_leader(AERpc, State),
    {follower, #{current_term := IncomingTerm},
     [{next_event, AERpc}]} = ra_server:handle_candidate(AERpc, State),
    % follower will handle this properly and is tested elsewhere

    Vote = #request_vote_rpc{candidate_id = n2, term = 6, last_log_index = 3,
                             last_log_term = 5},
    {follower, #{current_term := IncomingTerm,
                 log := ExpectLog},
     [{next_event, Vote}]} = ra_server:handle_leader(Vote, State),
    {follower, #{current_term := IncomingTerm,
                 log := ExpectLog},
     [{next_event, Vote}]} = ra_server:handle_candidate(Vote, State),

    IRS = #install_snapshot_result{term = IncomingTerm,
                                   last_term = 0,
                                   last_index = 0},
    {follower, #{current_term := IncomingTerm },
     _} = ra_server:handle_leader({n2, IRS}, State),
    ok.

consistent_query(_Config) ->
    Cluster = #{n1 => new_peer_with(#{next_index => 5, match_index => 3}),
                n2 => new_peer_with(#{next_index => 4, match_index => 3}),
                n3 => new_peer_with(#{next_index => 4, match_index => 3})},
    State = (base_state(3, ?FUNCTION_NAME))#{cluster => Cluster},
    {leader, State0, _} =
        ra_server:handle_leader({command, {'$ra_query', meta(),
                                         fun id/1, await_consensus}}, State),
    % ct:pal("next ~p", [Next]),
    {leader, State1, _} = ra_server:handle_leader({ra_log_event, {written, {4, 4, 5}}}, State0),
    AEReply = {n2, #append_entries_reply{term = 5, success = true,
                                         next_index = 5,
                                         last_index = 4, last_term = 5}},
    {leader, _State2, Effects} = ra_server:handle_leader(AEReply, State1),
    % ct:pal("Effects ~p", [Effects]),
    ?assert(lists:any(fun({reply, _, {wrap_reply, <<"hi3">>}}) ->
                              true;
                         (_) -> false
                      end, Effects)),
    ok.

leader_noop_operation_enables_cluster_change(_Config) ->
    State00 = (base_state(3, ?FUNCTION_NAME))#{cluster_change_permitted => false},
    {leader, #{cluster_change_permitted := false} = State0, _Effects} =
        ra_server:handle_leader({command, {noop, meta(), 0}}, State00),
    {leader, State, _} = ra_server:handle_leader({ra_log_event, {written, {4, 4, 5}}}, State0),
    AEReply = {n2, #append_entries_reply{term = 5, success = true,
                                         next_index = 5,
                                         last_index = 4, last_term = 5}},
    % noop consensus
    {leader, #{cluster_change_permitted := true}, _} =
        ra_server:handle_leader(AEReply, State),
    ok.

leader_noop_increments_machine_version(_Config) ->
    Mod = ?FUNCTION_NAME,
    MacVer = 2,
    OldMacVer = 1,
    State00 = (base_state(3, ?FUNCTION_NAME))#{cluster_change_permitted => false,
                                               machine_version => MacVer,
                                               effective_machine_version => OldMacVer},
    ModV2 = leader_noop_increments_machine_version_v2,
    meck:new(ModV2, [non_strict]),
    meck:expect(Mod, version, fun () -> MacVer end),
    meck:expect(Mod, which_module, fun (1) -> Mod;
                                       (2) -> ModV2
                                   end),
    {leader, #{effective_machine_version := OldMacVer,
               effective_machine_module := Mod} = State0, _Effects} =
        ra_server:handle_leader({command, {noop, meta(), MacVer}},
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
    AEReply = {n2, #append_entries_reply{term = 5, success = true,
                                         next_index = 5,
                                         last_index = 4, last_term = 5}},
    % noop consensus
    {leader, #{effective_machine_version := MacVer,
               effective_machine_module := ModV2}, _} =
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
                              term = 5, leader_id = n1,
                              prev_log_index = 3,
                              prev_log_term = 5,
                              leader_commit = 5},
    {follower, #{machine_version := 0,
                 effective_machine_version := 0,
                 last_applied := 3,
                 commit_index := 5} = State0, _} =
        ra_server:handle_follower(Aer, State00),
    %% new effective machine version is detected that is lower than available
    %% machine version
    %% last_applied is not updated we simply "peek" at the noop command to
    %% learn the next machine version to update it
    {follower, #{machine_version := 0,
                 effective_machine_version := 1,
                 last_applied := 3,
                 commit_index := 5,
                 log := _Log} = _State1, _Effects} =
    ra_server:handle_follower({ra_log_event, {written, {4, 5, 5}}}, State0),

    %% TODO: validate append entries reply effect

    %% TODO: simulate that follower is updated from this state
    %% ra_server_init
    ok.

leader_server_join(_Config) ->
    OldCluster = #{n1 => new_peer_with(#{next_index => 4, match_index => 3}),
                   n2 => new_peer_with(#{next_index => 4, match_index => 3}),
                   n3 => new_peer_with(#{next_index => 4, match_index => 3})},
    State0 = (base_state(3, ?FUNCTION_NAME))#{cluster => OldCluster},
    % raft servers should switch to the new configuration after log append
    % and further cluster changes should be disallowed
    {leader, #{cluster := #{n1 := _, n2 := _, n3 := _, n4 := _},
               cluster_change_permitted := false} = _State1, Effects} =
        ra_server:handle_leader({command, {'$ra_join', meta(),
                                         n4, await_consensus}}, State0),
    % {leader, State, Effects} = ra_server:handle_leader({written, 4}, State1),
    [{incr_metrics, _, _},
     {send_rpc, n4,
      #append_entries_rpc{entries =
                          [_, _, _, {4, 5, {'$ra_cluster_change', _,
                                            #{n1 := _, n2 := _,
                                              n3 := _, n4 := _},
                                            await_consensus}}]}},
     {send_rpc, n3,
      #append_entries_rpc{entries =
                          [{4, 5, {'$ra_cluster_change', _,
                                   #{n1 := _, n2 := _, n3 := _, n4 := _},
                                   await_consensus}}],
                          term = 5, leader_id = n1,
                          prev_log_index = 3,
                          prev_log_term = 5,
                          leader_commit = 3}},
     {send_rpc, n2,
      #append_entries_rpc{entries =
                          [{4, 5, {'$ra_cluster_change', _,
                                   #{n1 := _, n2 := _, n3 := _, n4 := _},
                                   await_consensus}}],
                          term = 5, leader_id = n1,
                          prev_log_index = 3,
                          prev_log_term = 5,
                          leader_commit = 3}}
     | _] = Effects,
    ok.

leader_server_leave(_Config) ->
    OldCluster = #{n1 => new_peer_with(#{next_index => 4, match_index => 3}),
                   n2 => new_peer_with(#{next_index => 4, match_index => 3}),
                   n3 => new_peer_with(#{next_index => 4, match_index => 3}),
                   n4 => new_peer_with(#{next_index => 1, match_index => 0})},
    State = (base_state(3, ?FUNCTION_NAME))#{cluster => OldCluster},
    % raft servers should switch to the new configuration after log append
    {leader, #{cluster := #{n1 := _, n2 := _, n3 := _}},
     [_, {send_rpc, n3, N3}, {send_rpc, n2, N2} | _]} =
        ra_server:handle_leader({command, {'$ra_leave', meta(), n4, await_consensus}},
                              State),
    % the leaving server is no longer included
    #append_entries_rpc{term = 5, leader_id = n1,
                        prev_log_index = 3,
                        prev_log_term = 5,
                        leader_commit = 3,
                        entries = [{4, 5, {'$ra_cluster_change', _,
                                           #{n1 := _, n2 := _, n3 := _},
                                           await_consensus}}]} = N3,
    #append_entries_rpc{term = 5, leader_id = n1,
                        prev_log_index = 3,
                        prev_log_term = 5,
                        leader_commit = 3,
                        entries = [{4, 5, {'$ra_cluster_change', _,
                                           #{n1 := _, n2 := _, n3 := _},
                                           await_consensus}}]} = N2,
    ok.

leader_is_removed(_Config) ->
    OldCluster = #{n1 => new_peer_with(#{next_index => 4, match_index => 3}),
                   n2 => new_peer_with(#{next_index => 4, match_index => 3}),
                   n3 => new_peer_with(#{next_index => 4, match_index => 3}),
                   n4 => new_peer_with(#{next_index => 1, match_index => 0})},
    State = (base_state(3, ?FUNCTION_NAME))#{cluster => OldCluster},

    {leader, State1, _} =
        ra_server:handle_leader({command, {'$ra_leave', meta(), n1, await_consensus}},
                              State),
    {leader, State1b, _} =
        ra_server:handle_leader(written_evt({4, 4, 5}), State1),

    % replies coming in
    AEReply = #append_entries_reply{term = 5, success = true, next_index = 5,
                                    last_index = 4, last_term = 5},
    {leader, State2, _} = ra_server:handle_leader({n2, AEReply}, State1b),
    % after committing the new entry the leader steps down
    {stop, #{commit_index := 4}, _} = ra_server:handle_leader({n3, AEReply}, State2),
    ok.

follower_cluster_change(_Config) ->
    OldCluster = #{n1 => new_peer_with(#{next_index => 4, match_index => 3}),
                   n2 => new_peer_with(#{next_index => 4, match_index => 3}),
                   n3 => new_peer_with(#{next_index => 4, match_index => 3})},
    State = (base_state(3, ?FUNCTION_NAME))#{id => n2,
                             cluster => OldCluster},
    NewCluster = #{n1 => new_peer_with(#{next_index => 4, match_index => 3}),
                   n2 => new_peer_with(#{next_index => 4, match_index => 3}),
                   n3 => new_peer_with(#{next_index => 4, match_index => 3}),
                   n4 => new_peer_with(#{next_index => 1})},
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
            {follower, Int, _} = ra_server:handle_follower(AE, State),
            ra_server:handle_follower(written_evt({4, 4, 5}), Int)
        end,

    ok.

written_evt(E) ->
    {ra_log_event, {written, E}}.

leader_applies_new_cluster(_Config) ->
    OldCluster = #{n1 => new_peer_with(#{next_index => 4, match_index => 3}),
                   n2 => new_peer_with(#{next_index => 4, match_index => 3}),
                   n3 => new_peer_with(#{next_index => 4, match_index => 3})},

    State = (base_state(3, ?FUNCTION_NAME))#{id => n1, cluster => OldCluster},
    Command = {command, {'$ra_join', meta(), n4, await_consensus}},
    % cluster records index and term it was applied to determine whether it has
    % been applied
    {leader, #{cluster_index_term := {4, 5},
               cluster := #{n1 := _, n2 := _,
                            n3 := _, n4 := _} } = State1, _} =
        ra_server:handle_leader(Command, State),
    {leader, State1b, _} =
        ra_server:handle_leader(written_evt({4, 4, 5}), State1),

    Command2 = {command, {'$ra_join', meta(), n5, await_consensus}},
    % additional cluster change commands are not applied whilst
    % cluster change is being committed
    {leader, #{cluster_index_term := {4, 5},
               cluster := #{n1 := _, n2 := _,
                            n3 := _, n4 := _},
               pending_cluster_changes := [_]} = State2, _} =
        ra_server:handle_leader(Command2, State1b),


    % replies coming in
    AEReply = #append_entries_reply{term = 5, success = true,
                                    next_index = 5,
                                    last_index = 4, last_term = 5},
    % leader does not yet have consensus as will need at least 3 votes
    {leader, State3 = #{commit_index := 3,
                        cluster_index_term := {4, 5},
                        cluster := #{n2 := #{next_index := 5,
                                             match_index := 4}}},
     _} = ra_server:handle_leader({n2, AEReply}, State2#{votes => 1}),

    % leader has consensus
    {leader, _State4 = #{commit_index := 4,
                         cluster := #{n3 := #{next_index := 5,
                                              match_index := 4}}},
     Effects} = ra_server:handle_leader({n3, AEReply}, State3),

    % the pending cluster change can now be processed as the
    % next event
    ?assert(lists:any(fun({next_event, {call, _}, {command, _} = C2}) ->
                              C2 =:= Command2;
                         (_) -> false
                      end, Effects)),

    ok.

leader_appends_cluster_change_then_steps_before_applying_it(_Config) ->
    OldCluster = #{n1 => new_peer_with(#{next_index => 4, match_index => 3}),
                   n2 => new_peer_with(#{next_index => 4, match_index => 3}),
                   n3 => new_peer_with(#{next_index => 4, match_index => 3})},

    State = (base_state(3, ?FUNCTION_NAME))#{id => n1, cluster => OldCluster},
    Command = {command, {'$ra_join', meta(), n4, await_consensus}},
    % cluster records index and term it was applied to determine whether it has
    % been applied
    {leader, #{cluster_index_term := {4, 5},
               cluster := #{n1 := _, n2 := _,
                            n3 := _, n4 := _}} = State1, _} =
    ra_server:handle_leader(Command, State),

    % leader has committed the entry but n2 and n3 have not yet seen it and
    % n2 has been elected leader and is replicating a different entry for
    % index 4 with a higher term
    AE = #append_entries_rpc{entries = [{4, 6, {noop, meta(), 1}}],
                             leader_id = n2,
                             term = 6,
                             prev_log_index = 3,
                             prev_log_term = 5,
                             leader_commit = 3},
    {follower, #{cluster := Cluster}, _} =
        ra_server:handle_follower(AE, State1),
    % assert n1 has switched back to the old cluster config
    #{n1 := _, n2 := _, n3 := _} = Cluster,
    3 = maps:size(Cluster),
    ok.

is_new(_Config) ->
    Id = some_id,
    Args = #{cluster_name => Id,
             id => {ra, node()},
             uid => <<"ra">>,
             initial_members => [],
             log_init_args => #{data_dir => "", uid => <<>>},
             machine => {simple, fun erlang:'+'/2, 0}},
    NewState = ra_server:init(Args),
    {leader, State, _} = ra_server:handle_leader(usr_cmd(1), NewState),
    false = ra_server:is_new(State),
    NewState = ra_server:init(Args),
    true = ra_server:is_new(NewState),
    ok.

command(_Config) ->
    State = base_state(3, ?FUNCTION_NAME),
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
    {leader, _, [{reply, From, {wrap_reply, {4, 5}}},
                 _,  % metrics
                 {send_rpc, n3, AE}, {send_rpc, n2, AE} |
                 _]} =
        ra_server:handle_leader({command, Cmd}, State),
    ok.

candidate_election(_Config) ->
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
    {leader, #{cluster := #{n2 := PeerState,
                            n3 := PeerState,
                            n4 := PeerState,
                            n5 := PeerState}},
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
    Token = make_ref(),
    State = (base_state(5, ?FUNCTION_NAME))#{votes => 1,
                             pre_vote_token => Token},
    % request vote with higher term
    VoteRpc = #request_vote_rpc{term = 6, candidate_id = n2,
                                last_log_index = 3, last_log_term = 5},
    {follower, #{current_term := 6, votes := 0}, [{next_event, VoteRpc}]}
        = ra_server:handle_pre_vote(VoteRpc, State),
    %  append entries rpc with equal term
    AE = #append_entries_rpc{term = 5, leader_id = n2, prev_log_index = 3,
                             prev_log_term = 5, leader_commit = 3},
    {follower, #{current_term := 5, votes := 0}, [{next_event, AE}]}
        = ra_server:handle_pre_vote(AE, State),
    % and higher term
    AETerm6 = AE#append_entries_rpc{term = 6},
    {follower, #{current_term := 6, votes := 0}, [{next_event, AETerm6}]}
        = ra_server:handle_pre_vote(AETerm6, State),

    % install snapshot rpc
    ISR = #install_snapshot_rpc{term = 5, leader_id = n2,
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
    PreVoteRpc = #pre_vote_rpc{term = 5, candidate_id = n1,
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
    ISRpc = #install_snapshot_rpc{term = Term + 1, leader_id = n5,
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
    #{n3 := {_, FState = #{cluster := Config}, _}}
    = init_servers([n1, n2, n3], {module, ra_queue, #{}}),
    LastTerm = 1, % snapshot term
    Term = 2, % leader term
    Idx = 3,
    ISRpc = #install_snapshot_rpc{term = Term, leader_id = n1,
                                  meta = snap_meta(Idx, LastTerm,
                                                   maps:keys(Config)),
                                  chunk_state = {1, last},
                                  data = []},
    {receive_snapshot, FState1,
     [{next_event, ISRpc}]} =
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
                 leader_id := n1},
     [{reply, #install_snapshot_result{}}]} =
        ra_server:handle_receive_snapshot(ISRpc, FState1),

    ok.

follower_receives_stale_snapshot(_Config) ->
    #{n3 := {_, FState0 = #{cluster := Config,
                            current_term := CurTerm}, _}}
    = init_servers([n1, n2, n3], {module, ra_queue, #{}}),
    FState = FState0#{last_applied => 3},
    LastTerm = 1, % snapshot term
    Idx = 2,
    ISRpc = #install_snapshot_rpc{term = CurTerm, leader_id = n1,
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
    #{n3 := {_, FState0 = #{cluster := Config,
                            current_term := CurTerm}, _}}
    = init_servers([n1, n2, n3], {module, ra_queue, #{}}),
    FState = FState0#{last_applied => 3},
    LastTerm = 1, % snapshot term
    Idx = 6,
    ISRpc = #install_snapshot_rpc{term = CurTerm, leader_id = n1,
                                  meta = snap_meta(Idx, LastTerm,
                                                   maps:keys(Config)),
                                  chunk_state = {1, last},
                                  data = []},
    {receive_snapshot, FState1,
     [{next_event, ISRpc}]} =
        ra_server:handle_follower(ISRpc, FState),

    %% revert back to follower on timeout
    {follower, #{log := Log}, _}
    = ra_server:handle_receive_snapshot(receive_snapshot_timeout, FState1),
    %% snapshot should be aborted
    SS = ra_log:snapshot_state(Log),
    undefined = ra_snapshot:accepting(SS),
    ok.

snapshotted_follower_received_append_entries(_Config) ->
    #{n3 := {_, FState0 = #{cluster := Config}, _}} =
        init_servers([n1, n2, n3], {module, ra_queue, #{}}),
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
    ISRpc = #install_snapshot_rpc{term = Term, leader_id = n1,
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
                              leader_id = n1,
                              term = Term,
                              prev_log_index = 3, % snapshot index
                              prev_log_term = 1,
                              leader_commit = 4 % entry is already committed
                             },
    {follower, _FState, [{cast, n1, {n3, #append_entries_reply{success = true}}},
                         _Metrics]} =
        begin
            {follower, Int, _} = ra_server:handle_follower(AER, FState1),
            ra_server:handle_follower(written_evt({4, 4, 2}), Int)
        end,
    ok.

leader_received_append_entries_reply_with_stale_last_index(_Config) ->
    Term = 2,
    N2NextIndex = 3,
    Log = lists:foldl(fun(E, L) ->
                              ra_log:append_sync(E, L)
                      end, ra_log:init(#{data_dir => "", uid => <<>>}),
                      [{1, 1, {noop, meta(), 1}},
                       {2, 2, {'$usr', meta(), {enq, apple}, after_log_append}},
                       {3, 5, {2, {'$usr', meta(), {enq, pear}, after_log_append}}}]),
    Leader0 = #{cluster =>
                #{n1 => new_peer_with(#{match_index => 0}), % current leader in term 2
                  n2 => new_peer_with(#{match_index => 0,
                                        next_index => N2NextIndex}), % stale peer - previous leader
                  n3 => new_peer_with(#{match_index => 3,
                                        next_index => 4,
                                        commit_index_sent => 3})}, % uptodate peer
                cluster_change_permitted => true,
                cluster_index_term => {0,0},
                commit_index => 3,
                current_term => Term,
                id => n1,
                uid => <<"n1">>,
                log_id => <<"n1">>,
                last_applied => 4,
                log => Log,
                machine => {machine, ra_machine_simple,
                            #{simple_fun => ?MACFUN,
                              initial_state => <<>>}},
                machine_version => 0,
                machine_versions => [{0, 0}],
                effective_machine_version => 0,
                effective_machine_module =>  ra_machine_simple,
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
     [{send_rpc, n2, #append_entries_rpc{entries = [{2, _, _}, {3, _, _}]}}]}
       = ra_server:handle_leader({n2, AER}, Leader0),
    ok.


leader_receives_install_snapshot_result(_Config) ->
    MacVer = {0, 1, ?MODULE},
    % should update peer next_index
    Term = 1,
    Log0 = lists:foldl(fun(E, L) ->
                              ra_log:append_sync(E, L)
                      end, ra_log:init(#{data_dir => "", uid => <<>>}),
                      [{1, 1, {noop, meta(), MacVer}},
                       {2, 1, {noop, meta(), MacVer}},
                       {3, 1, {'$usr', meta(), {enq,apple}, after_log_append}},
                       {4, 1, {'$usr', meta(), {enq,pear}, after_log_append}}]),
    mock_machine(?FUNCTION_NAME),
    Leader = #{cluster =>
               #{n1 => new_peer_with(#{match_index => 0}),
                 n2 => new_peer_with(#{match_index => 4, next_index => 5,
                                       commit_index_sent => 4}),
                 n3 => new_peer_with(#{match_index => 0, next_index => 1})},
               cluster_change_permitted => true,
               cluster_index_term => {0,0},
               commit_index => 4,
               current_term => Term,
               id => n1,
               uid => <<"n1">>,
               log_id => <<"n1">>,
               last_applied => 4,
               log => Log0,
               machine => {machine, ?FUNCTION_NAME, #{}},
               machine_state => [{4,apple}],
               machine_version => 0,
               machine_versions => [{0, 0}],
               effective_machine_version => 1,
               effective_machine_module => ra_machine_simple,
               pending_cluster_changes => []},
    ISR = #install_snapshot_result{term = Term,
                                   last_index = 2,
                                   last_term = 1},
    {leader, #{cluster := #{n3 := #{match_index := 2,
                                    commit_index_sent := 4,
                                    next_index := 5}}},
     Effects} = ra_server:handle_leader({n3, ISR}, Leader),
    ?assert(lists:any(fun({send_rpc, n3,
                           #append_entries_rpc{entries = [{3, _, _},
                                                          {4, _, _}]}}) ->
                              true;
                         (_) -> false end, Effects)),
    ok.

% %%% helpers

init_servers(ServerIds, Machine) ->
    lists:foldl(fun (ServerId, Acc) ->
                        Args = #{cluster_name => some_id,
                                 id => ServerId,
                                 uid => atom_to_binary(ServerId, utf8),
                                 initial_members => ServerIds,
                                 log_init_args => #{data_dir => "", uid => <<>>},
                                 machine => Machine},
                        Acc#{ServerId => {follower, ra_server_init(Args), []}}
                end, #{}, ServerIds).

list(L) when is_list(L) -> L;
list(L) -> [L].

entry(Idx, Term, Data) ->
    {Idx, Term, {'$usr', meta(), Data, after_log_append}}.

empty_state(NumServers, Id) ->
    Servers = lists:foldl(fun(N, Acc) ->
                                [list_to_atom("n" ++ integer_to_list(N)) | Acc]
                        end, [], lists:seq(1, NumServers)),
    ra_server_init(#{cluster_name => someid,
                     id => Id,
                     uid => atom_to_binary(Id, utf8),
                     initial_members => Servers,
                     log_init_args => #{data_dir => "", uid => <<>>},
                     machine => {simple, fun (E, _) -> E end, <<>>}}). % just keep last applied value

base_state(NumServers, MacMod) ->
    Log0 = lists:foldl(fun(E, L) ->
                              ra_log:append(E, L)
                      end, ra_log:init(#{data_dir => "", uid => <<>>}),
                      [{1, 1, usr(<<"hi1">>)},
                       {2, 3, usr(<<"hi2">>)},
                       {3, 5, usr(<<"hi3">>)}]),
    {Log, _} = ra_log:handle_event({written, {1, 3, 5}}, Log0),

    Servers = lists:foldl(fun(N, Acc) ->
                                Name = list_to_atom("n" ++ integer_to_list(N)),
                                Acc#{Name =>
                                     new_peer_with(#{next_index => 4,
                                                     match_index => 3})}
                        end, #{}, lists:seq(1, NumServers)),
    mock_machine(MacMod),
    #{id => n1,
      uid => <<"n1">>,
      log_id => <<"n1">>,
      leader_id => n1,
      cluster => Servers,
      cluster_index_term => {0, 0},
      cluster_change_permitted => true,
      pending_cluster_changes => [],
      current_term => 5,
      commit_index => 3,
      last_applied => 3,
      machine => {machine, MacMod, #{}}, % just keep last applied value
      machine_state => <<"hi3">>, % last entry has been applied
      machine_version => 0,
      machine_versions => [{0, 0}],
      effective_machine_version => 0,
      effective_machine_module => MacMod,
      log => Log,
      waiting_apply_index => #{},
      waiting_ro_heartbeats => #{}}.

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
    ct:pal("DUMP: ~p~n", [T]),
    T.

new_peer() ->
    #{next_index => 1,
      match_index => 0,
      commit_index_sent => 0}.

new_peer_with(Map) ->
    maps:merge(new_peer(), Map).

snap_meta(Idx, Term) ->
    snap_meta(Idx, Term, []).

snap_meta(Idx, Term, Cluster) ->
    #{index => Idx,
      term => Term,
      cluster => Cluster,
      machine_version => 0}.

