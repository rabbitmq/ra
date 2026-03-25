%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Reproduction test for rabbitmq/ra bug #70:
%%   pre_append_log_follower crashes with {badkey, previous_cluster}
%%   when a new leader overwrites an uncommitted cluster change entry
%%   with a regular entry.
%%
%% Bug location: ra_server.erl, pre_append_log_follower/2, first clause,
%%   non-cluster-change branch (line ~3521):
%%     {PrevIdx, PrevTerm, PrevCluster} = maps:get(previous_cluster, State)
%%   crashes because `previous_cluster` was never set by the follower path.
%%
%% Scenario:
%%   1. 3-node cluster {N1, N2, N3}. N1 is leader at term 5.
%%   2. N1 proposes adding N4 via $ra_cluster_change at index 4, sends AER to N2.
%%   3. N2 (follower) processes the cluster change. cluster_index_term -> {4,5}.
%%      No `previous_cluster` key is set (only the leader path sets it).
%%   4. N1 crashes. N3 becomes leader at term 6.
%%   5. N3 writes a regular command at index 4 (term 6), sends AER to N2.
%%   6. N2's pre_append_log_follower matches first clause (Idx=4, Term=6 /= 5),
%%      enters non-cluster-change branch, calls maps:get(previous_cluster, State)
%%      -> CRASH: {badkey, previous_cluster}
%%
%% Run with:
%%   cd case-studies/ra/artifact/ra
%%   ct_run -pa ebin -pa test -suite ../../../../repro/ra_bug70_repro_SUITE \
%%          -logdir /tmp/ct_logs
%%
%% Or standalone (see main/0 below).

-module(ra_bug70_repro_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include("src/ra.hrl").
-include("src/ra_server.hrl").
-include_lib("eunit/include/eunit.hrl").

%% Node IDs
-define(N1, {n1, node()}).
-define(N2, {n2, node()}).
-define(N3, {n3, node()}).
-define(N4, {n4, node()}).

%% ============================================================
%% Common Test callbacks
%% ============================================================

all() ->
    [bug70_pre_append_log_follower_badkey_crash].

init_per_suite(Config) ->
    ok = logger:set_primary_config(level, all),
    Config.

end_per_suite(Config) ->
    Config.

init_per_testcase(_TestCase, Config) ->
    ok = setup_log(),
    Config.

end_per_testcase(_TestCase, _Config) ->
    meck:unload(),
    ok.

%% ============================================================
%% Test case
%% ============================================================

bug70_pre_append_log_follower_badkey_crash(_Config) ->
    N1 = ?N1, N2 = ?N2, N3 = ?N3, N4 = ?N4,

    %% --- Build the follower state for N2 ---
    %% Base state has 3 log entries at indices 1-3, all confirmed written.
    %% cluster_index_term = {0, 0} (no cluster change applied yet).
    OldCluster = #{N1 => new_peer_with(#{next_index => 4, match_index => 3}),
                   N2 => new_peer_with(#{next_index => 4, match_index => 3}),
                   N3 => new_peer_with(#{next_index => 4, match_index => 3})},
    Base = base_state(3, ?FUNCTION_NAME),
    Cfg = maps:get(cfg, Base),
    FollowerState0 = Base#{cfg => Cfg#cfg{id = N2,
                                          uid = <<"n2">>,
                                          log_id = "n2"},
                           cluster => OldCluster},

    %% --- Step 1: N1 (leader, term 5) sends AER with cluster change at index 4 ---
    %% This adds N4 to the cluster.
    NewCluster = #{N1 => new_peer_with(#{next_index => 4, match_index => 3}),
                   N2 => new_peer_with(#{next_index => 4, match_index => 3}),
                   N3 => new_peer_with(#{next_index => 4, match_index => 3}),
                   N4 => new_peer_with(#{next_index => 1})},
    ClusterChangeEntry = {4, 5, {'$ra_cluster_change', meta(),
                                  NewCluster, await_consensus}},
    AER1 = #append_entries_rpc{term = 5,
                               leader_id = N1,
                               prev_log_index = 3,
                               prev_log_term = 5,
                               leader_commit = 3,
                               entries = [ClusterChangeEntry]},

    %% Process AER1: follower applies cluster change via pre_append_log_follower.
    %% The SECOND clause fires (new cluster change, no overwrite).
    %% Sets cluster_index_term => {4, 5}, but does NOT set previous_cluster.
    {follower, FollowerState1, _Effects1} =
        ra_server:handle_follower(AER1, FollowerState0),

    %% Confirm the write (simulate wal acknowledgment)
    {follower, FollowerState2, _Effects2} =
        ra_server:handle_follower(written_evt(5, {4, 4}), FollowerState1),

    %% Verify the cluster change was applied
    ?assertMatch(#{cluster_index_term := {4, 5}}, FollowerState2),
    ?assertMatch(#{cluster := #{N4 := _}}, FollowerState2),
    %% After fix: previous_cluster IS set by the follower cluster change path
    ?assert(maps:is_key(previous_cluster, FollowerState2)),

    %% --- Step 2: N3 becomes leader at term 6, overwrites index 4 ---
    %% N3 never received the cluster change. It writes a regular command
    %% at index 4 with term 6.
    RegularEntry = {4, 6, usr(<<"overwrite">>)},
    AER2 = #append_entries_rpc{term = 6,
                               leader_id = N3,
                               prev_log_index = 3,
                               prev_log_term = 5,
                               leader_commit = 3,
                               entries = [RegularEntry]},

    %% --- This is where the crash happens ---
    %% pre_append_log_follower matches first clause:
    %%   Idx=4, cluster_index_term={4,5}, Term=6 /= 5
    %% Enters non-cluster-change branch, calls:
    %%   maps:get(previous_cluster, State)  -> {badkey, previous_cluster}
    Result = (catch ra_server:handle_follower(AER2, FollowerState2)),

    case Result of
        {follower, _State, _Effects} ->
            ct:pal("FIX VERIFIED: handle_follower succeeded without crashing.~n"
                   "The overwrite path found previous_cluster in the state."),
            ok;
        {'EXIT', {{badkey, previous_cluster}, _Stacktrace}} ->
            ct:fail("FIX NOT WORKING: still crashes with {badkey, previous_cluster}");
        Other ->
            ct:fail("Unexpected result: ~p", [Other])
    end.

%% ============================================================
%% Standalone entry point (run without Common Test)
%% ============================================================

main() ->
    %% For running outside ct_run, e.g.:
    %%   erl -pa ebin -pa test -s ra_bug70_repro_SUITE main -s init stop
    ok = setup_log(),
    try
        bug70_pre_append_log_follower_badkey_crash(dummy_config),
        io:format("TEST PASSED: Bug #70 reproduced successfully.~n")
    catch
        Class:Reason:Stack ->
            io:format("TEST FAILED: ~p:~p~n~p~n", [Class, Reason, Stack])
    after
        meck:unload()
    end.

%% ============================================================
%% Helpers (adapted from ra_server_SUITE.erl)
%% ============================================================

meta() ->
    #{from => {self(), make_ref()},
      ts => os:system_time(millisecond)}.

usr(Data) ->
    {'$usr', meta(), Data, after_log_append}.

new_peer() ->
    #{next_index => 1,
      match_index => 0,
      query_index => 0,
      commit_index_sent => 0,
      status => normal}.

new_peer_with(Map) ->
    maps:merge(new_peer(), Map).

written_evt(Term, Range) when is_tuple(Range) ->
    {ra_log_event, {written, Term, [Range]}}.

mock_machine(Mod) ->
    meck:new(Mod, [non_strict]),
    meck:expect(Mod, init, fun (_) -> init_state end),
    meck:expect(Mod, apply, fun (_, Cmd, _) -> {Cmd, ok} end),
    ok.

base_state(NumServers, MacMod) ->
    Log0 = lists:foldl(fun(E, L) ->
                               ra_log:append(E, L)
                       end, ra_log:init(#{system_config =>
                                              ra_system:default_config(),
                                          uid => <<>>}),
                       [{1, 1, usr(<<"hi1">>)},
                        {2, 3, usr(<<"hi2">>)},
                        {3, 5, usr(<<"hi3">>)}]),
    {Log, _} = ra_log:handle_event({written, 5, [{1, 3}]}, Log0),

    Servers = lists:foldl(fun(N, Acc) ->
                                Name = {list_to_atom("n" ++ integer_to_list(N)),
                                        node()},
                                Acc#{Name =>
                                     new_peer_with(#{next_index => 4,
                                                     match_index => 3})}
                        end, #{}, lists:seq(1, NumServers)),
    mock_machine(MacMod),
    Cfg = #cfg{id = ?N1,
               uid = <<"n1">>,
               log_id = <<"n1">>,
               metrics_key = n1,
               metrics_labels = #{},
               machine = {machine, MacMod, #{}},
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
      machine_state => <<"hi3">>,
      current_term => 5,
      commit_index => 3,
      last_applied => 3,
      log => Log,
      query_index => 0,
      queries_waiting_heartbeats => queue:new(),
      pending_consistent_queries => []}.

setup_log() ->
    ok = meck:new(ra_log, []),
    ok = meck:new(ra_snapshot, [passthrough]),
    ok = meck:new(ra_machine, [passthrough]),
    meck:expect(ra_log, init, fun(C) -> ra_log_memory:init(C) end),
    meck:expect(ra_log, recover_snapshot, fun(Log) ->
        ra_log_memory:recover_snapshot(Log)
    end),
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
                fun(Meta, undefined) ->
                        {ok, {Meta, undefined}}
                end),
    meck:expect(ra_snapshot, accept_chunk,
                fun(Data, _OutOf, {Meta, _}) ->
                        {ok, {Meta, Data}, []}
                end),
    meck:expect(ra_snapshot, complete_accept,
                fun(Data, _Num, _Machine, {_Meta, MacSt} = State) ->
                        {State, Data ++ MacSt, [], []}
                end),
    meck:expect(ra_snapshot, abort_accept, fun(_SS) -> undefined end),
    meck:expect(ra_snapshot, accepting, fun({Meta, _}) ->
                                               {maps:get(index, Meta),
                                                maps:get(term, Meta)};
                                           (_) ->
                                                undefined
                                        end),
    meck:expect(ra_snapshot, recovery_checkpoint, fun(_) -> undefined end),
    meck:expect(ra_log, snapshot_state, fun ra_log_memory:snapshot_state/1),
    meck:expect(ra_log, set_snapshot_state,
                fun ra_log_memory:set_snapshot_state/2),
    meck:expect(ra_log, install_snapshot,
                fun ra_log_memory:install_snapshot/4),
    meck:expect(ra_log, recover_snapshot,
                fun ra_log_memory:recover_snapshot/1),
    meck:expect(ra_log, snapshot_index_term,
                fun ra_log_memory:snapshot_index_term/1),
    meck:expect(ra_log, fold, fun ra_log_memory:fold/5),
    meck:expect(ra_log, fold, fun  (A, B, C, D, E, _) ->
                                      ra_log_memory:fold(A, B, C, D, E)
                              end),
    meck:expect(ra_log, release_resources,
                fun ra_log_memory:release_resources/3),
    meck:expect(ra_log, overview, fun ra_log_memory:overview/1),
    meck:expect(ra_log, append_sync,
                fun({Idx, Term, _} = E, L0) ->
                        L1 = ra_log_memory:append(E, L0),
                        {L, _} = ra_log_memory:handle_event(
                                   {written, Term, [Idx]}, L1),
                        L
                end),
    meck:expect(ra_log, write_config, fun ra_log_memory:write_config/2),
    meck:expect(ra_log, next_index, fun ra_log_memory:next_index/1),
    meck:expect(ra_log, has_pending, fun (_) -> false end),
    meck:expect(ra_log, append, fun ra_log_memory:append/2),
    meck:expect(ra_log, write, fun ra_log_memory:write/2),
    meck:expect(ra_log, write_sparse, fun ra_log_memory:write_sparse/3),
    meck:expect(ra_log, handle_event, fun ra_log_memory:handle_event/2),
    meck:expect(ra_log, last_written, fun ra_log_memory:last_written/1),
    meck:expect(ra_log, last_index_term,
                fun ra_log_memory:last_index_term/1),
    meck:expect(ra_log, set_last_index,
                fun ra_log_memory:set_last_index/2),
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
