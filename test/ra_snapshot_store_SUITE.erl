%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2017-2025 Broadcom. All Rights Reserved. The term Broadcom refers to Broadcom Inc. and/or its subsidiaries.
%%
%% ra_snapshot with the ra_log_snapshot module and a running snapshot log
%% (ra_log_snap_store)
-module(ra_snapshot_store_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("src/ra.hrl").

-define(MACMOD, ?MODULE).
-define(MAX_SIZE, 8192).

all() ->
    [
     {group, tests}
    ].

all_tests() ->
    [
     small_snapshot_is_stored_in_the_log,
     restart_recovers_from_the_log,
     large_snapshot_uses_a_directory,
     checkpoints_do_not_use_the_log,
     directory_snapshot_supersedes_logged_snapshot,
     logged_snapshot_supersedes_directory_snapshot,
     delete_releases_logged_snapshot,
     delete_all,
     write_error_falls_back_to_directory,
     log_not_running_falls_back_to_directory_for_writes,
     unavailable_log_is_not_taken_for_an_empty_one,
     failed_snapshot_does_not_drop_the_current_logged_snapshot,
     failed_snapshot_that_was_logged_is_released,
     no_log_configured_uses_directories,
     send_logged_snapshot_full_file,
     send_logged_snapshot_compat,
     delete_effect_names_the_module
    ].

groups() ->
    [
     {tests, [], all_tests()}
    ].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(_Group, Config) ->
    Config.

end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(TestCase, Config) ->
    ok = ra_snapshot:init_ets(),
    Priv = ?config(priv_dir, Config),
    %% locations are <data_dir>/<uid>/snapshots/<term>_<index> where the data
    %% dir (priv_dir here) is what the snapshot log is registered against
    SnapDir = filename:join([Priv, TestCase, "snapshots"]),
    CheckpointDir = filename:join([Priv, TestCase, "checkpoints"]),
    RecoveryCheckpointDir = filename:join([Priv, TestCase,
                                           "recovery_checkpoint"]),
    [ok = ra_lib:make_dir(D) || D <- [SnapDir, CheckpointDir,
                                      RecoveryCheckpointDir]],
    StoreName = list_to_atom("snap_store_" ++ atom_to_list(TestCase)),
    persistent_term:erase({?MODULE, fail}),
    C1 = [{uid, ra_lib:to_binary(TestCase)},
          {snap_dir, SnapDir},
          {checkpoint_dir, CheckpointDir},
          {recovery_checkpoint_dir, RecoveryCheckpointDir},
          {max_checkpoints, ?DEFAULT_MAX_CHECKPOINTS},
          {store_name, StoreName},
          {store_dir, filename:join([Priv, "store_" ++ atom_to_list(TestCase)])}
          | Config],
    case lists:member(TestCase, [no_log_configured_uses_directories]) of
        true ->
            C1;
        false ->
            start_store(C1, #{}),
            C1
    end.

end_per_testcase(_TestCase, Config) ->
    catch ra_log_snap_store:stop(?config(store_name, Config)),
    persistent_term:erase(registry_key(Config)),
    persistent_term:erase({?MODULE, fail}),
    ok.

%%%===================================================================
%%% Test cases
%%%===================================================================

small_snapshot_is_stored_in_the_log(Config) ->
    UId = ?config(uid, Config),
    Name = ?config(store_name, Config),
    State0 = init_state(Config),
    MacState = crypto:strong_rand_bytes(2000),
    State = take(State0, 55, 2, MacState, snapshot),
    ?assertEqual({55, 2}, ra_snapshot:current(State)),
    ?assertEqual(55, ra_snapshot:last_index_for(UId)),
    %% nothing was written to the snapshots directory
    ?assertEqual({ok, []}, file:list_dir(?config(snap_dir, Config))),
    ?assertMatch({ok, #{idx := 55, term := 2}},
                 ra_log_snap_store:lookup(Name, UId)),
    ?assertMatch({ok, #{index := 55, term := 2}, MacState},
                 ra_snapshot:recover(State)),
    ?assertEqual(ra_seq:from_list([3, 5, 9]),
                 ra_log_snapshot_state:live_indexes(ra_log_snapshot_state,
                                                    UId)),
    ?assert(is_integer(ra_snapshot:snapshot_size(State))),
    ok.

restart_recovers_from_the_log(Config) ->
    UId = ?config(uid, Config),
    State0 = init_state(Config),
    MacState = crypto:strong_rand_bytes(2000),
    _ = take(State0, 55, 2, MacState, snapshot),
    ets:delete(ra_log_snapshot_state, UId),
    %% a new member process finds the snapshot in the log
    State = init_state(Config),
    ?assertEqual({55, 2}, ra_snapshot:current(State)),
    ?assertEqual(55, ra_snapshot:last_index_for(UId)),
    ?assertEqual(ra_seq:from_list([3, 5, 9]),
                 ra_log_snapshot_state:live_indexes(ra_log_snapshot_state,
                                                    UId)),
    ?assertMatch({ok, #{index := 55}, MacState}, ra_snapshot:recover(State)),
    ?assert(is_integer(ra_snapshot:snapshot_size(State))),
    ok.

large_snapshot_uses_a_directory(Config) ->
    UId = ?config(uid, Config),
    Name = ?config(store_name, Config),
    State0 = init_state(Config),
    MacState = crypto:strong_rand_bytes(?MAX_SIZE * 2),
    State = take(State0, 55, 2, MacState, snapshot),
    ?assertEqual({55, 2}, ra_snapshot:current(State)),
    ?assertMatch({ok, [_]}, file:list_dir(?config(snap_dir, Config))),
    ?assertEqual(not_found, ra_log_snap_store:lookup(Name, UId)),
    ?assertMatch({ok, #{index := 55}, MacState}, ra_snapshot:recover(State)),
    State2 = init_state(Config),
    ?assertEqual({55, 2}, ra_snapshot:current(State2)),
    ok.

checkpoints_do_not_use_the_log(Config) ->
    UId = ?config(uid, Config),
    Name = ?config(store_name, Config),
    State0 = init_state(Config),
    MacState = crypto:strong_rand_bytes(500),
    State = take(State0, 55, 2, MacState, checkpoint),
    ?assertEqual({55, 2}, ra_snapshot:latest_checkpoint(State)),
    ?assertEqual(not_found, ra_log_snap_store:lookup(Name, UId)),
    ?assertMatch({ok, [_]}, file:list_dir(?config(checkpoint_dir, Config))),
    ok.

directory_snapshot_supersedes_logged_snapshot(Config) ->
    UId = ?config(uid, Config),
    Name = ?config(store_name, Config),
    State0 = init_state(Config),
    Small = crypto:strong_rand_bytes(500),
    Large = crypto:strong_rand_bytes(?MAX_SIZE * 2),
    State1 = take(State0, 55, 2, Small, snapshot),
    ?assertMatch({ok, #{idx := 55}}, ra_log_snap_store:lookup(Name, UId)),
    State2 = take(State1, 60, 2, Large, snapshot),
    ?assertEqual({60, 2}, ra_snapshot:current(State2)),
    %% the effect ra_log emits for the previous current snapshot releases the
    %% logged entry
    {delete_snapshot, Mod, Dir, Old} =
        ra_snapshot:delete_effect(State2, snapshot, {55, 2}),
    ok = ra_snapshot:delete(Mod, Dir, Old),
    wait_for(fun () -> ra_log_snap_store:lookup(Name, UId) == not_found end),
    %% a new member process picks the directory snapshot
    State3 = init_state(Config),
    ?assertEqual({60, 2}, ra_snapshot:current(State3)),
    ?assertMatch({ok, #{index := 60}, Large}, ra_snapshot:recover(State3)),
    ok.

logged_snapshot_supersedes_directory_snapshot(Config) ->
    UId = ?config(uid, Config),
    Name = ?config(store_name, Config),
    State0 = init_state(Config),
    Large = crypto:strong_rand_bytes(?MAX_SIZE * 2),
    Small = crypto:strong_rand_bytes(500),
    State1 = take(State0, 55, 2, Large, snapshot),
    ?assertMatch({ok, [_]}, file:list_dir(?config(snap_dir, Config))),
    State2 = take(State1, 60, 2, Small, snapshot),
    ?assertEqual({60, 2}, ra_snapshot:current(State2)),
    ?assertMatch({ok, #{idx := 60}}, ra_log_snap_store:lookup(Name, UId)),
    %% a new member process picks the newer logged snapshot and removes the
    %% older directory
    State3 = init_state(Config),
    ?assertEqual({60, 2}, ra_snapshot:current(State3)),
    ?assertEqual({ok, []}, file:list_dir(?config(snap_dir, Config))),
    ?assertMatch({ok, #{index := 60}, Small}, ra_snapshot:recover(State3)),
    ok.

delete_releases_logged_snapshot(Config) ->
    UId = ?config(uid, Config),
    Name = ?config(store_name, Config),
    State0 = init_state(Config),
    State1 = take(State0, 55, 2, crypto:strong_rand_bytes(500), snapshot),
    State2 = take(State1, 60, 2, crypto:strong_rand_bytes(500), snapshot),
    Dir = ra_snapshot:directory(State2, snapshot),
    %% deleting the previous snapshot leaves the newer one alone
    ok = ra_snapshot:delete(ra_log_snapshot, Dir, {55, 2}),
    _ = ra_log_snap_store:info(Name),
    ?assertMatch({ok, #{idx := 60}}, ra_log_snap_store:lookup(Name, UId)),
    ok = ra_snapshot:delete(ra_log_snapshot, Dir, {60, 2}),
    wait_for(fun () -> ra_log_snap_store:lookup(Name, UId) == not_found end),
    ok.

delete_all(Config) ->
    UId = ?config(uid, Config),
    Name = ?config(store_name, Config),
    State0 = init_state(Config),
    State1 = take(State0, 55, 2, crypto:strong_rand_bytes(500), snapshot),
    ?assertMatch({ok, #{idx := 55}}, ra_log_snap_store:lookup(Name, UId)),
    ok = ra_snapshot:delete_all(State1),
    wait_for(fun () -> ra_log_snap_store:lookup(Name, UId) == not_found end),
    ok.

write_error_falls_back_to_directory(Config) ->
    %% the snapshot log is restarted with faults injected into its io
    UId = ?config(uid, Config),
    Name = ?config(store_name, Config),
    ra_log_snap_store:stop(Name),
    start_store(Config, #{io => io_with_faults()}),
    State0 = init_state(Config),
    inject_failure(pwrite),
    MacState = crypto:strong_rand_bytes(500),
    State = take(State0, 55, 2, MacState, snapshot),
    clear_failure(),
    ?assertEqual({55, 2}, ra_snapshot:current(State)),
    ?assertEqual(not_found, ra_log_snap_store:lookup(Name, UId)),
    ?assertMatch({ok, [_]}, file:list_dir(?config(snap_dir, Config))),
    ?assertMatch({ok, #{index := 55}, MacState}, ra_snapshot:recover(State)),
    ok.

log_not_running_falls_back_to_directory_for_writes(Config) ->
    %% configured, but not running (e.g. it is restarting): a member that is
    %% already running still takes its snapshot, as a directory
    Name = ?config(store_name, Config),
    State0 = init_state(Config),
    ra_log_snap_store:stop(Name),
    persistent_term:put(registry_key(Config),
                        #{name => Name, max_size => ?MAX_SIZE}),
    MacState = crypto:strong_rand_bytes(500),
    State = take(State0, 55, 2, MacState, snapshot),
    ?assertEqual({55, 2}, ra_snapshot:current(State)),
    ?assertMatch({ok, [_]}, file:list_dir(?config(snap_dir, Config))),
    ?assertMatch({ok, #{index := 55}, MacState}, ra_snapshot:recover(State)),
    %% and is picked up when the store is back
    start_store(Config, #{}),
    State2 = init_state(Config),
    ?assertEqual({55, 2}, ra_snapshot:current(State2)),
    ok.

%% A member that starts while the snapshot log is down must not start without
%% the snapshot that is in it: its log is truncated up to that snapshot.
unavailable_log_is_not_taken_for_an_empty_one(Config) ->
    UId = ?config(uid, Config),
    Name = ?config(store_name, Config),
    State0 = init_state(Config),
    MacState = crypto:strong_rand_bytes(500),
    _ = take(State0, 55, 2, MacState, snapshot),
    ?assertMatch({ok, #{idx := 55}}, ra_log_snap_store:lookup(Name, UId)),
    ra_log_snap_store:stop(Name),
    %% the registry is owned by whoever started the store and stays
    persistent_term:put(registry_key(Config),
                        #{name => Name, max_size => ?MAX_SIZE}),
    ?assertError({snapshot_store_unavailable, UId, _}, init_state(Config)),
    %% once it is back the member starts with its snapshot
    start_store(Config, #{}),
    persistent_term:put(registry_key(Config),
                        #{name => Name, max_size => ?MAX_SIZE}),
    State = init_state(Config),
    ?assertEqual({55, 2}, ra_snapshot:current(State)),
    ?assertMatch({ok, #{index := 55}, MacState}, ra_snapshot:recover(State)),
    ok.

%% handle_error deletes the snapshot that was being written, which must not
%% take the current snapshot with it
failed_snapshot_does_not_drop_the_current_logged_snapshot(Config) ->
    UId = ?config(uid, Config),
    Name = ?config(store_name, Config),
    State0 = init_state(Config),
    MacState = crypto:strong_rand_bytes(500),
    State1 = take(State0, 55, 2, MacState, snapshot),
    Meta = #{index => 60, term => 2, cluster => [node()], machine_version => 1},
    {State2, [{bg_work, _Fun, ErrFun}]} =
        ra_snapshot:begin_snapshot(Meta, ?MACMOD, crypto:strong_rand_bytes(500),
                                   snapshot, State1),
    ErrFun({error, enospc}),
    receive
        {ra_log_event, {snapshot_error, {60, 2} = IdxTerm, snapshot, Err}} ->
            State3 = ra_snapshot:handle_error(IdxTerm, Err, State2),
            ?assertEqual({55, 2}, ra_snapshot:current(State3)),
            _ = ra_log_snap_store:info(Name),
            ?assertMatch({ok, #{idx := 55}}, ra_log_snap_store:lookup(Name, UId)),
            ?assertMatch({ok, #{index := 55}, MacState},
                         ra_snapshot:recover(State3)),
            ?assertMatch({ok, #{index := 55}, _},
                         ra_snapshot:begin_read(State3,
                                                ra_log_snapshot:context()))
    after 5000 ->
              ct:fail(no_snapshot_error)
    end,
    ok.

%% ...but when it had been written to the log it is released
failed_snapshot_that_was_logged_is_released(Config) ->
    UId = ?config(uid, Config),
    Name = ?config(store_name, Config),
    State0 = init_state(Config),
    Meta = #{index => 60, term => 2, cluster => [node()], machine_version => 1},
    {State1, [{bg_work, Fun, _}]} =
        ra_snapshot:begin_snapshot(Meta, ?MACMOD, crypto:strong_rand_bytes(500),
                                   snapshot, State0),
    Fun(),
    ?assertMatch({ok, #{idx := 60}}, ra_log_snap_store:lookup(Name, UId)),
    State2 = ra_snapshot:handle_error({60, 2}, {error, blah}, State1),
    ?assertEqual(undefined, ra_snapshot:pending(State2)),
    wait_for(fun () -> ra_log_snap_store:lookup(Name, UId) == not_found end),
    ok.

no_log_configured_uses_directories(Config) ->
    State0 = init_state(Config),
    MacState = crypto:strong_rand_bytes(500),
    State = take(State0, 55, 2, MacState, snapshot),
    ?assertEqual({55, 2}, ra_snapshot:current(State)),
    ?assertMatch({ok, [_]}, file:list_dir(?config(snap_dir, Config))),
    ?assertMatch({ok, #{index := 55}, MacState}, ra_snapshot:recover(State)),
    ok.

send_logged_snapshot_full_file(Config) ->
    send_logged_snapshot(Config, ra_log_snapshot:context()).

send_logged_snapshot_compat(Config) ->
    %% a receiver that cannot accept the whole file
    send_logged_snapshot(Config, #{}).

send_logged_snapshot(Config, Context) ->
    Name = ?config(store_name, Config),
    UId = ?config(uid, Config),
    State0 = init_state(Config),
    MacState = crypto:strong_rand_bytes(3000),
    State = take(State0, 55, 2, MacState, snapshot),
    ?assertMatch({ok, #{idx := 55}}, ra_log_snap_store:lookup(Name, UId)),
    %% the receiver lives under a different data dir so it has no log
    Priv = ?config(priv_dir, Config),
    RDir = filename:join([Priv, "receiver", "follower"]),
    [ok = ra_lib:make_dir(filename:join(RDir, D))
     || D <- ["snapshots", "checkpoints", "recovery_checkpoint"]],
    Receiver0 = ra_snapshot:init(<<"follower">>, ra_log_snapshot,
                                 filename:join(RDir, "snapshots"),
                                 filename:join(RDir, "checkpoints"),
                                 filename:join(RDir, "recovery_checkpoint"),
                                 undefined, undefined,
                                 ?config(max_checkpoints, Config)),
    {ok, Meta, ReadState} = ra_snapshot:begin_read(State, Context),
    ?assertMatch(#{index := 55, term := 2}, Meta),
    {ok, Receiver1} = ra_snapshot:begin_accept(Meta, Receiver0),
    Machine = {machine, ?MODULE, #{}},
    {Receiver, _, _, _} = transfer(ReadState, State, Receiver1, 1, Machine),
    ?assertEqual({55, 2}, ra_snapshot:current(Receiver)),
    ?assertMatch({ok, #{index := 55}, MacState}, ra_snapshot:recover(Receiver)),
    ok.

transfer(ReadState, SendState, Receiver, Num, Machine) ->
    case ra_snapshot:read_chunk(ReadState, 1024, SendState) of
        {ok, Chunk, {next, ReadState1}} ->
            Receiver1 = ra_snapshot:accept_chunk(Chunk, Num, Receiver),
            transfer(ReadState1, SendState, Receiver1, Num + 1, Machine);
        {ok, Chunk, last} ->
            ra_snapshot:complete_accept(Chunk, Num, Machine, Receiver)
    end.

delete_effect_names_the_module(Config) ->
    State = init_state(Config),
    Dir = ra_snapshot:directory(State, snapshot),
    ?assertEqual({delete_snapshot, ra_log_snapshot, Dir, {5, 1}},
                 ra_snapshot:delete_effect(State, snapshot, {5, 1})),
    ok.

%%%===================================================================
%%% Helpers
%%%===================================================================

registry_key(Config) ->
    ra_log_snap_store:registry_key(?config(priv_dir, Config)).

start_store(Config, Opts) ->
    Name = ?config(store_name, Config),
    {ok, _} = ra_log_snap_store:start_link(
                Opts#{name => Name,
                      dir => ?config(store_dir, Config),
                      registry => {registry_key(Config),
                                   #{name => Name, max_size => ?MAX_SIZE}}}),
    ok.

init_state(Config) ->
    ra_snapshot:init(?config(uid, Config), ra_log_snapshot,
                     ?config(snap_dir, Config),
                     ?config(checkpoint_dir, Config),
                     ?config(recovery_checkpoint_dir, Config),
                     undefined, undefined,
                     ?config(max_checkpoints, Config)).

%% takes a snapshot or checkpoint the way ra_log does and completes it
take(State0, Idx, Term, MacState, Kind) ->
    Meta = #{index => Idx, term => Term, cluster => [node()],
             machine_version => 1},
    {State1, [{bg_work, Fun, _}]} =
        ra_snapshot:begin_snapshot(Meta, ?MACMOD, MacState, Kind, State0),
    Fun(),
    receive
        {ra_log_event,
         {snapshot_written, {Idx, Term} = IdxTerm, Indexes, Kind, Size, _}} ->
            ra_snapshot:complete_snapshot(IdxTerm, Kind, Indexes, Size, State1)
    after 5000 ->
              ct:fail(snapshot_event_timeout)
    end.

wait_for(Fun) ->
    wait_for(Fun, 100).

wait_for(_Fun, 0) ->
    ct:fail(condition_never_true);
wait_for(Fun, N) ->
    case Fun() of
        true -> ok;
        false ->
            timer:sleep(20),
            wait_for(Fun, N - 1)
    end.

io_with_faults() ->
    #{pwrite => fun (Fd, Off, IO) ->
                        case persistent_term:get({?MODULE, fail}, undefined) of
                            pwrite -> {error, eio};
                            _ -> file:pwrite(Fd, Off, IO)
                        end
                end}.

inject_failure(What) ->
    persistent_term:put({?MODULE, fail}, What).

clear_failure() ->
    persistent_term:erase({?MODULE, fail}).

%% ra_machine fakes
version() -> 1.
live_indexes(_) -> [3, 5, 9].
