%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2024 Broadcom. All Rights Reserved. The term Broadcom refers to Broadcom Inc. and/or its subsidiaries.
%%
-module(ra_checkpoint_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

%%%===================================================================
%%% Common Test callbacks
%%%===================================================================

all() ->
    [
     {group, tests}
    ].


all_tests() ->
    [
     init_empty,
     take_checkpoint,
     take_checkpoint_crash,
     recover_from_checkpoint_only,
     recover_from_checkpoint_and_snapshot,
     newer_snapshot_deletes_older_checkpoints,
     init_recover_corrupt,
     init_recover_multi_corrupt
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
    SnapDir = filename:join([?config(priv_dir, Config),
                             TestCase, "snapshots"]),
    CheckpointDir = filename:join([?config(priv_dir, Config),
                                   TestCase, "checkpoints"]),
    ok = ra_lib:make_dir(SnapDir),
    ok = ra_lib:make_dir(CheckpointDir),
    [{uid, ra_lib:to_binary(TestCase)},
     {snap_dir, SnapDir},
     {checkpoint_dir, CheckpointDir} | Config].

end_per_testcase(_TestCase, _Config) ->
    ok.

%%%===================================================================
%%% Test cases
%%%===================================================================

init_empty(Config) ->
    State = init_state(Config),
    undefined = ra_snapshot:latest_checkpoint(State),

    ok.

take_checkpoint(Config) ->
    State0 = init_state(Config),

    Meta = meta(55, 2, [node()]),
    MacRef = ?FUNCTION_NAME,
    {State1, [{monitor, process, snapshot_writer, Pid}]} =
         ra_snapshot:begin_snapshot(Meta, MacRef, checkpoint, State0),
    undefined = ra_snapshot:latest_checkpoint(State1),
    {Pid, {55, 2}, checkpoint} = ra_snapshot:pending(State1),
    receive
        {ra_log_event, {snapshot_written, {55, 2} = IdxTerm, checkpoint}} ->
            State = ra_snapshot:complete_snapshot(IdxTerm, checkpoint, State1),
            undefined = ra_snapshot:pending(State),
            {55, 2} = ra_snapshot:latest_checkpoint(State),
            ok
    after 1000 ->
            error(snapshot_event_timeout)
    end,

    ok.

take_checkpoint_crash(Config) ->
    State0 = init_state(Config),
    Meta = meta(55, 2, [node()]),
    MacRef = ?FUNCTION_NAME,
    {State1, [{monitor, process, snapshot_writer, Pid}]} =
         ra_snapshot:begin_snapshot(Meta, MacRef, checkpoint, State0),
    undefined = ra_snapshot:latest_checkpoint(State1),
    {Pid, {55, 2}, checkpoint} = ra_snapshot:pending(State1),
    receive
        {ra_log_event, _} ->
            %% Just pretend the snapshot event didn't happen
            %% and the process instead crashed.
            ok
    after 10 -> ok
    end,

    State = ra_snapshot:handle_down(Pid, it_crashed_dawg, State1),
    %% If the checkpoint process crashed we just have to consider the
    %% checkpoint as faulty and clear it up.
    undefined = ra_snapshot:pending(State),
    undefined = ra_snapshot:latest_checkpoint(State),

    %% The written checkpoint should be removed.
    ?assertEqual([], list_checkpoint_dirs(Config)),

    ok.

recover_from_checkpoint_only(Config) ->
    State0 = init_state(Config),
    {error, no_current_snapshot} = ra_snapshot:recover(State0),

    Meta = meta(55, 2, [node()]),
    {State1, [{monitor, process, snapshot_writer, _}]} =
        ra_snapshot:begin_snapshot(Meta, ?FUNCTION_NAME, checkpoint, State0),
    receive
        {ra_log_event, {snapshot_written, IdxTerm, checkpoint}} ->
            _ = ra_snapshot:complete_snapshot(IdxTerm, checkpoint, State1),
            ok
    after 1000 ->
              error(snapshot_event_timeout)
    end,

    %% Open a new snapshot state to simulate a restart.
    Recover = init_state(Config),
    undefined = ra_snapshot:pending(Recover),
    {55, 2} = ra_snapshot:latest_checkpoint(Recover),
    undefined = ra_snapshot:current(Recover),

    {ok, Meta, ?FUNCTION_NAME} = ra_snapshot:recover(Recover),

    ok.

recover_from_checkpoint_and_snapshot(Config) ->
    State0 = init_state(Config),
    {error, no_current_snapshot} = ra_snapshot:recover(State0),

    %% Snapshot.
    SnapMeta = meta(55, 2, [node()]),
    {State1, [{monitor, process, snapshot_writer, _}]} =
         ra_snapshot:begin_snapshot(SnapMeta, ?FUNCTION_NAME, snapshot, State0),
    State2 = receive
                 {ra_log_event, {snapshot_written, IdxTerm1, snapshot}} ->
                       ra_snapshot:complete_snapshot(IdxTerm1, snapshot, State1)
             after 1000 ->
                       error(snapshot_event_timeout)
             end,

    %% Checkpoint at a later index.
    CPMeta = meta(105, 3, [node()]),
    {State3, [{monitor, process, snapshot_writer, _}]} =
         ra_snapshot:begin_snapshot(CPMeta, ?FUNCTION_NAME, checkpoint, State2),
    receive
        {ra_log_event, {snapshot_written, IdxTerm2, checkpoint}} ->
             _ = ra_snapshot:complete_snapshot(IdxTerm2, checkpoint, State3),
             ok
    after 1000 ->
              error(snapshot_event_timeout)
    end,

    %% Open a new snapshot state to simulate a restart.
    Recover = init_state(Config),
    undefined = ra_snapshot:pending(Recover),
    %% Both the checkpoint and the snapshot exist.
    {105, 3} = ra_snapshot:latest_checkpoint(Recover),
    {55, 2} = ra_snapshot:current(Recover),
    %% The checkpoint is used for recovery since it is newer.
    {ok, CPMeta, ?FUNCTION_NAME} = ra_snapshot:recover(Recover),

    ok.

newer_snapshot_deletes_older_checkpoints(Config) ->
    State0 = init_state(Config),
    {error, no_current_snapshot} = ra_snapshot:recover(State0),

    %% Checkpoint at 25.
    CP1Meta = meta(25, 2, [node()]),
    {State1, [{monitor, process, snapshot_writer, _}]} =
         ra_snapshot:begin_snapshot(CP1Meta, ?FUNCTION_NAME, checkpoint, State0),
    State2 = receive
                 {ra_log_event, {snapshot_written, IdxTerm1, checkpoint}} ->
                       ra_snapshot:complete_snapshot(IdxTerm1, checkpoint, State1)
             after 1000 ->
                       error(snapshot_event_timeout)
             end,

    %% Checkpoint at 35.
    CP2Meta = meta(35, 3, [node()]),
    {State3, [{monitor, process, snapshot_writer, _}]} =
         ra_snapshot:begin_snapshot(CP2Meta, ?FUNCTION_NAME, checkpoint, State2),
    State4 = receive
                 {ra_log_event, {snapshot_written, IdxTerm2, checkpoint}} ->
                       ra_snapshot:complete_snapshot(IdxTerm2, checkpoint, State3)
             after 1000 ->
                       error(snapshot_event_timeout)
             end,

    %% Checkpoint at 55.
    CP3Meta = meta(55, 5, [node()]),
    {State5, [{monitor, process, snapshot_writer, _}]} =
         ra_snapshot:begin_snapshot(CP3Meta, ?FUNCTION_NAME, checkpoint, State4),
    State6 = receive
                 {ra_log_event, {snapshot_written, IdxTerm3, checkpoint}} ->
                       ra_snapshot:complete_snapshot(IdxTerm3, checkpoint, State5)
             after 1000 ->
                       error(snapshot_event_timeout)
             end,

    %% Snapshot at 45.
    SnapMeta = meta(45, 4, [node()]),
    {State7, [{monitor, process, snapshot_writer, _}]} =
         ra_snapshot:begin_snapshot(SnapMeta, ?FUNCTION_NAME, snapshot, State6),
    State8 = receive
                 {ra_log_event, {snapshot_written, IdxTerm4, snapshot}} ->
                      ra_snapshot:complete_snapshot(IdxTerm4, snapshot, State7)
             after 1000 ->
                       error(snapshot_event_timeout)
             end,

    %% The first and second checkpoint are older than the snapshot.
    {_State, [{35, 3}, {25, 2}]} =
        ra_snapshot:take_older_checkpoints(45, State8),

    %% Open a new snapshot state to simulate a restart.
    Recover = init_state(Config),
    undefined = ra_snapshot:pending(Recover),
    %% Both the latest checkpoint and the snapshot exist.
    {55, 5} = ra_snapshot:latest_checkpoint(Recover),
    {45, 4} = ra_snapshot:current(Recover),
    %% The latest checkpoint has the highest index so it is used for recovery.
    {ok, CP3Meta, ?FUNCTION_NAME} = ra_snapshot:recover(Recover),

    %% Initializing the state removes any checkpoints older than the snapshot,
    %% so there should be one snapshot and one checkpoint only.
    ?assertMatch([_], list_snap_dirs(Config)),
    ?assertMatch([_], list_checkpoint_dirs(Config)),

    ok.

init_recover_corrupt(Config) ->
    State0 = init_state(Config),

    %% Take a checkpoint.
    Meta = meta(55, 2, [node()]),
    MacRef = ?FUNCTION_NAME,
    {State1, _} = ra_snapshot:begin_snapshot(Meta, MacRef, checkpoint, State0),
    receive
        {ra_log_event, {snapshot_written, {55, 2} = IdxTerm, checkpoint}} ->
            _ = ra_snapshot:complete_snapshot(IdxTerm, checkpoint, State1),
            ok
    after 1000 ->
            error(snapshot_event_timeout)
    end,

    %% Delete the file but leave the directory intact.
    CorruptDir = filename:join(?config(checkpoint_dir, Config),
                               ra_lib:zpad_hex(2) ++ "_" ++ ra_lib:zpad_hex(55)),
    ok = file:delete(filename:join(CorruptDir, "snapshot.dat")),

    Recover = init_state(Config),
    %% The checkpoint isn't recovered and the directory is cleaned up.
    undefined = ra_snapshot:pending(Recover),
    undefined = ra_snapshot:current(Recover),
    undefined = ra_snapshot:latest_checkpoint(Recover),
    {error, no_current_snapshot} = ra_snapshot:recover(Recover),
    false = filelib:is_dir(CorruptDir),

    ok.

init_recover_multi_corrupt(Config) ->
    State0 = init_state(Config),
    {error, no_current_snapshot} = ra_snapshot:recover(State0),

    %% Checkpoint at 55.
    CP1Meta = meta(55, 2, [node()]),
    {State1, _} =
         ra_snapshot:begin_snapshot(CP1Meta, ?FUNCTION_NAME, checkpoint, State0),
    State2 = receive
                 {ra_log_event, {snapshot_written, IdxTerm1, checkpoint}} ->
                     ra_snapshot:complete_snapshot(IdxTerm1, checkpoint, State1)
             after 1000 ->
                     error(snapshot_event_timeout)
             end,

    %% Checkpoint at 165.
    CP2Meta = meta(165, 2, [node()]),
    {State3, _} =
         ra_snapshot:begin_snapshot(CP2Meta, ?FUNCTION_NAME, checkpoint, State2),
    State4 = receive
                 {ra_log_event, {snapshot_written, IdxTerm2, checkpoint}} ->
                      ra_snapshot:complete_snapshot(IdxTerm2, checkpoint, State3)
             after 1000 ->
                       error(snapshot_event_timeout)
             end,
    {165, 2} = ra_snapshot:latest_checkpoint(State4),

    %% Corrupt the latest checkpoint.
    Corrupt = filename:join(?config(checkpoint_dir, Config),
                            ra_lib:zpad_hex(2) ++ "_" ++ ra_lib:zpad_hex(165)),
    ok = file:delete(filename:join(Corrupt, "snapshot.dat")),

    %% Open a new snapshot state to simulate a restart.
    Recover = init_state(Config),
    undefined = ra_snapshot:pending(Recover),
    %% The latest non-corrupt checkpoint is now the latest checkpoint.
    {55, 2} = ra_snapshot:latest_checkpoint(Recover),
    %% The corrupt checkpoint is cleaned up.
    false = filelib:is_dir(Corrupt),

    {ok, CP1Meta, ?FUNCTION_NAME} = ra_snapshot:recover(Recover),

    ok.

%%%===================================================================
%%% Helper functions
%%%===================================================================

init_state(Config) ->
    ra_snapshot:init(?config(uid, Config),
                     ra_log_snapshot,
                     ?config(snap_dir, Config),
                     ?config(checkpoint_dir, Config),
                     undefined).

meta(Idx, Term, Cluster) ->
    #{index => Idx,
      term => Term,
      cluster => Cluster,
      machine_version => 1}.

list_checkpoint_dirs(Config) ->
    CPDir = ?config(checkpoint_dir, Config),
    filelib:wildcard(filename:join(CPDir, "*")).

list_snap_dirs(Config) ->
    SnapDir = ?config(snap_dir, Config),
    filelib:wildcard(filename:join(SnapDir, "*")).
