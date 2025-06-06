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
-include("src/ra.hrl").

-define(MACMOD, ?MODULE).

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
     {checkpoint_dir, CheckpointDir},
     {max_checkpoints, ?DEFAULT_MAX_CHECKPOINTS} | Config].

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
    MacState = ?FUNCTION_NAME,
    {State1, [{bg_work, Fun, _}]} =
         ra_snapshot:begin_snapshot(Meta, ?MACMOD, MacState, checkpoint, State0),

    undefined = ra_snapshot:latest_checkpoint(State1),
    {{55, 2}, checkpoint} = ra_snapshot:pending(State1),
    Fun(),
    receive
        {ra_log_event, {snapshot_written, {55, 2} = IdxTerm, Indexes, checkpoint}} ->
            State = ra_snapshot:complete_snapshot(IdxTerm, checkpoint,
                                                  Indexes, State1),
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
    MacState = ?FUNCTION_NAME,
    {State1, [{bg_work, _Fun, ErrFun}]} =
         ra_snapshot:begin_snapshot(Meta, ?MODULE, MacState, checkpoint, State0),
    undefined = ra_snapshot:latest_checkpoint(State1),
    {{55, 2}, checkpoint} = ra_snapshot:pending(State1),
    ErrFun(it_failed),
    {snapshot_error, {55,2}, checkpoint, Err} =
    receive
        {ra_log_event, Evt} ->
            Evt
    after 10 -> ok
    end,

    State = ra_snapshot:handle_error({55,2}, Err, State1),
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
    {State1, [{bg_work, Fun, _}]} =
        ra_snapshot:begin_snapshot(Meta, ?MODULE, ?FUNCTION_NAME,
                                   checkpoint, State0),
    Fun(),
    receive
        {ra_log_event, {snapshot_written, IdxTerm, Indexes, checkpoint}} ->
            _ = ra_snapshot:complete_snapshot(IdxTerm, checkpoint,
                                              Indexes, State1),
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
    {State1, [{bg_work, Fun, _}]} =
         ra_snapshot:begin_snapshot(SnapMeta, ?MODULE, ?FUNCTION_NAME,
                                    snapshot, State0),
    Fun(),
    State2 = receive
                 {ra_log_event, {snapshot_written, IdxTerm1, Indexes, snapshot}} ->
                       ra_snapshot:complete_snapshot(IdxTerm1, snapshot,
                                                     Indexes, State1)
             after 1000 ->
                       error(snapshot_event_timeout)
             end,

    %% Checkpoint at a later index.
    CPMeta = meta(105, 3, [node()]),
    {State3, [{bg_work, Fun2, _}]} =
         ra_snapshot:begin_snapshot(CPMeta, ?MODULE, ?FUNCTION_NAME,
                                    checkpoint, State2),
    Fun2(),
    receive
        {ra_log_event, {snapshot_written, IdxTerm2, Indexes2, checkpoint}} ->
             _ = ra_snapshot:complete_snapshot(IdxTerm2, checkpoint,
                                               Indexes2, State3),
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
    {State1, [{bg_work, Fun, _}]} =
         ra_snapshot:begin_snapshot(CP1Meta, ?MODULE, ?FUNCTION_NAME,
                                    checkpoint, State0),
    Fun(),
    State2 = receive
                 {ra_log_event, {snapshot_written, IdxTerm1, Indexes, checkpoint}} ->
                       ra_snapshot:complete_snapshot(IdxTerm1, checkpoint,
                                                     Indexes, State1)
             after 1000 ->
                       error(snapshot_event_timeout)
             end,

    %% Checkpoint at 35.
    CP2Meta = meta(35, 3, [node()]),
    {State3, [{bg_work, Fun2, _}]} =
         ra_snapshot:begin_snapshot(CP2Meta, ?MODULE, ?FUNCTION_NAME,
                                    checkpoint, State2),
    Fun2(),
    State4 = receive
                 {ra_log_event, {snapshot_written, IdxTerm2, Indexes2, checkpoint}} ->
                       ra_snapshot:complete_snapshot(IdxTerm2, checkpoint,
                                                     Indexes2, State3)
             after 1000 ->
                       error(snapshot_event_timeout)
             end,

    %% Checkpoint at 55.
    CP3Meta = meta(55, 5, [node()]),
    {State5, [{bg_work, Fun3, _}]} =
         ra_snapshot:begin_snapshot(CP3Meta, ?MODULE, ?FUNCTION_NAME,
                                    checkpoint, State4),
    Fun3(),
    State6 = receive
                 {ra_log_event, {snapshot_written, IdxTerm3, Indexes3, checkpoint}} ->
                       ra_snapshot:complete_snapshot(IdxTerm3, checkpoint,
                                                     Indexes3, State5)
             after 1000 ->
                       error(snapshot_event_timeout)
             end,

    %% Snapshot at 45.
    SnapMeta = meta(45, 4, [node()]),
    {State7, [{bg_work, Fun4, _}]} =
         ra_snapshot:begin_snapshot(SnapMeta, ?MODULE, ?FUNCTION_NAME,
                                    snapshot, State6),
    Fun4(),
    State8 = receive
                 {ra_log_event, {snapshot_written, IdxTerm4, Indexes4, snapshot}} ->
                      ra_snapshot:complete_snapshot(IdxTerm4, snapshot,
                                                    Indexes4, State7)
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
    Meta1 = meta(55, 2, [node()]),
    {State1, [{bg_work, Fun, _}]} =
        ra_snapshot:begin_snapshot(Meta1, ?MODULE, ?FUNCTION_NAME,
                                   checkpoint, State0),
    Fun(),
    State2 = receive
                 {ra_log_event, {snapshot_written, {55, 2} = IdxTerm1, Indexes, checkpoint}} ->
                     ra_snapshot:complete_snapshot(IdxTerm1, checkpoint, Indexes, State1)
             after 1000 ->
                     error(snapshot_event_timeout)
             end,

    %% Take another checkpoint.
    Meta2 = meta(165, 2, [node()]),
    {State3, [{bg_work, Fun2, _}]} =
        ra_snapshot:begin_snapshot(Meta2, ?MODULE, ?FUNCTION_NAME,
                                   checkpoint, State2),
    Fun2(),
    receive
        {ra_log_event, {snapshot_written, {165, 2} = IdxTerm2, Indexes2, checkpoint}} ->
            _ = ra_snapshot:complete_snapshot(IdxTerm2, checkpoint, Indexes2, State3),
            ok
    after 1000 ->
            error(snapshot_event_timeout)
    end,

    %% Corrupt the latest checkpoint by deleting the snapshot.dat file but
    %% leaving the checkpoint directory intact.
    CorruptDir = filename:join(?config(checkpoint_dir, Config),
                               ra_lib:zpad_hex(2) ++ "_" ++ ra_lib:zpad_hex(165)),
    ok = file:delete(filename:join(CorruptDir, "snapshot.dat")),

    Recover = init_state(Config),
    %% The checkpoint isn't recovered and the directory is cleaned up.
    undefined = ra_snapshot:pending(Recover),
    undefined = ra_snapshot:current(Recover),
    {55, 2} = ra_snapshot:latest_checkpoint(Recover),
    {ok, Meta1, ?FUNCTION_NAME} = ra_snapshot:recover(Recover),
    false = filelib:is_dir(CorruptDir),

    ok.

init_recover_multi_corrupt(Config) ->
    State0 = init_state(Config),
    {error, no_current_snapshot} = ra_snapshot:recover(State0),

    %% Checkpoint at 55.
    CP1Meta = meta(55, 2, [node()]),
    {State1, [{bg_work, Fun, _}]} =
         ra_snapshot:begin_snapshot(CP1Meta, ?MODULE, ?FUNCTION_NAME,
                                    checkpoint, State0),
    Fun(),
    State2 = receive
                 {ra_log_event, {snapshot_written, IdxTerm1, Indexes, checkpoint}} ->
                     ra_snapshot:complete_snapshot(IdxTerm1, checkpoint, Indexes, State1)
             after 1000 ->
                     error(snapshot_event_timeout)
             end,

    %% Checkpoint at 165.
    CP2Meta = meta(165, 2, [node()]),
    {State3, [{bg_work, Fun2, _}]} =
         ra_snapshot:begin_snapshot(CP2Meta, ?MODULE, ?FUNCTION_NAME,
                                    checkpoint, State2),
    Fun2(),
    State4 = receive
                 {ra_log_event, {snapshot_written, IdxTerm2, Indexes2, checkpoint}} ->
                      ra_snapshot:complete_snapshot(IdxTerm2, checkpoint,
                                                    Indexes2, State3)
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
                     undefined, ?config(max_checkpoints, Config)).

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

%% ra_machine fakes
version() -> 1.
live_indexes(_) -> [].
