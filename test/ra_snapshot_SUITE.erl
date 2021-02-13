%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2017-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%
-module(ra_snapshot_SUITE).

-compile(export_all).

-export([
         ]).

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
     init_multi,
     take_snapshot,
     take_snapshot_crash,
     init_recover,
     init_recover_multi_corrupt,
     init_recover_corrupt,
     read_snapshot,
     accept_snapshot,
     abort_accept,
     accept_receives_snapshot_written_with_lower_index
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
    ok = ra_lib:make_dir(SnapDir),
    [{uid, ra_lib:to_binary(TestCase)},
     {snap_dir, SnapDir} | Config].

end_per_testcase(_TestCase, _Config) ->
    ok.

%%%===================================================================
%%% Test cases
%%%===================================================================

init_empty(Config) ->
    UId = ?config(uid, Config),
    State = ra_snapshot:init(UId, ?MODULE, ?config(snap_dir, Config)),
    %% no pending, no current
    undefined = ra_snapshot:current(State),
    undefined = ra_snapshot:pending(State),
    undefined = ra_snapshot:last_index_for(UId),
    {error, no_current_snapshot} = ra_snapshot:recover(State),
    ok.

take_snapshot(Config) ->
    UId = ?config(uid, Config),
    State0 = ra_snapshot:init(UId, ra_log_snapshot,
                              ?config(snap_dir, Config)),
    Meta = meta(55, 2, [node()]),
    MacRef = ?FUNCTION_NAME,
    {State1, [{monitor, process, snapshot_writer, Pid}]} =
         ra_snapshot:begin_snapshot(Meta, MacRef, State0),
    undefined = ra_snapshot:current(State1),
    {Pid, {55, 2}} = ra_snapshot:pending(State1),
    receive
        {ra_log_event, {snapshot_written, {55, 2} = IdxTerm}} ->
            State = ra_snapshot:complete_snapshot(IdxTerm, State1),
            undefined = ra_snapshot:pending(State),
            {55, 2} = ra_snapshot:current(State),
            55 = ra_snapshot:last_index_for(UId),
            ok
    after 1000 ->
              error(snapshot_event_timeout)
    end,
    ok.

take_snapshot_crash(Config) ->
    UId = ?config(uid, Config),
    SnapDir = ?config(snap_dir, Config),
    State0 = ra_snapshot:init(UId, ra_log_snapshot, SnapDir),
    Meta = meta(55, 2, [node()]),
    MacRef = ?FUNCTION_NAME,
    {State1, [{monitor, process, snapshot_writer, Pid}]} =
         ra_snapshot:begin_snapshot(Meta, MacRef, State0),
    undefined = ra_snapshot:current(State1),
    {Pid, {55, 2}}  = ra_snapshot:pending(State1),
    receive
        {ra_log_event, _} ->
            %% just pretend the snapshot event didn't happen
            %% and the process instead crashed
            ok
    after 10 -> ok
    end,

    State = ra_snapshot:handle_down(Pid, it_crashed_dawg, State1),
    %% if the snapshot process crashed we just have to consider the
    %% snapshot as faulty and clear it up
    undefined = ra_snapshot:pending(State),
    undefined = ra_snapshot:current(State),
    undefined = ra_snapshot:last_index_for(UId),

    %% assert there are no snapshots now
    ?assertEqual([], filelib:wildcard(filename:join(SnapDir, "*"))),

    ok.

init_recover(Config) ->
    UId = ?config(uid, Config),
    State0 = ra_snapshot:init(UId, ra_log_snapshot,
                              ?config(snap_dir, Config)),
    Meta = meta(55, 2, [node()]),
    {State1, [{monitor, process, snapshot_writer, _}]} =
         ra_snapshot:begin_snapshot(Meta, ?FUNCTION_NAME, State0),
    receive
        {ra_log_event, {snapshot_written, IdxTerm}} ->
            _ = ra_snapshot:complete_snapshot(IdxTerm, State1),
            ok
    after 1000 ->
              error(snapshot_event_timeout)
    end,

    %% open a new snapshot state to simulate a restart
    Recover = ra_snapshot:init(UId, ra_log_snapshot,
                               ?config(snap_dir, Config)),
    %% ensure last snapshot is recovered
    %% it also needs to be validated as could have crashed mid write
    undefined = ra_snapshot:pending(Recover),
    {55, 2} = ra_snapshot:current(Recover),
    55 = ra_snapshot:last_index_for(UId),

    %% recover the meta data and machine state
    {ok, Meta, ?FUNCTION_NAME} = ra_snapshot:recover(Recover),
    ok.

init_multi(Config) ->
    UId = ?config(uid, Config),
    State0 = ra_snapshot:init(UId, ra_log_snapshot,
                              ?config(snap_dir, Config)),
    Meta1 = meta(55, 2, [node()]),
    Meta2 = meta(165, 2, [node()]),
    {State1, _} = ra_snapshot:begin_snapshot(Meta1, ?FUNCTION_NAME, State0),
    receive
        {ra_log_event, {snapshot_written, IdxTerm}} ->
            State2 = ra_snapshot:complete_snapshot(IdxTerm, State1),
            {State3, _} = ra_snapshot:begin_snapshot(Meta2, ?FUNCTION_NAME,
                                                     State2),
            {_, {165, 2}} = ra_snapshot:pending(State3),
            {55, 2} = ra_snapshot:current(State3),
            55 = ra_snapshot:last_index_for(UId),
            receive
                {ra_log_event, _} ->
                    %% don't complete snapshot
                    ok
            after 1000 ->
                      error(snapshot_event_timeout)
            end
    after 1000 ->
              error(snapshot_event_timeout)
    end,

    %% open a new snapshot state to simulate a restart
    Recover = ra_snapshot:init(UId, ra_log_snapshot,
                               ?config(snap_dir, Config)),
    %% ensure last snapshot is recovered
    %% it also needs to be validated as could have crashed mid write
    undefined = ra_snapshot:pending(Recover),
    {165, 2} = ra_snapshot:current(Recover),
    165 = ra_snapshot:last_index_for(UId),

    %% recover the meta data and machine state
    {ok, Meta2, ?FUNCTION_NAME} = ra_snapshot:recover(Recover),
    ok.

init_recover_multi_corrupt(Config) ->
    UId = ?config(uid, Config),
    SnapsDir = ?config(snap_dir, Config),
    State0 = ra_snapshot:init(UId, ra_log_snapshot, SnapsDir),
    Meta1 = meta(55, 2, [node()]),
    Meta2 = meta(165, 2, [node()]),
    {State1, _} = ra_snapshot:begin_snapshot(Meta1, ?FUNCTION_NAME, State0),
    receive
        {ra_log_event, {snapshot_written, IdxTerm}} ->
            State2 = ra_snapshot:complete_snapshot(IdxTerm, State1),
            {State3, _} = ra_snapshot:begin_snapshot(Meta2, ?FUNCTION_NAME,
                                                     State2),
            {_, {165, 2}} = ra_snapshot:pending(State3),
            {55, 2} = ra_snapshot:current(State3),
            55 = ra_snapshot:last_index_for(UId),
            receive
                {ra_log_event, _} ->
                    %% don't complete snapshot
                    ok
            after 1000 ->
                      error(snapshot_event_timeout)
            end
    after 1000 ->
              error(snapshot_event_timeout)
    end,
    %% corrupt the latest snapshot
    Corrupt = filename:join(SnapsDir,
                            ra_lib:zpad_hex(2) ++ "_" ++ ra_lib:zpad_hex(165)),
    ok = file:delete(filename:join(Corrupt, "snapshot.dat")),

    %% open a new snapshot state to simulate a restart
    Recover = ra_snapshot:init(UId, ra_log_snapshot,
                               ?config(snap_dir, Config)),
    %% ensure last snapshot is recovered
    %% it also needs to be validated as could have crashed mid write
    undefined = ra_snapshot:pending(Recover),
    {55, 2} = ra_snapshot:current(Recover),
    55 = ra_snapshot:last_index_for(UId),
    false = filelib:is_dir(Corrupt),

    %% recover the meta data and machine state
    {ok, Meta1, ?FUNCTION_NAME} = ra_snapshot:recover(Recover),
    ok.

init_recover_corrupt(Config) ->
    %% recovery should skip corrupt snapshots,
    %% e.g. empty snapshot directories
    UId = ?config(uid, Config),
    Meta = meta(55, 2, [node()]),
    SnapsDir = ?config(snap_dir, Config),
    State0 = ra_snapshot:init(UId, ra_log_snapshot, SnapsDir),
    {State1, _} = ra_snapshot:begin_snapshot(Meta, ?FUNCTION_NAME, State0),
    _ = receive
                 {ra_log_event, {snapshot_written, IdxTerm}} ->
                     ra_snapshot:complete_snapshot(IdxTerm, State1)
             after 1000 ->
                       error(snapshot_event_timeout)
             end,

    %% delete the snapshot file but leave the current directory
    Corrupt = filename:join(SnapsDir,
                            ra_lib:zpad_hex(2) ++ "_" ++ ra_lib:zpad_hex(55)),
    ok = file:delete(filename:join(Corrupt, "snapshot.dat")),

    %% clear out ets table
    ets:delete_all_objects(ra_log_snapshot_state),
    %% open a new snapshot state to simulate a restart
    Recover = ra_snapshot:init(UId, ra_log_snapshot,
                               ?config(snap_dir, Config)),
    %% ensure the corrupt snapshot isn't recovered
    undefined = ra_snapshot:pending(Recover),
    undefined = ra_snapshot:current(Recover),
    undefined = ra_snapshot:last_index_for(UId),
    %% corrupt dir should be cleared up
    false = filelib:is_dir(Corrupt),
    ok.

read_snapshot(Config) ->
    UId = ?config(uid, Config),
    State0 = ra_snapshot:init(UId, ra_log_snapshot,
                              ?config(snap_dir, Config)),
    Meta = meta(55, 2, [node()]),
    MacRef = crypto:strong_rand_bytes(1024 * 4),
    {State1, _} =
         ra_snapshot:begin_snapshot(Meta, MacRef, State0),
     State = receive
                 {ra_log_event, {snapshot_written, IdxTerm}} ->
                     ra_snapshot:complete_snapshot(IdxTerm, State1)
             after 1000 ->
                       error(snapshot_event_timeout)
             end,

    {ok, Meta, InitChunkState} = ra_snapshot:begin_read(State),

    <<_Crc:32/integer, Data/binary>> = read_all_chunks(InitChunkState, State, 1024, <<>>),
    ?assertEqual(MacRef, binary_to_term(Data)),

    ok.

read_all_chunks(ChunkState, State, Size, Acc) ->
    case ra_snapshot:read_chunk(ChunkState, Size, State) of
        {ok, Chunk, {next, ChunkState1}} ->
            read_all_chunks(ChunkState1, State, Size, <<Acc/binary, Chunk/binary>>);
        {ok, Chunk, last} ->
            <<Acc/binary, Chunk/binary>>
    end.

accept_snapshot(Config) ->
    UId = ?config(uid, Config),
    State0 = ra_snapshot:init(UId, ra_log_snapshot,
                              ?config(snap_dir, Config)),
    Meta = meta(55, 2, [node()]),
    MetaBin = term_to_binary(Meta),
    MacRef = crypto:strong_rand_bytes(1024 * 4),
    MacBin = term_to_binary(MacRef),
    Crc = erlang:crc32([<<(size(MetaBin)):32/unsigned>>,
                        MetaBin,
                        MacBin]),
    %% split into 1024 max byte chunks
    <<A:1024/binary,
      B:1024/binary,
      C:1024/binary,
      D:1024/binary,
      E/binary>> = <<Crc:32/integer, MacBin/binary>>,

    undefined = ra_snapshot:accepting(State0),
    {ok, S1} = ra_snapshot:begin_accept(Meta, State0),
    {55, 2} = ra_snapshot:accepting(S1),
    {ok, S2} = ra_snapshot:accept_chunk(A, 1, next, S1),
    {ok, S3} = ra_snapshot:accept_chunk(B, 2, next, S2),
    {ok, S4} = ra_snapshot:accept_chunk(C, 3, next, S3),
    {ok, S5} = ra_snapshot:accept_chunk(D, 4, next, S4),
    {ok, S}  = ra_snapshot:accept_chunk(E, 5, last, S5),

    undefined = ra_snapshot:accepting(S),
    undefined = ra_snapshot:pending(S),
    {55, 2} = ra_snapshot:current(S),
    55 = ra_snapshot:last_index_for(UId),
    ok.

abort_accept(Config) ->
    UId = ?config(uid, Config),
    State0 = ra_snapshot:init(UId, ra_log_snapshot,
                              ?config(snap_dir, Config)),
    Meta = meta(55, 2, [node()]),
    MacRef = crypto:strong_rand_bytes(1024 * 4),
    MacBin = term_to_binary(MacRef),
    %% split into 1024 max byte chunks
    <<A:1024/binary,
      B:1024/binary,
      _:1024/binary,
      _:1024/binary,
      _/binary>> = MacBin,

    undefined = ra_snapshot:accepting(State0),
    {ok, S1} = ra_snapshot:begin_accept(Meta, State0),
    {55, 2} = ra_snapshot:accepting(S1),
    {ok, S2} = ra_snapshot:accept_chunk(A, 1, next, S1),
    {ok, S3} = ra_snapshot:accept_chunk(B, 2, next, S2),
    S = ra_snapshot:abort_accept(S3),
    undefined = ra_snapshot:accepting(S),
    undefined = ra_snapshot:pending(S),
    undefined = ra_snapshot:current(S),
    undefined = ra_snapshot:last_index_for(UId),
    ok.

accept_receives_snapshot_written_with_lower_index(Config) ->
    UId = ?config(uid, Config),
    SnapDir = ?config(snap_dir, Config),
    State0 = ra_snapshot:init(UId, ra_log_snapshot, SnapDir),
    MetaLocal = meta(55, 2, [node()]),
    MetaRemote = meta(165, 2, [node()]),
    MetaRemoteBin = term_to_binary(MetaRemote),
    %% begin a local snapshot
    {State1, _} = ra_snapshot:begin_snapshot(MetaLocal, ?FUNCTION_NAME, State0),
    MacRef = crypto:strong_rand_bytes(1024),
    MacBin = term_to_binary(MacRef),
    Crc = erlang:crc32([<<(size(MetaRemoteBin)):32/unsigned>>,
                        MetaRemoteBin,
                        MacBin]),
    %% split into 1024 max byte chunks
    <<A:1024/binary,
      B/binary>> = <<Crc:32/integer, MacBin/binary>>,

    %% then begin an accept for a higher index
    {ok, State2} = ra_snapshot:begin_accept(MetaRemote, State1),
    {165, 2} = ra_snapshot:accepting(State2),
    {ok, State3} = ra_snapshot:accept_chunk(A, 1, next, State2),

    %% then the snapshot written event is received
    receive
        {ra_log_event, {snapshot_written, {55, 2} = IdxTerm}} ->
            State4 = ra_snapshot:complete_snapshot(IdxTerm, State3),
            undefined = ra_snapshot:pending(State4),
            {55, 2} = ra_snapshot:current(State4),
            55 = ra_snapshot:last_index_for(UId),
            %% then accept the last chunk
            {ok, State} = ra_snapshot:accept_chunk(B, 2, last, State4),
            undefined = ra_snapshot:accepting(State),
            {165, 2} = ra_snapshot:current(State),
            ok
    after 1000 ->
              error(snapshot_event_timeout)
    end,
    ok.

accept_receives_snapshot_written_with_higher_index(Config) ->
    UId = ?config(uid, Config),
    SnapDir = ?config(snap_dir, Config),
    State0 = ra_snapshot:init(UId, ra_log_snapshot, SnapDir),
    MetaRemote = meta(55, 2, [node()]),
    MetaLocal = meta(165, 2, [node()]),
    %% begin a local snapshot
    {State1, _} = ra_snapshot:begin_snapshot(MetaLocal, ?FUNCTION_NAME, State0),
    MacRef = crypto:strong_rand_bytes(1024),
    MacBin = term_to_binary(MacRef),
    %% split into 1024 max byte chunks
    <<A:1024/binary,
      B/binary>> = MacBin,

    %% then begin an accept for a higher index
    {ok, State2} = ra_snapshot:begin_accept(MetaRemote, State1),
    undefined = ra_snapshot:accepting(State2),
    {ok, State3} = ra_snapshot:accept_chunk(A, 1, next, State2),
    undefined = ra_snapshot:accepting(State3),

    %% then the snapshot written event is received
    receive
        {ra_log_event, {snapshot_written, {55, 2} = IdxTerm}} ->
            State4 = ra_snapshot:complete_snapshot(IdxTerm, State3),
            undefined = ra_snapshot:pending(State4),
            {55, 2} = ra_snapshot:current(State4),
            55 = ra_snapshot:last_index_for(UId),
            %% then accept the last chunk
            {ok, State} = ra_snapshot:accept_chunk(B, 2, last, State4),
            undefined = ra_snapshot:accepting(State),
            {165, 2} = ra_snapshot:current(State),
            ok
    after 1000 ->
              error(snapshot_event_timeout)
    end,
    ok.

meta(Idx, Term, Cluster) ->
    #{index => Idx,
      term => Term,
      cluster => Cluster,
      machine_version => 1}.
