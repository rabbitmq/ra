%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term Broadcom refers to Broadcom Inc. and/or its subsidiaries.
%%
-module(ra_snapshot_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-export([
         ]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("src/ra.hrl").

-define(MACMOD, ?MODULE).

-define(MAGIC, "RASN").
-define(VERSION, 1).
%%%===================================================================
%%% Common Test callbacks
%%%===================================================================

all() ->
    [
     {group, tests}
    ].


all_tests() ->
    [init_empty,
     init_multi,
     take_snapshot,
     take_snapshot_crash,
     init_recover,
     init_recover_voter_status,
     init_recover_multi_corrupt,
     init_recover_corrupt,
     read_snapshot,
     accept_snapshot,
     abort_accept,
     accept_receives_snapshot_written_with_higher_index,
     accept_receives_snapshot_written_with_higher_index_2
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
    UId = ?config(uid, Config),
    State = init_state(Config),
    %% no pending, no current
    undefined = ra_snapshot:current(State),
    undefined = ra_snapshot:pending(State),
    undefined = ra_snapshot:last_index_for(UId),
    {error, no_current_snapshot} = ra_snapshot:recover(State),
    ok.

take_snapshot(Config) ->
    UId = ?config(uid, Config),
    State0 = init_state(Config),
    Meta = meta(55, 2, [node()]),
    MacState = ?FUNCTION_NAME,
    {State1, [{bg_work, Fun, _}]} =
         ra_snapshot:begin_snapshot(Meta, ?MACMOD,MacState, snapshot, State0),
    undefined = ra_snapshot:current(State1),
    Fun(),
    {{55, 2}, snapshot} = ra_snapshot:pending(State1),
    receive
        {ra_log_event,
         {snapshot_written, {55, 2} = IdxTerm, Indexes, snapshot}} ->
            State = ra_snapshot:complete_snapshot(IdxTerm, snapshot,
                                                  Indexes, State1),
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
    State0 = init_state(Config),
    Meta = meta(55, 2, [node()]),
    MacState = ?FUNCTION_NAME,
    {State1, [{bg_work, _Fun, ErrFun}]} =
         ra_snapshot:begin_snapshot(Meta, ?MACMOD, MacState, snapshot, State0),
    ErrFun({error, blah}),
    undefined = ra_snapshot:current(State1),
    {{55, 2}, snapshot}  = ra_snapshot:pending(State1),
    receive
        {ra_log_event, {snapshot_error, {55, 2} = IdxTerm, snapshot, Err}} ->
            State = ra_snapshot:handle_error(IdxTerm, Err, State1),
            undefined = ra_snapshot:pending(State),
            undefined = ra_snapshot:current(State),
            undefined = ra_snapshot:last_index_for(UId),

            %% assert there are no snapshots now
            ?assertEqual([], filelib:wildcard(filename:join(SnapDir, "*"))),
            ok
    after 10 ->
              ct:fail("no log event")
    end,

    %% if the snapshot process crashed we just have to consider the
    %% snapshot as faulty and clear it up

    ok.

init_recover(Config) ->
    UId = ?config(uid, Config),
    State0 = init_state(Config),
    Meta = meta(55, 2, [node()]),
    {State1, [{bg_work, Fun, _}]} =
         ra_snapshot:begin_snapshot(Meta, ?MACMOD, ?FUNCTION_NAME, snapshot, State0),
    Fun(),
    receive
        {ra_log_event, {snapshot_written, IdxTerm, Indexes, snapshot}} ->
            _ = ra_snapshot:complete_snapshot(IdxTerm, snapshot,
                                              Indexes, State1),
            ok
    after 1000 ->
              error(snapshot_event_timeout)
    end,

    %% open a new snapshot state to simulate a restart
    Recover = init_state(Config),
    %% ensure last snapshot is recovered
    %% it also needs to be validated as could have crashed mid write
    undefined = ra_snapshot:pending(Recover),
    {55, 2} = ra_snapshot:current(Recover),
    55 = ra_snapshot:last_index_for(UId),

    %% recover the meta data and machine state
    {ok, Meta, ?FUNCTION_NAME} = ra_snapshot:recover(Recover),
    ok.

init_recover_voter_status(Config) ->
    UId = ?config(uid, Config),
    State0 = init_state(Config),
    Meta = meta(55, 2, #{node() => #{voter_status => test}}),
    {State1, [{bg_work, Fun, _}]} =
         ra_snapshot:begin_snapshot(Meta, ?MACMOD, ?FUNCTION_NAME, snapshot, State0),
    Fun(),
    receive
        {ra_log_event, {snapshot_written, IdxTerm, Indexes, snapshot}} ->
            _ = ra_snapshot:complete_snapshot(IdxTerm, snapshot, Indexes, State1),
            ok
    after 1000 ->
              error(snapshot_event_timeout)
    end,

    %% open a new snapshot state to simulate a restart
    Recover = init_state(Config),
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
    State0 = init_state(Config),
    Meta1 = meta(55, 2, [node()]),
    Meta2 = meta(165, 2, [node()]),
    {State1, [{bg_work, Fun, _}]} =
        ra_snapshot:begin_snapshot(Meta1, ?MACMOD, ?FUNCTION_NAME,
                                   snapshot, State0),
    %% simulate ra worker execution
    Fun(),
    receive
        {ra_log_event, {snapshot_written, IdxTerm, Indexes, snapshot}} ->
            State2 = ra_snapshot:complete_snapshot(IdxTerm, snapshot, Indexes, State1),
            {State3, [{bg_work, Fun2, _}]} =
                ra_snapshot:begin_snapshot(Meta2, ?MACMOD, ?FUNCTION_NAME,
                                           snapshot, State2),
            {{165, 2}, snapshot} = ra_snapshot:pending(State3),
            {55, 2} = ra_snapshot:current(State3),
            55 = ra_snapshot:last_index_for(UId),
            Fun2(),
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
    Recover = init_state(Config),
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
    State0 = init_state(Config),
    Meta1 = meta(55, 2, [node()]),
    Meta2 = meta(165, 2, [node()]),
    {State1, [{bg_work, Fun, _}]} =
        ra_snapshot:begin_snapshot(Meta1, ?MACMOD, ?FUNCTION_NAME,
                                   snapshot, State0),
    Fun(),
    receive
        {ra_log_event, {snapshot_written, IdxTerm, Indexes, snapshot}} ->
            State2 = ra_snapshot:complete_snapshot(IdxTerm, snapshot, Indexes, State1),
            {State3, [{bg_work, Fun2, _}]} =
                ra_snapshot:begin_snapshot(Meta2, ?MACMOD, ?FUNCTION_NAME,
                                           snapshot, State2),
            {{165, 2}, snapshot} = ra_snapshot:pending(State3),
            {55, 2} = ra_snapshot:current(State3),
            55 = ra_snapshot:last_index_for(UId),
            Fun2(),
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
    Recover = init_state(Config),
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
    State0 = init_state(Config),
    {State1, [{bg_work, Fun, _}]} =
        ra_snapshot:begin_snapshot(Meta, ?MACMOD, ?FUNCTION_NAME,
                                   snapshot, State0),
    Fun(),
    _ = receive
                 {ra_log_event, {snapshot_written, IdxTerm, Indexes, snapshot}} ->
                     ra_snapshot:complete_snapshot(IdxTerm, snapshot, Indexes, State1)
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
    Recover = init_state(Config),
    %% ensure the corrupt snapshot isn't recovered
    undefined = ra_snapshot:pending(Recover),
    undefined = ra_snapshot:current(Recover),
    undefined = ra_snapshot:last_index_for(UId),
    %% corrupt dir should be cleared up
    false = filelib:is_dir(Corrupt),
    ok.

read_snapshot(Config) ->
    State0 = init_state(Config),
    Meta = meta(55, 2, [node()]),
    MacRef = crypto:strong_rand_bytes(1024 * 4),
    {State1, [{bg_work, Fun, _}]} =
        ra_snapshot:begin_snapshot(Meta, ?MACMOD, MacRef, snapshot, State0),
    Fun(),
    State = receive
                {ra_log_event, {snapshot_written, IdxTerm, Indexes, snapshot}} ->
                    ra_snapshot:complete_snapshot(IdxTerm, snapshot, Indexes, State1)
            after 1000 ->
                      error(snapshot_event_timeout)
            end,
    Context = #{},

    {ok, Meta, InitChunkState} = ra_snapshot:begin_read(State, Context),

    <<_:32/integer, Data/binary>> = read_all_chunks(InitChunkState, State, 1024, <<>>),
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
    State0 = init_state(Config),
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
    S2 = ra_snapshot:accept_chunk(A, 1, S1),
    S3 = ra_snapshot:accept_chunk(B, 2, S2),
    S4 = ra_snapshot:accept_chunk(C, 3, S3),
    S5 = ra_snapshot:accept_chunk(D, 4, S4),
    Machine = {machine, ?MODULE, #{}},
    {S,_, _, _}  = ra_snapshot:complete_accept(E, 5, Machine, S5),

    undefined = ra_snapshot:accepting(S),
    undefined = ra_snapshot:pending(S),
    {55, 2} = ra_snapshot:current(S),
    55 = ra_snapshot:last_index_for(UId),
    ok.

abort_accept(Config) ->
    UId = ?config(uid, Config),
    State0 = init_state(Config),
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
    S2 = ra_snapshot:accept_chunk(A, 1, S1),
    S3 = ra_snapshot:accept_chunk(B, 2, S2),
    S = ra_snapshot:abort_accept(S3),
    undefined = ra_snapshot:accepting(S),
    undefined = ra_snapshot:pending(S),
    undefined = ra_snapshot:current(S),
    undefined = ra_snapshot:last_index_for(UId),
    ok.

accept_receives_snapshot_written_with_higher_index(Config) ->
    UId = ?config(uid, Config),
    State0 = init_state(Config),
    MetaLow = meta(55, 2, [node()]),
    MetaHigh = meta(165, 2, [node()]),
    MetaRemoteBin = term_to_binary(MetaHigh),
    %% begin a local snapshot
    {State1, [{bg_work, Fun, _}]} =
        ra_snapshot:begin_snapshot(MetaLow, ?MACMOD, ?FUNCTION_NAME, snapshot, State0),
    Fun(),
    MacRef = crypto:strong_rand_bytes(1024),
    MacBin = term_to_binary(MacRef),
    Crc = erlang:crc32([<<(size(MetaRemoteBin)):32/unsigned>>,
                        MetaRemoteBin,
                        MacBin]),
    %% split into 1024 max byte chunks
    <<A:1024/binary,
      B/binary>> = <<Crc:32/integer, MacBin/binary>>,

    %% then begin an accept for a higher index
    {ok, State2} = ra_snapshot:begin_accept(MetaHigh, State1),
    {165, 2} = ra_snapshot:accepting(State2),
    State3 = ra_snapshot:accept_chunk(A, 1, State2),

    %% then the snapshot written event is received
    receive
        {ra_log_event, {snapshot_written, {55, 2} = IdxTerm, Indexes, snapshot}} ->
            State4 = ra_snapshot:complete_snapshot(IdxTerm, snapshot, Indexes, State3),
            undefined = ra_snapshot:pending(State4),
            {55, 2} = ra_snapshot:current(State4),
            55 = ra_snapshot:last_index_for(UId),
            %% then accept the last chunk
            Machine = {machine, ?MODULE, #{}},
            {State, _, _, _} = ra_snapshot:complete_accept(B, 2, Machine, State4),
            undefined = ra_snapshot:accepting(State),
            {165, 2} = ra_snapshot:current(State),
            ok
    after 1000 ->
              error(snapshot_event_timeout)
    end,
    ok.

accept_receives_snapshot_written_with_higher_index_2(Config) ->
    UId = ?config(uid, Config),
    State0 = init_state(Config),
    MetaLow = meta(55, 2, [node()]),
    MetaHigh = meta(165, 2, [node()]),
    %% begin a local snapshot
    {State1, [{bg_work, Fun, _}]} =
        ra_snapshot:begin_snapshot(MetaLow, ?MACMOD, ?FUNCTION_NAME,
                                   snapshot, State0),
    Fun(),
    MacState = crypto:strong_rand_bytes(1024),
    MetaBin = term_to_binary(MetaHigh),
    IOVec = term_to_iovec(MacState),
    Data = [<<(size(MetaBin)):32/unsigned>>, MetaBin | IOVec],
    Checksum = erlang:crc32(Data),
    MacBin = iolist_to_binary([<<?MAGIC,
                                    ?VERSION:8/unsigned,
                                    Checksum:32/integer>>,Data]),
    %% split into 1024 max byte chunks
    <<A:1024/binary,
      B/binary>> = MacBin,

    %% then begin an accept for a higher index
    {ok, State2} = ra_snapshot:begin_accept(MetaHigh, State1),
    {165, 2} = ra_snapshot:accepting(State2),
    State3 = ra_snapshot:accept_chunk(A, 1, State2),
    {165, 2} = ra_snapshot:accepting(State3),

    {State4, _, _, _} = ra_snapshot:complete_accept(B, 2, {machine, ?MODULE, #{}},
                                                    State3),
    undefined = ra_snapshot:accepting(State4),
    {165, 2} = ra_snapshot:current(State4),
    undefined = ra_snapshot:pending(State4),
    %% then the snapshot written event is received after the higher index
    %% has been received
    receive
        {ra_log_event, {snapshot_written, {55, 2} = IdxTerm, Indexes, snapshot}} ->
            State5 = ra_snapshot:complete_snapshot(IdxTerm, snapshot, Indexes, State4),
            undefined = ra_snapshot:pending(State5),
            {165, 2} = ra_snapshot:current(State5),
            165 = ra_snapshot:last_index_for(UId),
            %% then accept the last chunk
            ok
    after 1000 ->
              error(snapshot_event_timeout)
    end,
    ok.

init_state(Config) ->
    ra_snapshot:init(?config(uid, Config), ra_log_snapshot,
                     ?config(snap_dir, Config),
                     ?config(checkpoint_dir, Config),
                     undefined, ?config(max_checkpoints, Config)).

meta(Idx, Term, Cluster) ->
    #{index => Idx,
      term => Term,
      cluster => Cluster,
      machine_version => 1}.

%% ra_machine fakes
version() -> 1.
live_indexes(_) -> [].
