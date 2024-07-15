%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term Broadcom refers to Broadcom Inc. and/or its subsidiaries.
%%
-module(ra_snapshot).

-include("ra.hrl").

-type file_err() :: ra_lib:file_err().

%% alias
-type meta() :: snapshot_meta().

-export([
         recover/1,
         read_meta/2,
         begin_read/2,
         read_chunk/3,
         delete/2,

         init/6,
         init_ets/0,
         current/1,
         pending/1,
         accepting/1,
         directory/2,
         last_index_for/1,

         begin_snapshot/4,
         promote_checkpoint/2,
         complete_snapshot/3,

         begin_accept/2,
         accept_chunk/4,
         abort_accept/1,

         context/2,

         handle_down/3,
         current_snapshot_dir/1,

         latest_checkpoint/1,

         take_older_checkpoints/2,
         take_extra_checkpoints/1
        ]).

-type effect() :: {monitor, process, snapshot_writer, pid()}.

-type kind() :: snapshot | checkpoint.

-type checkpoint() :: ra_idxterm().

-export_type([
              meta/0,
              file_err/0,
              effect/0,
              chunk_flag/0,
              kind/0,
              checkpoint/0
             ]).

-record(accept, {%% the next expected chunk
                 next = 1 :: non_neg_integer(),
                 state :: term(),
                 idxterm :: ra_idxterm()}).

-record(?MODULE,
        {uid :: ra_uid(),
         counter :: undefined | counters:counters_ref(),
         module :: module(),
         %% typically <data_dir>/snapshots
         %% snapshot subdirs are store below
         %% this as <data_dir>/snapshots/Term_Index
         snapshot_directory :: file:filename(),
         %% <data_dir>/checkpoints
         %% like snapshots, these are also stored in subdirs
         %% as <data_dir>/checkpoints/Term_Index
         checkpoint_directory :: file:filename(),
         pending :: option({pid(), ra_idxterm(), kind()}),
         accepting :: option(#accept{}),
         current :: option(ra_idxterm()),
         checkpoints = [] :: list(checkpoint()),
         max_checkpoints :: pos_integer()}).

-define(ETSTBL, ra_log_snapshot_state).

-opaque state() :: #?MODULE{}.

-export_type([state/0]).

-optional_callbacks([context/0]).

%% Side effect function
%% Turn the current state into immutable reference.
-callback prepare(Index :: ra_index(),
                  State :: term()) ->
    Ref :: term().

%% Saves snapshot from external state to disk.
%% Runs in a separate process.
%% External storage should be available to read
%% `Sync' suggests whether the file should be synchronized with `fsync(1)'.
-callback write(Location :: file:filename(),
                Meta :: meta(),
                Ref :: term(),
                Sync :: boolean()) ->
    ok |
    {ok, Bytes :: non_neg_integer()} |
    {error, file_err() | term()}.

%% Synchronizes the snapshot to disk.
-callback sync(Location :: file:filename()) ->
    ok |
    {error, file_err() | term()}.


%% Read the snapshot metadata and initialise a read state used in read_chunk/1
%% The read state should contain all the information required to read a chunk
%% The Context is the map returned by the context/0 callback
%% This can be used to inform the sender of receive capabilities.
-callback begin_read(Location :: file:filename(), Context :: map()) ->
    {ok, Meta :: meta(), ReadState :: term()}
    | {error, term()}.

%% Read a chunk of data from the snapshot using the read state
%% Returns a binary chunk of data and a continuation state
-callback read_chunk(ReadState,
                     ChunkSizeBytes :: non_neg_integer(),
                     Location :: file:filename()) ->
    {ok, Chunk :: term(), {next, ReadState} | last} | {error, term()}.

%% begin a stateful snapshot acceptance process
-callback begin_accept(SnapDir :: file:filename(),
                       Meta :: meta()) ->
    {ok, AcceptState :: term()} | {error, term()}.

%% accept a chunk of data
-callback accept_chunk(Chunk :: term(),
                       AcceptState :: term()) ->
    {ok, AcceptState :: term()} | {error, term()}.

%% accept the last chunk of data
-callback complete_accept(Chunk :: term(),
                          AcceptState :: term()) ->
    ok | {error, term()}.

%% Side-effect function
%% Recover machine state from file
-callback recover(Location :: file:filename()) ->
    {ok, Meta :: meta(), State :: term()} | {error, term()}.

%% validate the integrity of the snapshot
-callback validate(Location :: file:filename()) ->
    ok | {error, term()}.

%% Only read meta data from snapshot
-callback read_meta(Location :: file:filename()) ->
    {ok, meta()} |
    {error, invalid_format |
            {invalid_version, integer()} |
            checksum_error |
            file_err() |
            term()}.

-callback context() -> map().

-spec init(ra_uid(), module(), file:filename(), file:filename(),
           undefined | counters:counters_ref(), pos_integer()) ->
    state().
init(UId, Module, SnapshotsDir, CheckpointDir, Counter, MaxCheckpoints) ->
    State = #?MODULE{uid = UId,
                     counter = Counter,
                     module = Module,
                     snapshot_directory = SnapshotsDir,
                     checkpoint_directory = CheckpointDir,
                     max_checkpoints = MaxCheckpoints},
    State1 = find_snapshots(State),
    find_checkpoints(State1).

find_snapshots(#?MODULE{uid = UId,
                        module = Module,
                        snapshot_directory = SnapshotsDir} = State) ->
    true = ra_lib:is_dir(SnapshotsDir),
    {ok, Snaps0} = prim_file:list_dir(SnapshotsDir),
    Snaps = lists:reverse(lists:sort(Snaps0)),
    %% /snapshots/term_index/
    case pick_first_valid(UId, Module, SnapshotsDir, Snaps) of
        undefined ->
            ok = delete_snapshots(SnapshotsDir, Snaps),
            %% initialise snapshots table even if no snapshots have been taken
            %% this ensure these is an entry when the WAL queries it
            true = ets:insert(?ETSTBL, {UId, -1}),
            State;
        Current0 ->
            Current = filename:join(SnapshotsDir, Current0),
            {ok, #{index := Idx, term := Term}} = Module:read_meta(Current),
            true = ets:insert(?ETSTBL, {UId, Idx}),

            ok = delete_snapshots(SnapshotsDir, lists:delete(Current0, Snaps)),
            %% delete old snapshots if any
            State#?MODULE{current = {Idx, Term}}
    end.

delete_snapshots(Dir, Snaps) ->
    Old = [filename:join(Dir, O) || O <- Snaps],
    lists:foreach(fun ra_lib:recursive_delete/1, Old),
    ok.

pick_first_valid(_, _, _, []) ->
    undefined;
pick_first_valid(UId, Mod, Dir, [S | Rem]) ->
    case Mod:validate(filename:join(Dir, S)) of
        ok -> S;
        Err ->
            ?INFO("ra_snapshot: ~ts: skipping ~s as did not validate. Err: ~w",
                  [UId, S, Err]),
            pick_first_valid(UId, Mod, Dir, Rem)
    end.

find_checkpoints(#?MODULE{uid = UId,
                          module = Module,
                          current = Current,
                          checkpoint_directory = CheckpointDir} = State) ->
    true = ra_lib:is_dir(CheckpointDir),
    CurrentIdx = case Current of
                     undefined ->
                         -1;
                     {I, _} ->
                         I
                 end,
    {ok, CPFiles0} = prim_file:list_dir(CheckpointDir),
    CPFiles = lists:reverse(lists:sort(CPFiles0)),
    Checkpoints =
        lists:filtermap(
          fun(File) ->
                  CP = filename:join(CheckpointDir, File),
                  case Module:validate(CP) of
                      ok ->
                          {ok, #{index := Idx, term := Term}} =
                              Module:read_meta(CP),
                          case Idx > CurrentIdx of
                              true ->
                                  {true, {Idx, Term}};
                              false ->
                                  ?INFO("ra_snapshot: ~ts: removing "
                                        "checkpoint ~s as was older than the "
                                        "current snapshot.",
                                        [UId, CP]),
                                  delete(CheckpointDir, {Idx, Term}),
                                  false
                          end;
                      Err ->
                          ?INFO("ra_snapshot: ~ts: removing checkpoint ~s as "
                                "did not validate. Err: ~w",
                                [UId, CP, Err]),
                          ra_lib:recursive_delete(CP),
                          false
                  end
          end, CPFiles),
    State#?MODULE{checkpoints = Checkpoints}.

-spec init_ets() -> ok.
init_ets() ->
    TableFlags = [set,
                  named_table,
                  {read_concurrency, true},
                  {write_concurrency, true},
                  public],
    _ = ets:new(?ETSTBL, TableFlags),
    ok.

-spec current(state()) -> option(ra_idxterm()).
current(#?MODULE{current = Current}) -> Current.

-spec latest_checkpoint(state()) -> option(checkpoint()).
latest_checkpoint(#?MODULE{checkpoints = [Current | _]}) -> Current;
latest_checkpoint(#?MODULE{checkpoints = _}) -> undefined.

-spec pending(state()) -> option({pid(), ra_idxterm(), kind()}).
pending(#?MODULE{pending = Pending}) ->
    Pending.

-spec accepting(state()) -> option(ra_idxterm()).
accepting(#?MODULE{accepting = undefined}) ->
    undefined;
accepting(#?MODULE{accepting = #accept{idxterm = Accepting}}) ->
    Accepting.

-spec directory(state(), kind()) -> file:filename().
directory(#?MODULE{snapshot_directory = Dir}, snapshot) -> Dir;
directory(#?MODULE{checkpoint_directory = Dir}, checkpoint) -> Dir.

-spec last_index_for(ra_uid()) -> option(ra_index()).
last_index_for(UId) ->
    case ets:lookup(?ETSTBL, UId) of
        [{_, Index}] when Index >= 0 ->
            Index;
        _ ->
            undefined
    end.

-spec begin_snapshot(meta(), ReleaseCursorRef :: term(), kind(), state()) ->
    {state(), [effect()]}.
begin_snapshot(#{index := Idx, term := Term} = Meta, MacRef, SnapKind,
               #?MODULE{module = Mod,
                        counter = Counter,
                        snapshot_directory = SnapshotDir,
                        checkpoint_directory = CheckpointDir} = State) ->
    {CounterIdx, Dir} =
        case SnapKind of
            snapshot ->
                {?C_RA_LOG_SNAPSHOT_BYTES_WRITTEN, SnapshotDir};
            checkpoint ->
                {?C_RA_LOG_CHECKPOINT_BYTES_WRITTEN, CheckpointDir}
        end,
    %% Snapshots must be fsync'd but checkpoints are OK to not sync.
    %% Checkpoints are fsync'd before promotion instead.
    Sync = SnapKind =:= snapshot,
    %% create directory for this snapshot
    SnapDir = make_snapshot_dir(Dir, Idx, Term),
    %% call prepare then write_snapshot
    %% This needs to be called in the current process to "lock" potentially
    %% mutable machine state
    Ref = Mod:prepare(Meta, MacRef),
    %% write the snapshot in a separate process
    Self = self(),
    Pid = spawn(fun () ->
                        ok = ra_lib:make_dir(SnapDir),
                        case Mod:write(SnapDir, Meta, Ref, Sync) of
                            ok -> ok;
                            {ok, BytesWritten} ->
                                counters_add(Counter, CounterIdx,
                                             BytesWritten),
                                ok
                        end,
                        Self ! {ra_log_event,
                                {snapshot_written, {Idx, Term}, SnapKind}},
                        ok
                end),

    %% record snapshot in progress
    %% emit an effect that monitors the current snapshot attempt
    {State#?MODULE{pending = {Pid, {Idx, Term}, SnapKind}},
     [{monitor, process, snapshot_writer, Pid}]}.

-spec promote_checkpoint(Idx :: ra_index(), State0 :: state()) ->
    {boolean(), State :: state(), Effects :: [effect()]}.
promote_checkpoint(PromotionIdx,
                   #?MODULE{module = Mod,
                            snapshot_directory = SnapDir,
                            checkpoint_directory = CheckpointDir,
                            checkpoints = Checkpoints0} = State0) ->
    %% Find the checkpoint with the highest index smaller than or equal to the
    %% given `Idx' and rename the checkpoint directory to the snapshot
    %% directory.
    case find_promotable_checkpoint(PromotionIdx, Checkpoints0, []) of
        {Checkpoints, {Idx, Term}} ->
            Checkpoint = make_snapshot_dir(CheckpointDir, Idx, Term),
            Snapshot = make_snapshot_dir(SnapDir, Idx, Term),
            Self = self(),
            Pid = spawn(fun() ->
                                %% Checkpoints are created without calling
                                %% fsync. Snapshots must be fsync'd though, so
                                %% sync the checkpoint before promoting it
                                %% into a snapshot.
                                ok = Mod:sync(Checkpoint),
                                ok = prim_file:rename(Checkpoint, Snapshot),
                                Self ! {ra_log_event,
                                        {snapshot_written,
                                         {Idx, Term}, snapshot}}
                        end),
            State = State0#?MODULE{pending = {Pid, {Idx, Term}, snapshot},
                                   checkpoints = Checkpoints},
            {true, State, [{monitor, process, snapshot_writer, Pid}]};
        undefined ->
            {false, State0, []}
    end.

%% Find the first checkpoint smaller than or equal to the promotion index and
%% remove it from the checkpoint list.
-spec find_promotable_checkpoint(PromotionIdx, Checkpoints, Acc) -> Result
    when
      PromotionIdx :: ra_index(),
      Checkpoints :: [ra_idxterm()],
      Acc :: [ra_idxterm()],
      Result :: option({[ra_idxterm()], ra_idxterm()}).
find_promotable_checkpoint(Idx, [{CPIdx, _} = CP | Rest], Acc)
  when CPIdx =< Idx ->
    %% Checkpoints are sorted by index descending so the first checkpoint
    %% with an index smaller than or equal to the promotion index is the proper
    %% checkpoint to promote.
    {lists:reverse(Rest, Acc), CP};
find_promotable_checkpoint(Idx, [CP | Rest], Acc) ->
    find_promotable_checkpoint(Idx, Rest, [CP | Acc]);
find_promotable_checkpoint(_Idx, [], _Acc) ->
    undefined.

-spec complete_snapshot(ra_idxterm(), kind(), state()) ->
    state().
complete_snapshot({Idx, _} = IdxTerm, snapshot,
                  #?MODULE{uid = UId} = State) ->
    true = ets:insert(?ETSTBL, {UId, Idx}),
    State#?MODULE{pending = undefined,
                  current = IdxTerm};
complete_snapshot(IdxTerm, checkpoint,
                  #?MODULE{checkpoints = Checkpoints0} = State) ->
    State#?MODULE{pending = undefined,
                  checkpoints = [IdxTerm | Checkpoints0]}.

-spec begin_accept(meta(), state()) ->
    {ok, state()}.
begin_accept(#{index := Idx, term := Term} = Meta,
             #?MODULE{module = Mod,
                      snapshot_directory = Dir} = State) ->
    SnapDir = make_snapshot_dir(Dir, Idx, Term),
    ok = ra_lib:make_dir(SnapDir),
    {ok, AcceptState} = Mod:begin_accept(SnapDir, Meta),
    {ok, State#?MODULE{accepting = #accept{idxterm = {Idx, Term},
                                           state = AcceptState}}}.

-spec accept_chunk(term(), non_neg_integer(), chunk_flag(), state()) ->
    {ok, state()}.
accept_chunk(Chunk, Num, last,
             #?MODULE{uid = UId,
                      module = Mod,
                      snapshot_directory = Dir,
                      current = Current,
                      accepting = #accept{next = Num,
                                          idxterm = {Idx, _} = IdxTerm,
                                          state = AccState}} = State) ->
    %% last chunk
    ok = Mod:complete_accept(Chunk, AccState),
    %% run validate here?
    %% delete the current snapshot if any
    _ = spawn(fun () -> delete(Dir, Current) end),
    %% update ets table
    true = ets:insert(?ETSTBL, {UId, Idx}),
    {ok, State#?MODULE{accepting = undefined,
                       %% reset any pending snapshot writes
                       pending = undefined,
                       current = IdxTerm}};
accept_chunk(Chunk, Num, next,
             #?MODULE{module = Mod,
                      accepting =
                      #accept{state = AccState0,
                              next = Num} = Accept} = State) ->
    {ok, AccState} = Mod:accept_chunk(Chunk, AccState0),
    {ok, State#?MODULE{accepting = Accept#accept{state = AccState,
                                                 next = Num + 1}}};
accept_chunk(_Chunk, Num, _ChunkFlag,
             #?MODULE{accepting = #accept{next = Next}} = State)
  when Next > Num ->
    %% this must be a resend - we can just ignore it
    {ok, State}.

-spec abort_accept(state()) -> state().
abort_accept(#?MODULE{accepting = undefined} = State) ->
    State;
abort_accept(#?MODULE{accepting = #accept{idxterm = {Idx, Term}},
                      snapshot_directory = Dir} = State) ->
    ok = delete(Dir, {Idx, Term}),
    State#?MODULE{accepting = undefined}.

%% get the snapshot capabilities context of a remote node
-spec context(state(), node()) -> map().
context(#?MODULE{module = Mod}, Node) ->
    try erpc:call(Node, Mod, ?FUNCTION_NAME, []) of
        Result ->
            Result
    catch
        error:{exception, undef, _} ->
            #{}
    end.



-spec handle_down(pid(), Info :: term(), state()) ->
    state().
handle_down(_Pid, _Info, #?MODULE{pending = undefined} = State) ->
    State;
handle_down(_Pid, normal, State) ->
    State;
handle_down(_Pid, noproc, State) ->
    %% this could happen if the monitor was set up after the process had
    %% finished
    State;
handle_down(Pid, _Info,
            #?MODULE{snapshot_directory = SnapshotDir,
                     checkpoint_directory = CheckpointDir,
                     pending = {Pid, IdxTerm, SnapKind}} = State) ->
    %% delete the pending snapshot/checkpoint directory
    Dir = case SnapKind of
              snapshot -> SnapshotDir;
              checkpoint -> CheckpointDir
          end,
    ok = delete(Dir, IdxTerm),
    State#?MODULE{pending = undefined}.

delete(_, undefined) ->
    ok;
delete(Dir, {Idx, Term}) ->
    SnapDir = make_snapshot_dir(Dir, Idx, Term),
    ok = ra_lib:recursive_delete(SnapDir),
    ok.

-spec begin_read(State :: state(), Context :: map()) ->
    {ok, Meta :: meta(), ReadState} |
    {error, term()} when ReadState :: term().
begin_read(#?MODULE{module = Mod,
                    snapshot_directory = Dir,
                    current = {Idx, Term}},
          Context) when is_map(Context) ->
    Location = make_snapshot_dir(Dir, Idx, Term),
    Mod:begin_read(Location, Context).


-spec read_chunk(ReadState, ChunkSizeBytes :: non_neg_integer(),
                 State :: state()) ->
    {ok, Data :: term(), {next, ReadState} | last}  |
    {error, term()} when ReadState :: term().
read_chunk(ReadState, ChunkSizeBytes, #?MODULE{module = Mod,
                                               snapshot_directory = Dir,
                                               current = {Idx, Term}}) ->
    %% TODO: do we need to generate location for every chunk?
    Location = make_snapshot_dir(Dir, Idx, Term),
    Mod:read_chunk(ReadState, ChunkSizeBytes, Location).

%% Recovers from the latest checkpoint or snapshot, if available.
-spec recover(state()) ->
    {ok, Meta :: meta(), State :: term()} |
    {error, no_current_snapshot} |
    {error, term()}.
recover(#?MODULE{current = undefined, checkpoints = []}) ->
    {error, no_current_snapshot};
recover(#?MODULE{module = Mod,
                 current = Snapshot,
                 snapshot_directory = SnapDir,
                 checkpoints = Checkpoints,
                 checkpoint_directory = CheckpointDir}) ->
    %% If there are checkpoints and a snapshot, recover from whichever has the
    %% highest index. Otherwise recover from whichever exists.
    Dir = case {Snapshot, Checkpoints} of
              {{SnapIdx, _}, [{CPIdx, CPTerm} | _]} when CPIdx > SnapIdx ->
                  make_snapshot_dir(CheckpointDir, CPIdx, CPTerm);
              {{Idx, Term}, _} ->
                  make_snapshot_dir(SnapDir, Idx, Term);
              {undefined, [{Idx, Term} | _]} ->
                  make_snapshot_dir(CheckpointDir, Idx, Term)
          end,
    Mod:recover(Dir).

-spec read_meta(Module :: module(), Location :: file:filename()) ->
    {ok, meta()} |
    {error, invalid_format |
            {invalid_version, integer()} |
            checksum_error |
            file_err() |
            term()}.
read_meta(Module, Location) ->
    Module:read_meta(Location).

-spec current_snapshot_dir(state()) ->
    option(file:filename()).
current_snapshot_dir(#?MODULE{snapshot_directory = Dir,
                              current = {Idx, Term}}) ->
    make_snapshot_dir(Dir, Idx, Term);
current_snapshot_dir(_) ->
    undefined.

-spec take_older_checkpoints(ra_index(), state()) ->
    {state(), [checkpoint()]}.
take_older_checkpoints(Idx, #?MODULE{checkpoints = Checkpoints0} = State0) ->
    {Checkpoints, Outdated} = lists:splitwith(fun ({CPIdx, _Term}) ->
                                                      CPIdx > Idx
                                              end, Checkpoints0),
    {State0#?MODULE{checkpoints = Checkpoints}, Outdated}.

-spec take_extra_checkpoints(state()) ->
    {state(), [checkpoint()]}.
take_extra_checkpoints(State0) ->
    take_extra_checkpoints(State0, []).


take_extra_checkpoints(#?MODULE{checkpoints = Checkpoints0,
                                max_checkpoints = MaxCheckpoints} = State0,
                      Checks) ->
    Len = erlang:length(Checkpoints0),
    case Len > MaxCheckpoints of
        true ->
            %% when the number of checkpoints grow we increase the difference
            %% between checkpoints in order to keep the total count kept on disk
            %% down but keep some upper limit (~500k) to avoid huge differences
            Mult = min(8, Len div MaxCheckpoints),
            case find_checkpoint_to_delete(Mult, lists:reverse(Checkpoints0)) of
                undefined ->
                    {State0, Checks};
                {_, _} = Check ->
                    Checkpoints = lists:delete(Check, Checkpoints0),
                    {State0#?MODULE{checkpoints = Checkpoints},
                    [Check | Checks]}
            end;
        false ->
            {State0, Checks}
    end.

%% Utility

-define(MAX_DIFF, 65_536).

find_checkpoint_to_delete(Mult,
                          [{FstIdx, _},
                           {_, _} = Pot,
                           {ThrdIdx, _} | _] = Checks) ->
    case ThrdIdx - FstIdx < (?MAX_DIFF * Mult) of
        true ->
            Pot;
        false ->
            find_checkpoint_to_delete(Mult, tl(Checks))
    end;
find_checkpoint_to_delete(_, _) ->
    undefined.

make_snapshot_dir(Dir, Index, Term) ->
    I = ra_lib:zpad_hex(Index),
    T = ra_lib:zpad_hex(Term),
    filename:join(Dir, T ++ "_" ++ I).

counters_add(undefined, _, _) ->
    ok;
counters_add(Counter, Ix, Incr) ->
    counters:add(Counter, Ix, Incr).


-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").



-endif.
