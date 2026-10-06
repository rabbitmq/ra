%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2017-2025 Broadcom. All Rights Reserved. The term Broadcom refers to Broadcom Inc. and/or its subsidiaries.
%%
%% @hidden
%%
%% A shared, append-only log of small snapshots (one per system).
%%
%% Many Ra clusters snapshotting in parallel are expensive when every snapshot
%% is a new directory with files that each need syncing: the cost is dominated
%% by file system metadata and journal commits, not by the snapshot data. This
%% process batches snapshots from all members into appends to a single file
%% with one fsync per batch.
%%
%% Only the latest snapshot per member is live. A newer snapshot supersedes the
%% older one implicitly, there are no tombstones. Space is reclaimed by
%% rolling the active file when it reaches max(MinFileBytes, 2 x live bytes)
%% and then "retiring" the oldest rolled file: its still live records are
%% copied forward into the active file (piggybacking on normal batches) and the
%% file is deleted once the copies are durable. File numbers therefore always
%% reflect age, a torn tail can only be in the highest numbered file, and there
%% is no separate compactor process.
%%
%% File format
%% -----------
%% Header (64 bytes):
%%   "RASS" Version:8 FileNo:64 NextSeq:64 PrevLen:64 Crc:32 <zero padding>
%%   PrevLen is the number of valid bytes of the previous file (or all ones
%%   for "unknown / use everything that validates"). It is set to the last
%%   acknowledged offset when a file is abandoned after a write/fsync error so
%%   that unacknowledged (possibly not durable) records are ignored on
%%   recovery.
%% Records:
%%   Type:8 Len:32 Crc:32 Body:Len/binary
%%   Type 1 = put:
%%     Seq:64 ULen:16 UId ELen:16 Epoch Idx:64 Term:64 ImgLen:32 IdxLen:32
%%     Image Indexes
%%   Type 0 = padding (Len >= 1 zero bytes, Crc 0), used to 4KB align batches.
%%
%% Liveness of a record: it is the newest put (per Epoch, Idx, Seq) for its
%% UId and the owner says the member incarnation still exists (live_fun).
-module(ra_log_snap_store).

-behaviour(gen_batch_server).

-include("ra.hrl").
-include_lib("kernel/include/file.hrl").

-export([start_link/1,
         put/6,
         put_bin/6,
         lookup/2,
         read/3,
         registry_key/1,
         migrate_out/1,
         has_files/1,
         member_dir_exists/1,
         reconcile/3,
         release/3,
         delete/3,
         info/1,
         roll/1,
         status/1,
         stop/1]).

-export([init/1,
         handle_batch/2,
         terminate/2,
         format_status/1]).

-define(MAGIC, "RASS").
-define(VERSION, 1).
-define(HDR_SIZE, 64).
-define(ALIGN, 4096).
-define(PUT, 1).
-define(PAD, 0).
-define(REC_HDR, 9).
-define(UNKNOWN_LEN, 16#FFFFFFFFFFFFFFFF).
-define(DIR_KEY, '$dir').

-define(C_PUTS, 1).
-define(C_BATCHES, 2).
-define(C_BYTES_WRITTEN, 3).
-define(C_COPIES, 4).
-define(C_ROLLS, 5).
-define(C_RETIRED_FILES, 6).
-define(C_RETIRE_BLOCKED, 7).
-define(C_ERRORS, 8).
-define(C_STALE_PUTS, 9).
-define(C_CORRUPT_RECORDS, 10).
-define(C_FSYNC_TIME_US, 11).
-define(C_LIVE_BYTES, 12).
-define(C_ENTRIES, 13).
-define(C_FILES, 14).
-define(C_DEGRADED, 15).
-define(C_RECOVERY_TIME_MS, 16).
-define(COUNTER_FIELDS,
        [{puts, ?C_PUTS, counter,
          "Snapshots appended to the snapshot log"},
         {batches, ?C_BATCHES, counter,
          "Batches written (one fsync each)"},
         {bytes_written, ?C_BYTES_WRITTEN, counter,
          "Bytes appended, including records copied forward and padding"},
         {copies, ?C_COPIES, counter,
          "Records copied forward out of files that are being retired"},
         {rolls, ?C_ROLLS, counter,
          "Files rolled over"},
         {retired_files, ?C_RETIRED_FILES, counter,
          "Files retired and deleted"},
         {retire_blocked, ?C_RETIRE_BLOCKED, counter,
          "Files kept because snapshots in them could not be copied out"},
         {errors, ?C_ERRORS, counter,
          "Batches that failed to be written or files that could not be "
          "created"},
         {stale_puts, ?C_STALE_PUTS, counter,
          "Snapshots refused as older than the member's current one"},
         {corrupt_records, ?C_CORRUPT_RECORDS, counter,
          "Invalid records skipped when recovering or retiring files"},
         {fsync_time_us, ?C_FSYNC_TIME_US, counter,
          "Microseconds spent writing and syncing batches"},
         {live_bytes, ?C_LIVE_BYTES, gauge,
          "Bytes of the snapshots currently live"},
         {entries, ?C_ENTRIES, gauge,
          "Members with a snapshot in the log"},
         {files, ?C_FILES, gauge,
          "Snapshot log files on disk"},
         {degraded, ?C_DEGRADED, gauge,
          "1 if the snapshot log is unhealthy, see ra_log_snap_store:status/1"},
         {recovery_time_ms, ?C_RECOVERY_TIME_MS, gauge,
          "Time taken to recover the log files when it started"}
        ]).

-define(MIN_FILE_BYTES, ?ALIGN).
-define(DEFAULT_MIN_FILE_BYTES, 64 * 1024 * 1024).
-define(DEFAULT_RETIRE_CHUNK, 1024 * 1024).
-define(MIN_RETIRE_CHUNK, 4096).
-define(MAX_RETIRE_CHUNK, 64 * 1024 * 1024).
%% bytes of a file read to retire it for each byte appended by a batch
-define(RETIRE_FACTOR, 4).
-define(RETIRE_RETRY_MS, 1000).
-define(DEFAULT_MAX_BATCH, 1024).

%% ETS entry, key is the UId
%% {UId, Epoch, Idx, Term, Seq, FileNo, Off, TotalLen, ImgLen, IdxLen}

-type uid() :: binary().
-type epoch() :: binary().
-type io_fun() :: pwrite | sync | create | open_read.

-record(retire, {no :: non_neg_integer(),
                 fd :: file:fd(),
                 off :: non_neg_integer(),
                 limit :: non_neg_integer()}).

-record(?MODULE,
        {name :: atom(),
         dir :: file:filename_all(),
         tid :: ets:tid() | atom(),
         min_file_bytes :: pos_integer(),
         retire_chunk :: pos_integer(),
         live_fun :: fun((uid(), epoch()) -> boolean()),
         io = #{} :: #{io_fun() => fun()},
         registry_key :: undefined | term(),
         %% active file, undefined fd when no usable file (after an error)
         no :: non_neg_integer(),
         fd :: undefined | file:fd(),
         off = ?HDR_SIZE :: non_neg_integer(),
         %% bytes of records in the active file, i.e. without padding
         payload = 0 :: non_neg_integer(),
         force_roll = false :: boolean(),
         seq = 1 :: non_neg_integer(),
         live_bytes = 0 :: non_neg_integer(),
         %% rolled files, oldest first, with the number of valid bytes
         rolled = [] :: [{non_neg_integer(), non_neg_integer()}],
         retire :: undefined | #retire{},
         retire_token = false :: boolean(),
         %% not retiring until the retry timer fires, after an error
         retire_backoff = false :: boolean(),
         cref :: counters:counters_ref(),
         %% why the log is unhealthy, if it is
         health = [] :: [atom()],
         last_failed = false :: boolean(),
         retire_failing = false :: boolean(),
         %% files kept because they hold snapshots that could not be copied
         blocked = [] :: [non_neg_integer()]}).

-opaque state() :: #?MODULE{}.
-export_type([state/0]).

%%%===================================================================
%%% API
%%%===================================================================

%% Config:
%%   name := atom(), registered process name and ETS table name
%%   dir := directory the files live in
%%   min_file_bytes, retire_chunk_bytes
%%   registry := {Key, Value}, stored in a persistent term while the store is
%%               running so that code that only knows where a member's data
%%               lives can find the store (see ra_log_snapshot)
%%   live_fun := fun(UId, Epoch) -> boolean(), says whether the member
%%               incarnation that wrote a record still exists
%%   io := #{pwrite | sync | create => fun()}, overrides for fault injection
-spec start_link(map()) -> {ok, pid()} | {error, term()}.
start_link(#{name := Name} = Config) ->
    gen_batch_server:start_link({local, Name}, ?MODULE, Config,
                                [{max_batch_size, ?DEFAULT_MAX_BATCH}]).

stop(Name) ->
    gen_batch_server:stop(Name).

%% @doc Durably stores a snapshot. Returns once the batch it was part of has
%% been fsynced. `{error, stale}' is returned if a newer snapshot for the same
%% member incarnation is already stored. Storing the snapshot that is already
%% current is a no-op that returns ok.
-spec put(atom(), uid(), epoch(), {ra:index(), ra_term()},
          iodata(), ra_seq:state()) ->
    ok | {error, term()}.
put(Name, UId, Epoch, IdxTerm, Image, Indexes) ->
    put_bin(Name, UId, Epoch, IdxTerm, iolist_to_binary(Image),
            term_to_binary(Indexes)).

%% @doc as put/6 but with the image and the encoded indexes (the term_to_binary
%% of a ra_seq:state()) already binaries.
-spec put_bin(atom(), uid(), epoch(), {ra:index(), ra_term()},
              binary(), binary()) ->
    ok | {error, term()}.
put_bin(Name, UId, Epoch, {Idx, Term}, Image, IndexesBin)
  when is_binary(Image) andalso is_binary(IndexesBin) ->
    gen_batch_server:call(Name, {put, UId, Epoch, Idx, Term, Image,
                                 IndexesBin}, infinity).

%% @doc The persistent term key a store registers itself under (see the
%% `registry' config), given the data directory of the members it holds
%% snapshots for.
-spec registry_key(file:filename_all()) -> {?MODULE, binary()}.
registry_key(DataDir) ->
    {?MODULE, unicode:characters_to_binary(filename:join([DataDir]))}.

%% @doc True if the directory holds snapshot log files.
-spec has_files(file:filename_all()) -> boolean().
has_files(Dir) ->
    case prim_file:list_dir(Dir) of
        {ok, Files} ->
            lists:any(fun (F) -> filename:extension(F) == ".snap" end, Files);
        {error, _} ->
            false
    end.

%% @doc True unless the member directory is known not to exist. Used as the
%% liveness check: an error that is not "it does not exist" (e.g. a transient
%% I/O error) must not make a snapshot look dead.
-spec member_dir_exists(file:filename_all()) -> boolean().
member_dir_exists(Dir) ->
    case file:read_file_info(Dir) of
        {ok, #file_info{type = directory}} -> true;
        {ok, _} -> false;
        {error, enoent} -> false;
        {error, enotdir} -> false;
        {error, _} -> true
    end.

%% @doc Turns the snapshots held in a snapshot log back into the snapshot
%% directories that members use when no snapshot log is configured, then
%% removes the log. Needed when the feature is switched off, since snapshots
%% that only exist in the log would otherwise be invisible while the logs of
%% their members have already been truncated.
%%
%% Config:
%%   dir := the directory of the snapshot log files
%%   data_dir := the directory holding the member directories
%%   live_fun := fun(UId, Epoch) -> boolean(), as for start_link/1
%%
%% Each snapshot is written to a temporary directory, synced and renamed into
%% place so an interrupted run never leaves a partial snapshot where a member
%% looks for one. It is safe to run again: the log is only removed once every
%% snapshot is durable in its directory, and a snapshot that already has a
%% valid directory of the same or a newer index is left alone. A snapshot in
%% the log that is itself damaged is reported and skipped (it is lost either
%% way), any other failure leaves the log in place and is returned.
-spec migrate_out(map()) -> ok | {error, term()}.
migrate_out(#{dir := Dir, data_dir := DataDir} = Config) ->
    Tid = ets:new(snap_store_migration, [set, private]),
    try
        State = recover(#?MODULE{name = migration,
                                 cref = counters:new(length(?COUNTER_FIELDS),
                                                     [write_concurrency]),
                                 dir = Dir,
                                 tid = Tid,
                                 min_file_bytes = 1,
                                 retire_chunk = ?DEFAULT_RETIRE_CHUNK,
                                 live_fun = maps:get(live_fun, Config,
                                                     fun (_, _) -> true end),
                                 no = 0}),
        Entries = [E || E <- ets:tab2list(Tid), element(1, E) =/= ?DIR_KEY],
        Results = [migrate_entry(Dir, DataDir, E) || E <- Entries],
        case [R || {error, _} = R <- Results] of
            [] ->
                Lost = [L || {lost, L} <- Results],
                ?INFO("ra_log_snap_store: moved ~b snapshots out of the "
                      "snapshot log in ~ts, ~b could not be read",
                      [length(Entries) - length(Lost), Dir, length(Lost)]),
                _ = [file:delete(file_name(Dir, No))
                     || {No, _} <- State#?MODULE.rolled],
                _ = ra_lib:sync_dir(Dir),
                ok;
            [Error | _] ->
                Error
        end
    catch
        Class:Reason ->
            {error, {migrate_out, Class, Reason}}
    after
        ets:delete(Tid)
    end.

migrate_entry(Dir, DataDir,
              {UId, _Epoch, Idx, Term, _Seq, No, Off, TL, ImgLen, IdxLen}) ->
    ServerDir = filename:join(DataDir, ra_lib:to_list(UId)),
    SnapshotsDir = filename:join(ServerDir, "snapshots"),
    SnapDir = ra_snapshot:make_snapshot_dir(SnapshotsDir, Idx, Term),
    case have_valid_snapshot_at_least(SnapshotsDir, Idx) of
        true ->
            ok;
        false ->
            case read_record(Dir, No, Off, TL, ImgLen, IdxLen) of
                {ok, Image, IndexesBin} ->
                    write_migrated(ServerDir, SnapshotsDir, SnapDir, UId,
                                   Image, binary_to_term(IndexesBin));
                {error, Reason}
                  when Reason == checksum_error orelse
                       Reason == invalid_record orelse Reason == eof ->
                    ?ERROR("ra_log_snap_store: snapshot ~b of ~ts in file ~b "
                           "is damaged (~w), it cannot be moved out",
                           [Idx, UId, No, Reason]),
                    {lost, {UId, Idx, Reason}};
                {error, Reason} ->
                    {error, {read_snapshot, UId, Reason}}
            end
    end.

write_migrated(ServerDir, SnapshotsDir, SnapDir, UId, Image, Indexes) ->
    Staging = filename:join(ServerDir, "snapshot_migrating"),
    Tmp = filename:join(Staging, filename:basename(SnapDir)),
    try
        _ = ra_lib:recursive_delete(Tmp),
        ok = ra_lib:make_dir(Staging),
        ok = ra_lib:make_dir(Tmp),
        ok = ra_lib:write_file(filename:join(Tmp, "snapshot.dat"), Image, true),
        case Indexes of
            [] ->
                ok;
            _ ->
                ok = ra_snapshot:write_indexes(Tmp, Indexes),
                ok = ra_lib:sync_file(filename:join(Tmp, "indexes"))
        end,
        ok = sync_dir_strict(Tmp),
        ok = ra_lib:make_dir(SnapshotsDir),
        %% anything already there under this name did not validate
        _ = ra_lib:recursive_delete(SnapDir),
        ok = prim_file:rename(Tmp, SnapDir),
        ok = sync_dir_strict(SnapshotsDir),
        ok = sync_dir_strict(Staging),
        _ = file:del_dir(Staging),
        ok
    catch
        Class:Reason ->
            {error, {migrate_snapshot, UId, Class, Reason}}
    end.

%% a snapshot directory counts only if it is complete and intact
have_valid_snapshot_at_least(SnapshotsDir, Idx) ->
    case prim_file:list_dir(SnapshotsDir) of
        {ok, Names} ->
            lists:any(
              fun (Name) ->
                      case ra_snapshot:parse_snapshot_name(Name) of
                          {ok, {I, _}} when I >= Idx ->
                              Snap = filename:join(SnapshotsDir, Name),
                              not filelib:is_file(
                                    filename:join(Snap, "accepting")) andalso
                                  ra_log_snapshot:validate(Snap) == ok;
                          _ ->
                              false
                      end
              end, Names);
        {error, _} ->
            false
    end.

%% @doc Looks up the current entry for a member without going through the
%% writer. May return a snapshot that is superseded a moment later.
-spec lookup(atom(), uid()) ->
    {ok, #{epoch := epoch(), idx := ra:index(), term := ra_term(),
           size := non_neg_integer()}} |
    not_found | {error, store_unavailable}.
lookup(Name, UId) ->
    try ets:lookup(Name, UId) of
        [{UId, Epoch, Idx, Term, _Seq, _No, _Off, _TL, ImgLen, _IdxLen}] ->
            {ok, #{epoch => Epoch, idx => Idx, term => Term, size => ImgLen}};
        [] ->
            not_found
    catch
        error:badarg ->
            {error, store_unavailable}
    end.

%% @doc Reads the image and live indexes of the snapshot with the given index
%% and term. Returns `{error, superseded}' if the member has moved on.
-spec read(atom(), uid(), {ra:index(), ra_term()}) ->
    {ok, binary(), ra_seq:state()} |
    {error, not_found | superseded | store_unavailable | term()}.
read(Name, UId, IdxTerm) ->
    read(Name, UId, IdxTerm, 5).

%% @doc Synchronous lookup that is ordered after every request already in the
%% writer's mailbox (e.g. a put from a worker that was killed). Entries of a
%% different epoch (an earlier incarnation of the member) are dropped.
-spec reconcile(atom(), uid(), epoch()) ->
    {ok, #{idx := ra:index(), term := ra_term()}} | not_found.
reconcile(Name, UId, Epoch) ->
    gen_batch_server:call(Name, {reconcile, UId, Epoch}, infinity).

%% @doc Tells the store that the snapshot with the given index and term is no
%% longer needed (it was deleted, or superseded by a snapshot held elsewhere).
%% Its entry is dropped only if it is that exact snapshot: deleting a snapshot
%% that failed to be written must not drop the current one. Not durable.
-spec release(atom(), uid(), {ra:index(), ra_term()}) -> ok.
release(Name, UId, {_Idx, _Term} = IdxTerm) ->
    gen_batch_server:cast(Name, {release, UId, IdxTerm}).

%% @doc Drops the entry of a deleted member incarnation. Not durable: the
%% durable marker of deletion is the member's directory being gone, which
%% live_fun checks on recovery and when retiring files.
-spec delete(atom(), uid(), epoch() | any) -> ok.
delete(Name, UId, Epoch) ->
    gen_batch_server:call(Name, {delete, UId, Epoch}, infinity).

%% @doc Closes the active file and starts a new one, if the active file has
%% anything in it. The files of the log roll by themselves as they fill up, this
%% is for maintenance and tests.
-spec roll(atom()) -> ok.
roll(Name) ->
    gen_batch_server:call(Name, roll, infinity).

-spec info(atom()) -> map().
info(Name) ->
    gen_batch_server:call(Name, info, infinity).

%% @doc Whether the snapshot log is healthy. Reasons it is not: it has no file
%% to write to (it could not create one after a failure), the last batch
%% failed to be written, files could not be read to retire them, or files are
%% being kept because snapshots in them could not be copied out. The same is
%% in the `degraded' counter, which is 1 when this is not `ok'.
-spec status(atom()) -> ok | {degraded, [atom()]}.
status(Name) ->
    case info(Name) of
        #{health := []} -> ok;
        #{health := Reasons} -> {degraded, Reasons}
    end.

%%%===================================================================
%%% gen_batch_server callbacks
%%%===================================================================

init(#{name := Name, dir := Dir} = Config) ->
    process_flag(trap_exit, true),
    ok = ra_lib:make_dir(Dir),
    %% recovery fills a private table, readers only get to see the named table
    %% once it is complete
    RecTid = ets:new(snap_store_recovery, [set, private]),
    Registry = maps:get(registry, Config, undefined),
    CRef = new_counters(Name, maps:get(system, Config, undefined)),
    State0 = #?MODULE{name = Name,
                      cref = CRef,
                      dir = Dir,
                      tid = RecTid,
                      min_file_bytes = max(?MIN_FILE_BYTES,
                                           maps:get(min_file_bytes, Config,
                                                    ?DEFAULT_MIN_FILE_BYTES)),
                      retire_chunk = max(?MIN_RETIRE_CHUNK,
                                         maps:get(retire_chunk_bytes, Config,
                                                  ?DEFAULT_RETIRE_CHUNK)),
                      live_fun = maps:get(live_fun, Config,
                                          fun (_, _) -> true end),
                      io = maps:get(io, Config, #{}),
                      registry_key = case Registry of
                                         {K, _} -> K;
                                         undefined -> undefined
                                     end,
                      no = 0},
    RecoveryStart = erlang:monotonic_time(millisecond),
    State1 = recover(State0),
    counters:put(CRef, ?C_RECOVERY_TIME_MS,
                 erlang:monotonic_time(millisecond) - RecoveryStart),
    Tid = ets:new(Name, [named_table, protected, set,
                         {read_concurrency, true}]),
    true = ets:insert(Tid, ets:tab2list(RecTid)),
    true = ets:insert(Tid, {?DIR_KEY, Dir}),
    true = ets:delete(RecTid),
    case Registry of
        {Key, Value} ->
            persistent_term:put(Key, Value);
        undefined ->
            ok
    end,
    State2 = State1#?MODULE{tid = Tid},
    case new_active(State2) of
        {ok, State3} ->
            {ok, schedule_retire(refresh(State3))};
        {error, Reason, _} ->
            {stop, {cannot_create_snapshot_store_file, Reason}}
    end.

handle_batch(Ops, State0) ->
    {Puts, Ordered, Followers, State1} = classify(Ops, State0),
    PutBytes = lists:sum([byte_size(Image) + byte_size(Indexes)
                          || {put, _, _, _, _, _, Image, Indexes} <- Puts]),
    %% Files are retired in proportion to what is appended, so that they are
    %% retired as fast as they are made whatever the load
    Budget = min(?MAX_RETIRE_CHUNK,
                 max(State1#?MODULE.retire_chunk, ?RETIRE_FACTOR * PutBytes)),
    {Copies, State2} = case State1 of
                           %% nowhere to copy to
                           #?MODULE{fd = undefined} -> {[], State1};
                           %% backing off after an error
                           #?MODULE{retire_backoff = true} -> {[], State1};
                           _ -> retire_scan(State1, Budget)
                       end,
    {Outcome, State3} = write_batch(Copies, Puts, State2),
    {Replies, State4} = replay(Ordered, Followers, Outcome, State3),
    State5 = finish_retire(State4),
    {ok, Replies, schedule_retire(refresh(maybe_roll(State5)))}.

terminate(_Reason, #?MODULE{name = Name, fd = Fd, retire = Retire,
                            registry_key = RegKey}) ->
    RegKey == undefined orelse persistent_term:erase(RegKey),
    ?CATCH(ra_counters:delete(Name)),
    _ = close(Fd),
    case Retire of
        #retire{fd = RFd} -> _ = close(RFd);
        _ -> ok
    end,
    ok.

format_status(State) ->
    State.

%%%===================================================================
%%% batch handling
%%%===================================================================

%% Goes through the operations of a batch in arrival order and splits them into
%% the puts to be written and the operations to carry out, again in arrival
%% order, once the write is done. A deleted, released or reconciled member is
%% seen as such by the operations that follow it in the batch, so a put after a
%% delete is not judged against the entry that is about to go. A put that
%% repeats, or is stale compared to, one earlier in the same batch is a
%% "follower" of it and gets the outcome of the earlier one, as nothing is
%% durable yet.
classify(Ops, State) ->
    classify(Ops, State, [], [], [], #{}).

classify([], State, Puts, Ordered, Followers, _Virtual) ->
    {lists:reverse(Puts), lists:reverse(Ordered), lists:reverse(Followers),
     State};
classify([{call, From, {put, UId, Epoch, Idx, Term, Image, Indexes}} | Rem],
         State, Puts, Ordered, Followers, Virtual) ->
    {Current, Leader} = case virtual(UId, Virtual, State) of
                            {pending, E, I, T, L} -> {{E, I, T}, L};
                            {stored, E, I, T} -> {{E, I, T}, undefined};
                            none -> {undefined, undefined}
                        end,
    case put_decision(Current, Epoch, Idx, Term) of
        write ->
            Item = {put, From, UId, Epoch, Idx, Term, Image, Indexes},
            classify(Rem, State, [Item | Puts],
                     [{put, From, UId} | Ordered], Followers,
                     Virtual#{UId => {pending, Epoch, Idx, Term, From}});
        ok when Leader == undefined ->
            classify(Rem, State, Puts, [{reply, From, ok} | Ordered],
                     Followers, Virtual);
        ok ->
            classify(Rem, State, Puts, Ordered,
                     [{Leader, From, ok} | Followers], Virtual);
        stale when Leader == undefined ->
            classify(Rem, incr(stale_puts, State), Puts,
                     [{reply, From, {error, stale}} | Ordered], Followers,
                     Virtual);
        stale ->
            classify(Rem, incr(stale_puts, State), Puts, Ordered,
                     [{Leader, From, {error, stale}} | Followers], Virtual)
    end;
classify([{call, From, {reconcile, UId, Epoch}} | Rem], State, Puts,
         Ordered, Followers, Virtual) ->
    %% an entry of another incarnation is dropped by the reconcile
    V1 = case virtual(UId, Virtual, State) of
             {_, E, _, _} when E =/= Epoch -> Virtual#{UId => none};
             {pending, E, _, _, _} when E =/= Epoch -> Virtual#{UId => none};
             _ -> Virtual
         end,
    classify(Rem, State, Puts, [{reconcile, From, UId, Epoch} | Ordered],
             Followers, V1);
classify([{call, From, {delete, UId, Epoch}} | Rem], State, Puts,
         Ordered, Followers, Virtual) ->
    V1 = case virtual(UId, Virtual, State) of
             {pending, E, _, _, _}
               when Epoch == any orelse Epoch == E ->
                 Virtual#{UId => none};
             {stored, E, _, _}
               when Epoch == any orelse Epoch == E ->
                 Virtual#{UId => none};
             _ ->
                 Virtual
         end,
    classify(Rem, State, Puts, [{delete, From, UId, Epoch} | Ordered],
             Followers, V1);
classify([{call, From, roll} | Rem], State, Puts, Ordered, Followers,
         Virtual) ->
    classify(Rem, State, Puts, [{roll, From} | Ordered], Followers, Virtual);
classify([{call, From, info} | Rem], State, Puts, Ordered, Followers,
         Virtual) ->
    classify(Rem, State, Puts, [{info, From} | Ordered], Followers, Virtual);
classify([{call, From, _Unknown} | Rem], State, Puts, Ordered, Followers,
         Virtual) ->
    classify(Rem, State, Puts,
             [{reply, From, {error, unknown_request}} | Ordered],
             Followers, Virtual);
classify([{cast, {release, UId, {Idx, Term} = IdxTerm}} | Rem], State, Puts,
         Ordered, Followers, Virtual) ->
    V1 = case virtual(UId, Virtual, State) of
             {pending, _, Idx, Term, _} -> Virtual#{UId => none};
             {stored, _, Idx, Term} -> Virtual#{UId => none};
             _ -> Virtual
         end,
    classify(Rem, State, Puts, [{release, UId, IdxTerm} | Ordered], Followers,
             V1);
classify([{info, retire_step} | Rem], State, Puts, Ordered, Followers,
         Virtual) ->
    classify(Rem, State#?MODULE{retire_token = false, retire_backoff = false},
             Puts, Ordered, Followers, Virtual);
classify([_ | Rem], State, Puts, Ordered, Followers, Virtual) ->
    classify(Rem, State, Puts, Ordered, Followers, Virtual).

%% the member's entry as the operations of the batch before this one leave it
virtual(UId, Virtual, State) ->
    case Virtual of
        #{UId := none} ->
            none;
        #{UId := {pending, _, _, _, _} = Pending} ->
            Pending;
        _ ->
            case current(UId, State) of
                undefined -> none;
                {E, I, T} -> {stored, E, I, T}
            end
    end.

current(UId, #?MODULE{tid = Tid}) ->
    case ets:lookup(Tid, UId) of
        [{UId, Epoch, Idx, Term, _, _, _, _, _, _}] -> {Epoch, Idx, Term};
        [] -> undefined
    end.

put_decision(undefined, _, _, _) ->
    write;
put_decision({Epoch, Idx0, Term0}, Epoch, Idx, Term) ->
    if Idx > Idx0 -> write;
       Idx == Idx0 andalso Term == Term0 -> ok;
       Idx == Idx0 andalso Term > Term0 -> write;
       true -> stale
    end;
put_decision({_OtherEpoch, _, _}, _Epoch, _Idx, _Term) ->
    %% a different incarnation of the member, the old entry is dead
    write.

%% Writes copies (records carried forward from the file being retired) and
%% puts in a single append + fsync. Copies go first so that, if a put for the
%% same member is in the same batch, the put has the higher sequence number.
%% The copies are published once the batch is durable, the puts are by replay/4,
%% which has to apply them in the order the operations came in.
write_batch(_Copies, [], #?MODULE{fd = undefined} = State) ->
    %% nothing to write and no usable file, don't churn trying to make one
    {no_write, State};
write_batch(_Copies, Puts, #?MODULE{fd = undefined} = State0) ->
    %% no usable file: try to get one, otherwise fail the puts
    State = abandon_retire_progress(State0),
    case new_active(State) of
        {ok, State1} ->
            write_batch([], Puts, State1);
        {error, Reason, State1} ->
            {{error, Reason}, incr(errors, State1#?MODULE{last_failed = true})}
    end;
write_batch([], [], #?MODULE{} = State) ->
    {no_write, State};
write_batch(Copies, Puts, #?MODULE{no = No, off = Off0, seq = Seq0,
                                   fd = Fd} = State0) ->
    Items = [{copy, C} || C <- Copies] ++ [{put, P} || P <- Puts],
    {IO, Applies, Off1, Seq1} = encode_items(Items, No, Off0, Seq0),
    Bytes = Off1 - Off0,
    WriteStart = erlang:monotonic_time(microsecond),
    WriteRes = do_write(State0, Fd, Off0, IO),
    incr(fsync_time_us, erlang:monotonic_time(microsecond) - WriteStart,
         State0),
    case WriteRes of
        ok ->
            State1 = apply_entries([A || {copy, _, _, _} = A <- Applies],
                                   State0),
            Payload = lists:sum([element(8, element(2, A)) ||
                                    A <- Applies, element(1, A) == put] ++
                                [element(8, element(4, A)) ||
                                    A <- Applies, element(1, A) == copy]),
            State2 = State1#?MODULE{off = Off1,
                                    payload = State1#?MODULE.payload + Payload,
                                    seq = Seq1,
                                    last_failed = false},
            State3 = incr(puts, length(Puts),
                          incr(copies, length(Copies),
                               incr(bytes_written, Bytes,
                                    incr(batches, State2)))),
            {{ok, [E || {put, E} <- Applies]}, State3};
        {error, Reason} ->
            %% Do not retry the fsync: after a failed fsync the page cache can
            %% claim the data is clean while it never reached the disk. Fail
            %% every caller, forget the retire progress that depended on this
            %% batch, and abandon the file. Whatever was written after the
            %% last acknowledged offset is ignored on recovery.
            ?ERROR("ra_log_snap_store: ~ts: write failed: ~w, "
                   "abandoning file ~b",
                   [State0#?MODULE.name, Reason, No]),
            State1 = abandon_retire_progress(State0),
            State2 = abandon_file(State1, Off0),
            {{error, Reason},
             incr(errors, State2#?MODULE{last_failed = true})}
    end.

do_write(State, Fd, Off, IO) ->
    case io_pwrite(State, Fd, Off, IO) of
        ok -> io_sync(State, Fd);
        Err -> Err
    end.

encode_items(Items, No, Off0, Seq0) ->
    {RevIO, RevApplies, Off1, Seq1} =
        lists:foldl(
          fun ({put, {put, _From, UId, Epoch, Idx, Term, Image, Indexes}},
               {IOAcc, AAcc, Off, Seq}) ->
                  Body = [<<Seq:64, (byte_size(UId)):16>>, UId,
                          <<(byte_size(Epoch)):16>>, Epoch,
                          <<Idx:64, Term:64,
                            (byte_size(Image)):32, (byte_size(Indexes)):32>>,
                          Image, Indexes],
                  {Rec, TL} = record(?PUT, Body),
                  E = {UId, Epoch, Idx, Term, Seq, No, Off, TL,
                       byte_size(Image), byte_size(Indexes)},
                  {[Rec | IOAcc], [{put, E} | AAcc], Off + TL, Seq + 1};
              ({copy, {copy, OldNo, OldOff, UId, Epoch, Idx, Term,
                       ImgLen, IdxLen, <<_OldSeq:64, Rest/binary>>}},
               {IOAcc, AAcc, Off, Seq}) ->
                  Body = [<<Seq:64>>, Rest],
                  {Rec, TL} = record(?PUT, Body),
                  E = {UId, Epoch, Idx, Term, Seq, No, Off, TL,
                       ImgLen, IdxLen},
                  {[Rec | IOAcc], [{copy, OldNo, OldOff, E} | AAcc],
                   Off + TL, Seq + 1}
          end, {[], [], Off0, Seq0}, Items),
    {PadIO, PadLen} = pad(Off1),
    {lists:reverse([PadIO | RevIO]), lists:reverse(RevApplies),
     Off1 + PadLen, Seq1}.

record(Type, Body) ->
    Len = iolist_size(Body),
    {[<<Type:8, Len:32, (erlang:crc32(Body)):32>>, Body], ?REC_HDR + Len}.

pad(End) ->
    case End rem ?ALIGN of
        0 ->
            {[], 0};
        Rem ->
            P0 = ?ALIGN - Rem,
            %% a padding record needs at least one byte of body
            P = case P0 =< ?REC_HDR of
                    true -> P0 + ?ALIGN;
                    false -> P0
                end,
            N = P - ?REC_HDR,
            {<<?PAD:8, N:32, 0:32, 0:(N * 8)>>, P}
    end.

%% The batch is durable, publish it.
apply_entries(Applies, State) ->
    lists:foldl(
      fun ({put, E}, S) ->
              publish(E, S);
          ({copy, OldNo, OldOff, E}, S) ->
              %% only if the entry still lives where we copied it from
              UId = element(1, E),
              case ets:lookup(S#?MODULE.tid, UId) of
                  [{UId, _, _, _, _, OldNo, OldOff, _, _, _}] ->
                      publish(E, S);
                  _ ->
                      S
              end
      end, State, Applies).

publish(E, #?MODULE{tid = Tid, live_bytes = Live} = State) ->
    UId = element(1, E),
    TL = element(8, E),
    Old = case ets:lookup(Tid, UId) of
              [{UId, _, _, _, _, _, _, OldTL, _, _}] -> OldTL;
              [] -> 0
          end,
    true = ets:insert(Tid, E),
    State#?MODULE{live_bytes = Live - Old + TL}.

remove_entry(UId, #?MODULE{tid = Tid, live_bytes = Live} = State) ->
    case ets:lookup(Tid, UId) of
        [{UId, _, _, _, _, _, _, TL, _, _}] ->
            true = ets:delete(Tid, UId),
            State#?MODULE{live_bytes = Live - TL};
        [] ->
            State
    end.

%% Carries out the operations of the batch in the order they came in, now that
%% the write is done: a put is published (or failed), a delete or release takes
%% effect, a reconcile is answered from what the operations before it left.
replay(Ordered, Followers, Outcome, State0) ->
    Entries = case Outcome of
                  {ok, Es} -> Es;
                  _ -> []
              end,
    {Replies0, _, State1} =
        lists:foldl(fun (Op, {Acc, Es, S}) ->
                            replay_op(Op, Outcome, Es, Acc, S)
                    end, {[], Entries, State0}, Ordered),
    %% a repeat of a put in the same batch gets the outcome of the put
    Outcomes = maps:from_list([{From, Reply}
                               || {reply, From, Reply} <- Replies0]),
    Replies1 = [{reply, From, case Outcomes of
                                 #{Leader := ok} -> Reply;
                                 #{Leader := Error} -> Error;
                                 _ -> Reply
                             end}
                || {Leader, From, Reply} <- Followers],
    {lists:reverse(Replies0) ++ Replies1, State1}.

replay_op({reply, _From, _Reply} = R, _Outcome, Es, Acc, S) ->
    {[R | Acc], Es, S};
replay_op({put, From, _UId}, {ok, _}, [E | Es], Acc, S) ->
    {[{reply, From, ok} | Acc], Es, publish(E, S)};
replay_op({put, From, _UId}, {error, Reason}, Es, Acc, S) ->
    {[{reply, From, {error, Reason}} | Acc], Es, S};
replay_op({reconcile, From, UId, Epoch}, _Outcome, Es, Acc, S) ->
    case ets:lookup(S#?MODULE.tid, UId) of
        [{UId, Epoch, Idx, Term, _, _, _, _, _, _}] ->
            {[{reply, From, {ok, #{idx => Idx, term => Term}}} | Acc], Es, S};
        [{UId, _OtherEpoch, _, _, _, _, _, _, _, _}] ->
            {[{reply, From, not_found} | Acc], Es, remove_entry(UId, S)};
        [] ->
            {[{reply, From, not_found} | Acc], Es, S}
    end;
replay_op({delete, From, UId, Epoch}, _Outcome, Es, Acc, S) ->
    S1 = case ets:lookup(S#?MODULE.tid, UId) of
             [{UId, EntryEpoch, _, _, _, _, _, _, _, _}]
               when Epoch == any orelse Epoch == EntryEpoch ->
                 remove_entry(UId, S);
             _ ->
                 S
         end,
    {[{reply, From, ok} | Acc], Es, S1};
replay_op({release, UId, {Idx, Term}}, _Outcome, Es, Acc, S) ->
    S1 = case ets:lookup(S#?MODULE.tid, UId) of
             [{UId, _, Idx, Term, _, _, _, _, _, _}] ->
                 remove_entry(UId, S);
             _ ->
                 S
         end,
    {Acc, Es, S1};
replay_op({roll, From}, _Outcome, Es, Acc, S) ->
    {[{reply, From, ok} | Acc], Es, S#?MODULE{force_roll = true}};
replay_op({info, From}, _Outcome, Es, Acc, S) ->
    {[{reply, From, do_info(S)} | Acc], Es, S}.

do_info(#?MODULE{cref = CRef, live_bytes = Live, off = Off, no = No,
                 rolled = Rolled, retire = Retire, fd = Fd, health = Health}) ->
    Counters = maps:from_list([{Field, counters:get(CRef, Idx)}
                               || {Field, Idx, _, _} <- ?COUNTER_FIELDS]),
    Counters#{live_bytes => Live,
              active_file => No,
              active_offset => Off,
              rolled_files => length(Rolled),
              retiring => Retire =/= undefined,
              has_active_file => Fd =/= undefined,
              health => Health}.

%% Brings the gauges and the health up to date after a batch, logging when the
%% health changes.
refresh(#?MODULE{cref = CRef, tid = Tid, live_bytes = Live, rolled = Rolled,
                 fd = Fd, name = Name, health = Old} = State) ->
    counters:put(CRef, ?C_LIVE_BYTES, Live),
    counters:put(CRef, ?C_ENTRIES, ets:info(Tid, size) - 1),
    counters:put(CRef, ?C_FILES, length(Rolled) +
                 case Fd of undefined -> 0; _ -> 1 end),
    Health = health(State),
    counters:put(CRef, ?C_DEGRADED, case Health of [] -> 0; _ -> 1 end),
    case Health of
        Old ->
            ok;
        [] ->
            ?NOTICE("ra_log_snap_store: ~ts: healthy again", [Name]);
        _ ->
            ?ERROR("ra_log_snap_store: ~ts: unhealthy: ~w", [Name, Health])
    end,
    State#?MODULE{health = Health}.

health(#?MODULE{fd = Fd, last_failed = LastFailed,
                retire_failing = RetireFailing, blocked = Blocked}) ->
    [no_active_file || Fd == undefined] ++
        [write_errors || LastFailed] ++
        [retire_read_errors || RetireFailing] ++
        [files_blocked || Blocked =/= []].

%%%===================================================================
%%% rolling and retiring
%%%===================================================================

%% The file is rolled when the records in it are at least twice the live data
%% (and at least min_file_bytes) so that at most half of what is copied
%% forward out of a file is live, and copying the live data of a file can not
%% roll the next one by itself. Padding is not counted in that, but a file is
%% also rolled if it is a lot bigger than that anyway, e.g. made of many tiny
%% batches.
maybe_roll(#?MODULE{fd = Fd, off = Off, payload = Payload, live_bytes = Live,
                    min_file_bytes = Min, force_roll = Force} = State)
  when Fd =/= undefined andalso
       ((Force andalso Payload > 0) orelse
        Payload >= max(Min, 2 * Live) orelse
        Off >= 4 * max(Min, 2 * Live)) ->
    case new_active(close_active(State, Off)) of
        {ok, State1} ->
            incr(rolls, State1);
        {error, _Reason, State1} ->
            %% no active file, the next batch tries again
            State1
    end;
maybe_roll(State) ->
    State#?MODULE{force_roll = false}.

%% Closes the active file. Everything in it up to `AckedLen' is durable.
close_active(#?MODULE{fd = undefined} = State, _AckedLen) ->
    State;
close_active(#?MODULE{fd = Fd, no = No, rolled = Rolled} = State, AckedLen) ->
    _ = close(Fd),
    State#?MODULE{fd = undefined, rolled = Rolled ++ [{No, AckedLen}]}.

%% Starts the next file. Its header records how many bytes of the previous
%% file are valid so that anything written after that is ignored on recovery.
new_active(#?MODULE{no = No0, dir = Dir, seq = Seq, rolled = Rolled,
                    fd = undefined} = State) ->
    PrevLen = case lists:keyfind(No0, 1, Rolled) of
                  {No0, Len} -> Len;
                  false -> ?UNKNOWN_LEN
              end,
    No = No0 + 1,
    case create_file(State, Dir, No, Seq, PrevLen) of
        {ok, Fd} ->
            {ok, State#?MODULE{no = No, fd = Fd, off = ?HDR_SIZE, payload = 0,
                               force_roll = false}};
        {error, Reason} ->
            ?ERROR("ra_log_snap_store: ~ts: could not create file ~b: ~w",
                   [State#?MODULE.name, No, Reason]),
            _ = file:delete(file_name(Dir, No)),
            {error, Reason, State}
    end.

create_file(State, Dir, No, Seq, PrevLen) ->
    Path = file_name(Dir, No),
    case io_create(State, Path) of
        {ok, Fd} ->
            Hdr0 = <<?MAGIC, ?VERSION:8, No:64, Seq:64, PrevLen:64>>,
            Hdr = <<Hdr0/binary, (erlang:crc32(Hdr0)):32,
                    0:((?HDR_SIZE - byte_size(Hdr0) - 4) * 8)>>,
            case io_pwrite(State, Fd, 0, Hdr) of
                ok ->
                    case io_sync(State, Fd) of
                        ok ->
                            case sync_dir_strict(Dir) of
                                ok ->
                                    {ok, Fd};
                                Err ->
                                    _ = close(Fd),
                                    Err
                            end;
                        Err ->
                            _ = close(Fd),
                            Err
                    end;
                Err ->
                    _ = close(Fd),
                    Err
            end;
        Err ->
            Err
    end.

abandon_file(#?MODULE{dir = Dir, no = No} = State, AckedLen) ->
    State1 = close_active(State, AckedLen),
    %% best effort: the next file's header says where the valid data ends but
    %% if that file cannot be created it is not there to say it
    _ = truncate_file(file_name(Dir, No), AckedLen),
    case new_active(State1) of
        {ok, State2} -> State2;
        {error, _Reason, State2} -> State2
    end.

truncate_file(Path, Len) ->
    case file:open(Path, [read, write, raw, binary]) of
        {ok, Fd} ->
            _ = file:position(Fd, Len),
            _ = file:truncate(Fd),
            file:close(Fd);
        Err ->
            Err
    end.

schedule_retire(#?MODULE{retire_token = true} = State) ->
    State;
schedule_retire(#?MODULE{fd = undefined} = State) ->
    %% re-armed by the batch that gets a new file
    State;
schedule_retire(#?MODULE{rolled = [], retire = undefined} = State) ->
    State;
schedule_retire(State) ->
    self() ! retire_step,
    State#?MODULE{retire_token = true}.

%% Reads the next chunk of the file being retired and returns the records in
%% it that are still live, as copies to be re-appended.
retire_scan(#?MODULE{retire = undefined, rolled = []} = State, _Budget) ->
    {[], State};
retire_scan(#?MODULE{retire = undefined, rolled = [{No, Limit} | _],
                     dir = Dir} = State, Budget) ->
    case io_open_read(State, file_name(Dir, No)) of
        {ok, Fd} ->
            retire_scan(State#?MODULE{retire = #retire{no = No, fd = Fd,
                                                       off = ?HDR_SIZE,
                                                       limit = Limit}},
                        Budget);
        {error, enoent} ->
            %% already gone
            retire_scan(State#?MODULE{rolled = tl(State#?MODULE.rolled)},
                        Budget);
        {error, Reason} ->
            ?ERROR("ra_log_snap_store: ~ts: cannot open file ~b to retire: ~w, "
                   "will try again", [State#?MODULE.name, No, Reason]),
            {[], retire_later(State)}
    end;
retire_scan(#?MODULE{retire = #retire{no = No, fd = Fd, off = Off,
                                      limit = Limit} = R,
                     live_fun = LiveFun} = State, Budget) ->
    {Recs, Next, Status, Skipped} = read_records(Fd, Off, Limit, Budget),
    counters:add(State#?MODULE.cref, ?C_CORRUPT_RECORDS, Skipped),
    {Copies, State1} =
        lists:foldl(
          fun ({RecOff, _TL, #{uid := UId, epoch := Epoch} = F}, {Acc, S}) ->
                  case ets:lookup(S#?MODULE.tid, UId) of
                      [{UId, _, _, _, _, No, RecOff, _, _, _}] ->
                          case LiveFun(UId, Epoch) of
                              true ->
                                  {[{copy, No, RecOff, UId, Epoch,
                                     maps:get(idx, F), maps:get(term, F),
                                     maps:get(img_len, F),
                                     maps:get(idx_len, F),
                                     maps:get(body, F)} | Acc], S};
                              false ->
                                  {Acc, remove_entry(UId, S)}
                          end;
                      _ ->
                          {Acc, S}
                  end
          end, {[], State}, Recs),
    State2 = case Status of
                 cont ->
                     State1#?MODULE{retire = R#retire{off = Next},
                                    retire_failing = false};
                 io_error ->
                     %% could not read, try again from the same place later
                     ?ERROR("ra_log_snap_store: ~ts: read error retiring file "
                            "~b at offset ~b, will try again",
                            [State#?MODULE.name, No, Off]),
                     retire_later(State1);
                 _ ->
                     %% reached the end of the file (or an invalid record we
                     %% cannot get past). It is deleted once the copies are
                     %% durable, if nothing still refers to it
                     State1#?MODULE{retire = R#retire{off = Next,
                                                      limit = done},
                                    retire_failing = false}
             end,
    {lists:reverse(Copies), State2}.

%% back off rather than retrying in a loop
retire_later(#?MODULE{retire_token = true} = State) ->
    State;
retire_later(State) ->
    erlang:send_after(?RETIRE_RETRY_MS, self(), retire_step),
    State#?MODULE{retire_token = true, retire_failing = true,
                  retire_backoff = true}.

%% Called once a batch (including any copies) is durable.
finish_retire(#?MODULE{retire = #retire{no = No, fd = Fd, limit = done},
                       dir = Dir, rolled = Rolled, tid = Tid} = State) ->
    _ = close(Fd),
    Rolled1 = lists:keydelete(No, 1, Rolled),
    Refs = ets:select_count(Tid, [{{'_', '_', '_', '_', '_', No,
                                    '_', '_', '_', '_'}, [], [true]}]),
    case Refs of
        0 ->
            _ = file:delete(file_name(Dir, No)),
            _ = ra_lib:sync_dir(Dir),
            incr(retired_files,
                 State#?MODULE{retire = undefined, rolled = Rolled1});
        _ ->
            %% a record we could not get past (and so could not copy) is
            %% still in use. Keep the file, it is no longer retired.
            ?ERROR("ra_log_snap_store: ~ts: file ~b has ~b snapshots that "
                   "could not be copied out of it, keeping it",
                   [State#?MODULE.name, No, Refs]),
            incr(retire_blocked,
                 State#?MODULE{retire = undefined, rolled = Rolled1,
                               blocked = lists:usort([No | State#?MODULE.blocked])})
    end;
finish_retire(State) ->
    State.

%% The batch that should have made the copies durable failed. Go back to
%% re-reading the file from the start of the work in progress.
abandon_retire_progress(#?MODULE{retire = #retire{fd = Fd}} = State) ->
    _ = close(Fd),
    State#?MODULE{retire = undefined};
abandon_retire_progress(State) ->
    State.

%%%===================================================================
%%% recovery
%%%===================================================================

recover(#?MODULE{dir = Dir, tid = Tid} = State0) ->
    Nos = file_numbers(Dir),
    Headers = [{No, read_header(Dir, No)} || No <- Nos],
    %% the number of valid bytes of a file is recorded in the header of its
    %% successor
    Limits = maps:from_list(
               [{No - 1, PrevLen}
                || {No, {ok, #{prev_len := PrevLen}}} <- Headers,
                   PrevLen =/= ?UNKNOWN_LEN]),
    {Rolled, MaxNo, MaxSeq} =
        lists:foldl(
          fun ({No, {ok, #{next_seq := NextSeq}}}, {RAcc, MaxN, MaxS}) ->
                  Size = file_size(Dir, No),
                  Limit = min(Size, maps:get(No, Limits, Size)),
                  {Valid, Seen} = recover_file(State0, No, Limit),
                  {[{No, Valid} | RAcc], max(No, MaxN),
                   lists:max([MaxS, NextSeq, Seen + 1])};
              ({No, {error, {io, Reason}}}, _) ->
                  %% not being able to read a file says nothing about what is in
                  %% it, do not carry on as if it was damaged
                  error({snapshot_store_cannot_read_file, No, Reason});
              ({No, {error, {bad_header, Reason}}}, {RAcc, MaxN, MaxS}) ->
                  %% only the newest file can have been left half created,
                  %% anything else is damage and is set aside, not deleted
                  File = file_name(Dir, No),
                  case No == lists:last(Nos) andalso file_size(Dir, No) < ?ALIGN of
                      true ->
                          ?WARN("ra_log_snap_store: file ~b has an invalid "
                                "header (~w), deleting it", [No, Reason]),
                          _ = file:delete(File);
                      false ->
                          ?ERROR("ra_log_snap_store: invalid header in file "
                                 "~b: ~w, setting it aside as ~ts.bad",
                                 [No, Reason, File]),
                          _ = file:rename(File, [File, ".bad"])
                  end,
                  {RAcc, max(No, MaxN), MaxS}
          end, {[], 0, 1}, Headers),
    Live = ets:foldl(fun ({?DIR_KEY, _}, A) -> A;
                         (E, A) -> A + element(8, E)
                     end, 0, Tid),
    State0#?MODULE{rolled = lists:reverse(Rolled),
                   no = MaxNo,
                   seq = MaxSeq,
                   live_bytes = Live,
                   %% the highest file is not continued: a new file is
                   %% started with PrevLen set to its valid length so a torn
                   %% tail is never appended after
                   fd = undefined}.

%% Scans a file, publishing the newest records into the ETS table.
%% Returns the number of valid bytes and the highest sequence number seen.
recover_file(#?MODULE{dir = Dir} = State, No, Limit) ->
    case file:open(file_name(Dir, No), [read, raw, binary]) of
        {ok, Fd} ->
            try
                recover_loop(State, No, Fd, ?HDR_SIZE, Limit, 0)
            after
                _ = file:close(Fd)
            end;
        {error, Reason} ->
            error({snapshot_store_cannot_read_file, No, Reason})
    end.

recover_loop(State, No, Fd, Off, Limit, MaxSeq) ->
    {Recs, Next, Status, Skipped} = read_records(Fd, Off, Limit,
                                                 4 * 1024 * 1024),
    counters:add(State#?MODULE.cref, ?C_CORRUPT_RECORDS, Skipped),
    MaxSeq1 = lists:foldl(
                fun ({RecOff, TL, F}, Max) ->
                        recover_record(State, No, RecOff, TL, F),
                        max(Max, maps:get(seq, F))
                end, MaxSeq, Recs),
    case Status of
        cont ->
            recover_loop(State, No, Fd, Next, Limit, MaxSeq1);
        eof ->
            {Next, MaxSeq1};
        bad ->
            ?WARN("ra_log_snap_store: file ~b: invalid record at offset ~b, "
                  "ignoring the rest of the file", [No, Next]),
            {Next, MaxSeq1};
        io_error ->
            %% do not start with part of the data missing
            error({snapshot_store_read_error, No, Next})
    end.

recover_record(#?MODULE{tid = Tid, live_fun = LiveFun}, No, Off, TL,
               #{uid := UId, epoch := Epoch, idx := Idx, term := Term,
                 seq := Seq, img_len := ImgLen, idx_len := IdxLen}) ->
    case LiveFun(UId, Epoch) of
        true ->
            New = {UId, Epoch, Idx, Term, Seq, No, Off, TL, ImgLen, IdxLen},
            case ets:lookup(Tid, UId) of
                [Old] ->
                    case newer(New, Old) of
                        true -> ets:insert(Tid, New);
                        false -> ok
                    end;
                [] ->
                    ets:insert(Tid, New)
            end;
        false ->
            ok
    end.

%% within an incarnation the highest index (then sequence) wins, across
%% incarnations the later write does
newer({_, Epoch, Idx, _, Seq, _, _, _, _, _},
      {_, Epoch, OIdx, _, OSeq, _, _, _, _, _}) ->
    {Idx, Seq} > {OIdx, OSeq};
newer({_, _, _, _, Seq, _, _, _, _, _}, {_, _, _, _, OSeq, _, _, _, _, _}) ->
    Seq > OSeq.

%%%===================================================================
%%% reading
%%%===================================================================

read(_Name, _UId, _IdxTerm, 0) ->
    {error, retries_exhausted};
read(Name, UId, {Idx, Term} = IdxTerm, Retries) ->
    try ets:lookup(Name, UId) of
        [] ->
            {error, not_found};
        [{UId, _, EIdx, ETerm, _, _, _, _, _, _}]
          when {EIdx, ETerm} =/= {Idx, Term} ->
            {error, superseded};
        [{UId, _, _, _, _, No, Off, TL, ImgLen, IdxLen} = Entry] ->
            [{?DIR_KEY, Dir}] = ets:lookup(Name, ?DIR_KEY),
            case read_record(Dir, No, Off, TL, ImgLen, IdxLen) of
                {ok, Image, IndexesBin} ->
                    {ok, Image, binary_to_term(IndexesBin)};
                {error, _} ->
                    %% the file may have been retired and the entry moved
                    %% between our lookup and read, try again if so
                    case ets:lookup(Name, UId) of
                        [Entry] ->
                            {error, read_failed};
                        _ ->
                            read(Name, UId, IdxTerm, Retries - 1)
                    end
            end
    catch
        error:badarg ->
            {error, store_unavailable}
    end.

read_record(Dir, No, Off, TL, ImgLen, IdxLen) ->
    case file:open(file_name(Dir, No), [read, raw, binary]) of
        {ok, Fd} ->
            try file:pread(Fd, Off, TL) of
                {ok, <<?PUT:8, Len:32, Crc:32, Body:Len/binary>>}
                  when ?REC_HDR + Len =:= TL ->
                    case erlang:crc32(Body) of
                        Crc ->
                            <<_Seq:64, ULen:16, _:ULen/binary,
                              ELen:16, _:ELen/binary, _Idx:64, _Term:64,
                              ImgLen:32, IdxLen:32,
                              Image:ImgLen/binary,
                              Indexes:IdxLen/binary>> = Body,
                            {ok, Image, Indexes};
                        _ ->
                            {error, checksum_error}
                    end;
                {ok, _} ->
                    {error, invalid_record};
                eof ->
                    {error, eof};
                {error, _} = Err ->
                    Err
            after
                _ = file:close(Fd)
            end;
        {error, _} = Err ->
            Err
    end.

%%%===================================================================
%%% record parsing
%%%===================================================================

%% Reads up to `Chunk' bytes of whole records starting at `Off' (never past
%% `Limit'). Status is `cont' if more records may follow, `eof' if the limit
%% was reached or `bad' if an invalid or torn record was found (the rest of
%% the file is ignored). Returns the offset to continue from.
read_records(_Fd, Off, Limit, _Chunk) when Off >= Limit ->
    {[], Off, eof, 0};
read_records(Fd, Off, Limit, Chunk) ->
    ReadLen = min(Chunk, Limit - Off),
    case file:pread(Fd, Off, ReadLen) of
        {ok, Bin} ->
            case parse_records(Bin, 0, Off, Limit, [], 0) of
                {[], Next, more, _} when Next == Off ->
                    %% not even one whole record fit in the chunk, or the file
                    %% ends inside a record
                    case Bin of
                        <<_:8, Len:32, _:32, _/binary>>
                          when byte_size(Bin) == ReadLen,
                               ?REC_HDR + Len > ReadLen,
                               Off + ?REC_HDR + Len =< Limit ->
                            read_records(Fd, Off, Limit,
                                         ?REC_HDR + Len);
                        _ ->
                            {[], Off, bad, 0}
                    end;
                {Recs, Next, more, Skipped} ->
                    {Recs, Next, cont, Skipped};
                {Recs, Next, Status, Skipped} ->
                    {Recs, Next, Status, Skipped}
            end;
        eof ->
            {[], Off, bad, 0};
        {error, _} ->
            {[], Off, io_error, 0}
    end.

parse_records(Bin, Rel, Base, Limit, Acc, Skipped) ->
    Abs = Base + Rel,
    case Bin of
        _ when Abs >= Limit ->
            {lists:reverse(Acc), Abs, eof, Skipped};
        <<_:Rel/binary, Type:8, Len:32, Crc:32, Rest/binary>> ->
            TL = ?REC_HDR + Len,
            case Rest of
                _ when Abs + TL > Limit ->
                    {lists:reverse(Acc), Abs, bad, Skipped};
                <<Body:Len/binary, _/binary>> ->
                    case parse_body(Type, Len, Crc, Body) of
                        {put, Fields} ->
                            parse_records(Bin, Rel + TL, Base, Limit,
                                          [{Abs, TL, Fields} | Acc], Skipped);
                        pad ->
                            parse_records(Bin, Rel + TL, Base, Limit, Acc,
                                          Skipped);
                        bad when Type == ?PUT ->
                            %% a record that does not validate but whose
                            %% length fits: skip it, the ones after it are
                            %% independent
                            ?WARN("ra_log_snap_store: skipping invalid "
                                  "record at offset ~b", [Abs]),
                            parse_records(Bin, Rel + TL, Base, Limit, Acc,
                                          Skipped + 1);
                        bad ->
                            {lists:reverse(Acc), Abs, bad, Skipped}
                    end;
                _ ->
                    {lists:reverse(Acc), Abs, more, Skipped}
            end;
        _ ->
            {lists:reverse(Acc), Abs, more, Skipped}
    end.

parse_body(?PAD, Len, 0, _Body) when Len >= 1 ->
    pad;
parse_body(?PUT, _Len, Crc, Body) ->
    case erlang:crc32(Body) of
        Crc ->
            case Body of
                <<Seq:64, ULen:16, UId:ULen/binary, ELen:16, Epoch:ELen/binary,
                  Idx:64, Term:64, ImgLen:32, IdxLen:32, Payload/binary>>
                  when byte_size(Payload) == ImgLen + IdxLen ->
                    {put, #{seq => Seq, uid => UId, epoch => Epoch,
                            idx => Idx, term => Term, img_len => ImgLen,
                            idx_len => IdxLen, body => Body}};
                _ ->
                    bad
            end;
        _ ->
            bad
    end;
parse_body(_, _, _, _) ->
    bad.

%%%===================================================================
%%% files and io
%%%===================================================================

file_name(Dir, No) ->
    filename:join(Dir, io_lib:format("~8..0b.snap", [No])).

file_numbers(Dir) ->
    case prim_file:list_dir(Dir) of
        {ok, Files} ->
            lists:sort([N || F <- Files,
                             filename:extension(F) == ".snap",
                             {N, []} <- [string:to_integer(
                                           filename:rootname(F))]]);
        {error, Reason} ->
            error({snapshot_store_cannot_list_files, Dir, Reason})
    end.

%% An error here is not the same as an empty file: a snapshot log that can
%% not be read has to stop the system, not be taken for empty or damaged and
%% be deleted.
file_size(Dir, No) ->
    case prim_file:read_file_info(file_name(Dir, No)) of
        {ok, Info} -> element(2, Info);
        {error, Reason} -> error({snapshot_store_cannot_read_file, No, Reason})
    end.

read_header(Dir, No) ->
    case file:open(file_name(Dir, No), [read, raw, binary]) of
        {ok, Fd} ->
            try file:pread(Fd, 0, ?HDR_SIZE) of
                {ok, <<?MAGIC, ?VERSION:8, No:64, NextSeq:64, PrevLen:64,
                       Crc:32, _/binary>> = Bin} ->
                    <<Covered:29/binary, _/binary>> = Bin,
                    case erlang:crc32(Covered) of
                        Crc ->
                            {ok, #{next_seq => NextSeq, prev_len => PrevLen}};
                        _ ->
                            {error, {bad_header, bad_header_checksum}}
                    end;
                {ok, _} ->
                    {error, {bad_header, invalid_header}};
                eof ->
                    {error, {bad_header, truncated_header}};
                {error, Reason} ->
                    {error, {io, Reason}}
            after
                _ = file:close(Fd)
            end;
        {error, Reason} ->
            {error, {io, Reason}}
    end.

%% a directory sync that failed means a file in it may not be there after a
%% crash. Not all platforms can sync a directory.
sync_dir_strict(Dir) ->
    case ra_lib:sync_dir(Dir) of
        ok ->
            ok;
        {error, _} = Err ->
            case os:type() of
                {win32, _} -> ok;
                _ -> Err
            end
    end.

close(undefined) ->
    ok;
close(Fd) ->
    file:close(Fd).

io_create(#?MODULE{io = #{create := F}}, Path) -> F(Path);
io_create(_, Path) -> file:open(Path, [write, raw, binary]).

io_open_read(#?MODULE{io = #{open_read := F}}, Path) -> F(Path);
io_open_read(_, Path) -> file:open(Path, [read, raw, binary]).

io_pwrite(#?MODULE{io = #{pwrite := F}}, Fd, Off, IO) -> F(Fd, Off, IO);
io_pwrite(_, Fd, Off, IO) -> file:pwrite(Fd, Off, IO).

io_sync(#?MODULE{io = #{sync := F}}, Fd) -> F(Fd);
io_sync(_, Fd) -> ra_file:sync(Fd).

incr(Key, State) ->
    incr(Key, 1, State).

incr(Key, N, #?MODULE{cref = CRef} = State) ->
    counters:add(CRef, cidx(Key), N),
    State.

cidx(puts) -> ?C_PUTS;
cidx(batches) -> ?C_BATCHES;
cidx(bytes_written) -> ?C_BYTES_WRITTEN;
cidx(copies) -> ?C_COPIES;
cidx(rolls) -> ?C_ROLLS;
cidx(retired_files) -> ?C_RETIRED_FILES;
cidx(retire_blocked) -> ?C_RETIRE_BLOCKED;
cidx(errors) -> ?C_ERRORS;
cidx(stale_puts) -> ?C_STALE_PUTS;
cidx(corrupt_records) -> ?C_CORRUPT_RECORDS;
cidx(fsync_time_us) -> ?C_FSYNC_TIME_US.

%% registered with ra_counters like the counters of the WAL, falling back to
%% private ones when there is no seshat (e.g. unit tests of the store)
new_counters(Name, System) ->
    ?CATCH(ra_counters:delete(Name)),
    try
        ra_counters:new(Name, ?COUNTER_FIELDS,
                        #{ra_system => System, module => ?MODULE})
    catch
        _:_ ->
            counters:new(length(?COUNTER_FIELDS), [write_concurrency])
    end.
