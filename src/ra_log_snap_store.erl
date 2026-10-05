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

-define(MIN_FILE_BYTES, 4 * ?ALIGN).
-define(DEFAULT_MIN_FILE_BYTES, 64 * 1024 * 1024).
-define(DEFAULT_RETIRE_CHUNK, 1024 * 1024).
-define(MIN_RETIRE_CHUNK, 4096).
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
         seq = 1 :: non_neg_integer(),
         live_bytes = 0 :: non_neg_integer(),
         %% rolled files, oldest first, with the number of valid bytes
         rolled = [] :: [{non_neg_integer(), non_neg_integer()}],
         retire :: undefined | #retire{},
         retire_token = false :: boolean(),
         counters = #{} :: #{atom() => non_neg_integer()}}).

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

-spec info(atom()) -> map().
info(Name) ->
    gen_batch_server:call(Name, info, infinity).

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
    State0 = #?MODULE{name = Name,
                      dir = Dir,
                      tid = RecTid,
                      %% never so small that copying the live data forward
                      %% would roll the file again
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
    State1 = recover(State0),
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
            {ok, schedule_retire(State3)};
        {error, Reason, _} ->
            {stop, {cannot_create_snapshot_store_file, Reason}}
    end.

handle_batch(Ops, State0) ->
    {Replies, State} =
        lists:foldl(
          fun (Group, {Acc, S0}) ->
                  {R, S1} = handle_group(Group, S0),
                  {[R | Acc], S1}
          end, {[], State0}, split_at_barriers(Ops)),
    {ok, lists:append(lists:reverse(Replies)),
     schedule_retire(maybe_roll(State))}.

%% Reconciling and deleting a member are answered in order with the puts that
%% came before, and not after any that came later, so the operations of a batch
%% are handled in groups that end at one.
split_at_barriers(Ops) ->
    split_at_barriers(Ops, [], []).

split_at_barriers([], [], Groups) ->
    lists:reverse(Groups);
split_at_barriers([], Acc, Groups) ->
    lists:reverse([lists:reverse(Acc) | Groups]);
split_at_barriers([{call, _, {Barrier, _, _}} = Op | Rem], Acc, Groups)
  when Barrier == reconcile orelse Barrier == delete ->
    split_at_barriers(Rem, [], [lists:reverse([Op | Acc]) | Groups]);
split_at_barriers([Op | Rem], Acc, Groups) ->
    split_at_barriers(Rem, [Op | Acc], Groups).

handle_group(Ops, State0) ->
    {Puts, Others, Followers, State1} = classify(Ops, State0, [], [], [], #{}),
    %% without a usable file there is nowhere to copy to
    {Copies, State2} = case State1#?MODULE.fd of
                           undefined -> {[], State1};
                           _ -> retire_scan(State1)
                       end,
    {Replies0, State3} = write_batch(Copies, Puts, State2),
    %% a repeat of a put in the same batch gets the outcome of the put
    Outcomes = maps:from_list([{From, Reply}
                               || {reply, From, Reply} <- Replies0]),
    Replies1 = [{reply, From, case Outcomes of
                                 #{Leader := ok} -> Reply;
                                 #{Leader := Error} -> Error;
                                 _ -> Reply
                             end}
                || {Leader, From, Reply} <- Followers],
    {Replies2, State4} = run_others(Others, State3),
    {Replies0 ++ Replies1 ++ Replies2, State4}.

terminate(_Reason, #?MODULE{fd = Fd, retire = Retire,
                            registry_key = RegKey}) ->
    RegKey == undefined orelse persistent_term:erase(RegKey),
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

%% Splits a group of operations, in arrival order, into the puts that need to
%% be written and the other operations that are answered after them. A put
%% that repeats, or is stale compared to, one earlier in the same batch is a
%% "follower" of it: it is answered with the outcome of the earlier one, as
%% nothing is durable yet.
classify([], State, Puts, Others, Followers, _Pending) ->
    {lists:reverse(Puts), lists:reverse(Others), lists:reverse(Followers),
     State};
classify([{call, From, {put, UId, Epoch, Idx, Term, Image, Indexes}} | Rem],
         State, Puts, Others, Followers, Pending) ->
    {Current, Leader} = case Pending of
                            #{UId := {E, I, T, L}} -> {{E, I, T}, L};
                            _ -> {current(UId, State), undefined}
                        end,
    case put_decision(Current, Epoch, Idx, Term) of
        write ->
            Item = {put, From, UId, Epoch, Idx, Term, Image, Indexes},
            classify(Rem, State, [Item | Puts], Others, Followers,
                     Pending#{UId => {Epoch, Idx, Term, From}});
        ok when Leader == undefined ->
            classify(Rem, State, Puts, [{reply, From, ok} | Others],
                     Followers, Pending);
        ok ->
            classify(Rem, State, Puts, Others,
                     [{Leader, From, ok} | Followers], Pending);
        stale when Leader == undefined ->
            classify(Rem, incr(stale_puts, State), Puts,
                     [{reply, From, {error, stale}} | Others], Followers,
                     Pending);
        stale ->
            classify(Rem, incr(stale_puts, State), Puts, Others,
                     [{Leader, From, {error, stale}} | Followers], Pending)
    end;
classify([{call, From, {reconcile, UId, Epoch}} | Rem], State, Puts,
         Others, Followers, Pending) ->
    classify(Rem, State, Puts,
             [{reconcile, From, UId, Epoch} | Others], Followers, Pending);
classify([{call, From, {delete, UId, Epoch}} | Rem], State, Puts,
         Others, Followers, Pending) ->
    classify(Rem, State, Puts,
             [{delete, From, UId, Epoch} | Others], Followers,
             maps:remove(UId, Pending));
classify([{call, From, info} | Rem], State, Puts, Others, Followers,
         Pending) ->
    classify(Rem, State, Puts, [{info, From} | Others], Followers, Pending);
classify([{call, From, _Unknown} | Rem], State, Puts, Others, Followers,
         Pending) ->
    classify(Rem, State, Puts, [{reply, From, {error, unknown_request}} | Others],
             Followers, Pending);
classify([{cast, {release, UId, IdxTerm}} | Rem], State, Puts, Others,
         Followers, Pending) ->
    classify(Rem, State, Puts, [{release, UId, IdxTerm} | Others], Followers,
             Pending);
classify([{info, retire_step} | Rem], State, Puts, Others, Followers,
         Pending) ->
    classify(Rem, State#?MODULE{retire_token = false}, Puts, Others,
             Followers, Pending);
classify([_ | Rem], State, Puts, Others, Followers, Pending) ->
    classify(Rem, State, Puts, Others, Followers, Pending).

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
write_batch(_Copies, [], #?MODULE{fd = undefined} = State) ->
    %% nothing to write and no usable file, don't churn trying to make one
    {[], State};
write_batch(_Copies, Puts, #?MODULE{fd = undefined} = State0) ->
    %% no usable file: try to get one, otherwise fail the puts
    State = abandon_retire_progress(State0),
    case new_active(State) of
        {ok, State1} ->
            write_batch([], Puts, State1);
        {error, Reason, State1} ->
            {[{reply, From, {error, Reason}}
              || {put, From, _, _, _, _, _, _} <- Puts],
             incr(errors, State1)}
    end;
write_batch([], [], #?MODULE{} = State) ->
    {[], finish_retire(State)};
write_batch(Copies, Puts, #?MODULE{no = No, off = Off0, seq = Seq0,
                                   fd = Fd} = State0) ->
    Items = [{copy, C} || C <- Copies] ++ [{put, P} || P <- Puts],
    {IO, Applies, Off1, Seq1} = encode_items(Items, No, Off0, Seq0),
    Bytes = Off1 - Off0,
    case do_write(State0, Fd, Off0, IO) of
        ok ->
            State1 = apply_entries(Applies, State0),
            State2 = State1#?MODULE{off = Off1,
                                    seq = Seq1},
            State3 = incr(puts, length(Puts),
                          incr(copies, length(Copies),
                               incr(bytes_written, Bytes,
                                    incr(batches, State2)))),
            {[{reply, From, ok} || {put, From, _, _, _, _, _, _} <- Puts],
             finish_retire(State3)};
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
            {[{reply, From, {error, Reason}}
              || {put, From, _, _, _, _, _, _} <- Puts],
             incr(errors, State2)}
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

run_others(Others, State) ->
    {Replies, State1} = lists:foldl(
      fun ({reply, _From, _Reply} = R, {Acc, S}) ->
              {[R | Acc], S};
          ({reconcile, From, UId, Epoch}, {Acc, S}) ->
              case ets:lookup(S#?MODULE.tid, UId) of
                  [{UId, Epoch, Idx, Term, _, _, _, _, _, _}] ->
                      {[{reply, From, {ok, #{idx => Idx,
                                            term => Term}}} | Acc], S};
                  [{UId, _OtherEpoch, _, _, _, _, _, _, _, _}] ->
                      {[{reply, From, not_found} | Acc],
                       remove_entry(UId, S)};
                  [] ->
                      {[{reply, From, not_found} | Acc], S}
              end;
          ({delete, From, UId, Epoch}, {Acc, S}) ->
              S1 = case ets:lookup(S#?MODULE.tid, UId) of
                       [{UId, EntryEpoch, _, _, _, _, _, _, _, _}]
                         when Epoch == any orelse Epoch == EntryEpoch ->
                           remove_entry(UId, S);
                       _ ->
                           S
                   end,
              {[{reply, From, ok} | Acc], S1};
          ({release, UId, {Idx, Term}}, {Acc, S}) ->
              S1 = case ets:lookup(S#?MODULE.tid, UId) of
                       [{UId, _, Idx, Term, _, _, _, _, _, _}] ->
                           remove_entry(UId, S);
                       _ ->
                           S
                   end,
              {Acc, S1};
          ({info, From}, {Acc, S}) ->
              {[{reply, From, do_info(S)} | Acc], S}
      end, {[], State}, Others),
    {lists:reverse(Replies), State1}.

do_info(#?MODULE{live_bytes = Live, off = Off, no = No, rolled = Rolled,
                 counters = Counters, retire = Retire, fd = Fd}) ->
    Counters#{live_bytes => Live,
              active_file => No,
              active_offset => Off,
              rolled_files => length(Rolled),
              retiring => Retire =/= undefined,
              has_active_file => Fd =/= undefined}.

%%%===================================================================
%%% rolling and retiring
%%%===================================================================

maybe_roll(#?MODULE{fd = Fd, off = Off, live_bytes = Live,
                    min_file_bytes = Min} = State)
  when Fd =/= undefined andalso Off >= max(Min, 2 * Live) ->
    case new_active(close_active(State, Off)) of
        {ok, State1} ->
            incr(rolls, State1);
        {error, _Reason, State1} ->
            %% no active file, the next batch tries again
            State1
    end;
maybe_roll(State) ->
    State.

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
            {ok, State#?MODULE{no = No, fd = Fd, off = ?HDR_SIZE}};
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
        {ok, State1} -> State1;
        {error, _Reason, State1} -> State1
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
retire_scan(#?MODULE{retire = undefined, rolled = []} = State) ->
    {[], State};
retire_scan(#?MODULE{retire = undefined, rolled = [{No, Limit} | _],
                     dir = Dir} = State) ->
    case io_open_read(State, file_name(Dir, No)) of
        {ok, Fd} ->
            retire_scan(State#?MODULE{retire = #retire{no = No, fd = Fd,
                                                       off = ?HDR_SIZE,
                                                       limit = Limit}});
        {error, enoent} ->
            %% already gone
            retire_scan(State#?MODULE{rolled = tl(State#?MODULE.rolled)});
        {error, Reason} ->
            ?ERROR("ra_log_snap_store: ~ts: cannot open file ~b to retire: ~w, "
                   "will try again", [State#?MODULE.name, No, Reason]),
            {[], retire_later(State)}
    end;
retire_scan(#?MODULE{retire = #retire{no = No, fd = Fd, off = Off,
                                      limit = Limit} = R,
                     retire_chunk = Chunk, live_fun = LiveFun} = State) ->
    {Recs, Next, Status} = read_records(Fd, Off, Limit, Chunk),
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
                     State1#?MODULE{retire = R#retire{off = Next}};
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
                                                      limit = done}}
             end,
    {lists:reverse(Copies), State2}.

%% back off rather than retrying in a loop
retire_later(#?MODULE{retire_token = true} = State) ->
    State;
retire_later(State) ->
    erlang:send_after(?RETIRE_RETRY_MS, self(), retire_step),
    State#?MODULE{retire_token = true}.

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
                 State#?MODULE{retire = undefined, rolled = Rolled1})
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
              ({No, {error, Reason}}, {RAcc, MaxN, MaxS}) ->
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
            ?ERROR("ra_log_snap_store: cannot open file ~b: ~w", [No, Reason]),
            {0, 0}
    end.

recover_loop(State, No, Fd, Off, Limit, MaxSeq) ->
    {Recs, Next, Status} = read_records(Fd, Off, Limit, 4 * 1024 * 1024),
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
    {[], Off, eof};
read_records(Fd, Off, Limit, Chunk) ->
    ReadLen = min(Chunk, Limit - Off),
    case file:pread(Fd, Off, ReadLen) of
        {ok, Bin} ->
            case parse_records(Bin, 0, Off, Limit, []) of
                {[], Next, more} when Next == Off ->
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
                            {[], Off, bad}
                    end;
                {Recs, Next, more} ->
                    {Recs, Next, cont};
                {Recs, Next, Status} ->
                    {Recs, Next, Status}
            end;
        eof ->
            {[], Off, bad};
        {error, _} ->
            {[], Off, io_error}
    end.

parse_records(Bin, Rel, Base, Limit, Acc) ->
    Abs = Base + Rel,
    case Bin of
        _ when Abs >= Limit ->
            {lists:reverse(Acc), Abs, eof};
        <<_:Rel/binary, Type:8, Len:32, Crc:32, Rest/binary>> ->
            TL = ?REC_HDR + Len,
            case Rest of
                _ when Abs + TL > Limit ->
                    {lists:reverse(Acc), Abs, bad};
                <<Body:Len/binary, _/binary>> ->
                    case parse_body(Type, Len, Crc, Body) of
                        {put, Fields} ->
                            parse_records(Bin, Rel + TL, Base, Limit,
                                          [{Abs, TL, Fields} | Acc]);
                        pad ->
                            parse_records(Bin, Rel + TL, Base, Limit, Acc);
                        bad when Type == ?PUT ->
                            %% a record that does not validate but whose
                            %% length fits: skip it, the ones after it are
                            %% independent
                            ?WARN("ra_log_snap_store: skipping invalid "
                                  "record at offset ~b", [Abs]),
                            parse_records(Bin, Rel + TL, Base, Limit, Acc);
                        bad ->
                            {lists:reverse(Acc), Abs, bad}
                    end;
                _ ->
                    {lists:reverse(Acc), Abs, more}
            end;
        _ ->
            {lists:reverse(Acc), Abs, more}
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

file_size(Dir, No) ->
    case prim_file:read_file_info(file_name(Dir, No)) of
        {ok, Info} -> element(2, Info);
        {error, _} -> 0
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
                            {error, bad_header_checksum}
                    end;
                {ok, _} ->
                    {error, invalid_header};
                eof ->
                    {error, truncated_header};
                {error, _} = Err ->
                    Err
            after
                _ = file:close(Fd)
            end;
        {error, _} = Err ->
            Err
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

incr(Key, N, #?MODULE{counters = C} = State) ->
    State#?MODULE{counters = C#{Key => maps:get(Key, C, 0) + N}}.
