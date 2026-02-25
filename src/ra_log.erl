%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2017-2025 Broadcom. All Rights Reserved. The term Broadcom refers to Broadcom Inc. and/or its subsidiaries.
%%
%% @hidden
-module(ra_log).

-include_lib("stdlib/include/assert.hrl").
-compile([inline_list_funcs]).

-export([pre_init/1,
         init/1,
         close/1,
         begin_tx/1,
         commit_tx/1,
         append/2,
         write/2,
         write_sparse/3,
         append_sync/2,
         write_sync/2,
         fold/5,
         fold/6,
         sparse_read/2,
         partial_read/3,
         execute_read_plan/4,
         read_plan_info/1,
         previous_wal_index/1,
         last_index_term/1,
         set_last_index/2,
         handle_event/2,
         last_written/1,
         fetch/2,
         fetch_term/2,
         next_index/1,
         snapshot_state/1,
         set_snapshot_state/2,
         install_snapshot/4,
         recover_snapshot/1,
         snapshot_index_term/1,
         update_release_cursor/5,
         checkpoint/5,
         promote_checkpoint/2,

         can_write/1,
         exists/2,
         has_pending/1,
         overview/1,
         %% config
         write_config/2,
         read_config/1,

         delete_everything/1,
         release_resources/3,

         tick/2,
         assert/1
        ]).

-include("ra.hrl").

-define(DEFAULT_RESEND_WINDOW_SEC, 20).
-define(MIN_SNAPSHOT_INTERVAL, 4096).
-define(MIN_CHECKPOINT_INTERVAL, 16384).
-define(LOG_APPEND_TIMEOUT, 5000).
-define(WAL_RESEND_TIMEOUT, 5000).
-define(ETSTBL, ra_log_snapshot_state).

-type ra_meta_key() :: atom().
-type segment_ref() :: {File :: binary(), ra_range:range()}.
-type event_body() :: {written, ra_term(), ra_seq:state()} |
                      {segments, [{ets:tid(), ra:range()}], [segment_ref()]} |
                      {resend_write, ra_index()} |
                      {snapshot_written, ra_idxterm(),
                       LiveIndexes :: ra_seq:state(),
                       ra_snapshot:kind(),
                       Duration :: non_neg_integer()} |
                      {compaction_result, term()} |
                      major_compaction |
                      {down, pid(), term()}.

-type event() :: {ra_log_event, event_body()}.
-type transform_fun() :: fun ((ra_index(), ra_term(), ra_server:command()) -> term()).

-type effect() ::
    {delete_snapshot, Dir :: file:filename_all(), ra_idxterm()} |
    {monitor, process, log, pid()} |
    ra_snapshot:effect() |
    ra_server:effect().
%% logs can have effects too so that they can be coordinated with other state
%% such as avoiding to delete old snapshots whilst they are still being
%% replicated

-type effects() :: [effect()].

-record(cfg, {uid :: ra_uid(),
              log_id :: unicode:chardata(),
              directory :: file:filename_all(),
              min_snapshot_interval = ?MIN_SNAPSHOT_INTERVAL :: non_neg_integer(),
              min_checkpoint_interval = ?MIN_CHECKPOINT_INTERVAL :: non_neg_integer(),
              snapshot_module :: module(),
              resend_window_seconds = ?DEFAULT_RESEND_WINDOW_SEC :: integer(),
              wal :: atom(),
              segment_writer :: atom(),
              counter :: undefined | counters:counters_ref(),
              names :: ra_system:names()}).

-record(?MODULE,
        {cfg = #cfg{},
         %% mutable data below
         range :: ra:range(),
         last_term = 0 :: ra_term(),
         last_written_index_term = {0, 0} :: ra_idxterm(),
         snapshot_state :: ra_snapshot:state(),
         current_snapshot :: option(ra_idxterm()),
         last_resend_time :: option({integer(), WalPid :: pid() | undefined}),
         last_wal_write :: {pid(), Ms :: integer(), ra:index() | -1},
         reader :: ra_log_segments:state(),
         mem_table :: ra_mt:state(),
         tx = false :: false | {true, ra:range()},
         pending = [] :: ra_seq:state(),
         live_indexes = [] :: ra_seq:state()
        }).

-record(read_plan, {dir :: file:filename_all(),
                    read :: #{ra_index() := log_entry()},
                    plan :: ra_log_segments:read_plan()}).

-opaque read_plan() :: #read_plan{}.
-opaque state() :: #?MODULE{}.

-type ra_log_init_args() :: #{uid := ra_uid(),
                              system_config => ra_system:config(),
                              log_id => unicode:chardata(),
                              %% Deprecated in favor of `min_snapshot_interval'
                              %% but this value is used as a fallback if
                              %% `min_snapshot_interval' is not provided.
                              snapshot_interval => non_neg_integer(),
                              min_snapshot_interval => non_neg_integer(),
                              min_checkpoint_interval => non_neg_integer(),
                              resend_window => integer(),
                              max_open_segments => non_neg_integer(),
                              snapshot_module => module(),
                              counter => counters:counters_ref(),
                              initial_access_pattern => sequential | random,
                              max_checkpoints => non_neg_integer(),
                              major_compaction_strategy =>
                                  ra_log_segments:major_compaction_strategy()}.


-type overview() ::
    #{type := ra_log,
      range := ra:range(),
      last_index := ra:index(),
      last_term := ra_term(),
      last_written_index_term := ra_idxterm(),
      num_segments := non_neg_integer(),
      open_segments => non_neg_integer(),
      snapshot_index => undefined | ra_index(),
      snapshot_term => undefined | ra_index(),
      mem_table_size => non_neg_integer(),
      latest_checkpoint_index => undefined | ra_index(),
      atom() => term()}.

-export_type([state/0,
              read_plan/0,
              ra_log_init_args/0,
              ra_meta_key/0,
              segment_ref/0,
              event/0,
              event_body/0,
              effect/0,
              overview/0
             ]).

-define(SNAPSHOTS_DIR, <<"snapshots">>).
-define(CHECKPOINTS_DIR, <<"checkpoints">>).
-define(RECOVERY_CHECKPOINT_DIR, <<"recovery_checkpoint">>).

pre_init(#{uid := UId,
           system_config := #{data_dir := DataDir}} = Conf) ->
    Dir = server_data_dir(DataDir, UId),
    SnapModule = maps:get(snapshot_module, Conf, ?DEFAULT_SNAPSHOT_MODULE),
    MaxCheckpoints = maps:get(max_checkpoints, Conf, ?DEFAULT_MAX_CHECKPOINTS),
    SnapshotsDir = filename:join(Dir, ?SNAPSHOTS_DIR),
    CheckpointsDir = filename:join(Dir, ?CHECKPOINTS_DIR),
    RecoveryCheckpointDir = filename:join(Dir, ?RECOVERY_CHECKPOINT_DIR),
    _ = ra_snapshot:init(UId, SnapModule, SnapshotsDir,
                         CheckpointsDir, RecoveryCheckpointDir, undefined, MaxCheckpoints),
    ok.

-spec init(ra_log_init_args()) -> state().
init(#{uid := UId,
       system_config := #{data_dir := DataDir,
                          names := #{wal := Wal,
                                     segment_writer := SegWriter} = Names}
      } = Conf) ->
    Dir = server_data_dir(DataDir, UId),
    MaxOpen = maps:get(max_open_segments, Conf, 1),
    SnapModule = maps:get(snapshot_module, Conf, ?DEFAULT_SNAPSHOT_MODULE),
    %% this has to be patched by ra_server
    LogId = maps:get(log_id, Conf, UId),
    ResendWindow = maps:get(resend_window, Conf, ?DEFAULT_RESEND_WINDOW_SEC),
    SnapInterval = maps:get(min_snapshot_interval, Conf,
                            maps:get(snapshot_interval, Conf,
                                     ?MIN_SNAPSHOT_INTERVAL)),
    CPInterval = maps:get(min_checkpoint_interval, Conf,
                          ?MIN_CHECKPOINT_INTERVAL),
    MaxCheckpoints = maps:get(max_checkpoints, Conf, ?DEFAULT_MAX_CHECKPOINTS),
    SnapshotsDir = filename:join(Dir, ?SNAPSHOTS_DIR),
    CheckpointsDir = filename:join(Dir, ?CHECKPOINTS_DIR),
    RecoveryCheckpointDir = filename:join(Dir, ?RECOVERY_CHECKPOINT_DIR),
    Counter = maps:get(counter, Conf, undefined),

    %% ensure directories are there
    ok = ra_lib:make_dir(Dir),
    ok = ra_lib:make_dir(SnapshotsDir),
    ok = ra_lib:make_dir(CheckpointsDir),
    ok = ra_lib:make_dir(RecoveryCheckpointDir),
    % initialise metrics for this server
    SyncServer = maps:get(log_sync, Names, undefined),
    SnapshotState = ra_snapshot:init(UId, SnapModule, SnapshotsDir,
                                     CheckpointsDir, RecoveryCheckpointDir,
                                     SyncServer, Counter, MaxCheckpoints),
    {SnapIdx, SnapTerm} = case ra_snapshot:current(SnapshotState) of
                              undefined -> {-1, 0};
                              Curr -> Curr
                          end,
    %% Live indexes are already loaded into ETS by ra_snapshot:init
    LiveIndexes = ra_log_snapshot_state:live_indexes(?ETSTBL, UId),

    AccessPattern = maps:get(initial_access_pattern, Conf, sequential),
    {ok, Mt0} = ra_log_ets:mem_table_please(Names, UId),
    % recover current range and any references to segments
    % this queries the segment writer and thus blocks until any
    % segments it is currently processed have been finished
    MtRange = ra_mt:range(Mt0),
    ok = ra_log_segments:purge_dangling_symlinks(Dir),
    SegRefs = my_segrefs(UId, SegWriter),
    SegmentMaxCount = maps:get(segment_max_entries, Conf, ?SEGMENT_MAX_ENTRIES),
    SegmentMaxSize = maps:get(segment_max_size_bytes, Conf, ?SEGMENT_MAX_SIZE_B),
    MajorCompStrat = maps:get(major_compaction_strategy, Conf,
                              ?DEF_MAJOR_COMPACTION_STRAT),
    CompConf = #{max_size => SegmentMaxSize,
                 major_strategy => MajorCompStrat,
                 max_count => SegmentMaxCount},
    Reader = ra_log_segments:init(UId, Dir, MaxOpen, AccessPattern, SegRefs,
                                  Counter, CompConf, LogId),
    SegmentRange = ra_log_segments:range(Reader),
    %% The ranges can be sparse at this point so ra_range:add/2 does
    %% not do the right thing here as it requires a contiguous range
    Range = ra_range:combine(MtRange, SegmentRange),

    %% if the mt range contains indexes that overwrite part of the segment
    %% range we need to truncate the segment range to the index before
    %% the first mem table index
    TruncSegmentRange = case MtRange of
                            undefined ->
                                SegmentRange;
                            {FstMtIdx, _} ->
                                ra_range:limit(FstMtIdx, SegmentRange)
                        end,

    case ra_range:overlap(MtRange, SegmentRange) of
        {_, _} = Overlap ->
            ?INFO("~ts: ra_log:init/1 mem table and segment ranges overlap ~w"
                  "mem table range ~w, segment range ~w",
                  [LogId, Overlap, MtRange, SegmentRange]);
        _ -> ok
    end,
    %% TODO: if MtRange and SegmentRange overlaps check if the overlap exists in the
    %% mt and if it is the same in the segments, if so we can set first on the
    %% mt to match the end + 1 of the SegmentRange

    [begin
         ?DEBUG("~ts: deleting overwritten segment ~w",
                [LogId, SR]),
         _ = catch prim_file:delete(filename:join(Dir, F)),
         ok
     end
     || {F, _} = SR <- SegRefs -- ra_log_segments:segment_refs(Reader)],

    %% assert there is no gap between the snapshot
    %% and the first index in the log
    Mt = case Range of
             undefined ->
                 Mt0;
             {FstIdx, LstIdx} ->
                 case FstIdx == SnapIdx + 1 orelse
                      ra_range:in(SnapIdx, Range) orelse
                      SnapIdx > LstIdx of
                     true ->
                         {DeleteSpecs, Mt1} = ra_mt:set_first(FstIdx, Mt0),
                         ok = exec_mem_table_delete(Names, UId, DeleteSpecs),
                         Mt1;
                     false ->
                         exit({corrupt_log,
                               gap_between_snapshot_and_log_range,
                               {SnapIdx, Range}})
                 end
         end,
    LastWalIdx = case ra_log_wal:last_writer_seq(Wal, UId) of
                     {ok, undefined} ->
                         -1;
                     {ok, Idx} ->
                         Idx;
                     {error, wal_down} ->
                         %% TODO: we could enter a condition loop here to
                         %% wait for the WAL to come back
                         ?ERROR("~ts: ra_log:init/1 cannot complete as wal"
                                " process is down.",
                                [LogId]),
                         exit(wal_down)
                     end,

    %% recover the pending seq
    MaxConfirmedWrittenIdx = case TruncSegmentRange of
                                 {_, LastSegIdx} ->
                                     max(LastWalIdx, LastSegIdx);
                                 _ ->
                                     max(LastWalIdx, 0)
                             end,
    Pending = ra_seq:floor(MaxConfirmedWrittenIdx + 1, ra_mt:indexes(Mt)),
    ?DEBUG_IF(Pending =/= [],
              "~ts: recovered pending indexes ~w",
              [LogId, Pending]),
    Cfg = #cfg{directory = Dir,
               uid = UId,
               log_id = LogId,
               min_snapshot_interval = SnapInterval,
               min_checkpoint_interval = CPInterval,
               wal = Wal,
               segment_writer = SegWriter,
               resend_window_seconds = ResendWindow,
               snapshot_module = SnapModule,
               counter = Counter,
               names = Names},
    State0 = #?MODULE{cfg = Cfg,
                      range = ra_range:truncate(SnapIdx, Range),
                      reader = Reader,
                      mem_table = Mt,
                      snapshot_state = SnapshotState,
                      current_snapshot = ra_snapshot:current(SnapshotState),
                      last_wal_write = {whereis(Wal), now_ms(), LastWalIdx},
                      live_indexes = LiveIndexes,
                      pending = Pending
                     },
    put_counter(Cfg, ?C_RA_SVR_METRIC_SNAPSHOT_INDEX, SnapIdx),
    LastIdx = case Range of
                  undefined ->
                      SnapIdx;
                  {_, Lst} ->
                      Lst
              end,
    put_counter(Cfg, ?C_RA_SVR_METRIC_LAST_INDEX, LastIdx),
    put_counter(Cfg, ?C_RA_SVR_METRIC_LAST_WRITTEN_INDEX, LastIdx),
    put_counter(Cfg, ?C_RA_SVR_METRIC_NUM_SEGMENTS,
                ra_log_segments:segment_ref_count(Reader)),
    case ra_snapshot:latest_checkpoint(SnapshotState) of
        undefined ->
            ok;
        {ChIdx, _ChTerm} ->
            put_counter(Cfg, ?C_RA_SVR_METRIC_CHECKPOINT_INDEX, ChIdx)
    end,

    % recover the last term
    {LastTerm0, State2} = case Range of
                              undefined ->
                                  {SnapTerm, State0};
                              {_, LI} ->
                                  fetch_term(LI, State0)
                          end,
    LastSegRefIdx = case TruncSegmentRange of
                        undefined ->
                            -1;
                        {_, L} ->
                            L
                    end,
    LastWrittenIdx = lists:max([LastWalIdx, SnapIdx, LastSegRefIdx]),
    {LastWrittenTerm, State3} = case LastWrittenIdx of
                                    SnapIdx ->
                                        {SnapTerm, State2};
                                    _ ->
                                        fetch_term(LastWrittenIdx, State2)
                                end,

    LastTerm = ra_lib:default(LastTerm0, -1),
    State4 = State3#?MODULE{last_term = LastTerm,
                            last_written_index_term =
                                {LastWrittenIdx, LastWrittenTerm}},

    % initialized with a default 0 index 0 term dummy value
    % and an empty meta data map
    State = maybe_append_first_entry(State4),
    ?DEBUG("~ts: ra_log:init recovered last_index_term ~w"
           " snapshot_index_term ~w, last_written_index_term ~w",
           [LogId, last_index_term(State), {SnapIdx, SnapTerm},
            State#?MODULE.last_written_index_term
           ]),
    assert(resend_pending(LastWalIdx, State)).

-spec close(state()) -> ok.
close(#?MODULE{cfg = #cfg{uid = _UId},
               reader = Reader}) ->
    % deliberately ignoring return value
    % close all open segments
    _ = ra_log_segments:close(Reader),
    ok.

-spec begin_tx(state()) -> state().
begin_tx(State) ->
    State#?MODULE{tx = {true, undefined}}.

-spec commit_tx(state()) -> {ok, state()} | {error, wal_down, state()}.
commit_tx(#?MODULE{cfg = #cfg{uid = UId,
                              wal = Wal} = Cfg,
                   tx = {true, TxRange},
                   range = Range,
                   pending = Pend0,
                   mem_table = Mt1} = State) ->
    %% TODO: staged could contain entries from previous? I don't think that is
    %% ever the case as that would mean overwriting withing a single append batch
    Entries = ra_mt:staged(Mt1),
    Tid = ra_mt:tid(Mt1),
    WriterId = {UId, self()},
    PrevIdx = previous_wal_index(State),
    {WalCommands, Num, _} =
        lists:foldl(fun ({Idx, Term, Cmd0}, {WC, N, Prev}) ->
                            Cmd = {ttb, term_to_iovec(Cmd0)},
                            WalC = {append, WriterId, Tid, Prev, Idx, Term, Cmd},
                            {[WalC | WC], N+1, Idx}
                    end, {[], 0, PrevIdx}, Entries),
    {_, LastIdx} = TxRange,

    case ra_log_wal:write_batch(Wal, lists:reverse(WalCommands)) of
        {ok, Pid} ->
            %% commit after send to WAL, else abort
            {_, Mt} = ra_mt:commit(Mt1),
            ok = incr_counter(Cfg, ?C_RA_LOG_WRITE_OPS, Num),
            {ok, State#?MODULE{tx = false,
                               range = ra_range:add(TxRange, Range),
                               last_wal_write = {Pid, now_ms(), LastIdx},
                               mem_table = Mt}};
        {error, wal_down} ->
            {Idx, _, _} = hd(Entries),
            Mt = ra_mt:abort(Mt1),
            %% TODO: review this -  still need to return the state here
            {error, wal_down,
             State#?MODULE{tx = false,
                           pending = ra_seq:limit(Idx - 1, Pend0),
                           mem_table = Mt}}
    end;
commit_tx(#?MODULE{tx = false} = State) ->
    State.

-define(IS_NEXT_IDX(Idx, Range),
        Range == undefined orelse
        element(2, Range) + 1 =:= Idx).

-define(IS_IN_RANGE(Idx, Range),
        Range =/= undefined andalso
        Idx >= element(1, Range) andalso
        Idx =< element(2, Range)).

-spec append(Entry :: log_entry(), State :: state()) ->
    state() | no_return().
append({Idx, Term, Cmd0} = Entry,
       #?MODULE{cfg = #cfg{uid = UId,
                           wal = Wal} = Cfg,
                range = Range,
                tx = false,
                pending = Pend0,
                mem_table = Mt0} = State)
      when ?IS_NEXT_IDX(Idx, Range) ->
    case ra_mt:insert(Entry, Mt0) of
        {ok, Mt} ->
            Cmd = {ttb, term_to_iovec(Cmd0)},
            case ra_log_wal:write(Wal, {UId, self()}, ra_mt:tid(Mt),
                                  previous_wal_index(State),
                                  Idx, Term, Cmd) of
                {ok, Pid} ->
                    Pend = ra_seq:limit(Idx - 1, Pend0),
                    ok = incr_counter(Cfg, ?C_RA_LOG_WRITE_OPS, 1),
                    put_counter(Cfg, ?C_RA_SVR_METRIC_LAST_INDEX, Idx),
                    State#?MODULE{range = ra_range:extend(Idx, Range),
                                  last_term = Term,
                                  last_wal_write = {Pid, now_ms(), Idx},
                                  pending = ra_seq:append(Idx, Pend),
                                  mem_table = Mt};
                {error, wal_down} ->
                    error(wal_down)
            end;
        {error, Reason} ->
            ?DEBUG("~ts: mem table ~s detected appending index ~b, "
                   "opening new mem table",
                   [Cfg#cfg.log_id, Reason, Idx]),
            %% this function uses the infinity timeout
            {ok, M0} = ra_log_ets:new_mem_table_please(Cfg#cfg.names,
                                                       Cfg#cfg.uid, Mt0),
            append(Entry, State#?MODULE{mem_table = M0})
    end;
append({Idx, Term, _Cmd} = Entry,
       #?MODULE{cfg = Cfg,
                tx = {true, TxRange},
                pending = Pend0,
                mem_table = Mt0} = State)
      when ?IS_NEXT_IDX(Idx, TxRange) ->
    case ra_mt:stage(Entry, Mt0) of
        {ok, Mt} ->
            put_counter(Cfg, ?C_RA_SVR_METRIC_LAST_INDEX, Idx),
            State#?MODULE{tx = {true, ra_range:extend(Idx, TxRange)},
                          last_term = Term,
                          pending = ra_seq:append(Idx, Pend0),
                          mem_table = Mt};
        {error, Reason} ->
            ?DEBUG("~ts: mem table ~s detected appending index ~b, tx=true "
                   "opening new mem table",
                   [Cfg#cfg.log_id, Reason, Idx]),
            %% this function uses the infinity timeout
            {ok, M0} = ra_log_ets:new_mem_table_please(Cfg#cfg.names,
                                                       Cfg#cfg.uid, Mt0),
            append(Entry, State#?MODULE{mem_table = M0})
    end;
append({Idx, _, _}, #?MODULE{range = Range,
                             tx = Tx}) ->
    Msg = lists:flatten(io_lib:format("tried writing ~b - current range ~w tx ~p",
                                      [Idx, Range, Tx])),
    exit({integrity_error, Msg}).

-spec write(Entries :: [log_entry()], State :: state()) ->
    {ok, state()} |
    {error, {integrity_error, term()} | wal_down}.
write([{FstIdx, _, _} | _Rest] = Entries,
      #?MODULE{cfg = Cfg,
               range = Range,
               pending = Pend0,
               mem_table = Mt0} = State0)
  when Range == undefined orelse
       (FstIdx =< element(2, Range) + 1 andalso
        FstIdx >= 0) ->
    case stage_entries(Cfg, Entries, Mt0) of
        {ok, Mt} ->
            Pend = ra_seq:limit(FstIdx - 1, Pend0),
            wal_write_batch(State0#?MODULE{mem_table = Mt,
                                           pending = Pend}, Entries);
        Error ->
            Error
    end;
write([], State) ->
    {ok, State};
write([{Idx, _, _} | _], #?MODULE{cfg = #cfg{uid = UId},
                                  range = Range}) ->
    Msg = lists:flatten(io_lib:format("~s: ra_log:write/2 "
                                      "tried writing ~b - current range ~w",
                                      [UId, Idx, Range])),
    {error, {integrity_error, Msg}}.

-spec write_sparse(log_entry(), option(ra:index()), state()) ->
    {ok, state()} | {error, wal_down | gap_detected}.
write_sparse({Idx, Term, _} = Entry, PrevIdx0,
             #?MODULE{cfg = #cfg{uid = UId,
                                 wal = Wal} = Cfg,
                      range = Range,
                      pending = Pend0,
                      mem_table = Mt0} = State0)
  when PrevIdx0 == undefined orelse
       Range == undefined orelse
       (PrevIdx0 == element(2, Range)) ->
    {ok, Mt} = ra_mt:insert_sparse(Entry, PrevIdx0, Mt0),
    ok = incr_counter(Cfg, ?C_RA_LOG_WRITE_OPS, 1),
    Tid = ra_mt:tid(Mt),
    PrevIdx = previous_wal_index(State0),
    case ra_log_wal:write(Wal, {UId, self()}, Tid, PrevIdx, Idx,
                          Term, Entry) of
        {ok, Pid} ->
            ok = incr_counter(Cfg, ?C_RA_LOG_WRITE_OPS, 1),
            put_counter(Cfg, ?C_RA_SVR_METRIC_LAST_INDEX, Idx),
            NewRange = case Range of
                           undefined ->
                               ra_range:new(Idx);
                           {S, _} ->
                               ra_range:new(S, Idx)
                       end,
            Pend = ra_seq:limit(Idx - 1, Pend0),
            {ok, State0#?MODULE{range = NewRange,
                                last_term = Term,
                                mem_table = Mt,
                                pending = ra_seq:append(Idx, Pend),
                                last_wal_write = {Pid, now_ms(), Idx}}};
        {error, wal_down} = Err->
            Err
    end.

-spec fold(FromIdx :: ra_index(), ToIdx :: ra_index(),
           fun((log_entry(), Acc) -> Acc), Acc, state()) ->
    {Acc, state()} when Acc :: term().
fold(From0, To0, Fun, Acc0, State) ->
    fold(From0, To0, Fun, Acc0, State, error).

-spec fold(FromIdx :: ra_index(), ToIdx :: ra_index(),
           fun((log_entry(), Acc) -> Acc), Acc, state(),
            MissingKeyStrategy :: error | return) ->
    {Acc, state()} when Acc :: term().
fold(From0, To0, Fun, Acc0,
     #?MODULE{cfg = Cfg,
              mem_table = Mt,
              range = {StartIdx, EndIdx},
              reader = Reader0} = State, MissingKeyStrat)
  when To0 >= From0 andalso
       To0 >= StartIdx ->

    %% TODO: move to ra_range function
    From = max(From0, StartIdx),
    To = min(To0, EndIdx),

    ok = incr_counter(Cfg, ?C_RA_LOG_READ_OPS, 1),
    MtOverlap = ra_mt:range_overlap({From, To}, Mt),
    case MtOverlap of
        {undefined, {RemStart, RemEnd}} ->
            {Reader, Acc} = ra_log_segments:fold(RemStart, RemEnd, Fun,
                                                 Acc0, Reader0,
                                                 MissingKeyStrat),
            {Acc, State#?MODULE{reader = Reader}};
        {{MtStart, MtEnd}, {RemStart, RemEnd}} ->
            {Reader, Acc1} = ra_log_segments:fold(RemStart, RemEnd, Fun,
                                                  Acc0, Reader0,
                                                  MissingKeyStrat),
            Acc = ra_mt:fold(MtStart, MtEnd, Fun, Acc1, Mt, MissingKeyStrat),
            NumRead = MtEnd - MtStart + 1,
            ok = incr_counter(Cfg, ?C_RA_LOG_READ_MEM_TBL, NumRead),
            {Acc, State#?MODULE{reader = Reader}};
        {{MtStart, MtEnd}, undefined} ->
            Acc = ra_mt:fold(MtStart, MtEnd, Fun, Acc0, Mt, MissingKeyStrat),
            %% TODO: if fold is short circuited with MissingKeyStrat == return
            %% this count isn't correct, it doesn't massively matter so leaving
            %% for now
            NumRead = MtEnd - MtStart + 1,
            ok = incr_counter(Cfg, ?C_RA_LOG_READ_MEM_TBL, NumRead),
            {Acc, State}
    end;
fold(_From, _To, _Fun, Acc, State, _) ->
    {Acc, State}.

%% @doc Reads a list of indexes.
%% Found indexes are returned in the same order as the input list of indexes
%% @end
-spec sparse_read([ra_index()], state()) ->
    {[log_entry()], state()}.
sparse_read(Indexes0, #?MODULE{cfg = Cfg,
                               reader = Reader0,
                               range = Range,
                               live_indexes = LiveIndexes,
                               mem_table = Mt} = State) ->
    ok = incr_counter(Cfg, ?C_RA_LOG_READ_OPS, 1),
    %% indexes need to be sorted high -> low for correct and efficient reading
    Sort = ra_lib:lists_detect_sort(Indexes0),
    Indexes1 = case Sort of
                   unsorted ->
                       lists:sort(fun erlang:'>'/2, Indexes0);
                   ascending ->
                       lists:reverse(Indexes0);
                   _ ->
                       % descending or undefined
                       Indexes0
               end,

    %% drop any indexes that are larger than the last index available
    %% or smaller than first index and not in live indexes
    Indexes2 = lists:filter(fun (I) ->
                                    ra_range:in(I, Range) orelse
                                    ra_seq:in(I, LiveIndexes)
                            end, Indexes1),
    {Entries0, MemTblNumRead, Indexes} = ra_mt:get_items(Indexes2, Mt),
    ok = incr_counter(Cfg, ?C_RA_LOG_READ_MEM_TBL, MemTblNumRead),
    {Entries1, Reader} = ra_log_segments:sparse_read(Reader0, Indexes, Entries0),
    %% here we recover the original order of indexes
    Entries = case Sort of
                  descending ->
                      lists:reverse(Entries1);
                  unsorted ->
                      Lookup = lists:foldl(
                                 fun ({I, _, _} = E, Acc) ->
                                         maps:put(I, E, Acc)
                                 end, #{}, Entries1),
                      maps_with_values(Indexes0, Lookup);
                  _ ->
                      %% nothing to do for ascending or undefined
                      Entries1
              end,
    {Entries, State#?MODULE{reader = Reader}}.


%% read a list of indexes,
%% found indexes be returned in the same order as the input list of indexes
-spec partial_read([ra_index()], state(),
                   fun ((ra_index(),
                         ra_term(),
                         ra_server:command()) -> term())) ->
    read_plan().
partial_read(Indexes0, #?MODULE{cfg = Cfg,
                                reader = Reader0,
                                range = Range,
                                snapshot_state = SnapState,
                                mem_table = Mt},
            TransformFun) ->
    ok = incr_counter(Cfg, ?C_RA_LOG_READ_OPS, 1),
    %% indexes need to be sorted high -> low for correct and efficient reading
    Sort = ra_lib:lists_detect_sort(Indexes0),
    Indexes1 = case Sort of
                   unsorted ->
                       lists:sort(fun erlang:'>'/2, Indexes0);
                   ascending ->
                       lists:reverse(Indexes0);
                   _ ->
                       % descending or undefined
                       Indexes0
               end,
    LastIdx = case Range of
                  undefined ->
                      case ra_snapshot:current(SnapState) of
                          undefined ->
                              -1;
                          {SnapIdx, _} ->
                              SnapIdx
                      end;
                  {_, End} ->
                      End
              end,

    %% drop any indexes that are larger than the last index available
    Indexes2 = lists:dropwhile(fun (I) -> I > LastIdx end, Indexes1),
    {Entries0, MemTblNumRead, Indexes} = ra_mt:get_items(Indexes2, Mt),
    ok = incr_counter(Cfg, ?C_RA_LOG_READ_MEM_TBL, MemTblNumRead),
    Read = lists:foldl(fun ({I, T, Cmd}, Acc) ->
                               maps:put(I, TransformFun(I, T, Cmd), Acc)
                       end, #{}, Entries0),

    Plan = ra_log_segments:read_plan(Reader0, Indexes),
    #read_plan{dir = Cfg#cfg.directory,
               read = Read,
               plan = Plan}.


-spec execute_read_plan(read_plan(), undefined | ra_flru:state(),
                        TransformFun :: transform_fun(),
                        ra_log_segments:read_plan_options()) ->
    {#{ra_index() => Command :: term()}, ra_flru:state()}.
execute_read_plan(#read_plan{dir = Dir,
                             read = Read,
                             plan = Plan}, Flru0, TransformFun,
                  Options) ->
    ra_log_segments:exec_read_plan(Dir, Plan, Flru0, TransformFun,
                                   Options, Read).

-spec read_plan_info(read_plan()) -> map().
read_plan_info(#read_plan{read = Read,
                          plan = Plan}) ->
    NumSegments = length(Plan),
    NumInSegments = lists:foldl(fun ({_, Idxs}, Acc) ->
                                        Acc + length(Idxs)
                                end, 0, Plan),
    #{num_read => map_size(Read),
      num_in_segments => NumInSegments,
      num_segments => NumSegments}.


-spec previous_wal_index(state()) -> ra_idxterm() | -1.
previous_wal_index(#?MODULE{range = Range}) ->
    case Range of
        undefined ->
            -1;
        {_, LastIdx} ->
            LastIdx
    end.

-spec last_index_term(state()) -> option(ra_idxterm()).
last_index_term(#?MODULE{range = {_, LastIdx},
                         last_term = LastTerm}) ->
    {LastIdx, LastTerm};
last_index_term(#?MODULE{current_snapshot = CurSnap}) ->
    CurSnap.

-spec last_written(state()) -> ra_idxterm().
last_written(#?MODULE{last_written_index_term = LWTI}) ->
    LWTI.

%% forces the last index and last written index back to a prior index
-spec set_last_index(ra_index(), state()) ->
    {ok, state()} | {not_found, state()}.
set_last_index(Idx, #?MODULE{cfg = Cfg,
                             range = Range,
                             snapshot_state = SnapState,
                             mem_table = Mt0,
                             last_written_index_term = {LWIdx0, _}} = State0) ->
    Cur = ra_snapshot:current(SnapState),
    %% After set_last_index, recovery depends on the snapshot state.
    %% If Idx matches the snapshot index, we can recover from there.
    case fetch_term(Idx, State0) of
        {undefined, State} when element(1, Cur) =/= Idx ->
            %% not found and Idx isn't equal to latest snapshot index
            {not_found, State};
        {_, State} when element(1, Cur) =:= Idx ->
            {_, SnapTerm} = Cur,
            %% Idx is equal to the current snapshot
            {ok, Mt} = ra_log_ets:new_mem_table_please(Cfg#cfg.names,
                                                       Cfg#cfg.uid, Mt0),
            put_counter(Cfg, ?C_RA_SVR_METRIC_LAST_INDEX, Idx),
            put_counter(Cfg, ?C_RA_SVR_METRIC_LAST_WRITTEN_INDEX, Idx),
            {ok, State#?MODULE{range = ra_range:limit(Idx + 1, Range),
                               last_term = SnapTerm,
                               mem_table = Mt,
                               last_written_index_term = Cur}};
        {Term, State1} ->
            LWIdx = min(Idx, LWIdx0),
            {LWTerm, State2} = case Cur of
                                   {LWIdx, SnapTerm} ->
                                       {SnapTerm, State1};
                                   _ ->
                                       fetch_term(LWIdx, State1)
                               end,

            %% this should always be found but still assert just in case
            %% _if_ this ends up as a genuine reversal next time we try
            %% to write to the mem table it will detect this and open
            %% a new one
            true = LWTerm =/= undefined,
            {ok, Mt} = ra_log_ets:new_mem_table_please(Cfg#cfg.names,
                                                       Cfg#cfg.uid, Mt0),
            put_counter(Cfg, ?C_RA_SVR_METRIC_LAST_INDEX, Idx),
            put_counter(Cfg, ?C_RA_SVR_METRIC_LAST_WRITTEN_INDEX, LWIdx),
            {ok, State2#?MODULE{range = ra_range:limit(Idx + 1, Range),
                                last_term = Term,
                                mem_table = Mt,
                                last_written_index_term = {LWIdx, LWTerm}}}
    end.

-spec handle_event(event_body(), state()) ->
    {state(), [effect()]}.
handle_event({written, Term, WrittenSeq},
             #?MODULE{cfg = Cfg,
                      last_written_index_term = {PrevIdx, _},
                      snapshot_state = SnapState,
                      pending = Pend0} = State0) ->
    CurSnap = ra_snapshot:current(SnapState),
    %% gap detection
    %% 1. pending has lower indexes than the ra_seq:first index in WrittenSeq
    %% 2.
    LastWrittenIdx = ra_seq:last(WrittenSeq),
    case fetch_term(LastWrittenIdx, State0) of
        {Term, State} when is_integer(Term) ->
            ok = put_counter(Cfg, ?C_RA_SVR_METRIC_LAST_WRITTEN_INDEX,
                             LastWrittenIdx),

            case ra_seq:remove_prefix(WrittenSeq, Pend0) of
                {ok, Pend} ->
                    {State#?MODULE{last_written_index_term = {LastWrittenIdx, Term},
                                   pending = Pend}, []};
                {error, not_prefix} ->
                    ?DEBUG("~ts: ~w not prefix of ~w",
                           [Cfg#cfg.log_id, WrittenSeq, Pend0]),
                    {resend_pending(PrevIdx, State0), []}
            end;
        {undefined, State} when LastWrittenIdx =< element(1, CurSnap) ->
            % A snapshot happened before the written event came in
            % This can only happen on a leader when consensus is achieved by
            % followers returning appending the entry and the leader committing
            % and processing a snapshot before the written event comes in.
            %
            % At this point the items may still be in pending so need to
            % remove them
            {ok, Pend} = ra_seq:remove_prefix(WrittenSeq, Pend0),
            {State#?MODULE{pending = Pend}, []};
        {OtherTerm, State} when OtherTerm =/= Term ->
            %% term mismatch, let's reduce the seq and try again to see
            %% if any entries in the range are valid
            case ra_seq:limit(LastWrittenIdx - 1, WrittenSeq) of
                [] ->
                    ?DEBUG("~ts: written event did not find term ~b for index ~b "
                           "found ~w",
                           [Cfg#cfg.log_id, Term, LastWrittenIdx, OtherTerm]),
                    {State, []};
                NewWrittenSeq ->
                    %% retry with a reduced range
                    handle_event({written, Term, NewWrittenSeq}, State0)
            end
    end;
handle_event({segments, TidSeqs, NewSegs},
             #?MODULE{cfg = #cfg{uid = UId,
                                 log_id = LogId,
                                 directory = Dir,
                                 names = Names} = Cfg,
                      reader = Reader0,
                      pending = Pend0,
                      last_written_index_term = LWIT0,
                      mem_table = Mt0} = State0) ->
    {Reader1, OverwrittenSegRefs} =
        ra_log_segments:update_segments(NewSegs, Reader0),

    put_counter(Cfg, ?C_RA_SVR_METRIC_NUM_SEGMENTS,
                ra_log_segments:segment_ref_count(Reader1)),
    %% the tid ranges arrive in the reverse order they were written
    %% (new -> old) so we need to foldr here to process the oldest first
    Mt = lists:foldr(
           fun ({Tid, Seq}, Acc0) ->
                   {Spec, Acc} = ra_mt:record_flushed(Tid, Seq, Acc0),
                   ok = ra_log_ets:execute_delete(Names, UId, Spec),
                   Acc
           end, Mt0, TidSeqs),

    %% it is theoretically possible that the segment writer flush _could_
    %% over take WAL notifications
    FstPend = ra_seq:first(Pend0),
    MtRange = ra_mt:range(Mt),
    SegRange = ra_log_segments:range(Reader1),
    Pend = case MtRange of
               {Start, _End} when is_integer(FstPend) andalso
                                  Start > FstPend ->
                   %% set the first pending item to that of the first mem
                   %% table index
                   ra_seq:floor(Start, Pend0);
               {_, _} ->
                   Pend0;
               undefined ->
                   %% if there are no mem entries, all pending items must be
                   %% flushed
                   []
           end,

    %% if the last written index term is lower than the last segment index
    %% we need to update this.
    {LWIT, Reader} =
        case LWIT0 of
            {LWI, _} when is_tuple(SegRange) andalso
                          element(2, SegRange) > LWI ->
                {_, LastSegIdx} = SegRange,
                {LWTerm, Reader2} = ra_log_segments:fetch_term(LastSegIdx,
                                                               Reader1),
                {{LastSegIdx, LWTerm}, Reader2};
            _ ->
                {LWIT0, Reader1}
        end,
    ?DEBUG("~ts: ~b new segment(s) received - mem table range ~w"
           " segment range ~w",
           [LogId, length(NewSegs), MtRange, SegRange]),
    State = State0#?MODULE{reader = Reader,
                           pending = Pend,
                           last_written_index_term = LWIT,
                           mem_table = Mt},
    Fun = fun () ->
                  [begin
                    ?DEBUG("~ts: deleting overwritten segment ~w",
                           [LogId, SR]),
                    _ = catch prim_file:delete(filename:join(Dir, F)),
                    ok
                   end
                   || {F, _} = SR <- OverwrittenSegRefs],
                  ok
          end,
    {State, [{bg_work, Fun, fun (_Err) -> ok end}]};
handle_event({compaction_result, Result},
             #?MODULE{cfg = #cfg{log_id = _LogId},
                      current_snapshot = {CurSnapIdx, _},
                      live_indexes = LiveIndexes,
                      reader = Segments0} = State) ->
    % ?DEBUG("~ts: compaction result ~p", [LogId, Result]),
    Compaction = ra_log_segments:compaction(Segments0),
    {Segments1, Effs} = ra_log_segments:handle_compaction_result(Result,
                                                                 Segments0),
    case Compaction of
        {_Type, SnapIdx} when CurSnapIdx > SnapIdx ->
            %% snapshot has moved whilst compacting, need to perform another
            %% minor at least
            {Segments, Effs2} = ra_log_segments:schedule_compaction(minor,
                                                                    CurSnapIdx,
                                                                    LiveIndexes,
                                                                    Segments1),
            {State#?MODULE{reader = Segments}, Effs ++ Effs2};
        _ ->
            {State#?MODULE{reader = Segments1}, Effs}
    end;
handle_event(major_compaction, #?MODULE{cfg = #cfg{log_id = LogId},
                                        reader = Reader0,
                                        live_indexes = LiveIndexes,
                                        snapshot_state = SS} = State) ->
    case ra_snapshot:current(SS) of
        {SnapIdx, _} ->
            ?DEBUG("~ts: ra_log: major_compaction requested at snapshot index ~b, "
                   "~b live indexes",
                   [LogId, SnapIdx, ra_seq:length(LiveIndexes)]),
            {Reader, Effs} = ra_log_segments:schedule_compaction(major, SnapIdx,
                                                                 LiveIndexes, Reader0),
            {State#?MODULE{reader = Reader}, Effs};
        _ ->
            {State, []}
    end;
handle_event({snapshot_written, {SnapIdx, _} = Snap, LiveIndexes,
              SnapKind, Duration},
             #?MODULE{cfg = #cfg{uid = UId,
                                 log_id = LogId,
                                 names = Names} = Cfg,
                      range = {FstIdx, _} = Range,
                      mem_table = Mt0,
                      pending = Pend0,
                      last_written_index_term =
                          {LastWrittenIdx, _} = LWIdxTerm0,
                      snapshot_state = SnapState0} = State0)
%% only update snapshot if it is newer than the last snapshot
  when SnapIdx >= FstIdx ->
    % ?assert(ra_snapshot:pending(SnapState0) =/= undefined),
    SnapState1 = ra_snapshot:complete_snapshot(Snap, SnapKind, LiveIndexes,
                                               SnapState0),
    ?DEBUG("~ts: ra_log: ~s written at index ~b with ~b live indexes in ~bms",
           [LogId, SnapKind, SnapIdx, ra_seq:length(LiveIndexes), Duration]),
    case SnapKind of
        snapshot ->
            put_counter(Cfg, ?C_RA_SVR_METRIC_SNAPSHOT_INDEX, SnapIdx),
            %% Delete old snapshot files. This is done as an effect
            %% so that if an old snapshot is still being replicated
            %% the cleanup can be delayed until it is safe.
            DeleteCurrentSnap = {delete_snapshot,
                                 ra_snapshot:directory(SnapState1, snapshot),
                                 ra_snapshot:current(SnapState0)},
            %% Also delete any checkpoints older than this snapshot.
            {SnapState, Checkpoints} =
                ra_snapshot:take_older_checkpoints(SnapIdx, SnapState1),
            CPEffects = [{delete_snapshot,
                          ra_snapshot:directory(SnapState, checkpoint),
                          Checkpoint} || Checkpoint <- Checkpoints],
            Effects0 = [DeleteCurrentSnap | CPEffects],

            LWIdxTerm =
                case LastWrittenIdx > SnapIdx of
                    true ->
                        LWIdxTerm0;
                    false ->
                        Snap
                end,

            %% remove all pending below smallest live index as the wal
            %% may not write them
            %% TODO: test that a written event can still be processed if it
            %% contains lower indexes than pending
            SmallestLiveIdx = case ra_seq:first(LiveIndexes) of
                                  undefined ->
                                      SnapIdx + 1;
                                  I ->
                                      I
                              end,
            %% TODO: optimise - ra_seq:floor/2 is O(n),
            Pend = ra_seq:floor(SmallestLiveIdx, Pend0),
            %% delete from mem table
            %% this will race with the segment writer but if the
            %% segwriter detects a missing index it will query the snaphost
            %% state and if that is higher it will resume flush
            {Spec, Mt1} = ra_mt:set_first(SmallestLiveIdx, Mt0),
            ok = exec_mem_table_delete(Names, UId, Spec),

            State = State0#?MODULE{range = ra_range:truncate(SnapIdx, Range),
                                   last_written_index_term = LWIdxTerm,
                                   mem_table = Mt1,
                                   pending = Pend,
                                   live_indexes = LiveIndexes,
                                   current_snapshot = Snap,
                                   snapshot_state = SnapState},
            {Reader, CompEffs} = ra_log_segments:schedule_compaction(minor, SnapIdx,
                                                                     LiveIndexes,
                                                                     State#?MODULE.reader),
            Effects = CompEffs ++ Effects0,
            {State#?MODULE{reader = Reader}, Effects};
        checkpoint ->
            put_counter(Cfg, ?C_RA_SVR_METRIC_CHECKPOINT_INDEX, SnapIdx),
            %% If we already have the maximum allowed number of checkpoints,
            %% remove some checkpoints to make space.
            {SnapState, CPs} = ra_snapshot:take_extra_checkpoints(SnapState1),
            Effects = [{delete_snapshot,
                        ra_snapshot:directory(SnapState, SnapKind),
                        CP} || CP <- CPs],
            {State0#?MODULE{snapshot_state = SnapState}, Effects}
    end;
handle_event({snapshot_written, {Idx, Term} = Snap, _Indexes, SnapKind, _Duration},
             #?MODULE{cfg =#cfg{log_id = LogId},
                      snapshot_state = SnapState} = State0) ->
    %% if the snapshot/checkpoint is stale we just want to delete it
    Current = ra_snapshot:current(SnapState),
    ?INFO("~ts: old snapshot_written received for index ~b in term ~b
          current snapshot ~w, deleting old ~s",
           [LogId, Idx, Term, Current, SnapKind]),
    Effects = [{delete_snapshot,
                ra_snapshot:directory(SnapState, SnapKind),
                Snap}],
    {State0, Effects};
handle_event({snapshot_error, Snap, SnapKind, Error},
             #?MODULE{cfg =#cfg{log_id = LogId},
                      snapshot_state = SnapState0} = State0) ->
    ?INFO("~ts: snapshot error for ~w ~s ", [LogId, Snap, SnapKind]),
    SnapState = ra_snapshot:handle_error(Snap, Error, SnapState0),
    {State0#?MODULE{snapshot_state = SnapState}, []};
handle_event({resend_write, Idx},
             #?MODULE{cfg = #cfg{log_id = LogId}} = State) ->
    % resend missing entries from mem tables.
    ?INFO("~ts: ra_log: wal requested resend from ~b",
          [LogId, Idx]),
    {resend_from(Idx, State), []};
handle_event({down, _Pid, _Info}, #?MODULE{} = State) ->
    {State, []}.

-spec next_index(state()) -> ra_index().
next_index(#?MODULE{tx = {true, {_, Last}}}) ->
    Last + 1;
next_index(#?MODULE{range = {_, LastIdx}}) ->
    LastIdx + 1;
next_index(#?MODULE{current_snapshot = {SnapIdx, _}}) ->
    SnapIdx + 1;
next_index(#?MODULE{current_snapshot = undefined}) ->
    0.

-spec fetch(ra_index(), state()) ->
    {option(log_entry()), state()}.
fetch(Idx, State0) ->
    case sparse_read([Idx], State0) of
        {[Entry], State} ->
            {Entry, State};
        {[], State} ->
            {undefined, State}
    end.

-spec fetch_term(ra_index(), state()) ->
    {option(ra_term()), state()}.
fetch_term(Idx, #?MODULE{mem_table = Mt,
                         range = Range,
                         reader = Reader0} = State0)
  when ?IS_IN_RANGE(Idx, Range) ->
    case ra_mt:lookup_term(Idx, Mt) of
        undefined ->
            {Term, Reader} = ra_log_segments:fetch_term(Idx, Reader0),
            {Term, State0#?MODULE{reader = Reader}};
        Term when is_integer(Term) ->
            {Term, State0}
    end;
fetch_term(_Idx, #?MODULE{} = State0) ->
    {undefined, State0}.

-spec snapshot_state(State :: state()) -> ra_snapshot:state().
snapshot_state(State) ->
    State#?MODULE.?FUNCTION_NAME.

-spec set_snapshot_state(ra_snapshot:state(), state()) -> state().
set_snapshot_state(SnapState, State) ->
    State#?MODULE{snapshot_state = SnapState}.

-spec install_snapshot(ra_idxterm(), module(), ra_seq:state(), state()) ->
    {ok, state(), effects()}.
install_snapshot({SnapIdx, SnapTerm} = IdxTerm, MacMod, LiveIndexes,
                 #?MODULE{cfg = #cfg{uid = UId,
                                     names = Names} = Cfg,
                          snapshot_state = SnapState0,
                          pending = Pend0,
                          mem_table = Mt0} = State0)
  when is_atom(MacMod) ->
    ok = incr_counter(Cfg, ?C_RA_LOG_SNAPSHOTS_INSTALLED, 1),
    ok = put_counter(Cfg, ?C_RA_SVR_METRIC_SNAPSHOT_INDEX, SnapIdx),
    put_counter(Cfg, ?C_RA_SVR_METRIC_LAST_INDEX, SnapIdx),
    put_counter(Cfg, ?C_RA_SVR_METRIC_LAST_WRITTEN_INDEX, SnapIdx),

    {SnapState, Checkpoints} =
        ra_snapshot:take_older_checkpoints(SnapIdx, SnapState0),
    CPEffects = [{delete_snapshot,
                  ra_snapshot:directory(SnapState, checkpoint),
                  Checkpoint} || Checkpoint <- Checkpoints],
    SmallestLiveIndex = case ra_seq:first(LiveIndexes) of
                            undefined ->
                                SnapIdx + 1;
                            I ->
                                I
                        end,
    %% TODO: more mt entries could potentially be cleared up in the
    %% mem table here if we walked the live indexes
    {Spec, Mt1} = ra_mt:set_first(SmallestLiveIndex, Mt0),
    ok = exec_mem_table_delete(Names, UId, Spec),
    Pend = ra_seq:floor(SmallestLiveIndex, Pend0),
    %% always create a new mem table here as we could have written
    %% sparese entries in the snapshot install
    %% TODO: check an empty mt doesn't leak
    {ok, Mt} = ra_log_ets:new_mem_table_please(Cfg#cfg.names,
                                               Cfg#cfg.uid, Mt1),
    State = State0#?MODULE{snapshot_state = SnapState,
                           current_snapshot = IdxTerm,
                           range = undefined,
                           last_term = SnapTerm,
                           live_indexes = LiveIndexes,
                           mem_table = Mt,
                           pending = Pend,
                           last_written_index_term = IdxTerm},
    {Reader, CompEffs} = ra_log_segments:schedule_compaction(minor, SnapIdx,
                                                             LiveIndexes,
                                                             State#?MODULE.reader),
    {ok, State#?MODULE{reader = Reader}, CompEffs ++ CPEffects}.


-spec recover_snapshot(State :: state()) ->
    option({ra_snapshot:meta(), term()}).
recover_snapshot(#?MODULE{snapshot_state = SnapState}) ->
    case ra_snapshot:recover(SnapState) of
        {ok, Meta, MacState} ->
            {Meta, MacState};
        {error, no_current_snapshot} ->
            undefined;
        {error, Reason} ->
            %% Other errors during recovery - log and return undefined
            %% This allows the system to start fresh if snapshots are corrupted
            ?WARN("ra_log: snapshot recovery failed: ~p", [Reason]),
            undefined
    end.

-spec snapshot_index_term(State :: state()) -> option(ra_idxterm()).
snapshot_index_term(#?MODULE{snapshot_state = SS}) ->
    ra_snapshot:current(SS).

-spec update_release_cursor(Idx :: ra_index(),
                            Cluster :: ra_cluster(),
                            MacCtx :: {MacVer :: ra_machine:version(), module()},
                            MacState :: term(), State :: state()) ->
    {state(), effects()}.
update_release_cursor(Idx, Cluster, {MacVersion, MacModule} = MacCtx,
                      MacState, State)
  when is_atom(MacModule) andalso
       is_integer(MacVersion) ->
    suggest_snapshot(snapshot, Idx, Cluster, MacCtx, MacState, State).

-spec checkpoint(Idx :: ra_index(), Cluster :: ra_cluster(),
                 MacCtx :: {MacVer :: ra_machine:version(), module()},
                 MacState :: term(), State :: state()) ->
    {state(), effects()}.
checkpoint(Idx, Cluster, {MacVersion, MacModule} = MacCtx, MacState, State)
  when is_atom(MacModule) andalso
       is_integer(MacVersion) ->
    suggest_snapshot(checkpoint, Idx, Cluster, MacCtx, MacState, State).

suggest_snapshot(SnapKind, Idx, Cluster, MacCtx, MacState,
                 #?MODULE{snapshot_state = SnapshotState} = State) ->
    case ra_snapshot:pending(SnapshotState) of
        undefined ->
            suggest_snapshot0(SnapKind, Idx, Cluster, MacCtx, MacState, State);
        _ ->
            %% Only one snapshot or checkpoint may be written at a time to
            %% prevent excessive I/O usage.
            {State, []}
    end.

promote_checkpoint(Idx, #?MODULE{cfg = Cfg,
                                 snapshot_state = SnapState0} = State) ->
    case ra_snapshot:pending(SnapState0) of
        {_IdxTerm, snapshot} ->
            %% If we're currently writing a snapshot, skip promoting a
            %% checkpoint.
            {State, []};
        _ ->
            {WasPromoted, SnapState, Effects} =
                ra_snapshot:promote_checkpoint(Idx, SnapState0),
            if WasPromoted ->
                   ok = incr_counter(Cfg, ?C_RA_LOG_CHECKPOINTS_PROMOTED, 1);
               true ->
                   ok
            end,

            {State#?MODULE{snapshot_state = SnapState}, Effects}
    end.

-spec tick(Now :: integer(), state()) -> state().
tick(Now, #?MODULE{cfg = #cfg{wal = Wal},
                   mem_table = Mt,
                   last_written_index_term = {LastWrittenIdx, _},
                   last_wal_write = {WalPid, Ms, _}} = State) ->
    CurWalPid = whereis(Wal),
    MtRange = ra_mt:range(Mt),
    case Now > Ms + ?WAL_RESEND_TIMEOUT andalso
         is_pid(CurWalPid) andalso
         CurWalPid =/= WalPid andalso
         ra_range:in(LastWrittenIdx + 1, MtRange)
    of
        true ->
            %% the wal has restarted, it has been at least 5s and there are
            %% cached items, resend them
            resend_from(LastWrittenIdx + 1, State);
        false ->
            State
    end.

assert(#?MODULE{cfg = #cfg{log_id = LogId},
                range = Range,
                snapshot_state = SnapState,
                current_snapshot = CurrSnap,
                live_indexes = LiveIndexes
               } = State) ->
    %% These assertions verify log state consistency during recovery.
    %% Consider removing once log recovery is stable and well-tested.
    ?DEBUG("~ts: ra_log: asserting Range ~w Snapshot ~w",
           [LogId, Range, CurrSnap]),
    %% perform assertions to ensure log state is correct
    ?assert(CurrSnap =:= ra_snapshot:current(SnapState)),
    ?assert(Range == undefined orelse
            CurrSnap == undefined orelse
            element(1, Range) - 1 == element(1, CurrSnap)),
    ?assert(CurrSnap == undefined orelse
            LiveIndexes == [] orelse
            ra_seq:last(LiveIndexes) =< element(1, CurrSnap)),
    State.

suggest_snapshot0(SnapKind, Idx, Cluster, {MachineVersion, MacModule},
                  MacState, State0) ->
    case should_snapshot(SnapKind, Idx, State0) of
        true ->
            % TODO: here we use the current cluster configuration in
            % the snapshot,
            % _not_ the configuration at the snapshot point.
            % Given cluster changes
            % are applied as they are received I cannot think of any scenarios
            % where this can cause a problem. That said there may
            % well be :dragons: here.
            % The MacState is a reference to the state at
            % the release_cursor point.
            % It can be some dehydrated form of the state itself
            % or a reference for external storage (e.g. ETS table)
            case fetch_term(Idx, State0) of
                {undefined, _} ->
                    {State0, []};
                {Term, State} ->
                    ClusterServerIds =
                        maps:map(fun (_, V) ->
                                         maps:with([voter_status], V)
                                 end, Cluster),
                    Meta = #{index => Idx,
                             term => Term,
                             cluster => ClusterServerIds,
                             machine_version => MachineVersion},
                    write_snapshot(Meta, MacModule, MacState,
                                   SnapKind, State)
            end;
        false ->
            {State0, []}
    end.

should_snapshot(snapshot, Idx,
                #?MODULE{cfg = #cfg{min_snapshot_interval = SnapInter},
                         reader = Reader,
                         snapshot_state = SnapState}) ->
    SnapLimit = case ra_snapshot:current(SnapState) of
                    undefined -> SnapInter;
                    {I, _} -> I + SnapInter
                end,
    % The release cursor index is the last entry _not_ contributing
    % to the current state. I.e. the last entry that can be discarded.
    % We should take a snapshot if the new snapshot index would allow us
    % to discard any segments or if the we've handled enough commands
    % since the last snapshot.
    CanFreeSegments = case ra_log_segments:range(Reader) of
                          undefined ->
                              false;
                          {Start, _End} ->
                              %% this isn't 100% guaranteed to free a segment
                              %% but there is a good chance
                              Idx > Start
                      end,
    CanFreeSegments orelse Idx > SnapLimit;
should_snapshot(checkpoint, Idx,
                #?MODULE{cfg = #cfg{min_checkpoint_interval = CheckpointInter},
                         snapshot_state = SnapState}) ->
    CheckpointLimit = case ra_snapshot:latest_checkpoint(SnapState) of
                          undefined -> CheckpointInter;
                          {I, _} -> I + CheckpointInter
                      end,
    Idx > CheckpointLimit.

-spec append_sync(Entry :: log_entry(), State :: state()) ->
    state() | no_return().
append_sync({Idx, Term, _} = Entry, Log0) ->
    Log = append(Entry, Log0),
    await_written_idx(Idx, Term, Log).

-spec write_sync(Entries :: [log_entry()], State :: state()) ->
    {ok, state()} |
    {error, {integrity_error, term()} | wal_down}.
write_sync(Entries, Log0) ->
    {Idx, Term, _} = lists:last(Entries),
    case ra_log:write(Entries, Log0) of
        {ok, Log} ->
            {ok, await_written_idx(Idx, Term, Log)};
        {error, _} = Err ->
            Err
    end.

-spec can_write(state()) -> boolean().
can_write(#?MODULE{cfg = #cfg{wal = Wal}}) ->
    undefined =/= whereis(Wal).

-spec exists(ra_idxterm(), state()) ->
    {boolean(), state()}.
exists({Idx, Term}, Log0) ->
    case fetch_term(Idx, Log0) of
        {Term, Log} when is_integer(Term) ->
            {true, Log};
        {_, Log} ->
            {false, Log}
    end.

-spec overview(state()) -> overview().
-spec has_pending(state()) -> boolean().
has_pending(#?MODULE{pending = []}) ->
    false;
has_pending(#?MODULE{}) ->
    true.

overview(#?MODULE{range = Range,
                  last_term = LastTerm,
                  last_written_index_term = LWIT,
                  snapshot_state = SnapshotState,
                  current_snapshot = CurrSnap,
                  reader = Reader,
                  last_wal_write = {_LastPid, LastMs, LastWalIdx},
                  mem_table = Mt,
                  pending = Pend
                 } = State) ->
    {LastIndex, _} = last_index_term(State),
    #{type => ?MODULE,
      range => Range,
      last_index => LastIndex,
      last_term => LastTerm,
      last_written_index_term => LWIT,
      num_segments => ra_log_segments:segment_ref_count(Reader),
      segments_range => ra_log_segments:range(Reader),
      open_segments => ra_log_segments:num_open_segments(Reader),
      snapshot_index => case CurrSnap of
                            undefined -> undefined;
                            {I, _} -> I
                        end,
      snapshot_term => case CurrSnap of
                           undefined -> undefined;
                           {_, T} -> T
                       end,
      latest_checkpoint_index =>
          case ra_snapshot:latest_checkpoint(SnapshotState) of
              undefined -> undefined;
              {I, _} -> I
          end,
      mem_table_range => ra_mt:range(Mt),
      mem_table_info => ra_mt:info(Mt),
      last_wal_write => LastMs,
      last_wal_index => LastWalIdx,
      num_pending => ra_seq:length(Pend)
     }.

-spec write_config(ra_server:config(), state()) -> ok.
write_config(Config0, #?MODULE{cfg = #cfg{directory = Dir}}) ->
    ConfigPath = filename:join(Dir, <<"config">>),
    TmpConfigPath = filename:join(Dir, <<"config.tmp">>),
    % clean config of potentially unserialisable data
    Config = maps:without([parent,
                           counter,
                           has_changed,
                           %% don't write system config to disk as it will
                           %% be updated each time
                           system_config], Config0),
    ok = ra_lib:write_file(TmpConfigPath,
                           list_to_binary(io_lib:format("~p.", [Config]))),
    ok = prim_file:rename(TmpConfigPath, ConfigPath),
    ok.

-spec read_config(state() | file:filename_all()) ->
    {ok, ra_server:config()} | {error, term()}.
read_config(#?MODULE{cfg = #cfg{directory = Dir}}) ->
    read_config(Dir);
read_config(Dir) ->
    ConfigPath = filename:join(Dir, <<"config">>),
    ra_lib:consult(ConfigPath).

-spec delete_everything(state()) -> ok.
delete_everything(#?MODULE{cfg = #cfg{uid = UId,
                                      names = Names,
                                      directory = Dir},
                           snapshot_state = _SnapState} = Log) ->
    _ = close(Log),
    %% if there is a snapshot process pending it could cause the directory
    %% deletion to fail, best kill the snapshot process first
    ok = ra_log_ets:delete_mem_tables(Names, UId),
    catch ra_log_snapshot_state:delete(?ETSTBL, UId),
    try ra_lib:recursive_delete(Dir) of
        ok -> ok
    catch
        _:_ = Err ->
            ?WARN("ra_log:delete_everything/1 failed to delete "
                  "directory ~ts. Error: ~p", [Dir, Err])
    end,
    ok.

-spec release_resources(non_neg_integer(),
                        sequential | random, state()) -> state().
release_resources(MaxOpenSegments, AccessPattern,
                  #?MODULE{cfg = #cfg{},
                           reader = Reader} = State) ->
    State#?MODULE{reader = ra_log_segments:update_conf(MaxOpenSegments,
                                                       AccessPattern, Reader)}.

%%% Local functions


%% only used by resend to wal functionality and doesn't update the mem table
wal_rewrite(#?MODULE{cfg = #cfg{uid = UId,
                                wal = Wal} = Cfg,
                     last_wal_write = {_, _, _}} = State,
            Tid, PrevIdx, {Idx, Term, Cmd}) ->
    case ra_log_wal:write(Wal, {UId, self()}, Tid,
                          PrevIdx, Idx, Term, Cmd) of
        {ok, Pid} ->
            ok = incr_counter(Cfg, ?C_RA_LOG_WRITE_OPS, 1),
            put_counter(Cfg, ?C_RA_SVR_METRIC_LAST_INDEX, Idx),
            State#?MODULE{last_term = Term,
                          last_wal_write = {Pid, now_ms(), Idx}
                         };
        {error, wal_down} ->
            error(wal_down)
    end.

wal_write_batch(#?MODULE{cfg = #cfg{uid = UId,
                                    wal = Wal} = Cfg,
                         pending = Pend0,
                         range = Range,
                         mem_table = Mt0} = State,
                [{FstIdx, _, _} | _] = Entries) ->
    WriterId = {UId, self()},
    PrevIdx = previous_wal_index(State),
    %% all entries in a transaction are written to the same tid
    Tid = ra_mt:tid(Mt0),
    {WalCommands, Num, LastIdx, Pend} =
        lists:foldl(fun ({Idx, Term, Cmd0}, {WC, N, Prev, P}) ->
                            Cmd = {ttb, term_to_iovec(Cmd0)},
                            WalC = {append, WriterId, Tid, Prev, Idx, Term, Cmd},
                            {[WalC | WC], N+1, Idx, ra_seq:append(Idx, P)}
                    end, {[], 0, PrevIdx, Pend0}, Entries),

    [{_, _, _, _PrevIdx, LastIdx, LastTerm, _} | _] = WalCommands,
    {_, Mt} = ra_mt:commit(Mt0),
    put_counter(Cfg, ?C_RA_SVR_METRIC_LAST_INDEX, LastIdx),
    ok = incr_counter(Cfg, ?C_RA_LOG_WRITE_OPS, Num),
    NewRange = case Range of
                   undefined ->
                       ra_range:new(FstIdx, LastIdx);
                   {Start, _} ->
                       ra_range:new(Start, LastIdx)
               end,
    case ra_log_wal:write_batch(Wal, lists:reverse(WalCommands)) of
        {ok, Pid} ->
            {ok, State#?MODULE{range = NewRange,
                               last_term = LastTerm,
                               last_wal_write = {Pid, now_ms(), LastIdx},
                               mem_table = Mt,
                               pending = Pend}};
        {error, wal_down} = Err ->
            %% if we get there the entry has already been inserted
            %% into the mem table but never reached the wal
            %% the resend logic will take care of that
            Err
    end.

maybe_append_first_entry(#?MODULE{range = undefined,
                                  current_snapshot = undefined} = State0) ->
    State1 = append({0, 0, undefined}, State0),
    receive
        {ra_log_event, {written, 0, [0]} = Evt} ->
            State2 = State1#?MODULE{range = ra_range:new(0)},
            {State, _Effs} = handle_event(Evt, State2),
            State
    after 60000 ->
              exit({?FUNCTION_NAME, timeout})
    end;
maybe_append_first_entry(State) ->
    State.

resend_from(Idx, #?MODULE{cfg = #cfg{uid = UId}} = State0) ->
    %% the wal will always request the next sequential
    %% index before the last one it got
    try resend_from0(Idx, Idx - 1, State0) of
        State -> State
    catch
        error:wal_down ->
            ?WARN("~ts: ra_log: resending from ~b failed with wal_down",
                  [UId, Idx]),
            State0
    end.

resend_pending(_, #?MODULE{pending = []} = State) ->
    State;
resend_pending(PrevIdx, #?MODULE{pending = Pend} = State) ->
    resend_from0(ra_seq:first(Pend), PrevIdx, State).

resend_from0(Idx, PrevIdx,
             #?MODULE{cfg = Cfg,
                      last_resend_time = undefined,
                      pending = Pend0,
                      mem_table = Mt} = State0) ->
    ?DEBUG("~ts: ra_log: resending pending range ~w from ~b prev idx ~b",
           [State0#?MODULE.cfg#cfg.log_id, ra_seq:range(Pend0), Idx, PrevIdx]),
    Pend = ra_seq:floor(Idx, Pend0),
    ok = incr_counter(Cfg, ?C_RA_LOG_WRITE_RESENDS, ra_seq:length(Pend)),
    State1 = State0#?MODULE{last_resend_time = {erlang:system_time(seconds),
                                                whereis(Cfg#cfg.wal)},
                            pending = Pend},
    {_, State} = ra_seq:fold(fun (I, {P, Acc}) ->
                                     {I, T, C} = ra_mt:lookup(I, Mt),
                                     Tid = ra_mt:tid_for(I, T, Mt),
                                     {I, wal_rewrite(Acc, Tid, P, {I, T, C})}
                             end,
                             {PrevIdx, State1},
                             Pend),
    State;
resend_from0(Idx, _PrevIdx,
             #?MODULE{last_resend_time = {LastResend, WalPid},
                      cfg = #cfg{resend_window_seconds = ResendWindow}} = State) ->
    case erlang:system_time(seconds) > LastResend + ResendWindow orelse
         (is_pid(WalPid) andalso not is_process_alive(WalPid)) of
        true ->
            % it has been more than the resend window since last resend
            % _or_ the wal has been restarted since then
            % ok to try again
            resend_from(Idx, State#?MODULE{last_resend_time = undefined});
        false ->
            State
    end.

stage_entries(Cfg, [Entry | Rem] = Entries, Mt0) ->
    case ra_mt:stage(Entry, Mt0) of
        {ok, Mt} ->
            stage_entries0(Cfg, Rem, Mt);
        {error, OverwritingOrLimitReached} ->
            ?DEBUG("~ts: mem table ~s detected whilst staging entries, opening new mem table",
                   [Cfg#cfg.log_id, OverwritingOrLimitReached]),
            %% TODO: do we need error handling here - this function uses the infinity
            %% timeout
            {ok, Mt} = ra_log_ets:new_mem_table_please(Cfg#cfg.names,
                                                       Cfg#cfg.uid, Mt0),
            stage_entries(Cfg, Entries, Mt)
    end.

stage_entries0(_Cfg, [], Mt) ->
    {ok, Mt};
stage_entries0(Cfg, [Entry | Rem], Mt0) ->
    case ra_mt:stage(Entry, Mt0) of
        {ok, Mt} ->
            stage_entries0(Cfg, Rem, Mt);
        {error, overwriting} ->
            Range = ra_mt:range(Mt0),
            Msg = io_lib:format("ra_log:stage_entries/3 "
                                "tried writing ~p - mem table range ~w",
                                [Rem, Range]),
            {error, {integrity_error, lists:flatten(Msg)}}
    end.



write_snapshot(Meta, MacModule, MacState, SnapKind,
               #?MODULE{cfg = Cfg,
                        snapshot_state = SnapState0} = State) ->
    Counter = case SnapKind of
                  snapshot -> ?C_RA_LOG_SNAPSHOTS_WRITTEN;
                  checkpoint -> ?C_RA_LOG_CHECKPOINTS_WRITTEN
              end,
    ok = incr_counter(Cfg, Counter, 1),
    {SnapState, Effects} = ra_snapshot:begin_snapshot(Meta, MacModule, MacState,
                                                      SnapKind, SnapState0),
    {State#?MODULE{snapshot_state = SnapState}, Effects}.

my_segrefs(UId, SegWriter) ->
    SegFiles = ra_log_segment_writer:my_segments(SegWriter, UId),
    lists:foldl(fun (File, Acc) ->
                        %% if a server recovered when a segment had been opened
                        %% but never had any entries written the segref would be
                        %% undefined
                        case ra_log_segment:info(File) of
                            #{ref := SegRef,
                              file_type := regular}
                              when is_tuple(SegRef) ->
                                [SegRef | Acc];
                            _ ->
                                Acc
                        end
                end, [], SegFiles).

%% TODO: implement synchronous writes using gen_batch_server:call/3
await_written_idx(Idx, Term, Log0) ->
    receive
        {ra_log_event, {written, Term, _Seq} = Evt} ->
            {Log, _} = handle_event(Evt, Log0),
            case last_written(Log) of
                {Idx, Term} ->
                    Log;
                _ ->
                    await_written_idx(Idx, Term, Log)
            end
    after ?LOG_APPEND_TIMEOUT ->
              throw(ra_log_append_timeout)
    end.

incr_counter(#cfg{counter = Cnt}, Ix, N) when Cnt =/= undefined ->
    counters:add(Cnt, Ix, N);
incr_counter(#cfg{counter = undefined}, _Ix, _N) ->
    ok.

put_counter(#cfg{counter = Cnt}, Ix, N) when Cnt =/= undefined ->
    counters:put(Cnt, Ix, N);
put_counter(#cfg{counter = undefined}, _Ix, _N) ->
    ok.

server_data_dir(Dir, UId) ->
    filename:join(Dir, UId).

maps_with_values(Keys, Map) ->
    lists:foldr(
      fun (K, Acc) ->
              case Map of
                  #{K := Value} ->
                      [Value | Acc];
                  _ ->
                      Acc
              end
      end, [], Keys).

now_ms() ->
    erlang:system_time(millisecond).

exec_mem_table_delete(#{} = Names, UId, Specs)
  when is_list(Specs) ->
    [ra_log_ets:execute_delete(Names, UId, Spec)
     || Spec <- Specs],
    ok.
