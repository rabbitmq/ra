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
         sparse_read/2,
         partial_read/3,
         execute_read_plan/4,
         read_plan_info/1,
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

-type ra_meta_key() :: atom().
-type segment_ref() :: {File :: binary(), ra_range:range()}.
-type event_body() :: {written, ra_term(), ra_seq:state()} |
                      {segments, [{ets:tid(), ra:range()}], [segment_ref()]} |
                      {resend_write, ra_index()} |
                      {snapshot_written, ra_idxterm(),
                       LiveIndexes :: ra_seq:state(),
                       ra_snapshot:kind()} |
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
         last_wal_write :: {pid(), Ms :: integer()},
         reader :: ra_log_segments:state(),
         mem_table :: ra_mt:state(),
         tx = false :: boolean(),
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
                              max_checkpoints => non_neg_integer()}.


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

pre_init(#{uid := UId,
           system_config := #{data_dir := DataDir}} = Conf) ->
    Dir = server_data_dir(DataDir, UId),
    SnapModule = maps:get(snapshot_module, Conf, ?DEFAULT_SNAPSHOT_MODULE),
    MaxCheckpoints = maps:get(max_checkpoints, Conf, ?DEFAULT_MAX_CHECKPOINTS),
    SnapshotsDir = filename:join(Dir, ?SNAPSHOTS_DIR),
    CheckpointsDir = filename:join(Dir, ?CHECKPOINTS_DIR),
    _ = ra_snapshot:init(UId, SnapModule, SnapshotsDir,
                         CheckpointsDir, undefined, MaxCheckpoints),
    ok.

-spec init(ra_log_init_args()) -> state().
init(#{uid := UId,
       system_config := #{data_dir := DataDir,
                          names := #{wal := Wal,
                                     segment_writer := SegWriter} = Names}
      } = Conf) ->
    Dir = server_data_dir(DataDir, UId),
    MaxOpen = maps:get(max_open_segments, Conf, 5),
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
    Counter = maps:get(counter, Conf, undefined),

    %% ensure directories are there
    ok = ra_lib:make_dir(Dir),
    ok = ra_lib:make_dir(SnapshotsDir),
    ok = ra_lib:make_dir(CheckpointsDir),
    % initialise metrics for this server
    SnapshotState = ra_snapshot:init(UId, SnapModule, SnapshotsDir,
                                     CheckpointsDir, Counter, MaxCheckpoints),
    {SnapIdx, SnapTerm} = case ra_snapshot:current(SnapshotState) of
                              undefined -> {-1, 0};
                              Curr -> Curr
                          end,
    %% TODO: error handling
    %% TODO: the "indexes" file isn't authoritative when it comes to live
    %% indexes, we need to recover the snapshot and query it for live indexes
    %% to get the actual valua
    {ok, LiveIndexes} = ra_snapshot:indexes(
                          ra_snapshot:current_snapshot_dir(SnapshotState)),

    AccessPattern = maps:get(initial_access_pattern, Conf, sequential),
    {ok, Mt0} = ra_log_ets:mem_table_please(Names, UId),
    % recover current range and any references to segments
    % this queries the segment writer and thus blocks until any
    % segments it is currently processed have been finished
    MtRange = ra_mt:range(Mt0),
    SegRefs = my_segrefs(UId, SegWriter),
    Reader = ra_log_segments:init(UId, Dir, MaxOpen, AccessPattern, SegRefs,
                                  Counter, LogId),
    SegmentRange = ra_log_segments:range(Reader),
    %% TODO: check ra_range:add/2 actually performas the correct logic we expect
    Range = ra_range:add(MtRange, SegmentRange),

    [begin
         ?DEBUG("~ts: deleting overwritten segment ~w",
                [LogId, SR]),
         catch prim_file:delete(filename:join(Dir, F))
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
                      last_wal_write = {whereis(Wal), now_ms()},
                      live_indexes = LiveIndexes
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
    LastSegRefIdx = case SegmentRange of
                        undefined ->
                            -1;
                        {_, L} ->
                            L
                    end,
    LastWrittenIdx = case ra_log_wal:last_writer_seq(Wal, UId) of
                         {ok, undefined} ->
                             %% take last segref index
                             max(SnapIdx, LastSegRefIdx);
                         {ok, Idx} ->
                             max(Idx, LastSegRefIdx);
                         {error, wal_down} ->
                             ?ERROR("~ts: ra_log:init/1 cannot complete as wal"
                                    " process is down.",
                                    [State2#?MODULE.cfg#cfg.log_id]),
                             exit(wal_down)
                     end,
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
    assert(State).

-spec close(state()) -> ok.
close(#?MODULE{cfg = #cfg{uid = _UId},
               reader = Reader}) ->
    % deliberately ignoring return value
    % close all open segments
    _ = ra_log_segments:close(Reader),
    ok.

-spec begin_tx(state()) -> state().
begin_tx(State) ->
    State#?MODULE{tx = true}.

-spec commit_tx(state()) -> {ok, state()} | {error, wal_down, state()}.
commit_tx(#?MODULE{cfg = #cfg{uid = UId,
                              wal = Wal} = Cfg,
                   tx = true,
                   mem_table = Mt1} = State) ->
    {Entries, Mt} = ra_mt:commit(Mt1),
    Tid = ra_mt:tid(Mt),
    WriterId = {UId, self()},
    {WalCommands, Num} =
        lists:foldl(fun ({Idx, Term, Cmd0}, {WC, N}) ->
                            Cmd = {ttb, term_to_iovec(Cmd0)},
                            WalC = {append, WriterId, Tid, Idx-1, Idx, Term, Cmd},
                            {[WalC | WC], N+1}
                    end, {[], 0}, Entries),

    case ra_log_wal:write_batch(Wal, lists:reverse(WalCommands)) of
        {ok, Pid} ->
            ok = incr_counter(Cfg, ?C_RA_LOG_WRITE_OPS, Num),
            {ok, State#?MODULE{tx = false,
                               last_wal_write = {Pid, now_ms()},
                               mem_table = Mt}};
        {error, wal_down} ->
            %% still need to return the state here
            {error, wal_down, State#?MODULE{tx = false,
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
                                  Idx, Term, Cmd) of
                {ok, Pid} ->
                    Pend = ra_seq:limit(Idx - 1, Pend0),
                    ok = incr_counter(Cfg, ?C_RA_LOG_WRITE_OPS, 1),
                    put_counter(Cfg, ?C_RA_SVR_METRIC_LAST_INDEX, Idx),
                    State#?MODULE{range = ra_range:extend(Idx, Range),
                                  last_term = Term,
                                  last_wal_write = {Pid, now_ms()},
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
                range = Range,
                tx = true,
                pending = Pend0,
                mem_table = Mt0} = State)
      when ?IS_NEXT_IDX(Idx, Range) ->
    case ra_mt:stage(Entry, Mt0) of
        {ok, Mt} ->
            put_counter(Cfg, ?C_RA_SVR_METRIC_LAST_INDEX, Idx),
            State#?MODULE{range = ra_range:extend(Idx, Range),
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
append({Idx, _, _}, #?MODULE{range = Range}) ->
    Msg = lists:flatten(io_lib:format("tried writing ~b - current range ~w",
                                      [Idx, Range])),
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

-spec write_sparse(log_entry(), ra:index(), state()) ->
    {ok, state()} | {error, wal_down | gap_detected}.
write_sparse({Idx, Term, _} = Entry, PrevIdx0,
             #?MODULE{cfg = #cfg{uid = UId,
                                 wal = Wal} = Cfg,
                      range = Range,
                      mem_table = Mt0} = State0)
  when PrevIdx0 == undefined orelse
       Range == undefined orelse
       (PrevIdx0 == element(2, Range)) ->
    {ok, Mt} = ra_mt:insert_sparse(Entry, PrevIdx0, Mt0),
    ok = incr_counter(Cfg, ?C_RA_LOG_WRITE_OPS, 1),
    Tid = ra_mt:tid(Mt),
    PrevIdx = case PrevIdx0 of
                  undefined ->
                      Idx - 1;
                  _ ->
                      PrevIdx0
              end,
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
            {ok, State0#?MODULE{range = NewRange,
                                last_term = Term,
                                mem_table = Mt,
                                last_wal_write = {Pid, now_ms()}}};
        {error, wal_down} = Err->
            Err
    end.

-spec fold(FromIdx :: ra_index(), ToIdx :: ra_index(),
           fun((log_entry(), Acc) -> Acc), Acc, state()) ->
    {Acc, state()} when Acc :: term().
fold(From0, To0, Fun, Acc0,
     #?MODULE{cfg = Cfg,
              mem_table = Mt,
              range = {StartIdx, EndIdx},
              reader = Reader0} = State)
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
                                               Acc0, Reader0),
            {Acc, State#?MODULE{reader = Reader}};
        {{MtStart, MtEnd}, {RemStart, RemEnd}} ->
            {Reader, Acc1} = ra_log_segments:fold(RemStart, RemEnd, Fun,
                                                Acc0, Reader0),
            Acc = ra_mt:fold(MtStart, MtEnd, Fun, Acc1, Mt),
            NumRead = MtEnd - MtStart + 1,
            ok = incr_counter(Cfg, ?C_RA_LOG_READ_MEM_TBL, NumRead),
            {Acc, State#?MODULE{reader = Reader}};
        {{MtStart, MtEnd}, undefined} ->
            Acc = ra_mt:fold(MtStart, MtEnd, Fun, Acc0, Mt),
            NumRead = MtEnd - MtStart + 1,
            ok = incr_counter(Cfg, ?C_RA_LOG_READ_MEM_TBL, NumRead),
            {Acc, State}
    end;
fold(_From, _To, _Fun, Acc, State) ->
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
                             last_written_index_term = {LWIdx0, _}} = State0) ->
    case fetch_term(Idx, State0) of
        {undefined, State} ->
            {not_found, State};
        {Term, State1} ->
            LWIdx = min(Idx, LWIdx0),
            {LWTerm, State2} = fetch_term(LWIdx, State1),
            %% this should always be found but still assert just in case
            %% _if_ this ends up as a genuine reversal next time we try
            %% to write to the mem table it will detect this and open
            %% a new one
            true = LWTerm =/= undefined,
            put_counter(Cfg, ?C_RA_SVR_METRIC_LAST_INDEX, Idx),
            put_counter(Cfg, ?C_RA_SVR_METRIC_LAST_WRITTEN_INDEX, LWIdx),
            {ok, State2#?MODULE{range = ra_range:limit(Idx + 1, Range),
                                last_term = Term,
                                last_written_index_term = {LWIdx, LWTerm}}}
    end.

-spec handle_event(event_body(), state()) ->
    {state(), [effect()]}.
handle_event({written, Term, WrittenSeq},
             #?MODULE{cfg = Cfg,
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
                    ?DEBUG("~ts: ~p not prefix of ~p",
                           [Cfg#cfg.log_id, WrittenSeq, Pend0]),
                    {resend_pending(State0), []}
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
handle_event({segments, TidRanges, NewSegs},
             #?MODULE{cfg = #cfg{uid = UId,
                                 log_id = LogId,
                                 directory = Dir,
                                 names = Names} = Cfg,
                      reader = Reader0,
                      pending = Pend0,
                      mem_table = Mt0} = State0) ->
    {Reader, OverwrittenSegRefs} = ra_log_segments:update_segments(NewSegs, Reader0),

    put_counter(Cfg, ?C_RA_SVR_METRIC_NUM_SEGMENTS,
                ra_log_segments:segment_ref_count(Reader)),
    %% the tid ranges arrive in the reverse order they were written
    %% (new -> old) so we need to foldr here to process the oldest first
    Mt = lists:foldr(
           fun ({Tid, Seq}, Acc0) ->
                   {Spec, Acc} = ra_mt:record_flushed(Tid, Seq, Acc0),
                   ok = ra_log_ets:execute_delete(Names, UId, Spec),
                   Acc
           end, Mt0, TidRanges),

    %% it is theoretically possible that the segment writer flush _could_
    %% over take WAL notifications
    %%
    FstPend = ra_seq:first(Pend0),
    Pend = case ra_mt:range(Mt) of
               {Start, _End} when Start > FstPend ->
                   ra_seq:floor(Start, Pend0);
               _ ->
                   Pend0
           end,
    State = State0#?MODULE{reader = Reader,
                           pending = Pend,
                           mem_table = Mt},
    Fun = fun () ->
                  [begin
                    ?DEBUG("~ts: deleting overwritten segment ~w",
                           [LogId, SR]),
                    catch prim_file:delete(filename:join(Dir, F))
                   end
                   || {F, _} = SR <- OverwrittenSegRefs],
                  ok
          end,
    {State, [{bg_work, Fun, fun (_Err) -> ok end}]};
handle_event({compaction_result, Result},
             #?MODULE{reader = Reader0} = State) ->
    {Reader, Effs} = ra_log_segments:handle_compaction_result(Result, Reader0),
    {State#?MODULE{reader = Reader}, Effs};
handle_event(major_compaction, #?MODULE{reader = Reader0,
                                        live_indexes = LiveIndexes,
                                        snapshot_state = SS} = State) ->
    case ra_snapshot:current(SS) of
        {SnapIdx, _} ->
            Effs = ra_log_segments:schedule_compaction(major, SnapIdx,
                                                       LiveIndexes, Reader0),
            {State, Effs};
        _ ->
            {State, []}
    end;
handle_event({snapshot_written, {SnapIdx, _} = Snap, LiveIndexes, SnapKind},
             #?MODULE{cfg = #cfg{uid = UId,
                                 names = Names} = Cfg,
                      range = {FstIdx, _} = Range,
                      mem_table = Mt0,
                      pending = Pend0,
                      last_written_index_term = {LastWrittenIdx, _} = LWIdxTerm0,
                      snapshot_state = SnapState0} = State0)
%% only update snapshot if it is newer than the last snapshot
  when SnapIdx >= FstIdx ->
    % ?assert(ra_snapshot:pending(SnapState0) =/= undefined),
    SnapState1 = ra_snapshot:complete_snapshot(Snap, SnapKind, LiveIndexes,
                                               SnapState0),
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
            %% TODO: test that a written even can still be processed if it
            %% contains lower indexes than pending
            SmallestLiveIdx = case ra_seq:first(LiveIndexes) of
                                  undefined ->
                                      SnapIdx + 1;
                                  I ->
                                      I
                              end,
            %$% TODO: optimise - ra_seq:floor/2 is O(n),
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
            CompEffs = ra_log_segments:schedule_compaction(minor, SnapIdx,
                                                           LiveIndexes,
                                                           State#?MODULE.reader),
            Effects = CompEffs ++ Effects0,
            {State, Effects};
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
handle_event({snapshot_written, {Idx, Term} = Snap, _Indexes, SnapKind},
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
    %% mem table here
    {Spec, Mt} = ra_mt:set_first(SmallestLiveIndex, Mt0),
    ok = exec_mem_table_delete(Names, UId, Spec),
    State = State0#?MODULE{snapshot_state = SnapState,
                           current_snapshot = IdxTerm,
                           range = undefined,
                           last_term = SnapTerm,
                           live_indexes = LiveIndexes,
                           mem_table = Mt,
                           last_written_index_term = IdxTerm},
    CompEffs = ra_log_segments:schedule_compaction(minor, SnapIdx,
                                                   LiveIndexes,
                                                   State#?MODULE.reader),
    {ok, State, CompEffs ++ CPEffects}.


-spec recover_snapshot(State :: state()) ->
    option({ra_snapshot:meta(), term()}).
recover_snapshot(#?MODULE{snapshot_state = SnapState}) ->
    case ra_snapshot:recover(SnapState) of
        {ok, Meta, MacState} ->
            {Meta, MacState};
        {error, no_current_snapshot} ->
            undefined
    end.

-spec snapshot_index_term(State :: state()) -> option(ra_idxterm()).
snapshot_index_term(#?MODULE{snapshot_state = SS}) ->
    ra_snapshot:current(SS).

-spec update_release_cursor(Idx :: ra_index(),
                            Cluster :: ra_cluster(),
                            MacModule :: module(),
                            MacState :: term(), State :: state()) ->
    {state(), effects()}.
update_release_cursor(Idx, Cluster, MacModule, MacState, State)
  when is_atom(MacModule) ->
    suggest_snapshot(snapshot, Idx, Cluster, MacModule, MacState, State).

-spec checkpoint(Idx :: ra_index(), Cluster :: ra_cluster(),
                 MacModule :: module(),
                 MacState :: term(), State :: state()) ->
    {state(), effects()}.
checkpoint(Idx, Cluster, MacModule, MacState, State)
  when is_atom(MacModule) ->
    suggest_snapshot(checkpoint, Idx, Cluster, MacModule, MacState, State).

suggest_snapshot(SnapKind, Idx, Cluster, MacModule, MacState,
                 #?MODULE{snapshot_state = SnapshotState} = State) ->
    case ra_snapshot:pending(SnapshotState) of
        undefined ->
            suggest_snapshot0(SnapKind, Idx, Cluster, MacModule, MacState, State);
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
                   last_wal_write = {WalPid, Ms}} = State) ->
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
                live_indexes = LiveIndexes,
                mem_table = _Mt
               } = State) ->
    %% TODO: remove this at some point?
    ?DEBUG("~ts: ra_log: asserting Range ~p Snapshot ~p LiveIndexes ~p",
           [LogId, Range, CurrSnap, LiveIndexes]),
    %% perform assertions to ensure log state is correct
    ?assert(CurrSnap =:= ra_snapshot:current(SnapState)),
    ?assert(Range == undefined orelse
            CurrSnap == undefined orelse
            element(1, Range) - 1 == element(1, CurrSnap)),
    ?assert(CurrSnap == undefined orelse
            LiveIndexes == [] orelse
            ra_seq:last(LiveIndexes) =< element(1, CurrSnap)),
    State.

suggest_snapshot0(SnapKind, Idx, Cluster, MacModule, MacState, State0) ->
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
                    MachineVersion = ra_machine:version(MacModule),
                    Meta = #{index => Idx,
                             cluster => ClusterServerIds,
                             machine_version => MachineVersion},
                    write_snapshot(Meta#{term => Term}, MacModule, MacState,
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
overview(#?MODULE{range = Range,
                  last_term = LastTerm,
                  last_written_index_term = LWIT,
                  snapshot_state = SnapshotState,
                  current_snapshot = CurrSnap,
                  reader = Reader,
                  last_wal_write = {_LastPid, LastMs},
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
    catch ra_log_snapshot_state:delete(ra_log_snapshot_state, UId),
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
                  #?MODULE{cfg = #cfg{uid = UId,
                                      log_id = LogId,
                                      directory = Dir,
                                      counter = Counter},
                           reader = Reader} = State) ->
    ActiveSegs = ra_log_segments:segment_refs(Reader),
    % close all open segments
    % deliberately ignoring return value
    _ = ra_log_segments:close(Reader),
    %% open a new segment with the new max open segment value
    State#?MODULE{reader = ra_log_segments:init(UId, Dir, MaxOpenSegments,
                                                AccessPattern, ActiveSegs,
                                                Counter, LogId)}.


%%% Local functions


%% only used by resend to wal functionality and doesn't update the mem table
wal_rewrite(#?MODULE{cfg = #cfg{uid = UId,
                                wal = Wal} = Cfg,
                    range = _Range} = State,
            Tid, {Idx, Term, Cmd}) ->
    case ra_log_wal:write(Wal, {UId, self()}, Tid, Idx, Term, Cmd) of
        {ok, Pid} ->
            ok = incr_counter(Cfg, ?C_RA_LOG_WRITE_OPS, 1),
            put_counter(Cfg, ?C_RA_SVR_METRIC_LAST_INDEX, Idx),
            State#?MODULE{%last_index = Idx,
                          last_term = Term,
                          last_wal_write = {Pid, now_ms()}
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
    %% all entries in a transaction are written to the same tid
    Tid = ra_mt:tid(Mt0),
    {WalCommands, Num, Pend} =
        lists:foldl(fun ({Idx, Term, Cmd0}, {WC, N, P}) ->
                            Cmd = {ttb, term_to_iovec(Cmd0)},
                            WalC = {append, WriterId, Tid, Idx-1, Idx, Term, Cmd},
                            {[WalC | WC], N+1, ra_seq:append(Idx, P)}
                    end, {[], 0, Pend0}, Entries),

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
                               last_wal_write = {Pid, now_ms()},
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
    try resend_from0(Idx, State0) of
        State -> State
    catch
        error:wal_down ->
            ?WARN("~ts: ra_log: resending from ~b failed with wal_down",
                  [UId, Idx]),
            State0
    end.

resend_pending(#?MODULE{cfg = Cfg,
                        last_resend_time = undefined,
                        pending = Pend,
                        mem_table = Mt} = State) ->
    ?DEBUG("~ts: ra_log: resending from ~b to ~b mt ~p",
           [State#?MODULE.cfg#cfg.log_id, ra_seq:first(Pend),
            ra_seq:last(Pend), ra_mt:range(Mt)]),
    ok = incr_counter(Cfg, ?C_RA_LOG_WRITE_RESENDS, ra_seq:length(Pend)),
    ra_seq:fold(fun (I, Acc) ->
                        {I, T, C} = ra_mt:lookup(I, Mt),
                        Tid = ra_mt:tid_for(I, T, Mt),
                        wal_rewrite(Acc, Tid, {I, T, C})
                end,
                State#?MODULE{last_resend_time = {erlang:system_time(seconds),
                                                  whereis(Cfg#cfg.wal)}},
                Pend).

resend_from0(Idx, #?MODULE{cfg = Cfg,
                           range = {_, LastIdx},
                           last_resend_time = undefined,
                           mem_table = Mt} = State) ->
    ?DEBUG("~ts: ra_log: resending from ~b to ~b",
           [State#?MODULE.cfg#cfg.log_id, Idx, LastIdx]),
    ok = incr_counter(Cfg, ?C_RA_LOG_WRITE_RESENDS, LastIdx - Idx + 1),
    lists:foldl(fun (I, Acc) ->
                        {I, T, C} = ra_mt:lookup(I, Mt),
                        Tid = ra_mt:tid_for(I, T, Mt),
                        wal_rewrite(Acc, Tid, {I, T, C})
                end,
                State#?MODULE{last_resend_time = {erlang:system_time(seconds),
                                                  whereis(Cfg#cfg.wal)}},
                lists:seq(Idx, LastIdx));
resend_from0(Idx, #?MODULE{last_resend_time = {LastResend, WalPid},
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
            Msg = io_lib:format("ra_log:verify_entries/2 "
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

%%%% TESTS

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

% pick_range_test() ->
%     Ranges1 = [{76, 90}, {50, 75}, {1, 100}],
%     {1, 90} = pick_range(Ranges1, undefined),

%     Ranges2 = [{76, 110}, {50, 75}, {1, 49}],
%     {1, 110} = pick_range(Ranges2, undefined),

%     Ranges3 = [{25, 30}, {25, 35}, {1, 50}],
%     {1, 30} = pick_range(Ranges3, undefined),
%     ok.
-endif.
