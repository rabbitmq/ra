%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term Broadcom refers to Broadcom Inc. and/or its subsidiaries.
%%
%% @hidden
-module(ra_log).

-compile([inline_list_funcs]).

-export([pre_init/1,
         init/1,
         close/1,
         begin_tx/1,
         commit_tx/1,
         append/2,
         write/2,
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
         install_snapshot/3,
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

         % external reader
         register_reader/2,
         readers/1,
         tick/2
        ]).

-include("ra.hrl").

-define(DEFAULT_RESEND_WINDOW_SEC, 20).
-define(MIN_SNAPSHOT_INTERVAL, 4096).
-define(MIN_CHECKPOINT_INTERVAL, 16384).
-define(LOG_APPEND_TIMEOUT, 5000).
-define(WAL_RESEND_TIMEOUT, 5000).

-type ra_meta_key() :: atom().
-type segment_ref() :: {ra_range:range(), File :: file:filename_all()}.
-type event_body() :: {written, ra_term(), ra:range()} |
                      {segments, [{ets:tid(), ra:range()}], [segment_ref()]} |
                      {resend_write, ra_index()} |
                      {snapshot_written, ra_idxterm(), ra_snapshot:kind()} |
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
         first_index = -1 :: ra_index(),
         last_index = -1 :: -1 | ra_index(),
         last_term = 0 :: ra_term(),
         last_written_index_term = {0, 0} :: ra_idxterm(),
         snapshot_state :: ra_snapshot:state(),
         last_resend_time :: option({integer(), WalPid :: pid() | undefined}),
         last_wal_write :: {pid(), Ms :: integer()},
         reader :: ra_log_reader:state(),
         readers = [] :: [pid()],
         mem_table :: ra_mt:state(),
         tx = false :: boolean()
        }).

-record(read_plan, {dir :: file:filename_all(),
                    read :: #{ra_index() := log_entry()},
                    plan :: ra_log_reader:read_plan()}).

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
      last_index := ra_index(),
      last_term := ra_term(),
      first_index := ra_index(),
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
                              undefined -> {-1, -1};
                              Curr -> Curr
                          end,

    AccessPattern = maps:get(initial_access_pattern, Conf, sequential),
    {ok, Mt0} = ra_log_ets:mem_table_please(Names, UId),
    % recover current range and any references to segments
    % this queries the segment writer and thus blocks until any
    % segments it is currently processed have been finished
    MtRange = ra_mt:range(Mt0),
    {{FirstIdx, LastIdx0}, SegRefs} = case recover_ranges(UId, MtRange, SegWriter) of
                                          {undefined, SRs} ->
                                              {{-1, -1}, SRs};
                                          R ->  R
                                      end,
    %% TODO: don't think this is necessary given the range is calculated from this
    %% but can't hurt as it may trigger some cleanup
    {DeleteSpecs, Mt} = ra_mt:set_first(FirstIdx, Mt0),

    ok = exec_mem_table_delete(Names, UId, DeleteSpecs),
    Reader = ra_log_reader:init(UId, Dir, MaxOpen, AccessPattern, SegRefs,
                                Names, Counter),
    %% assert there is no gap between the snapshot
    %% and the first index in the log
    case (FirstIdx - SnapIdx) > 1 of
        true ->
            exit({corrupt_log, gap_between_snapshot_and_first_index,
                  {SnapIdx, FirstIdx}});
        false -> ok
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
                      first_index = max(SnapIdx + 1, FirstIdx),
                      last_index = max(SnapIdx, LastIdx0),
                      reader = Reader,
                      mem_table = Mt,
                      snapshot_state = SnapshotState,
                      last_wal_write = {whereis(Wal), now_ms()}
                     },
    put_counter(Cfg, ?C_RA_SVR_METRIC_SNAPSHOT_INDEX, SnapIdx),
    LastIdx = State0#?MODULE.last_index,
    put_counter(Cfg, ?C_RA_SVR_METRIC_LAST_INDEX, LastIdx),
    put_counter(Cfg, ?C_RA_SVR_METRIC_LAST_WRITTEN_INDEX, LastIdx),
    put_counter(Cfg, ?C_RA_SVR_METRIC_NUM_SEGMENTS, ra_log_reader:segment_ref_count(Reader)),
    case ra_snapshot:latest_checkpoint(SnapshotState) of
        undefined ->
            ok;
        {ChIdx, _ChTerm} ->
            put_counter(Cfg, ?C_RA_SVR_METRIC_CHECKPOINT_INDEX, ChIdx)
    end,

    % recover the last term
    {LastTerm0, State2} = case LastIdx of
                               SnapIdx ->
                                   {SnapTerm, State0};
                               -1 ->
                                   {0, State0};
                               LI ->
                                   fetch_term(LI, State0)
                           end,
    LastSegRefIdx = case SegRefs of
                        [] ->
                            -1;
                        [{{_, L}, _} | _] ->
                            L
                    end,
    LastWrittenIdx = case ra_log_wal:last_writer_seq(Wal, UId) of
                         {ok, undefined} ->
                             %% take last segref index
                             max(SnapIdx, LastSegRefIdx);
                         {ok, Idx} ->
                             max(Idx, LastSegRefIdx);
                         {error, wal_down} ->
                             ?ERROR("~ts: ra_log:init/1 cannot complete as wal process is down.",
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
           [State#?MODULE.cfg#cfg.log_id,
            last_index_term(State),
            {SnapIdx, SnapTerm},
            State#?MODULE.last_written_index_term
           ]),
    element(1, delete_segments(SnapIdx, State)).

-spec close(state()) -> ok.
close(#?MODULE{cfg = #cfg{uid = _UId},
               reader = Reader}) ->
    % deliberately ignoring return value
    % close all open segments
    _ = ra_log_reader:close(Reader),
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
                            WalC = {append, WriterId, Tid, Idx, Term, Cmd},
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

-spec append(Entry :: log_entry(), State :: state()) ->
    state() | no_return().
append({Idx, Term, Cmd0} = Entry,
       #?MODULE{cfg = #cfg{uid = UId,
                           wal = Wal} = Cfg,
                last_index = LastIdx,
                tx = false,
                mem_table = Mt0} = State)
      when Idx =:= LastIdx + 1 ->
    case ra_mt:insert(Entry, Mt0) of
        {ok, Mt} ->
            Cmd = {ttb, term_to_iovec(Cmd0)},
            case ra_log_wal:write(Wal, {UId, self()}, ra_mt:tid(Mt),
                                  Idx, Term, Cmd) of
                {ok, Pid} ->
                    ok = incr_counter(Cfg, ?C_RA_LOG_WRITE_OPS, 1),
                    put_counter(Cfg, ?C_RA_SVR_METRIC_LAST_INDEX, Idx),
                    State#?MODULE{last_index = Idx,
                                  last_term = Term,
                                  last_wal_write = {Pid, now_ms()},
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
                last_index = LastIdx,
                tx = true,
                mem_table = Mt0} = State)
      when Idx =:= LastIdx + 1 ->
    case ra_mt:stage(Entry, Mt0) of
        {ok, Mt} ->
            put_counter(Cfg, ?C_RA_SVR_METRIC_LAST_INDEX, Idx),
            State#?MODULE{last_index = Idx,
                          last_term = Term,
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
append({Idx, _, _}, #?MODULE{last_index = LastIdx}) ->
    Msg = lists:flatten(io_lib:format("tried writing ~b - expected ~b",
                                      [Idx, LastIdx+1])),
    exit({integrity_error, Msg}).

-spec write(Entries :: [log_entry()], State :: state()) ->
    {ok, state()} |
    {error, {integrity_error, term()} | wal_down}.
write([{FstIdx, _, _} | _Rest] = Entries,
      #?MODULE{cfg = Cfg,
               last_index = LastIdx,
               mem_table = Mt0} = State0)
  when FstIdx =< LastIdx + 1 andalso
       FstIdx >= 0 ->
    case stage_entries(Cfg, Entries, Mt0) of
        {ok, Mt} ->
            wal_write_batch(State0#?MODULE{mem_table = Mt}, Entries);
        Error ->
            Error
    end;
write([], State) ->
    {ok, State};
write([{Idx, _, _} | _], #?MODULE{cfg = #cfg{uid = UId},
                                  last_index = LastIdx}) ->
    Msg = lists:flatten(io_lib:format("~s: ra_log:write/2 "
                                      "tried writing ~b - expected ~b",
                                      [UId, Idx, LastIdx+1])),
    {error, {integrity_error, Msg}}.

-spec fold(FromIdx :: ra_index(), ToIdx :: ra_index(),
           fun((log_entry(), Acc) -> Acc), Acc, state()) ->
    {Acc, state()} when Acc :: term().
fold(From0, To0, Fun, Acc0,
     #?MODULE{cfg = Cfg,
              mem_table = Mt,
              first_index = FirstIdx,
              last_index = LastIdx,
              reader = Reader0} = State)
  when To0 >= From0 andalso
       To0 >= FirstIdx ->
    From = max(From0, FirstIdx),
    To = min(To0, LastIdx),
    ok = incr_counter(Cfg, ?C_RA_LOG_READ_OPS, 1),

    MtOverlap = ra_mt:range_overlap({From, To}, Mt),
    case MtOverlap of
        {undefined, {RemStart, RemEnd}} ->
            {Reader, Acc} = ra_log_reader:fold(RemStart, RemEnd, Fun, Acc0, Reader0),
            {Acc, State#?MODULE{reader = Reader}};
        {{MtStart, MtEnd}, {RemStart, RemEnd}} ->
            {Reader, Acc1} = ra_log_reader:fold(RemStart, RemEnd, Fun, Acc0, Reader0),
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
                               last_index = LastIdx,
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
    Indexes2 = lists:dropwhile(fun (I) -> I > LastIdx end, Indexes1),
    {Entries0, MemTblNumRead, Indexes} = ra_mt:get_items(Indexes2, Mt),
    ok = incr_counter(Cfg, ?C_RA_LOG_READ_MEM_TBL, MemTblNumRead),
    {Entries1, Reader} = ra_log_reader:sparse_read(Reader0, Indexes, Entries0),
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
                   fun ((ra_index(), ra_term(), ra_server:command()) -> term())
                   ) ->
    read_plan().
partial_read(Indexes0, #?MODULE{cfg = Cfg,
                                reader = Reader0,
                                last_index = LastIdx,
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

    %% drop any indexes that are larger than the last index available
    Indexes2 = lists:dropwhile(fun (I) -> I > LastIdx end, Indexes1),
    {Entries0, MemTblNumRead, Indexes} = ra_mt:get_items(Indexes2, Mt),
    ok = incr_counter(Cfg, ?C_RA_LOG_READ_MEM_TBL, MemTblNumRead),
    Read = lists:foldl(fun ({I, T, Cmd}, Acc) ->
                               maps:put(I, TransformFun(I, T, Cmd), Acc)
                       end, #{}, Entries0),

    Plan = ra_log_reader:read_plan(Reader0, Indexes),
    #read_plan{dir = Cfg#cfg.directory,
               read = Read,
               plan = Plan}.


-spec execute_read_plan(read_plan(), undefined | ra_flru:state(),
                        TransformFun :: transform_fun(),
                        ra_log_reader:read_plan_options()) ->
    {#{ra_index() => Command :: term()}, ra_flru:state()}.
execute_read_plan(#read_plan{dir = Dir,
                             read = Read,
                             plan = Plan}, Flru0, TransformFun,
                  Options) ->
    ra_log_reader:exec_read_plan(Dir, Plan, Flru0, TransformFun,
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


-spec last_index_term(state()) -> ra_idxterm().
last_index_term(#?MODULE{last_index = LastIdx, last_term = LastTerm}) ->
    {LastIdx, LastTerm}.

-spec last_written(state()) -> ra_idxterm().
last_written(#?MODULE{last_written_index_term = LWTI}) ->
    LWTI.

%% forces the last index and last written index back to a prior index
-spec set_last_index(ra_index(), state()) ->
    {ok, state()} | {not_found, state()}.
set_last_index(Idx, #?MODULE{cfg = Cfg,
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
            {ok, State2#?MODULE{last_index = Idx,
                                last_term = Term,
                                last_written_index_term = {LWIdx, LWTerm}}}
    end.

-spec handle_event(event_body(), state()) ->
    {state(), [effect()]}.
handle_event({written, _Term, {FromIdx, _ToIdx}},
             #?MODULE{last_index = LastIdx} = State)
  when FromIdx > LastIdx ->
    %% we must have reverted back, either by explicit reset or by a snapshot
    %% installation taking place whilst the WAL was processing the write
    %% Just drop the event in this case as it is stale
    {State, []};
handle_event({written, Term, {FromIdx, ToIdx}},
             #?MODULE{cfg = Cfg,
                      last_written_index_term = {LastWrittenIdx0,
                                                 _LastWrittenTerm0},
                      first_index = FirstIdx} = State0)
  when FromIdx =< LastWrittenIdx0 + 1 ->
    % We need to ignore any written events for the same index
    % but in a prior term if we do not we may end up confirming
    % to a leader writes that have not yet
    % been fully flushed
    case fetch_term(ToIdx, State0) of
        {Term, State} when is_integer(Term) ->
            ok = put_counter(Cfg, ?C_RA_SVR_METRIC_LAST_WRITTEN_INDEX, ToIdx),
            {State#?MODULE{last_written_index_term = {ToIdx, Term}}, []};
        {undefined, State} when ToIdx < FirstIdx ->
            % A snapshot happened before the written event came in
            % This can only happen on a leader when consensus is achieved by
            % followers returning appending the entry and the leader committing
            % and processing a snapshot before the written event comes in.
            {State, []};
        {OtherTerm, State} ->
            %% term mismatch, let's reduce the range and try again to see
            %% if any entries in the range are valid
            case ra_range:new(FromIdx, ToIdx-1) of
                undefined ->
                    ?DEBUG("~ts: written event did not find term ~b for index ~b "
                           "found ~w",
                           [State#?MODULE.cfg#cfg.log_id, Term, ToIdx, OtherTerm]),
                    {State, []};
                NextWrittenRange ->
                    %% retry with a reduced range
                    handle_event({written, Term, NextWrittenRange}, State0)
            end
    end;
handle_event({written, _Term, {FromIdx, _}} = Evt,
             #?MODULE{cfg = #cfg{log_id = LogId},
                      mem_table = Mt,
                      last_written_index_term = {LastWrittenIdx, _}} = State0)
  when FromIdx > LastWrittenIdx + 1 ->
    % leaving a gap is not ok - may need to resend from mem table
    Expected = LastWrittenIdx + 1,
    MtRange = ra_mt:range(Mt),
    case ra_range:in(Expected, MtRange) of
        true ->
            ?INFO("~ts: ra_log: written gap detected at ~b expected ~b!",
                  [LogId, FromIdx, Expected]),
            {resend_from(Expected, State0), []};
        false ->
            ?INFO("~ts: ra_log: written gap detected at ~b but is outside
                  of mem table range. Updating last written index to ~b!",
                  [LogId, FromIdx, Expected]),
            %% if the entry is not in the mem table we may have missed a
            %% written event due to wal crash. Accept written event by updating
            %% last written index term and recursing
            {Term, State} = fetch_term(Expected, State0),
            handle_event(Evt,
                         State#?MODULE{last_written_index_term = {Expected, Term}})
    end;
handle_event({segments, TidRanges, NewSegs},
             #?MODULE{cfg = #cfg{uid = UId, names = Names} = Cfg,
                      reader = Reader0,
                      mem_table = Mt0,
                      readers = Readers
                     } = State0) ->
    Reader = ra_log_reader:update_segments(NewSegs, Reader0),
    put_counter(Cfg, ?C_RA_SVR_METRIC_NUM_SEGMENTS, ra_log_reader:segment_ref_count(Reader)),
    %% the tid ranges arrive in the reverse order they were written
    %% (new -> old) so we need to foldr here to process the oldest first
    Mt = lists:foldr(
           fun ({Tid, Range}, Acc0) ->
                   {Spec, Acc} = ra_mt:record_flushed(Tid, Range, Acc0),
                    ok = ra_log_ets:execute_delete(Names, UId, Spec),
                    Acc
           end, Mt0, TidRanges),
    State = State0#?MODULE{reader = Reader,
                           mem_table = Mt},
    case Readers of
        [] ->
            {State, []};
        _ ->
            %% HACK: but this feature is deprecated anyway
            %% Dummy pid to swallow update notifications
            Pid = spawn(fun () -> ok end),
            {State, log_update_effects(Readers, Pid, State)}
    end;
handle_event({snapshot_written, {SnapIdx, _} = Snap, SnapKind},
             #?MODULE{cfg = #cfg{uid = UId,
                                 names = Names} = Cfg,
                      first_index = FstIdx,
                      last_index = LstIdx,
                      mem_table = Mt0,
                      last_written_index_term = {LastWrittenIdx, _} = LWIdxTerm0,
                      snapshot_state = SnapState0} = State0)
%% only update snapshot if it is newer than the last snapshot
  when SnapIdx >= FstIdx ->
    SnapState1 = ra_snapshot:complete_snapshot(Snap, SnapKind, SnapState0),
    case SnapKind of
        snapshot ->
            put_counter(Cfg, ?C_RA_SVR_METRIC_SNAPSHOT_INDEX, SnapIdx),
            % delete any segments outside of first_index
            {State, Effects0} = delete_segments(SnapIdx, State0),
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
            Effects1 = [DeleteCurrentSnap | CPEffects] ++ Effects0,

            {LWIdxTerm, Effects} =
                case LastWrittenIdx > SnapIdx of
                    true ->
                        {LWIdxTerm0, Effects1};
                    false ->
                        {Snap, Effects1}
                end,
            %% this will race with the segment writer but if the
            %% segwriter detects a missing index it will query the snaphost
            %% state and if that is higher it will resume flush
            {Spec, Mt1} = ra_mt:set_first(SnapIdx + 1, Mt0),
            ok = exec_mem_table_delete(Names, UId, Spec),

            {State#?MODULE{first_index = SnapIdx + 1,
                           last_index = max(LstIdx, SnapIdx),
                           last_written_index_term = LWIdxTerm,
                           mem_table = Mt1,
                           snapshot_state = SnapState}, Effects};
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
handle_event({snapshot_written, {Idx, Term} = Snap, SnapKind},
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
handle_event({resend_write, Idx},
             #?MODULE{cfg =#cfg{log_id = LogId}} = State) ->
    % resend missing entries from mem tables.
    ?INFO("~ts: ra_log: wal requested resend from ~b",
          [LogId, Idx]),
    {resend_from(Idx, State), []};
handle_event({down, Pid, _Info},
             #?MODULE{readers = Readers} =
             State) ->
    {State#?MODULE{readers = lists:delete(Pid, Readers)}, []}.

-spec next_index(state()) -> ra_index().
next_index(#?MODULE{last_index = LastIdx}) ->
    LastIdx + 1.

-spec fetch(ra_index(), state()) ->
    {option(log_entry()), state()}.
fetch(Idx, State0) ->
    case fold(Idx, Idx, fun(E, Acc) -> [E | Acc] end, [], State0) of
        {[], State} ->
            {undefined, State};
        {[Entry], State} ->
            {Entry, State}
    end.

-spec fetch_term(ra_index(), state()) ->
    {option(ra_term()), state()}.
fetch_term(Idx, #?MODULE{last_index = LastIdx,
                         first_index = FirstIdx} = State0)
  when Idx < FirstIdx orelse Idx > LastIdx ->
    {undefined, State0};
fetch_term(Idx, #?MODULE{mem_table = Mt, reader = Reader0} = State0) ->
    case ra_mt:lookup_term(Idx, Mt) of
        undefined ->
            {Term, Reader} = ra_log_reader:fetch_term(Idx, Reader0),
            {Term, State0#?MODULE{reader = Reader}};
        Term when is_integer(Term) ->
            {Term, State0}
    end.

-spec snapshot_state(State :: state()) -> ra_snapshot:state().
snapshot_state(State) ->
    State#?MODULE.?FUNCTION_NAME.

-spec set_snapshot_state(ra_snapshot:state(), state()) -> state().
set_snapshot_state(SnapState, State) ->
    State#?MODULE{snapshot_state = SnapState}.

-spec install_snapshot(ra_idxterm(), ra_snapshot:state(), state()) ->
    {state(), effects()}.
install_snapshot({SnapIdx, SnapTerm} = IdxTerm, SnapState0,
                 #?MODULE{cfg = #cfg{uid = UId,
                                     names = Names} = Cfg,
                          mem_table = Mt0
                         } = State0) ->
    ok = incr_counter(Cfg, ?C_RA_LOG_SNAPSHOTS_INSTALLED, 1),
    ok = put_counter(Cfg, ?C_RA_SVR_METRIC_SNAPSHOT_INDEX, SnapIdx),
    put_counter(Cfg, ?C_RA_SVR_METRIC_LAST_INDEX, SnapIdx),
    put_counter(Cfg, ?C_RA_SVR_METRIC_LAST_WRITTEN_INDEX, SnapIdx),
    {State, Effs} = delete_segments(SnapIdx, State0),
    {SnapState, Checkpoints} =
        ra_snapshot:take_older_checkpoints(SnapIdx, SnapState0),
    CPEffects = [{delete_snapshot,
                  ra_snapshot:directory(SnapState, checkpoint),
                  Checkpoint} || Checkpoint <- Checkpoints],
    {Spec, Mt} = ra_mt:set_first(SnapIdx, Mt0),
    ok = exec_mem_table_delete(Names, UId, Spec),
    {State#?MODULE{snapshot_state = SnapState,
                   first_index = SnapIdx + 1,
                   last_index = SnapIdx,
                   last_term = SnapTerm,
                   mem_table = Mt,
                   last_written_index_term = IdxTerm},
     Effs ++ CPEffects}.

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

-spec update_release_cursor(Idx :: ra_index(), Cluster :: ra_cluster(),
                            MacVersion :: ra_machine:version(),
                            MacState :: term(), State :: state()) ->
    {state(), effects()}.
update_release_cursor(Idx, Cluster, MacVersion, MacState, State) ->
    suggest_snapshot(snapshot, Idx, Cluster, MacVersion, MacState, State).

-spec checkpoint(Idx :: ra_index(), Cluster :: ra_cluster(),
                 MacVersion :: ra_machine:version(),
                 MacState :: term(), State :: state()) ->
    {state(), effects()}.
checkpoint(Idx, Cluster, MacVersion, MacState, State) ->
    suggest_snapshot(checkpoint, Idx, Cluster, MacVersion, MacState, State).

suggest_snapshot(SnapKind, Idx, Cluster, MacVersion, MacState,
                 #?MODULE{snapshot_state = SnapshotState} = State) ->
    case ra_snapshot:pending(SnapshotState) of
        undefined ->
            suggest_snapshot0(SnapKind, Idx, Cluster, MacVersion, MacState, State);
        _ ->
            %% Only one snapshot or checkpoint may be written at a time to
            %% prevent excessive I/O usage.
            {State, []}
    end.

promote_checkpoint(Idx, #?MODULE{cfg = Cfg,
                                 snapshot_state = SnapState0} = State) ->
    case ra_snapshot:pending(SnapState0) of
        {_WriterPid, _IdxTerm, snapshot} ->
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

suggest_snapshot0(SnapKind, Idx, Cluster, MacVersion, MacState, State0) ->
    ClusterServerIds = maps:map(fun (_, V) ->
                                        maps:with([voter_status], V)
                                end, Cluster),
    Meta = #{index => Idx,
             cluster => ClusterServerIds,
             machine_version => MacVersion},
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
                    write_snapshot(Meta#{term => Term}, MacState,
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
    CanFreeSegments = case ra_log_reader:range(Reader) of
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
overview(#?MODULE{last_index = LastIndex,
                  last_term = LastTerm,
                  first_index = FirstIndex,
                  last_written_index_term = LWIT,
                  snapshot_state = SnapshotState,
                  reader = Reader,
                  last_wal_write = {_LastPid, LastMs},
                  mem_table = Mt
                 }) ->
    CurrSnap = ra_snapshot:current(SnapshotState),
    #{type => ?MODULE,
      last_index => LastIndex,
      last_term => LastTerm,
      first_index => FirstIndex,
      last_written_index_term => LWIT,
      num_segments => ra_log_reader:segment_ref_count(Reader),
      segments_range => ra_log_reader:range(Reader),
      open_segments => ra_log_reader:num_open_segments(Reader),
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
      last_wal_write => LastMs
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
                           snapshot_state = SnapState} = Log) ->
    _ = close(Log),
    %% if there is a snapshot process pending it could cause the directory
    %% deletion to fail, best kill the snapshot process first
    ok = ra_log_ets:delete_mem_tables(Names, UId),
    catch ets:delete(ra_log_snapshot_state, UId),
    case ra_snapshot:pending(SnapState) of
        {Pid, _, _} ->
            case is_process_alive(Pid) of
                true ->
                    exit(Pid, kill),
                    ok;
                false ->
                    ok
            end;
        _ ->
            ok
    end,
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
release_resources(MaxOpenSegments,
                  AccessPattern,
                  #?MODULE{cfg = #cfg{uid = UId,
                                      directory = Dir,
                                      counter = Counter,
                                      names = Names},
                           reader = Reader} = State) ->
    ActiveSegs = ra_log_reader:segment_refs(Reader),
    % close all open segments
    % deliberately ignoring return value
    _ = ra_log_reader:close(Reader),
    %% open a new segment with the new max open segment value
    State#?MODULE{reader = ra_log_reader:init(UId, Dir, MaxOpenSegments,
                                              AccessPattern,
                                              ActiveSegs, Names, Counter)}.

-spec register_reader(pid(), state()) ->
    {state(), effects()}.
register_reader(Pid, #?MODULE{cfg = #cfg{uid = UId,
                                         directory = Dir,
                                         names = Names},
                              reader = Reader,
                              readers = Readers} = State) ->
    SegRefs = ra_log_reader:segment_refs(Reader),
    NewReader = ra_log_reader:init(UId, Dir, 1, SegRefs, Names),
    {State#?MODULE{readers = [Pid | Readers]},
     [{reply, {ok, NewReader}},
      {monitor, process, log, Pid}]}.

readers(#?MODULE{readers = Readers}) ->
    Readers.


%%% Local functions

log_update_effects(Pids, ReplyPid, #?MODULE{first_index = Idx,
                                            reader = Reader}) ->
    SegRefs = ra_log_reader:segment_refs(Reader),
    [{send_msg, P, {ra_log_update, ReplyPid, Idx, SegRefs},
      [ra_event, local]} || P <- Pids].


%% deletes all segments where the last index is lower than
%% the Idx argument
delete_segments(SnapIdx, #?MODULE{cfg = #cfg{log_id = LogId,
                                             segment_writer = SegWriter,
                                             uid = UId} = Cfg,
                                  readers = Readers,
                                  reader = Reader0} = State0) ->
    case ra_log_reader:update_first_index(SnapIdx + 1, Reader0) of
        {Reader, []} ->
            State = State0#?MODULE{reader = Reader},
            {State, log_update_effects(Readers, undefined, State)};
        {Reader, [Pivot | _] = Obsolete} ->
            Pid = spawn(
                    fun () ->
                            ok = log_update_wait_n(length(Readers)),
                            ok = ra_log_segment_writer:truncate_segments(SegWriter,
                                                                         UId, Pivot)
                    end),
            NumActive = ra_log_reader:segment_ref_count(Reader),
            ?DEBUG("~ts: ~b obsolete segments at ~b - remaining: ~b, pivot ~0p",
                   [LogId, length(Obsolete), SnapIdx, NumActive, Pivot]),
            put_counter(Cfg, ?C_RA_SVR_METRIC_NUM_SEGMENTS, NumActive),
            State = State0#?MODULE{reader = Reader},
            {State, log_update_effects(Readers, Pid, State)}
    end.

%% unly used by resend to wal functionality and doesn't update the mem table
wal_rewrite(#?MODULE{cfg = #cfg{uid = UId,
                                wal = Wal} = Cfg} = State,
            Tid, {Idx, Term, Cmd}) ->
    case ra_log_wal:write(Wal, {UId, self()}, Tid, Idx, Term, Cmd) of
        {ok, Pid} ->
            ok = incr_counter(Cfg, ?C_RA_LOG_WRITE_OPS, 1),
            put_counter(Cfg, ?C_RA_SVR_METRIC_LAST_INDEX, Idx),
            State#?MODULE{last_index = Idx,
                          last_term = Term,
                          last_wal_write = {Pid, now_ms()}
                         };
        {error, wal_down} ->
            error(wal_down)
    end.

wal_write_batch(#?MODULE{cfg = #cfg{uid = UId,
                                    wal = Wal} = Cfg,
                         mem_table = Mt0} = State,
                Entries) ->
    WriterId = {UId, self()},
    %% all entries in a transaction are written to the same tid
    Tid = ra_mt:tid(Mt0),
    {WalCommands, Num} =
        lists:foldl(fun ({Idx, Term, Cmd0}, {WC, N}) ->
                            Cmd = {ttb, term_to_iovec(Cmd0)},
                            WalC = {append, WriterId, Tid, Idx, Term, Cmd},
                            {[WalC | WC], N+1}
                    end, {[], 0}, Entries),

    [{_, _, _, LastIdx, LastTerm, _} | _] = WalCommands,
    {_, Mt} = ra_mt:commit(Mt0),
    put_counter(Cfg, ?C_RA_SVR_METRIC_LAST_INDEX, LastIdx),
    ok = incr_counter(Cfg, ?C_RA_LOG_WRITE_OPS, Num),
    case ra_log_wal:write_batch(Wal, lists:reverse(WalCommands)) of
        {ok, Pid} ->
            {ok, State#?MODULE{last_index = LastIdx,
                               last_term = LastTerm,
                               last_wal_write = {Pid, now_ms()},
                               mem_table = Mt}};
        {error, wal_down} = Err ->
            %% if we get there the entry has already been inserted
            %% into the mem table but never reached the wal
            %% the resend logic will take care of that
            Err
    end.

maybe_append_first_entry(State0 = #?MODULE{last_index = -1}) ->
    State = append({0, 0, undefined}, State0),
    receive
        {ra_log_event, {written, 0, {0, 0}}} ->
            ok
    after 60000 ->
              exit({?FUNCTION_NAME, timeout})
    end,
    State#?MODULE{first_index = 0,
                  last_written_index_term = {0, 0}};
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

resend_from0(Idx, #?MODULE{cfg = Cfg,
                           last_index = LastIdx,
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



write_snapshot(Meta, MacRef, SnapKind,
               #?MODULE{cfg = Cfg,
                        snapshot_state = SnapState0} = State) ->
    Counter = case SnapKind of
                  snapshot -> ?C_RA_LOG_SNAPSHOTS_WRITTEN;
                  checkpoint -> ?C_RA_LOG_CHECKPOINTS_WRITTEN
              end,
    ok = incr_counter(Cfg, Counter, 1),
    {SnapState, Effects} = ra_snapshot:begin_snapshot(Meta, MacRef, SnapKind,
                                                      SnapState0),
    {State#?MODULE{snapshot_state = SnapState}, Effects}.

recover_ranges(UId, MtRange, SegWriter) ->
    % 1. check mem_tables (this assumes wal has finished recovering
    % which means it is essential that ra_servers are part of the same
    % supervision tree
    % 2. check segments
    SegFiles = ra_log_segment_writer:my_segments(SegWriter, UId),
    SegRefs = lists:foldl(
                fun (File, Acc) ->
                        %% if a server recovered when a segment had been opened
                        %% but never had any entries written the segref would be
                        %% undefined
                        case ra_log_segment:segref(File) of
                            undefined ->
                                Acc;
                            SegRef ->
                                [SegRef | Acc]
                        end
                end, [], SegFiles),
    SegRanges = [Range || {Range, _} <- SegRefs],
    Ranges = [MtRange | SegRanges],
    {pick_range(Ranges, undefined), SegRefs}.

% picks the current range from a sorted (newest to oldest) list of ranges
pick_range([], Res) ->
    Res;
pick_range([H | Tail], undefined) ->
    pick_range(Tail, H);
pick_range([{Fst, _Lst} | Tail], {CurFst, CurLst}) ->
    pick_range(Tail, {min(Fst, CurFst), CurLst}).


%% TODO: implement synchronous writes using gen_batch_server:call/3
await_written_idx(Idx, Term, Log0) ->
    IDX = Idx,
    receive
        {ra_log_event, {written, Term, {_, IDX}} = Evt} ->
            {Log, _} = handle_event(Evt, Log0),
            Log;
        {ra_log_event, {written, _, _} = Evt} ->
            {Log, _} = handle_event(Evt, Log0),
            await_written_idx(Idx, Term, Log)
    after ?LOG_APPEND_TIMEOUT ->
              throw(ra_log_append_timeout)
    end.

log_update_wait_n(0) ->
    ok;
log_update_wait_n(N) ->
    receive
        ra_log_update_processed ->
            log_update_wait_n(N - 1)
    after 1500 ->
              %% just go ahead anyway
              ok
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

pick_range_test() ->
    Ranges1 = [{76, 90}, {50, 75}, {1, 100}],
    {1, 90} = pick_range(Ranges1, undefined),

    Ranges2 = [{76, 110}, {50, 75}, {1, 49}],
    {1, 110} = pick_range(Ranges2, undefined),

    Ranges3 = [{25, 30}, {25, 35}, {1, 50}],
    {1, 30} = pick_range(Ranges3, undefined),
    ok.
-endif.
