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
         append/2,
         write/2,
         append_sync/2,
         write_sync/2,
         fold/5,
         sparse_read/2,
         last_index_term/1,
         set_last_index/2,
         reset_to_last_known_written/1,
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
         needs_cache_flush/1,

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
-type segment_ref() :: {From :: ra_index(), To :: ra_index(),
                        File :: string()}.
-type event_body() :: {written, {From :: ra_index(),
                                 To :: ra_index(),
                                 ToTerm :: ra_term()}} |
                      {segments, ets:tid(), [segment_ref()]} |
                      {resend_write, ra_index()} |
                      {snapshot_written, ra_idxterm(), ra_snapshot:kind()} |
                      {down, pid(), term()}.

-type event() :: {ra_log_event, event_body()}.

-type effect() ::
    {delete_snapshot, Dir :: file:filename(), ra_idxterm()} |
    {monitor, process, log, pid()} |
    ra_snapshot:effect() |
    ra_server:effect().
%% logs can have effects too so that they can be coordinated with other state
%% such as avoiding to delete old snapshots whilst they are still being
%% replicated

-type effects() :: [effect()].

-record(cfg, {uid :: ra_uid(),
              log_id :: unicode:chardata(),
              directory :: file:filename(),
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
         % if this is set a snapshot write is in progress for the
         % index specified
         cache = ra_log_cache:init() :: ra_log_cache:state(),
         last_resend_time :: option({integer(), WalPid :: pid() | undefined}),
         last_wal_write :: {pid(), Ms :: integer()},
         reader :: ra_log_reader:state(),
         readers = [] :: [pid()]
        }).


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
      first_index := ra_index(),
      last_written_index_term := ra_idxterm(),
      num_segments := non_neg_integer(),
      open_segments => non_neg_integer(),
      snapshot_index => undefined | ra_index(),
      cache_size => non_neg_integer(),
      latest_checkpoint_index => undefined | ra_index(),
      atom() => term()}.

-export_type([state/0,
              ra_log_init_args/0,
              ra_meta_key/0,
              segment_ref/0,
              event/0,
              event_body/0,
              effect/0,
              overview/0
             ]).

pre_init(#{uid := UId,
           system_config := #{data_dir := DataDir}} = Conf) ->
    Dir = server_data_dir(DataDir, UId),
    SnapModule = maps:get(snapshot_module, Conf, ?DEFAULT_SNAPSHOT_MODULE),
    MaxCheckpoints = maps:get(max_checkpoints, Conf, ?DEFAULT_MAX_CHECKPOINTS),
    SnapshotsDir = filename:join(Dir, "snapshots"),
    CheckpointsDir = filename:join(Dir, "checkpoints"),
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
    SnapshotsDir = filename:join(Dir, "snapshots"),
    CheckpointsDir = filename:join(Dir, "checkpoints"),
    Counter = maps:get(counter, Conf, undefined),

    %% ensure directories are there
    ok = ra_lib:make_dir(Dir),
    ok = ra_lib:make_dir(SnapshotsDir),
    ok = ra_lib:make_dir(CheckpointsDir),
    % initialise metrics for this server
    true = ets:insert(ra_log_metrics, {UId, 0, 0, 0, 0}),
    SnapshotState = ra_snapshot:init(UId, SnapModule, SnapshotsDir,
                                     CheckpointsDir, Counter, MaxCheckpoints),
    {SnapIdx, SnapTerm} = case ra_snapshot:current(SnapshotState) of
                              undefined -> {-1, -1};
                              Curr -> Curr
                          end,

    AccessPattern = maps:get(initial_access_pattern, Conf, random),
    Reader0 = ra_log_reader:init(UId, Dir, 0, MaxOpen, AccessPattern, [],
                                 Names, Counter),
    % recover current range and any references to segments
    % this queries the segment writer and thus blocks until any
    % segments it is currently processed have been finished
    {{FirstIdx, LastIdx0}, SegRefs} = case recover_range(UId, Reader0, SegWriter) of
                                          {undefined, SRs} ->
                                              {{-1, -1}, SRs};
                                          R ->  R
                                      end,
    %% TODO: can there be obsolete segments returned here?
    {Reader1, []} = ra_log_reader:update_first_index(FirstIdx, Reader0),
    Reader = ra_log_reader:update_segments(SegRefs, Reader1),
    % recover last snapshot file
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
    State000 = #?MODULE{cfg = Cfg,
                        first_index = max(SnapIdx + 1, FirstIdx),
                        last_index = max(SnapIdx, LastIdx0),
                        reader = Reader,
                        snapshot_state = SnapshotState,
                        last_wal_write = {whereis(Wal), now_ms()}
                       },
    put_counter(Cfg, ?C_RA_SVR_METRIC_SNAPSHOT_INDEX, SnapIdx),
    LastIdx = State000#?MODULE.last_index,
    put_counter(Cfg, ?C_RA_SVR_METRIC_LAST_INDEX, LastIdx),
    put_counter(Cfg, ?C_RA_SVR_METRIC_LAST_WRITTEN_INDEX, LastIdx),
    case ra_snapshot:latest_checkpoint(SnapshotState) of
        undefined ->
            ok;
        {ChIdx, _ChTerm} ->
            put_counter(Cfg, ?C_RA_SVR_METRIC_CHECKPOINT_INDEX, ChIdx)
    end,

    % recover the last term
    {LastTerm0, State00} = case LastIdx of
                               SnapIdx ->
                                   {SnapTerm, State000};
                               -1 ->
                                   {0, State000};
                               LI ->
                                   fetch_term(LI, State000)
                           end,
    LastTerm = ra_lib:default(LastTerm0, -1),
    State0 = State00#?MODULE{last_term = LastTerm,
                             last_written_index_term = {LastIdx, LastTerm}},

    % initialized with a default 0 index 0 term dummy value
    % and an empty meta data map
    State = maybe_append_first_entry(State0),
    ?DEBUG("~ts: ra_log:init recovered last_index_term ~w"
           " first index ~b",
           [State#?MODULE.cfg#cfg.log_id,
            last_index_term(State),
            State#?MODULE.first_index]),
    element(1, delete_segments(SnapIdx, State)).

-spec close(state()) -> ok.
close(#?MODULE{cfg = #cfg{uid = UId},
               reader = Reader}) ->
    % deliberately ignoring return value
    % close all open segments
    _ = ra_log_reader:close(Reader),
    %% delete ra_log_metrics record
    catch ets:delete(ra_log_metrics, UId),
    %% inserted in ra_snapshot but it doesn't have a terminate callback so
    %% deleting ets table here
    catch ets:delete(ra_log_snapshot_state, UId),
    ok.

-spec append(Entry :: log_entry(), State :: state()) ->
    state() | no_return().
append({Idx, _, _Cmd} = Entry,
       #?MODULE{last_index = LastIdx,
                snapshot_state = SnapState} = State0)
      when Idx =:= LastIdx + 1 ->
    case ra_snapshot:current(SnapState) of
        {SnapIdx, _} when Idx =:= SnapIdx + 1 ->
            % it is the next entry after a snapshot
            % we need to tell the wal to truncate as we
            % are not going to receive any entries prior to the snapshot
            wal_truncate_write(State0, Entry);
        _ ->
            wal_write(State0, Entry)
    end;
append({Idx, _, _}, #?MODULE{last_index = LastIdx}) ->
    Msg = lists:flatten(io_lib:format("tried writing ~b - expected ~b",
                                      [Idx, LastIdx+1])),
    exit({integrity_error, Msg}).

-spec write(Entries :: [log_entry()], State :: state()) ->
    {ok, state()} |
    {error, {integrity_error, term()} | wal_down}.
write([{FstIdx, _, _} = First | Rest] = Entries,
      #?MODULE{last_index = LastIdx,
               snapshot_state = SnapState} = State00)
  when FstIdx =< LastIdx + 1 andalso FstIdx >= 0 ->
    case ra_snapshot:current(SnapState) of
        {SnapIdx, _} when FstIdx =:= SnapIdx + 1 ->
            % it is the next entry after a snapshot
            % we need to tell the wal to truncate as we
            % are not going to receive any entries prior to the snapshot
            try wal_truncate_write(State00, First) of
                State0 ->
                    % write the rest normally
                    write_entries(Rest, State0)
            catch error:wal_down ->
                      {error, wal_down}
            end;
        _ ->
            write_entries(Entries, State00)
    end;
write([], State) ->
    {ok, State};
write([{Idx, _, _} | _], #?MODULE{cfg = #cfg{uid = UId},
                                  last_index = LastIdx}) ->
    Msg = lists:flatten(io_lib:format("~p: ra_log:write/2 "
                                      "tried writing ~b - expected ~b",
                                      [UId, Idx, LastIdx+1])),
    {error, {integrity_error, Msg}}.

-spec fold(FromIdx :: ra_index(), ToIdx :: ra_index(),
           fun((log_entry(), Acc) -> Acc), Acc, state()) ->
    {Acc, state()} when Acc :: term().
fold(From0, To0, Fun, Acc0,
     #?MODULE{cfg = Cfg,
              cache = Cache,
              first_index = FirstIdx,
              last_index = LastIdx,
              reader = Reader0} = State)
  when To0 >= From0 andalso
       To0 >= FirstIdx ->
    From = max(From0, FirstIdx),
    To = min(To0, LastIdx),
    ok = incr_counter(Cfg, ?C_RA_LOG_READ_OPS, 1),

    CacheOverlap = case ra_log_cache:range(Cache) of
                       {CacheFrom, CacheTo} ->
                           ra_log_reader:range_overlap(From, To,
                                                       CacheFrom, CacheTo);
                       _ ->
                           {undefined, From, To}
                   end,
    case CacheOverlap of
        {undefined, F, T} ->
            {Reader, Acc} = ra_log_reader:fold(F, T, Fun, Acc0, Reader0),
            {Acc, State#?MODULE{reader = Reader}};
        {CF, CT, F, T} ->
            {Reader, Acc1} = ra_log_reader:fold(F, T, Fun, Acc0, Reader0),
            Acc = ra_log_cache:fold(CF, CT, Fun, Acc1, Cache),
            NumRead = CT - CF + 1,
            ok = incr_counter(Cfg, ?C_RA_LOG_READ_CACHE, NumRead),
            {Acc, State#?MODULE{reader = Reader}}
    end;
fold(_From, _To, _Fun, Acc, State) ->
    {Acc, State}.

%% read a list of indexes,
%% found indexes be returned in the same order as the input list of indexes
-spec sparse_read([ra_index()], state()) ->
    {[log_entry()], state()}.
sparse_read(Indexes0, #?MODULE{cfg = Cfg,
                               reader = Reader0,
                               last_index = LastIdx,
                               cache = Cache} = State) ->
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
    {Entries0, CacheNumRead, Indexes} = ra_log_cache:get_items(Indexes2, Cache),
    ok = incr_counter(Cfg, ?C_RA_LOG_READ_CACHE, CacheNumRead),
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
                             cache = Cache0,
                             last_written_index_term = {LWIdx0, _}} = State0) ->
    case fetch_term(Idx, State0) of
        {undefined, State} ->
            {not_found, State};
        {Term, State1} ->
            LWIdx = min(Idx, LWIdx0),
            {LWTerm, State2} = fetch_term(LWIdx, State1),
            %% this should always be found but still assert just in case
            true = LWTerm =/= undefined,
            Cache = ra_log_cache:set_last(Idx, Cache0),
            put_counter(Cfg, ?C_RA_SVR_METRIC_LAST_INDEX, Idx),
            put_counter(Cfg, ?C_RA_SVR_METRIC_LAST_WRITTEN_INDEX, LWIdx),
            {ok, State2#?MODULE{last_index = Idx,
                                last_term = Term,
                                cache = Cache,
                                last_written_index_term = {LWIdx, LWTerm}}}
    end.

%% this function forces both last_index and last_written_index_term to
%% the last know index to be written to the wal.
%% This is only used after the wal has been detected down
%% to try to avoid ever having to resend data to the wal
-spec reset_to_last_known_written(state()) -> state().
reset_to_last_known_written(#?MODULE{cfg = Cfg,
                                     cache = Cache0,
                                     last_index = LastIdx,
                                     last_written_index_term = LW} = State0) ->
    {Idx, Term, State} = last_index_term_in_wal(LastIdx, State0),
    ?DEBUG("~ts ~s: index: ~b term: ~b: previous ~w",
           [Cfg#cfg.log_id, ?FUNCTION_NAME, Idx, Term, LW]),
    Cache = ra_log_cache:set_last(Idx, Cache0),
    put_counter(Cfg, ?C_RA_SVR_METRIC_LAST_INDEX, Idx),
    put_counter(Cfg, ?C_RA_SVR_METRIC_LAST_WRITTEN_INDEX, Idx),
    State#?MODULE{last_index = Idx,
                  last_term = Term,
                  cache = Cache,
                  last_written_index_term = {Idx, Term}}.

-spec handle_event(event_body(), state()) ->
    {state(), [effect()]}.
handle_event({written, {FromIdx, _ToIdx, _Term}},
             #?MODULE{last_index = LastIdx} = State)
  when FromIdx > LastIdx ->
    %% we must have reverted back, either by explicit reset or by a snapshot
    %% installation taking place whilst the WAL was processing the write
    %% Just drop the event in this case as it is stale
    {State, []};
handle_event({written, {FromIdx, ToIdx0, Term}},
             #?MODULE{cfg = Cfg,
                      last_written_index_term = {LastWrittenIdx0,
                                                 LastWrittenTerm0},
                      last_index = LastIdx,
                      snapshot_state = SnapState} = State0)
  when FromIdx =< LastWrittenIdx0 + 1 ->
    MaybeCurrentSnap = ra_snapshot:current(SnapState),
    % We need to ignore any written events for the same index
    % but in a prior term if we do not we may end up confirming
    % to a leader writes that have not yet
    % been fully flushed
    %
    % last written cannot even go larger than last_index
    ToIdx = min(ToIdx0, LastIdx),
    case fetch_term(ToIdx, State0) of
        {Term, State} when is_integer(Term) ->
            ok = put_counter(Cfg, ?C_RA_SVR_METRIC_LAST_WRITTEN_INDEX, ToIdx),
            {State#?MODULE{last_written_index_term = {ToIdx, Term}},
             %% delaying truncate_cache until the next event allows any entries
             %% that became committed to be read from cache rather than ETS
             [{next_event, {ra_log_event, {truncate_cache, FromIdx, ToIdx}}}]};
        {undefined, State} when FromIdx =< element(1, MaybeCurrentSnap) ->
            % A snapshot happened before the written event came in
            % This can only happen on a leader when consensus is achieved by
            % followers returning appending the entry and the leader committing
            % and processing a snapshot before the written event comes in.
            % ensure last_written_index_term does not go backwards
            LastWrittenIdx = max(LastWrittenIdx0, ToIdx),
            LastWrittenIdxTerm = {LastWrittenIdx,
                                  max(LastWrittenTerm0, Term)},
            ok = put_counter(Cfg, ?C_RA_SVR_METRIC_LAST_WRITTEN_INDEX, LastWrittenIdx),
            {State#?MODULE{last_written_index_term = LastWrittenIdxTerm},
             [{next_event, {ra_log_event, {truncate_cache, FromIdx, ToIdx}}}]};
        {OtherTerm, State} ->
            case FromIdx < ToIdx  of
                false ->
                    ?DEBUG("~ts: written event did not find term ~b for index ~b "
                           "found ~w",
                           [State#?MODULE.cfg#cfg.log_id, Term, ToIdx, OtherTerm]),
                    % if the term doesn't match we just ignore it
                    {State, []};
                true ->
                    handle_event({written, {FromIdx, ToIdx -1, Term}}, State0)
            end
    end;
handle_event({written, {FromIdx, _, _Term}},
             #?MODULE{cfg = #cfg{log_id = LogId},
                      last_written_index_term = {LastWrittenIdx, _}} = State)
  when FromIdx > LastWrittenIdx + 1 ->
    % leaving a gap is not ok - may need to resend from cache
    Expected = LastWrittenIdx + 1,
    ?INFO("~ts: ra_log: written gap detected at ~b expected ~b!",
          [LogId, FromIdx, Expected]),
    {resend_from(Expected, State), []};
handle_event({truncate_cache, FromIdx, ToIdx}, State) ->
    truncate_cache(FromIdx, ToIdx, State, []);
handle_event(flush_cache, State) ->
    {flush_cache(State), []};
handle_event({segments, Tid, NewSegs},
             #?MODULE{cfg = #cfg{names = Names},
                      reader = Reader0,
                      readers = Readers} = State0) ->
    ClosedTables = ra_log_reader:closed_mem_tables(Reader0),
    Active = lists:takewhile(fun ({_, _, _, _, T}) -> T =/= Tid end,
                             ClosedTables),
    Reader = ra_log_reader:update_segments(NewSegs, Reader0),
    State = State0#?MODULE{reader = Reader},
    % not fast but we rarely should have more than one or two closed tables
    % at any time
    Obsolete = ClosedTables -- Active,

    DeleteFun =
    fun () ->
            TidsToDelete =
            [begin
                 %% first delete the entry in the
                 %% closed table lookup
                 true = ra_log_reader:delete_closed_mem_table_object(Reader, ClosedTbl),
                 T
             end || {_, _, _, _, T} = ClosedTbl <- Obsolete],
            ok = ra_log_ets:delete_tables(Names, TidsToDelete)
    end,

    case Readers of
        [] ->
            %% delete immediately
            DeleteFun(),
            {State, []};
        _ ->
            %% HACK
            %% TODO: replace with reader coordination
            %% delay deletion until all readers confirmed they have received
            %% the update
            Pid = spawn(fun () ->
                                ok = log_update_wait_n(length(Readers)),
                                DeleteFun()
                        end),
            {State, log_update_effects(Readers, Pid, State)}
    end;
handle_event({snapshot_written, {SnapIdx, _} = Snap, SnapKind},
             #?MODULE{cfg = Cfg,
                      first_index = FstIdx,
                      last_index = LstIdx,
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
                        {Snap,
                         [{next_event,
                           {ra_log_event,
                            {truncate_cache, LastWrittenIdx, SnapIdx}}}
                          | Effects1]}
                end,

            {State#?MODULE{first_index = SnapIdx + 1,
                           last_index = max(LstIdx, SnapIdx),
                           last_written_index_term = LWIdxTerm,
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
handle_event({resend_write, Idx}, State) ->
    % resend missing entries from cache.
    % The assumption is they are available in the cache
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
fetch_term(Idx, #?MODULE{cache = Cache, reader = Reader0} = State0) ->
    case ra_log_cache:fetch_term(Idx, Cache, undefined) of
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
install_snapshot({SnapIdx, _} = IdxTerm, SnapState0,
                 #?MODULE{cfg = Cfg,
                          cache = Cache} = State0) ->
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
    {State#?MODULE{snapshot_state = SnapState,
                   first_index = SnapIdx + 1,
                   last_index = SnapIdx,
                   %% TODO: update last_term too?
                   %% cache can be reset
                   cache = ra_log_cache:reset(Cache),
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

-spec flush_cache(state()) -> state().
flush_cache(#?MODULE{cache = Cache} = State) ->
    State#?MODULE{cache = ra_log_cache:flush(Cache)}.

-spec needs_cache_flush(state()) -> boolean().
needs_cache_flush(#?MODULE{cache = Cache}) ->
    ra_log_cache:needs_flush(Cache).

-spec tick(Now :: integer(), state()) -> state().
tick(Now, #?MODULE{cfg = #cfg{wal = Wal},
                   cache = Cache,
                   last_written_index_term = {LastWrittenIdx, _},
                   last_wal_write = {WalPid, Ms}} = State) ->
    CurWalPid = whereis(Wal),
    case Now > Ms + ?WAL_RESEND_TIMEOUT andalso
         CurWalPid =/= undefined andalso
         CurWalPid  =/= WalPid andalso
         ra_log_cache:size(Cache) > 0 of
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
    CanFreeSegments = lists:any(fun({_, To, _}) -> To =< Idx end,
                                ra_log_reader:segment_refs(Reader)),
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
                  first_index = FirstIndex,
                  last_written_index_term = LWIT,
                  snapshot_state = SnapshotState,
                  reader = Reader,
                  last_wal_write = {_LastPid, LastMs},
                  cache = Cache}) ->
    #{type => ?MODULE,
      last_index => LastIndex,
      first_index => FirstIndex,
      last_written_index_term => LWIT,
      num_segments => length(ra_log_reader:segment_refs(Reader)),
      open_segments => ra_log_reader:num_open_segments(Reader),
      snapshot_index => case ra_snapshot:current(SnapshotState) of
                            undefined -> undefined;
                            {I, _} -> I
                        end,
      latest_checkpoint_index =>
          case ra_snapshot:latest_checkpoint(SnapshotState) of
              undefined -> undefined;
              {I, _} -> I
          end,
      cache_size => ra_log_cache:size(Cache),
      cache_range => ra_log_cache:range(Cache),
      last_wal_write => LastMs
     }.

-spec write_config(ra_server:config(), state()) -> ok.
write_config(Config0, #?MODULE{cfg = #cfg{directory = Dir}}) ->
    ConfigPath = filename:join(Dir, "config"),
    TmpConfigPath = filename:join(Dir, "config.tmp"),
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

-spec read_config(state() | file:filename()) ->
    {ok, ra_server:config()} | {error, term()}.
read_config(#?MODULE{cfg = #cfg{directory = Dir}}) ->
    read_config(Dir);
read_config(Dir) ->
    ConfigPath = filename:join(Dir, "config"),
    ra_lib:consult(ConfigPath).

-spec delete_everything(state()) -> ok.
delete_everything(#?MODULE{cfg = #cfg{directory = Dir},
                           snapshot_state = SnapState} = Log) ->
    _ = close(Log),
    %% if there is a snapshot process pending it could cause the directory
    %% deletion to fail, best kill the snapshot process first
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
                  "directory ~ts~n Error: ~p", [Dir, Err])
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
                           first_index = FstIdx,
                           reader = Reader} = State) ->
    ActiveSegs = ra_log_reader:segment_refs(Reader),
    % close all open segments
    % deliberately ignoring return value
    _ = ra_log_reader:close(Reader),
    %% open a new segment with the new max open segment value
    State#?MODULE{reader = ra_log_reader:init(UId, Dir, FstIdx, MaxOpenSegments,
                                              AccessPattern,
                                              ActiveSegs, Names, Counter)}.

-spec register_reader(pid(), state()) ->
    {state(), effects()}.
register_reader(Pid, #?MODULE{cfg = #cfg{uid = UId,
                                         directory = Dir,
                                         names = Names},
                              first_index = Idx,
                              reader = Reader,
                              readers = Readers} = State) ->
    SegRefs = ra_log_reader:segment_refs(Reader),
    NewReader = ra_log_reader:init(UId, Dir, Idx, 1, SegRefs, Names),
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
                                             uid = UId},
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
            Active = ra_log_reader:segment_refs(Reader),
            ?DEBUG("~ts: ~b obsolete segments at ~b - remaining: ~b, pivot ~0p",
                   [LogId, length(Obsolete), SnapIdx, length(Active), Pivot]),
            State = State0#?MODULE{reader = Reader},
            {State, log_update_effects(Readers, Pid, State)}
    end.

wal_truncate_write(#?MODULE{cfg = #cfg{uid = UId,
                                       wal = Wal} = Cfg,
                            cache = Cache} = State,
                   {Idx, Term, Cmd} = Entry) ->
    % this is the next write after a snapshot was taken or received
    % we need to indicate to the WAL that this may be a non-contiguous write
    % and that prior entries should be considered stale
    case ra_log_wal:truncate_write({UId, self()}, Wal, Idx, Term, Cmd) of
        {ok, Pid} ->
            ok = incr_counter(Cfg, ?C_RA_LOG_WRITE_OPS, 1),
            put_counter(Cfg, ?C_RA_SVR_METRIC_LAST_INDEX, Idx),
            State#?MODULE{last_index = Idx, last_term = Term,
                          last_wal_write = {Pid, now_ms()},
                          cache = ra_log_cache:add(Entry, Cache)};
        {error, wal_down} ->
            error(wal_down)
    end.

wal_write(#?MODULE{cfg = #cfg{uid = UId,
                              wal = Wal} = Cfg,
                   cache = Cache} = State,
          {Idx, Term, Cmd} = Entry) ->
    case ra_log_wal:write({UId, self()}, Wal, Idx, Term, Cmd) of
        {ok, Pid} ->
            ok = incr_counter(Cfg, ?C_RA_LOG_WRITE_OPS, 1),
            put_counter(Cfg, ?C_RA_SVR_METRIC_LAST_INDEX, Idx),
            State#?MODULE{last_index = Idx, last_term = Term,
                          last_wal_write = {Pid, now_ms()},
                          cache = ra_log_cache:add(Entry, Cache)};
        {error, wal_down} ->
            error(wal_down)
    end.

%% unly used by resend to wal functionality and doesn't set the cache as it
%% is already set
wal_rewrite(#?MODULE{cfg = #cfg{uid = UId,
                                wal = Wal} = Cfg} = State,
            {Idx, Term, Cmd}) ->
    case ra_log_wal:write({UId, self()}, Wal, Idx, Term, Cmd) of
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
                         cache = Cache0} = State,
                Entries) ->
    WriterId = {UId, self()},
    {WalCommands, Num, Cache} =
        lists:foldl(fun ({Idx, Term, Cmd} = Entry, {WC, N, C0}) ->
                            WalC = {append, WriterId, Idx, Term, Cmd},
                            C = ra_log_cache:add(Entry, C0),
                            {[WalC | WC], N+1, C}
                    end, {[], 0, Cache0}, Entries),

    [{_, _, LastIdx, LastTerm, _} | _] = WalCommands,
    case ra_log_wal:write_batch(Wal, lists:reverse(WalCommands)) of
        {ok, Pid} ->
            ok = incr_counter(Cfg, ?C_RA_LOG_WRITE_OPS, Num),
            put_counter(Cfg, ?C_RA_SVR_METRIC_LAST_INDEX, LastIdx),
            State#?MODULE{last_index = LastIdx,
                          last_term = LastTerm,
                          last_wal_write = {Pid, now_ms()},
                          cache = Cache};
        {error, wal_down} ->
            error(wal_down)
    end.

truncate_cache(_FromIdx, ToIdx,
               #?MODULE{cache = Cache} = State,
               Effects) ->
    CacheAfter = ra_log_cache:trim(ToIdx, Cache),
    {State#?MODULE{cache = CacheAfter}, Effects}.

maybe_append_first_entry(State0 = #?MODULE{last_index = -1}) ->
    State = append({0, 0, undefined}, State0),
    receive
        {ra_log_event, {written, {0, 0, 0}}} -> ok
    end,
    State#?MODULE{first_index = 0,
                  cache = ra_log_cache:reset(State#?MODULE.cache),
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
                           cache = Cache} = State) ->
    ?DEBUG("~ts: ra_log: resending from ~b to ~b",
           [State#?MODULE.cfg#cfg.log_id, Idx, LastIdx]),
    ok = incr_counter(Cfg, ?C_RA_LOG_WRITE_RESENDS, LastIdx - Idx + 1),
    lists:foldl(fun (I, Acc) ->
                        {I, T, C} = ra_log_cache:fetch(I, Cache),
                        wal_rewrite(Acc, {I, T, C})
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

verify_entries(_, []) ->
    ok;
verify_entries(Idx, [{NextIdx, _, _} | Tail]) when Idx + 1 == NextIdx ->
    verify_entries(NextIdx, Tail);
verify_entries(Idx, Tail) ->
    Msg = io_lib:format("ra_log:verify_entries/2 "
                        "tried writing ~p - expected ~b",
                        [Tail, Idx+1]),
    {error, {integrity_error, lists:flatten(Msg)}}.

write_entries([], State) ->
    {ok, State};
write_entries([{FstIdx, _, _} | Rest] = Entries, State0) ->
    %% TODO: verify and build up wal commands in one iteration
    case verify_entries(FstIdx, Rest) of
        ok ->
            try
                {ok, wal_write_batch(State0, Entries)}
            catch
                error:wal_down ->
                    {error, wal_down}
            end;
        Error ->
            Error
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

recover_range(UId, Reader, SegWriter) ->
    % 0. check open mem_tables (this assumes wal has finished recovering
    % which means it is essential that ra_servers are part of the same
    % supervision tree
    % 1. check closed mem_tables to extend
    OpenRanges = case ra_log_reader:open_mem_table_lookup(Reader) of
                     [] ->
                         [];
                     [{UId, First, Last, _}] ->
                         [{First, Last}]
                 end,
    ClosedRanges = [{F, L} || {_, _, F, L, _} <- ra_log_reader:closed_mem_tables(Reader)],
    % 2. check segments
    SegFiles = ra_log_segment_writer:my_segments(SegWriter, UId),
    SegRefs = lists:foldl(
                fun (S, Acc) ->
                        {ok, Seg} = ra_log_segment:open(S, #{mode => read}),
                        %% if a server recovered when a segment had been opened
                        %% but never had any entries written the segref would be
                        %% undefined
                        case ra_log_segment:segref(Seg) of
                            undefined ->
                                ok = ra_log_segment:close(Seg),
                                Acc;
                            SegRef ->
                                ok = ra_log_segment:close(Seg),
                                [SegRef | Acc]
                        end
                end, [], SegFiles),
    SegRanges = [{F, L} || {F, L, _} <- SegRefs],
    Ranges = OpenRanges ++ ClosedRanges ++ SegRanges,
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
    receive
        {ra_log_event, {written, {_, Idx, Term}} = Evt} ->
            {Log, _} = handle_event(Evt, Log0),
            Log;
        {ra_log_event, {written, _} = Evt} ->
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
    Me = ra_lib:to_list(UId),
    filename:join(Dir, Me).

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

last_index_term_in_wal(Idx, #?MODULE{last_written_index_term = {Idx, Term}} = State) ->
    % we reached the lower limit which is the last known written index
    {Idx, Term, State};
last_index_term_in_wal(Idx, #?MODULE{reader = Reader0} = State) ->
    case ra_log_reader:fetch_term(Idx, Reader0) of
        {undefined, Reader} ->
            last_index_term_in_wal(Idx-1, State#?MODULE{reader = Reader});
        {Term, Reader} ->
            %% if it can be read when bypassing the local cache it is in the
            %% wal
            {Idx, Term, State#?MODULE{reader = Reader}}
    end.

now_ms() ->
    erlang:system_time(millisecond).

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
