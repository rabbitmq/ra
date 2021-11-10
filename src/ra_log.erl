%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2017-2021 VMware, Inc. or its affiliates.  All rights reserved.
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
         take/3,
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
         readers/1
        ]).

-include("ra.hrl").

-define(DEFAULT_RESEND_WINDOW_SEC, 20).
-define(SNAPSHOT_INTERVAL, 4096).
-define(LOG_APPEND_TIMEOUT, 5000).

-type ra_meta_key() :: atom().
-type segment_ref() :: {From :: ra_index(), To :: ra_index(),
                        File :: string()}.
-type event_body() :: {written, {From :: ra_index(),
                                 To :: ra_index(),
                                 ToTerm :: ra_term()}} |
                      {segments, ets:tid(), [segment_ref()]} |
                      {resend_write, ra_index()} |
                      {snapshot_written, ra_idxterm()} |
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
              snapshot_interval = ?SNAPSHOT_INTERVAL :: non_neg_integer(),
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
         cache = #{} :: #{ra_index() => {ra_index(), ra_term(), log_entry()}},
         last_resend_time :: maybe(integer()),
         reader :: ra_log_reader:state(),
         readers = [] :: [pid()]
        }).


-opaque state() :: #?MODULE{}.

-type ra_log_init_args() :: #{uid := ra_uid(),
                              system_config => ra_system:config(),
                              log_id => unicode:chardata(),
                              snapshot_interval => non_neg_integer(),
                              resend_window => integer(),
                              max_open_segments => non_neg_integer(),
                              snapshot_module => module(),
                              counter => counters:counters_ref(),
                              initial_access_pattern => sequential | random}.

-export_type([state/0,
              ra_log_init_args/0,
              ra_meta_key/0,
              segment_ref/0,
              event/0,
              event_body/0,
              effect/0
             ]).

pre_init(#{uid := UId,
           system_config := #{data_dir := DataDir}
          } = Conf) ->
    Dir = server_data_dir(DataDir, UId),
    SnapModule = maps:get(snapshot_module, Conf, ?DEFAULT_SNAPSHOT_MODULE),
    SnapshotsDir = filename:join(Dir, "snapshots"),
    _ = ra_snapshot:init(UId, SnapModule, SnapshotsDir),
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
    SnapInterval = maps:get(snapshot_interval, Conf, ?SNAPSHOT_INTERVAL),
    SnapshotsDir = filename:join(Dir, "snapshots"),
    Counter = maps:get(counter, Conf, undefined),

    %% ensure directories are there
    ok = ra_lib:make_dir(Dir),
    ok = ra_lib:make_dir(SnapshotsDir),
    % initialise metrics for this server
    true = ets:insert(ra_log_metrics, {UId, 0, 0, 0, 0}),
    SnapshotState = ra_snapshot:init(UId, SnapModule, SnapshotsDir),
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
               snapshot_interval = SnapInterval,
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
                        snapshot_state = SnapshotState
                       },

    LastIdx = State000#?MODULE.last_index,
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
    ?DEBUG("~s: ra_log:init recovered last_index_term ~w"
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
    %% inserted in ra_snapshot but it doesn't havea terminate callback so
    %% deleting ets table here
    catch ets:delete(ra_log_snapshot_state, UId),
    ok.

-spec append(Entry :: log_entry(), State :: state()) ->
    state() | no_return().
append({Idx, _, _} = Entry,
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
            State0 = wal_truncate_write(State00, First),
            % write the rest normally
            % TODO: batch api for wal
            write_entries(Rest, State0);
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

-spec take(ra_index(), non_neg_integer(), state()) ->
    {[log_entry()], NumRead :: non_neg_integer(), state()}.
take(Start, Num, #?MODULE{cfg = Cfg,
                          first_index = FirstIdx,
                          reader = Reader0,
                          last_index = LastIdx} = State)
  when Start >= FirstIdx andalso Start =< LastIdx andalso Num > 0 ->
    ok = incr_counter(Cfg, ?C_RA_LOG_READ_OPS, 1),
    % 0. Check that the request isn't outside of first_index and last_index
    % 1. Check the local cache for any unflushed entries, carry remainders
    % 2. Check ra_log_open_mem_tables
    % 3. Check ra_log_closed_mem_tables in turn
    % 4. Check on disk segments in turn
    case cache_take(Start, Num, State) of
        {Entries, C0, undefined} ->
            {Entries, C0, State};
        {Entries0, C0, {S, F}} ->
            {Entries, C1, Reader} = ra_log_reader:read(S, F, Reader0, Entries0),
            {Entries, C0 + C1, State#?MODULE{reader = Reader}}
    end;
take(_, _, State) ->
    {[], 0, State}.

-spec last_index_term(state()) -> ra_idxterm().
last_index_term(#?MODULE{last_index = LastIdx, last_term = LastTerm}) ->
    {LastIdx, LastTerm}.

-spec last_written(state()) -> ra_idxterm().
last_written(#?MODULE{last_written_index_term = LWTI}) ->
    LWTI.

%% forces the last index and last written index back to a prior index
-spec set_last_index(ra_index(), state()) ->
    {ok, state()} | {not_found, state()}.
set_last_index(Idx, #?MODULE{last_written_index_term = {LWIdx0, _}} = State0) ->
    case fetch_term(Idx, State0) of
        {undefined, State} ->
            {not_found, State};
        {Term, State1} ->
            LWIdx = min(Idx, LWIdx0),
            {LWTerm, State2} = fetch_term(LWIdx, State1),
            %% this should always be found but still assert just in case
            true = LWTerm =/= undefined,
            {ok, State2#?MODULE{last_index = Idx,
                                last_term = Term,
                                last_written_index_term = {LWIdx, LWTerm}}}
    end.

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
             #?MODULE{last_written_index_term = {LastWrittenIdx0,
                                                 LastWrittenTerm0},
                      last_index = LastIdx,
                      snapshot_state = SnapState} = State0)
  when FromIdx =< LastWrittenIdx0 + 1 ->
    MaybeCurrent = ra_snapshot:current(SnapState),
    % We need to ignore any written events for the same index
    % but in a prior term if we do not we may end up confirming
    % to a leader writes that have not yet
    % been fully flushed
    %
    % last written cannot even go larger than last_index
    ToIdx = min(ToIdx0, LastIdx),
    case fetch_term(ToIdx, State0) of
        {Term, State} when is_integer(Term) ->
            % this case truncation shouldn't be too expensive as the cache
            % only contains the unflushed window of entries typically less than
            % 10ms worth of entries
            truncate_cache(FromIdx, ToIdx,
                           State#?MODULE{last_written_index_term = {ToIdx, Term}},
                           []);
        {undefined, State} when FromIdx =< element(1, MaybeCurrent) ->
            % A snapshot happened before the written event came in
            % This can only happen on a leader when consensus is achieved by
            % followers returning appending the entry and the leader committing
            % and processing a snapshot before the written event comes in.
            % ensure last_written_index_term does not go backwards
            LastWrittenIdxTerm = {max(LastWrittenIdx0, ToIdx),
                                  max(LastWrittenTerm0, Term)},
            {State#?MODULE{last_written_index_term = LastWrittenIdxTerm}, []};
        {OtherTerm, State} ->
            ?DEBUG("~s: written event did not find term ~b for index ~b "
                   "found ~w",
                   [State#?MODULE.cfg#cfg.log_id, Term, ToIdx, OtherTerm]),
            {State, []}
    end;
handle_event({written, {FromIdx, _, _}},
             #?MODULE{cfg = #cfg{log_id = LogId},
                      last_written_index_term = {LastWrittenIdx, _}} = State0)
  when FromIdx > LastWrittenIdx + 1 ->
    % leaving a gap is not ok - resend from cache
    Expected = LastWrittenIdx + 1,
    ?DEBUG("~s: ra_log: written gap detected at ~b expected ~b!",
           [LogId, FromIdx, Expected]),
    {resend_from(Expected, State0), []};
handle_event({segments, Tid, NewSegs},
             #?MODULE{cfg = #cfg{names = Names},
                      reader = Reader0,
                      readers = Readers} = State0) ->
    ClosedTables = ra_log_reader:closed_mem_tables(Reader0),
    Active = lists:takewhile(fun ({_, _, _, _, T}) -> T =/= Tid end,
                             ClosedTables),
    Reader = ra_log_reader:update_segments(NewSegs, Reader0),
    % compact seg ref list so that only the latest range for a segment
    % file has an entry
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
handle_event({snapshot_written, {Idx, _} = Snap},
             #?MODULE{snapshot_state = SnapState0} = State0) ->
    % delete any segments outside of first_index
    {State, Effects0} = delete_segments(Idx, State0),
    SnapState = ra_snapshot:complete_snapshot(Snap, SnapState0),
    %% delete old snapshot files
    %% This is done as an effect
    %% so that if an old snapshot is still being replicated
    %% the cleanup can be delayed until it is safe
    Effects = [{delete_snapshot,
                ra_snapshot:directory(SnapState),
                ra_snapshot:current(SnapState0)} | Effects0],
    %% do not set last written index here as the snapshot may
    %% be for a past index
    {State#?MODULE{first_index = Idx + 1,
                   snapshot_state = SnapState}, Effects};
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
    {maybe(log_entry()), state()}.
fetch(Idx, State0) ->
    case take(Idx, 1, State0) of
        {[], _, State} ->
            {undefined, State};
        {[Entry], _, State} ->
            {Entry, State}
    end.

-spec fetch_term(ra_index(), state()) ->
    {maybe(ra_term()), state()}.
fetch_term(Idx, #?MODULE{last_index = LastIdx,
                         first_index = FirstIdx} = State0)
  when Idx < FirstIdx orelse Idx > LastIdx ->
    {undefined, State0};
fetch_term(Idx, #?MODULE{cache = Cache, reader = Reader0} = State0) ->
    case Cache of
        #{Idx := {Idx, Term, _}} ->
            {Term, State0};
        _ ->
            {Term, Reader} = ra_log_reader:fetch_term(Idx, Reader0),
            {Term, State0#?MODULE{reader = Reader}}
    end.

-spec snapshot_state(State :: state()) -> ra_snapshot:state().
snapshot_state(State) ->
    State#?MODULE.?FUNCTION_NAME.

-spec set_snapshot_state(ra_snapshot:state(), state()) -> state().
set_snapshot_state(SnapState, State) ->
    State#?MODULE{snapshot_state = SnapState}.

-spec install_snapshot(ra_idxterm(), ra_snapshot:state(), state()) ->
    {state(), effects()}.
install_snapshot({Idx, _} = IdxTerm, SnapState,
                 #?MODULE{cfg = Cfg} = State0) ->
    ok = incr_counter(Cfg, ?C_RA_LOG_SNAPSHOTS_INSTALLED, 1),
    {State, Effs} = delete_segments(Idx, State0),
    {State#?MODULE{snapshot_state = SnapState,
                   first_index = Idx + 1,
                   last_index = Idx,
                   %% TODO: update last_term too?
                   %% cache can go
                   cache = #{},
                   last_written_index_term = IdxTerm},
     Effs}.

-spec recover_snapshot(State :: state()) ->
    maybe({ra_snapshot:meta(), term()}).
recover_snapshot(#?MODULE{snapshot_state = SnapState}) ->
    case ra_snapshot:recover(SnapState) of
        {ok, Meta, MacState} ->
            {Meta, MacState};
        {error, no_current_snapshot} ->
            undefined
    end.

-spec snapshot_index_term(State :: state()) -> maybe(ra_idxterm()).
snapshot_index_term(#?MODULE{snapshot_state = SS}) ->
    ra_snapshot:current(SS).

-spec update_release_cursor(Idx :: ra_index(), Cluster :: ra_cluster(),
                            MacVersion :: ra_machine:version(),
                            MacState :: term(), State :: state()) ->
    {state(), effects()}.
update_release_cursor(Idx, Cluster, MacVersion, MacState,
                      #?MODULE{snapshot_state = SnapState} = State) ->
    case ra_snapshot:pending(SnapState) of
        undefined ->
            update_release_cursor0(Idx, Cluster, MacVersion, MacState, State);
        _ ->
            % if a snapshot is in progress don't even evaluate
            {State, []}
    end.

update_release_cursor0(Idx, Cluster, MacVersion, MacState,
                       #?MODULE{cfg = #cfg{snapshot_interval = SnapInter},
                                reader = Reader,
                                snapshot_state = SnapState} = State0) ->
    ClusterServerIds = maps:keys(Cluster),
    SnapLimit = case ra_snapshot:current(SnapState) of
                    undefined -> SnapInter;
                    {I, _} -> I + SnapInter
                end,
    Meta = #{index => Idx,
             cluster => ClusterServerIds,
             machine_version => MacVersion},
    % The release cursor index is the last entry _not_ contributing
    % to the current state. I.e. the last entry that can be discarded.
    % Check here if any segments can be release.
    case lists:any(fun({_, To, _}) -> To =< Idx end,
                   ra_log_reader:segment_refs(Reader)) of
        true ->
            % segments can be cleared up
            % take a snapshot at the release_cursor
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
                    exit({term_not_found_for_index, Idx});
                {Term, State} ->
                    write_snapshot(Meta#{term => Term}, MacState, State)
            end;
        false when Idx > SnapLimit ->
            %% periodically take snapshots even if segments cannot be cleared
            %% up
            case fetch_term(Idx, State0) of
                {undefined, State} ->
                    {State, []};
                {Term, State} ->
                    write_snapshot(Meta#{term => Term}, MacState, State)
            end;
        false ->
            {State0, []}
    end.

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

-spec overview(state()) -> map().
overview(#?MODULE{last_index = LastIndex,
                  first_index = FirstIndex,
                  last_written_index_term = LWIT,
                  snapshot_state = SnapshotState,
                  reader = Reader,
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
      cache_size => maps:size(Cache)
     }.

-spec write_config(ra_server:config(), state()) -> ok.
write_config(Config0, #?MODULE{cfg = #cfg{directory = Dir}}) ->
    ConfigPath = filename:join(Dir, "config"),
    % clean config of potentially unserialisable data
    Config = maps:without([parent, counter, has_changed], Config0),
    ok = ra_lib:write_file(ConfigPath,
                           list_to_binary(io_lib:format("~p.", [Config]))),
    ok.

-spec read_config(state() | file:filename()) ->
    {ok, ra_server:config()} | {error, term()}.
read_config(#?MODULE{cfg = #cfg{directory = Dir}}) ->
    read_config(Dir);
read_config(Dir) ->
    ConfigPath = filename:join(Dir, "config"),
    ra_lib:consult(ConfigPath).

-spec delete_everything(state()) -> ok.
delete_everything(#?MODULE{cfg = #cfg{directory = Dir}} = Log) ->
    _ = close(Log),
    try ra_lib:recursive_delete(Dir) of
        ok -> ok
    catch
        _:_ = Err ->
            ?WARN("ra_log:delete_everything/1 failed to delete "
                  "directory ~s~n Error: ~p", [Dir, Err])
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


%% deletes all segments where the last index is lower or equal to
%% the Idx argumement
delete_segments(Idx, #?MODULE{cfg = #cfg{log_id = LogId,
                                         segment_writer = SegWriter,
                                         uid = UId},
                              readers = Readers,
                              reader = Reader0} = State0) ->
    case ra_log_reader:update_first_index(Idx, Reader0) of
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
            ?DEBUG("~s: ~b obsolete segments at ~b - remaining: ~b",
                   [LogId, length(Obsolete), Idx, length(Active)]),
            State = State0#?MODULE{reader = Reader},
            {State, log_update_effects(Readers, Pid, State)}
    end.

wal_truncate_write(#?MODULE{cfg = #cfg{uid = UId,
                                       wal = Wal} = Cfg,
                            cache = Cache} = State,
                   {Idx, Term, Data}) ->
    % this is the next write after a snapshot was taken or received
    % we need to indicate to the WAL that this may be a non-contiguous write
    % and that prior entries should be considered stale
    ok = ra_log_wal:truncate_write({UId, self()}, Wal, Idx, Term, Data),
    ok = incr_counter(Cfg, ?C_RA_LOG_WRITE_OPS, 1),
    State#?MODULE{last_index = Idx, last_term = Term,
                  cache = Cache#{Idx => {Idx, Term, Data}}}.

wal_write(#?MODULE{cfg = #cfg{uid = UId,
                              wal = Wal} = Cfg,
                   cache = Cache} = State,
          {Idx, Term, Data}) ->
    case ra_log_wal:write({UId, self()}, Wal, Idx, Term, Data) of
        ok ->
            ok = incr_counter(Cfg, ?C_RA_LOG_WRITE_OPS, 1),
            State#?MODULE{last_index = Idx, last_term = Term,
                          cache = Cache#{Idx => {Idx, Term, Data}}};
        {error, wal_down} ->
            exit(wal_down)
    end.

wal_write_batch(#?MODULE{cfg = #cfg{uid = UId,
                                    wal = Wal} = Cfg,
                         cache = Cache0} = State,
                Entries) ->
    WriterId = {UId, self()},
    {WalCommands, Num, Cache} =
        lists:foldl(fun ({Idx, Term, Data}, {WC, N, C0}) ->
                            WalC = {append, WriterId, Idx, Term, Data},
                            {[WalC | WC], N+1, C0#{Idx => {Idx, Term, Data}}}
                    end, {[], 0, Cache0}, Entries),

    [{_, _, LastIdx, LastTerm, _} | _] = WalCommands,
    case ra_log_wal:write_batch(Wal, lists:reverse(WalCommands)) of
        ok ->
            ok = incr_counter(Cfg, ?C_RA_LOG_WRITE_OPS, Num),
            State#?MODULE{last_index = LastIdx,
                          last_term = LastTerm,
                          cache = Cache};
        {error, wal_down} ->
            exit(wal_down)
    end.

truncate_cache(FromIdx, ToIdx,
               #?MODULE{cache = Cache0,
                        last_index = LastIdx} = State,
               Effects) ->
    Cache = case ToIdx - FromIdx < LastIdx - ToIdx of
                true ->
                    %% if the range to be deleted is smaller than the
                    %% remaining range truncate the cache by removing entries
                    cache_without(FromIdx, ToIdx, Cache0);
                false ->
                    %% if there are fewer entries left than to be removed
                    %% extract the remaning entries
                    cache_with(ToIdx + 1, LastIdx, Cache0, #{})
            end,
    {State#?MODULE{cache = Cache}, Effects}.

cache_with(FromIdx, ToIdx, _, Cache)
  when FromIdx > ToIdx ->
    Cache;
cache_with(From, To, Source, Cache) ->
    cache_with(From + 1, To, Source, Cache#{From => maps:get(From, Source)}).

cache_without(Idx, Idx, Cache) ->
    maps:remove(Idx, Cache);
cache_without(FromIdx, ToIdx, Cache) ->
    cache_without(FromIdx + 1, ToIdx, maps:remove(FromIdx, Cache)).

cache_take(Start, Num, #?MODULE{cfg = Cfg,
                                cache = Cache,
                                last_index = LastIdx}) ->
    Highest = min(LastIdx, Start + Num - 1),
    % cache needs to be queried in reverse to ensure
    % we can bail out when an item is not found
    case cache_take0(Highest, Start, Cache, []) of
        [] ->
            {[], 0, {Start, Highest}};
        [Last | _] = Entries when element(1, Last) =:= Start ->
            NumRead = Highest - Start + 1,
            ok = incr_counter(Cfg, ?C_RA_LOG_READ_CACHE, NumRead),
            % there is no remainder - everything was in the cache
            {Entries, NumRead, undefined};
        [Last | _] = Entries ->
            LastEntryIdx = element(1, Last),
            NumRead = Highest - LastEntryIdx + 1,
            ok = incr_counter(Cfg, ?C_RA_LOG_READ_CACHE, NumRead),
            {Entries, NumRead, {Start, LastEntryIdx - 1}}
    end.

cache_take0(Next, Last, _Cache, Acc)
  when Next < Last ->
    Acc;
cache_take0(Next, Last, Cache, Acc) ->
    case Cache of
        #{Next := Entry} ->
            cache_take0(Next-1, Last, Cache, [Entry | Acc]);
        _ ->
            Acc
    end.

maybe_append_first_entry(State0 = #?MODULE{last_index = -1}) ->
    State = append({0, 0, undefined}, State0),
    receive
        {ra_log_event, {written, {0, 0, 0}}} -> ok
    end,
    State#?MODULE{first_index = 0, last_written_index_term = {0, 0}};
maybe_append_first_entry(State) ->
    State.

resend_from(Idx, #?MODULE{cfg = #cfg{uid = UId}} = State0) ->
    try resend_from0(Idx, State0) of
        State -> State
    catch
        exit:wal_down ->
            ?WARN("~s: ra_log: resending from ~b failed with wal_down",
                  [UId, Idx]),
            State0
    end.

resend_from0(Idx, #?MODULE{cfg = Cfg,
                           last_index = LastIdx,
                           last_resend_time = undefined,
                           cache = Cache} = State) ->
    ?DEBUG("~s: ra_log: resending from ~b to ~b",
           [State#?MODULE.cfg#cfg.log_id, Idx, LastIdx]),
    ok = incr_counter(Cfg, ?C_RA_LOG_WRITE_RESENDS, LastIdx - Idx + 1),
    lists:foldl(fun (I, Acc) ->
                        wal_write(Acc, maps:get(I, Cache))
                end,
                State#?MODULE{last_resend_time = erlang:system_time(seconds)},
                lists:seq(Idx, LastIdx));
resend_from0(Idx, #?MODULE{last_resend_time = LastResend,
                           cfg = #cfg{resend_window_seconds = ResendWindow}} = State) ->
    case erlang:system_time(seconds) > LastResend + ResendWindow of
        true ->
            % it has been more than a minute since last resend
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
                exit:wal_down ->
                    {error, wal_down}
            end;
        Error ->
            Error
    end.

write_snapshot(Meta, MacRef,
               #?MODULE{cfg = Cfg,
                        snapshot_state = SnapState0} = State) ->
    ok = incr_counter(Cfg, ?C_RA_LOG_SNAPSHOTS_WRITTEN, 1),
    {SnapState, Effects} = ra_snapshot:begin_snapshot(Meta, MacRef, SnapState0),
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


%% TODO: implent synchronous writes using gen_batch_server:call/3
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

server_data_dir(Dir, UId) ->
    Me = ra_lib:to_list(UId),
    filename:join(Dir, Me).
%%%% TESTS

-ifdef(TEST).

cache_take0_test() ->
    Cache = #{1 => {1, 9, a}, 2 => {2, 9, b}, 3 => {3, 9, c}},
    State = #?MODULE{cache = Cache, last_index = 3, first_index = 1},
    % no remainder
    {[{2, 9, b}], 1, undefined} = cache_take(2, 1, State),
    {[{2, 9, b}, {3, 9, c}], 2, undefined} = cache_take(2, 2, State),
    {[{1, 9, a}, {2, 9, b}, {3, 9, c}], 3, undefined} = cache_take(1, 3, State),
    % small remainder
    {[{3, 9, c}], 1, {1, 2}} = cache_take(1, 3, State#?MODULE{cache = #{3 => {3, 9, c}}}),
    {[], 0, {1, 3}} = cache_take(1, 3, State#?MODULE{cache = #{4 => {4, 9, d}}}),
    ok.

pick_range_test() ->
    Ranges1 = [{76, 90}, {50, 75}, {1, 100}],
    {1, 90} = pick_range(Ranges1, undefined),

    Ranges2 = [{76, 110}, {50, 75}, {1, 49}],
    {1, 110} = pick_range(Ranges2, undefined),

    Ranges3 = [{25, 30}, {25, 35}, {1, 50}],
    {1, 30} = pick_range(Ranges3, undefined),
    ok.
-endif.
