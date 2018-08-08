-module(ra_log).

-compile([inline_list_funcs]).

-export([init/1,
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
         install_snapshot/2,
         read_snapshot/1,
         snapshot_index_term/1,
         update_release_cursor/4,
         read_meta/2,
         read_meta/3,
         write_meta/3,
         write_meta/4,
         sync_meta/1,
         can_write/1,
         exists/2,
         overview/1,
         write_config/2,
         read_config/1,
         delete_everything/1,
         release_resources/2
        ]).

-include("ra.hrl").

-define(METRICS_CACHE_POS, 2).
-define(METRICS_OPEN_MEM_TBL_POS, 3).
-define(METRICS_CLOSED_MEM_TBL_POS, 4).
-define(METRICS_SEGMENT_POS, 5).

-define(DEFAULT_RESEND_WINDOW_SEC, 20).
-define(SNAPSHOT_INTERVAL, 4096).
-define(LOG_APPEND_TIMEOUT, 5000).

-type ra_meta_key() :: atom().
-type snapshot() :: ra_log_snapshot:state().
-type segment_ref() :: {From :: ra_index(), To :: ra_index(),
                        File :: string()}.
-type event_body() :: {written, {From :: ra_index(),
                                 To :: ra_index(),
                                 ToTerm :: ra_term()}} |
                      {segments, ets:tid(), [segment_ref()]} |
                      {resend_write, ra_index()} |
                      {snapshot_written,
                       ra_idxterm(),
                       File :: file:filename(),
                       Old :: [file:filename()]}.

-type event() :: {ra_log_event, event_body()}.

-record(state,
        {uid :: ra_uid(),
         first_index = -1 :: ra_index(),
         last_index = -1 :: -1 | ra_index(),
         last_term = 0 :: ra_term(),
         last_written_index_term = {0, 0} :: ra_idxterm(),
         segment_refs = [] :: [segment_ref()],
         open_segments = ra_flru:new(5, fun flru_handler/1) :: ra_flru:state(),
         directory :: file:filename(),
         kv :: ra_log_meta:state(),
         snapshot_state :: maybe({ra_index(), ra_term(),
                                  maybe(file:filename())}),
         snapshot_interval = ?SNAPSHOT_INTERVAL :: non_neg_integer(),
         % if this is set a snapshot write is in progress for the
         % index specified
         snapshot_index_in_progress :: maybe(ra_index()),
         cache = #{} :: #{ra_index() => {ra_term(), log_entry()}},
         wal :: atom(), % registered name
         last_resend_time :: maybe(integer()),
         resend_window_seconds = ?DEFAULT_RESEND_WINDOW_SEC :: integer()
        }).

-type ra_log() :: #state{}.

-type ra_log_init_args() :: #{data_dir => string(),
                              uid := ra_uid(),
                              wal => atom(),
                              snapshot_interval => non_neg_integer(),
                              resend_window => integer(),
                              max_open_segments => non_neg_integer()}.

-export_type([ra_log_init_args/0,
              ra_log/0,
              ra_meta_key/0,
              snapshot/0,
              segment_ref/0,
              event/0,
              event_body/0
             ]).

-spec init(ra_log_init_args()) -> ra_log().
init(#{uid := UId} = Conf) ->
    %% overriding the data_dir is only here for test compatibility
    %% as it needs to match what the segment writer has it makes no real
    %% sense to make it independently configurable
    BaseDir = case Conf of
                  #{data_dir := D} -> D;
                  _ ->
                      ra_env:data_dir()
              end,
    MaxOpen = maps:get(max_open_segments, Conf, 5),
    Dir = filename:join(BaseDir, ra_lib:to_list(UId)),
    Meta = filename:join(Dir, "meta.dat"),
    ok = filelib:ensure_dir(Meta),
    Kv = ra_log_meta:init(Meta),

    % initialise metrics for this node
    true = ets:insert(ra_log_metrics, {UId, 0, 0, 0, 0}),
    Wal = maps:get(wal, Conf, ra_log_wal),
    ResendWindow = maps:get(resend_window, Conf, ?DEFAULT_RESEND_WINDOW_SEC),
    SnapInterval = maps:get(snapshot_interval, Conf, ?SNAPSHOT_INTERVAL),

    % create subdir for log id
    % TODO: safely encode UId for use a directory name

    % recover current range and any references to segments
    {{FirstIdx, LastIdx0}, SegRefs} = case recover_range(UId) of
                                          {undefined, SRs} ->
                                              {{-1, -1}, SRs};
                                          R ->  R
                                      end,
    % recove las snapshot file
    SnapshotState  =
        case lists:sort(filelib:wildcard(filename:join(Dir, "*.snapshot"))) of
            [File | _] ->
                %% TODO provide function that only reads the index and term
                %% of the snapshot file.
                {ok, {SI, ST}} = ra_log_snapshot:read_indexterm(File),
                {SI, ST, File};
            [] ->
                undefined
        end,
    {SnapIdx, SnapTerm} = case SnapshotState of
                              undefined -> {-1, -1};
                              {I, T, _} -> {I, T}
                          end,
    State000 = #state{directory = Dir,
                      uid = UId,
                      first_index = max(SnapIdx + 1, FirstIdx),
                      last_index = max(SnapIdx, LastIdx0),
                      segment_refs = SegRefs,
                      snapshot_state = SnapshotState,
                      snapshot_interval = SnapInterval,
                      kv = Kv,
                      wal = Wal,
                      open_segments = ra_flru:new(MaxOpen, fun flru_handler/1),
                      resend_window_seconds = ResendWindow},

    LastIdx = State000#state.last_index,
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
    State0 = State00#state{last_term = LastTerm,
                           last_written_index_term = {LastIdx, LastTerm}},


    % initialized with a default 0 index 0 term dummy value
    % and an empty meta data map
    State = maybe_append_0_0_entry(State0),
    ?INFO("ra_log:init recovered last_index_term ~w~n",
          [last_index_term(State)]),
    State.

recover_range(UId) ->
    % 0. check open mem_tables (this assumes wal has finished recovering
    % which means it is essential that ra_nodes are part of the same
    % supervision tree
    % 1. check closed mem_tables to extend
    OpenRanges = case ets:lookup(ra_log_open_mem_tables, UId) of
                     [] ->
                         [];
                     [{UId, First, Last, _}] ->
                         [{First, Last}]
                 end,
    ClosedRanges = [{F, L} || {_, _, F, L, _} <- closed_mem_tables(UId)],
    % 2. check segments
    SegFiles = ra_log_segment_writer:my_segments(UId),
    SegRefs =
    [begin
         %% ?INFO("ra_log: recovering ~p~n", [S]),
         {ok, Seg} = ra_log_segment:open(S, #{mode => read}),
         SegRef = ra_log_segment:segref(Seg),
         %% ?INFO("ra_log: recovered ~p ~p~n", [S, {F, L}]),
         ok = ra_log_segment:close(Seg),
         SegRef
     end || S <- lists:reverse(SegFiles)],
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


-spec close(ra_log()) -> ok.
close(#state{kv = Kv, open_segments = OpenSegs}) ->
    % deliberately ignoring return value
    % close all open segments
    _ = ra_flru:evict_all(OpenSegs),
    % close also fsyncs
    _ = ra_log_meta:close(Kv),
    ok.

-spec append(Entry :: log_entry(), State :: ra_log()) ->
    ra_log() | no_return().
append(Entry, #state{last_index = LastIdx} = State0)
      when element(1, Entry) =:= LastIdx + 1 ->
    wal_write(State0, Entry);
append({Idx, _, _}, #state{last_index = LastIdx}) ->
    Msg = lists:flatten(io_lib:format("tried writing ~b - expected ~b",
                                      [Idx, LastIdx+1])),
    exit({integrity_error, Msg}).

-spec write(Entries :: [log_entry()],
            State :: ra_log()) ->
    {ok, ra_log()} |
    {error, {integrity_error, term()} | wal_down}.
write([{FstIdx, _, _} = First | Rest] = Entries,
      #state{last_index = LastIdx,
             snapshot_state = SnapState} = State00)
  when FstIdx =< LastIdx + 1 andalso FstIdx >= 0 ->
    case SnapState of
        {SnapIdx, _, _} when FstIdx =:= SnapIdx + 1 ->
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
write([{Idx, _, _} | _], #state{uid = UId, last_index = LastIdx}) ->
    Msg = lists:flatten(io_lib:format("~p: ra_log:write/2 "
                                      "tried writing ~b - expected ~b",
                                      [UId, Idx, LastIdx+1])),
    {error, {integrity_error, Msg}}.

-spec take(ra_index(), non_neg_integer(), ra_log()) ->
    {[log_entry()], ra_log()}.
take(Start, Num, #state{uid = UId, first_index = FirstIdx,
                        last_index = LastIdx} = State)
  when Start >= FirstIdx andalso Start =< LastIdx ->
    % 0. Check that the request isn't outside of first_index and last_index
    % 1. Check the local cache for any unflushed entries, carry reminders
    % 2. Check ra_log_open_mem_tables
    % 3. Check ra_log_closed_mem_tables in turn
    % 4. Check on disk segments in turn
    case cache_take(Start, Num, State) of
        {Entries, MetricOps0, undefined} ->
            ok = update_metrics(UId, MetricOps0),
            {Entries, State};
        {Entries0, MetricOps0, Rem0} ->
            case open_mem_tbl_take(UId, Rem0, MetricOps0, Entries0) of
                {Entries1, MetricOps, undefined} ->
                    ok = update_metrics(UId, MetricOps),
                    {Entries1, State};
                {Entries1, MetricOps1, Rem1} ->
                    case closed_mem_tbl_take(UId, Rem1, MetricOps1, Entries1) of
                        {Entries2, MetricOps, undefined} ->
                            ok = update_metrics(UId, MetricOps),
                            {Entries2, State};
                        {Entries2, MetricOps2, {S, E} = Rem2} ->
                            case catch segment_take(State, Rem2, Entries2) of
                                {Open, undefined, Entries} ->
                                    MetricOp = {?METRICS_SEGMENT_POS, E - S + 1},
                                    ok = update_metrics(UId, [MetricOp | MetricOps2]),
                                    {Entries, State#state{open_segments = Open}}
                            end
                    end
            end
    end;
take(_Start, _Num, State) ->
    {[], State}.

-spec last_index_term(ra_log()) -> ra_idxterm().
last_index_term(#state{last_index = LastIdx, last_term = LastTerm}) ->
    {LastIdx, LastTerm}.

-spec last_written(ra_log()) -> ra_idxterm().
last_written(#state{last_written_index_term = LWTI}) ->
    LWTI.

%% forces the last index and last written index back to a prior index
-spec set_last_index(ra_index(), ra_log()) ->
    {ok, ra_log()} | {not_found, ra_log()}.
set_last_index(Idx, #state{last_written_index_term = {LWIdx0, _}} = State0) ->
    case fetch_term(Idx, State0) of
        {undefined, State} ->
            {not_found, State};
        {Term, State1} ->
            LWIdx = min(Idx, LWIdx0),
            {LWTerm, State2} = fetch_term(LWIdx, State1),
            %% this should always be found but still assert just in case
            true = LWTerm =/= undefined,
            {ok, State2#state{last_index = Idx,
                              last_term = Term,
                              last_written_index_term = {LWIdx, LWTerm}}}
    end.

-spec handle_event(event_body(), ra_log()) ->
    ra_log().
handle_event({written, {FromIdx, ToIdx, Term}},
             #state{last_written_index_term = {LastWrittenIdx0,
                                               LastWrittenTerm0},
                    snapshot_state = SnapState} = State0)
  when FromIdx =< LastWrittenIdx0 + 1 ->
    % We need to ignore any written events for the same index
    % but in a prior term if we do not we may end up confirming
    % to a leader writes that have not yet
    % been fully flushed
    case fetch_term(ToIdx, State0) of
        {Term, State} ->
            % this case truncation shouldn't be too expensive as the cache
            % only containes the unflushed window of entries typically less than
            % 10ms worth of entries
            truncate_cache(ToIdx,
                           State#state{last_written_index_term = {ToIdx, Term}});
        {undefined, State} when FromIdx =< element(1, SnapState) ->
            % A snapshot happened before the written event came in
            % This can only happen on a leader when consensus is achieved by
            % followers returning appending the entry and the leader committing
            % and processing a snapshot before the written event comes in.
            % ensure last_written_index_term does not go backwards
            LastWrittenIdxTerm = {max(LastWrittenIdx0, ToIdx),
                                  max(LastWrittenTerm0, Term)},
            State#state{last_written_index_term = LastWrittenIdxTerm};
        {_X, State} ->
            ?INFO("~s: written event did not find term ~b for index ~b "
                  "found ~w",
                  [State#state.uid, Term, ToIdx, _X]),
            State
    end;
handle_event({written, {FromIdx, _ToIdx, _Term}},
             #state{uid = UId,
                    last_written_index_term = {LastWrittenIdx, _}} = State0)
  when FromIdx > LastWrittenIdx + 1 ->
    % leaving a gap is not ok - resend from cache
    Expected = LastWrittenIdx + 1,
    ?WARN("~s: ra_log: written gap detected at ~b expected ~b!",
         [UId, FromIdx, Expected]),
    resend_from(Expected, State0);
handle_event({segments, Tid, NewSegs},
             #state{uid = UId,
                    open_segments = Open0,
                    segment_refs = SegmentRefs} = State0) ->
    % Append new segment refs
    % mem_table cleanup
    % any closed mem tables older than the one just having been flushed should
    % be ok to delete
    ClosedTables = closed_mem_tables(UId),
    Active = lists:takewhile(fun ({_, _, _, _, T}) -> T =/= Tid end,
                             ClosedTables),
    % not fast but we rarely should have more than one or two closed tables
    % at any time
    Obsolete = ClosedTables -- Active,
    % assert at least one table was found
    % commented out as if the wal restarts more than once in short succession
    % we may be processing a segments event referring to open mem tables
    % that the wal replaced during init. the segment file is still valid and
    % we'll probably receive another copy shortly.
    % [_|_] = Obsolete,
    [begin
         true = ets:delete_object(ra_log_closed_mem_tables, ClosedTbl),
         % Then delete the actual ETS table
         true = ets:delete(T)
     end || {_, _, _, _, T} = ClosedTbl  <- Obsolete],
    %% check if any of the segrefs refer to open segment and re-load their
    %% new indexs if so
    Open = lists:foldl(fun ({_, _, F}, Acc0) ->
                               case ra_flru:evict(F, Acc0) of
                                   {_, Acc} ->
                                       %% TODO: evicting here could result in
                                       %% unecessary
                                       %% file handle churn but may not be worth
                                       %% optimising
                                       %% Alternative would be something like:
                                       %% ra_log_segment:reload_index/1
                                       Acc;
                                   error ->
                                       Acc0
                               end
                       end, Open0, SegmentRefs),

    % compact seg ref list so that only the latest range for a segment
    % file has an entry
    State0#state{segment_refs = compact_seg_refs(NewSegs ++ SegmentRefs),
                 open_segments = Open,
                 % re-enable snapshots based on release cursor updates
                 % in case snapshot_written was lost
                 snapshot_index_in_progress = undefined};
handle_event({snapshot_written, {Idx, Term}, File, Old},
             #state{uid = UId, open_segments = OpenSegs0,
                    directory = Dir,
                    segment_refs = SegRefs0} = State0) ->
    % delete any segments outside of first_index
    {SegRefs, OpenSegs, NumObsolete} =
        % all segments created prior to the first reclaimable should be
        % reclaimed even if they have a more up to date end point
        case lists:partition(fun({_, To, _}) when To > Idx -> true;
                                (_) -> false
                             end, SegRefs0) of
            {_, []} ->
                {SegRefs0, OpenSegs0, 0};
            {Active, Obsolete} ->
                % close all relevant active segments
                ObsoleteKeys = [element(3, O) || O <- Obsolete],
                % close any open segments
                OpenSegs1 = lists:foldl(fun (K, OS0) ->
                                                case ra_flru:evict(K, OS0) of
                                                    {_, OS} ->
                                                        OS;
                                                    error ->
                                                        OS0
                                                end
                                        end, OpenSegs0, ObsoleteKeys),
                % [ok = ra_log_segment:close(S)
                %  || S <- maps:values(maps:with(ObsoleteKeys, OpenSegs0))],
                ObsoleteFiles = [filename:join(Dir, O) || O <- ObsoleteKeys],
                ok = ra_log_segment_writer:delete_segments(UId, Idx,
                                                           ObsoleteFiles),
                {Active, OpenSegs1, length(ObsoleteKeys)}
        end,
    ?INFO("~s: Snapshot written at index ~b in term ~b. ~b"
          " obsolete segments~n",
          [UId, Idx, Term, NumObsolete]),
    true = ets:insert(ra_log_snapshot_state, {UId, Idx}),
    %% delete old files
    [file:delete(F) || F <- Old],
    truncate_cache(Idx,
                   State0#state{first_index = Idx + 1,
                                segment_refs = SegRefs,
                                open_segments = OpenSegs,
                                snapshot_index_in_progress = undefined,
                                snapshot_state = {Idx, Term, File}});
handle_event({resend_write, Idx}, State) ->
    % resend missing entries from cache.
    % The assumption is they are available in the cache
    resend_from(Idx, State).

-spec next_index(ra_log()) -> ra_index().
next_index(#state{last_index = LastIdx}) ->
    LastIdx + 1.

-spec fetch(ra_index(), ra_log()) ->
    {maybe(log_entry()), ra_log()}.
fetch(Idx, State0) ->
    case take(Idx, 1, State0) of
        {[], State} ->
            {undefined, State};
        {[Entry], State} ->
            {Entry, State}
    end.

-spec fetch_term(ra_index(), ra_log()) ->
    {maybe(ra_term()), ra_log()}.
fetch_term(Idx, #state{last_index = LastIdx,
                       first_index = FirstIdx} = State0)
  when Idx < FirstIdx orelse Idx > LastIdx ->
    {undefined, State0};
fetch_term(Idx, #state{cache = Cache, uid = UId} = State0) ->
    case Cache of
        #{Idx := {Term, _}} ->
            {Term, State0};
        _ ->
            case ets:lookup(ra_log_open_mem_tables, UId) of
                [{_, From, To, Tid}] when Idx >= From andalso Idx =< To ->
                    Term = ets:lookup_element(Tid, Idx, 2),
                    {Term, State0};
                _ ->
                    case closed_mem_table_term_query(Idx, UId) of
                        undefined ->
                            segment_term_query(Idx, State0);
                        Term ->
                            {Term, State0}
                    end
            end
    end.

-spec install_snapshot(Snapshot :: ra_log:snapshot(),
                       State :: ra_log()) ->
    ra_log().
install_snapshot({Idx, Term, _, _} = Snapshot,
                 #state{directory = Dir} = State) ->
    % syncronous call when follower receives a snapshot
    {ok, File, Old} = ra_log_snapshot_writer:write_snapshot_call(Dir, Snapshot),
    handle_event({snapshot_written, {Idx, Term}, File, Old},
                 State#state{last_index = Idx,
                             last_written_index_term = {Idx, Term}}).

-spec read_snapshot(State :: ra_log()) ->
    maybe(ra_log:snapshot()).
read_snapshot(#state{snapshot_state = undefined}) ->
    undefined;
read_snapshot(#state{snapshot_state = {_, _, File}}) ->
    case ra_log_snapshot:read(File) of
        {ok, Snapshot} ->
            Snapshot;
        {error, enoent} ->
            undefined
    end.

-spec snapshot_index_term(State :: ra_log()) -> maybe(ra_idxterm()).
snapshot_index_term(#state{snapshot_state = {Idx, Term, _}}) ->
    {Idx, Term};
snapshot_index_term(_State) ->
    undefined.

-spec update_release_cursor(Idx :: ra_index(), Cluster :: ra_cluster(),
                            MachineState :: term(), State :: ra_log()) ->
    ra_log().
update_release_cursor(_, _, _, %% Idx, Cluster, MacState
                      #state{snapshot_index_in_progress = SIIP} = State)
  when is_integer(SIIP) ->
    % if a snapshot is in progress don't even evaluate
    % new segments will always set snapshot_index_in_progress = undefined
    % to ensure liveliness in case a snapshot_written message is lost.
    State;
update_release_cursor(Idx, Cluster, MachineState,
                      #state{segment_refs = SegRefs,
                             snapshot_state = SnapState,
                             snapshot_interval = SnapInter} = State0) ->
    ClusterNodes = maps:keys(Cluster),
    SnapLimit = case SnapState of
                    undefined -> SnapInter;
                    {I, _, _} -> I + SnapInter
                end,
    % The release cursor index is the last entry _not_ contributing
    % to the current state. I.e. the last entry that can be discarded.
    % Check here if any segments can be release.
    case lists:any(fun({_From, To, _}) when To =< Idx -> true;
                      (_) -> false end, SegRefs) of
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
            % The MachineState is a dehydrated version of the state at
            % the release_cursor point.
            case fetch_term(Idx, State0) of
                {undefined, _} ->
                    exit({term_not_found_for_index, Idx});
                {Term, State} ->
                    ok = sync_meta(State),
                    write_snapshot({Idx, Term, ClusterNodes, MachineState},
                                   State)
            end;
        false when Idx > SnapLimit ->
            %% periodically take snapshots event if segments cannot be cleared
            %% up
            case fetch_term(Idx, State0) of
                {undefined, State} ->
                    State;
                {Term, State} ->
                    ok = sync_meta(State),
                    write_snapshot({Idx, Term, ClusterNodes, MachineState},
                                   State)
            end;
        false ->
            State0
    end.


-spec read_meta(Key :: ra_meta_key(),
                State :: ra_log()) -> maybe(term()).
read_meta(Key, #state{kv = Kv}) ->
    ra_log_meta:fetch(Key, Kv).

-spec read_meta(Key :: ra_meta_key(), State :: ra_log(),
                Default :: term()) -> term().
read_meta(Key, Log, Default) ->
    ra_lib:default(read_meta(Key, Log), Default).

-spec write_meta(Key :: ra_meta_key(), Value :: term(), State :: ra_log()) ->
    ra_log().
write_meta(Key, Value, Log) ->
    write_meta(Key, Value, Log, true).

-spec write_meta(Key :: ra_meta_key(), Value :: term(),
                 State :: ra_log(), Sync :: boolean()) -> ra_log().
write_meta(Key, Value, #state{kv = Kv} = Log, true) ->
    ok = ra_log_meta:store(Key, Value, Kv),
    ok = sync_meta(Log),
    Log;
write_meta(Key, Value, #state{kv = Kv} = Log, false) ->
    ok = ra_log_meta:store(Key, Value, Kv),
    Log.

append_sync({Idx, Term, _} = Entry, Log0) ->
    Log = ra_log:append(Entry, Log0),
    receive
        {ra_log_event, {written, {_, Idx, Term}} = Evt} ->
            ra_log:handle_event(Evt, Log)
    after ?LOG_APPEND_TIMEOUT ->
              throw(ra_log_append_timeout)
    end.

write_sync(Entries, Log0) ->
    {Idx, Term, _} = lists:last(Entries),
    case ra_log:write(Entries, Log0) of
        {ok, Log} ->
            receive
                {ra_log_event, {written, {_, Idx, Term}} = Evt} ->
                    ra_log:handle_event(Evt, Log)
            after ?LOG_APPEND_TIMEOUT ->
                      throw(ra_log_append_timeout)
            end;
        {error, _} = Err ->
            Err
    end.
-spec sync_meta(State :: ra_log()) -> ok.
sync_meta(#state{kv = Kv}) ->
    ok = ra_log_meta:sync(Kv),
    ok.

can_write(#state{wal = Wal}) ->
    undefined =/= whereis(Wal).

-spec exists(ra_idxterm(), ra_log()) ->
    {boolean(), ra_log()}.
exists({Idx, Term}, Log0) ->
    case fetch_term(Idx, Log0) of
        {Term, Log} -> {true, Log};
        {_, Log} -> {false, Log}
    end.

overview(#state{last_index = LastIndex,
                last_written_index_term = LWIT,
                snapshot_index_in_progress = SIIP,
                segment_refs = Segs,
                snapshot_state = SnapshotState,
                open_segments = OpenSegs}) ->
    #{type => ?MODULE,
      last_index => LastIndex,
      last_written_index_term => LWIT,
      num_segments => length(Segs),
      open_segments => ra_flru:size(OpenSegs),
      snapshot_index_in_progress => SIIP,
      snapshot_index => case SnapshotState of
                            undefined -> undefined;
                            _ -> element(1, SnapshotState)
                        end
     }.

write_config(Config, #state{directory = Dir}) ->
    ConfigPath = filename:join(Dir, "config"),
    ok = file:write_file(ConfigPath,
                         list_to_binary(io_lib:format("~p.", [Config]))),
    ok.

read_config(Dir) ->
    ConfigPath = filename:join(Dir, "config"),
    case filelib:is_file(ConfigPath) of
        true ->
            {ok, [C]} =  file:consult(ConfigPath),
            {ok, C};
        false ->
            not_found
    end.

delete_everything(#state{directory = Dir} = Log) ->
    _ = close(Log),
    try ra_lib:recursive_delete(Dir) of
        ok -> ok
    catch
        _:_ = Err ->
            ?WARN("ra_log:delete_everything/1 failed to delete "
                  "directory ~s~n Error: ~p~n", [Dir, Err])
    end,
    ok.

release_resources(MaxOpenSegments,
                  #state{open_segments = OpenSegs} = State) ->
    % close all open segments
    % deliberately ignoring return value
    _ = ra_flru:evict_all(OpenSegs),
    %% open a new segment with the new max open segment value
    State#state{open_segments = ra_flru:new(MaxOpenSegments,
                                            fun flru_handler/1)}.

%%% Local functions

wal_truncate_write(#state{uid = UId, cache = Cache, wal = Wal} = State,
                   {Idx, Term, Data}) ->
    % this is the next write after a snapshot was taken or received
    % we need to indicate to the WAL that this may be a non-contiguous write
    % and that prior entries should be considered stale
    ok = ra_log_wal:truncate_write({UId, self()}, Wal, Idx, Term, Data),
    State#state{last_index = Idx, last_term = Term,
                cache = Cache#{Idx => {Term, Data}}}.

wal_write(#state{uid = UId, cache = Cache, wal = Wal} = State,
          {Idx, Term, Data}) ->
    case ra_log_wal:write({UId, self()}, Wal, Idx, Term, Data) of
        ok ->
            State#state{last_index = Idx, last_term = Term,
                        cache = Cache#{Idx => {Term, Data}}};
        {error, wal_down} ->
            exit(wal_down)
    end.

truncate_cache(Idx, #state{cache = Cache0} = State) ->
    Cache = maps:filter(fun (K, _) when K > Idx -> true;
                            (_, _) -> false
                        end, Cache0),
    State#state{cache = Cache}.

update_metrics(Id, Ops) ->
    _ = ets:update_counter(ra_log_metrics, Id, Ops),
    ok.

open_mem_tbl_take(Id, {Start0, End}, MetricOps, Acc0) ->
    case ets:lookup(ra_log_open_mem_tables, Id) of
        [{_, TStart, TEnd, Tid}] ->
            {Entries, Count, Rem} = mem_tbl_take({Start0, End}, TStart, TEnd,
                                                 Tid, 0, Acc0),
            {Entries, [{?METRICS_OPEN_MEM_TBL_POS, Count} | MetricOps], Rem};
        [] ->
            {Acc0, MetricOps, {Start0, End}}
    end.

closed_mem_tbl_take(Id, {Start0, End}, MetricOps, Acc0) ->
    case closed_mem_tables(Id) of
        [] ->
            {Acc0, MetricOps, {Start0, End}};
        Tables ->
            {Entries, Count, Rem} =
            lists:foldl(fun({_, _, TblSt, TblEnd, Tid}, {Ac, Count, Range}) ->
                                mem_tbl_take(Range, TblSt, TblEnd, Tid, Count, Ac)
                        end, {Acc0, 0, {Start0, End}}, Tables),
            {Entries, [{?METRICS_CLOSED_MEM_TBL_POS, Count} | MetricOps], Rem}
    end.

closed_mem_table_term_query(Idx, Id) ->
    case closed_mem_tables(Id) of
        [] ->
            undefined;
        Tables ->
            closed_mem_table_term_query0(Idx, Tables)
    end.

closed_mem_table_term_query0(_Idx, []) ->
    undefined;
closed_mem_table_term_query0(Idx, [{_, _, From, To, Tid} | _Tail])
  when Idx >= From andalso Idx =< To ->
    ets:lookup_element(Tid, Idx, 2);
closed_mem_table_term_query0(Idx, [_ | Tail]) ->
    closed_mem_table_term_query0(Idx, Tail).

closed_mem_tables(Id) ->
    case ets:lookup(ra_log_closed_mem_tables, Id) of
        [] ->
            [];
        Tables ->
            lists:sort(fun (A, B) ->
                               element(2, A) > element(2, B)
                       end, Tables)
    end.

mem_tbl_take(undefined, _TblStart, _TblEnd, _Tid, Count, Acc0) ->
    {Acc0, Count, undefined};
mem_tbl_take({_Start0, End} = Range, TblStart, _TblEnd, _Tid, Count, Acc0)
  when TblStart > End ->
    % optimisation to bypass request that has no overlap
    {Acc0, Count, Range};
mem_tbl_take({Start0, End}, TblStart, TblEnd, Tid, Count, Acc0)
  when TblEnd >= End ->
    Start = max(TblStart, Start0),
    Entries = lookup_range(Tid, Start, End, Acc0),
    Remainder = case Start =:= Start0 of
                    true ->
                        % the range was fully covered by the mem table
                        undefined;
                    false ->
                        {Start0, Start-1}
                end,
    {Entries, Count + (End - Start + 1), Remainder}.

lookup_range(Tid, Start, Start, Acc) ->
    [Entry] = ets:lookup(Tid, Start),
    [Entry | Acc];
lookup_range(Tid, Start, End, Acc) when End > Start ->
    [Entry] = ets:lookup(Tid, End),
    lookup_range(Tid, Start, End-1, [Entry | Acc]).


segment_take(#state{segment_refs = SegRefs,
                    open_segments = OpenSegs,
                    directory = Dir},
             Range, Entries0) ->
    lists:foldl(
      fun(_, {_, undefined, _} = Acc) ->
              %% we're done reading
              throw(Acc);
         ({From, _, _}, {_, {_, End}, _} = Acc)
           when From > End ->
              Acc;
         ({From, To, Fn}, {Open0, {Start0, End}, E0})
           when To >= End ->
              {Seg, Open} =
                  case ra_flru:fetch(Fn, Open0) of
                      {ok, S, Open1} ->
                          {S, Open1};
                      error ->
                          AbsFn = filename:join(Dir, Fn),
                          case ra_log_segment:open(AbsFn,
                                                   #{mode => read}) of
                              {ok, S} ->
                                  {S, ra_flru:insert(Fn, S, Open0)};
                              {error, Err} ->
                                  exit({ra_log_failed_to_open_segment, Err,
                                        AbsFn})
                          end
                  end,

              % actual start point cannot be prior to first segment
              % index
              Start = max(Start0, From),
              Num = End - Start + 1,
              Entries = ra_log_segment:read_cons(Seg, Start, Num,
                                                 fun binary_to_term/1,
                                                 E0),
              Rem = case Start of
                        Start0 -> undefined;
                        _ ->
                            {Start0, Start-1}
                    end,
              {Open, Rem, Entries}
      end, {OpenSegs, Range, Entries0}, SegRefs).

segment_term_query(Idx, #state{segment_refs = SegRefs,
                               directory = Dir,
                               open_segments = OpenSegs} = State) ->
    {Result, Open} = segment_term_query0(Idx, SegRefs, OpenSegs, Dir),
    {Result, State#state{open_segments = Open}}.

segment_term_query0(Idx, [{From, To, Filename} | _], Open0, Dir)
  when Idx >= From andalso Idx =< To ->
    case ra_flru:fetch(Filename, Open0) of
        {ok, Seg, Open} ->
            Term = ra_log_segment:term_query(Seg, Idx),
            {Term, Open};
        error ->
            AbsFn = filename:join(Dir, Filename),
            {ok, Seg} = ra_log_segment:open(AbsFn, #{mode => read}),
            Term = ra_log_segment:term_query(Seg, Idx),
            {Term, ra_flru:insert(Filename, Seg, Open0)}
    end;
segment_term_query0(Idx, [_ | Tail], Open, Dir) ->
    segment_term_query0(Idx, Tail, Open, Dir);
segment_term_query0(_Idx, [], Open, _) ->
    {undefined, Open}.


cache_take(Start, Num, #state{cache = Cache, last_index = LastIdx}) ->
    Highest = min(LastIdx, Start + Num - 1),
    % cache needs to be queried in reverse to ensure
    % we can bail out when an item is not found
    case cache_take0(Highest, Start, Cache, []) of
        [] ->
            {[], [], {Start, Highest}};
        [Last | _] = Entries when element(1, Last) =:= Start ->
            % there is no remainder - everything was in the cache
            {Entries, [{?METRICS_CACHE_POS, Highest - Start + 1}], undefined};
        [Last | _] = Entries ->
            LastEntryIdx = element(1, Last),
            {Entries, [{?METRICS_CACHE_POS, LastIdx - Start + 1}],
             {Start, LastEntryIdx - 1}}
    end.

cache_take0(Next, Last, _Cache, Acc)
  when Next < Last ->
    Acc;
cache_take0(Next, Last, Cache, Acc) ->
    case Cache of
        #{Next := Entry} ->
            cache_take0(Next-1, Last, Cache,
                        [erlang:insert_element(1, Entry, Next) | Acc]);
        _ ->
            Acc
    end.

maybe_append_0_0_entry(State0 = #state{last_index = -1}) ->
    State = append({0, 0, undefined}, State0),
    receive
        {ra_log_event, {written, {0, 0, 0}}} -> ok
    end,
    State#state{first_index = 0, last_written_index_term = {0, 0}};
maybe_append_0_0_entry(State) ->
    State.

resend_from(Idx, #state{uid = UId} = State0) ->
    try resend_from0(Idx, State0) of
        State -> State
    catch
        exit:wal_down ->
            ?WARN("~s: ra_log: resending from ~b failed with wal_down",
                  [UId, Idx]),
            State0
    end.

resend_from0(Idx, #state{last_index = LastIdx,
                         last_resend_time = undefined,
                         cache = Cache} = State) ->
    ?INFO("~s: ra_log: resending from ~b to ~b",
          [State#state.uid, Idx, LastIdx]),
    lists:foldl(fun (I, Acc) ->
                        X = maps:get(I, Cache),
                        wal_write(Acc, erlang:insert_element(1, X, I))
                end,
                State#state{last_resend_time = erlang:system_time(seconds)},
                % TODO: replace with recursive function
                lists:seq(Idx, LastIdx));
resend_from0(Idx, #state{last_resend_time = LastResend,
                        resend_window_seconds = ResendWindow} = State) ->
    case erlang:system_time(seconds) > LastResend + ResendWindow of
        true ->
            % it has been more than a minute since last resend
            % ok to try again
            resend_from(Idx, State#state{last_resend_time = undefined});
        false ->
            State
    end.


compact_seg_refs(SegRefs) ->
    lists:reverse(
      lists:foldl(fun ({_, _, File} = S, Acc) ->
                          case lists:any(fun({_, _, F}) when F =:= File ->
                                                 true;
                                            (_) -> false
                                         end, Acc) of
                              true -> Acc;
                              false -> [S | Acc]
                          end
                  end, [], SegRefs)).

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
    case verify_entries(FstIdx, Rest) of
        ok ->
            try
                % TODO: wal should provide batch api
                State = lists:foldl(fun (Entry, S) ->
                                            wal_write(S, Entry)
                                    end, State0, Entries),
                {ok, State}
            catch
                exit:wal_down ->
                    {error, wal_down}
            end;
        Error ->
            Error
    end.

write_snapshot(Snapshot, #state{directory = Dir} = State) ->
    ok = ra_log_snapshot_writer:write_snapshot(self(), Dir, Snapshot),
    State#state{snapshot_index_in_progress = element(1, Snapshot)}.

flru_handler({_, Seg}) ->
    _ = ra_log_segment:close(Seg),
    ok.


%%%% TESTS

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

compact_seg_refs_test() ->
    % {From, To, File}
    Refs = [{10, 100, "2"}, {10, 75, "2"}, {10, 50, "2"}, {1, 9, "1"}],
    [{10, 100, "2"}, {1, 9, "1"}] = compact_seg_refs(Refs),
    ok.

cache_take0_test() ->
    Cache = #{1 => {a}, 2 => {b}, 3 => {c}},
    State = #state{cache = Cache, last_index = 3, first_index = 1},
    % no remainder
    {[{2, b}], _, undefined} = cache_take(2, 1, State),
    {[{2, b}, {3, c}], _,  undefined} = cache_take(2, 2, State),
    {[{1, a}, {2, b}, {3, c}], _, undefined} = cache_take(1, 3, State),
    % small remainder
    {[{3, c}], _, {1, 2}} = cache_take(1, 3, State#state{cache = #{3 => {c}}}),
    {[], _, {1, 3}} = cache_take(1, 3, State#state{cache = #{4 => {d}}}),
    ok.

open_mem_tbl_take_test() ->
    _ = ets:new(ra_log_open_mem_tables, [named_table]),
    Tid = ets:new(test_id, []),
    true = ets:insert(ra_log_open_mem_tables, {test_id, 3, 7, Tid}),
    Entries = [{3, 2, "3"}, {4, 2, "4"},
               {5, 2, "5"}, {6, 2, "6"},
               {7, 2, "7"}],
    % seed the mem table
    [ets:insert(Tid, E) || E <- Entries],

    {Entries, _, undefined} = open_mem_tbl_take(test_id, {3, 7}, [], []),
    EntriesPlus8 = Entries ++ [{8, 2, "8"}],
    {EntriesPlus8, _, {1, 2}} = open_mem_tbl_take(test_id, {1, 7}, [], [{8, 2, "8"}]),
    {[{6, 2, "6"}], _, undefined} = open_mem_tbl_take(test_id, {6, 6}, [], []),
    {[], _, {1, 2}} = open_mem_tbl_take(test_id, {1, 2}, [], []),

    ets:delete(Tid),
    ets:delete(ra_log_open_mem_tables),

    ok.

closed_mem_tbl_take_test() ->
    _ = ets:new(ra_log_closed_mem_tables, [named_table, bag]),
    Tid1 = ets:new(test_id, []),
    Tid2 = ets:new(test_id, []),
    M1 = erlang:unique_integer([monotonic, positive]),
    M2 = erlang:unique_integer([monotonic, positive]),
    true = ets:insert(ra_log_closed_mem_tables, {test_id, M1, 5, 7, Tid1}),
    true = ets:insert(ra_log_closed_mem_tables, {test_id, M2, 8, 10, Tid2}),
    Entries1 = [{5, 2, "5"}, {6, 2, "6"}, {7, 2, "7"}],
    Entries2 = [{8, 2, "8"}, {9, 2, "9"}, {10, 2, "10"}],
    % seed the mem tables
    [ets:insert(Tid1, E) || E <- Entries1],
    [ets:insert(Tid2, E) || E <- Entries2],

    {Entries1, _, undefined} = closed_mem_tbl_take(test_id, {5, 7}, [], []),
    {Entries2, _, undefined} = closed_mem_tbl_take(test_id, {8, 10}, [], []),
    {[{9, 2, "9"}], _, undefined} = closed_mem_tbl_take(test_id, {9, 9}, [], []),
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
