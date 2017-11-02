-module(ra_log_file).
-behaviour(ra_log).

-compile([inline_list_funcs]).

-export([init/1,
         close/1,
         append/3,
         take/3,
         last_index_term/1,
         handle_event/2,
         last_written/1,
         fetch/2,
         fetch_term/2,
         next_index/1,
         write_snapshot/2,
         read_snapshot/1,
         read_meta/2,
         write_meta/3,
         sync_meta/1
        ]).

-include("ra.hrl").

-define(METRICS_CACHE_POS, 2).
-define(METRICS_OPEN_MEM_TBL_POS, 3).
-define(METRICS_CLOSED_MEM_TBL_POS, 4).
-define(METRICS_SEGMENT_POS, 5).

-record(state,
        {first_index = -1 :: ra_index(),
         last_index = -1 :: -1 | ra_index(),
         last_term = 0 :: ra_term(),
         last_written_index_term = {0, 0} :: ra_idxterm(),
         id :: atom(),
         segment_refs = [] :: [ra_log:ra_segment_ref()],
         open_segments = #{} :: #{file:filename() => ra_log_file_segment:state()},
         directory :: list(),
         kv :: reference(),
         cache = #{} :: #{ra_index() => {ra_term(), log_entry()}}
        }).

-type ra_log_file_state() :: #state{}.


-spec init(ra_log:ra_log_init_args()) -> ra_log_file_state().
init(#{directory := Dir, id := Id}) ->
    Dets = filename:join(Dir, "ra_log_kv.dets"),
    ok = filelib:ensure_dir(Dets),
    {ok, Kv} = dets:open_file(Dets, []),
    % index recovery is done by the storage engine
    % at some point we need to recover some kind of index of
    % flushed segments
    {FirstIndex, LastIndex} = last_index_from_mem_table(Id),
    State0 = #state{directory = Dir, id = Id,
                    first_index = FirstIndex,
                    last_index = LastIndex,
                    kv = Kv},

    % initialise metrics for this node
    true = ets:insert(ra_log_file_metrics, {Id, 0, 0, 0, 0}),

    % initialized with a default 0 index 0 term dummy value
    % and an empty meta data map
    State = maybe_append_0_0_entry(State0),
    ?DBG("ra_log_file recovered last_index_term ~p~n", [last_index_term(State)]),
    State.

-spec close(ra_log_file_state()) -> ok.
close(#state{kv = Kv}) ->
    % deliberately ignoring return value
    _ = dets:close(Kv),
    ok.

-spec append(Entry :: log_entry(),
             overwrite | no_overwrite,
             State :: ra_log_file_state()) ->
    {queued, ra_log_file_state()} |
    {error, integrity_error}.
append(Entry, no_overwrite, #state{last_index = LastIdx})
      when element(1, Entry) =< LastIdx ->
    {error, integrity_error};
append(Entry, overwrite, State) ->
    {queued, write(State, Entry)};
append(Entry, no_overwrite, State = #state{last_index = LastIdx})
      when element(1, Entry) > LastIdx ->
    {queued, write(State, Entry)}.

-spec take(ra_index(), non_neg_integer(), ra_log_file_state()) ->
    {[log_entry()], ra_log_file_state()}.
take(Start, Num, #state{id = Id, first_index = F,
                                 last_index = LastIdx} = State)
  when Start >= F andalso Start =< LastIdx ->
    % 0. Check that the request isn't outside of first_index and last_index
    % 1. Check the local cache for any unflushed entries, carry reminders
    % 2. Check ra_log_open_mem_tables
    % 3. Check ra_log_closed_mem_tables in turn
    % 4. Check on disk segments in turn
    case cache_take(Start, Num, State) of
        {Entries, MetricOps0, undefined} ->
            ok = update_metrics(Id, MetricOps0),
            {Entries, State};
        {Entries0, MetricOps0, Rem0} ->
            case open_mem_tbl_take(Id, Rem0, MetricOps0, Entries0) of
                {Entries1, MetricOps, undefined} ->
                    ok = update_metrics(Id, MetricOps),
                    {Entries1, State};
                {Entries1, MetricOps1, Rem1} ->
                    case closed_mem_tbl_take(Id, Rem1, MetricOps1, Entries1) of
                        {Entries2, MetricOps, undefined} ->
                            ok = update_metrics(Id, MetricOps),
                            {Entries2, State};
                        {Entries2, MetricOps2, {S, E} = Rem2} ->
                            case segment_take(State, Rem2, Entries2) of
                                {Open, undefined, Entries} ->
                                    MetricOp = {?METRICS_SEGMENT_POS, E - S + 1},
                                    ok = update_metrics(Id, [MetricOp | MetricOps2]),
                                    {Entries, State#state{open_segments = Open}}
                            end
                    end
            end
    end;
take(_Start, _Num, State) ->
    {[], State}.

-spec last_index_term(ra_log_file_state()) -> maybe(ra_idxterm()).
last_index_term(#state{last_index = LastIdx, last_term = LastTerm}) ->
    {LastIdx, LastTerm}.

-spec last_written(ra_log_file_state()) -> ra_idxterm().
last_written(#state{last_written_index_term = LWTI}) ->
    LWTI.

-spec handle_event(ra_log:ra_log_event(), ra_log_file_state()) ->
    ra_log_file_state().
handle_event({written, {Idx, Term} = IdxTerm}, State0) ->
    case fetch_term(Idx, State0) of
        {Term, State} ->
            % TODO: this case truncation shouldn't be too expensive as the cache
            % only containes the unflushed window of entries typically less than
            % 10ms worth of entries
            truncate_cache(Idx, State#state{last_written_index_term = IdxTerm});
        _ ->
            State0
    end;
handle_event({segments, Tid, NewSegs},
             #state{id = Id, segment_refs = SegmentRefs} = State0) ->
    % Append new segment refs
    % TODO: some segments could possibly be recovered at this point if the new
    % segments already cover their ranges
    % mem_table cleanup
    % TODO: measure - if this proves expensive it could be done in a separate processes
    % First remove the closed mem table reference
    ClsdTbl = lists_find(fun ({_, _, _, _, T}) -> T =:= Tid end,
                         ets:lookup(ra_log_closed_mem_tables, Id)),
    false = ClsdTbl =:= undefined, % assert table was found
    true = ets:delete_object(ra_log_closed_mem_tables, ClsdTbl),
    % Then delete the actual ETS table
    true = ets:delete(Tid),
    State0#state{segment_refs = NewSegs ++ SegmentRefs}.

-spec next_index(ra_log_file_state()) -> ra_index().
next_index(#state{last_index = LastIdx}) ->
    LastIdx + 1.

-spec fetch(ra_index(), ra_log_file_state()) ->
    {maybe(log_entry()), ra_log_file_state()}.
fetch(Idx, #state{last_index = Last, first_index = First} = State)
  when Idx > Last orelse Idx < First ->
    {undefined, State};
fetch(Idx, State0) ->
    case take(Idx, 1, State0) of
        {[], State} ->
            {undefined, State};
        {[Entry], State} ->
            {Entry, State}
    end.

-spec fetch_term(ra_index(), ra_log_file_state()) ->
    {maybe(ra_term()), ra_log_file_state()}.
fetch_term(Idx, #state{cache = Cache} = State0) ->
    % needs to check cache first
    case Cache of
        #{Idx := {Term, _}} ->
            {Term, State0};
        _ ->
            % TODO: optimise this as we do not need to
            % read the full body
            case fetch(Idx, State0) of
                {undefined, State} ->
                    {undefined, State};
                {{_, Term, _}, State} ->
                    {Term, State}
            end
    end.

-spec write_snapshot(Snapshot :: ra_log:ra_log_snapshot(),
                     State :: ra_log_file_state()) ->
    ra_log_file_state().
write_snapshot({Idx, _, _, _} = Snapshot, State = #state{directory = Dir}) ->
    File = filename:join(Dir, "ra.snapshot"),
    file:write_file(File, term_to_binary(Snapshot)),
    % update first index to the index following the latest snapshot
    truncate_cache(Idx, State#state{first_index = Idx+1}).

-spec read_snapshot(State :: ra_log_file_state()) ->
    maybe(ra_log:ra_log_snapshot()).
read_snapshot(#state{directory = Dir}) ->
    File = filename:join(Dir, "ra.snapshot"),
    case file:read_file(File) of
        {ok, Bin} ->
            binary_to_term(Bin);
        {error, enoent} ->
            undefined
    end.

-spec read_meta(Key :: ra_log:ra_meta_key(),
                State :: ra_log_file_state()) -> maybe(term()).
read_meta(Key, #state{kv = Kv}) ->
    case dets:lookup(Kv, Key) of
        [] -> undefined;
        [Value] ->
            element(2, Value)
    end.

-spec write_meta(Key :: ra_log:ra_meta_key(), Value :: term(),
                 State :: ra_log_file_state()) ->
    {ok,  ra_log_file_state()} | {error, term()}.
write_meta(Key, Value, State = #state{kv = Kv}) ->
    ok = dets:insert(Kv, {Key, Value}),
    {ok, State}.

sync_meta(#state{kv = Kv}) ->
    ok = dets:sync(Kv),
    ok.

%%% Local functions

write(State = #state{id = Id, cache = Cache},
      {Idx, Term, Data}) ->
    ok = ra_log_wal:write(Id, ra_log_wal, Idx, Term, Data),
    State#state{last_index = Idx, last_term = Term,
                cache = Cache#{Idx => {Term, Data}}}.

truncate_cache(Idx, State = #state{cache = Cache0}) ->
    Cache = maps:filter(fun (K, _) when K > Idx -> true;
                            (_, _) -> false
                        end, Cache0),
    State#state{cache = Cache}.
update_metrics(Id, Ops) ->
    _ = ets:update_counter(ra_log_file_metrics, Id, Ops),
    ok.

open_mem_tbl_take(Id, {Start0, End}, MetricOps, Acc0) ->
    case ets:lookup(ra_log_open_mem_tables, Id) of
        [{_, TStart, TEnd, Tid}] ->
            {Entries, Count, Rem} = mem_tbl_take({Start0, End}, TStart, TEnd,
                                                 Tid, 0, Acc0),
            {Entries, [{?METRICS_OPEN_MEM_TBL_POS, Count} | MetricOps], Rem};
        [] ->
            {[], MetricOps, {Start0, End}}
    end.

closed_mem_tbl_take(Id, {Start0, End}, MetricOps, Acc0) ->
    case ets:lookup(ra_log_closed_mem_tables, Id) of
        [] ->
            {[], MetricOps, {Start0, End}};
        Tables0 ->
            Tables = lists:sort(fun (A, B) ->
                                        element(4, A) > element(4, B)
                                end, Tables0),
            {Entries, Count, Rem} =
            lists:foldl(fun({_, TblSt, TblEnd, _, Tid}, {Ac, Count, Range}) ->
                                mem_tbl_take(Range, TblSt, TblEnd, Tid, Count, Ac)
                        end, {Acc0, 0, {Start0, End}}, Tables),
            {Entries, [{?METRICS_CLOSED_MEM_TBL_POS, Count} | MetricOps], Rem}

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
    % TODO: replace fold with recursive function
    Entries = lists:foldl(fun (Idx, Acc) ->
                                  [Entry] = ets:lookup(Tid, Idx),
                                  [Entry | Acc]
                          end, Acc0, lists:seq(End, Start, -1)),
    Remainder = case Start =:= Start0 of
                    true ->
                        % the range was fully covered by the mem table
                        undefined;
                    false ->
                        {Start0, Start-1}
                end,
    {Entries, Count + (End - Start + 1), Remainder}.

segment_take(#state{segment_refs = SegRefs, open_segments = OpenSegs},
             Range, Entries0) ->
    lists:foldl(fun(_, {_, undefined, _} = Acc) ->
                    Acc;
                   ({From, _, _}, {_, {_Start0, End}, _} = Acc)
                        when From > End ->
                        Acc;
                   ({From, To, Fn}, {Open, {Start0, End}, Entries})
                     when To >= End ->
                        Seg = case Open of
                                  #{Fn := S} -> S;
                                  _ ->
                                      {ok, S} = ra_log_file_segment:open(Fn, #{mode => read}),
                                      S
                              end,
                        Start = max(Start0, From),
                        Num = End - Start + 1,
                        New = [ra_lib:update_element(3, E, fun binary_to_term/1)
                               || E <- ra_log_file_segment:read(Seg, Start, Num)],
                        % TODO: should we validate Num was read?
                        Num = length(New),
                        Rem = case Start of
                                  Start0 -> undefined;
                                  _ ->
                                      {Start0, Start-1}
                              end,
                        {Open#{Fn => Seg}, Rem, New ++ Entries}
                  end, {OpenSegs, Range, Entries0}, SegRefs).

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
    {queued, State} = append({0, 0, undefined}, no_overwrite, State0),
    receive
        {ra_log_event, {written, {0, 0}}} -> ok
    end,
    State#state{first_index = 0, last_written_index_term = {0, 0}};
maybe_append_0_0_entry(State) ->
    State.

last_index_from_mem_table(Id) ->
    case ets:lookup(ra_log_open_mem_tables, Id) of
        [] ->
            {-1, -1};
        [{Id, First, Last, _}] ->
            {First, Last}
    end.
    % First = ra_lib:default(lookup_element(ra_log_open_mem_tables, Id, 2), -1),
    % {First, Last}.

lists_find(_Pred, []) ->
    undefined;
lists_find(Pred, [H | Tail]) ->
    case Pred(H) of
        true -> H;
        false ->
            lists_find(Pred, Tail)
    end.

%%%% TESTS

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

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
    true = ets:insert(ra_log_closed_mem_tables, {test_id, 5, 7, M1, Tid1}),
    true = ets:insert(ra_log_closed_mem_tables, {test_id, 8, 10, M2, Tid2}),
    Entries1 = [{5, 2, "5"}, {6, 2, "6"}, {7, 2, "7"}],
    Entries2 = [{8, 2, "8"}, {9, 2, "9"}, {10, 2, "10"}],
    % seed the mem tables
    [ets:insert(Tid1, E) || E <- Entries1],
    [ets:insert(Tid2, E) || E <- Entries2],

    {Entries1, _, undefined} = closed_mem_tbl_take(test_id, {5, 7}, [], []),
    {Entries2, _, undefined} = closed_mem_tbl_take(test_id, {8, 10}, [], []),
    {[{9, 2, "9"}], _, undefined} = closed_mem_tbl_take(test_id, {9, 9}, [], []),
    % EntriesPlus8 = Entries ++ [{8, 2, "8"}],
    % {EntriesPlus8, {1, 2}} = open_mem_tbl_take(test_id, 1, 7, [{8, 2, "8"}]),
    % {[{6, 2, "6"}], undefined} = open_mem_tbl_take(test_id, 6, 6, []),
    % {[], {1, 2}} = open_mem_tbl_take(test_id, 1, 2, []),

    ok.
-endif.
