%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2017-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%
-module(ra_log_reader).

-compile(inline_list_funcs).

-export([
         init/6,
         init/8,
         close/1,
         update_segments/2,
         handle_log_update/2,
         segment_refs/1,
         num_open_segments/1,
         update_first_index/2,
         read/3,
         read/4,
         fetch_term/2,
         delete_closed_mem_table_object/2,
         closed_mem_tables/1,
         open_mem_table_lookup/1
         ]).

-include("ra.hrl").

-define(STATE, ?MODULE).

-type access_pattern() :: sequential | random.
%% holds static or rarely changing fields
-record(cfg, {uid :: ra_uid(),
              counter :: undefined | counters:counters_ref(),
              directory :: file:filename(),
              open_mem_tbls ::  atom(),
              closed_mem_tbls :: atom(),
              access_pattern = random :: access_pattern()
             }).

-type segment_ref() :: {From :: ra_index(), To :: ra_index(),
                        File :: string()}.
-record(?STATE, {cfg :: #cfg{},
                 first_index = 0 :: ra_index(),
                 segment_refs = [] :: [segment_ref()],
                 open_segments = ra_flru:new(1, fun flru_handler/1) :: ra_flru:state()
                }).

-opaque state() :: #?STATE{}.


-export_type([
              state/0
              ]).

%% PUBLIC

-spec init(ra_uid(), file:filename(), ra_index(), non_neg_integer(),
           [segment_ref()], ra_system:names()) -> state().
init(UId, Dir, FirstIdx, MaxOpen, SegRefs, Names) ->
    init(UId, Dir, FirstIdx, MaxOpen, random, SegRefs, Names, undefined).

-spec init(ra_uid(), file:filename(), ra_index(), non_neg_integer(),
           access_pattern(),
           [segment_ref()], ra_system:names(),
           undefined | counters:counters_ref()) -> state().
init(UId, Dir, FirstIdx, MaxOpen, AccessPattern, SegRefs,
     #{open_mem_tbls := OpnMemTbls,
       closed_mem_tbls := ClsdMemTbls}, Counter)
  when is_binary(UId) ->
    #?STATE{cfg = #cfg{uid = UId,
                       counter = Counter,
                       directory = Dir,
                       open_mem_tbls = OpnMemTbls,
                       closed_mem_tbls = ClsdMemTbls,
                       access_pattern = AccessPattern
                      },
            open_segments = ra_flru:new(MaxOpen, fun flru_handler/1),
            first_index = FirstIdx,
            segment_refs = SegRefs}.

-spec close(state()) -> ok.
close(#?STATE{open_segments = Open}) ->
    _ = ra_flru:evict_all(Open),
    ok.

-spec update_segments([segment_ref()], state()) -> state().
update_segments(NewSegmentRefs,
                #?STATE{open_segments = Open0,
                        segment_refs = SegmentRefs0} = State) ->
    SegmentRefs = compact_seg_refs(NewSegmentRefs ++ SegmentRefs0),
    %% check if any of the updated segrefs refer to open segments
    %% we close these segments so that they can be re-opened with updated
    %% indexes if needed
    Open = lists:foldl(fun ({_, _, F}, Acc0) ->
                               case ra_flru:evict(F, Acc0) of
                                   {_, Acc} -> Acc;
                                   error -> Acc0
                               end
                       end, Open0, SegmentRefs),
    State#?MODULE{segment_refs = SegmentRefs,
                  open_segments = Open}.

-spec handle_log_update({ra_log_update, undefined | pid(), ra_index(),
                         [segment_ref()]}, state()) -> state().
handle_log_update({ra_log_update, From, FstIdx, SegRefs},
                  #?STATE{open_segments = Open0} = State) ->
    Open = ra_flru:evict_all(Open0),
    case From of
        undefined -> ok;
        _ ->
            %% reply to the updater process
            From ! ra_log_update_processed
    end,
    State#?MODULE{segment_refs = SegRefs,
                  first_index = FstIdx,
                  open_segments = Open}.

-spec update_first_index(ra_index(), state()) ->
    {state(), [segment_ref()]}.
update_first_index(Idx, #?STATE{segment_refs = SegRefs0,
                                open_segments = OpenSegs0} = State) ->
    case lists:partition(fun({_, To, _}) when To > Idx -> true;
                            (_) -> false
                         end, SegRefs0) of
        {_, []} ->
            {State, []};
        {Active, Obsolete} ->
            ObsoleteKeys = [element(3, O) || O <- Obsolete],
            % close any open segments
            OpenSegs = lists:foldl(fun (K, OS0) ->
                                           case ra_flru:evict(K, OS0) of
                                               {_, OS} -> OS;
                                               error -> OS0
                                           end
                                   end, OpenSegs0, ObsoleteKeys),
            {State#?STATE{open_segments = OpenSegs,
                          first_index = Idx,
                          segment_refs = Active},
             Obsolete}
    end.

-spec segment_refs(state()) -> [segment_ref()].
segment_refs(#?STATE{segment_refs = SegmentRefs}) ->
    SegmentRefs.

-spec num_open_segments(state()) -> non_neg_integer().
num_open_segments(#?STATE{open_segments = Open}) ->
     ra_flru:size(Open).

-spec read(ra_index(), ra_index(), state()) ->
    {[log_entry()], NumRead :: non_neg_integer(), state()}.
read(From, To, State) ->
    read(From, To, State, []).

-spec read(ra_index(), ra_index(), state(), [log_entry()]) ->
    {[log_entry()], NumRead :: non_neg_integer(), state()}.
read(From, To, State, Entries) when From =< To ->
    retry_read(2, From, To, Entries, State);
read(_From, _To, State, Entries) ->
    {Entries, 0, State}.

retry_read(0, From, To, _Acc, State) ->
    exit({ra_log_reader_reader_retry_exhausted, From, To, State});
retry_read(N, From, To, Acc,
           #?STATE{cfg = #cfg{uid = UId,
                              open_mem_tbls = OpenTbl,
                              closed_mem_tbls = ClosedTbl} = Cfg} = State) ->
    % 2. Check open mem table
    % 3. Check closed mem tables in turn
    % 4. Check on disk segments in turn
    case open_mem_tbl_take(OpenTbl, UId, {From, To}, []) of
        {Entries1, {_, C} = Counter0, undefined} ->
            ok = incr_counter(Cfg, Counter0),
            {Entries1, C, State};
        {Entries1, {_, C0} = Counter0, Rem1} ->
            ok = incr_counter(Cfg, Counter0),
            case catch closed_mem_tbl_take(ClosedTbl, UId, Rem1, Entries1) of
                {Entries2, {_, C1} = Counter1, undefined} ->
                    ok = incr_counter(Cfg, Counter1),
                    {Entries2, C0 + C1, State};
                {Entries2, {_, C1} = Counter1, {S, E} = Rem2} ->
                    ok = incr_counter(Cfg, Counter1),
                    case catch segment_take(State, Rem2, Entries2) of
                        {Open, undefined, Entries} ->
                            C = (E - S + 1) + C0 + C1,
                            incr_counter(Cfg, {?C_RA_LOG_READ_SEGMENT, E - S + 1}),
                            {Entries, C, State#?MODULE{open_segments = Open}}
                    end;
                {ets_miss, _Index} ->
                    %% this would happend if a mem table was deleted after
                    %% an external reader had read the range
                    retry_read(N-1, From, To, Acc, State)
            end
    end.


-spec fetch_term(ra_index(), state()) -> {ra_index(), state()}.
fetch_term(Idx, #?STATE{cfg = #cfg{uid = UId,
                                   open_mem_tbls = OpenTbl,
                                   closed_mem_tbls = ClosedTbl} = Cfg} = State0) ->
    incr_counter(Cfg, {?C_RA_LOG_FETCH_TERM, 1}),
    case ets:lookup(OpenTbl, UId) of
        [{_, From, To, Tid}] when Idx >= From andalso Idx =< To ->
            Term = ets:lookup_element(Tid, Idx, 2),
            {Term, State0};
        _ ->
            case closed_mem_table_term_query(ClosedTbl, Idx, UId) of
                undefined ->
                    segment_term_query(Idx, State0);
                Term ->
                    {Term, State0}
            end
    end.

-spec delete_closed_mem_table_object(state(), term()) -> true.
delete_closed_mem_table_object(#?STATE{cfg =
                                       #cfg{closed_mem_tbls = Tbl}}, Id) ->
    true = ets:delete_object(Tbl, Id).

-spec closed_mem_tables(state()) -> list().
closed_mem_tables(#?STATE{cfg = #cfg{uid = UId,
                                     closed_mem_tbls = Tbl}}) ->
    closed_mem_tables(Tbl, UId).

-spec open_mem_table_lookup(state()) -> list().
open_mem_table_lookup(#?STATE{cfg = #cfg{uid = UId,
                                         open_mem_tbls = Tbl}}) ->
    ets:lookup(Tbl, UId).


%% LOCAL

segment_term_query(Idx, #?MODULE{segment_refs = SegRefs,
                                 cfg = Cfg,
                                 open_segments = OpenSegs} = State) ->
    {Result, Open} = segment_term_query0(Idx, SegRefs, OpenSegs, Cfg),
    {Result, State#?MODULE{open_segments = Open}}.

segment_term_query0(Idx, [{From, To, Filename} | _], Open0,
                    #cfg{directory = Dir,
                         access_pattern = AccessPattern})
  when Idx >= From andalso Idx =< To ->
    case ra_flru:fetch(Filename, Open0) of
        {ok, Seg, Open} ->
            Term = ra_log_segment:term_query(Seg, Idx),
            {Term, Open};
        error ->
            AbsFn = filename:join(Dir, Filename),
            {ok, Seg} = ra_log_segment:open(AbsFn, #{mode => read,
                                                     access_pattern => AccessPattern}),
            Term = ra_log_segment:term_query(Seg, Idx),
            {Term, ra_flru:insert(Filename, Seg, Open0)}
    end;
segment_term_query0(Idx, [_ | Tail], Open, Cfg) ->
    segment_term_query0(Idx, Tail, Open, Cfg);
segment_term_query0(_Idx, [], Open, _) ->
    {undefined, Open}.

open_mem_tbl_take(OpenTbl, Id, {Start0, End}, Acc0) ->
    case ets:lookup(OpenTbl, Id) of
        [{_, TStart, TEnd, Tid}] ->
            {Entries, Count, Rem} = mem_tbl_take({Start0, End}, TStart, TEnd,
                                                 Tid, 0, Acc0),
            {Entries, {?C_RA_LOG_READ_OPEN_MEM_TBL, Count}, Rem};
        [] ->
            {Acc0, {?C_RA_LOG_READ_OPEN_MEM_TBL, 0}, {Start0, End}}
    end.

closed_mem_tbl_take(ClosedTbl, Id, {Start0, End}, Acc0) ->
    case closed_mem_tables(ClosedTbl, Id) of
        [] ->
            {Acc0, {?C_RA_LOG_READ_CLOSED_MEM_TBL, 0}, {Start0, End}};
        Tables ->
            {Entries, Count, Rem} =
            lists:foldl(fun({_, _, TblSt, TblEnd, Tid}, {Ac, Count, Range}) ->
                                mem_tbl_take(Range, TblSt, TblEnd,
                                             Tid, Count, Ac)
                        end, {Acc0, 0, {Start0, End}}, Tables),
            {Entries, {?C_RA_LOG_READ_CLOSED_MEM_TBL, Count}, Rem}
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
    {Entries, Count + (End - Start + 1), Remainder};
mem_tbl_take({Start0, End}, TblStart, TblEnd, Tid, Count, Acc0)
  when TblEnd < End ->
    %% defensive case - truncate the read to end at table end
    mem_tbl_take({Start0, TblEnd}, TblStart, TblEnd, Tid, Count, Acc0).

lookup_range(Tid, Start, Start, Acc) ->
    try ets:lookup(Tid, Start) of
        [Entry] ->
            [Entry | Acc]
    catch
        error:badarg ->
            throw({ets_miss, Start})
    end;
lookup_range(Tid, Start, End, Acc) when End > Start ->
    try ets:lookup(Tid, End) of
        [Entry] ->
            lookup_range(Tid, Start, End-1, [Entry | Acc])
    catch
        error:badarg ->
            throw({ets_miss, Start})
    end.

segment_take(#?STATE{segment_refs = [],
                     open_segments = Open},
             _Range, Entries0) ->
    {Open, undefined, Entries0};
segment_take(#?STATE{segment_refs = [{_From, SEnd, _Fn} | _] = SegRefs,
                     open_segments = OpenSegs,
                     cfg = #cfg{directory = Dir,
                                access_pattern = AccessPattern}},
             {RStart, REnd}, Entries0) ->
    Range = {RStart, min(SEnd, REnd)},
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
                          case ra_log_segment:open(AbsFn, #{mode => read,
                                                            access_pattern => AccessPattern}) of
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

flru_handler({_, Seg}) ->
    _ = ra_log_segment:close(Seg),
    ok.

closed_mem_tables(Tbl, Id) ->
    case ets:lookup(Tbl, Id) of
        [] ->
            [];
        Tables ->
            lists:sort(fun (A, B) ->
                               element(2, A) > element(2, B)
                       end, Tables)
    end.

closed_mem_table_term_query(Tbl, Idx, Id) ->
    case closed_mem_tables(Tbl, Id) of
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

compact_seg_refs(SegRefs) ->
    lists:reverse(
      lists:foldl(
        fun ({_, _, File} = S, Acc) ->
                case lists:any(fun({_, _, F}) when F =:= File ->
                                       true;
                                  (_) -> false
                               end, Acc) of
                    true -> Acc;
                    false -> [S | Acc]
                end
        end, [], SegRefs)).

incr_counter(#cfg{counter = Cnt}, {Ix, N}) when Cnt =/= undefined ->
    counters:add(Cnt, Ix, N);
incr_counter(#cfg{counter = undefined}, _) ->
    ok.

-ifdef(TEST).

open_mem_tbl_take_test() ->
    OTbl = ra_log_open_mem_tables,
    _ = ets:new(OTbl, [named_table]),
    Tid = ets:new(test_id, []),
    true = ets:insert(OTbl, {test_id, 3, 7, Tid}),
    Entries = [{3, 2, "3"}, {4, 2, "4"},
               {5, 2, "5"}, {6, 2, "6"},
               {7, 2, "7"}],
    % seed the mem table
    [ets:insert(Tid, E) || E <- Entries],

    {Entries, _, undefined} = open_mem_tbl_take(OTbl, test_id, {3, 7}, []),
    EntriesPlus8 = Entries ++ [{8, 2, "8"}],
    {EntriesPlus8, _, {1, 2}} = open_mem_tbl_take(OTbl, test_id, {1, 7},
                                                  [{8, 2, "8"}]),
    {[{6, 2, "6"}], _, undefined} = open_mem_tbl_take(OTbl, test_id, {6, 6}, []),
    {[], _, {1, 2}} = open_mem_tbl_take(OTbl, test_id, {1, 2}, []),

    ets:delete(Tid),
    ets:delete(OTbl),

    ok.

closed_mem_tbl_take_test() ->
    CTbl = ra_log_closed_mem_tables,
    _ = ets:new(CTbl, [named_table, bag]),
    Tid1 = ets:new(test_id, []),
    Tid2 = ets:new(test_id, []),
    M1 = erlang:unique_integer([monotonic, positive]),
    M2 = erlang:unique_integer([monotonic, positive]),
    true = ets:insert(CTbl, {test_id, M1, 5, 7, Tid1}),
    true = ets:insert(CTbl, {test_id, M2, 8, 10, Tid2}),
    Entries1 = [{5, 2, "5"}, {6, 2, "6"}, {7, 2, "7"}],
    Entries2 = [{8, 2, "8"}, {9, 2, "9"}, {10, 2, "10"}],
    % seed the mem tables
    [ets:insert(Tid1, E) || E <- Entries1],
    [ets:insert(Tid2, E) || E <- Entries2],

    {Entries1, _, undefined} = closed_mem_tbl_take(CTbl, test_id, {5, 7}, []),
    {Entries2, _, undefined} = closed_mem_tbl_take(CTbl, test_id, {8, 10}, []),
    {[{9, 2, "9"}], _, undefined} = closed_mem_tbl_take(CTbl, test_id, {9, 9}, []),
    ok.

compact_seg_refs_test() ->
    % {From, To, File}
    Refs = [{10, 100, "2"}, {10, 75, "2"}, {10, 50, "2"}, {1, 9, "1"}],
    [{10, 100, "2"}, {1, 9, "1"}] = compact_seg_refs(Refs),
    ok.

-endif.
