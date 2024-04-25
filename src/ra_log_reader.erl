%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term Broadcom refers to Broadcom Inc. and/or its subsidiaries.
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
         fold/5,
         sparse_read/3,
         fetch_term/2,
         delete_closed_mem_table_object/2,
         closed_mem_tables/1,
         open_mem_table_lookup/1,
         range_overlap/4
         ]).

-include("ra.hrl").

-define(STATE, ?MODULE).

-type access_pattern() :: sequential | random.
%% holds static or rarely changing fields
-record(cfg, {uid :: ra_uid(),
              counter :: undefined | counters:counters_ref(),
              directory :: file:filename(),
              open_mem_tbls :: ets:tid(),
              closed_mem_tbls :: ets:tid(),
              access_pattern = random :: access_pattern()
             }).

-type segment_ref() :: {From :: ra_index(), To :: ra_index(),
                        File :: string()}.
-record(?STATE, {cfg :: #cfg{},
                 first_index = 0 :: ra_index(),
                 segment_refs = [] :: [segment_ref()],
                 open_segments :: ra_flru:state()
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
    Cfg = #cfg{uid = UId,
               counter = Counter,
               directory = Dir,
               open_mem_tbls = ets:whereis(OpnMemTbls),
               closed_mem_tbls = ets:whereis(ClsdMemTbls),
               access_pattern = AccessPattern},
    FlruHandler = fun ({_, Seg}) ->
                          _ = ra_log_segment:close(Seg),
                          decr_counter(Cfg, ?C_RA_LOG_OPEN_SEGMENTS, 1)
                  end,
    #?STATE{cfg = Cfg,
            open_segments = ra_flru:new(MaxOpen, FlruHandler),
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
    SegmentRefs = compact_seg_refs(NewSegmentRefs, SegmentRefs0),
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
update_first_index(FstIdx, #?STATE{segment_refs = SegRefs0,
                                   open_segments = OpenSegs0} = State) ->
    case lists:partition(fun({_, To, _})
                               when To >= FstIdx -> true;
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
                          first_index = FstIdx,
                          segment_refs = Active},
             Obsolete}
    end.

-spec segment_refs(state()) -> [segment_ref()].
segment_refs(#?STATE{segment_refs = SegmentRefs}) ->
    SegmentRefs.

-spec num_open_segments(state()) -> non_neg_integer().
num_open_segments(#?STATE{open_segments = Open}) ->
     ra_flru:size(Open).

mem_tbl_fold(_Tid, From, To, _Fun, Acc)
  when From > To ->
    Acc;
mem_tbl_fold(Tid, From, To, Fun, Acc0) ->
    [Entry] = ets:lookup(Tid, From),
    Acc = Fun(Entry, Acc0),
    mem_tbl_fold(Tid, From+1, To, Fun, Acc).


-spec fold(ra_index(), ra_index(), fun(), term(), state()) ->
    {state(), term()}.
fold(FromIdx, ToIdx, Fun, Acc,
    #?STATE{cfg = #cfg{} = Cfg} = State)
  when ToIdx >= FromIdx ->
    Plan = read_plan(Cfg, FromIdx, ToIdx),
    lists:foldl(
      fun ({ets, Tid, CIx, From, To}, {S, Ac}) ->
              ok = incr_counter(Cfg,  CIx, To - From + 1),
              {S, mem_tbl_fold(Tid, From, To, Fun, Ac)};
          ({segments, From, To}, {S, Ac}) ->
              ok = incr_counter(Cfg, ?C_RA_LOG_READ_SEGMENT, To - From + 1),
              segment_fold(S, From, To, Fun, Ac)
      end, {State, Acc}, Plan);
fold(_FromIdx, _ToIdx, _Fun, Acc,
     #?STATE{} = State) ->
    {State, Acc}.

-spec sparse_read(state(), [ra_index()], [log_entry()]) ->
    {[log_entry()], state()}.
sparse_read(#?STATE{cfg = #cfg{} = Cfg} = State, Indexes0, Entries0) ->
    try open_mem_tbl_sparse_read(Cfg, Indexes0, Entries0) of
        {Entries1, OpenC, []} ->
            ok = incr_counter(Cfg, ?C_RA_LOG_READ_OPEN_MEM_TBL, OpenC),
            {Entries1, State};
        {Entries1, OpenC, Rem1} ->
            ok = incr_counter(Cfg, ?C_RA_LOG_READ_OPEN_MEM_TBL, OpenC),
            try closed_mem_tbl_sparse_read(Cfg, Rem1, Entries1) of
                {Entries2, ClosedC, []} ->
                    ok = incr_counter(Cfg, ?C_RA_LOG_READ_CLOSED_MEM_TBL, ClosedC),
                    {Entries2, State};
                {Entries2, ClosedC, Rem2} ->
                    ok = incr_counter(Cfg, ?C_RA_LOG_READ_CLOSED_MEM_TBL, ClosedC),
                    {Open, _, SegC, Entries} = (catch segment_sparse_read(State, Rem2, Entries2)),
                    ok = incr_counter(Cfg, ?C_RA_LOG_READ_SEGMENT, SegC),
                    {Entries, State#?MODULE{open_segments = Open}}
            catch _:_ ->
                      sparse_read(State, Indexes0, Entries0)
            end
    catch _:_ ->
              %% table was most likely concurrently deleted
              %% try again
              %% TODO: avoid infinite loop
              sparse_read(State, Indexes0, Entries0)
    end.

-spec fetch_term(ra_index(), state()) -> {option(ra_index()), state()}.
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
                         access_pattern = AccessPattern} = Cfg)
  when Idx >= From andalso Idx =< To ->
    case ra_flru:fetch(Filename, Open0) of
        {ok, Seg, Open} ->
            Term = ra_log_segment:term_query(Seg, Idx),
            {Term, Open};
        error ->
            AbsFn = filename:join(Dir, Filename),
            {ok, Seg} = ra_log_segment:open(AbsFn,
                                            #{mode => read,
                                              access_pattern => AccessPattern}),

            incr_counter(Cfg, ?C_RA_LOG_OPEN_SEGMENTS, 1),
            Term = ra_log_segment:term_query(Seg, Idx),
            {Term, ra_flru:insert(Filename, Seg, Open0)}
    end;
segment_term_query0(Idx, [_ | Tail], Open, Cfg) ->
    segment_term_query0(Idx, Tail, Open, Cfg);
segment_term_query0(_Idx, [], Open, _) ->
    {undefined, Open}.

range_overlap(F, L, S, E)
  when E >= F andalso
       L >= S andalso
       F =< L ->
    X = max(F, S),
    {X, min(L, E), F, X - 1};
range_overlap(F, L, _, _) ->
    {undefined, F, L}.

read_plan(#cfg{uid = UId,
               open_mem_tbls = OpenTbl,
               closed_mem_tbls = ClosedTbl},
          FromIdx, ToIdx) ->
    Acc0 = case ets:lookup(OpenTbl, UId) of
               [{_, TStart, TEnd, Tid}] ->
                   case range_overlap(FromIdx, ToIdx, TStart, TEnd) of
                       {undefined, _, _} ->
                           {FromIdx, ToIdx, []};
                       {S, E, F, T} ->
                           {F, T,
                            [{ets, Tid, ?C_RA_LOG_READ_OPEN_MEM_TBL, S, E}]}
                   end;
               _ ->
                   {FromIdx, ToIdx, []}
           end,

    {RemF, RemL, Plan} =
        case closed_mem_tables(ClosedTbl, UId) of
            [] ->
                Acc0;
            Tables ->
                lists:foldl(
                  fun({_, _, S, E, Tid}, {F, T, Plan} = Acc) ->
                          case range_overlap(F, T, S, E) of
                              {undefined, _, _} ->
                                  Acc;
                              {S1, E1, F1, T1} ->
                                  {F1, T1,
                                   [{ets, Tid, ?C_RA_LOG_READ_CLOSED_MEM_TBL, S1, E1}
                                    | Plan]}
                          end
                  end, Acc0, Tables)
        end,
    case RemF =< RemL of
        true ->
            [{segments, RemF, RemL} | Plan];
        false ->
            Plan
    end.

open_mem_tbl_sparse_read(#cfg{uid = UId,
                              open_mem_tbls = OpenTbl},
                         Indexes, Acc0) ->
    case ets:lookup(OpenTbl, UId) of
        [{_, TStart, TEnd, Tid}] ->
            mem_tbl_sparse_read(Indexes, TStart, TEnd, Tid, 0, Acc0);
        [] ->
            {Acc0, 0, Indexes}
    end.

closed_mem_tbl_sparse_read(#cfg{uid = UId,
                                closed_mem_tbls = ClosedTbl}, Indexes, Acc0) ->
    case closed_mem_tables(ClosedTbl, UId) of
        [] ->
            {Acc0, 0, Indexes};
        Tables ->
            lists:foldl(fun({_, _, TblSt, TblEnd, Tid}, {Ac, Num, Idxs}) ->
                                mem_tbl_sparse_read(Idxs, TblSt, TblEnd, Tid, Num, Ac)
                        end, {Acc0, 0, Indexes}, Tables)
    end.

mem_tbl_sparse_read([I | Rem], TblStart, TblEnd, Tid, C, Entries0)
  when I >= TblStart andalso I =< TblEnd ->
    [Entry] = ets:lookup(Tid, I),
    mem_tbl_sparse_read(Rem, TblStart, TblEnd, Tid, C + 1, [Entry | Entries0]);
mem_tbl_sparse_read(Rem, _TblStart, _TblEnd, _Tid, C, Entries0) ->
    {Entries0, C, Rem}.

segrefs_to_read(From0, To0, _SegRefs, Acc)
  when To0 < From0 ->
    Acc;
segrefs_to_read(From0, To0, [{SStart, SEnd, FileName} | SegRefs], Acc)
  when SStart =< To0 andalso
       SEnd >= From0 ->
    From = max(From0, SStart),
    To = min(To0, SEnd),
    Spec = {From, To, FileName},
    segrefs_to_read(From0, SStart - 1, SegRefs, [Spec | Acc]);
segrefs_to_read(From0, To0, [_ | SegRefs], Acc) ->
    segrefs_to_read(From0, To0, SegRefs, Acc).

segment_fold(#?STATE{segment_refs = SegRefs,
                     open_segments = OpenSegs,
                     cfg = Cfg} = State,
             RStart, REnd, Fun, Acc) ->
    SegRefsToReadFrom = segrefs_to_read(RStart, REnd, SegRefs, []),
    {Op, A} =
        lists:foldl(
          fun ({From, To, Fn}, {Open0, Ac0}) ->
                  {Seg, Open} = get_segment(Cfg, Open0, Fn),
                  {Open, ra_log_segment:fold(Seg, From, To,
                                             fun binary_to_term/1,
                                             Fun,
                                             Ac0)}
          end, {OpenSegs, Acc}, SegRefsToReadFrom),
    {State#?MODULE{open_segments = Op}, A}.


segment_sparse_read(#?STATE{open_segments = Open}, [], Entries0) ->
    {Open, [], 0, Entries0};
segment_sparse_read(#?STATE{segment_refs = SegRefs,
                            open_segments = OpenSegs,
                            cfg = Cfg}, Indexes, Entries0) ->
    lists:foldl(
      fun(_, {_, [], _, _} = Acc) ->
              %% we're done reading
              throw(Acc);
         ({From, To, Fn}, {Open0, [NextIdx | _] = Idxs, C, En0})
           when NextIdx >= From andalso NextIdx =< To ->
              {Seg, Open} = get_segment(Cfg, Open0, Fn),
              {ReadIdxs, RemIdxs} =
                  sparse_read_split(fun (I) ->
                                            I >= From andalso I =< To
                                    end, Idxs, []),
              {ReadSparseCount, Entries} =
                  ra_log_segment:read_sparse(Seg, ReadIdxs,
                                             fun binary_to_term/1, []),
              {Open, RemIdxs, C +  ReadSparseCount,
               lists:reverse(Entries, En0)};
         (_Segref, Acc) ->
              Acc
      end, {OpenSegs, Indexes, 0, Entries0}, SegRefs).

%% like lists:splitwith but without reversing the accumulator
sparse_read_split(Fun, [E | Rem] = All, Acc) ->
    case Fun(E) of
        true ->
            sparse_read_split(Fun, Rem, [E | Acc]);
        false ->
            {Acc, All}
    end;
sparse_read_split(_Fun, [], Acc) ->
    {Acc, []}.


get_segment(#cfg{directory = Dir,
                 access_pattern = AccessPattern} = Cfg, Open0, Fn) ->
    case ra_flru:fetch(Fn, Open0) of
        {ok, S, Open1} ->
            {S, Open1};
        error ->
            AbsFn = filename:join(Dir, Fn),
            case ra_log_segment:open(AbsFn,
                                     #{mode => read,
                                       access_pattern => AccessPattern})
            of
                {ok, S} ->
                    incr_counter(Cfg, ?C_RA_LOG_OPEN_SEGMENTS, 1),
                    {S, ra_flru:insert(Fn, S, Open0)};
                {error, Err} ->
                    exit({ra_log_failed_to_open_segment, Err,
                          AbsFn})
            end
    end.

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

compact_seg_refs([], PreviousSegRefs) ->
    PreviousSegRefs;
compact_seg_refs(NewSegRefs, []) ->
    NewSegRefs;
compact_seg_refs(NewSegRefs,
                 [{_, _, SegFile} | RemSegRefs] = PreviousSegRefs) ->
    case lists:last(NewSegRefs) of
        {_, _, SegFile} ->
            % update information about the last previously seen segment
            NewSegRefs ++ RemSegRefs;
        _ ->
            NewSegRefs ++ PreviousSegRefs
    end.

incr_counter(#cfg{counter = Cnt}, Ix, N) when Cnt =/= undefined ->
    counters:add(Cnt, Ix, N);
incr_counter(#cfg{counter = undefined}, _, _) ->
    ok.

incr_counter(#cfg{counter = Cnt}, {Ix, N}) when Cnt =/= undefined ->
    counters:add(Cnt, Ix, N);
incr_counter(#cfg{counter = undefined}, _) ->
    ok.

decr_counter(#cfg{counter = Cnt}, Ix, N) when Cnt =/= undefined ->
    counters:sub(Cnt, Ix, N);
decr_counter(#cfg{counter = undefined}, _, _) ->
    ok.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

compact_seg_refs_test() ->
    % {From, To, File}
    NewRefs = [{10, 100, "2"}],
    PrevRefs = [{10, 75, "2"}, {1, 9, "1"}],
    ?assertEqual([{10, 100, "2"}, {1, 9, "1"}], compact_seg_refs(NewRefs, PrevRefs)).

range_overlap_test() ->
    {undefined, 1, 10} = range_overlap(1, 10, 20, 30),
    {undefined, 21, 30} = range_overlap(21, 30, 10, 20),
    {20, 20, 20, 19} = range_overlap(20, 30, 10, 20),
    ?assertEqual({79, 99, 79, 78}, range_overlap(79, 99, 75, 111)),
    ?assertEqual({undefined, 79, 78}, range_overlap(79, 78, 50, 176)),
    ?assertEqual({undefined, 79, 78}, range_overlap(79, 78, 25, 49)),
    % {10, 10} = range_overlap(1, 10, 10, 30),
    % {5, 10} = range_overlap(1, 10, 5, 30),
    % {7, 10} = range_overlap(7, 10, 5, 30),
    ok.

read_plan_test() ->
    UId = <<"this_uid">>,
    OTbl = ra_log_open_mem_tables,
    OpnTbl = ets:new(OTbl, []),
    CTbl = ra_log_closed_mem_tables,
    ClsdTbl = ets:new(CTbl, [bag]),
    M1 = erlang:unique_integer([monotonic, positive]),
    M2 = erlang:unique_integer([monotonic, positive]),

    true = ets:insert(OpnTbl, {UId, 75, 111, OTbl}),
    true = ets:insert(ClsdTbl, {UId, M2, 50, 176, CTbl}),
    true = ets:insert(ClsdTbl, {UId, M1, 25, 49,  CTbl}),
    %% segments 0 - 24
    Cfg = #cfg{uid = UId,
               open_mem_tbls = OpnTbl,
               closed_mem_tbls = ClsdTbl},
    ?debugFmt("Read Plan: ~p~n", [read_plan(Cfg, 0, 100)]),
    ?assertMatch([{segments, 0, 24},
                  {ets, _, _, 25, 49},
                  {ets, _, _, 50, 74},
                  {ets, _, _, 75, 100}],
                 read_plan(Cfg, 0, 100)),

    ?debugFmt("Read Plan: ~p~n", [read_plan(Cfg, 10, 55)]),
    ?assertMatch([{segments, 10, 24},
                  {ets, _, _, 25, 49},
                  {ets, _, _, 50, 55}],
                 read_plan(Cfg, 10, 55)),
    ?assertMatch([
                  {ets, _, _, 79, 99}
                 ],
                 read_plan(Cfg, 79, 99)),
    ok.

segrefs_to_read_test() ->
    SegRefs = [{412,499,"00000005.segment"},
               {284,411,"00000004.segment"},
               {284,310,"00000004b.segment"},
               {200,285,"00000003.segment"},
               {128,255,"00000002.segment"},
               {0,127,"00000001.segment"}],

    ?assertEqual([{199,199,"00000002.segment"},
                  {200,283,"00000003.segment"},
                  {284,411,"00000004.segment"},
                  {412,499,"00000005.segment"}],
                 segrefs_to_read(199, 499, SegRefs, [])),
    ok.

-endif.
