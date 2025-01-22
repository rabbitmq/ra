%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term Broadcom refers to Broadcom Inc. and/or its subsidiaries.
%%
-module(ra_log_reader).

-compile(inline_list_funcs).

-export([
         init/5,
         init/7,
         close/1,
         update_segments/2,
         handle_log_update/2,
         segment_refs/1,
         segment_ref_count/1,
         range/1,
         num_open_segments/1,
         update_first_index/2,
         fold/5,
         sparse_read/3,
         read_plan/2,
         exec_read_plan/6,
         fetch_term/2
         ]).

-include("ra.hrl").

-define(STATE, ?MODULE).

-type access_pattern() :: sequential | random.
%% holds static or rarely changing fields
-record(cfg, {uid :: ra_uid(),
              counter :: undefined | counters:counters_ref(),
              directory :: file:filename(),
              access_pattern = random :: access_pattern()
             }).

-type segment_ref() :: ra_log:segment_ref().
-record(?STATE, {cfg :: #cfg{},
                 range :: ra_range:range(),
                 segment_refs :: ra_lol:state(),
                 open_segments :: ra_flru:state()
                }).

-opaque state() :: #?STATE{}.
-type read_plan() :: [{BaseName :: file:filename_all(), [ra:index()]}].
-type read_plan_options() :: #{access_pattern => random | sequential,
                               file_advise => ra_log_segment:posix_file_advise()}.


-export_type([
              state/0,
              read_plan/0,
              read_plan_options/0
             ]).

%% PUBLIC

-spec init(ra_uid(), file:filename(), non_neg_integer(),
           [segment_ref()], ra_system:names()) -> state().
init(UId, Dir, MaxOpen, SegRefs, Names) ->
    init(UId, Dir, MaxOpen, random, SegRefs, Names, undefined).

-spec init(ra_uid(), file:filename(), non_neg_integer(),
           access_pattern(),
           [segment_ref()], ra_system:names(),
           undefined | counters:counters_ref()) -> state().
init(UId, Dir, MaxOpen, AccessPattern, SegRefs0, #{}, Counter)
  when is_binary(UId) ->
    Cfg = #cfg{uid = UId,
               counter = Counter,
               directory = Dir,
               access_pattern = AccessPattern},
    FlruHandler = fun ({_, Seg}) ->
                          _ = ra_log_segment:close(Seg),
                          decr_counter(Cfg, ?C_RA_LOG_OPEN_SEGMENTS, 1)
                  end,
    SegRefs = compact_segrefs(SegRefs0, []),
    Range = case SegRefs of
                  [{{_, L}, _} | _] ->
                      {{F, _}, _} = lists:last(SegRefs),
                      ra_range:new(F, L);
                  _ ->
                      undefined
              end,
    SegRefsRev = lists:reverse(SegRefs),
    #?STATE{cfg = Cfg,
            open_segments = ra_flru:new(MaxOpen, FlruHandler),
            range = Range,
            segment_refs =
                ra_lol:from_list(fun seg_ref_gt/2, SegRefsRev)}.

seg_ref_gt({{Start, _}, Fn1}, {{_, End}, Fn2}) ->
    Start > End andalso Fn1 > Fn2.

-spec close(state()) -> ok.
close(#?STATE{open_segments = Open}) ->
    _ = ra_flru:evict_all(Open),
    ok.

-spec update_segments([segment_ref()], state()) -> state().
update_segments(NewSegmentRefs,
                #?STATE{open_segments = Open0,
                        segment_refs = SegRefs0} = State) ->

    SegmentRefs0 = ra_lol:to_list(SegRefs0),
    SegmentRefsComp = compact_segrefs(NewSegmentRefs, SegmentRefs0),
    SegmentRefsCompRev = lists:reverse(SegmentRefsComp ),
    SegRefs = ra_lol:from_list(fun seg_ref_gt/2, SegmentRefsCompRev),
    Range = case SegmentRefsComp of
                [{{_, L}, _} | _] ->
                    [{{F, _}, _} | _] = SegmentRefsCompRev,
                    ra_range:new(F, L);
                _ ->
                    undefined
            end,
    %% check if any of the updated segrefs refer to open segments
    %% we close these segments so that they can be re-opened with updated
    %% indexes if needed
    Open = lists:foldl(fun ({_, Fn}, Acc0) ->
                               case ra_flru:evict(Fn, Acc0) of
                                   {_, Acc} -> Acc;
                                   error -> Acc0
                               end
                       end, Open0, NewSegmentRefs),
    State#?MODULE{segment_refs = SegRefs,
                  range = Range,
                  open_segments = Open}.

-spec handle_log_update({ra_log_update, undefined | pid(), ra_index(),
                         [segment_ref()]}, state()) -> state().
handle_log_update({ra_log_update, From, _FstIdx, SegRefs},
                  #?STATE{open_segments = Open0} = State) ->
    Open = ra_flru:evict_all(Open0),
    case From of
        undefined -> ok;
        _ ->
            %% reply to the updater process
            From ! ra_log_update_processed
    end,
    State#?MODULE{segment_refs = ra_lol:from_list(fun seg_ref_gt/2,
                                                  lists:reverse(SegRefs)),
                  open_segments = Open}.

-spec update_first_index(ra_index(), state()) ->
    {state(), [segment_ref()]}.
update_first_index(FstIdx, #?STATE{segment_refs = SegRefs0,
                                   open_segments = OpenSegs0} = State) ->
    %% TODO: refactor this so that ra_lol just returns plain lists on both sides?
    case ra_lol:takewhile(fun({{_, To}, _}) ->
                                  To >= FstIdx
                          end, SegRefs0) of
        {Active, Obsolete0} ->
            case ra_lol:len(Obsolete0) of
                0 ->
                    {State, []};
                _ ->
                    Obsolete = ra_lol:to_list(Obsolete0),
                    ObsoleteKeys = [K || {_, K} <- Obsolete],
                    % close any open segments
                    OpenSegs = lists:foldl(fun (K, OS0) ->
                                                   case ra_flru:evict(K, OS0) of
                                                       {_, OS} -> OS;
                                                       error -> OS0
                                                   end
                                           end, OpenSegs0, ObsoleteKeys),
                    {State#?STATE{open_segments = OpenSegs,
                                  segment_refs = ra_lol:from_list(fun seg_ref_gt/2,
                                                                  lists:reverse(Active))},
                     Obsolete}
            end
    end.

-spec segment_refs(state()) -> [segment_ref()].
segment_refs(#?STATE{segment_refs = SegmentRefs}) ->
    ra_lol:to_list(SegmentRefs).

-spec segment_ref_count(state()) -> non_neg_integer().
segment_ref_count(#?STATE{segment_refs = SegmentRefs}) ->
    ra_lol:len(SegmentRefs).

-spec range(state()) -> ra_range:range().
range(#?STATE{range = Range}) ->
    Range.

-spec num_open_segments(state()) -> non_neg_integer().
num_open_segments(#?STATE{open_segments = Open}) ->
     ra_flru:size(Open).

-spec fold(ra_index(), ra_index(), fun(), term(), state()) ->
    {state(), term()}.
fold(FromIdx, ToIdx, Fun, Acc,
    #?STATE{cfg = #cfg{} = Cfg} = State0)
  when ToIdx >= FromIdx ->
    ok = incr_counter(Cfg, ?C_RA_LOG_READ_SEGMENT, ToIdx - FromIdx + 1),
    segment_fold(State0, FromIdx, ToIdx, Fun, Acc);
fold(_FromIdx, _ToIdx, _Fun, Acc, #?STATE{} = State) ->
    {State, Acc}.

-spec sparse_read(state(), [ra_index()], [log_entry()]) ->
    {[log_entry()], state()}.
sparse_read(#?STATE{cfg = #cfg{} = Cfg} = State, Indexes, Entries0) ->
    {Open, SegC, Entries} = (catch segment_sparse_read(State, Indexes, Entries0)),
    ok = incr_counter(Cfg, ?C_RA_LOG_READ_SEGMENT, SegC),
    {Entries, State#?MODULE{open_segments = Open}}.

-spec read_plan(state(), [ra_index()]) -> read_plan().
read_plan(#?STATE{segment_refs = SegRefs}, Indexes) ->
    %% TODO: add counter for number of read plans requested
    segment_read_plan(SegRefs, Indexes, []).

-spec exec_read_plan(file:filename_all(),
                     read_plan(),
                     undefined | ra_flru:state(),
                     TransformFun :: fun((ra_index(), ra_term(), binary()) -> term()),
                     read_plan_options(),
                     #{ra_index() => Command :: term()}) ->
    {#{ra_index() => Command :: term()}, ra_flru:state()}.
exec_read_plan(Dir, Plan, undefined, TransformFun, Options, Acc0) ->
    Open = ra_flru:new(1, fun({_, Seg}) -> ra_log_segment:close(Seg) end),
    exec_read_plan(Dir, Plan, Open, TransformFun, Options, Acc0);
exec_read_plan(Dir, Plan, Open0, TransformFun, Options, Acc0)
  when is_list(Plan) ->
    Fun = fun (I, T, B, Acc) ->
                  E = TransformFun(I, T, binary_to_term(B)),
                  Acc#{I => E}
          end,
    lists:foldl(
      fun ({Idxs, BaseName}, {Acc1, Open1}) ->
              {Seg, Open2} = get_segment_ext(Dir, Open1, BaseName, Options),
              case ra_log_segment:read_sparse(Seg, Idxs, Fun, Acc1) of
                  {ok, _, Acc} ->
                      {Acc, Open2};
                  {error, modified} ->
                      %% if the segment has been modified since it was opened
                      %% it is not safe to attempt the read as the read plan
                      %% may refer to indexes that weren't in the segment at
                      %% that time. In this case we evict all segments and
                      %% re-open what we need.
                      {_, Open3} = ra_flru:evict(BaseName, Open2),
                      {SegNew, Open} = get_segment_ext(Dir, Open3, BaseName, Options),
                      %% at this point we can read without checking for modification
                      %% as the read plan would have been created before we
                      %% read the index from the segment
                      {ok, _, Acc} = ra_log_segment:read_sparse_no_checks(
                                       SegNew, Idxs, Fun, Acc1),
                      {Acc, Open}
              end
      end, {Acc0, Open0}, Plan).

-spec fetch_term(ra_index(), state()) -> {option(ra_index()), state()}.
fetch_term(Idx, #?STATE{cfg = #cfg{} = Cfg} = State0) ->
    incr_counter(Cfg, ?C_RA_LOG_FETCH_TERM, 1),
    segment_term_query(Idx, State0).

%% LOCAL

segment_read_plan(_SegRefs, [], Acc) ->
    lists:reverse(Acc);
segment_read_plan(SegRefs, [Idx | _] = Indexes, Acc) ->
    case ra_lol:search(seg_ref_search_fun(Idx), SegRefs) of
        {{Range, Fn}, Cont} ->
            case sparse_read_split(fun (I) ->
                                           ra_range:in(I, Range)
                                   end, Indexes, []) of
                {[], _} ->
                    segment_read_plan(Cont, Indexes, Acc);
                {Idxs, Rem} ->
                    segment_read_plan(Cont, Rem, [{Idxs, Fn} | Acc])
            end;
        undefined ->
            %% not found
            lists:reverse(Acc)
    end.

seg_ref_search_fun(Idx) ->
    fun({{Start, End}, _}) ->
            if Idx > End -> higher;
               Idx < Start -> lower;
               true -> equal
            end
    end.

segment_term_query(Idx, #?MODULE{segment_refs = SegRefs,
                                 cfg = Cfg,
                                 open_segments = OpenSegs} = State) ->
    {Result, Open} = segment_term_query0(Idx, SegRefs, OpenSegs, Cfg),
    {Result, State#?MODULE{open_segments = Open}}.

segment_term_query0(Idx, SegRefs, Open0,
                    #cfg{directory = Dir,
                         access_pattern = AccessPattern} = Cfg) ->
    case ra_lol:search(seg_ref_search_fun(Idx), SegRefs) of
        {{_Range, Fn}, _Cont} ->
            case ra_flru:fetch(Fn, Open0) of
                {ok, Seg, Open} ->
                    Term = ra_log_segment:term_query(Seg, Idx),
                    {Term, Open};
                error ->
                    AbsFn = filename:join(Dir, Fn),
                    {ok, Seg} = ra_log_segment:open(AbsFn,
                                                    #{mode => read,
                                                      access_pattern => AccessPattern}),

                    incr_counter(Cfg, ?C_RA_LOG_OPEN_SEGMENTS, 1),
                    Term = ra_log_segment:term_query(Seg, Idx),
                    {Term, ra_flru:insert(Fn, Seg, Open0)}
            end;
        undefined ->
            {undefined, Open0}
    end.

segment_fold_plan(_SegRefs, undefined, Acc) ->
    Acc;
segment_fold_plan(SegRefs, {_ReqStart, ReqEnd} = ReqRange, Acc) ->
    case ra_lol:search(seg_ref_search_fun(ReqEnd), SegRefs) of
        {{Range, Fn}, Cont} ->
            This = ra_range:overlap(ReqRange, Range),
            ReqRem = case ra_range:subtract(This, ReqRange) of
                         [] ->
                             undefined;
                         [Rem] ->
                             Rem
                     end,
            segment_fold_plan(Cont, ReqRem, [{This, Fn} | Acc]);
        undefined ->
            %% not found
            Acc
    end.

segment_fold(#?STATE{segment_refs = SegRefs,
                     open_segments = OpenSegs,
                     cfg = Cfg} = State,
             RStart, REnd, Fun, Acc) ->
    Plan = segment_fold_plan(SegRefs, {RStart, REnd}, []),
    {Op, A} =
        lists:foldl(
          fun ({{Start, End}, Fn}, {Open0, Ac0}) ->
                  {Seg, Open} = get_segment(Cfg, Open0, Fn),
                  {Open, ra_log_segment:fold(Seg, Start, End,
                                             fun binary_to_term/1,
                                             Fun,
                                             Ac0)}
          end, {OpenSegs, Acc}, Plan),
    {State#?MODULE{open_segments = Op}, A}.


segment_sparse_read(#?STATE{open_segments = Open}, [], Entries0) ->
    {Open, 0, Entries0};
segment_sparse_read(#?STATE{segment_refs = SegRefs,
                            open_segments = OpenSegs,
                            cfg = Cfg}, Indexes, Entries0) ->
    Plan = segment_read_plan(SegRefs, Indexes, []),
    lists:foldl(
      fun ({Idxs, Fn}, {Open0, C, En0}) ->
              {Seg, Open} = get_segment(Cfg, Open0, Fn),
              {ok, ReadSparseCount, Entries} =
                  ra_log_segment:read_sparse(Seg, Idxs,
                                             fun (I, T, B, Acc) ->
                                                     [{I, T, binary_to_term(B)} | Acc]
                                             end, []),
              {Open, C +  ReadSparseCount, lists:reverse(Entries, En0)}
      end, {OpenSegs, 0, Entries0}, Plan).

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

get_segment_ext(Dir, Open0, Fn, Options) ->
    case ra_flru:fetch(Fn, Open0) of
        {ok, S, Open1} ->
            {S, Open1};
        error ->
            AbsFn = filename:join(Dir, Fn),
            case ra_log_segment:open(AbsFn,
                                     Options#{mode => read})
            of
                {ok, S} ->
                    {S, ra_flru:insert(Fn, S, Open0)};
                {error, Err} ->
                    exit({ra_log_failed_to_open_segment, Err,
                          AbsFn})
            end
    end.

compact_segrefs(New, Cur) ->
    %% all are in descending order
    lists:foldr(
      fun
          (S, []) ->
              [S];
          ({{Start, _}, _} = SegRef, Prev) ->
              [SegRef | limit(Start, Prev)]
      end, Cur, New).

limit(_LimitIdx, []) ->
    [];
limit(LimitIdx, [{PrevRange, PrevFn} | PrevRem]) ->
    case ra_range:limit(LimitIdx, PrevRange) of
        undefined ->
            limit(LimitIdx, PrevRem);
        NewPrevRange ->
            [{NewPrevRange, PrevFn} | PrevRem]
    end.

incr_counter(#cfg{counter = Cnt}, Ix, N) when Cnt =/= undefined ->
    counters:add(Cnt, Ix, N);
incr_counter(#cfg{counter = undefined}, _, _) ->
    ok.

decr_counter(#cfg{counter = Cnt}, Ix, N) when Cnt =/= undefined ->
    counters:sub(Cnt, Ix, N);
decr_counter(#cfg{counter = undefined}, _, _) ->
    ok.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

compact_seg_refs_test() ->
    NewRefs = [{{10, 100}, "2"}],
    PrevRefs = [{{10, 75}, "2"}, {{1, 9}, "1"}],
    ?assertEqual([{{10, 100}, "2"}, {{1, 9}, "1"}],
                 compact_segrefs(NewRefs, PrevRefs)).

compact_segref_3_test() ->
    Data = [
            {{2, 7}, "B"},
            %% this entry has overwritten the prior two
            {{5, 10}, "B"},
            {{1, 4}, "A"}
           ],
    Res = compact_segrefs(Data, []),
    ?assertMatch([{{2, 7}, "B"},
                  {{1, 1}, "A"}], Res),
    ok.

compact_segref_2_test() ->
    Data = [
            {{80, 89}, "80"},
            %% this entry has overwritten the prior two
            {{56, 79}, "71"},
            {{70, 85}, "70"},
            {{60, 69}, "60"},
            {{50, 59}, "50"}
           ],
    Res = compact_segrefs(Data, []),
    ?assertMatch([{{80, 89}, "80"},
                  {{56, 79}, "71"},
                  {{50, 55}, "50"}
                 ], Res),
    ok.

compact_segref_1_test() ->
    Data = [
            {{80, 89}, "80"},
            %% this entry has overwritten the prior one
            {{70, 79}, "71"},
            {{70, 85}, "70"},
            %% partial overwrite
            {{65, 69}, "65"},
            {{60, 69}, "60"},
            {{50, 59}, "50"},
            {{40, 49}, "40"}
           ],

    Res = compact_segrefs(Data, [
                                 {{30, 39}, "30"},
                                 {{20, 29}, "20"}
                                ]),

    %% overwritten entry is no longer there
    %% and the segment prior to the partial overwrite has been limited
    %% to provide a continuous range
    ?assertMatch([{{80, 89}, "80"},
                  {{70, 79}, "71"},
                  {{65, 69}, "65"},
                  {{60, 64}, "60"},
                  {{50, 59}, "50"},
                  {{40, 49}, "40"},
                  {{30, 39}, "30"},
                  {{20, 29}, "20"}
                 ], Res),
    ok.


segrefs_to_read_test() ->

    SegRefs = ra_lol:from_list(
                fun seg_ref_gt/2,
                lists:reverse(
                  compact_segrefs(
                    [{{412,499},"00000006.segment"},
                     {{284,411},"00000005.segment"},
                     %% this segment got overwritten
                     {{284,500},"00000004.segment"},
                     {{200,285},"00000003.segment"},
                     {{128,255},"00000002.segment"},
                     {{0,127},"00000001.segment"}], []))),


    ?assertEqual([{{199, 199}, "00000002.segment"},
                  {{200, 283}, "00000003.segment"},
                  {{284, 411}, "00000005.segment"},
                  {{412, 499}, "00000006.segment"}],
                 segment_fold_plan(SegRefs, {199, 499}, [])),

    %% out of range
    ?assertEqual([], segment_fold_plan(SegRefs, {500, 500}, [])),
    ?assertEqual([
                  {{127,127},"00000001.segment"},
                  {{128,128},"00000002.segment"}
                 ],
                 segment_fold_plan(SegRefs, {127, 128}, [])),
    ok.

-endif.
