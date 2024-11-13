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
         num_open_segments/1,
         update_first_index/2,
         fold/5,
         sparse_read/3,
         read_plan/2,
         exec_read_plan/5,
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

-type segment_ref() :: {From :: ra_index(), To :: ra_index(),
                        File :: string()}.
-record(?STATE, {cfg :: #cfg{},
                 % first_index = 0 :: ra_index(),
                 last_index = 0 :: ra:index(),
                 segment_refs = [] :: [segment_ref()],
                 open_segments :: ra_flru:state()
                }).

-opaque state() :: #?STATE{}.
-type read_plan() :: [{BaseName :: file:filename_all(), [ra:index()]}].


-export_type([
              state/0,
              read_plan/0
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
init(UId, Dir, MaxOpen, AccessPattern, SegRefs, #{}, Counter)
  when is_binary(UId) ->
    Cfg = #cfg{uid = UId,
               counter = Counter,
               directory = Dir,
               access_pattern = AccessPattern},
    FlruHandler = fun ({_, Seg}) ->
                          _ = ra_log_segment:close(Seg),
                          decr_counter(Cfg, ?C_RA_LOG_OPEN_SEGMENTS, 1)
                  end,
    LastIdx = case SegRefs of
                  [{_, L, _} | _] ->
                      L;
                  _ ->
                      0
              end,
    #?STATE{cfg = Cfg,
            open_segments = ra_flru:new(MaxOpen, FlruHandler),
            % first_index = FirstIdx,
            last_index = LastIdx,
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
handle_log_update({ra_log_update, From, _FstIdx, SegRefs},
                  #?STATE{open_segments = Open0} = State) ->
    Open = ra_flru:evict_all(Open0),
    case From of
        undefined -> ok;
        _ ->
            %% reply to the updater process
            From ! ra_log_update_processed
    end,
    State#?MODULE{segment_refs = SegRefs,
                  % first_index = FstIdx,
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
                          % first_index = FstIdx,
                          segment_refs = Active},
             Obsolete}
    end.

-spec segment_refs(state()) -> [segment_ref()].
segment_refs(#?STATE{segment_refs = SegmentRefs}) ->
    SegmentRefs.

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
    {Open, _, SegC, Entries} = (catch segment_sparse_read(State, Indexes, Entries0)),
    ok = incr_counter(Cfg, ?C_RA_LOG_READ_SEGMENT, SegC),
    {Entries, State#?MODULE{open_segments = Open}}.

-spec read_plan(state(), [ra_index()]) -> read_plan().
read_plan(#?STATE{segment_refs = SegRefs}, Indexes) ->
    %% TODO: add counter for number of read plans requested
    segment_read_plan(SegRefs, Indexes, []).

-spec exec_read_plan(file:filename_all(), read_plan(), undefined | ra_flru:state(),
                     TransformFun :: fun(),
                     #{ra_index() => Command :: term()}) ->
    {#{ra_index() => Command :: term()}, ra_flru:state()}.
exec_read_plan(Dir, Plan, undefined, TransformFun, Acc0) ->
    Open = ra_flru:new(1, fun({_, Seg}) -> ra_log_segment:close(Seg) end),
    exec_read_plan(Dir, Plan, Open, TransformFun, Acc0);
exec_read_plan(Dir, Plan, Open0, TransformFun, Acc0)
  when is_list(Plan) ->
    Fun = fun (I, T, B, Acc) ->
                  E = TransformFun(I, T, binary_to_term(B)),
                  Acc#{I => E}
          end,
    lists:foldl(
      fun ({Idxs, BaseName}, {Acc1, Open1}) ->
              {Seg, Open} = get_segment_ext(Dir, Open1, BaseName),
              {_, Acc} = ra_log_segment:read_sparse(Seg, Idxs, Fun, Acc1),
              {Acc, Open}
      end, {Acc0, Open0}, Plan).

-spec fetch_term(ra_index(), state()) -> {option(ra_index()), state()}.
fetch_term(Idx, #?STATE{cfg = #cfg{} = Cfg} = State0) ->
    incr_counter(Cfg, ?C_RA_LOG_FETCH_TERM, 1),
    segment_term_query(Idx, State0).

%% LOCAL

segment_read_plan(_RegRefs, [], Acc) ->
    lists:reverse(Acc);
segment_read_plan([], _Indexes, Acc) ->
    %% not all indexes were found
    lists:reverse(Acc);
segment_read_plan([{To, From, Fn} | SegRefs], Indexes, Acc) ->
    %% TODO: address unnecessary allocation here
    Range = {To, From},
    case sparse_read_split(fun (I) ->
                                   ra_range:in(I, Range)
                           end, Indexes, []) of
        {[], _} ->
            segment_read_plan(SegRefs, Indexes, Acc);
        {Idxs, Rem} ->
            segment_read_plan(SegRefs, Rem, [{Idxs, Fn} | Acc])
    end.

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

segrefs_to_read(From0, To0, _SegRefs, Acc)
  when To0 < From0 ->
    Acc;
segrefs_to_read(From0, To0, [{SStart, SEnd, FileName} | SegRefs], Acc)
  when SStart =< To0 andalso
       SEnd >= From0 ->
    %% TODO: use ra_range:range_overlap/2 here?
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
                                             fun (I, T, B, Acc) ->
                                                     [{I, T, binary_to_term(B)} | Acc]
                                             end,
                                             []),
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

get_segment_ext(Dir, Open0, Fn) ->
    case ra_flru:fetch(Fn, Open0) of
        {ok, S, Open1} ->
            {S, Open1};
        error ->
            AbsFn = filename:join(Dir, Fn),
            case ra_log_segment:open(AbsFn,
                                     #{mode => read,
                                       access_pattern => random})
            of
                {ok, S} ->
                    {S, ra_flru:insert(Fn, S, Open0)};
                {error, Err} ->
                    exit({ra_log_failed_to_open_segment, Err,
                          AbsFn})
            end
    end.

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
