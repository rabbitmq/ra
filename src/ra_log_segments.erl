%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2017-2025 Broadcom. All Rights Reserved. The term Broadcom
%% refers to Broadcom Inc. and/or its subsidiaries.
%% @hidden
-module(ra_log_segments).

-compile(inline_list_funcs).

-include_lib("kernel/include/file.hrl").
-export([
         init/8,
         update_conf/3,
         close/1,
         update_segments/2,
         schedule_compaction/4,
         handle_compaction_result/2,
         segment_refs/1,
         segment_ref_count/1,
         range/1,
         num_open_segments/1,
         update_first_index/2,
         fold/6,
         sparse_read/3,
         read_plan/2,
         exec_read_plan/6,
         fetch_term/2,
         info/1,
         purge_symlinks/2,
         purge_dangling_symlinks/1,
         compaction_conf/1,
         compaction/1
         ]).

-include("ra.hrl").

-define(STATE, ?MODULE).

-define(SYMLINK_KEEPFOR_S, 60).

%% type for configuring automatic major compaction strategies
-type major_compaction_strategy() :: manual |
                                     {num_minors, pos_integer()}.

-type compaction_conf() :: #{max_count => non_neg_integer(),
                             max_size => non_neg_integer(),
                             major_strategy => major_compaction_strategy()}.
%% hardly ever used anymore, the sequential access pattern is only activated
%% during recovery
-type access_pattern() :: sequential | random.
%% holds static or rarely changing fields
-record(cfg, {uid :: ra_uid(),
              log_id = "" :: unicode:chardata(),
              counter :: undefined | counters:counters_ref(),
              directory :: file:filename(),
              access_pattern = random :: access_pattern(),
              compaction_conf :: compaction_conf()
             }).

-type segment_ref() :: ra_log:segment_ref().

-record(?STATE, {cfg :: #cfg{},
                 range :: ra_range:range(),
                 segment_refs :: ra_lol:state(),
                 open_segments :: ra_flru:state(),
                 compaction :: undefined | {major | minor, SnapIdx :: ra:index()},
                 minor_compaction_count = 0 :: non_neg_integer()
                }).

-record(compaction_result,
        {type = minor :: major | minor,
         unreferenced = [] :: [file:filename_all()],
         linked = [] :: [file:filename_all()],
         compacted_segrefs = [] :: [segment_ref()]}).

-opaque state() :: #?STATE{}.
-type read_plan() :: [{BaseName :: file:filename_all(), [ra:index()]}].
-type read_plan_options() :: #{access_pattern => random | sequential,
                               file_advise => ra_log_segment:posix_file_advise(),
                               index_mode => ra_log_segment:index_mode()}.

-export_type([
              state/0,
              read_plan/0,
              read_plan_options/0,
              major_compaction_strategy/0
             ]).

%% PUBLIC

-spec init(ra_uid(), file:filename_all(), non_neg_integer(),
           access_pattern(), [segment_ref()],
           undefined | counters:counters_ref(),
           map(),
           unicode:chardata()) -> state().
init(UId, Dir, MaxOpen, AccessPattern, SegRefs0, Counter, CompConf, LogId)
  when is_binary(UId) andalso
       is_map(CompConf) ->
    Cfg = #cfg{uid = UId,
               log_id = LogId,
               counter = Counter,
               directory = Dir,
               access_pattern = AccessPattern,
               compaction_conf = CompConf},
    FlruHandler = fun ({_, Seg}) ->
                          _ = ra_log_segment:close(Seg),
                          decr_counter(Cfg, ?C_RA_LOG_OPEN_SEGMENTS, 1)
                  end,
    SegRefs = compact_segrefs(SegRefs0, []),
    Range = case SegRefs of
                  [{_, {_, L}} | _] ->
                      {_, {F, _}} = lists:last(SegRefs),
                      ra_range:new(F, L);
                  _ ->
                      undefined
              end,
    reset_counter(Cfg, ?C_RA_LOG_OPEN_SEGMENTS),
    Result = recover_compaction(Dir),
    %% handle_compaction_result/2 will never return an effect here
    %% as no segments got deleted
    State0 = #?STATE{cfg = Cfg,
                     open_segments = ra_flru:new(MaxOpen, FlruHandler),
                     range = Range,
                     segment_refs = ra_lol:from_list(fun seg_ref_gt/2,
                                                     SegRefs)},
    {State, _} = handle_compaction_result(Result, State0),
    State.

-spec close(state()) -> ok.
close(#?STATE{open_segments = Open}) ->
    _ = ra_flru:evict_all(Open),
    ok.


-spec update_conf(non_neg_integer(), sequential | random, state()) ->
    state().
update_conf(MaxOpen, AccessPattern,
            #?STATE{cfg = Cfg,
                    open_segments = Open} = State) ->
    FlruHandler = fun ({_, Seg}) ->
                          _ = ra_log_segment:close(Seg),
                          decr_counter(Cfg, ?C_RA_LOG_OPEN_SEGMENTS, 1)
                  end,
    _ = ra_flru:evict_all(Open),
    State#?STATE{cfg = Cfg#cfg{access_pattern = AccessPattern},
                 open_segments = ra_flru:new(MaxOpen, FlruHandler)}.


-spec update_segments([segment_ref()], state()) ->
    {state(), OverwrittenSegments :: [segment_ref()]}.
update_segments(NewSegmentRefs, #?STATE{open_segments = Open0,
                                        segment_refs = SegRefs0} = State) ->

    SegmentRefs0 = ra_lol:to_list(SegRefs0),
    SegmentRefsComp = compact_segrefs(NewSegmentRefs, SegmentRefs0),
    %% capture segrefs removed by compact_segrefs/2 and delete them
    %% a major compaction will also remove these
    OverwrittenSegments = NewSegmentRefs -- SegmentRefsComp,
    SegRefs = ra_lol:from_list(fun seg_ref_gt/2, SegmentRefsComp),
    Range = case SegmentRefsComp of
                [{_, {_, L}} | _] ->
                    {_, {F, _}} = lists:last(SegmentRefsComp),
                    ra_range:new(F, L);
                _ ->
                    undefined
            end,
    %% check if any of the updated segrefs refer to open segments
    %% we close these segments so that they can be re-opened with updated
    %% indexes if needed
    Open = lists:foldl(fun ({Fn, _}, Acc0) ->
                               case ra_flru:evict(Fn, Acc0) of
                                   {_, Acc} -> Acc;
                                   error -> Acc0
                               end
                       end, Open0, NewSegmentRefs),
    {State#?MODULE{segment_refs = SegRefs,
                   range = Range,
                   open_segments = Open},
     OverwrittenSegments}.

-spec schedule_compaction(minor | major, ra:index(),
                          ra_seq:state(), state()) ->
    {state(), [ra_server:effect()]}.
schedule_compaction(minor, SnapIdx, LiveIndexes,
                    #?MODULE{cfg =
                             #cfg{compaction_conf =
                                  #{major_strategy :=
                                    {num_minors, NumMinors}}},
                             minor_compaction_count = MinorCount} = State)
  when MinorCount >= NumMinors ->
    %% promote to major compaction
    schedule_compaction(major, SnapIdx, LiveIndexes, State);
schedule_compaction(Type, SnapIdx, LiveIndexes,
                    #?MODULE{cfg = #cfg{log_id = LogId,
                                        compaction_conf = CompConf,
                                        directory = Dir} = Cfg,
                             minor_compaction_count = MinorCompCnt,
                             compaction = undefined} = State) ->
    case compactable_segrefs(SnapIdx, State) of
        [] ->
            {State, []};
        SegRefs when LiveIndexes == [] ->
            %% if LiveIndexes is [] we can just delete all compactable
            %% segment refs
            Unreferenced = [F || {F, _} <- SegRefs],
            ok = incr_counter(Cfg, ?C_RA_LOG_COMPACTIONS_MINOR_COUNT, 1),
            Result = #compaction_result{unreferenced = Unreferenced},
            {State#?MODULE{compaction = {minor, SnapIdx},
                           minor_compaction_count = MinorCompCnt + 1},
             [{next_event,
               {ra_log_event, {compaction_result, Result}}}]};
        SegRefs when Type == minor ->
            %% TODO: evaluate if minor compactions are fast enough to run
            %% in server process
            ok = incr_counter(Cfg, ?C_RA_LOG_COMPACTIONS_MINOR_COUNT, 1),
            Result = minor_compaction(SegRefs, LiveIndexes),
            {State#?MODULE{compaction = {minor, SnapIdx},
                           minor_compaction_count = MinorCompCnt + 1},
             [{next_event,
               {ra_log_event, {compaction_result, Result}}}]};
        SegRefs ->
            Self = self(),
            Fun = fun () ->
                          ok = incr_counter(Cfg, ?C_RA_LOG_COMPACTIONS_MAJOR_COUNT, 1),
                          MajConf = CompConf#{dir => Dir},
                          Result = major_compaction(MajConf, SegRefs,
                                                    LiveIndexes),
                          %% need to update the ra_servers list of seg refs
                          %% _before_ the segments can actually be deleted
                          Self ! {ra_log_event,
                                  {compaction_result, Result}},
                          %% TODO: this could be done on a timer if more
                          %% timely symlink cleanup is needed
                          purge_symlinks(Dir, ?SYMLINK_KEEPFOR_S),
                          ok
                  end,

            {State#?MODULE{compaction = {major, SnapIdx},
                           minor_compaction_count = 0},
             [{bg_work, Fun,
               fun (Err) ->
                       %% send an empty compaction result to ensure the
                       %% a future compaction can be performed
                       Self ! {ra_log_event,
                               {compaction_result, #compaction_result{}}},
                       ?WARN("~ts: Major compaction failed with ~p",
                             [LogId, Err]),
                       ok
               end}]}
    end;
schedule_compaction(Type, SnapIdx, _LiveIndexes,
                    #?MODULE{cfg = #cfg{log_id = LogId},
                             compaction = {Comp, CurSnapIdx}} = State) ->
    ?DEBUG("~ts: ~s compaction requested at ~b but ~s compaction"
           " already in progress for snapshot index ~b",
           [LogId, Type, SnapIdx, Comp, CurSnapIdx]),
    {State, []}.


-spec handle_compaction_result(#compaction_result{}, state()) ->
    {state(), [ra_server:effect()]}.
handle_compaction_result(#compaction_result{unreferenced = [],
                                            linked = [],
                                            compacted_segrefs = []},
                         State) ->
    {State#?MODULE{compaction = undefined}, []};

%% Fast path: minor compaction with only unreferenced segments
%% Avoids expensive map conversions for the common case
handle_compaction_result(#compaction_result{unreferenced = Unreferenced,
                                            linked = [],
                                            compacted_segrefs = []},
                         #?STATE{cfg = #cfg{directory = Dir},
                                 open_segments = Open0,
                                 segment_refs = SegRefs0} = State)
  when Unreferenced =/= [] ->
    %% Filter segment refs directly without map conversion
    UnrefSet = sets:from_list(Unreferenced, [{version, 2}]),
    FilterFun = fun({Fn, _}) -> not sets:is_element(Fn, UnrefSet) end,
    SegmentRefs = [SR || SR <- ra_lol:to_list(SegRefs0), FilterFun(SR)],
    %% Selectively evict only removed segments
    Open = lists:foldl(fun (Fn, Acc0) ->
                               case ra_flru:evict(Fn, Acc0) of
                                   {_, Acc} -> Acc;
                                   error -> Acc0
                               end
                       end, Open0, Unreferenced),
    Fun = fun () ->
                  [ok = prim_file:delete(filename:join(Dir, F))
                   || F <- Unreferenced],
                  purge_dangling_symlinks(Dir),
                  ok
          end,
    {State#?MODULE{segment_refs = ra_lol:from_list(fun seg_ref_gt/2,
                                                   SegmentRefs),
                   compaction = undefined,
                   open_segments = Open},
     [{bg_work, Fun, fun (_Err) -> ok end}]};
%% General path: major compaction with linked/compacted segments
handle_compaction_result(#compaction_result{unreferenced = Unreferenced,
                                            linked = Linked,
                                            compacted_segrefs = CompactedSegRefs},
                         #?STATE{cfg = #cfg{directory = Dir} = Cfg,
                                 open_segments = Open0,
                                 segment_refs = SegRefs0} = State) ->
    %% Build exclusion set for O(1) lookups - include filenames from
    %% CompactedSegRefs since they should override existing entries
    CompactedFns = [Fn || {Fn, _} <- CompactedSegRefs],
    ExcludeSet = sets:from_list(Unreferenced ++ Linked ++ CompactedFns,
                                [{version, 2}]),
    %% Filter segment refs in single pass (result is already sorted by filename desc)
    FilteredRefs = [SR || {Fn, _} = SR <- ra_lol:to_list(SegRefs0),
                          not sets:is_element(Fn, ExcludeSet)],
    %% CompactedSegRefs is in ascending filename order, reverse to get descending
    SegmentRefs = lists:merge(fun({Fn1, _}, {Fn2, _}) -> Fn1 >= Fn2 end,
                              FilteredRefs, lists:reverse(CompactedSegRefs)),
    Open = ra_flru:evict_all(Open0),
    Fun = fun () ->
                  [ok = prim_file:delete(filename:join(Dir, F))
                   || F <- Unreferenced],
                  purge_dangling_symlinks(Dir),
                  ok
          end,
    NumCompacted = length(CompactedSegRefs),
    ok = incr_counter(Cfg, ?C_RA_LOG_COMPACTIONS_SEGMENTS_WRITTEN,
                      NumCompacted),
    ok = incr_counter(Cfg, ?C_RA_LOG_COMPACTIONS_SEGMENTS_COMPACTED,
                      length(Linked) + NumCompacted),
    {State#?MODULE{segment_refs = ra_lol:from_list(fun seg_ref_gt/2,
                                                   SegmentRefs),
                   compaction = undefined,
                   open_segments = Open},
     [{bg_work, Fun, fun (_Err) -> ok end}]}.


-spec update_first_index(ra_index(), state()) ->
    {state(), [segment_ref()]}.
update_first_index(FstIdx, #?STATE{segment_refs = SegRefs0,
                                   open_segments = OpenSegs0} = State) ->
    case ra_lol:takewhile(fun({_Fn, {_, To}}) ->
                                  To >= FstIdx
                          end, SegRefs0) of
        {Active, Obsolete0} ->
            case ra_lol:len(Obsolete0) of
                0 ->
                    {State, []};
                _ ->
                    Obsolete = ra_lol:to_list(Obsolete0),
                    ObsoleteKeys = [K || {K, _} <- Obsolete],
                    % close any open segments
                    OpenSegs = lists:foldl(fun (K, OS0) ->
                                                   case ra_flru:evict(K, OS0) of
                                                       {_, OS} -> OS;
                                                       error -> OS0
                                                   end
                                           end, OpenSegs0, ObsoleteKeys),
                    {State#?STATE{open_segments = OpenSegs,
                                  segment_refs = ra_lol:from_list(
                                                   fun seg_ref_gt/2, Active)},
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

-spec compaction_conf(state()) -> map().
compaction_conf(#?STATE{cfg = #cfg{compaction_conf = Conf}}) ->
    Conf.

-spec compaction(state()) -> undefined | {major | minor, ra:index()}.
compaction(#?STATE{compaction = Conf}) ->
    Conf.

-spec num_open_segments(state()) -> non_neg_integer().
num_open_segments(#?STATE{open_segments = Open}) ->
    ra_flru:size(Open).

-spec fold(ra_index(), ra_index(), fun(), term(), state(),
                                       MissingKeyStrategy :: error | return) ->
    {state(), term()}.
fold(FromIdx, ToIdx, Fun, Acc,
    #?STATE{cfg = #cfg{} = Cfg} = State0, MissingKeyStrat)
  when ToIdx >= FromIdx ->
    ok = incr_counter(Cfg, ?C_RA_LOG_READ_SEGMENT, ToIdx - FromIdx + 1),
    segment_fold(State0, FromIdx, ToIdx, Fun, Acc, MissingKeyStrat);
fold(_FromIdx, _ToIdx, _Fun, Acc, #?STATE{} = State, _Strat) ->
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
      fun ({BaseName, Idxs}, {Acc1, Open1}) ->
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

-spec info(state()) -> map().
info(#?STATE{cfg = #cfg{} = _Cfg,
             minor_compaction_count = MinorCount,
             open_segments = Open} = State) ->
    #{max_size => ra_flru:max_size(Open),
      num_segments => segment_ref_count(State),
      minor_compactions_count => MinorCount}.

-spec purge_symlinks(file:filename_all(),
                     OlderThanSec :: non_neg_integer()) -> ok.
purge_symlinks(Dir, OlderThanSec) ->
    Now = erlang:system_time(second),
    [begin
         Fn = filename:join(Dir, F),
         case prim_file:read_link_info(Fn, [raw, {time, posix}]) of
             {ok, #file_info{type = symlink,
                             ctime = Time}}
               when Now - Time > OlderThanSec ->
                 prim_file:delete(Fn),
                 ok;
             _ ->
                 ok
         end
     end || F <- list_files(Dir, ".segment")],
    ok.

-spec purge_dangling_symlinks(file:filename_all()) -> ok.
purge_dangling_symlinks(Dir) ->
    [begin
         Fn = filename:join(Dir, list_to_binary(File)),
         case file:read_link_info(Fn, [raw]) of
             {ok, #file_info{type = symlink}} ->
                 case file:read_file_info(Fn, [raw]) of
                     {ok, _} ->
                         ok;
                     {error, enoent} ->
                         %% dangling symlink
                         ok = prim_file:delete(Fn)
                 end;
             _ ->
                 ok
         end
     end || File <- list_dir(Dir),
            filename:extension(File) =:= ".segment"],
    ok.
%% LOCAL

segment_read_plan(_SegRefs, [], Acc) ->
    lists:reverse(Acc);
segment_read_plan(SegRefs, [Idx | _] = Indexes, Acc) ->
    case ra_lol:search(seg_ref_search_fun(Idx), SegRefs) of
        {{Fn, Range}, Cont} ->
            case sparse_read_split(fun (I) ->
                                           ra_range:in(I, Range)
                                   end, Indexes, []) of
                {[], _} ->
                    segment_read_plan(Cont, Indexes, Acc);
                {Idxs, Rem} ->
                    segment_read_plan(Cont, Rem, [{Fn, Idxs} | Acc])
            end;
        undefined ->
            %% not found
            lists:reverse(Acc)
    end.

seg_ref_search_fun(Idx) ->
    fun({__Fn, {Start, End}}) ->
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
        {{Fn, _Range}, _Cont} ->
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
        {{Fn, Range}, Cont} ->
            This = ra_range:overlap(ReqRange, Range),
            ReqRem = case ra_range:subtract(This, ReqRange) of
                         [] ->
                             undefined;
                         [Rem] ->
                             Rem
                     end,
            segment_fold_plan(Cont, ReqRem, [{Fn, This} | Acc]);
        undefined ->
            %% not found
            Acc
    end.

segment_fold(#?STATE{segment_refs = SegRefs,
                     open_segments = OpenSegs,
                     cfg = Cfg} = State,
             RStart, REnd, Fun, Acc, MissingKeyStrat) ->
    Plan = segment_fold_plan(SegRefs, {RStart, REnd}, []),
    {Op, A} =
        lists:foldl(
          fun ({Fn, {Start, End}}, {Open0, Ac0}) ->
                  {Seg, Open} = get_segment(Cfg, Open0, Fn),
                  {Open, ra_log_segment:fold(Seg, Start, End,
                                             fun binary_to_term/1,
                                             Fun, Ac0, MissingKeyStrat)}
          end, {OpenSegs, Acc}, Plan),
    {State#?MODULE{open_segments = Op}, A}.


segment_sparse_read(#?STATE{open_segments = Open}, [], Entries0) ->
    {Open, 0, Entries0};
segment_sparse_read(#?STATE{segment_refs = SegRefs,
                            open_segments = OpenSegs,
                            cfg = Cfg}, Indexes, Entries0) ->
    Plan = segment_read_plan(SegRefs, Indexes, []),
    lists:foldl(
      fun ({Fn, Idxs}, {Open0, C, En0}) ->
              {Seg, Open} = get_segment(Cfg, Open0, Fn),
              {ok, ReadSparseCount, Entries} =
                  ra_log_segment:read_sparse_no_checks(
                    Seg, Idxs, fun (I, T, B, Acc) ->
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
                 access_pattern = AccessPattern} = Cfg, Open0, Fn)
  when is_binary(Fn) ->
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
          ({_, {Start, _}} = SegRef, Prev) ->
              [SegRef | limit(Start, Prev)]
      end, Cur, New).

limit(_LimitIdx, []) ->
    [];
limit(LimitIdx, [{PrevFn, PrevRange} | PrevRem]) ->
    case ra_range:limit(LimitIdx, PrevRange) of
        undefined ->
            limit(LimitIdx, PrevRem);
        NewPrevRange ->
            [{PrevFn, NewPrevRange} | PrevRem]
    end.

reset_counter(#cfg{counter = Cnt}, Ix)
  when Cnt =/= undefined ->
    counters:put(Cnt, Ix, 0);
reset_counter(#cfg{counter = undefined}, _) ->
    ok.

incr_counter(#cfg{counter = Cnt}, Ix, N) when Cnt =/= undefined ->
    counters:add(Cnt, Ix, N);
incr_counter(#cfg{counter = undefined}, _, _) ->
    ok.

decr_counter(#cfg{counter = Cnt}, Ix, N) when Cnt =/= undefined ->
    counters:sub(Cnt, Ix, N);
decr_counter(#cfg{counter = undefined}, _, _) ->
    ok.

segment_files(Dir, Fun) ->
    list_files(Dir, ".segment", Fun).

list_files(Dir, Ext) ->
    list_files(Dir, Ext, fun (_) -> true end).

list_files(Dir, Ext, Fun) ->
    Files = [list_to_binary(F)
             || F <- list_dir(Dir),
                filename:extension(F) =:= Ext,
                Fun(F)],
    lists:sort(Files).

list_dir(Dir) ->
    case prim_file:list_dir(Dir) of
        {ok, Files} ->
            Files;
        {error, enoent} ->
            []
    end.

%% Returns true if the file is a regular segment file (not a symlink).
%% Symlinks are kept around for pending readers.
is_regular_file(Filename) ->
    case prim_file:read_link_info(Filename) of
        {ok, #file_info{type = regular}} ->
            true;
        _ ->
            false
    end.

major_compaction(#{dir := Dir} = CompConf, SegRefs, LiveIndexes) ->
    %% Segments are processed from highest to lowest index (newest to oldest),
    %% so we progressively limit the LiveIndexes sequence after each check
    %% to improve performance for large sequences.
    {Compactable, Delete, _} =
        lists:foldl(fun({Fn0, {_Start, End} = Range} = S,
                        {Comps, Del, Live}) ->
                            case ra_seq:in_range(Range, Live) of
                                [] ->
                                    {Comps, [Fn0 | Del], Live};
                                Seq ->
                                    %% get the info map from each
                                    %% potential segment
                                    Fn = filename:join(Dir, Fn0),
                                    Info = ra_log_segment:info(Fn, Seq),
                                    %% Limit the sequence to remove entries above End
                                    %% for faster subsequent checks
                                    {[{Info, Seq, S} | Comps], Del,
                                     ra_seq:limit(End, Live)}
                            end
                    end, {[], [], LiveIndexes}, SegRefs),

    %% ensure there are no remaining fully overwritten (unused) segments in
    %% the compacted range
    Lookup = maps:from_list(SegRefs),
    {FirstFn, {_, _}} = hd(SegRefs),
    UnusedFiles = segment_files(Dir,
                                fun (F) ->
                                        Key = list_to_binary(F),
                                        Key =< FirstFn andalso
                                        not maps:is_key(Key, Lookup) andalso
                                        is_regular_file(filename:join(Dir, F))
                                end),
    [begin
         ok  = prim_file:delete(filename:join(Dir, F))
     end || F <- UnusedFiles],
    %% group compactable - Compactable is now in low→high order (oldest to newest)
    %% after the foldl reversal, which is what compaction_groups expects
    CompactionGroups = compaction_groups(Compactable, [], CompConf),
    Compacted0 =
    [begin
         AllFns = [F || {_, _, {F, _}} <- All],
         %% create a compaction marker with the compaction group i
         CompactionMarker = filename:join(Dir, with_ext(CompGroupLeaderFn,
                                                        ".compaction_group")),
         ok = ra_lib:write_file(CompactionMarker, term_to_binary(AllFns)),
         %% create a new segment with .compacting extension
         CompactingFn = filename:join(Dir, with_ext(CompGroupLeaderFn,
                                                    ".compacting")),
         %% max_count is the sum of all live indexes for segments in the
         %% compaction group
         MaxCount = lists:sum([ra_seq:length(S) || {_, S, _} <- All]),
         %% copy live indexes from all segments in compaction group to
         %% the compacting segment
         {ok, CompSeg0} = ra_log_segment:open(CompactingFn,
                                              #{max_count => MaxCount}),
         CompSeg = lists:foldl(
                     fun ({_, Live, {F, _}}, S0) ->
                             {ok, S} = ra_log_segment:copy(S0, filename:join(Dir, F),
                                                           ra_seq:expand(Live)),
                             S
                     end, CompSeg0, All),
         ok = ra_log_segment:close(CompSeg),

         FirstSegmentFn = filename:join(Dir, CompGroupLeaderFn),

         %% perform sym linking of the additional segments in the compaction
         %% group, the target is not yet updated which can be detected at
         %% recovery by the presence of a sym link _and_ the .compacting
         %% file
         ok = make_symlinks(Dir, FirstSegmentFn,
                            [F || {_, _, {F, _}} <- Additional]),

         %% rename the .compacting segment on top of the group leader
         ok = prim_file:rename(CompactingFn, FirstSegmentFn),
         %% finally delete the .compaction_marker file to signal
         %% compaction group is complete
         ok = prim_file:delete(CompactionMarker),
         %% sync the directory as changes have been made
         %% ignore the result as not supported on windows
         _ = ra_lib:sync_dir(Dir),
         %% return the new segref and additional segment keys
         {ra_log_segment:segref(FirstSegmentFn),
          [A || {_, _, {A, _}} <- Additional]}
     end || [{_Info, _, {CompGroupLeaderFn, _}} | Additional] = All
            <- CompactionGroups],

    {Compacted, AddDelete} = lists:unzip(Compacted0),

    #compaction_result{type = major,
                       unreferenced = Delete,
                       linked = lists:append(AddDelete),
                       compacted_segrefs = Compacted}.

minor_compaction(SegRefs, LiveIndexes) ->
    %% identifies unreferenced / unused segments with no live indexes
    %% in them. Segments are processed from highest to lowest index
    %% (newest to oldest), so we progressively limit the LiveIndexes
    %% sequence after each overlap check to improve performance for
    %% large sequences.
    {Delete, _} = lists:foldl(
                    fun({Fn, {_Start, End} = Range}, {Del, Live}) ->
                            case ra_seq:has_overlap(Range, Live) of
                                false ->
                                    {[Fn | Del], Live};
                                true ->
                                    %% Limit the sequence to remove entries above End.
                                    %% This makes subsequent checks on lower-indexed
                                    %% segments faster.
                                    {Del, ra_seq:limit(End, Live)}
                            end
                    end, {[], LiveIndexes}, SegRefs),
    #compaction_result{unreferenced = Delete}.

compactable_segrefs(SnapIdx, #?STATE{segment_refs = SegRefs}) ->
    %% Use foldr to iterate from oldest to newest, prepending matches.
    %% This produces a high→low ordered result list (newest to oldest)
    %% which enables efficient has_overlap + limit optimization in compaction.
    %% Skip the newest segment (processed last in foldr) as we never compact
    %% the current/active segment.
    Len = ra_lol:len(SegRefs),
    case Len of
        N when N =< 1 ->
            [];
        _ ->
            {Result, _} = ra_lol:foldr(
                fun({_Fn, {_Start, End}} = SegRef, {Acc, Pos}) ->
                    case Pos of
                        1 ->
                            %% Skip the newest segment
                            {Acc, 0};
                        _ when End =< SnapIdx ->
                            {[SegRef | Acc], Pos - 1};
                        _ ->
                            {Acc, Pos - 1}
                    end
                end, {[], Len}, SegRefs),
            Result
    end.

make_symlinks(Dir, To, From)
  when is_list(From) ->
    [begin
         SymFn = filename:join(Dir, with_ext(FromFn, ".link")),
         SegFn = filename:join(Dir, with_ext(FromFn, ".segment")),
         %% just in case it already exists
         _ = prim_file:delete(SymFn),
         %% make a symlink from the compacted target segment to a new .link
         %% where the compacted indexes now can be found
         ok = prim_file:make_symlink(filename:basename(To), SymFn),
         %% rename to link to replace original segment
         ok = prim_file:rename(SymFn, SegFn)
     end || FromFn <- From],
    ok.

with_ext(Fn, Ext) when is_binary(Fn) andalso is_list(Ext) ->
    <<(filename:rootname(Fn))/binary, (ra_lib:to_binary(Ext))/binary>>.

compaction_groups([], Groups, _Conf) ->
    lists:reverse(Groups);
compaction_groups(Infos, Groups, Conf) ->
    case take_group(Infos, Conf, []) of
        {[], RemInfos} ->
            compaction_groups(RemInfos, Groups, Conf);
        {Group, RemInfos} ->
            compaction_groups(RemInfos, [Group | Groups], Conf)
    end.

take_group([], _, Acc) ->
    {lists:reverse(Acc), []};
take_group([{#{num_entries := NumEnts,
               index_size := IdxSz,
               size := Sz,
               live_size := LiveSz}, Live, {_, _}} = E | Rem] = All,
           #{max_count := MaxCnt,
             max_size := MaxSz}, Acc) ->
    NumLive = ra_seq:length(Live),
    AllDataSz = Sz - IdxSz,
    %% group on either num relaimable entries or data saved
    case NumLive / NumEnts < 0.5 orelse
         LiveSz / AllDataSz < 0.5 of
        %% there are fewer than half live entries in the segment
        true ->
            %% check that adding this segment to the current group will no
            %% exceed entry or size limits
            case MaxCnt - NumLive < 0 orelse
                 MaxSz - LiveSz < 0 of
                true ->
                    %% adding this segment to the group will exceed limits
                    %% so returning current group
                    {lists:reverse(Acc), All};
                false ->
                    take_group(Rem, #{max_count => MaxCnt - NumLive,
                                      max_size => MaxSz - LiveSz},
                               [E | Acc])
            end;
        %% skip this segment
        false when Acc == [] ->
            take_group(Rem, #{max_count => MaxCnt,
                              max_size => MaxSz}, Acc);
        false ->
            {lists:reverse(Acc), Rem}
    end.

recover_compaction(Dir) ->
    case list_files(Dir, ".compaction_group") of
        [] ->
            %% no pending compactions
            #compaction_result{};
        [CompactionGroupFn0] ->
            %% compaction recovery is needed as there is a .compaction_group file
            CompactionGroupFn = filename:join(Dir, CompactionGroupFn0),
            %% if corrupt, just delete .compaction_group file
            {ok, Bin} = prim_file:read_file(CompactionGroupFn),
            CompactionGroup = try binary_to_term(Bin) of
                                  Group ->
                                      Group
                              catch _:_ ->
                                        %% any error just return empty
                                        _ = prim_file:delete(CompactionGroupFn),
                                        []
                              end,

            %% there _may_ be a .compacting file
            CompactingFn = filename:join(Dir, with_ext(CompactionGroupFn0,
                                                       ".compacting")),

            case CompactionGroup of
                [] ->
                    #compaction_result{};
                [_] ->
                    %% single segment compaction, we cannot know if the
                    %% compaction into the compacting segment completed or
                    %% not
                    %% ignore return value as CompactingFn may not exist
                    _ = prim_file:delete(CompactingFn),
                    ok = prim_file:delete(CompactionGroupFn),
                    #compaction_result{};
                [TargetShortFn | [FstLinkSeg | _] = LinkTargets] ->
                    %% multiple segments in group,
                    %% if any of the additional segments is a symlink
                    %% the writes to the .compacting segment completed and we
                    %% can complete the compaction work
                    FstLinkSegFn = filename:join(Dir, FstLinkSeg),
                    FstLinkSegLinkFn = filename:join(Dir, with_ext(FstLinkSeg, ".link")),
                    Target = filename:join(Dir, TargetShortFn),
                    AtLeastOneLink = ra_lib:is_any_file(FstLinkSegLinkFn),
                    CompactingExists = ra_lib:is_any_file(CompactingFn),
                    case file:read_link_info(FstLinkSegFn, [raw]) of
                        {ok, #file_info{type = Type}}
                          when Type == symlink orelse
                               AtLeastOneLink ->
                            %% it is a symlink, recreate all symlinks and delete
                            %% compaction marker
                            ok = make_symlinks(Dir, Target, LinkTargets),
                            %% if compacting file exists, rename it to target
                            if CompactingExists ->
                                   ok = prim_file:rename(CompactingFn, Target);
                               true ->
                                   ok
                            end,
                            ok = prim_file:delete(CompactionGroupFn),
                            Compacted = [ra_log_segment:segref(Target)],
                            #compaction_result{type = major,
                                               compacted_segrefs = Compacted,
                                               linked = LinkTargets};
                        {error, enoent} ->
                            %% segment does not exist indicates what exactly?
                            _ = prim_file:delete(CompactingFn),
                            ok = prim_file:delete(CompactionGroupFn),
                            #compaction_result{};
                        {ok, #file_info{type = regular}} ->
                            _ = prim_file:delete(CompactingFn),
                            ok = prim_file:delete(CompactionGroupFn),
                            #compaction_result{}
                    end
            end
    end.

seg_ref_gt({Fn1, {Start, _}}, {Fn2, {_, End}}) ->
    Start > End andalso Fn1 > Fn2.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

-define(SR(N, R), {<<N>>, R}).

compact_seg_refs_test() ->
    NewRefs = [?SR("2", {10, 100})],
    PrevRefs = [?SR("2", {10, 75}),
                ?SR("1", {1, 9})],
    ?assertEqual([?SR("2", {10, 100}),
                  ?SR("1", {1, 9})],
                 compact_segrefs(NewRefs, PrevRefs)).

compact_segref_3_test() ->
    Data = [
            {"C", {2, 7}},
            %% this entry has overwritten the prior two
            {"B", {5, 10}},
            {"A", {1, 4}}
           ],
    Res = compact_segrefs(Data, []),
    ?assertMatch([{"C", {2, 7}},
                  {"A", {1, 1}}], Res),
    ok.

compact_segref_2_test() ->
    Data = [
            {"80", {80, 89}},
            %% this entry has overwritten the prior two
            {"71", {56, 79}},
            {"70", {70, 85}},
            {"60", {60, 69}},
            {"50", {50, 59}}
           ],
    Res = compact_segrefs(Data, []),
    ?assertMatch([{"80", {80, 89}},
                  {"71", {56, 79}},
                  {"50", {50, 55}}
                 ], Res),
    ok.

compact_segref_1_test() ->
    Data = [
            {"80", {80, 89}},
            %% this entry has overwritten the prior one
            {"71", {70, 79}},
            {"70", {70, 85}},
            %% partial overwrite
            {"65", {65, 69}},
            {"60", {60, 69}},
            {"50", {50, 59}},
            {"40", {40, 49}}
           ],

    Res = compact_segrefs(Data, [
                                 {"30", {30, 39}},
                                 {"20", {20, 29}}
                                ]),

    %% overwritten entry is no longer there
    %% and the segment prior to the partial overwrite has been limited
    %% to provide a continuous range
    ?assertMatch([{"80", {80, 89}},
                  {"71", {70, 79}},
                  {"65", {65, 69}},
                  {"60", {60, 64}},
                  {"50", {50, 59}},
                  {"40", {40, 49}},
                  {"30", {30, 39}},
                  {"20", {20, 29}}
                 ], Res),
    ok.


segrefs_to_read_test() ->

    SegRefs = ra_lol:from_list(
                fun seg_ref_gt/2,
                  compact_segrefs(
                    [{"00000006.segment", {412, 499}},
                     {"00000005.segment", {284, 411}},
                     %% this segment got overwritten
                     {"00000004.segment",{284, 500}},
                     {"00000003.segment",{200, 285}},
                     {"00000002.segment",{128, 255}},
                     {"00000001.segment", {0, 127}}], [])),


    ?assertEqual([{"00000002.segment", {199, 199}},
                  {"00000003.segment", {200, 283}},
                  {"00000005.segment", {284, 411}},
                  {"00000006.segment", {412, 499}}],
                 segment_fold_plan(SegRefs, {199, 499}, [])),

    %% out of range
    ?assertEqual([], segment_fold_plan(SegRefs, {500, 500}, [])),
    ?assertEqual([
                  {"00000001.segment", {127,127}},
                  {"00000002.segment", {128,128}}
                 ],
                 segment_fold_plan(SegRefs, {127, 128}, [])),
    ok.

-endif.
