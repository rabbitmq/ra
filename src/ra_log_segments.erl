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
         init/7,
         close/1,
         update_segments/2,
         schedule_compaction/4,
         handle_compaction_result/2,
         segment_refs/1,
         segment_ref_count/1,
         range/1,
         num_open_segments/1,
         update_first_index/2,
         fold/5,
         sparse_read/3,
         read_plan/2,
         exec_read_plan/6,
         fetch_term/2,
         info/1,
         purge_symlinks/2
         ]).

-include("ra.hrl").

-define(STATE, ?MODULE).

-define(SYMLINK_KEEPFOR_S, 60).

-type access_pattern() :: sequential | random.
%% holds static or rarely changing fields
-record(cfg, {uid :: ra_uid(),
              log_id = "" :: unicode:chardata(),
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

-record(compaction_result,
        {unreferenced = [] :: [file:filename_all()],
         linked = [] :: [file:filename_all()],
         compacted = [] :: [segment_ref()]}).

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

-spec init(ra_uid(), file:filename_all(), non_neg_integer(),
           access_pattern(), [segment_ref()],
           undefined | counters:counters_ref(),
           unicode:chardata()) -> state().
init(UId, Dir, MaxOpen, AccessPattern, SegRefs0, Counter, LogId)
  when is_binary(UId) ->
    Cfg = #cfg{uid = UId,
               log_id = LogId,
               counter = Counter,
               directory = Dir,
               access_pattern = AccessPattern},
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
    SegRefsRev = lists:reverse(SegRefs),
    reset_counter(Cfg, ?C_RA_LOG_OPEN_SEGMENTS),
    Result = recover_compaction(Dir),
    %% handle_compaction_result/2 will never return an effect here
    %% as no segments got deleted
    State0 = #?STATE{cfg = Cfg,
                     open_segments = ra_flru:new(MaxOpen, FlruHandler),
                     range = Range,
                     segment_refs = ra_lol:from_list(
                                      fun seg_ref_gt/2, SegRefsRev)},
    {State, _} = handle_compaction_result(Result, State0),
    State.

-spec close(state()) -> ok.
close(#?STATE{open_segments = Open}) ->
    _ = ra_flru:evict_all(Open),
    ok.

-spec update_segments([segment_ref()], state()) ->
    {state(), OverwrittenSegments :: [segment_ref()]}.
update_segments(NewSegmentRefs, #?STATE{open_segments = Open0,
                                        segment_refs = SegRefs0} = State) ->

    SegmentRefs0 = ra_lol:to_list(SegRefs0),
    SegmentRefsComp = compact_segrefs(NewSegmentRefs, SegmentRefs0),
    %% capture segrefs removed by compact_segrefs/2 and delete them
    %% a major compaction will also remove these
    OverwrittenSegments = NewSegmentRefs -- SegmentRefsComp,
    SegmentRefsCompRev = lists:reverse(SegmentRefsComp),
    SegRefs = ra_lol:from_list(fun seg_ref_gt/2, SegmentRefsCompRev),
    Range = case SegmentRefsComp of
                [{_, {_, L}} | _] ->
                    [{_, {F, _}} | _] = SegmentRefsCompRev,
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
    [ra_server:effect()].
schedule_compaction(Type, SnapIdx, LiveIndexes,
                    #?MODULE{cfg = #cfg{log_id = LogId,
                                        directory = Dir}} = State) ->
    case compactable_segrefs(SnapIdx, State) of
        [] ->
            [];
        SegRefs when LiveIndexes == [] ->
            %% if LiveIndexes is [] we can just delete all compactable
            %% segment refs
            Unreferenced = [F || {F, _} <- SegRefs],
            Result = #compaction_result{unreferenced = Unreferenced},
            [{next_event,
              {ra_log_event, {compaction_result, Result}}}];
        SegRefs when Type == minor ->
            %% TODO evaluate if minor compactions are fast enough to run
            %% in server process
            Result = minor_compaction(SegRefs, LiveIndexes),
            [{next_event,
              {ra_log_event, {compaction_result, Result}}}];
        SegRefs ->
            Self = self(),
            Fun = fun () ->
                          MajConf = #{dir => Dir},
                          Result = major_compaction(MajConf, SegRefs,
                                                    LiveIndexes),
                          %% TODO: this could be done on a timer if more
                          %% timely symlink cleanup is needed
                          purge_symlinks(Dir, ?SYMLINK_KEEPFOR_S),
                          %% need to update the ra_servers list of seg refs
                          %% _before_ the segments can actually be deleted
                          Self ! {ra_log_event,
                                  {compaction_result, Result}},
                          ok
                  end,

            [{bg_work, Fun,
              fun (Err) ->
                      ?WARN("~ts: Major compaction failed with ~p",
                            [LogId, Err]), ok
              end}]
    end.

-spec handle_compaction_result(#compaction_result{}, state()) ->
    {state(), [ra_server:effect()]}.
handle_compaction_result(#compaction_result{unreferenced = [],
                                            linked = [],
                                            compacted = []},
                         State) ->
    {State, []};
handle_compaction_result(#compaction_result{unreferenced = Unreferenced,
                                            linked = Linked,
                                            compacted = Compacted},
                         #?STATE{cfg = #cfg{directory = Dir},
                                 open_segments = Open0,
                                 segment_refs = SegRefs0} = State) ->
    SegRefs1 = maps:from_list(ra_lol:to_list(SegRefs0)),
    SegRefs2 = maps:without(Unreferenced, SegRefs1),
    SegRefs = maps:without(Linked, SegRefs2),
    SegmentRefs0 = maps:merge(SegRefs, maps:from_list(Compacted)),
    SegmentRefs = maps:to_list(maps:iterator(SegmentRefs0, ordered)),
    Open = ra_flru:evict_all(Open0),
    Fun = fun () ->
                  [prim_file:delete(filename:join(Dir, F))
                   || F <- Unreferenced],
                  ok
          end,
    {State#?MODULE{segment_refs = ra_lol:from_list(fun seg_ref_gt/2,
                                                   SegmentRefs),
                   open_segments = Open},
     [{bg_work, Fun, fun (_Err) -> ok end}]}.


-spec update_first_index(ra_index(), state()) ->
    {state(), [segment_ref()]}.
update_first_index(FstIdx, #?STATE{segment_refs = SegRefs0,
                                   open_segments = OpenSegs0} = State) ->
    %% TODO: refactor this so that ra_lol just returns plain lists on both sides?
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
             open_segments = Open} = State) ->
    #{max_size => ra_flru:max_size(Open),
      num_segments => segment_ref_count(State)}.

-spec purge_symlinks(file:filename_all(),
                     OlderThanSec :: non_neg_integer()) -> ok.
purge_symlinks(Dir, OlderThanSec) ->
    Now = erlang:system_time(second),
    [begin
         Fn = filename:join(Dir, F),
         case file:read_link_info(Fn, [raw, {time, posix}]) of
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
             RStart, REnd, Fun, Acc) ->
    Plan = segment_fold_plan(SegRefs, {RStart, REnd}, []),
    {Op, A} =
        lists:foldl(
          fun ({Fn, {Start, End}}, {Open0, Ac0}) ->
                  {Seg, Open} = get_segment(Cfg, Open0, Fn),
                  {Open, ra_log_segment:fold(Seg, Start, End,
                                             fun binary_to_term/1,
                                             Fun, Ac0)}
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
    case prim_file:list_dir(Dir) of
        {ok, Files0} ->
            Files = [list_to_binary(F)
                     || F <- Files0,
                        filename:extension(F) =:= Ext,
                        Fun(F)],
            lists:sort(Files);
        {error, enoent} ->
            []
    end.

major_compaction(#{dir := Dir}, SegRefs, LiveIndexes) ->
    {Compactable, Delete} =
        lists:foldl(fun({Fn0, Range} = S,
                        {Comps, Del}) ->
                            case ra_seq:in_range(Range,
                                                 LiveIndexes) of
                                [] ->
                                    {Comps, [Fn0 | Del]};
                                Seq ->
                                    %% get the info map from each
                                    %% potential segment
                                    Fn = filename:join(Dir, Fn0),
                                    Info = ra_log_segment:info(Fn),
                                    {[{Info, Seq, S} | Comps], Del}
                            end
                    end, {[], []}, SegRefs),

    %% ensure there are no remaining fully overwritten (unused) segments in
    %% the compacted range
    Lookup = maps:from_list(SegRefs),
    {LastFn, {_, _}} = lists:last(SegRefs),
    UnusedFiles = segment_files(Dir, fun (F) ->
                                             Key = list_to_binary(F),
                                             Key =< LastFn andalso
                                             not maps:is_key(Key, Lookup)
                                     end),
    [begin
         ok  = prim_file:delete(filename:join(Dir, F))
     end || F <- UnusedFiles],
    %% group compactable
    CompactionGroups = compaction_groups(lists:reverse(Compactable), []),
    Compacted0 =
    [begin
         %% create a new segment with .compacting extension
         AllShortFns = [F || {_, _, {F, _}} <- All],
         CompactingShortFn = make_compacting_file_name(AllShortFns),
         CompactingFn = filename:join(Dir, CompactingShortFn),
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

         %% link .compacting segment to the original .segment file
         %% first we have to create a hard link to a new .compacted file
         %% from the .compacting file (as we need to keep this as a marker
         %% until the end
         %% then we can rename this on top of the first segment file in the
         %% group (the target)
         FirstSegmentFn = filename:join(Dir, FstFn0),
         CompactedFn = filename:join(Dir, with_ext(FstFn0, ".compacted")),
         ok = prim_file:make_link(CompactingFn, CompactedFn),
         ok = prim_file:rename(CompactedFn, FirstSegmentFn),

         %% perform sym linking of the additional segments in the compaction
         %% group
         ok = make_links(Dir, FirstSegmentFn,
                         [F || {_, _, {F, _}} <- Additional]),
         %% finally deleted the .compacting file to signal compaction group
         %% is complete
         ok = prim_file:delete(CompactingFn),
         %% return the new segref and additional segment keys
         {ra_log_segment:segref(FirstSegmentFn),
          [A || {_, _, {A, _}} <- Additional]}
     end || [{_Info, _, {FstFn0, _}} | Additional] = All
            <- CompactionGroups],

    {Compacted, AddDelete} = lists:unzip(Compacted0),

    #compaction_result{unreferenced = Delete,
                       linked = lists:append(AddDelete),
                       compacted = Compacted}.

minor_compaction(SegRefs, LiveIndexes) ->
    %% identifies unreferences / unused segments with no live indexes
    %% in them
    Delete = lists:foldl(fun({Fn, Range}, Del) ->
                                 case ra_seq:in_range(Range,
                                                      LiveIndexes) of
                                     [] ->
                                         [Fn | Del];
                                     _ ->
                                         Del
                                 end
                         end, [], SegRefs),
    #compaction_result{unreferenced = Delete}.

compactable_segrefs(SnapIdx, State) ->
    %% TODO: provide a ra_lol:foldr API to avoid creatinga  segref list
    %% then filtering that
    case segment_refs(State) of
        [] ->
            [];
        [_] ->
            [];
        [_ | Compactable] ->
            %% never compact the current segment
            %% only take those who have a range lower than the snapshot index as
            %% we never want to compact more than that
            lists:foldl(fun ({_Fn, {_Start, End}} = SR, Acc)
                              when End =< SnapIdx ->
                                [SR | Acc];
                            (_, Acc) ->
                                Acc
                        end, [], Compactable)
    end.

make_links(Dir, To, From)
  when is_list(From) ->
    [begin
         SymFn = filename:join(Dir, with_ext(FromFn, ".link")),
         SegFn = filename:join(Dir, with_ext(FromFn, ".segment")),
         %% just in case it already exists
         _ = prim_file:delete(SymFn),
         %% make a symlink from the compacted target segment to a new .link
         %% where the compacted indexes now can be found
         ok = prim_file:make_symlink(To, SymFn),
         %% rename to link to replace original segment
         ok = prim_file:rename(SymFn, SegFn)
     end || FromFn <- From],
    ok.

with_ext(Fn, Ext) when is_binary(Fn) andalso is_list(Ext) ->
    <<(filename:rootname(Fn))/binary, (ra_lib:to_binary(Ext))/binary>>.

compaction_groups([], Groups) ->
    lists:reverse(Groups);
compaction_groups(Infos, Groups) ->
    case take_group(Infos, #{max_count => 128}, []) of
        {Group, RemInfos} ->
            compaction_groups(RemInfos, [Group | Groups])
    end.

%% TODO: try to take potential size into account
take_group([], _, Acc) ->
    {lists:reverse(Acc), []};
take_group([{#{num_entries := NumEnts}, Live, {_, _}} = E | Rem] = All,
           #{max_count := Mc}, Acc) ->
    Num = ra_seq:length(Live),
    case Num < NumEnts div 2 of
        true ->
            case Mc - Num < 0 of
                true ->
                    {lists:reverse(Acc), All};
                false ->
                    take_group(Rem, #{max_count => Mc - Num}, [E | Acc])
            end;
            %% skip this secment
        false when Acc == [] ->
            take_group(Rem, #{max_count => Mc}, Acc);
        false ->
            {lists:reverse(Acc), Rem}
    end.


parse_compacting_filename(Fn) when is_binary(Fn) ->
    binary:split(filename:rootname(Fn), <<"-">>, [global]).

make_compacting_file_name([N1 | Names]) ->
    Root = lists:foldl(fun (N, Acc) ->
                               [filename:rootname(N), <<"-">> | Acc]
                       end, [N1], Names),
    iolist_to_binary(lists:reverse([<<".compacting">> | Root])).

recover_compaction(Dir) ->
    case list_files(Dir, ".compacting") of
        [] ->
            %% no pending compactions
            #compaction_result{};
        [ShortFn] ->
            %% compaction recovery is needed
            CompactingFn = filename:join(Dir, ShortFn),
            {ok, #file_info{links = Links}} =
                file:read_link_info(CompactingFn, [raw, {time, posix}]),
            case Links of
                1 ->
                    %% must have exited before the target file was renamed
                    %% just delete
                    ok = prim_file:delete(CompactingFn),
                    #compaction_result{};
                2 ->
                    [FstFn | RemFns] = parse_compacting_filename(ShortFn),
                    %% there may be a .compacted file
                    Target = filename:join(Dir, with_ext(FstFn, ".segment")),
                    case list_files(Dir, ".compacted") of
                        [CompactedShortFn] ->
                            CompactedFn = filename:join(Dir, CompactedShortFn),
                            %% all entries were copied but it failed before
                            %% this hard link could be renamed over the target
                            ok = prim_file:rename(CompactedFn, Target),
                            ok;
                        [] ->
                            %% links may not have been fully created,
                            %% delete all .link files then relink
                            ok
                    end,
                    ok = make_links(Dir, Target, RemFns),
                    ok = prim_file:delete(CompactingFn),

                    Linked = [with_ext(L, ".segment") || L <- RemFns],
                    Compacted = [ra_log_segment:segref(Target)],
                    #compaction_result{compacted = Compacted,
                                       linked = Linked}
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
                lists:reverse(
                  compact_segrefs(
                    [{"00000006.segment", {412, 499}},
                     {"00000005.segment", {284, 411}},
                     %% this segment got overwritten
                     {"00000004.segment",{284, 500}},
                     {"00000003.segment",{200, 285}},
                     {"00000002.segment",{128, 255}},
                     {"00000001.segment", {0, 127}}], []))),


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
