%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2017-2025 Broadcom. All Rights Reserved. The term Broadcom refers to Broadcom Inc. and/or its subsidiaries.
-module(ra_seq).

%% open type
%% sequences are ordered high -> low but ranges are ordered
%% {low, high} so a typical sequence could look like
%% [55, {20, 52}, 3]
-type state() :: [ra:index() | ra:range()].

-record(i, {seq :: state()}).
-opaque iter() :: #i{}.

-export_type([state/0,
              iter/0]).


-export([
         append/2,
         from_list/1,
         floor/2,
         limit/2,
         add/2,
         fold/3,
         expand/1,
         subtract/2,
         remove_prefix/2,
         first/1,
         last/1,
         iterator/1,
         next/1,
         list_chunk/2,
         length/1,
         in/2,
         range/1,
         in_range/2,
         has_overlap/2
        ]).

-spec append(ra:index(), state()) -> state().
append(Idx, [IdxN1, IdxN2 | Rem])
  when Idx == IdxN1 + 1 andalso
       Idx == IdxN2 + 2 ->
    %% we can compact into a range
    [{IdxN2, Idx} | Rem];
append(Idx, [{IdxN, IdxN1} | Rem])
  when Idx == IdxN1 + 1 ->
    %% Extend the raage
    [{IdxN, Idx} | Rem];
append(Idx, [])
  when is_integer(Idx) ->
    [Idx];
append(Idx, [Prev | _] = Seq)
  when is_integer(Idx) andalso
       ((is_tuple(Prev) andalso
         Idx > element(2, Prev)) orelse
        Idx > Prev) ->
    [Idx | Seq].

-spec from_list([ra:index()]) -> state().
from_list(L) ->
    lists:foldl(fun append/2, [], lists:usort(L)).

%% @doc This operation is O(n) + a list:reverse/1
-spec floor(ra:index(), state()) -> state().
floor(FloorIdxIncl, Seq) when is_list(Seq) ->
    %% TODO: assert appendable
    %% for now assume appendable
    floor0(FloorIdxIncl, Seq, []).


-spec limit(ra:index(), state()) -> state().
limit(CeilIdxIncl, [Last | Rem])
  when is_integer(Last) andalso
       Last > CeilIdxIncl ->
    limit(CeilIdxIncl, Rem);
limit(CeilIdxIncl, [{_, _} = T | Rem])
  when is_integer(CeilIdxIncl) ->
    case ra_range:limit(CeilIdxIncl + 1, T) of
        undefined ->
            limit(CeilIdxIncl, Rem);
        {I, I} ->
            [I | Rem];
        {I, I2} when I == I2 - 1 ->
            [I2, I | Rem];
        NewRange ->
            [NewRange | Rem]
    end;
limit(_CeilIdxIncl, Seq) ->
    Seq.

%% @doc adds two sequences together where To is
%% the "lower" sequence
%% TODO: optimise to avoid the fold which could be expensive
%% for very large sequences containing large ranges
-spec add(Add :: state(), To :: state()) -> state().
add([], To) ->
    To;
add(Add, []) ->
    Add;
add(Add, To) ->
    Fst = first(Add),
    fold(fun append/2, limit(Fst - 1, To), Add).

-spec fold(fun ((ra:index(), Acc) -> Acc), Acc, state()) ->
    Acc when Acc :: term().
fold(Fun, Acc0, Seq) ->
    lists:foldr(
      fun ({_, _} = Range, Acc) ->
              ra_range:fold(Range, Fun, Acc);
          (Idx, Acc) ->
              Fun(Idx, Acc)
      end, Acc0, Seq).

-spec expand(state()) -> [ra:index()].
expand(Seq) ->
    fold(fun (I, Acc) -> [I | Acc] end, [], Seq).

-spec subtract(Min :: state(), Sub :: state()) -> Diff :: state().
subtract(SeqA, SeqB) ->
    %% TODO: not efficient at all but good enough for now
    %% optimise if we end up using this in critical path
    A = expand(SeqA),
    B = expand(SeqB),
    from_list(A -- B).

-spec first(state()) -> undefined | ra:index().
first([]) ->
    undefined;
first(Seq) ->
    case lists:last(Seq) of
        {I, _} ->
            I;
        I ->
            I
    end.

-spec last(state()) -> undefined | ra:index().
last([]) ->
    undefined;
last(Seq) ->
    case hd(Seq) of
        {_, I} ->
            I;
        I ->
            I
    end.

-spec remove_prefix(state(), state()) ->
    {ok, state()} | {error, not_prefix}.
remove_prefix(Prefix, Seq) ->
    P = iterator(Prefix),
    S = iterator(Seq),
    drop_prefix(next(P), next(S)).

-spec iterator(state()) -> iter() | end_of_seq.
iterator(Seq) when is_list(Seq) ->
    #i{seq = lists:reverse(Seq)}.

-spec next(iter()) -> {ra:index(), iter()} | end_of_seq.
next(#i{seq = []}) ->
    end_of_seq;
next(#i{seq = [Next | Rem]})
  when is_integer(Next) ->
    {Next, #i{seq = Rem}};
next(#i{seq = [{Next, End} | Rem]}) ->
    case ra_range:new(Next + 1, End) of
        undefined ->
            {Next, #i{seq = Rem}};
        NextRange ->
            {Next, #i{seq = [NextRange | Rem]}}
    end.

%% @doc Returns a chunk of up to ChunkSize expanded indices from the sequence
%% without eagerly expanding the entire sequence. On first call, pass a state().
%% On subsequent calls, pass the returned iterator.
%% Returns `{Chunk, NewIterator}' or `end_of_seq' when exhausted.
%% Indices are returned in ascending order.
-spec list_chunk(ChunkSize :: pos_integer(), state() | iter()) ->
    {[ra:index()], iter()} | end_of_seq.
list_chunk(ChunkSize, Seq) when is_list(Seq) ->
    list_chunk(ChunkSize, iterator(Seq));
list_chunk(ChunkSize, Iter) when is_record(Iter, i) ->
    list_chunk(ChunkSize, Iter, []).

list_chunk(0, Iter, Acc) ->
    {lists:reverse(Acc), Iter};
list_chunk(N, Iter, Acc) ->
    case next(Iter) of
        end_of_seq when Acc =:= [] ->
            end_of_seq;
        end_of_seq ->
            {lists:reverse(Acc), Iter};
        {Idx, NextIter} ->
            list_chunk(N - 1, NextIter, [Idx | Acc])
    end.

length(Seq) ->
    lists:foldl(
      fun (Idx, Acc) when is_integer(Idx) ->
              Acc + 1;
          (Range, Acc) when is_tuple(Range) ->
              Acc + ra_range:size(Range)
      end, 0, Seq).

in(_Idx, []) ->
    false;
in(Idx, [Idx | _]) ->
    true;
in(Idx, [Next | Rem])
 when is_integer(Next) ->
    in(Idx, Rem);
in(Idx, [Range | Rem]) ->
    case ra_range:in(Idx, Range) of
        true ->
            true;
        false ->
            in(Idx, Rem)
    end.

-spec range(state()) -> ra:range().
range([]) ->
    undefined;
range(Seq) ->
    ra_range:new(first(Seq), last(Seq)).


-spec in_range(ra:range(), state()) ->
    state().
in_range(_Range, []) ->
    [];
in_range(undefined, _) ->
    [];
in_range({Start, End}, Seq0) ->
    %% TODO: optimise
    floor(Start, limit(End, Seq0)).

%% @doc Check if any element in the sequence overlaps with the given range.
%% This is a pure query that does not modify the sequence and can terminate
%% early as soon as an overlap is found.
%% Sequences are ordered high -> low, so we traverse from highest to lowest.
%% @end
-spec has_overlap(ra:range(), state()) -> boolean().
has_overlap(_Range, []) ->
    false;
has_overlap(undefined, _Seq) ->
    false;
has_overlap({Start, End}, Seq) ->
    has_overlap0(Start, End, Seq).

%% Internal functions

%% Traverse the sequence (ordered high -> low) checking for overlap.
%% - Skip elements entirely above End
%% - Return true if any element overlaps [Start, End]
%% - Return false once we pass below Start (no need to check further)
has_overlap0(_Start, _End, []) ->
    false;
has_overlap0(Start, End, [Idx | Rem]) when is_integer(Idx) ->
    if Idx > End ->
           %% Element is above the range, skip it
           has_overlap0(Start, End, Rem);
       Idx >= Start ->
           %% Element is within [Start, End], overlap found
           true;
       true ->
           %% Idx < Start, and since sequence is ordered high->low,
           %% all remaining elements are also < Start, no overlap possible
           false
    end;
has_overlap0(Start, End, [{RStart, REnd} | Rem]) ->
    %% Range element: check if it overlaps with [Start, End]
    if RStart > End ->
           %% Entire range is above End, skip it
           has_overlap0(Start, End, Rem);
       REnd < Start ->
           %% Entire range is below Start, and since sequence is ordered
           %% high->low, all remaining elements are also below Start
           false;
       true ->
           %% Ranges overlap: RStart =< End and REnd >= Start
           true
    end.

drop_prefix({IDX, PI}, {IDX, SI}) ->
    drop_prefix(next(PI), next(SI));
drop_prefix(_, end_of_seq) ->
    %% TODO: is this always right as it includes the case where there is
    %% more prefex left to drop but nothing in the target?
    {ok, []};
drop_prefix(end_of_seq, {Idx, #i{seq = RevSeq}}) ->
    {ok, add(lists:reverse(RevSeq), [Idx])};
drop_prefix({PrefIdx, PI}, {Idx, _SI} = I)
  when PrefIdx < Idx ->
    drop_prefix(next(PI), I);
drop_prefix({PrefIdx, _PI}, {Idx, _SI})
  when Idx < PrefIdx ->
    {error, not_prefix}.



floor0(FloorIdx, [Last | Rem], Acc)
  when is_integer(Last) andalso
       Last >= FloorIdx ->
    floor0(FloorIdx, Rem, [Last | Acc]);
floor0(FloorIdx, [{_, _} = T | Rem], Acc) ->
    case ra_range:truncate(FloorIdx - 1, T) of
        undefined ->
            lists:reverse(Acc);
        {I, I} ->
            floor0(FloorIdx, Rem, [I | Acc]);
        {I, I2} when I == I2 - 1 ->
            floor0(FloorIdx, Rem, [I, I2 | Acc]);
        NewRange ->
            floor0(FloorIdx, Rem, [NewRange | Acc])
    end;
floor0(_FloorIdx, _Seq, Acc) ->
    lists:reverse(Acc).
