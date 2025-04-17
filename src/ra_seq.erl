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
         next/1
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
    lists:foldl(fun append/2, [], lists:sort(L)).

-spec floor(ra:index(), state()) -> state().
floor(FloorIdxIncl, Seq) ->
    %% TODO: assert appendable
    %% for now assume appendable
    floor0(FloorIdxIncl, Seq, []).


-spec limit(ra:index(), state()) -> state().
limit(CeilIdx, [Last | Rem])
  when is_integer(Last) andalso
       Last > CeilIdx ->
    limit(CeilIdx, Rem);
limit(CeilIdx, [{_, _} = T | Rem]) ->
    case ra_range:limit(CeilIdx + 1, T) of
        undefined ->
            limit(CeilIdx, Rem);
        {I, I} ->
            [I | Rem];
        {I, I2} when I == I2 - 1 ->
            [I2, I | Rem];
        NewRange ->
            [NewRange | Rem]
    end;
limit(_CeilIdx, Seq) ->
    Seq.

-spec add(state(), state()) -> state().
add([], Seq2) ->
    Seq2;
add(Seq1, Seq2) ->
    Fst = case lists:last(Seq1) of
              {I, _} -> I;
              I -> I
          end,
    fold(fun append/2, limit(Fst - 1, Seq2), Seq1).

-spec fold(fun ((ra:index(), Acc) -> Acc), Acc, state()) ->
    Acc when Acc :: term().
fold(Fun, Acc0, Seq) ->
    %% TODO: factor out the lists:seq/2
    lists:foldr(
      fun ({S, E}, Acc) ->
              lists:foldl(Fun, Acc, lists:seq(S, E));
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

%% Internal functions

drop_prefix({IDX, PI}, {IDX, SI}) ->
    drop_prefix(next(PI), next(SI));
drop_prefix(end_of_seq, {Idx, #i{seq = RevSeq}}) ->
    {ok, lists:reverse([Idx | RevSeq])};
drop_prefix({PrefIdx, PI}, {Idx, _SI} = I)
  when PrefIdx < Idx ->
    drop_prefix(next(PI), I);
drop_prefix({PrefIdx, PI}, {Idx, _SI} = I)
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


% last_index([{_, I} | _]) ->
%     I;
% last_index([I | _])
%   when is_integer(I) ->
%     I;
% last_index([]) ->
%     undefined.

% first_index([{_, I} | _]) ->
%     I;
% first_index([I | _])
%   when is_integer(I) ->
%     I;
% first_index([]) ->
%     undefined.
