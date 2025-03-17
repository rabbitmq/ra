%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term Broadcom refers to Broadcom Inc. and/or its subsidiaries.
%%
%% @hidden
-module(ra_range).

-export([
         new/1,
         new/2,
         add/2,
         in/2,
         extend/2,
         limit/2,
         truncate/2,
         size/1,
         overlap/2,
         subtract/2
        ]).


-type range() :: undefined | {ra:index(), ra:index()}.

-export_type([range/0]).

-define(IS_RANGE(R), ((is_tuple(R) andalso
                       tuple_size(R) == 2 andalso
                       is_integer(element(1, R)) andalso
                       is_integer(element(2, R))) orelse
                      R == undefined)).


-spec new(ra:index()) -> range().
new(Start) when is_integer(Start) ->
    {Start, Start}.

-spec new(ra:index(), ra:index()) -> range().
new(Start, End)
  when is_integer(Start) andalso
       is_integer(End) andalso
       Start =< End ->
    {Start, End};
new(_Start, _End) ->
    undefined.

-spec add(AddRange :: range(), CurRange :: range()) -> range().
add(undefined, Range) ->
    Range;
add({AddStart, AddEnd}, {Start, End})
  when Start =< AddEnd + 1 andalso
       End + 1 >= AddStart ->
    {min(AddStart, Start), max(AddEnd, End)};
add(AddRange, _Range) ->
    %% no overlap, return add range
    AddRange.

-spec in(ra:index(), range()) -> boolean().
in(_Idx, undefined) ->
    false;
in(Idx, {Start, End}) ->
    Idx >= Start andalso Idx =< End.

-spec limit(ra:index(), range()) -> range().
limit(CeilExcl, {Start, _End})
  when is_integer(CeilExcl) andalso
       CeilExcl =< Start ->
    undefined;
limit(CeilExcl, {Start, End})
  when is_integer(CeilExcl) andalso
       CeilExcl =< End ->
    {Start, CeilExcl - 1};
limit(CeilExcl, Range)
  when is_integer(CeilExcl) ->
    Range.

-spec truncate(ra:index(), range()) -> range().
truncate(UpToIncl, {_Start, End})
  when is_integer(UpToIncl) andalso
       is_integer(End) andalso
       UpToIncl >= End ->
    undefined;
truncate(UpToIncl, {Start, End})
  when is_integer(UpToIncl) andalso
       is_integer(Start) andalso
       UpToIncl >= Start ->
    {UpToIncl + 1, End};
truncate(UpToIncl, Range)
  when is_integer(UpToIncl) andalso
       ?IS_RANGE(Range) ->
    Range.

size(undefined) ->
    0;
size({Start, End}) ->
    End - Start + 1.

-spec extend(ra:index(), range()) -> range().
extend(Idx, {Start, End})
  when Idx == End + 1 ->
    {Start, Idx};
extend(Idx, undefined) when is_integer(Idx) ->
    ra_range:new(Idx);
extend(Idx, Range) ->
    error({cannot_extend, Idx, Range}).

-spec overlap(range(), range()) -> range().
overlap({ReqStart, ReqEnd}, {Start, End}) ->
    new(max(ReqStart, Start), min(ReqEnd, End));
overlap(_Range1, _Range2) ->
    undefined.

%% @doc subtracts the range in the first argument
%% from that of the range in the second arg.
%% Returns a list of remaining ranges
%% @end
-spec subtract(range(), range()) -> [range()].
subtract(_Range1, undefined) ->
    [];
subtract(undefined, Range) ->
    [Range];
subtract({_SubStart, _SubEnd} = SubRange, {Start, End} = Range) ->
    case overlap(SubRange, Range) of
        undefined ->
            [Range];
        {OStart, OEnd} ->
            [R || {_, _} = R <- [new(Start, OStart -1),
                                 new(OEnd + 1, End)]]
    end.


-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

subtract_test() ->
    ?assertEqual([], subtract({1, 10}, undefined)),
    ?assertEqual([{1, 10}], subtract(undefined, {1, 10})),
    ?assertEqual([], subtract({1, 10}, {1, 10})),
    ?assertEqual([], subtract({1, 10}, {4, 6})),
    ?assertEqual([{4, 6}], subtract({8, 10}, {4, 6})),

    ?assertEqual([{11, 20}], subtract({1, 10}, {1, 20})),
    ?assertEqual([{1, 1}, {11, 20}], subtract({2, 10}, {1, 20})),
    ?assertEqual([{1, 9}], subtract({10, 20}, {1, 20})),

    ok.

overlap_test() ->
    ?assertEqual(undefined, overlap({1, 10}, undefined)),
    ?assertEqual(undefined, overlap(undefined, {1, 10})),
    ?assertEqual(undefined, overlap({1, 10}, {11, 15})),
    ?assertEqual(undefined, overlap({16, 20}, {11, 15})),
    ?assertEqual({11, 11}, overlap({1, 11}, {11, 15})),
    ?assertEqual({14, 15}, overlap({14, 20}, {11, 15})),
    ?assertEqual({12, 14}, overlap({12, 14}, {11, 15})),
    ?assertEqual({11, 15}, overlap({1, 20}, {11, 15})),
    ok.

add_test() ->
    ?assertEqual(undefined, add(undefined, undefined)),
    ?assertEqual({1, 10}, add(undefined, {1, 10})),
    ?assertEqual({1, 10}, add({1, 10}, undefined)),

    ?assertEqual({1, 20}, add({1, 10}, {11, 20})),
    ?assertEqual({1, 20}, add({1, 10}, {5, 20})),

    ?assertEqual({1, 10}, add({1, 10}, {5, 9})),
    ?assertEqual({5, 10}, add({6, 10}, {5, 9})),
    ?assertEqual({1, 10}, add({6, 10}, {1, 5})),

    %% when the add range is smaller than the prior range
    %% return the additional range
    ?assertEqual({1, 3}, add({1, 3}, {6, 10})),
    ?assertEqual({1, 10}, add({6, 10}, {1, 7})),
    ok.

truncate_test() ->
    ?assertEqual(undefined, truncate(9, undefined)),
    ?assertEqual({6, 10}, truncate(5, {1, 10})),
    ?assertEqual(undefined, truncate(11, {1, 10})),
    ?assertEqual({10, 20}, truncate(9, {10, 20})),
    ok.

limit_test() ->
    ?assertEqual(undefined, limit(9, undefined)),
    ?assertEqual({1, 4}, limit(5, {1, 10})),
    ?assertEqual({1, 10}, limit(11, {1, 10})),
    ?assertEqual(undefined, limit(10, {10, 20})),
    ?assertEqual(undefined, limit(1, {10, 20})),
    ok.

in_test() ->
    ?assertEqual(false, in(9, undefined)),
    ?assertEqual(true, in(5, {1, 10})),
    ?assertEqual(true, in(10, {1, 10})),
    ?assertEqual(true, in(1, {1, 10})),
    ?assertEqual(false, in(11, {1, 10})),
    ?assertEqual(false, in(0, {1, 10})),
    ok.

extend_test() ->
    ?assertEqual({9, 9}, extend(9, undefined)),
    ?assertEqual({1, 11}, extend(11, {1, 10})),
    ?assertError({cannot_extend, 1, {5, 10}}, extend(1, {5, 10})),
    ?assertError({cannot_extend, 12, {1, 10}}, extend(12, {1, 10})),
    ok.

-endif.
