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
         size/1
        ]).


-type range() :: undefined | {ra:index(), ra:index()}.

-export_type([range/0]).

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
add(_Range, undefined) ->
    undefined;
add({Start1, End1}, {Start2, End2})
  when Start2 =< End1 + 1 andalso
       End2 >= Start1 ->
    %% TODO: refine logic for unhappy cases
    {min(Start1, Start2), End2};
add(_Range, Range) ->
    Range.

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
       UpToIncl >= End ->
    undefined;
truncate(UpToIncl, {Start, End})
  when is_integer(UpToIncl) andalso
       UpToIncl >= Start ->
    {UpToIncl + 1, End};
truncate(UpToIncl, Range)
  when is_integer(UpToIncl) ->
    Range.

size(undefined) ->
    0;
size({Start, End}) ->
    End - Start + 1.

-spec extend(range() | ra:index(), range()) -> range().
extend({NewStart, NewEnd}, {Start, End})
  when NewStart == End + 1 ->
    {Start, NewEnd};
extend({_NewStart, _NewEnd}, {_Start, _End}) ->
    not_extension;
extend(Idx, {Start, End})
  when is_integer(Idx) andalso
       Idx == End + 1 ->
    {Start, Idx};
extend({_, _} = AddRange, undefined) ->
    AddRange;
extend(Idx, undefined) when is_integer(Idx) ->
    ra_range:new(Idx).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

add_test() ->
    ?assertEqual(undefined, add(undefined, undefined)),
    ?assertEqual({1, 10}, add(undefined, {1, 10})),
    ?assertEqual(undefined, add({1, 10}, undefined)),

    ?assertEqual({1, 20}, add({1, 10}, {11, 20})),
    ?assertEqual({1, 20}, add({1, 10}, {5, 20})),

    ?assertEqual({1, 9}, add({1, 10}, {5, 9})),
    ?assertEqual({5, 9}, add({6, 10}, {5, 9})),

    %% when the new range is smaller than the prior range
    ?assertEqual({1, 3}, add({6, 10}, {1, 3})),
    ?assertEqual({1, 7}, add({6, 10}, {1, 7})),
    %% when the old range is smaller than the add range
    ?assertEqual({1, 3}, add({6, 10}, {1, 3})),
    ?assertEqual({1, 7}, add({6, 10}, {1, 7})),
    ok.

-endif.
