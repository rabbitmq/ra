-module(ra_seq_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-export([
         ]).

-include_lib("eunit/include/eunit.hrl").

%%%===================================================================
%%% Common Test callbacks
%%%===================================================================

all() ->
    [
     {group, tests}
    ].


all_tests() ->
    [
     append,
     floor,
     limit,
     add
    ].

groups() ->
    [{tests, [], all_tests()}].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(_Group, Config) ->
    Config.

end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok.

%%%===================================================================
%%% Test cases
%%%===================================================================

append(_Config) ->
    S1 = [1] = ra_seq:append(1, []),
    S2 = [2, 1] = ra_seq:append(2, S1),
    S3 = [{1, 3}] = ra_seq:append(3, S2),
    S4 = [{1, 4}] = ra_seq:append(4, S3),
    S5 = [6, {1, 4}] = ra_seq:append(6, S4),

    ?assertError(function_clause, ra_seq:append(2, S4)),
    ?assertError(function_clause, ra_seq:append(6, S5)),

    ok.

floor(_Config) ->
    S = ra_seq:from_list([1, 2, 3, 5, 6, 7, 8, 9, 11]),
    [11] = ra_seq:floor(11, S),
    [11, 9] = ra_seq:floor(9, S),
    [11, 9, 8] = ra_seq:floor(8, S),
    [11, {7, 9}] = ra_seq:floor(7, S),
    [11, {6, 9}] = ra_seq:floor(6, S),
    [11, {5, 9}] = ra_seq:floor(5, S),
    [11, {5, 9}] = ra_seq:floor(4, S),
    [11, {5, 9}, 3] = ra_seq:floor(3, S),
    [11, {5, 9}, 3, 2] = ra_seq:floor(2, S),
    [11, {5, 9}, {1, 3}] = ra_seq:floor(1, S),
    [11, {5, 9}, {1, 3}] = ra_seq:floor(0, S),
    ok.

limit(_Config) ->
    S = ra_seq:from_list([1, 2, 3, 5, 6, 7, 8, 9, 11]),
    [11, {5, 9}, {1, 3}] = ra_seq:limit(11, S),
    [{5, 9}, {1, 3}] = ra_seq:limit(10, S),
    [{5, 9}, {1, 3}] = ra_seq:limit(9, S),
    [{5, 8}, {1, 3}] = ra_seq:limit(8, S),
    [{5, 7}, {1, 3}] = ra_seq:limit(7, S),
    [6, 5, {1, 3}] = ra_seq:limit(6, S),
    [5, {1, 3}] = ra_seq:limit(5, S),
    [{1, 3}] = ra_seq:limit(4, S),
    [{1, 3}] = ra_seq:limit(3, S),
    [2, 1] = ra_seq:limit(2, S),
    [1] = ra_seq:limit(1, S),
    [] = ra_seq:limit(0, S),
    ok.

add(_Config) ->
    S1 = ra_seq:from_list([1, 2, 3, 5, 6]),
    S2 = ra_seq:from_list([7, 8, 9, 11]),
    [11, {5, 9}, {1, 3}] = ra_seq:add(S2, S1),

    S3 = ra_seq:from_list([1, 2, 3, 5, 6, 7, 8]),
    S4 = ra_seq:from_list([7, 8, 9, 11]),
    [11, {5, 9}, {1, 3}] = ra_seq:add(S4, S3),
    ok.
