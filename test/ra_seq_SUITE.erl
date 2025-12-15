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
     add,
     subtract,
     iter,
     remove_prefix,
     remove_prefix_2,
     from_list_with_duplicates
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

subtract(_Config) ->
    [11, {7, 9}, {1, 3}] = ra_seq:subtract([{1, 11}], [10, {4, 6}]),
    ok.

iter(_Config) ->
    S = ra_seq:from_list([1, 2, 3, 5, 6, 8, 9, 10, 12]),
    I0 = ra_seq:iterator(S),
    {1, I1} = ra_seq:next(I0),
    {2, I2} = ra_seq:next(I1),
    {3, I3} = ra_seq:next(I2),
    {5, I4} = ra_seq:next(I3),
    {6, I5} = ra_seq:next(I4),
    {8, I6} = ra_seq:next(I5),
    {9, I7} = ra_seq:next(I6),
    {10, I8} = ra_seq:next(I7),
    {12, I9} = ra_seq:next(I8),
    end_of_seq = ra_seq:next(I9),
    ok.

remove_prefix(_Config) ->
    S0 = ra_seq:from_list([2, 3, 5, 6, 8, 9, 10, 12]),
    Pref1 = ra_seq:from_list([2, 3, 5]),
    {ok, S1} = ra_seq:remove_prefix(Pref1, S0),
    [12, 10, 9, 8, 6] = ra_seq:expand(S1),

    %% prefix includes already removed items
    Pref2 = ra_seq:from_list([1, 2, 3, 5]),
    {ok, S2} = ra_seq:remove_prefix(Pref2, S0),
    [12, 10, 9, 8, 6] = ra_seq:expand(S2),
    %% not a prefix
    Pref3 = ra_seq:from_list([5, 6, 8]),
    {error, not_prefix} = ra_seq:remove_prefix(Pref3, S0),

    {ok, []} = ra_seq:remove_prefix(S0, S0),
    ok.

remove_prefix_2(_Config) ->
    S1 = ra_seq:from_list([2, 3, 4, 5]),
    S2 = ra_seq:from_list([1, 2, 3]),
    {ok, [5, 4]} = ra_seq:remove_prefix(S2, S1),
    ok.

from_list_with_duplicates(_Config) ->
    S1 = ra_seq:from_list([1, 2, 2, 3]),
    [{1, 3}] = S1,
    [3, 2, 1] = ra_seq:expand(S1),

    S2 = ra_seq:from_list([5, 5, 5, 5]),
    [5] = S2,
    [5] = ra_seq:expand(S2),

    S3 = ra_seq:from_list([3, 1, 2, 1, 3, 2]),
    [{1, 3}] = S3,
    [3, 2, 1] = ra_seq:expand(S3),

    S4 = ra_seq:from_list([1, 2, 3, 3, 5, 6, 7, 7, 10, 11, 11]),
    [11, 10, {5, 7}, {1, 3}] = S4,
    [11, 10, 7, 6, 5, 3, 2, 1] = ra_seq:expand(S4),

    ok.
