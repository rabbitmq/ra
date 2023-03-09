-module(ra_log_cache_SUITE).

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
     basics,
     set_last,
     overwrite,
     trim
    ].

groups() ->
    [
     {tests, [], all_tests()}
    ].

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

basics(_Config) ->
    C0 = ra_log_cache:init(),
    C1 = ra_log_cache:add({1, 1, a}, C0),
    ?assertEqual({1, 1}, ra_log_cache:range(C1)),
    ?assertEqual(1, ra_log_cache:size(C1)),
    ?assertEqual(true, ra_log_cache:needs_flush(C1)),
    C2 = ra_log_cache:flush(C1),
    ?assertEqual({1, 1}, ra_log_cache:range(C2)),
    ?assertEqual(1, ra_log_cache:size(C2)),
    ?assertEqual(false, ra_log_cache:needs_flush(C2)),

    C3 = ra_log_cache:add({2, 1, b}, C2),
    ?assertEqual({1, 2}, ra_log_cache:range(C3)),
    ?assertEqual(2, ra_log_cache:size(C3)),
    ?assertEqual(true, ra_log_cache:needs_flush(C3)),

    C4 = ra_log_cache:trim(1, C3),
    ?assertEqual({2, 2}, ra_log_cache:range(C4)),
    ?assertEqual(1, ra_log_cache:size(C4)),
    ?assertEqual(true, ra_log_cache:needs_flush(C4)),
    ok.

set_last(_Config) ->
    C0 = ra_log_cache:init(),
    C1 = lists:foldl(fun ra_log_cache:add/2,
                     C0, [{I, 1, I} || I <- lists:seq(1, 10)]),
    ?assertEqual({1, 10}, ra_log_cache:range(C1)),
    ?assertEqual(10, ra_log_cache:size(C1)),
    ?assertEqual(true, ra_log_cache:needs_flush(C1)),
    C2 = ra_log_cache:set_last(5, C1),
    ?assertEqual({1, 5}, ra_log_cache:range(C2)),
    ?assertEqual(5, ra_log_cache:size(C2)),
    ?assertEqual(true, ra_log_cache:needs_flush(C2)),
    ok.

overwrite(_Config) ->
    C0 = lists:foldl(fun ra_log_cache:add/2,
                     ra_log_cache:init(),
                     [{I, 1, I} || I <- lists:seq(1, 10)]),
    C1 = ra_log_cache:add({5, 2, 5}, C0),
    ?assertEqual({1, 5}, ra_log_cache:range(C1)),
    ?assertEqual(5, ra_log_cache:size(C1)),
    ?assertEqual(true, ra_log_cache:needs_flush(C1)),
    ok.

trim(_Config) ->
    C0 = ra_log_cache:init(),
    C0 = ra_log_cache:trim(1, C0),
    C1 = ra_log_cache:flush(
           lists:foldl(fun ra_log_cache:add/2,
                       C0, [{I, 1, I} || I <- lists:seq(1, 10)])),
    C2 = ra_log_cache:trim(5, C1),
    ?assertEqual({6, 10}, ra_log_cache:range(C2)),
    ?assertEqual(5, ra_log_cache:size(C2)),

    C3 = ra_log_cache:trim(20, C2),
    ?assertEqual(undefined, ra_log_cache:range(C3)),
    ?assertEqual(0, ra_log_cache:size(C3)),
    ok.


%% Utility
