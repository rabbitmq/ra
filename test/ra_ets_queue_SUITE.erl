-module(ra_ets_queue_SUITE).

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
     many,
     reset
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
    Q0 = ra_ets_queue:new(),
    ?assertMatch(0, ra_ets_queue:len(Q0)),
    Q1 = ra_ets_queue:in(one, Q0),
    ?assertMatch(1, ra_ets_queue:len(Q1)),
    ?assertMatch({[one], _}, ra_ets_queue:take(8, Q1)),
    ok.


many(_Config) ->
    Q0 = ra_ets_queue:new(),
    Items = lists:seq(1, 32),
    Q1 = lists:foldl(fun ra_ets_queue:in/2,
                    Q0, Items),
    ?assertMatch(32, ra_ets_queue:len(Q1)),
    {Out, Q2} = ra_ets_queue:take(32, Q1),
    ?assertEqual(Items, Out),
    ?assertMatch(0, ra_ets_queue:len(Q2)),
    ok.

reset(_Config) ->
    Q0 = ra_ets_queue:new(),
    Items = lists:seq(1, 32),
    Q1 = lists:foldl(fun ra_ets_queue:in/2,
                    Q0, Items),
    ?assertMatch(32, ra_ets_queue:len(Q1)),
    Q2 = ra_ets_queue:reset(Q1),
    ?assertMatch(0, ra_ets_queue:len(Q2)),
    ok.
