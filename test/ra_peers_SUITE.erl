-module(ra_peers_SUITE).

-compile(export_all).

-export([
         ]).

-include_lib("common_test/include/ct.hrl").
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
     init,
     match_index,
     update_post_snapshot_installation,
     make_pipeline_rpc_effect

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

init(_Config) ->
    P1 = ra_peers:init(n1, []),
    ?assertEqual(1, ra_peers:count(P1)),
    ?assert(ra_peers:is_member(n1, P1)),
    ?assert(false == ra_peers:is_member(n2, P1)),
    ?assertMatch([n1], ra_peers:all(P1)),
    ?assertMatch([], ra_peers:peer_ids(P1)),
    P2 = ra_peers:add(n2, P1),
    ?assertEqual(2, ra_peers:count(P2)),
    ?assert(ra_peers:is_member(n1, P2)),
    ?assert(ra_peers:is_member(n2, P2)),
    ?assertMatch([n1, n2], lists:sort(ra_peers:all(P2))),
    ?assertMatch([n2], ra_peers:peer_ids(P2)),
    ?assertEqual(P1, ra_peers:remove(n2, P2)),
    ok.

match_index(_Config) ->
    P0 = ra_peers:init(n1, [n2]),
    ?assertEqual(0, ra_peers:match_index(n2, P0)),
    P1 = ra_peers:set_match_index(n2, 4, P0),
    %% Invariant: match index cannot go backwards
    ?assertExit(_, ra_peers:set_match_index(n2, 2, P1)),
    ?assertExit(_, ra_peers:set_match_and_next_index(n2, 2, 5, P1)),
    ok.

update_post_snapshot_installation(_Config) ->
    P0 = ra_peers:set_status(n2, {sending_snapshot, self()},
                             ra_peers:init(n1, [n2])),
    ?assertEqual(0, ra_peers:match_index(n2, P0)),
    P1 = ra_peers:update_post_snapshot_installation(n2, 4, P0),
    %% Invariant: match index cannot go backwards
    ?assertEqual(4, ra_peers:match_index(n2, P1)),
    ?assertEqual(5, ra_peers:next_index(n2, P1)),
    ?assertEqual(normal, ra_peers:status(n2, P1)),
    ok.

make_pipeline_rpc_effect(_Config) ->
    P0 = ra_peers:init(n1, [n2, n3]),
    [] = ra_peers:make_pipeline_rpc_effect(0, 1, P0),
    [{pipeline_rpcs, [n3, n2]}] = ra_peers:make_pipeline_rpc_effect(0, 2, P0),
    P1 = ra_peers:set_next_index(n2, 2, P0),
    [{pipeline_rpcs, [n3]}] = ra_peers:make_pipeline_rpc_effect(0, 2, P1),
    [{pipeline_rpcs, [n3, n2]}] = ra_peers:make_pipeline_rpc_effect(1, 2, P1),
    P2 = ra_peers:set_next_index(n3, 2, P1),
    [] = ra_peers:make_pipeline_rpc_effect(0, 2, P2),
    [{pipeline_rpcs, [n3, n2]}] = ra_peers:make_pipeline_rpc_effect(0, 10000, P2),
    %% go beyond pipeline window - simulating slow or down peer
    P3 = ra_peers:set_next_index(n2, 2 + 10000, P2),
    [{pipeline_rpcs, [n3]}] = ra_peers:make_pipeline_rpc_effect(0, 10000 + 3, P3),
    P4 = ra_peers:set_match_index(n2, 5000, P3),
    [{pipeline_rpcs, [n3, n2]}] = ra_peers:make_pipeline_rpc_effect(0, 10000 + 3, P4),
    ok.

%% Utility
