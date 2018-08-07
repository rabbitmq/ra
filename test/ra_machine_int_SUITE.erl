-module(ra_machine_int_SUITE).

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
     machine_replies
    ].

groups() ->
    [
     {tests, [], all_tests()}
    ].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(_, Config) ->
    PrivDir = ?config(priv_dir, Config),
    _ = application:load(ra),
    ok = application:set_env(ra, data_dir, PrivDir),
    application:ensure_all_started(ra),
    Config.

end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(TestCase, Config) ->
    ra_nodes_sup:remove_all(),
    NodeName2 = list_to_atom(atom_to_list(TestCase) ++ "2"),
    NodeName3 = list_to_atom(atom_to_list(TestCase) ++ "3"),
    [
     {modname, TestCase},
     {cluster_id, TestCase},
     {uid, atom_to_binary(TestCase, utf8)},
     {node_id, {TestCase, node()}},
     {uid2, atom_to_binary(NodeName2, utf8)},
     {node_id2, {NodeName2, node()}},
     {uid3, atom_to_binary(NodeName3, utf8)},
     {node_id3, {NodeName3, node()}}
     | Config].

end_per_testcase(_TestCase, _Config) ->
    meck:unload(),
    ok.

%%%===================================================================
%%% Test cases
%%%===================================================================

machine_replies(Config) ->
    Mod = ?config(modname, Config),
    meck:new(Mod, [non_strict]),
    meck:expect(Mod, init, fun (_) -> {the_state, []} end),
    meck:expect(Mod, apply, fun (_, c1, _, State) ->
                                    {State, [], the_reply};
                                (_, c2, _, State) ->
                                    {State, [], {error, some_error_reply}}
                            end),
    ClusterId = ?config(cluster_id, Config),
    NodeId = ?config(node_id, Config),
    ok = start_cluster(ClusterId, {module, Mod, #{}}, [NodeId]),
    {ok, the_reply, NodeId} = ra:send_and_await_consensus(NodeId, c1),
    %% ensure we can return any reply type
    {ok, {error, some_error_reply}, NodeId} = ra:send_and_await_consensus(NodeId, c2),
    ok.

%% Utility

start_cluster(ClusterId, Machine, NodeIds) ->
    {ok, NodeIds, _} = ra:start_cluster(ClusterId, Machine, NodeIds),
    ok.
