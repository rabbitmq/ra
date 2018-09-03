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
     machine_replies,
     leader_monitors,
     follower_takes_over_monitor,
     deleted_cluster_emits_eol_effects
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

leader_monitors(Config) ->
    ClusterId = ?config(priv_dir, Config),
    NodeId = ?config(node_id, Config),
    Name = element(1, NodeId),
    Mod = ?config(modname, Config),
    meck:new(Mod, [non_strict]),
    meck:expect(Mod, init, fun (_) -> {[], []} end),
    meck:expect(Mod, apply, fun (_, {monitor_me, Pid}, _, State) ->
                                    {[Pid | State], [{monitor, process, Pid}], ok}
                            end),
    meck:expect(Mod, leader_effects,
                fun (State) ->
                        [{monitor, process, P} || P <- State]
                end),
    ok = start_cluster(ClusterId, {module, Mod, #{}}, [NodeId]),
    {ok, ok, NodeId} = ra:send_and_await_consensus(NodeId, {monitor_me, self()}),
    {monitored_by, [MonitoredBy]} = erlang:process_info(self(), monitored_by),
    ?assert(MonitoredBy =:= whereis(Name)),
    ra:stop_node(NodeId),
    _ = ra:restart_node(NodeId),
    ra:members(NodeId),
    % check monitors are re-applied after restart
    timer:sleep(200),
    {monitored_by, [MonitoredByAfter]} = erlang:process_info(self(),
                                                             monitored_by),
    ?assert(MonitoredByAfter =:= whereis(Name)),
    ra:stop_node(NodeId),
    ok.

follower_takes_over_monitor(Config) ->
    ClusterId = ?config(cluster_id, Config),
    {Name1, _} = NodeId1 = ?config(node_id, Config),
    {Name2, _} = NodeId2 = ?config(node_id2, Config),
    {Name3, _} = NodeId3 = ?config(node_id3, Config),
    Cluster = [NodeId1, NodeId2, NodeId3],
    Mod = ?config(modname, Config),
    meck:new(Mod, [non_strict]),
    meck:expect(Mod, init, fun (_) -> {[], []} end),
    meck:expect(Mod, apply,
                fun (_, {monitor_me, Pid}, _, State) ->
                        {[Pid | State], [{monitor, process, Pid}], ok};
                    (_, Cmd, _, State) ->
                        ct:pal("handling ~p", [Cmd]),
                        %% handle all
                        {State, []}
                end),
    meck:expect(Mod, leader_effects,
                fun (State) ->
                        [{monitor, process, P} || P <- State]
                end),
    ok = start_cluster(ClusterId, {module, Mod, #{}}, Cluster),
    {ok, ok, _} = ra:send_and_await_consensus(NodeId1, {monitor_me, self()}),
    %% sleep here as it seems monitors, or this stat aren't updated synchronously
    timer:sleep(100),
    {monitored_by, [MonitoredBy]} = erlang:process_info(self(), monitored_by),
    ?assert(MonitoredBy =:= whereis(Name1)),

    ok = ra:stop_node(NodeId1),
    % give the election process a bit of time before issuing a command
    timer:sleep(200),
    {ok, _, _} = ra:send_and_await_consensus(NodeId2, dummy),
    timer:sleep(200),

    {monitored_by, [MonitoredByAfter]} = erlang:process_info(self(),
                                                             monitored_by),
    ?assert((MonitoredByAfter =:= whereis(Name2)) or
            (MonitoredByAfter =:= whereis(Name3))),
    ra:stop_node(NodeId1),
    ra:stop_node(NodeId2),
    ra:stop_node(NodeId3),
    ok.

deleted_cluster_emits_eol_effects(Config) ->
    PrivDir = ?config(priv_dir, Config),
    NodeId = ?config(node_id, Config),
    UId = ?config(uid, Config),
    ClusterId = ?config(cluster_id, Config),
    Mod = ?config(modname, Config),
    meck:new(Mod, [non_strict]),
    meck:expect(Mod, init, fun (_) -> {[], []} end),
    meck:expect(Mod, apply,
                fun (_, {monitor_me, Pid}, _, State) ->
                        {[Pid | State], [{monitor, process, Pid}], ok}
                end),
    meck:expect(Mod, eol_effects,
                fun (State) ->
                        [{send_msg, P, eol} || P <- State]
                end),
    ok = start_cluster(ClusterId, {module, Mod, #{}}, [NodeId]),
    {ok, ok, _} = ra:send_and_await_consensus(NodeId, {monitor_me, self()}),
    {ok, _} = ra:delete_cluster([NodeId]),
    % validate
    ok = validate_process_down(element(1, NodeId), 50),
    Dir = filename:join(PrivDir, UId),
    false = filelib:is_dir(Dir),
    [] = supervisor:which_children(ra_nodes_sup),
    % validate an end of life is emitted
    receive
        {ra_event, _, {machine, eol}} -> ok
    after 500 ->
          exit(timeout)
    end,
    ok.

%% Utility

start_cluster(ClusterId, Machine, NodeIds) ->
    {ok, NodeIds, _} = ra:start_cluster(ClusterId, Machine, NodeIds),
    ok.

validate_process_down(Name, 0) ->
    exit({process_not_down, Name});
validate_process_down(Name, Num) ->
    case whereis(Name) of
        undefined ->
            ok;
        _ ->
            timer:sleep(100),
            validate_process_down(Name, Num-1)
    end.
