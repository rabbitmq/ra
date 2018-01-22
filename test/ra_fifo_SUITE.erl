-module(ra_fifo_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

%% common ra_log tests to ensure behaviour is equivalent across
%% ra_log backends

all() ->
    [
     {group, tests}
    ].

all_tests() ->
    [
     first,
     leader_monitors_customer,
     follower_takes_over_monitor
    ].

groups() ->
    [
     {tests, [], all_tests()}
    ].

init_per_group(_, Config) ->
    PrivDir = ?config(priv_dir, Config),
    _ = application:load(ra),
    ok = application:set_env(ra, data_dir, PrivDir),
    application:ensure_all_started(ra),
    Config.

end_per_group(_, Config) ->
    _ = application:stop(ra),
    Config.

init_per_testcase(TestCase, Config) ->
    case ets:info(ra_fifo_metrics) of
        undefined ->
            _ = ets:new(ra_fifo_metrics, [public, named_table, {write_concurrency, true}]);
        _ ->
            ok
    end,
    NodeName2 = list_to_atom(atom_to_list(TestCase) ++ "2"),
    [{node_id, {TestCase, node()}}, {node_id2, {NodeName2, node()}}
     |  Config].

first(Config) ->
    PrivDir = ?config(priv_dir, Config),
    NodeId = ?config(node_id, Config),
    Name = element(1, NodeId),
    Conf = #{id => NodeId,
             log_module => ra_log_file,
             log_init_args => #{data_dir => PrivDir, id => Name},
             initial_nodes => [],
             machine => {module, ra_fifo}},
    _ = ets:insert(ra_fifo_metrics, {Name, 0, 0, 0, 0}),
    _ = ra_nodes_sup:start_node(Conf),
    ok = ra:trigger_election(NodeId),
    _ = ra:send_and_await_consensus(NodeId, {checkout, {auto, 10}, self()}),

    ra_log_wal:force_roll_over(ra_log_wal),
    % create segment the segment will trigger a snapshot
    timer:sleep(1000),

    _ = ra:send_and_await_consensus(NodeId, {enqueue, one}),
    receive
        {ra_event, _, machine, {msg, MsgId, _}} ->
            _ = ra:send_and_await_consensus(NodeId, {settle, MsgId, self()})
    after 5000 ->
              exit(await_msg_timeout)
    end,

    _ = ra_nodes_sup:stop_node(NodeId),
    _ = ra_nodes_sup:restart_node(NodeId),

    _ = ra:send_and_await_consensus(NodeId, {enqueue, two}),
    ct:pal("restarted node"),
    receive
        {ra_event, _, machine, {msg, _, two}} -> ok
    after 2000 ->
              exit(await_msg_timeout)
    end,
    ra_nodes_sup:stop_node(NodeId),
    ok.

leader_monitors_customer(Config) ->
    PrivDir = ?config(priv_dir, Config),
    NodeId = ?config(node_id, Config),
    Name = element(1, NodeId),
    Conf = #{id => NodeId,
             log_module => ra_log_file,
             log_init_args => #{data_dir => PrivDir, id => Name},
             initial_nodes => [],
             machine => {module, ra_fifo}},
    _ = ets:insert(ra_fifo_metrics, {Name, 0, 0, 0, 0}),
    _ = ra_nodes_sup:start_node(Conf),
    ok = ra:trigger_election(NodeId),
    _ = ra:send_and_await_consensus(NodeId, {checkout, {auto, 10}, self()}),
    {monitored_by, [MonitoredBy]} = erlang:process_info(self(), monitored_by),
    ?assert(MonitoredBy =:= whereis(Name)),
    ra_nodes_sup:stop_node(NodeId),
    _ = ra_nodes_sup:restart_node(NodeId),
    % check monitors are re-applied after restart
    _ = ra:send_and_await_consensus(NodeId, {enqueue, msg1}),
    {monitored_by, [MonitoredByAfter]} = erlang:process_info(self(), monitored_by),
    ?assert(MonitoredByAfter =:= whereis(Name)),
    ra_nodes_sup:stop_node(NodeId),
    ok.

follower_takes_over_monitor(Config) ->
    PrivDir = ?config(priv_dir, Config),
    {Name1, _} = NodeId1 = ?config(node_id, Config),
    {Name2, _} = NodeId2 = ?config(node_id2, Config),
    Conf1 = conf(NodeId1, PrivDir, [NodeId1, NodeId2]),
    Conf2 = conf(NodeId2, PrivDir, [NodeId1, NodeId2]),
    _ = ets:insert(ra_fifo_metrics, {Name1, 0, 0, 0, 0}),
    _ = ets:insert(ra_fifo_metrics, {Name2, 0, 0, 0, 0}),
    _ = ra_nodes_sup:start_node(Conf1),
    _ = ra_nodes_sup:start_node(Conf2),
    ok = ra:trigger_election(NodeId1),
    _ = ra:send_and_await_consensus(NodeId1, {checkout, {auto, 10}, self()}),
    timer:sleep(500),
    {monitored_by, [MonitoredBy]} = erlang:process_info(self(), monitored_by),
    ?assert(MonitoredBy =:= whereis(Name1)),

    ok = ra:trigger_election(NodeId2),
    {ok, _, NodeId2} = ra:send_and_await_consensus(NodeId2,
                                                   {enqueue, msg1}),

    {monitored_by, [MonitoredByAfter]} = erlang:process_info(self(), monitored_by),
    ?assert(MonitoredByAfter =:= whereis(Name2)),
    ra_nodes_sup:stop_node(NodeId1),
    ra_nodes_sup:stop_node(NodeId2),
    ok.


conf({Name, _} = NodeId, Dir, Peers) ->
    #{id => NodeId,
      log_module => ra_log_file,
      log_init_args => #{data_dir => Dir, id => Name},
      initial_nodes => Peers,
      machine => {module, ra_fifo}}.
