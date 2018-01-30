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
     follower_takes_over_monitor,
     node_is_deleted,
     node_restart_after_application_restart,
     restarted_node_does_not_reissue_side_effects
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
    NodeName2 = list_to_atom(atom_to_list(TestCase) ++ "2"),
    [
     {uid, atom_to_binary(TestCase, utf8)},
     {node_id, {TestCase, node()}},
     {uid2, atom_to_binary(NodeName2, utf8)},
     {node_id2, {NodeName2, node()}}
     | Config].

first(Config) ->
    PrivDir = ?config(priv_dir, Config),
    NodeId = ?config(node_id, Config),
    UId = ?config(uid, Config),
    Conf = #{id => NodeId,
             uid => UId,
             log_module => ra_log_file,
             log_init_args => #{data_dir => PrivDir, uid => UId},
             initial_nodes => [],
             machine => {module, ra_fifo}},
    _ = ra:start_node(Conf),
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

    _ = ra:stop_node(UId),
    _ = ra:restart_node(Conf),

    _ = ra:send_and_await_consensus(NodeId, {enqueue, two}),
    ct:pal("restarted node"),
    receive
        {ra_event, _, machine, {msg, _, two}} -> ok
    after 2000 ->
              exit(await_msg_timeout)
    end,
    ra:stop_node(NodeId),
    ok.

leader_monitors_customer(Config) ->
    PrivDir = ?config(priv_dir, Config),
    NodeId = ?config(node_id, Config),
    UId = ?config(uid, Config),
    Name = element(1, NodeId),
    Conf = #{id => NodeId,
             uid => UId,
             log_module => ra_log_file,
             log_init_args => #{data_dir => PrivDir, uid => UId},
             initial_nodes => [],
             machine => {module, ra_fifo}},
    _ = ra:start_node(Conf),
    ok = ra:trigger_election(NodeId),
    _ = ra:send_and_await_consensus(NodeId, {checkout, {auto, 10}, self()}),
    {monitored_by, [MonitoredBy]} = erlang:process_info(self(), monitored_by),
    ?assert(MonitoredBy =:= whereis(Name)),
    ra:stop_node(UId),
    _ = ra:restart_node(Conf),
    % check monitors are re-applied after restart
    {ok, _, _} = ra:send_and_await_consensus(NodeId, {enqueue, msg1}),
    {monitored_by, [MonitoredByAfter]} = erlang:process_info(self(), monitored_by),
    ?assert(MonitoredByAfter =:= whereis(Name)),
    ra:stop_node(NodeId),
    ok.

follower_takes_over_monitor(Config) ->
    PrivDir = ?config(priv_dir, Config),
    {Name1, _} = NodeId1 = ?config(node_id, Config),
    {Name2, _} = NodeId2 = ?config(node_id2, Config),
    UId1 = ?config(uid, Config),
    UId2 = ?config(uid2, Config),
    Conf1 = conf(UId1, NodeId1, PrivDir, [NodeId1, NodeId2]),
    Conf2 = conf(UId2, NodeId2, PrivDir, [NodeId1, NodeId2]),
    _ = ra:start_node(Conf1),
    _ = ra:start_node(Conf2),
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
    ra:stop_node(NodeId1),
    ra:stop_node(NodeId2),
    ok.

node_is_deleted(Config) ->
    PrivDir = ?config(priv_dir, Config),
    NodeId = ?config(node_id, Config),
    UId = ?config(uid, Config),
    Conf = #{id => NodeId,
             uid => UId,
             log_module => ra_log_file,
             log_init_args => #{data_dir => PrivDir, uid => UId},
             initial_nodes => [],
             machine => {module, ra_fifo}},
    _ = ra:start_node(Conf),
    ok = ra:trigger_election(NodeId),
    {ok, _, NodeId} = ra:send_and_await_consensus(NodeId, {enqueue, msg1}),
    % force roll over
    ok = ra_log_wal:force_roll_over(ra_log_wal),
    ok = ra:delete_node(NodeId),

    % start a node with the same nodeid but different uid
    % simulatin the case where a queue got deleted then re-declared shortly
    % afterwards
    UId2 = ?config(uid2, Config),
    ok = ra:start_node(Conf#{uid => UId2,
                             log_init_args => #{data_dir => PrivDir,
                                                uid => UId2}}),
    ok = ra:trigger_election(NodeId),
    {ok, _, _} = ra:send_and_await_consensus(NodeId,
                                             {checkout, {auto, 10}, self()}),
    receive
        {ra_event, _, machine, Evt} ->
            exit({unexpected_machine_event, Evt})
    after 500 -> ok
    end,
    ok = ra:delete_node(NodeId),
    ok.

node_restart_after_application_restart(Config) ->
    PrivDir = ?config(priv_dir, Config),
    NodeId = ?config(node_id, Config),
    UId = ?config(uid, Config),
    Conf = #{id => NodeId,
             uid => UId,
             log_module => ra_log_file,
             log_init_args => #{data_dir => PrivDir, uid => UId},
             initial_nodes => [],
             machine => {module, ra_fifo}},
    _ = ra:start_node(Conf),
    ok = ra:trigger_election(NodeId),
    {ok, _, NodeId} = ra:send_and_await_consensus(NodeId, {enqueue, msg1}),
    application:stop(ra),
    application:start(ra),
    % TODO: only until the init race condition in wal is addressed
    timer:sleep(500),
    % restart node
    ok = ra:restart_node(Conf),
    {ok, _, NodeId} = ra:send_and_await_consensus(NodeId, {enqueue, msg2}),
    ok = ra:stop_node(NodeId),
    ok.



% NB: this is not guaranteed not to re-issue side-effects but only tests
% that the likelyhood is small
restarted_node_does_not_reissue_side_effects(Config) ->
    PrivDir = ?config(priv_dir, Config),
    NodeId = ?config(node_id, Config),
    UId = ?config(uid, Config),
    Name = element(1, NodeId),
    Conf = #{id => NodeId,
             uid => UId,
             log_module => ra_log_file,
             log_init_args => #{data_dir => PrivDir, uid => UId},
             initial_nodes => [],
             machine => {module, ra_fifo}},
    _ = ra:start_node(Conf),
    ok = ra:trigger_election(NodeId),
    {ok, _, _} = ra:send_and_await_consensus(NodeId, {checkout, {auto, 10}, self()}),
    {ok, _, _} = ra:send_and_await_consensus(NodeId, {enqueue, msg1}),
    receive
        {ra_event, _, machine, {msg, MsgId, _}} ->
            {ok, _, _} = ra:send_and_await_consensus(NodeId, {settle, MsgId, self()})
    after 2000 ->
              exit(ra_event_timeout)
    end,
    % give the process time to persist the last_applied index
    timer:sleep(1000),
    % kill the process and have it restarted by the supervisor
    exit(whereis(Name), kill),

    %  check message isn't received again
    receive
        {ra_event, _, machine, {msg, _, _}} ->
            exit(unexpected_ra_event)
    after 1000 ->
              ok
    end,
    ok = ra:stop_node(UId),
    ok.

conf(UId, NodeId, Dir, Peers) ->
    #{id => NodeId,
      uid => UId,
      log_module => ra_log_file,
      log_init_args => #{data_dir => Dir, uid => UId},
      initial_nodes => Peers,
      machine => {module, ra_fifo}}.
