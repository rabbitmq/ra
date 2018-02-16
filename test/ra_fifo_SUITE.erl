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
     ra_fifo_client_basics,
     ra_fifo_client_returns_correlation,
     ra_fifo_client_resends_lost_command,
     ra_fifo_client_resends_after_lost_applied,
     ra_fifo_client_handles_reject_notification,
     ra_fifo_client_two_quick_enqueues,
     ra_fifo_client_detects_lost_delivery,
     leader_monitors_customer,
     follower_takes_over_monitor,
     node_is_deleted,
     node_restart_after_application_restart,
     restarted_node_does_not_reissue_side_effects,
     ra_fifo_client_dequeue
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

ra_fifo_client_basics(Config) ->
    PrivDir = ?config(priv_dir, Config),
    NodeId = ?config(node_id, Config),
    UId = ?config(uid, Config),
    CustomerTag = UId,
    Conf = #{id => NodeId,
             uid => UId,
             log_module => ra_log_file,
             log_init_args => #{data_dir => PrivDir, uid => UId},
             initial_nodes => [],
             machine => {module, ra_fifo}},
    _ = ra:start_node(Conf),
    ok = ra:trigger_election(NodeId),
    FState0 = ra_fifo_client:init([NodeId]),
    {ok, FState1} = ra_fifo_client:checkout(CustomerTag, 1, FState0),
    % _ = ra:send_and_await_consensus(NodeId, {checkout, {auto, 10}, Cid}),

    ra_log_wal:force_roll_over(ra_log_wal),
    % create segment the segment will trigger a snapshot
    timer:sleep(1000),

    {ok, FState2} = ra_fifo_client:enqueue(one, FState1),
    % process ra events
    FState3 = process_ra_event(FState2, 250),

    FState5 = receive
                  {ra_event, From, Evt} ->
                      case ra_fifo_client:handle_ra_event(From, Evt, FState3) of
                          {internal, _AcceptedSeqs, _FState4} ->
                              exit(unexpected_internal_event);
                          {{delivery, C, [{MsgId, _Msg}]}, FState4} ->
                              {ok, S} = ra_fifo_client:settle(C, [MsgId],
                                                              FState4),
                              S
                      end
              after 5000 ->
                        exit(await_msg_timeout)
              end,

    % process settle applied notificaiton
    FState5b = process_ra_event(FState5, 250),
    _ = ra:stop_node(UId),
    _ = ra:restart_node(Conf),

    % give time to become leader
    timer:sleep(500),
    {ok, FState6} = ra_fifo_client:enqueue(two, FState5b),
    % process applied event
    FState6b = process_ra_event(FState6, 250),

    receive
        {ra_event, Frm, E} ->
            case ra_fifo_client:handle_ra_event(Frm, E, FState6b) of
                {internal, _, _FState7} ->
                    ct:pal("unexpected event ~p~n", [E]),
                    exit({unexpected_internal_event, E});
                {{delivery, Ctag, [{Mid, {_, two}}]}, FState7} ->
                    {ok, _S} = ra_fifo_client:return(Ctag, [Mid], FState7),
                    ok
            end
    after 2000 ->
              exit(await_msg_timeout)
    end,
    ra:stop_node(NodeId),
    ok.

ra_fifo_client_returns_correlation(Config) ->
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
    F0 = ra_fifo_client:init([NodeId]),
    {ok, F1} = ra_fifo_client:enqueue(corr1, msg1, F0),
    receive
        {ra_event, Frm, E} ->
            case ra_fifo_client:handle_ra_event(Frm, E, F1) of
                {internal, [corr1], _F2} ->
                    ok;
                {Del, _} ->
                    exit({unexpected, Del})
            end
    after 2000 ->
              exit(await_msg_timeout)
    end,
    ok.

ra_fifo_client_resends_lost_command(Config) ->
    PrivDir = ?config(priv_dir, Config),
    NodeId = ?config(node_id, Config),
    UId = ?config(uid, Config),
    Conf = conf(UId, NodeId, PrivDir, []),
    _ = ra:start_node(Conf),
    ok = ra:trigger_election(NodeId),
    timer:sleep(100),

    ok = meck:new(ra, [passthrough]),

    F0 = ra_fifo_client:init([NodeId]),
    {ok, F1} = ra_fifo_client:enqueue(msg1, F0),
    % lose the enqueue
    meck:expect(ra, send_and_notify, fun (_, _, _) -> ok end),
    {ok, F2} = ra_fifo_client:enqueue(msg2, F1),
    meck:unload(ra),
    {ok, F3} = ra_fifo_client:enqueue(msg3, F2),
    {_, F4} = process_ra_events(F3, 500),
    {ok, {_, {_, msg1}}, F5} = ra_fifo_client:dequeue(<<"tag">>, settled, F4),
    {ok, {_, {_, msg2}}, F6} = ra_fifo_client:dequeue(<<"tag">>, settled, F5),
    {ok, {_, {_, msg3}}, _F7} = ra_fifo_client:dequeue(<<"tag">>, settled, F6),
    ok.

ra_fifo_client_two_quick_enqueues(Config) ->
    PrivDir = ?config(priv_dir, Config),
    NodeId = ?config(node_id, Config),
    UId = ?config(uid, Config),
    Conf = conf(UId, NodeId, PrivDir, []),
    _ = ra:start_node(Conf),
    ok = ra:trigger_election(NodeId),
    timer:sleep(100),

    F0 = ra_fifo_client:init([NodeId]),
    F1 = element(2, ra_fifo_client:enqueue(msg1, F0)),
    {ok, F2} = ra_fifo_client:enqueue(msg2, F1),
    _ = process_ra_events(F2, 500),
    ok.

ra_fifo_client_detects_lost_delivery(Config) ->
    PrivDir = ?config(priv_dir, Config),
    NodeId = ?config(node_id, Config),
    UId = ?config(uid, Config),
    Conf = conf(UId, NodeId, PrivDir, []),
    _ = ra:start_node(Conf),
    ok = ra:trigger_election(NodeId),
    timer:sleep(100),

    F00 = ra_fifo_client:init([NodeId]),
    {ok, F0} = ra_fifo_client:checkout(<<"tag">>, 10, F00),
    {ok, F1} = ra_fifo_client:enqueue(msg1, F0),
    {ok, F2} = ra_fifo_client:enqueue(msg2, F1),
    {ok, F3} = ra_fifo_client:enqueue(msg3, F2),
    % lose first delivery
    receive
        {ra_event, _, {machine, {delivery, _, [{_, {_, msg1}}]}}} ->
            ok
    after 250 ->
              exit(await_delivery_timeout)
    end,

    % assert three deliveries were received
    {[_, _, _], _} = process_ra_events(F3, 500),
    ok.

ra_fifo_client_resends_after_lost_applied(Config) ->
    PrivDir = ?config(priv_dir, Config),
    NodeId = ?config(node_id, Config),
    UId = ?config(uid, Config),
    Conf = conf(UId, NodeId, PrivDir, []),
    _ = ra:start_node(Conf),
    ok = ra:trigger_election(NodeId),
    timer:sleep(100),

    F0 = ra_fifo_client:init([NodeId]),
    {_, F1} = process_ra_events(element(2, ra_fifo_client:enqueue(msg1, F0)),
                           500),
    {ok, F2} = ra_fifo_client:enqueue(msg2, F1),
    % lose an applied event
    receive
        {ra_event, _, {applied, _}} ->
            ok
    after 500 ->
              exit(await_ra_event_timeout)
    end,
    % send another message
    {ok, F3} = ra_fifo_client:enqueue(msg3, F2),
    {_, F4} = process_ra_events(F3, 500),
    {ok, {_, {_, msg1}}, F5} = ra_fifo_client:dequeue(<<"tag">>, settled, F4),
    {ok, {_, {_, msg2}}, F6} = ra_fifo_client:dequeue(<<"tag">>, settled, F5),
    {ok, {_, {_, msg3}}, _F7} = ra_fifo_client:dequeue(<<"tag">>, settled, F6),
    ok.

ra_fifo_client_handles_reject_notification(Config) ->
    PrivDir = ?config(priv_dir, Config),
    NodeId1 = ?config(node_id, Config),
    NodeId2 = ?config(node_id2, Config),
    UId1 = ?config(uid, Config),
    CId = {UId1, self()},
    UId2 = ?config(uid2, Config),
    Conf1 = conf(UId1, NodeId1, PrivDir, [NodeId1, NodeId2]),
    Conf2 = conf(UId2, NodeId2, PrivDir, [NodeId1, NodeId2]),
    _ = ra:start_node(Conf1),
    _ = ra:start_node(Conf2),
    ok = ra:trigger_election(NodeId1),
    _ = ra:send_and_await_consensus(NodeId1, {checkout, {auto, 10}, CId}),
    % reverse order - should try the first node in the list first
    F0 = ra_fifo_client:init([NodeId2, NodeId1]),
    {ok, F1} = ra_fifo_client:enqueue(one, F0),

    timer:sleep(500),

    % the applied notification
    _F2 = process_ra_event(F1, 250),
    ra:stop_node(NodeId1),
    ra:stop_node(NodeId2),
    ok.

leader_monitors_customer(Config) ->
    PrivDir = ?config(priv_dir, Config),
    NodeId = ?config(node_id, Config),
    UId = ?config(uid, Config),
    Tag = UId,
    Name = element(1, NodeId),
    Conf = #{id => NodeId,
             uid => UId,
             log_module => ra_log_file,
             log_init_args => #{data_dir => PrivDir, uid => UId},
             initial_nodes => [],
             machine => {module, ra_fifo}},
    _ = ra:start_node(Conf),
    ok = ra:trigger_election(NodeId),
    F0 =  ra_fifo_client:init([NodeId]),
    {ok, F1} = ra_fifo_client:checkout(Tag, 10, F0),
    {monitored_by, [MonitoredBy]} = erlang:process_info(self(), monitored_by),
    ?assert(MonitoredBy =:= whereis(Name)),
    ra:stop_node(UId),
    _ = ra:restart_node(Conf),
    % check monitors are re-applied after restart
    {ok, _F3} = ra_fifo_client:checkout(Tag, 5, F1),
    {monitored_by, [MonitoredByAfter]} = erlang:process_info(self(),
                                                             monitored_by),
    ?assert(MonitoredByAfter =:= whereis(Name)),
    ra:stop_node(NodeId),
    ok.

follower_takes_over_monitor(Config) ->
    PrivDir = ?config(priv_dir, Config),
    {Name1, _} = NodeId1 = ?config(node_id, Config),
    {Name2, _} = NodeId2 = ?config(node_id2, Config),
    UId1 = ?config(uid, Config),
    Tag = UId1,
    UId2 = ?config(uid2, Config),
    Conf1 = conf(UId1, NodeId1, PrivDir, [NodeId1, NodeId2]),
    Conf2 = conf(UId2, NodeId2, PrivDir, [NodeId1, NodeId2]),
    _ = ra:start_node(Conf1),
    _ = ra:start_node(Conf2),
    ok = ra:trigger_election(NodeId1),
    F0 =  ra_fifo_client:init([NodeId1, NodeId2]),
    {ok, F1} = ra_fifo_client:checkout(Tag, 10, F0),
    % _ = ra:send_and_await_consensus(NodeId1, {checkout, {auto, 10}, CId}),
    % timer:sleep(500),
    {monitored_by, [MonitoredBy]} = erlang:process_info(self(), monitored_by),
    ?assert(MonitoredBy =:= whereis(Name1)),

    ok = ra:trigger_election(NodeId2),
    % give the election process a bit of time before issuing a command
    timer:sleep(100),

    {ok, _F2} = ra_fifo_client:checkout(Tag, 10, F1),
    % {ok, _, NodeId2} = ra:send_and_await_consensus(NodeId2, {enqueue, msg1}),

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
    F0 = ra_fifo_client:init([NodeId]),
    {ok, F1} = ra_fifo_client:enqueue(msg1, F0),
    _ = process_ra_event(F1, 250),
    % {ok, _, NodeId} = ra:send_and_await_consensus(NodeId, {enqueue, msg1}),
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
    F = ra_fifo_client:init([NodeId]),
    {ok, _} = ra_fifo_client:checkout(<<"tag">>, 10, F),
    % {ok, _, _} = ra:send_and_await_consensus(NodeId,
    %                                          {checkout, {auto, 10}, CId}),
    receive
        {ra_event, _, Evt} ->
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
    F0 = ra_fifo_client:init([NodeId]),
    {ok, F1} = ra_fifo_client:checkout(<<"tag">>, 10, F0),
    application:stop(ra),
    application:start(ra),
    % restart node
    ok = ra:restart_node(Conf),
    {ok, _} = ra_fifo_client:checkout(<<"tag2">>, 10, F1),
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
    F0 = ra_fifo_client:init([NodeId]),
    {ok, F1} = ra_fifo_client:checkout(<<"tag">>, 10, F0),
    {ok, F2} = ra_fifo_client:enqueue(<<"msg">>, F1),
    F3 = process_ra_event(F2, 100),
    receive
        {ra_event, From, Evt = {machine, {delivery, C, [{MsgId, _}]}}} ->
            case ra_fifo_client:handle_ra_event(From, Evt, F3) of
                {internal, _, _} ->
                    exit(unexpected_internal);
                {{delivery, C, [{MsgId, _}]}, F4} ->
                    _ = ra_fifo_client:settle(C, [MsgId], F4)
            end
    after 2000 ->
              exit(ra_fifo_event_timeout)
    end,
    % give the process time to persist the last_applied index
    timer:sleep(1000),
    % kill the process and have it restarted by the supervisor
    exit(whereis(Name), kill),

    %  check message isn't received again
    receive
        {ra_event, _, {machine, {delivery, _, _}}} ->
            exit(unexpected_ra_fifo_event)
    after 1000 ->
              ok
    end,
    ok = ra:stop_node(UId),
    ok.

ra_fifo_client_dequeue(Config) ->
    PrivDir = ?config(priv_dir, Config),
    NodeId = ?config(node_id, Config),
    UId = ?config(uid, Config),
    Tag = UId,
    Conf = #{id => NodeId,
             uid => UId,
             log_module => ra_log_file,
             log_init_args => #{data_dir => PrivDir, uid => UId},
             initial_nodes => [],
             machine => {module, ra_fifo}},
    _ = ra:start_node(Conf),
    ok = ra:trigger_election(NodeId),
    F1 = ra_fifo_client:init([NodeId]),
    {ok, empty, F1b} = ra_fifo_client:dequeue(Tag, settled, F1),
    {ok, F2} = ra_fifo_client:enqueue(msg1, F1b),
    {ok, {0, {_, msg1}}, F3} = ra_fifo_client:dequeue(Tag, settled, F2),
    {ok, F4} = ra_fifo_client:enqueue(msg2, F3),
    {ok, {MsgId, {_, msg2}}, F5} = ra_fifo_client:dequeue(Tag, unsettled, F4),
    {ok, F6} = ra_fifo_client:settle(Tag, [MsgId], F5),
    ct:pal("F6 ~p~n", [F6]),
    ok.

conf(UId, NodeId, Dir, Peers) ->
    #{id => NodeId,
      uid => UId,
      log_module => ra_log_file,
      log_init_args => #{data_dir => Dir, uid => UId},
      initial_nodes => Peers,
      machine => {module, ra_fifo}}.

process_ra_event(State, Wait) ->
    receive
        {ra_event, From, Evt} ->
            ct:pal("processed ra event ~p~n", [Evt]),
            {internal, _, S} = ra_fifo_client:handle_ra_event(From, Evt, State),
            S
    after Wait ->
              exit(ra_event_timeout)
    end.

process_ra_events(State0, Wait) ->
    process_ra_events(State0, [], Wait).

process_ra_events(State0, Acc, Wait) ->
    receive
        {ra_event, From, Evt} ->
            case ra_fifo_client:handle_ra_event(From, Evt, State0) of
                {internal, _, State} ->
                    process_ra_events(State, Acc, Wait);
                {{delivery, MsgId, Msgs}, State1} ->
                    {ok, State} = ra_fifo_client:settle(<<"tag">>, [MsgId], State1),
                    process_ra_events(State, Acc ++ Msgs, Wait)
            end
    after Wait ->
              {Acc, State0}
    end.

