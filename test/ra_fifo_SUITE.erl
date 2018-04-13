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
     ra_fifo_client_return,
     ra_fifo_returns_correlation,
     ra_fifo_client_resends_lost_command,
     ra_fifo_client_returns_after_down,
     ra_fifo_client_resends_after_lost_applied,
     ra_fifo_client_handles_reject_notification,
     ra_fifo_client_two_quick_enqueues,
     ra_fifo_client_detects_lost_delivery,
     leader_monitors_customer,
     follower_takes_over_monitor,
     node_is_deleted,
     cluster_is_deleted_with_node_down,
     cluster_is_deleted,
     cluster_cannot_be_deleted_in_minority,
     node_restart_after_application_restart,
     restarted_node_does_not_reissue_side_effects,
     ra_fifo_client_dequeue,
     ra_fifo_client_discard,
     ra_fifo_client_cancel_checkout,
     ra_fifo_client_untracked_enqueue,
     ra_fifo_client_flow,
     test_queries,
     log_fold,
     startup_performance_test
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
    application:ensure_all_started(lg),
    Config.

end_per_group(_, Config) ->
    _ = application:stop(ra),
    Config.

init_per_testcase(TestCase, Config) ->
    ra_nodes_sup:remove_all(),
    NodeName2 = list_to_atom(atom_to_list(TestCase) ++ "2"),
    NodeName3 = list_to_atom(atom_to_list(TestCase) ++ "3"),
    [
     {cluster_id, TestCase},
     {uid, atom_to_binary(TestCase, utf8)},
     {node_id, {TestCase, node()}},
     {uid2, atom_to_binary(NodeName2, utf8)},
     {node_id2, {NodeName2, node()}},
     {uid3, atom_to_binary(NodeName3, utf8)},
     {node_id3, {NodeName3, node()}}
     | Config].

ra_fifo_client_basics(Config) ->
    ClusterId = ?config(cluster_id, Config),
    NodeId = ?config(node_id, Config),
    UId = ?config(uid, Config),
    CustomerTag = UId,
    ok = start_cluster(ClusterId, [NodeId]),
    FState0 = ra_fifo_client:init(ClusterId, [NodeId]),
    {ok, FState1} = ra_fifo_client:checkout(CustomerTag, 1, FState0),

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
    _ = ra:stop_node(NodeId),
    _ = ra:restart_node(NodeId),

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

ra_fifo_client_return(Config) ->
    ClusterId = ?config(cluster_id, Config),
    NodeId = ?config(node_id, Config),
    NodeId2 = ?config(node_id2, Config),
    ok = start_cluster(ClusterId, [NodeId, NodeId2]),

    F00 = ra_fifo_client:init(ClusterId, [NodeId, NodeId2]),
    {ok, F0} = ra_fifo_client:enqueue(1, msg1, F00),
    {ok, F} = ra_fifo_client:enqueue(2, msg2, F0),
    {ok, {MsgId, _}, F1} = ra_fifo_client:dequeue(<<"tag">>, unsettled, F),
    {ok, _F2} = ra_fifo_client:return(<<"tag">>, [MsgId], F1),

    % F2 = return_next_delivery(F1, 500),
    % _F3 = discard_next_delivery(F2, 500),
    ra:stop_node(NodeId),
    ok.

ra_fifo_returns_correlation(Config) ->
    ClusterId = ?config(cluster_id, Config),
    NodeId = ?config(node_id, Config),
    ok = start_cluster(ClusterId, [NodeId]),
    F0 = ra_fifo_client:init(ClusterId, [NodeId]),
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
    ra:stop_node(NodeId),
    ok.

ra_fifo_client_resends_lost_command(Config) ->
    ClusterId = ?config(cluster_id, Config),
    NodeId = ?config(node_id, Config),
    ok = start_cluster(ClusterId, [NodeId]),

    ok = meck:new(ra, [passthrough]),

    F0 = ra_fifo_client:init(ClusterId, [NodeId]),
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
    ra:stop_node(NodeId),
    ok.

ra_fifo_client_two_quick_enqueues(Config) ->
    ClusterId = ?config(cluster_id, Config),
    NodeId = ?config(node_id, Config),
    ok = start_cluster(ClusterId, [NodeId]),

    F0 = ra_fifo_client:init(ClusterId, [NodeId]),
    F1 = element(2, ra_fifo_client:enqueue(msg1, F0)),
    {ok, F2} = ra_fifo_client:enqueue(msg2, F1),
    _ = process_ra_events(F2, 500),
    ra:stop_node(NodeId),
    ok.

ra_fifo_client_detects_lost_delivery(Config) ->
    ClusterId = ?config(cluster_id, Config),
    NodeId = ?config(node_id, Config),
    ok = start_cluster(ClusterId, [NodeId]),

    F00 = ra_fifo_client:init(ClusterId, [NodeId]),
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
    ra:stop_node(NodeId),
    ok.

ra_fifo_client_returns_after_down(Config) ->
    ClusterId = ?config(cluster_id, Config),
    NodeId = ?config(node_id, Config),
    ok = start_cluster(ClusterId, [NodeId]),

    F0 = ra_fifo_client:init(ClusterId, [NodeId]),
    {ok, F1} = ra_fifo_client:enqueue(msg1, F0),
    {_, F2} = process_ra_events(F1, 500),
    % start a customer in a separate processes
    % that exits after checkout
    Self = self(),
    _Pid = spawn(fun () ->
                         F = ra_fifo_client:init(ClusterId, [NodeId]),
                         {ok, _} = ra_fifo_client:checkout(<<"tag">>, 10, F),
                         Self ! checkout_done
                 end),
    receive checkout_done -> ok after 1000 -> exit(checkout_done_timeout) end,
    % message should be available for dequeue
    {ok, {_, {_, msg1}}, _} = ra_fifo_client:dequeue(<<"tag">>, settled, F2),
    ra:stop_node(NodeId),
    ok.

ra_fifo_client_resends_after_lost_applied(Config) ->
    ClusterId = ?config(cluster_id, Config),
    NodeId = ?config(node_id, Config),
    ok = start_cluster(ClusterId, [NodeId]),

    F0 = ra_fifo_client:init(ClusterId, [NodeId]),
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
    ra:stop_node(NodeId),
    ok.

ra_fifo_client_handles_reject_notification(Config) ->
    ClusterId = ?config(cluster_id, Config),
    NodeId1 = ?config(node_id, Config),
    NodeId2 = ?config(node_id2, Config),
    UId1 = ?config(uid, Config),
    CId = {UId1, self()},

    ok = start_cluster(ClusterId, [NodeId1, NodeId2]),
    _ = ra:send_and_await_consensus(NodeId1, {checkout, {auto, 10}, CId}),
    % reverse order - should try the first node in the list first
    F0 = ra_fifo_client:init(ClusterId, [NodeId2, NodeId1]),
    {ok, F1} = ra_fifo_client:enqueue(one, F0),

    timer:sleep(500),

    % the applied notification
    _F2 = process_ra_event(F1, 250),
    ra:stop_node(NodeId1),
    ra:stop_node(NodeId2),
    ok.

ra_fifo_client_discard(Config) ->
    PrivDir = ?config(priv_dir, Config),
    NodeId = ?config(node_id, Config),
    UId = ?config(uid, Config),
    ClusterId = ?config(cluster_id, Config),
    Conf = #{cluster_id => ClusterId,
             id => NodeId,
             uid => UId,
             log_module => ra_log_file,
             log_init_args => #{data_dir => PrivDir, uid => UId},
             initial_nodes => [],
             machine => {module, ra_fifo,
                         #{dead_letter_handler =>
                           {?MODULE, dead_letter_handler, [self()]}}}},
    _ = ra:start_node(Conf),
    ok = ra:trigger_election(NodeId),
    _ = ra:members(NodeId),

    F0 = ra_fifo_client:init(ClusterId, [NodeId]),
    {ok, F1} = ra_fifo_client:checkout(<<"tag">>, 10, F0),
    {ok, F2} = ra_fifo_client:enqueue(msg1, F1),
    F3 = discard_next_delivery(F2, 500),
    {ok, empty, _F4} = ra_fifo_client:dequeue(<<"tag1">>, settled, F3),
    receive
        {dead_letter, Letters} ->
            ct:pal("dead letters ~p~n", [Letters]),
            [{_, msg1}] = Letters,
            ok
    after 500 ->
              exit(dead_letter_timeout)
    end,
    ra:stop_node(NodeId),
    ok.

ra_fifo_client_cancel_checkout(Config) ->
    ClusterId = ?config(cluster_id, Config),
    NodeId = ?config(node_id, Config),
    ok = start_cluster(ClusterId, [NodeId]),
    F0 = ra_fifo_client:init(ClusterId, [NodeId], 4),
    {ok, F1} = ra_fifo_client:enqueue(m1, F0),
    {ok, F2} = ra_fifo_client:checkout(<<"tag">>, 10, F1),
    {_, F3} = process_ra_events0(F2, [], 250, fun (_, S) -> S end),
    {ok, F4} = ra_fifo_client:cancel_checkout(<<"tag">>, F3),
    {ok, {_, {_, m1}}, _} = ra_fifo_client:dequeue(<<"d1">>, settled, F4),
    ok.

ra_fifo_client_untracked_enqueue(Config) ->
    ClusterId = ?config(cluster_id, Config),
    NodeId = ?config(node_id, Config),
    ok = start_cluster(ClusterId, [NodeId]),

    ok = ra_fifo_client:untracked_enqueue(ClusterId, [NodeId], msg1),
    F0 = ra_fifo_client:init(ClusterId, [NodeId]),
    {ok, {_, {_, msg1}}, _} = ra_fifo_client:dequeue(<<"tag">>, settled, F0),
    ra:stop_node(NodeId),
    ok.


ra_fifo_client_flow(Config) ->
    ClusterId = ?config(cluster_id, Config),
    NodeId = ?config(node_id, Config),
    ok = start_cluster(ClusterId, [NodeId]),
    F0 = ra_fifo_client:init(ClusterId, [NodeId], 4),
    {ok, F1} = ra_fifo_client:enqueue(m1, F0),
    {ok, F2} = ra_fifo_client:enqueue(m2, F1),
    {ok, F3} = ra_fifo_client:enqueue(m3, F2),
    {slow, F4} = ra_fifo_client:enqueue(m4, F3),
    {error, stop_sending} = ra_fifo_client:enqueue(m5, F4),
    {_, F5} = process_ra_events(F4, 500),
    {ok, _} = ra_fifo_client:enqueue(m5, F5),
    ra:stop_node(NodeId),
    ok.

test_queries(Config) ->
    ClusterId = ?config(cluster_id, Config),
    NodeId = ?config(node_id, Config),
    ok = start_cluster(ClusterId, [NodeId]),
    P = spawn(fun () ->
                  F0 = ra_fifo_client:init(ClusterId, [NodeId], 4),
                  {ok, F1} = ra_fifo_client:enqueue(m1, F0),
                  {ok, _F2} = ra_fifo_client:enqueue(m2, F1),
                  receive stop ->  ok end
          end),
    F0 = ra_fifo_client:init(ClusterId, [NodeId], 4),
    {ok, _} = ra_fifo_client:checkout(<<"tag">>, 1, F0),
    {ok, {_, Ready}, _} = ra:dirty_query(NodeId,
                                         fun ra_fifo:query_messages_ready/1),
    ?assertEqual(1, maps:size(Ready)),
    ct:pal("Ready ~w~n", [Ready]),
    {ok, {_, Checked}, _} = ra:dirty_query(NodeId,
                                           fun ra_fifo:query_messages_checked_out/1),
    ?assertEqual(1, maps:size(Checked)),
    ct:pal("Checked ~w~n", [Checked]),
    {ok, {_, Processes}, _} = ra:dirty_query(NodeId,
                                           fun ra_fifo:query_processes/1),
    ct:pal("Processes ~w~n", [Processes]),
    ?assertEqual(2, length(Processes)),
    P !  stop,
    ra:stop_node(NodeId),
    ok.

dead_letter_handler(Pid, Msgs) ->
    Pid ! {dead_letter, Msgs}.

ra_fifo_client_dequeue(Config) ->
    ClusterId = ?config(priv_dir, Config),
    NodeId = ?config(node_id, Config),
    UId = ?config(uid, Config),
    Tag = UId,
    ok = start_cluster(ClusterId, [NodeId]),
    F1 = ra_fifo_client:init(ClusterId, [NodeId]),
    {ok, empty, F1b} = ra_fifo_client:dequeue(Tag, settled, F1),
    {ok, F2} = ra_fifo_client:enqueue(msg1, F1b),
    {ok, {0, {_, msg1}}, F3} = ra_fifo_client:dequeue(Tag, settled, F2),
    {ok, F4} = ra_fifo_client:enqueue(msg2, F3),
    {ok, {MsgId, {_, msg2}}, F5} = ra_fifo_client:dequeue(Tag, unsettled, F4),
    {ok, _F6} = ra_fifo_client:settle(Tag, [MsgId], F5),
    ra:stop_node(NodeId),
    ok.

leader_monitors_customer(Config) ->
    ClusterId = ?config(priv_dir, Config),
    NodeId = ?config(node_id, Config),
    UId = ?config(uid, Config),
    Tag = UId,
    Name = element(1, NodeId),
    ok = start_cluster(ClusterId, [NodeId]),
    F0 =  ra_fifo_client:init(ClusterId, [NodeId]),
    {ok, F1} = ra_fifo_client:checkout(Tag, 10, F0),
    {monitored_by, [MonitoredBy]} = erlang:process_info(self(), monitored_by),
    ?assert(MonitoredBy =:= whereis(Name)),
    ra:stop_node(NodeId),
    _ = ra:restart_node(NodeId),
    % check monitors are re-applied after restart
    {ok, _F3} = ra_fifo_client:checkout(Tag, 5, F1),
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
    UId1 = ?config(uid, Config),
    Tag = UId1,
    Cluster = [NodeId1, NodeId2, NodeId3],
    ok = start_cluster(ClusterId, Cluster),
    F0 =  ra_fifo_client:init(ClusterId, [NodeId1, NodeId2, NodeId3]),
    {ok, F1} = ra_fifo_client:checkout(Tag, 10, F0),

    {monitored_by, [MonitoredBy]} = erlang:process_info(self(), monitored_by),
    ?assert(MonitoredBy =:= whereis(Name1)),

    ok = ra:stop_node(NodeId1),
    % give the election process a bit of time before issuing a command
    timer:sleep(100),

    {ok, _F2} = ra_fifo_client:checkout(Tag, 10, F1),

    {monitored_by, [MonitoredByAfter]} = erlang:process_info(self(),
                                                             monitored_by),
    ?assert((MonitoredByAfter =:= whereis(Name2)) or
            (MonitoredByAfter =:= whereis(Name3))),
    ra:stop_node(NodeId1),
    ra:stop_node(NodeId2),
    ra:stop_node(NodeId3),
    ok.

node_is_deleted(Config) ->
    ClusterId = ?config(cluster_id, Config),
    PrivDir = ?config(priv_dir, Config),
    NodeId = ?config(node_id, Config),
    UId = ?config(uid, Config),
    Conf = conf(ClusterId, UId, NodeId, PrivDir, []),
    _ = ra:start_node(Conf),
    ok = ra:trigger_election(NodeId),
    F0 = ra_fifo_client:init(ClusterId, [NodeId]),
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
    F = ra_fifo_client:init(ClusterId, [NodeId]),
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

cluster_is_deleted(Config) ->
    PrivDir = ?config(priv_dir, Config),
    NodeId = ?config(node_id, Config),
    UId = ?config(uid, Config),
    ClusterId = ?config(cluster_id, Config),
    ok = start_cluster(ClusterId, [NodeId]),
    F0 = ra_fifo_client:init(ClusterId, [NodeId]),
    {ok, F1} = ra_fifo_client:enqueue(msg1, F0),
    {ok, _} = ra:delete_cluster([NodeId]),
    % validate
    ok = validate_process_down(element(1, NodeId), 50),
    Dir = filename:join(PrivDir, UId),
    false = filelib:is_dir(Dir),
    [] = supervisor:which_children(ra_nodes_sup),
    % validate an end of life is emitted
    eol = process_ra_events(F1, 250),
    ok.

cluster_is_deleted_with_node_down(Config) ->
    %% cluster deletion is a coordingated consensus action
    %% The leader will commit and replicate a "poison pill" message
    %% Once each follower applies this messages it terminates and deletes all
    %% it's data
    %% the leader waits until the poison pill message has been replicated to
    %% _all_ followers then terminates and deletes it's own data.
    ClusterId = ?config(cluster_id, Config),
    PrivDir = ?config(priv_dir, Config),
    NodeId1 = ?config(node_id, Config),
    NodeId2 = ?config(node_id2, Config),
    NodeId3 = ?config(node_id3, Config),
    Peers = [NodeId1, NodeId2, NodeId3],
    ok = start_cluster(ClusterId, Peers),
    timer:sleep(100),
    % check data dirs exist for all nodes
    Wildcard = lists:flatten(filename:join([PrivDir,
                                            atom_to_list(ClusterId) ++ "**"])),
    % assert there are three matching data dirs
    [_, _, _] = filelib:wildcard(Wildcard),

    ok = ra:stop_node(NodeId3),
    {ok, _} = ra:delete_cluster(Peers),
    timer:sleep(100),
    % start node again
    ra:restart_node(NodeId3),
    % validate all nodes have been shut down and terminated
    ok = validate_process_down(element(1, NodeId1), 10),
    ok = validate_process_down(element(1, NodeId2), 10),
    ok = validate_process_down(element(1, NodeId3), 10),

    % validate there are no data dirs anymore
    [] = filelib:wildcard(Wildcard),
    ok.

cluster_cannot_be_deleted_in_minority(Config) ->
    ClusterId = ?config(cluster_id, Config),
    PrivDir = ?config(priv_dir, Config),
    NodeId1 = ?config(node_id, Config),
    NodeId2 = ?config(node_id2, Config),
    NodeId3 = ?config(node_id3, Config),
    Peers = [NodeId1, NodeId2, NodeId3],
    ok = start_cluster(ClusterId, Peers),
    % check data dirs exist for all nodes
    Wildcard = lists:flatten(filename:join([PrivDir,
                                            atom_to_list(ClusterId) ++ "**"])),
    % assert there are three matching data dirs
    [_,_,_] = filelib:wildcard(Wildcard),

    ra:stop_node(NodeId2),
    ra:stop_node(NodeId3),
    {error, {no_more_nodes_to_try, Errs}} = ra:delete_cluster(Peers, 250),
    ct:pal("Errs~p", [Errs]),
    ra:stop_node(NodeId1),
    ok.

node_restart_after_application_restart(Config) ->
    NodeId = ?config(node_id, Config),
    ClusterId = ?config(cluster_id, Config),
    ok = start_cluster(ClusterId, [NodeId]),
    F0 = ra_fifo_client:init(ClusterId, [NodeId]),
    {ok, F1} = ra_fifo_client:checkout(<<"tag">>, 10, F0),
    application:stop(ra),
    application:start(ra),
    % restart node
    ok = ra:restart_node(NodeId),
    {ok, _} = ra_fifo_client:checkout(<<"tag2">>, 10, F1),
    ok = ra:stop_node(NodeId),
    ok.


% NB: this is not guaranteed not to re-issue side-effects but only tests
% that the likelyhood is small
restarted_node_does_not_reissue_side_effects(Config) ->
    NodeId = ?config(node_id, Config),
    Name = element(1, NodeId),
    ClusterId = ?config(cluster_id, Config),
    ok = start_cluster(ClusterId, [NodeId]),
    F0 = ra_fifo_client:init(ClusterId, [NodeId]),
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
    ok = ra:stop_node(NodeId),
    ok.

log_fold(Config) ->
    ClusterId = ?config(cluster_id, Config),
    NodeId = ?config(node_id, Config),
    ok = start_cluster(ClusterId, [NodeId]),
    F0 = ra_fifo_client:init(ClusterId, [NodeId]),
    {ok, F1} = ra_fifo_client:enqueue(m1, F0),
    {ok, F2} = ra_fifo_client:enqueue(m2, F1),
    {ok, F3} = ra_fifo_client:enqueue(m3, F2),
    {_, F4} = process_ra_events(F3, 500),
    Fun = fun({_, _, {'$usr', _, {enqueue, _, _, _}, _}}, {E, C, S, D, R}) ->
                  {E + 1, C, S, D, R};
             ({_, _, {'$usr', _, {settle, _, _}, _}}, {E, C, S, D, R}) ->
                  {E, C, S + 1, D, R};
             ({_, _, {'$usr', _, {discard, _, _}, _}}, {E, C, S, D, R}) ->
                  {E, C, S, D + 1, R};
             ({_, _, {'$usr', _, {return, _, _}, _}}, {E, C, S, D, R}) ->
                  {E, C, S, D, R + 1};
             ({_, _, {'$usr', _, {checkout, _, _}, _}}, {E, C, S, D, R}) ->
                  {E, C + 1, S, D, R};
             (_, Acc) ->
                  Acc
          end,
    ct:pal("~p~n", [ra_node_proc:log_fold(NodeId, %% fun(E, Acc) -> [E | Acc] end, [])]),
                                          Fun, {0, 0, 0, 0, 0}, 30000)]),
    ra:stop_node(NodeId),
    ok.

startup_performance_test(Config) ->
    ClusterId = ?config(cluster_id, Config),
    NodeId = ?config(node_id, Config),
    DataDir = ?config(data_dir, Config),
    UId = ?config(uid, Config),
    PrivDir = ?config(priv_dir, Config),

    Conf = data_conf(ClusterId, UId, NodeId, PrivDir, [], DataDir),
    os:cmd("cp -r " ++ DataDir ++ " " ++ PrivDir),
    start_profile(Config, [ra_fifo]),
    ct:pal("Start takes ~p microseconds ~n", [element(1, timer:tc(fun() -> ra:start_node(Conf) end))]),
    stop_profile(Config),
    ra:send_and_await_consensus(NodeId, {enqueue, self(), undefined, last}),
    ra:stop_node(NodeId),
    ok.

start_profile(Config, Modules) ->
    Dir = ?config(priv_dir, Config),
    Case = ?config(test_case, Config),
    GzFile = filename:join([Dir, "lg_" ++ atom_to_list(Case) ++ ".gz"]),
    ct:pal("Profiling to ~p~n", [GzFile]),

    lg:trace(Modules, lg_file_tracer,
             GzFile, #{running => false, mode => profile}).

stop_profile(Config) ->
    Case = ?config(test_case, Config),
    ct:pal("Stopping profiling for ~p~n", [Case]),
    lg:stop(),
    % this segfaults
    % timer:sleep(2000),
    Dir = ?config(priv_dir, Config),
    Name = filename:join([Dir, "lg_" ++ atom_to_list(Case)]),
    lg_callgrind:profile_many(Name ++ ".gz.*", Name ++ ".out",#{}),
    ok.

data_conf(ClusterId, UId, NodeId, _, Peers, DataDir) ->
    #{cluster_id => ClusterId,
      id => NodeId,
      uid => UId,
      log_module => ra_log_file,
      log_init_args => #{uid => UId,
                         data_dir => DataDir},
      initial_nodes => Peers,
      machine => {module, ra_fifo, #{}}}.

conf(ClusterId, UId, NodeId, _, Peers) ->
    #{cluster_id => ClusterId,
      id => NodeId,
      uid => UId,
      log_module => ra_log_file,
      log_init_args => #{uid => UId},
      initial_nodes => Peers,
      machine => {module, ra_fifo, #{}}}.

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

process_ra_events(State, Acc, Wait) ->
    DeliveryFun = fun ({delivery, Tag, Msgs}, S) ->
                          MsgIds = [element(1, M) || M <- Msgs],
                          {ok, S2} = ra_fifo_client:settle(Tag, MsgIds, S),
                          S2
                  end,
    process_ra_events0(State, Acc, Wait, DeliveryFun).

process_ra_events0(State0, Acc, Wait, DeliveryFun) ->
    receive
        {ra_event, From, Evt} ->
            ct:pal("processing ra event ~p~n", [Evt]),
            case ra_fifo_client:handle_ra_event(From, Evt, State0) of
                {internal, _, State} ->
                    process_ra_events0(State, Acc, Wait, DeliveryFun);
                {{delivery, _Tag, Msgs} = Del, State1} ->
                    State = DeliveryFun(Del, State1),
                    process_ra_events0(State, Acc ++ Msgs, Wait, DeliveryFun);
                eol ->
                    eol
            end
    after Wait ->
              {Acc, State0}
    end.

discard_next_delivery(State0, Wait) ->
    receive
        {ra_event, From, Evt} ->
            case ra_fifo_client:handle_ra_event(From, Evt, State0) of
                {internal, _, State} ->
                    discard_next_delivery(State, Wait);
                {{delivery, Tag, Msgs}, State1} ->
                    MsgIds = [element(1, M) || M <- Msgs],
                    ct:pal("discarding ~p", [Msgs]),
                    {ok, State} = ra_fifo_client:discard(Tag, MsgIds,
                                                         State1),
                    State
            end
    after Wait ->
              State0
    end.

return_next_delivery(State0, Wait) ->
    receive
        {ra_event, From, Evt} ->
            case ra_fifo_client:handle_ra_event(From, Evt, State0) of
                {internal, _, State} ->
                    return_next_delivery(State, Wait);
                {{delivery, Tag, Msgs}, State1} ->
                    MsgIds = [element(1, M) || M <- Msgs],
                    ct:pal("returning ~p", [Msgs]),
                    {ok, State} = ra_fifo_client:return(Tag, MsgIds,
                                                        State1),
                    State
            end
    after Wait ->
              State0
    end.

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

start_cluster(ClusterId, NodeIds) ->
    {ok, NodeIds, _} = ra:start_cluster(ClusterId,
                                        {module, ra_fifo, #{}},
                                        NodeIds),
    ok.
