-module(ra_2_SUITE).
-behaviour(ra_machine).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-define(info, true).

%% common ra_log tests to ensure behaviour is equivalent across
%% ra_log backends

all() ->
    [
     {group, tests}
    ].

all_tests() ->
    [
     start_stopped_node,
     node_is_deleted,
     cluster_is_deleted_with_node_down,
     cluster_cannot_be_deleted_in_minority,
     node_restart_after_application_restart,
     restarted_node_does_not_reissue_side_effects,
     recover,
     recover_after_kill,
     start_node_uid_validation
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
     {modname, TestCase},
     {cluster_id, TestCase},
     {uid, atom_to_binary(TestCase, utf8)},
     {node_id, {TestCase, node()}},
     {uid2, atom_to_binary(NodeName2, utf8)},
     {node_id2, {NodeName2, node()}},
     {uid3, atom_to_binary(NodeName3, utf8)},
     {node_id3, {NodeName3, node()}}
     | Config].

enqueue(Node, Msg) ->
    {ok, _, _} = ra:send_and_await_consensus(Node, {enq, Msg}),
    ok.

dequeue(Node) ->
    {ok, Res, _} = ra:send_and_await_consensus(Node, deq),
    Res.

start_stopped_node(Config) ->
    %% ra:start_node should fail if the node already exists
    ClusterId = ?config(cluster_id, Config),
    PrivDir = ?config(priv_dir, Config),
    NodeId = ?config(node_id, Config),
    UId = ?config(uid, Config),
    Conf = conf(ClusterId, UId, NodeId, PrivDir, []),
    ok = ra:start_node(Conf),
    ok = ra:trigger_election(NodeId),
    ok = enqueue(NodeId, msg1),
    %%
    {error, {already_started, _}} = ra:start_node(Conf),
    ok = ra:stop_node(NodeId),
    {error, not_new} = ra:start_node(Conf),
    ok = ra:restart_node(NodeId),
    ok.


node_is_deleted(Config) ->
    ClusterId = ?config(cluster_id, Config),
    PrivDir = ?config(priv_dir, Config),
    NodeId = ?config(node_id, Config),
    UId = ?config(uid, Config),
    Conf = conf(ClusterId, UId, NodeId, PrivDir, []),
    _ = ra:start_node(Conf),
    ok = ra:trigger_election(NodeId),
    ok = enqueue(NodeId, msg1),
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
    case dequeue(NodeId) of
        empty -> ok;
        _ ->
            exit(unexpected_dequeue_result)
    end,

    ok = ra:delete_node(NodeId),
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
    [ begin
          UId = ra_directory:uid_of(Name),
          ?assert(filelib:is_dir(filename:join([PrivDir, UId])))
      end || {Name, _} <- Peers],

    % check data dirs exist for all nodes
    % Wildcard = lists:flatten(filename:join([PrivDir, "**"])),
    % assert there are three matching data dirs

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
    [ begin
          UId = ra_directory:uid_of(Name),
          ?assert(false =:= filelib:is_dir(filename:join([PrivDir, UId])))
      end || {Name, _} <- Peers],
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
    [ begin
          UId = ra_directory:uid_of(Name),
          ?assert(filelib:is_dir(filename:join([PrivDir, UId])))
      end || {Name, _} <- Peers],
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
    ok= enqueue(NodeId, msg1),
    application:stop(ra),
    application:start(ra),
    % restart node
    ok = ra:restart_node(NodeId),
    ok= enqueue(NodeId, msg2),
    ok = ra:stop_node(NodeId),
    ok.


% NB: this is not guaranteed not to re-issue side-effects but only tests
% that the likelyhood is small
restarted_node_does_not_reissue_side_effects(Config) ->
    NodeId = ?config(node_id, Config),
    Name = element(1, NodeId),
    ClusterId = ?config(cluster_id, Config),
    ok = start_cluster(ClusterId, [NodeId]),
    ok = enqueue(NodeId, msg1),
    {ok, _, _} = ra:send_and_await_consensus(NodeId, {deq, self()}),
    receive
        {ra_event, _, {machine, msg1}} ->
            ok
    after 2000 ->
              exit(ra_event_timeout)
    end,
    % give the process time to persist the last_applied index
    timer:sleep(1000),
    % kill the process and have it restarted by the supervisor
    exit(whereis(Name), kill),

    %  check message isn't received again
    receive
        {ra_event, _, {machine, msg1}} ->
            exit(unexpected_ra_event)
    after 1000 ->
              ok
    end,
    ok = ra:stop_node(NodeId),
    ok.

recover(Config) ->
    NodeId = ?config(node_id, Config),
    ClusterId = ?config(cluster_id, Config),
    ok = start_cluster(ClusterId, [NodeId]),
    ok = enqueue(NodeId, msg1),
    ra:members(NodeId),
    ra:stop_node(NodeId),
    ra:restart_node(NodeId),
    ra:members(NodeId),
    msg1 = dequeue(NodeId),

    ok = ra:stop_node(NodeId),
    ok.

recover_after_kill(Config) ->
    NodeId = {Name, _} = ?config(node_id, Config),
    ClusterId = ?config(cluster_id, Config),
    ok = start_cluster(ClusterId, [NodeId]),
    ra:members(NodeId),
    ok = enqueue(NodeId, msg1),
    {F2, Deqd} = enq_deq_n(64, NodeId),
    % timer:sleep(100),
    exit(whereis(Name), kill),
    application:stop(ra),
    application:start(ra),
    ra:restart_node(NodeId),
    ra:members(NodeId),
    %% this should by the default release cursor interval of 128
    %% create a new snapshot
    {_F3, _AllDeq} = enq_deq_n(65, F2, Deqd),
    {ok, {_X, MS}, _} = ra:local_query(NodeId, fun (S) -> S end),
    %% kill node again to trigger post snapshot recovery
    exit(whereis(Name), kill),
    timer:sleep(250),
    ra:members(NodeId),
    timer:sleep(100),
    % give leader time to commit noop
    {ok, {_X2, MS2}, _} = ra:local_query(NodeId, fun (S) -> S end),
    ok = ra:stop_node(NodeId),
    % ct:pal("Indexes ~p ~p~nMS: ~p ~n MS2: ~p~nDequeued ~p~n",
    %        [X, X2, MS, MS2, AllDeq]),
    ?assertEqual(MS, MS2),
    ok = ra:restart_node(NodeId),
    {ok, _, _} = ra:members(NodeId, 30000),
    {ok, {_, MS3}, _} = ra:local_query(NodeId, fun (S) -> S end),
    ct:pal("~p ~p", [MS2, MS3]),
    ?assertEqual(MS2, MS3),
    ok.

start_node_uid_validation(Config) ->
    NodeId = ?config(node_id, Config),
    UId = <<"ADSFASDFø"/utf8>>,
    Conf = #{cluster_id => ?config(cluster_id, Config),
             id => NodeId,
             uid => UId,
             initial_nodes => [NodeId],
             log_init_args => #{uid => UId},
             machine => {module, ?MODULE, #{}}},
    {error, invalid_uid} = ra:start_node(Conf),
    {error, invalid_uid} = ra:start_node(Conf#{uid => <<"">>}),
    ok.

enq_deq_n(N, NodeId) ->
    enq_deq_n(N, NodeId, []).

enq_deq_n(0, F, Acc) ->
    {F, Acc};
enq_deq_n(N, NodeId, Acc) ->
    ok = enqueue(NodeId, N),
    Deq = dequeue(NodeId),
    true = Deq /= empty,
    enq_deq_n(N-1, NodeId, [Deq | Acc]).

conf(ClusterId, UId, NodeId, _, Peers) ->
    #{cluster_id => ClusterId,
      id => NodeId,
      uid => UId,
      log_init_args => #{uid => UId},
      initial_nodes => Peers,
      machine => {module, ?MODULE, #{}}}.


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

start_cluster(ClusterId, NodeIds, Config) ->
    {ok, NodeIds, _} = ra:start_cluster(ClusterId,
                                        {module, ?MODULE, Config},
                                        NodeIds),
    ok.

start_cluster(ClusterId, NodeIds) ->
    start_cluster(ClusterId, NodeIds, #{}).

%% ra_machine test impl
init(_) ->
    {queue:new(), []}.

'apply'(_Meta, {enq, Msg}, Effects, State) ->
    {queue:in(Msg, State), Effects};
'apply'(_Meta, deq, Effects, State0) ->
    case queue:out(State0) of
        {{value, Item}, State} ->
            {State, Effects, Item};
        {empty, _} ->
            {State0, Effects, empty}
    end;
'apply'(_Meta, {deq, Pid}, Effects, State0) ->
    case queue:out(State0) of
        {{value, Item}, State} ->
            {State, [{send_msg, Pid, Item} | Effects]};
        {empty, _} ->
            {State0, Effects}
    end.

eol_effects(State) ->
    [{send_msg, P, eol} || {P, _} <- queue:to_list(State), is_pid(P)].
