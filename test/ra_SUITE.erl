-module(ra_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").

all() ->
    [
     {group, ra_log_memory},
     {group, ra_log_file}
    ].

all_tests() ->
    [
     single_node,
     stop_node_idemp,
     minority,
     start_nodes,
     node_recovery,
     send_and_await_consensus,
     send_and_notify,
     dirty_query,
     members,
     consistent_query,
     snapshot,
     add_node,
     queue_example,
     ramp_up_and_ramp_down,
     start_and_join_then_leave_and_terminate,
     leader_steps_down_after_replicating_new_cluster
    ].

groups() ->
    [
     {ra_log_memory, [], all_tests()},
     {ra_log_file, [], all_tests()}
    ].

suite() -> [{timetrap, {seconds, 30}}].

init_per_suite(Config) ->
    _ = application:load(ra),
    ok = application:set_env(ra, data_dir, ?config(priv_dir, Config)),
    application:ensure_all_started(ra),
    Config.

end_per_suite(Config) ->
    application:stop(ra),
    Config.

init_per_group(ra_log_memory, Config) ->
    Fun = fun (_TestCase) ->
                  fun (Name, Nodes, ApplyFun, InitialState) ->
                          Conf = #{log_module => ra_log_memory,
                                   log_init_args => #{},
                                   initial_nodes => Nodes,
                                   apply_fun => ApplyFun,
                                   init_fun => fun (_) -> InitialState end,
                                   cluster_id => Name},
                          ra:start_node(Name, Conf)
                  end
          end,
   [{start_node_fun, Fun} | Config];
init_per_group(ra_log_file, Config) ->
    PrivDir = ?config(priv_dir, Config),
    Fun = fun (TestCase) ->
                  fun (Name, Nodes, ApplyFun, InitialState) ->
                          Dir = filename:join([PrivDir, TestCase, ra_lib:to_list(Name)]),
                          ok = filelib:ensure_dir(Dir),
                          % {ok, _} = ra_log_wal:start_link(#{dir => Dir}, []),
                          Conf = #{log_module => ra_log_file,
                                   log_init_args => #{directory => Dir,
                                                      id => Name},
                                   initial_nodes => Nodes,
                                   apply_fun => ApplyFun,
                                   init_fun => fun (_) -> InitialState end,
                                   cluster_id => Name},
                          ra:start_node(Name, Conf)
                  end
          end,
    [{start_node_fun, Fun} | Config].

end_per_group(_, Config) ->
    Config.

init_per_testcase(TestCase, Config0) ->
    Fun0 = ?config(start_node_fun, Config0),
    Fun = Fun0(TestCase), % "partial application"
    Config = proplists:delete(start_node_fun, Config0),
    [{test_name, ra_lib:to_list(TestCase)}, {start_node_fun, Fun} | Config].

single_node(Config) ->
    StartNode = ?config(start_node_fun, Config),
    N1 = nn(Config, 1),
    ok = StartNode(N1, [], fun erlang:'+'/2, 0),
    timer:sleep(1000),
    % index is 2 as leaders commit a noop entry on becoming leaders
    {ok, {2,1}, _} = ra:send_and_await_consensus({N1, node()}, 5, 2000),
    terminate_cluster([N1]).

stop_node_idemp(Config) ->
    StartNode = ?config(start_node_fun, Config),
    N1 = nn(Config, 1),
    ok = StartNode(N1, [], fun erlang:'+'/2, 0),
    timer:sleep(1000),
    ok = ra:stop_node({N1, node()}),
    % should not raise exception
    ok = ra:stop_node({N1, node()}),
    ok = ra:stop_node({N1, randomnode@bananas}),
    ok.

leader_steps_down_after_replicating_new_cluster(Config) ->
    N1 = nn(Config, 1),
    N2 = nn(Config, 2),
    N3 = nn(Config, 3),
    ok = new_node(N1, Config),
    timer:sleep(1000),
    _ = issue_op(N1, 5),
    validate(N1, 5),
    ok = start_and_join(N1, N2, Config),
    _ = issue_op(N1, 5),
    validate(N1, 10),
    ok = start_and_join(N1, N3, Config),
    _ = issue_op(N1, 5),
    validate(N1, 15),
    % allow N3 some time to catch up
    timer:sleep(1000),
    % remove leader node
    % the leader should here replicate the new cluster config
    % then step down + shut itself down
    ok = remove_node(N1),
    timer:sleep(1000),
    {error, noproc} = ra:send_and_await_consensus(N1, 5, 2000),
    _ = issue_op(N2, 5),
    validate(N2, 20),
    terminate_cluster([N2, N3]).


start_and_join_then_leave_and_terminate(Config) ->
    N1 = nn(Config, 1),
    N2 = nn(Config, 2),
    % safe node removal
    ok = new_node(N1, Config),
    timer:sleep(1000),
    _ = issue_op(N1, 5),
    validate(N1, 5),
    ok = start_and_join(N1, N2, Config),
    _ = issue_op(N2, 5),
    validate(N2, 10),
    ok = ra:leave_and_terminate({N1, node()}, {N2, node()}),
    validate(N1, 10),
    terminate_cluster([N1]),
    ok.

ramp_up_and_ramp_down(Config) ->
    N1 = nn(Config, 1),
    N2 = nn(Config, 2),
    N3 = nn(Config, 3),
    ok = new_node(N1, Config),
    timer:sleep(1000),
    _ = issue_op(N1, 5),
    validate(N1, 5),

    ok = start_and_join(N1, N2, Config),
    _ = issue_op(N2, 5),
    validate(N2, 10),

    ok = start_and_join(N1, N3, Config),
    _ = issue_op(N3, 5),
    validate(N3, 15),

    ok = ra:leave_and_terminate({N3, node()}),
    _ = issue_op(N2, 5),
    validate(N2, 20),

    % this is dangerous territory
    % we need a quorum from the node that is to be removed for the cluster
    % change. if we stop the node before removing it from the cluster
    % configuration the cluster becomes non-functional
    ok = remove_node(N2),
    % a longish sleep here simulates a node that has been removed but not
    % shut down and thus may start issuing request_vote_rpcs
    timer:sleep(1000),
    ok = stop_node(N2),
    _ = issue_op(N1, 5),
    validate(N1, 25),
    terminate_cluster([N1]).

minority(Config) ->
    N1 = nn(Config, 1),
    N2 = nn(Config, 2),
    N3 = nn(Config, 3),
    StartNode = ?config(start_node_fun, Config),
    ok = StartNode(N1, [{N2, node()}, {N3, node()}], fun erlang:'+'/2, 0),
    {timeout, _} = ra:send_and_await_consensus({N1, node()}, 5, 500),
    terminate_cluster([N1]).

start_nodes(Config) ->
    StartNode = ?config(start_node_fun, Config),
    % suite unique node names
    N1 = nn(Config, 1),
    N2 = nn(Config, 2),
    N3 = nn(Config, 3),
    % start the first node and wait a bit
    ok = StartNode (N1, [{N2, node()}, {N3, node()}], fun erlang:'+'/2, 0),
    % start second node
    ok = StartNode(N2, [{N1, node()}, {N3, node()}], fun erlang:'+'/2, 0),
    timer:sleep(1000),
    % a consensus command tells us there is a functioning cluster
    {ok, _, _Leader} = ra:send_and_await_consensus({N1, node()}, 5),
    % start the 3rd node and issue another command
    ok = StartNode(N3, [{N1, node()}, {N2, node()}], fun erlang:'+'/2, 0),
    timer:sleep(1000),
    % issue command
    {ok, {3, Term}, Leader} = ra:send_and_await_consensus({N3, node()}, 5),
    % shut down non leader
    Target = case Leader of
                 {N1, _} -> {N2, node()};
                 _ -> {N1, node()}
             end,
    gen_statem:stop(Target, normal, 2000),
    % issue command to confirm n3 joined the cluster successfully
    {ok, {4, Term}, _} = ra:send_and_await_consensus({N3, node()}, 5),
    terminate_cluster([N1, N2, N3] -- [element(1, Target)]).

node_recovery(Config) ->
    N1 = nn(Config, 1),
    N2 = nn(Config, 2),
    N3 = nn(Config, 3),
    StartNode = ?config(start_node_fun, Config),
    % start the first node and wait a bit
    ok = StartNode(N1, [{N2, node()}, {N3, node()}], fun erlang:'+'/2, 0),
    % start second node
    ok = StartNode(N2, [{N1, node()}, {N3, node()}], fun erlang:'+'/2, 0),
    % a consensus command tells us there is a functioning 2 node cluster
    {ok, {_, _}, Leader} = ra:send_and_await_consensus({N2, node()}, 5),
    % restart Leader
    gen_statem:stop(Leader, normal, 2000),
    timer:sleep(1000),
    N = node(),
    case Leader of
        {N1, N} ->
            ok = StartNode(N1, [{N2, node()}, {N3, node()}], fun erlang:'+'/2, 0);
        {N2, N} ->
            ok = StartNode(N2, [{N1, node()}, {N3, node()}], fun erlang:'+'/2, 0)
    end,
    timer:sleep(1000),
    % issue command
    {ok, {_, _}, _Leader} = ra:send_and_await_consensus({N2, node()}, 5),
    terminate_cluster([N1, N2]).

send_and_await_consensus(Config) ->
    [A, _B, _C] = Cluster =
        start_local_cluster(3, ?config(test_name, Config),
                            fun erlang:'+'/2, 9, Config),
    {ok, {_, _}, _Leader} = ra:send_and_await_consensus(A, 5),
    terminate_cluster(Cluster).

send_and_notify(Config) ->
    [A, _B, _C] = Cluster =
        start_local_cluster(3, ?config(test_name, Config),
                            fun erlang:'+'/2, 9, Config),
    {ok, IdxTerm, _Leader} = ra:send_and_notify(A, 5),
    receive
        {consensus, IdxTerm} -> ok
    after 2000 ->
              exit(consensus_timeout)
    end,
    terminate_cluster(Cluster).

dirty_query(Config) ->
    [A, B, _C] = Cluster = start_local_cluster(3, ?config(test_name, Config),
                                               fun erlang:'+'/2, 9, Config),
    {ok, {{_, _}, 9}, _} = ra:dirty_query(B, fun(S) -> S end),
    {ok, {_, Term}, Leader} = ra:send_and_await_consensus(A, 5),
    {ok, {{_, Term}, 14}, _} = ra:dirty_query(Leader, fun(S) -> S end),
    terminate_cluster(Cluster).

members(Config) ->
    Cluster = start_local_cluster(3, ?config(test_name, Config),
                                  fun erlang:'+'/2, 9, Config),
    {ok, _, Leader} = ra:send_and_await_consensus(hd(Cluster), 5),
    {ok, Cluster, Leader} = ra:members(Leader),
    terminate_cluster(Cluster).

consistent_query(Config) ->
    [A, _B, _C]  = Cluster = start_local_cluster(3, ?config(test_name, Config),
                                                 fun erlang:'+'/2,
                                                 0, Config),
    {ok, {_, Term}, Leader} = ra:send_and_await_consensus(A, 9),
    {ok, {_, Term}, _Leader} = ra:send(Leader, 5),
    {ok, {{_, Term}, 14}, Leader} = ra:consistent_query(A, fun(S) -> S end),
    terminate_cluster(Cluster).

add_node(Config) ->
    [A, _B] = Cluster = start_local_cluster(2, ?config(test_name, Config),
                                            fun erlang:'+'/2, 0,
                                            Config),
    {ok, {_, Term}, Leader} = ra:send_and_await_consensus(A, 9),
    C = ra_node:name(?config(test_name, Config), "3"),
    {ok, {_, Term}, _Leader} = ra:add_node(Leader, {C, node()}),
    ok = ra:start_node(C, Cluster, fun erlang:'+'/2, 0),
    timer:sleep(2000),
    {ok, {{_, Term}, 9}, Leader} = ra:consistent_query(C, fun(S) -> S end),
    terminate_cluster([C | Cluster]).

snapshot(Config) ->
    N1 = nn(Config, 1),
    N2 = nn(Config, 2),
    N3 = nn(Config, 3),
    StartNode = ?config(start_node_fun, Config),
    InitialNodes = [{N1, node()}, {N2, node()}],
    %%TODO look into cluster changes WITH INVALID NAMES!!!

    % start two nodes
    ok = StartNode(N1, InitialNodes, fun ra_queue:simple_apply/3, []),
    ok = StartNode(N2, InitialNodes, fun ra_queue:simple_apply/3, []),
    % N1 = {N1, node()}, N2 = {N2, node()}, N3 = {N3, node()},
    {ok, {_, Term}, Leader} = ra:send(N1, {enq, banana}),
    {ok, {_, Term}, Leader} = ra:send(Leader, {deq, self()}),
    {ok, {_, Term}, Leader} = ra:send_and_await_consensus(Leader, {enq, apple}),
    % waitfor(banana, apply_timeout),
    ok = ra:start_node(N3, InitialNodes, fun ra_queue:simple_apply/3, []),
    {ok, {_, Term}, _Leader} = ra:add_node(Leader, {N3, node()}),
    timer:sleep(1000),
    % at this point snapshot should have been taken
    {ok, {_, Res}, _} = ra:dirty_query(N1, fun ra_lib:id/1),
    {ok, {_, Res}, _} = ra:dirty_query(N2, fun ra_lib:id/1),
    {ok, {_, Res}, _} = ra:dirty_query(N3, fun ra_lib:id/1),
    % check that the message isn't delivered multiple times
    terminate_cluster([N3 | InitialNodes]).

queue_example(Config) ->
    Self = self(),
    [A, _B, _C] = Cluster = start_local_cluster(3, ?config(test_name, Config),
                                                fun queue_apply/2,
                                                #{queue => queue:new(),
                                                  pending_dequeues => []}, Config),

    {ok, {_, Term}, Leader} = ra:send(A, {dequeue, Self}),
    {ok, {_, Term}, _} = ra:send(Leader, {enqueue, test_msg}),
    waitfor(test_msg, apply_timeout),
    % check that the message isn't delivered multiple times
    receive
        test_msg -> exit(double_delivery)
    after 500 -> ok
    end,
    terminate_cluster(Cluster).

% implements a simple queue machine
queue_apply({enqueue, Msg}, State =#{queue := Q0, pending_dequeues := []}) ->
    Q = queue:in(Msg, Q0),
    State#{queue => Q};
queue_apply({enqueue, Msg}, State = #{queue := Q0,
                                      pending_dequeues := [Next | Rest]}) ->
    Q1 = queue:in(Msg, Q0),
    {{value, Item}, Q} = queue:out(Q1),
    {effects, State#{queue => Q, pending_dequeues => Rest}, [{send_msg, Next, Item}]};
queue_apply({dequeue, For}, State = #{queue := Q0, pending_dequeues := []}) ->
    case queue:out(Q0) of
        {empty, Q} ->
            State#{queue => Q, pending_dequeues => [For]};
        {{value, Item}, Q} ->
            {effects, State#{queue => Q}, [{send_msg, For, Item}]}
    end;
queue_apply({dequeue, For},
            State = #{queue := Q0,
                      pending_dequeues := [Next | Rest] = Pending}) ->
    case queue:out(Q0) of
        {empty, Q} ->
            State#{queue => Q, pending_dequeues => Pending ++ [For]};
        {{value, Item}, Q} ->
            {effects, State#{queue => Q,
                             pending_dequeues => Rest ++ [For]},
             [{send_msg, Next, Item}]}
    end.


waitfor(Msg, ExitWith) ->
    receive
        Msg -> ok
    after 3000 ->
              exit(ExitWith)
    end.

terminate_cluster(Nodes) ->
    [gen_statem:stop(P, normal, 2000) || P <- Nodes].

new_node(Name, Config) ->
    StartNode = ?config(start_node_fun, Config),
    ok = StartNode(Name, [], fun erlang:'+'/2, 0),
    ok.

stop_node(Name) ->
    ok = ra:stop_node({Name, node()}),
    ok.

add_node(Ref, New) ->
    {ok, _IdxTerm, _Leader} = ra:add_node({Ref, node()}, {New, node()}),
    ok.

start_and_join(Ref, New, Config) ->
    StartNode = ?config(start_node_fun, Config),
    ServerRef = {Ref, node()},
    ok = StartNode(New, [], fun erlang:'+'/2, 0),
    {ok, _, _} = ra:add_node(ServerRef, {New, node()}),
    ok.

start_local_cluster(Num, Name, ApplyFun, InitialState, Config) ->
    StartNode = ?config(start_node_fun, Config),
    Nodes0 = [{ra_node:name(Name, integer_to_list(N)), node()}
             || N <- lists:seq(1, Num)],
    ct:pal("NOdes ~p~n", [Nodes0]),
    [Head | _] = Nodes = [begin
                              ok = StartNode(N, Nodes0, ApplyFun, InitialState),
                              Id
                          end || Id = {N, _} <- Nodes0],
    _ = ra_node_proc:state_query(Head, all),
    Nodes.

remove_node(Name) ->
    {ok, _IdxTerm, _Leader} = ra:remove_node({Name, node()}, {Name, node()}),
    ok.

issue_op(Name, Op) ->
    {ok, IdxTrm, Res} = ra:send_and_await_consensus(Name, Op, 2000),
    {IdxTrm, Res}.

validate(Name, Expected) ->
    {ok, {_, Expected}, _} = ra:consistent_query({Name, node()},
                                                 fun(X) -> X end).

dump(T) ->
    ct:pal("DUMP: ~p~n", [T]),
    T.

nn(Config, N) when is_integer(N) ->
    ra_node:name(?config(test_name, Config), erlang:integer_to_list(N)).

