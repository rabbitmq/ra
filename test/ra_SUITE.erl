-module(ra_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").

all() ->
    [
     single_node,
     minority,
     start_nodes,
     node_recovery,
     send_and_await_consensus,
     send_and_notify,
     dirty_query,
     consistent_query,
     add_node,
     queue_example,
     ramp_up_and_ramp_down,
     leave_and_terminate,
     leader_steps_down_after_replicating_new_cluster
    ].

groups() ->
    [{tests, [], all()}].

suite() -> [ {timetrap,{seconds,30}} ].

single_node(_Config) ->
    ok = ra:start_node(n1, [], fun erlang:'+'/2, 0),
    timer:sleep(1000),
    {ok, {1,1}, _} = ra:send_and_await_consensus({n1, node()}, 5, 2000),
    terminate_cluster([n1]).

leader_steps_down_after_replicating_new_cluster(_Config) ->
    ok = new_node(n1),
    timer:sleep(1000),
    _ = issue_op(n1, 5),
    validate(n1, 5),
    ok = new_node(n2),
    ok = add_node(n1, n2),
    _ = issue_op(n1, 5),
    validate(n1, 10),
    ok = new_node(n3),
    ok = add_node(n1, n3),
    _ = issue_op(n1, 5),
    validate(n1, 15),
    % remove leader node
    % the leader should here replicate the new cluster config
    % then step down + shut itself down
    ok = remove_node(n1),
    timer:sleep(1000),
    {error, no_proc} = ra:send_and_await_consensus(n1, 5, 2000),
    _ = issue_op(n2, 5),
    validate(n2, 20),
    terminate_cluster([n2, n3]).


leave_and_terminate(_Config) ->
    % safe node removal
    ok = new_node(n1),
    timer:sleep(1000),
    _ = issue_op(n1, 5),
    validate(n1, 5),
    ok = add_node(n1, n2),
    ok = new_node(n2),
    _ = issue_op(n2, 5),
    validate(n2, 10),
    ok = ra:leave_and_terminate({n2, node()}),
    validate(n1, 10),
    terminate_cluster([n1]),
    ok.


ramp_up_and_ramp_down(_Config) ->
    ok = new_node(n1),
    timer:sleep(1000),
    _ = issue_op(n1, 5),
    validate(n1, 5),

    ok = add_node(n1, n2),
    ok = new_node(n2),
    _ = issue_op(n2, 5),
    validate(n2, 10),

    ok = add_node(n1, n3),
    ok = new_node(n3),
    _ = issue_op(n3, 5),
    validate(n3, 15),

    ok = remove_node(n3),
    ok = stop_node(n3),
    _ = issue_op(n2, 5),
    validate(n2, 20),

    % this is dangerous territory
    % we need a quorum from the node that is to be removed for the cluster
    % change. if we stop the node before removing it from the cluster
    % configuration the cluster becomes non-functional
    ok = remove_node(n2),
    % a longish sleep here simulates a node that has been removed but not
    % shut down and thus may start issuing request_vote_rpcs
    timer:sleep(1000),
    ok = stop_node(n2),
    _ = issue_op(n1, 5),
    validate(n1, 25),
    terminate_cluster([n1]).

minority(_Config) ->
    ok = ra:start_node(n1, [{n2, node()}, {n3, node()}], fun erlang:'+'/2, 0),
    {timeout, _} = ra:send_and_await_consensus({n1, node()}, 5, 500).

start_nodes(_Config) ->
    % start the first node and wait a bit
    ok = ra:start_node(n1, [{n2, node()}, {n3, node()}], fun erlang:'+'/2, 0),
    timer:sleep(1000),
    % start second node
    ok = ra:start_node(n2, [{n1, node()}, {n3, node()}], fun erlang:'+'/2, 0),
    % a consensus command tells us there is a functioning cluster
    {ok, {1, Term}, _Leader} = ra:send_and_await_consensus({n1, node()}, 5),
    % start the 3rd node and issue another command
    ok = ra:start_node(n3, [{n1, node()}, {n2, node()}], fun erlang:'+'/2, 0),
    timer:sleep(1000),
    % issue command
    {ok, {2, Term}, Leader} = ra:send_and_await_consensus({n3, node()}, 5),
    % shut down non leader
    Target = case Leader of
                 {n1, _} -> {n2, node()};
                 _ -> {n1, node()}
             end,
    gen_statem:stop(Target, normal, 2000),
    % issue command to confirm n3 joined the cluster successfully
    {ok, {3, Term}, _} = ra:send_and_await_consensus({n3, node()}, 5),
    terminate_cluster([n1, n3]).

node_recovery(_Config) ->
    % start the first node and wait a bit
    ok = ra:start_node(n1, [{n2, node()}, {n3, node()}], fun erlang:'+'/2, 0),
    % start second node
    ok = ra:start_node(n2, [{n1, node()}, {n3, node()}], fun erlang:'+'/2, 0),
    % a consensus command tells us there is a functioning 2 node cluster
    {ok, {1, _}, Leader} = ra:send_and_await_consensus({n2, node()}, 5),
    % restart Leader
    gen_statem:stop(Leader, normal, 2000),
    timer:sleep(1000),
    N = node(),
    case Leader of
        {n1, N} ->
            ok = ra:start_node(n1, [{n2, node()}, {n3, node()}], fun erlang:'+'/2, 0);
        {n2, N} ->
            ok = ra:start_node(n2, [{n1, node()}, {n3, node()}], fun erlang:'+'/2, 0)
    end,
    timer:sleep(1000),
    % issue command
    {ok, {2, _}, _Leader} = ra:send_and_await_consensus({n2, node()}, 5),
    terminate_cluster([n1, n2]).


send_and_await_consensus(_Config) ->
    [{APid, _A}, _B, _C] = Cluster =
    ra:start_local_cluster(3, "test", fun erlang:'+'/2, 9),
    {ok, {1, 1}, _Leader} = ra:send_and_await_consensus(APid, 5),
    terminate_cluster(Cluster).

send_and_notify(_Config) ->
    [{APid, _A}, _B, _C] = Cluster =
    ra:start_local_cluster(3, "test", fun erlang:'+'/2, 9),
    {ok, {1, 1}, _Leader} = ra:send_and_notify(APid, 5),
    receive
        {consensus, {1, 1}} -> ok
    after 2000 ->
              exit(consensus_timeout)
    end,
    terminate_cluster(Cluster).

dirty_query(_Config) ->
    [A, B, _C]  = Cluster =
    ra:start_local_cluster(3, "test", fun erlang:'+'/2, 9),
    {ok, {{_, _}, 9}, _} = ra:dirty_query(B, fun(S) -> S end),
    {ok, {1, 1}, Leader} = ra:send_and_await_consensus(A, 5, 1000),
    {ok, {{1, 1}, 14}, _} = ra:dirty_query(Leader, fun(S) -> S end),
    terminate_cluster(Cluster).

consistent_query(_Config) ->
    [A, _B, _C]  = Cluster =
    ra:start_local_cluster(3, "test", fun erlang:'+'/2, 0),
    {ok, {1, 1}, Leader} = ra:send_and_await_consensus(A, 9, 1000),
    {ok, {2, 1}, _Leader} = ra:send(Leader, 5),
    {ok, {{3, 1}, 14}, Leader} = ra:consistent_query(A, fun(S) -> S end),
    terminate_cluster(Cluster).

add_node(_Config) ->
    [A, _B] = Cluster = ra:start_local_cluster(2, "test", fun erlang:'+'/2, 0),
    {ok, {1, 1}, Leader} = ra:send_and_await_consensus(A, 9, 1000),
    C = ra_node:name("test", "3"),
    {ok, {2, 1}, _Leader} = ra:add_node(Leader, C),
    ok = ra:start_node(C, Cluster, fun erlang:'+'/2, 0),
    timer:sleep(2000),
    {ok, {{4, 1}, 9}, Leader} = ra:consistent_query(C, fun(S) -> S end),
    terminate_cluster([C | Cluster]).

queue_example(_Config) ->
    Self = self(),
    [A, _B, _C] = Cluster =
    ra:start_local_cluster(3, "test", fun queue_apply/2,
                     #{queue => queue:new(),
                       pending_dequeues => []}),

    {ok, {1, 1}, Leader} = ra:send(A, {dequeue, Self}),
    {ok, {2, 1}, _} = ra:send(Leader, {enqueue, test_msg}),
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
    {State#{queue => Q, pending_dequeues => Rest}, [{send_msg, Next, Item}]};
queue_apply({dequeue, For}, State = #{queue := Q0, pending_dequeues := []}) ->
    case queue:out(Q0) of
        {empty, Q} ->
            State#{queue => Q, pending_dequeues => [For]};
        {{value, Item}, Q} ->
            {State#{queue => Q}, [{send_msg, For, Item}]}
    end;
queue_apply({dequeue, For},
            State = #{queue := Q0,
                      pending_dequeues := [Next | Rest] = Pending}) ->
    case queue:out(Q0) of
        {empty, Q} ->
            State#{queue => Q, pending_dequeues => Pending ++ [For]};
        {{value, Item}, Q} ->
            {State#{queue => Q,
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

new_node(Name) ->
    ok = ra:start_node(Name, [], fun erlang:'+'/2, 0),
    ok.

stop_node(Name) ->
    ok = ra:stop_node({Name, node()}),
    ok.

add_node(Ref, New) ->
    {ok, _IdxTerm, _Leader} = ra:add_node({Ref, node()}, {New, node()}),
    ok.

remove_node(Name) ->
    {ok, _IdxTerm, _Leader} = ra:remove_node({Name, node()}, {Name, node()}),
    ok.

issue_op(Name, Op) ->
    {ok, IdxTrm, Res} = ra:send_and_await_consensus(Name, Op, 2000),
    {IdxTrm, Res}.

validate(Name, Expected) ->
    {ok, {_, Expected}, _} = ra:consistent_query({Name, node()},
                                                 fun(X) -> X end).

