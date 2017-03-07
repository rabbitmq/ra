-module(ra_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").

all() ->
    [
     single_node,
     start_node,
     queue,
     send_and_await_consensus,
     send_and_notify,
     dirty_query,
     consistent_query
    ].

groups() ->
    [{tests, [], all()}].

single_node(_Config) ->
    ok = ra:start_node(n1, [{n2, node()}, {n3, node()}], fun erlang:'+'/2, 0),
    {timeout, _} = ra:send_and_await_consensus({n1, node()}, 5).

start_node(_Config) ->
    % start the first node and wait a bit
    ok = ra:start_node(n1, [{n2, node()}, {n3, node()}], fun erlang:'+'/2, 0),
    timer:sleep(5000),
    % start second node
    ok = ra:start_node(n2, [{n1, node()}, {n3, node()}], fun erlang:'+'/2, 0),
    % a consensus command tells us there is a functioning cluster
    {ok, {1, Term}, _Leader} = ra:send_and_await_consensus({n2, node()}, 5),
    ct:pal("Term ~p~n", [Term]),
    % start the 3rd node and issue another command
    ok = ra:start_node(n3, [{n1, node()}, {n2, node()}], fun erlang:'+'/2, 0),
    timer:sleep(1000),
    % issue command
    {ok, {2, Term}, _Leader} = ra:send_and_await_consensus({n3, node()}, 5),
    % shut down n2
    gen_statem:stop(n2, normal, 2000),
    % issue command to confirm n3 joined the cluster successfully
    {ok, {3, Term}, _Leader} = ra:send_and_await_consensus({n3, node()}, 5),
    terminate_cluster([n1, n2]).

queue(_Config) ->
    Self = self(),
    [{APid, _A}, _B, _C] = Cluster =
    ra:start_cluster(3, "test", fun queue_apply/2,
                     #{queue => queue:new(),
                       pending_dequeues => []}),

    {ok, {1, 1}, Leader} = ra:send(APid, {dequeue, Self}),
    {ok, {2, 1}, _} = ra:send(Leader, {enqueue, test_msg}),
    waitfor(test_msg, apply_timeout),
    % check that the message isn't delivered multiple times
    receive
        test_msg -> exit(double_delivery)
    after 500 -> ok
    end,
    terminate_cluster(Cluster).

send_and_await_consensus(_Config) ->
    [{APid, _A}, _B, _C] = Cluster =
    ra:start_cluster(3, "test", fun erlang:'+'/2, 9),
    {ok, {1, 1}, _Leader} = ra:send_and_await_consensus(APid, 5),
    terminate_cluster(Cluster).

send_and_notify(_Config) ->
    [{APid, _A}, _B, _C] = Cluster =
    ra:start_cluster(3, "test", fun erlang:'+'/2, 9),
    {ok, {1, 1}, _Leader} = ra:send_and_notify(APid, 5),
    receive
        {consensus, {1, 1}} -> ok
    after 2000 ->
              exit(consensus_timeout)
    end,
    terminate_cluster(Cluster).

dirty_query(_Config) ->
    [{APid, _A}, _B, _C]  = Cluster =
    ra:start_cluster(3, "test", fun erlang:'+'/2, 9),
    {ok, {1, 1}, _Leader} = ra:send_and_await_consensus(APid, 5),
    {ok, {1, 1}, 14} = ra:dirty_query(APid, fun(S) -> S end),
    terminate_cluster(Cluster).

% consistent_query(_Config) ->
%     [{APid, _A}, _B, _C]  = Cluster =
%     ra:start_cluster(3, "test", fun erlang:'+'/2, 9),
%     {ok, {1, 1}, _Leader} = ra:send(APid, 5),
%     {ok, {1, 1}, 14} = ra:consistent_query(APid, fun(S) -> S end),
%     terminate_cluster(Cluster).

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
    [gen_statem:stop(P, normal, 2000) || {P, _} <- Nodes].

