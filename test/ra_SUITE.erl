-module(ra_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").

all() ->
    [
     basic,
     send_and_await_consensus,
     send_and_notify
    ].

groups() ->
    [ {tests, [], all()} ].

basic(_Config) ->
    Self = self(),
    [{APid, _A}, _B, _C] = Cluster =
    ra:start_cluster(3, "test", fun basic_apply/2,
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
    [{APid, _A}, _B, _C]  = Cluster =
    ra:start_cluster(3, "test", fun erlang:'+'/2, 9),

    {ok, {1, 1}, _Leader} = ra:send_and_await_consensus(APid, 5),

    % {{1, 1}, 14} = ra:query(APid, fun(S) -> S end, dirty),
    terminate_cluster(Cluster).

send_and_notify(_Config) ->
    [{APid, _A}, _B, _C]  = Cluster =
    ra:start_cluster(3, "test", fun erlang:'+'/2, 9),
    {ok, {1, 1}, _Leader} = ra:send_and_notify(APid, 5),
    receive
        {consensus, {1, 1}} -> ok
    after 2000 ->
              exit(consensus_timeout)
    end,
    terminate_cluster(Cluster).

% implements a simple queue machine
basic_apply({enqueue, Msg}, State =#{queue := Q0, pending_dequeues := []}) ->
    Q = queue:in(Msg, Q0),
    State#{queue => Q};
basic_apply({enqueue, Msg}, State = #{queue := Q0,
                                      pending_dequeues := [Next | Rest]}) ->
    Q1 = queue:in(Msg, Q0),
    {{value, Item}, Q} = queue:out(Q1),
    {State#{queue => Q, pending_dequeues => Rest}, [{send_msg, Next, Item}]};
basic_apply({dequeue, For}, State = #{queue := Q0, pending_dequeues := []}) ->
    case queue:out(Q0) of
        {empty, Q} ->
            State#{queue => Q, pending_dequeues => [For]};
        {{value, Item}, Q} ->
            {State#{queue => Q}, [{send_msg, For, Item}]}
    end;
basic_apply({dequeue, For},
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

