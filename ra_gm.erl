-module(ra_gm).


-compile(export_all).

wait() ->
    receive
        {qp, Procs} ->
            Procs
    end.

start(n1) ->
    register(shell, self()),
    State = wait(),
    io:format("starting"),
    Peers = [{n2, ra2@snowman}, {n3, ra3@snowman}],
    ok = ra:start_node(n1, Peers, fun gm_apply/2, State);
start(n2) ->
    register(shell, self()),
    State = wait(),
    io:format("starting"),
    Peers = [{n1, ra1@snowman}, {n3, ra3@snowman}],
    ok = ra:start_node(n2, Peers, fun gm_apply/2, State);
start(n3) ->
    register(shell, self()),
    State = wait(),
    io:format("starting"),
    Peers = [{n1, ra1@snowman}, {n2, ra2@snowman}],
    ok = ra:start_node(n3, Peers, fun gm_apply/2, State).

command(Node, C) ->
    ra:send_and_await_consensus(Node, C).

gm_apply(Msg, QueueProcs) ->
    % just create some send_msg effects to forward message on to queue processes
    Effects = [{send_msg, Q, Msg} || Q <- QueueProcs],
    {QueueProcs, Effects}.

queue_receive() ->
    receive
        Msg ->
            io:format("qp msg: ~p~n", [Msg]),
            queue_receive()
    end.

start_queues() ->
    Q = spawn(fun queue_receive/0),
    [{shell, N} ! {qp, [Q]} || N <-
                               [ra1@snowman,
                                ra2@snowman,
                                ra3@snowman]].

