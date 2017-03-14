-module(rat).

-compile(export_all).

start(n1) ->
    Peers = [{n2, ra2@snowman}, {n3, ra3@snowman}],
    ok = ra:start_node(n1, Peers, fun erlang:'+'/2, 0);
start(n2) ->
    Peers = [{n1, ra1@snowman}, {n3, ra3@snowman}],
    ok = ra:start_node(n2, Peers, fun erlang:'+'/2, 0);
start(n3) ->
    Peers = [{n1, ra1@snowman}, {n2, ra2@snowman}],
    ok = ra:start_node(n3, Peers, fun erlang:'+'/2, 0).


command(Node, C) ->
    ra:send_and_await_consensus(Node, C).

query(Node) ->
    ra:dirty_query(Node, fun (S) -> S end).

looking_glass() ->
    [A, _B, _C]  =
        ra:start_local_cluster(3, "test", fun erlang:'+'/2, 9),
        lg:trace({callback, rat, patterns}, lg_file_tracer, "traces.gz", #{running => true,
                                                                           mode => profile}),
    {ok, {1,1}, _Leader} = ra:send_and_await_consensus(A, 5),
    % lg_callgrind:profile_many("traces.gz.*", "callgrind.out", #{running => true}).
    lg:stop().

patterns() ->
    [rat, {scope, [self()]}].

    % [ {ok, _, _Leader} = ra:send_and_await_consensus(APid, 5),

