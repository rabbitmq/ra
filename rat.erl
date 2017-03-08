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
