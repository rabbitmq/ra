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

l() ->
    ra:start_node(ra_test, [], fun erlang:'+'/2, 0).

ql() ->
    ra:start_node(q_test, [], fun simple_apply/3, []).

qj(Leader) ->
    {ok, _, _Leader} = ra:add_node({q_test, Leader}, {q_test, node()}),
    ra:start_node(q_test, [{q_test, Leader}], fun simple_apply/3, []),
    ok.

pl() ->
    Name = p_test,
    start_persistent_node(Name, [], fun erlang:'+'/2, 0).

pj(Leader) ->
    {ok, _, _Leader} = ra:add_node({p_test, Leader}, {p_test, node()}),
    start_persistent_node(p_test, [{p_test, Leader}], fun erlang:'+'/2, 0),
    ok.

pj(Leader, Vol) ->
    Dir = filename:join(["/Volumes", Vol]),
    {ok, _, _Leader} = ra:add_node({p_test, Leader}, {p_test, node()}),
    start_persistent_node0(p_test, [{p_test, Leader}], fun erlang:'+'/2, 0, Dir),
    ok.

ps() ->
    start_persistent_node(p_test, [], fun erlang:'+'/2, 0),
    ok.

ps(Vol) ->
    Dir = filename:join(["/Volumes", Vol]),
    start_persistent_node0(p_test, [], fun erlang:'+'/2, 0, Dir),
    ok.


p_tp(Node0, C, Num) ->
    Node = {p_test, Node0},
    timer:tc(fun () ->
                     [ra:send(Node, C) || _ <- lists:seq(2, Num)],
                     ra:send_and_await_consensus(Node, C, 30000)
             end).

p_lat(Node0, C) ->
    Node = {p_test, Node0},
    timer:tc(fun () ->
                     ra:send_and_await_consensus(Node, C)
             end).

start_persistent_node(Name, Nodes, ApplyFun, InitialState) ->
    Dir = filename:join(["./tmp", ra_lib:to_list(node()),
                         ra_lib:to_list(Name)]),
    ok = filelib:ensure_dir(Dir),
    start_persistent_node0(Name, Nodes, ApplyFun, InitialState, Dir).

start_persistent_node0(Name, Nodes, ApplyFun, InitialState, Dir) ->
    application:ensure_all_started(ra),
    Conf = #{log_module => ra_log_file,
             log_init_args => #{directory => Dir},
             initial_nodes => Nodes,
             apply_fun => ApplyFun,
             init_fun => fun (_) -> InitialState end,
             cluster_id => Name,
             election_timeout_multiplier => 50},
    ra:start_node(Name, Conf).

enq(Node0) ->
    Node = {q_test, Node0},
    ra:send(Node, {enq, <<"q">>}).

deq(Node0) ->
    Node = {q_test, Node0},
    ra:send(Node, {deq, self()}),
    receive
        <<"q">> -> ok
    after 5000 ->
              exit(deq_timeout)
    end.

ed(Node, Num) ->
    [enq(Node) || _ <- lists:seq(1, Num)],
    [deq(Node) || _ <- lists:seq(1, Num)],
    ok.

j(Leader) ->
    {ok, _, _Leader} = ra:add_node({ra_test, Leader}, {ra_test, node()}),
    ok = ra:start_node(ra_test, [{ra_test, Leader}], fun erlang:'+'/2, 0),
    ok.


command(Node, C) ->
    ra:send_and_await_consensus(Node, C).

ta_cmds(Node0, C, Num) ->
    Node = {ra_test, Node0},
    timer:tc(fun () ->
        [ra:send(Node, C) || _ <- lists:seq(2, Num)],
        ra:send_and_await_consensus(Node, C)
             end).

tcmd(Node, C) ->
    timer:tc(fun () -> command(Node, C) end).

avg(Node) ->
    Num = 1000,
    L = lists:map(fun(_) ->
                          timer:sleep(2),
                          element(1, tcmd(Node, 5))
                  end, lists:seq(1, Num)),
    L1 = lists:sublist(lists:sort(L), 50, 900),
    {lists:sum(L1) / length(L1), perc(L)}.

perc(Numbers) ->
    Percentile = fun(List, Size, Perc) ->
        Element = round(Perc * Size),
        lists:nth(Element, List)
    end,
    Len = length(Numbers),
    Sorted = lists:sort(Numbers),
    [{trunc(Perc*100), Percentile(Sorted, Len, Perc)} ||
        Perc <- [0.50, 0.75, 0.90, 0.95, 0.99, 0.999]].

query(Node) ->
    ra:dirty_query(Node, fun (S) -> S end).

p(F) ->
    lg:trace([ra_node, ra_node_proc, ra_proxy, ra_log_file], lg_file_tracer,
             F ++ ".gz", #{running => false, mode => profile}).

sp() ->
    lg:stop().

lgc(Name) ->
    lg_callgrind:profile_many(Name ++ ".gz.*", Name ++ ".out",
                              #{running => false}).





simple_apply(Idx, {enq, Msg}, State) ->
    {effects, State ++ [{Idx, Msg}], [{snapshot_point, Idx}]};
simple_apply(_Idx, {deq, ToPid}, [{EncIdx, Msg} | State]) ->
    {effects, State, [{send_msg, ToPid, Msg}, {release_cursor, EncIdx, []}]};
simple_apply(_Idx, deq, [{EncIdx, _Msg} | State]) ->
    {effects, State, [{release_cursor, EncIdx, []}]};
% due to compaction there may not be a dequeue op to do
simple_apply(_Idx, _, [] = State) ->
    {effects, State, []}.
