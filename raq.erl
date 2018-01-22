-module(raq).

-export([
         i/1,
         s/1,
         s/2,
         sj/2,
         pub/1,
         setl/2,
         pub_wait/1,
         pub_many/2,
         pop/1,
         check/1,
         auto/1,
         recv/1,
         go/1
        ]).


%%%
%%% raq api
%%%

i(Vol0) ->
    application:load(ra),
    Vol = filename:join("/Volumes", Vol0),
    ok = application:set_env(ra, data_dir, Vol),
    application:ensure_all_started(ra),
    case ets:info(ra_fifo_metrics) of
        undefined ->
            _ = ets:new(ra_fifo_metrics, [public, named_table, {write_concurrency, true}]);
        _ ->
            ok
    end,
    ok.

s(Vol) ->
    s(Vol, []),
    ra:trigger_election({raq, node()}).

s(Vol, Nodes) ->
    Dir = filename:join(["/Volumes", Vol]),
    i(Dir),
    _ = ets:insert(ra_fifo_metrics, {raq, 0, 0, 0, 0}),
    start_node(raq, [{raq, N} || N <- Nodes],
               {module, ra_fifo}, Dir).

sj(PeerNode, Vol) ->
    {ok, _, _Leader} = ra:add_node({raq, PeerNode}, {raq, node()}),
    % tryi init node without known peers
    s(Vol, []).

check(Node) ->
    ra:send({raq, Node}, {checkout, {once, 5}, self()}).

auto(Node) ->
    ra:send({raq, Node}, {checkout, {auto, 5}, self()}).

pub(Node) ->
    Msg = os:system_time(millisecond),
    ra:send({raq, Node}, {enqueue, Msg}).

setl(Node, MsgId) ->
    ra:send({raq, Node}, {settle, MsgId, self()}).

pub_wait(Node) ->
    Msg = os:system_time(millisecond),
    ra:send_and_await_consensus({raq, Node}, {enqueue, Msg}).

pub_many(Node, Num) ->
    timer:tc(fun () ->
                     [pub(Node) || _ <- lists:seq(2, Num)],
                     pub_wait(Node)
             end).

go(Node) ->
    auto(Node),
    go0(Node).

go0(Node0) ->
    TS = os:system_time(millisecond),
    Data = crypto:strong_rand_bytes(1024),
    {ok, _, {raq, Node1}} = ra:send({raq, Node0}, {enqueue, {TS, Data}}),
    receive
        {msg, MsgId, _} ->
            {ok, _, {raq, Node}} = setl(Node1, MsgId),
            go0(Node)
    after 5000 ->
              throw(timeout_waiting_for_receive)
    end.

pop(Node) ->
    ra:send({raq, Node}, {checkout, {once, 1}, self()}),
    receive
        {msg, _, _} = Msg ->
            Msg
    after 5000 ->
              timeout
    end.

recv(Node0) ->
    receive
        {msg, Id, TS} ->
            % TODO: ra_msg should include sending node
            Now = os:system_time(millisecond),
            io:format("MsgId: ~b, Latency: ~bms~n", [Id, Now - TS]),
            {ok, _, {raq, Node}} = ra:send({raq, Node0}, {settle, Id, self()}),
            recv(Node)
    end.


start_node(Name, Nodes, Machine, Dir) ->
    Conf = #{log_module => ra_log_file,
             log_init_args => #{data_dir => Dir, id => Name},
             initial_nodes => Nodes,
             machine => Machine,
             cluster_id => Name},
    ra:start_node(Name, Conf).

