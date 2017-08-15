-module(raq).

-export([
         i/0,
         s/1,
         sj/2,
         pub/2,
         setl/2,
         pub_wait/2,
         pop/1,
         check/1,
         auto/1,
         recv/1
        ]).


%%%
%%% raq api
%%%

i() ->
    application:ensure_all_started(ra),
    case ets:info(ra_fifo_metrics) of
        undefined ->
            _ = ets:new(ra_fifo_metrics, [public, named_table, {write_concurrency, true}]);
        _ ->
            ok
    end,
    ok.

s(Vol) ->
    i(),
    Dir = filename:join(["/Volumes", Vol]),
    InitFun = fun (Name) ->
                      _ = ets:insert(ra_fifo_metrics, {Name, 0, 0, 0, 0}),
                      ra_fifo:init(raq)
              end,
    start_node(raq, [], fun ra_fifo:apply/3, InitFun, Dir).

sj(PeerNode, Vol) ->
    i(),
    {ok, _, _Leader} = ra:add_node({raq, PeerNode}, {raq, node()}),
    s(Vol).

check(Node) ->
    ra:send({raq, Node}, {checkout, {once, 5}, self()}).

auto(Node) ->
    ra:send({raq, Node}, {checkout, {auto, 5}, self()}).

pub(Node, Msg) ->
    ra:send({raq, Node}, {enqueue, Msg}).

setl(Node, MsgId) ->
    ra:send({raq, Node}, {settle, MsgId, self()}).

pub_wait(Node, Msg) ->
    ra:send_and_await_consensus({raq, Node}, {enqueue, Msg}).

pop(Node) ->
    ra:send({raq, Node}, {checkout, {once, 1}, self()}),
    receive
        {msg, _, _} = Msg ->
            Msg
    after 5000 ->
              timeout
    end.

recv(Node) ->
    receive
        {msg, Idx, _} = Msg ->
            io:format("got ~p~n", [Msg]),
            ra:send({raq, Node}, {settle, Idx, self()}),
            recv(Node)
    end.


start_node(Name, Nodes, ApplyFun, InitFun, Dir) ->
    Conf = #{log_module => ra_log_file,
             log_init_args => #{directory => Dir},
             initial_nodes => Nodes,
             apply_fun => ApplyFun,
             init_fun => InitFun,
             cluster_id => Name},
    ra:start_node(Name, Conf).

