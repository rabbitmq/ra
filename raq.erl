-module(raq).

-export([
         s/1,
         sj/2,
         pub/2,
         pub_wait/2,
         pop/1,
         check/1,
         auto/1,
         recv/1
        ]).


%%%
%%% raq api
%%%

s(Vol) ->
    Dir = filename:join(["/Volumes", Vol]),
    start_node(raq, [], fun ra_fifo:apply/3, ra_fifo:empty_state(), Dir).

sj(PeerNode, Vol) ->
    {ok, _, _Leader} = ra:add_node({raq, PeerNode}, {raq, node()}),
    s(Vol).

check(Node) ->
    ra:send({raq, Node}, {checkout, {once, 5}, self()}).

auto(Node) ->
    ra:send({raq, Node}, {checkout, {auto, 5}, self()}).

pub(Node, Msg) ->
    ra:send({raq, Node}, {enqueue, Msg}).

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


start_node(Name, Nodes, ApplyFun, InitialState, Dir) ->
    Conf = #{log_module => ra_log_file,
             log_init_args => #{directory => Dir},
             initial_nodes => Nodes,
             apply_fun => ApplyFun,
             initial_state => InitialState,
             cluster_id => Name},
    ra:start_node(Name, Conf).

