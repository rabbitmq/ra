-module(raq).

-export([
         i/1,
         s/1,
         sj/2,
         pub/1,
         setl/2,
         pub_wait/1,
         pub_many/2,
         pop/1,
         check/1,
         auto/1,
         recv/1
        ]).


%%%
%%% raq api
%%%

i(Vol) ->
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
    Dir = filename:join(["/Volumes", Vol]),
    i(Dir),
    InitFun = fun (Name) ->
                      _ = ets:insert(ra_fifo_metrics, {Name, 0, 0, 0, 0}),
                      ra_fifo:init(raq)
              end,
    start_node(raq, [], fun ra_fifo:apply/3, InitFun, Dir).

sj(PeerNode, Vol) ->
    {ok, _, _Leader} = ra:add_node({raq, PeerNode}, {raq, node()}),
    s(Vol).

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
        {msg, Id, TS} ->
            Now = os:system_time(millisecond),
            io:format("MsgId: ~b, Latency: ~bms~n", [Id, Now - TS]),
            ra:send({raq, Node}, {settle, Id, self()}),
            recv(Node)
    end.


start_node(Name, Nodes, ApplyFun, InitFun, Dir) ->
    Conf = #{log_module => ra_log_file,
             log_init_args => #{directory => Dir, id => Name},
             initial_nodes => Nodes,
             apply_fun => ApplyFun,
             init_fun => InitFun,
             cluster_id => Name},
    ra:start_node(Name, Conf).

