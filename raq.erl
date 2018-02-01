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
         go/1,
         go_many/2,
         lg_trace/0,
         lg_stop/0
        ]).


%%%
%%% raq api
%%%

i(Vol0) ->
    application:load(ra),
    Vol = filename:join("/Volumes", Vol0),
    ok = application:set_env(ra, data_dir, Vol),
    application:ensure_all_started(ra),
    ok.

s(Vol) ->
    s(Vol, []),
    ra:trigger_election({raq, node()}).

s(Vol, Nodes) ->
    Dir = filename:join(["/Volumes", Vol]),
    i(Dir),
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
        {ra_event, _, machine, {msg, MsgId, _}} ->
            {ok, _, {raq, Node}} = setl(Node1, MsgId),
            go0(Node)
    after 5000 ->
              throw(timeout_waiting_for_receive)
    end.

go_many(Num, Node) ->
    [spawn(fun () -> go(Node) end) || _ <- lists:seq(1, Num)].

pop(Node) ->
    ra:send({raq, Node}, {checkout, {once, 1}, self()}),
    receive
        {ra_event, _, machine, {msg, _, _}} = Msg ->
            Msg
    after 5000 ->
              timeout
    end.

recv(Node0) ->
    receive
        {ra_event, _, machine, {msg, Id, TS}} ->
            % TODO: ra_msg should include sending node
            Now = os:system_time(millisecond),
            io:format("MsgId: ~b, Latency: ~bms~n", [Id, Now - TS]),
            {ok, _, {raq, Node}} = ra:send({raq, Node0}, {settle, Id, self()}),
            recv(Node)
    end.


start_node(Name, Nodes, Machine, Dir) ->
    UId =  atom_to_binary(Name, utf8),
    Conf = #{id => {Name, node()},
             uid => UId,
             log_module => ra_log_file,
             log_init_args => #{data_dir => Dir, uid => UId},
             initial_nodes => Nodes,
             machine => Machine,
             cluster_id => Name},
    ra:start_node(Conf).

lg_trace() ->
    Name = atom_to_list(node()),
    lg:trace([ra_node, ra_node_proc, ra_proxy, ra_log_file, ra_fifo],
             lg_file_tracer,
             Name ++ ".gz", #{running => false, mode => profile}).

lg_stop() ->
    Name = atom_to_list(node()),
    lg:stop(),
    lgc(Name).

lgc(Name) ->
    lg_callgrind:profile_many(Name ++ ".gz.*", Name ++ ".out",#{running => false}).
