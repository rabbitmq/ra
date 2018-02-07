-module(jepsen_like_partitions_SUITE).
-compile(export_all).

-include_lib("common_test/include/ct.hrl").

all() -> [publish_ack].

init_per_testcase(_, Config0) ->
    error_logger:tty(false),
    Nodes = erlang_nodes(5),
    Nemesis = spawn_nemesis(Nodes, 2, {random, 10000}, {random, 10000, 20000}),
    Config1 = [{nodes, Nodes}, {name, publish_ack}, {nemesis, Nemesis} | Config0 ],
    Config2 = prepare_erlang_cluster(Config1),
    NodeId = setup_ra_cluster(Config2),
    %% Make sure nodes are synchronised
    ct:pal("Members ~p~n", [ra:members(NodeId)]),
    [{node_id, NodeId} | Config2].

end_per_testcase(_, Config) ->
    Nemesis = ?config(nemesis, Config),
    unlink(Nemesis),
    exit(Nemesis, stop),
    undefined = process_info(Nemesis),

    Nodes = ?config(nodes, Config),
    erlang_node_helpers:stop_erlang_nodes(Nodes),
    ct:pal("Stopped nodes ~p~n", [Nodes]),
    Dir = data_dir(),
    os:cmd("rm -rf " ++ Dir).

publish_ack(Config) ->
    NodeId = ?config(node_id, Config),
    Nodes = ?config(nodes, Config),

    PublisherNode = get_random_node(Nodes),
    ConsumerNode = get_random_node(Nodes),

    Publisher = spawn_publisher(PublisherNode, NodeId, 10000, self()),
    Consumer = spawn_consumer(ConsumerNode, NodeId, 10000, self()),
    Nemesis = ?config(nemesis, Config),
    start_delayed(Publisher),
    start_delayed(Consumer),

    start_delayed(Nemesis),

    wait_for_publisher_and_consumer(Publisher, Consumer, false, false).

wait_for_publisher_and_consumer(Publisher, Consumer, true, true) ->
    ok;
wait_for_publisher_and_consumer(Publisher, Consumer, PublisherFinished, ConsumerFinished) ->
    receive
        {consumed, Consumed} ->
            ct:log(" Consumed ~p~n", [Consumed]),
            wait_for_publisher_and_consumer(Publisher, Consumer, PublisherFinished, true);
        {published, Sent, Acked, Nacked} ->
            ct:log(" Sent ~p~n Acked ~p~n Nacked ~p~n", [Sent, Acked, Nacked]),
            wait_for_publisher_and_consumer(Publisher, Consumer, true, ConsumerFinished);
        {publisher_in_progress, Sent, Acked, Nacked} ->
            ct:pal("Still waiting on publisher~n Sent ~p~n Acked ~p~n Nacked ~p~n", [length(Sent), length(Acked), length(Nacked)]),
            wait_for_publisher_and_consumer(Publisher, Consumer, PublisherFinished, ConsumerFinished);
        {consumer_in_progress, Delivered, Applied, Rejected} ->
            ct:pal("Still waiting on consumer.~n Delivered ~p~n Applied ~p~n Rejected ~p~n", [length(Delivered), length(Applied), length(Rejected)]),
            wait_for_publisher_and_consumer(Publisher, Consumer, PublisherFinished, ConsumerFinished)
    end.

erlang_nodes(5) ->
    [
     foo1@localhost,
     foo2@localhost,
     foo3@localhost,
     foo4@localhost,
     foo5@localhost
     ].

client_id() ->
     {<<"consumer">>, self()}.

spawn_consumer(Node, NodeId, MessageCount, Pid) ->
    spawn_delayed(Node,
        fun
        Consumer({init, Leader}) ->
            ct:pal("Init consumer"),
            Cid = client_id(),
            {ok, _Result, NewLeader} = ra:send_and_await_consensus(Leader, {checkout, {auto, 50}, Cid}, infinity),
            Consumer({NewLeader, [], [], []});
        Consumer({Leader, Delivered, Applied, Rejected}) ->
            receive
            {ra_fifo, NewLeader, {delivery, _ClientTag, MsgId, N}} ->
            % {ra_event, NewLeader, machine, {msg, MsgId, N}} ->
                Ref = case length([D || D <- Delivered, D == N]) of
                    0 ->
                        {ack, N, 0};
                    Duplicates ->
                        ct:pal("Duplicate delivery ~p~n", [N]),
                        {ack, N, Duplicates}
                end,
                %% Assuming client id does not change.
                Cid = client_id(),
                ok = ra:send_and_notify(NewLeader, {settle, MsgId, Cid}, Ref),
                Consumer({NewLeader, [N | Delivered], Applied, Rejected});
            {ra_event, {applied, NewLeader, Ref}} ->
            % {ra_event, NewLeader, applied, Ref} ->
                Consumer({NewLeader, Delivered, [Ref | Applied], Rejected});
            {ra_event, {rejected, NewLeader, Ref}} ->
            % {ra_event, NewLeader, rejected, Ref} ->
                Consumer({NewLeader, Delivered, Applied, [Ref | Rejected]})
            after 10000 ->
                case length(Applied) + length(Rejected) of
                    MessageCount ->
                        Pid ! {consumed, Delivered},
                        ok;
                    Other ->
                        ct:pal("Still waiting on consumer ~p~n", [Other]),
                        Pid ! {consumer_in_progress, Delivered, Applied, Rejected},
                        % print_metrics(erlang_nodes(5)),
                        %% Keep waiting
                        Consumer({Leader, Delivered, Applied, Rejected})
                end
            end
        end,
        {init, NodeId}).

spawn_publisher(Node, NodeId, MessageCount, Pid) ->
    spawn_delayed(Node,
        fun Publisher({Leader, N, Sent, Acked, Nacked}) ->
            {Sent1, N1, Wait} = case N-1 of
                MessageCount -> {Sent, N, 1000};
                _            ->
                    ok = ra:send_and_notify(Leader, {enqueue, N}, N),
                    {[N | Sent], N+1, 0}
            end,
            receive
                {ra_event, {applied, NewLeader, Ref}} ->
                % {ra_event, NewLeader, applied, Ref} ->
                    Acked1 = [Ref | Acked],
                    Publisher({NewLeader, N1, Sent1, Acked1, Nacked});
                {ra_event, {rejected, NewLeader, Ref}} ->
                % {ra_event, NewLeader, rejected, Ref} ->
                    Nacked1 = [Ref | Nacked],
                    Publisher({NewLeader, N1, Sent1, Acked, Nacked1})
            after Wait ->
                    case length(Acked) + length(Nacked) of
                        MessageCount ->
                            %% All messages are received
                            % ct:pal("Published ~p~n~p~n~p~n", [Sent1, Acked, Nacked]),
                            Pid ! {published, Sent1, Acked, Nacked},
                            ok;
                        Other ->
                            ct:pal("Still waiting on publisher ~p~n", [Other]),
                            Pid ! {publisher_in_progress, Sent, Acked, Nacked},
                            % print_metrics(erlang_nodes(5)),
                            %% Keep waiting for acks and nacks
                            Publisher({Leader, N1, Sent1, Acked, Nacked})
                    end
            end
        end,
        {NodeId, 1, [], [], []}).


-type wait_time() :: (Exactly :: integer() | infinity)
                     | {random, UpTo :: integer()}
                     | {random, From :: integer(), UpTo :: integer()}.

-type partition_spec() :: (Size :: integer()) | (Nodes :: [node()]).
-spec spawn_nemesis([node()], partition_spec(), wait_time(), wait_time()) -> pid().
spawn_nemesis(Nodes, Partition, TimeForPartition, TimeForHeal) ->
    spawn_delayed(
        fun Nemesis(ok) ->
            unblock_inet_tcp_proxy(Nodes),
            wait(TimeForHeal),
            block_random_partition_inet_tcp_proxy(Partition, Nodes),
            wait(TimeForPartition),
            Nemesis(ok)
        end,
        ok).

start_delayed(Pid) ->
    ct:pal("Starting ~p~n", [Pid]),
    Pid ! start.


spawn_delayed(Node, Fun, State) ->
    spawn_link(Node, fun() ->
        receive start -> ok
        end,
        ct:pal("Started ~p~n", [self()]),
        Fun(State)
    end).

spawn_delayed(Fun, State) ->
    spawn_delayed(node(), Fun, State).

wait(Time) when is_integer(Time); Time == infinity ->
    timer:sleep(Time);
wait({random, UpTo}) when is_integer(UpTo) ->
    Time = rand:uniform(UpTo),
    timer:sleep(Time);
wait({random, From, UpTo}) when is_integer(From), is_integer(UpTo), UpTo =/= From ->
    Time = rand:uniform(UpTo - From),
    timer:sleep(From + Time).

prepare_erlang_cluster(Config) ->
    Nodes = ?config(nodes, Config),
    Config0 = tcp_inet_proxy_helpers:enable_dist_proxy_manager(Config),
    erlang_node_helpers:start_erlang_nodes(Nodes, Config0),
    tcp_inet_proxy_helpers:enable_dist_proxy(Nodes, Config0).

setup_ra_cluster(Config) ->
    Nodes = ?config(nodes, Config),
    Name = ?config(name, Config),
    {ok, Cwd} = file:get_cwd(),
    DataDir = data_dir(),
    filelib:ensure_dir(DataDir),

    Configs = lists:map(fun(Node) ->
        ct:pal("Start app on ~p~n", [Node]),
        NodeConfig = make_node_ra_config(Name, Nodes, Node, Cwd),
        ok = ct_rpc:call(Node, application, load, [ra]),
        ok = ct_rpc:call(Node, application, set_env, [ra, data_dir, filename:join([DataDir, atom_to_list(Node)])]),
        {ok, _} = ct_rpc:call(Node, application, ensure_all_started, [ra]),
        spawn(Node, fun() ->
            ets:new(ra_fifo_metrics, [public, named_table, {write_concurrency, true}]),
            receive stop -> ok end
        end),
        NodeConfig
    end,
    Nodes),
    lists:map(fun(#{id := {_, Node}} = NodeConfig) ->
        ct:pal("Start ra node on ~p~n", [Node]),
        ok = ct_rpc:call(Node, ra, start_node, [NodeConfig]),
        NodeConfig
    end,
    Configs),

    NodeId = {Name, hd(Nodes)},
    ok = ra:trigger_election(NodeId),
    NodeId.

make_node_ra_config(Name, Nodes, Node, Cwd) ->
    #{ id => {Name, Node},
       uid => atom_to_binary(Name, utf8),
       initial_nodes => [{Name, N} || N <- Nodes],
       log_module => ra_log_file,
       log_init_args =>
            #{data_dir => filename:join([data_dir(), atom_to_list(Node)]),
              uid => atom_to_binary(Name, utf8)},
       machine => {module, ra_fifo}
       }.

print_metrics(Nodes) ->
    [print_node_metrics(Node) || Node <- Nodes].

print_node_metrics(Node) ->
    ct:pal("Node ~p metrics ~p~n", [Node, ct_rpc:call(Node, ets, tab2list, [ra_fifo_metrics])]).



unblock_inet_tcp_proxy(Nodes) ->
    ct:pal("Rejoining all nodes"),
    [ tcp_inet_proxy_helpers:allow_traffic_between(Node, OtherNode)
      || OtherNode <- Nodes,
         Node <- Nodes,
         OtherNode =/= Node ].

block_random_partition_inet_tcp_proxy(Partition, Nodes) ->
    Partition1 = case Partition of
        PartitionSize when is_integer(PartitionSize) ->
            lists:foldl(fun(_, SelectedNodes) ->
                Node = get_random_node(SelectedNodes, Nodes),
                [Node | SelectedNodes]
            end,
            [],
            lists:seq(1, PartitionSize));
        PartitionNodes when is_list(PartitionNodes) ->
            PartitionNodes
    end,

    ct:pal("Cutting off nodes: ~p from the rest of the cluster",
           [Partition1]),

    Partition2 = Nodes -- Partition1,

    lists:foreach(
        fun(Node) ->
            [ tcp_inet_proxy_helpers:block_traffic_between(Node, OtherNode)
              || OtherNode <- Partition2 ]
        end,
        Partition1).

get_random_node(Nodes) ->
    lists:nth(rand:uniform(length(Nodes)), Nodes).

get_random_node(Exceptions, Nodes) ->
    PossibleNodes = Nodes -- Exceptions,
    get_random_node(PossibleNodes).


data_dir() ->
    {ok, Cwd} = file:get_cwd(),
    filename:join(Cwd, "jepsen_like_partitions_SUITE").
