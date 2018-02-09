-module(jepsen_like_partitions_SUITE).
-compile(export_all).

-include_lib("common_test/include/ct.hrl").

all() -> [
 publish_ack_inet_tcp_proxy
 % ,
% publish_ack_iptables
].

init_per_testcase(TestCase, Config0) ->
    error_logger:tty(false),
    Nodes = erlang_nodes(5),
    NemesisType = case TestCase of
        publish_ack_inet_tcp_proxy -> inet_tcp_proxy;
        publish_ack_iptables -> iptables
    end,
    Nemesis = spawn_nemesis(Nodes, 2, 5000, 10000, NemesisType),
    Config1 = [{nodes, Nodes}, {name, publish_ack}, {nemesis, Nemesis}, {nemesis_type, NemesisType} | Config0 ],
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
    Dir = data_dir(Config),
    os:cmd("rm -rf " ++ Dir).

publish_ack_inet_tcp_proxy(Config) -> publish_ack(Config).

publish_ack_iptables(Config) -> publish_ack(Config).

publish_ack(Config) ->
    NodeId = ?config(node_id, Config),
    Nodes = ?config(nodes, Config),

    PublisherNode = get_random_node(Nodes),
    ConsumerNode = get_random_node(Nodes),

    Publisher = spawn_publisher(PublisherNode, NodeId, 1000, self(), 30000),
    Consumer = spawn_consumer(ConsumerNode, NodeId, 1000, self(), 20000),
    Nemesis = ?config(nemesis, Config),
    start_delayed(Publisher),
    start_delayed(Consumer),

    start_delayed(Nemesis),
 %timer:sleep(10000000),
    wait_for_publisher_and_consumer(Publisher, Consumer, false, false).

wait_for_publisher_and_consumer(Publisher, Consumer, true, true) ->
    ok;
wait_for_publisher_and_consumer(Publisher, Consumer, PublisherFinished, ConsumerFinished) ->
    receive
        {consumed, Consumed, Acked, Nacked} ->
            ct:log("Consume end Delivered ~p ~p~n Acked settle ~p ~p~n Nacked settle ~p ~p~n",
                   [length(Consumed), Consumed, length(Acked), Acked, length(Nacked), Nacked]),
            wait_for_publisher_and_consumer(Publisher, Consumer, PublisherFinished, true);
        {published, Sent, Acked, Nacked} ->
            ct:pal("Publish end Sent ~p~n Acked ~p~n Nacked ~p~n",
                   [length(Sent), length(Acked), length(Nacked)]),
            ct:log("Publish end Sent ~p ~p~n Acked ~p ~p~n Nacked ~p ~p~n",
                  [length(Sent), Sent, length(Acked), Acked, length(Nacked), Nacked]),
            %% Do not wait for nacked messages on consumers
            Consumer ! {publish_end, length(Acked)},
            wait_for_publisher_and_consumer(Publisher, Consumer, true, ConsumerFinished);
        {publisher_in_progress, Sent, Acked, Nacked} ->
            ct:pal("Still waiting on publisher~n Sent ~p~n Acked ~p~n Nacked ~p~n", [length(Sent), length(Acked), length(Nacked)]),
            wait_for_publisher_and_consumer(Publisher, Consumer, PublisherFinished, ConsumerFinished);
        {consumer_in_progress, Delivered, Applied, Rejected, MsgCount} ->
            ct:pal("Still waiting on consumer.~n Delivered ~p~n Applied ~p~n Rejected ~p~n MsgCount ~p~n", [length(Delivered), length(Applied), length(Rejected), MsgCount]),
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

spawn_consumer(Node, NodeId, MessageCount, Pid, TimeToWaitForAcks0) ->
    spawn_delayed(Node,
        fun
        Consumer({init, Leader}) ->
            ct:pal("Init consumer"),
            Cid = client_id(),
            {ok, _Result, NewLeader} = ra:send_and_await_consensus(Leader, {checkout, {auto, 50}, Cid}, infinity),
            Consumer({[], [], [], MessageCount, false, TimeToWaitForAcks0});
        Consumer({Delivered, Applied, Rejected, MsgCount, PublishEnded, TimeToWaitForAcks}) ->
            receive
            {publish_end, NewMsgCount} ->
                ct:pal("Updating message count to ~p~n", [NewMsgCount]),
                Consumer({Delivered, Applied, Rejected, NewMsgCount, true, TimeToWaitForAcks});
            {ra_fifo, NewLeader, {delivery, _ClientTag, MsgId, N}} ->
            % {ra_event, NewLeader, machine, {msg, MsgId, N}} ->
                Ref = case length([D || D <- Delivered, D == N]) of
                    0 ->
                        {ack, N, 0};
                    Duplicates ->
                        ct:pal("Duplicate delivery ~p~n", [N]),
                        {ack, N, Duplicates}
                end,
                timer:sleep(100),
                %% Assuming client id does not change.
                Cid = client_id(),
                ok = ra:send_and_notify(NewLeader, {settle, MsgId, Cid}, Ref),
                Consumer({[N | Delivered], Applied, Rejected, MsgCount, PublishEnded, TimeToWaitForAcks});
            {ra_event, {applied, NewLeader, Ref}} ->
            % {ra_event, NewLeader, applied, Ref} ->
                Consumer({Delivered, [Ref | Applied], Rejected, MsgCount, PublishEnded, TimeToWaitForAcks});
            {ra_event, {rejected, NewLeader, Ref}} ->
            % {ra_event, NewLeader, rejected, Ref} ->
                Consumer({Delivered, Applied, [Ref | Rejected], MsgCount, PublishEnded, TimeToWaitForAcks})
            after 1000 ->
                case length(Applied) + length(Rejected) of
                    MsgCount ->
                        Pid ! {consumed, Delivered, Applied, Rejected},
                        ok;
                    Other ->
                        case TimeToWaitForAcks =< 0 of
                            true ->
                                ct:pal("Timeout waiting for consumer"),
                                Pid ! {consumed, Delivered, Applied, Rejected},
                                ok;
                            false ->
                                Pid ! {consumer_in_progress, Delivered, Applied, Rejected, MsgCount},
                                print_metrics(erlang_nodes(5)),
                                %%
                                NewTimeToWaitForAcks = case PublishEnded of
                                    false -> TimeToWaitForAcks;
                                    true  -> TimeToWaitForAcks - 1000
                                end,
                                % ct:pal("NewTimeToWaitForAcks ~p~n Delivered ~p~n MsgCount ~p~n", [NewTimeToWaitForAcks, Delivered, MsgCount]),
                                Consumer({Delivered, Applied, Rejected, MsgCount, PublishEnded, NewTimeToWaitForAcks})
                        end
                end
            end
        end,
        {init, NodeId}).

spawn_publisher(Node, NodeId, MessageCount, Pid, TimeToWaitForAcks0) ->
    spawn_delayed(Node,
        fun Publisher({Leader, N, Sent, Acked, Nacked, TimeToWaitForAcks}) ->
            {Sent1, N1, Wait} = case N-1 of
                MessageCount -> {Sent, N, 1000};
                _            ->
                    timer:sleep(100),
                    ok = ra:send_and_notify(Leader, {enqueue, N}, N),
                    {[N | Sent], N+1, 0}
            end,
            receive
                {ra_event, {applied, NewLeader, Ref}} ->
                % {ra_event, NewLeader, applied, Ref} ->
                    Acked1 = [Ref | Acked],
                    Publisher({NewLeader, N1, Sent1, Acked1, Nacked, TimeToWaitForAcks});
                {ra_event, {rejected, _Leader, {not_leader, NewLeader, _} = Ref}} when NewLeader =/= undefined ->
                % {ra_event, NewLeader, rejected, Ref} ->
                    Nacked1 = [Ref | Nacked],
                    Publisher({NewLeader, N1, Sent1, Acked, Nacked1, TimeToWaitForAcks});
                {ra_event, {rejected, _Leader, Ref}} ->
                % {ra_event, NewLeader, rejected, Ref} ->
                    Nacked1 = [Ref | Nacked],
                    Publisher({Leader, N1, Sent1, Acked, Nacked1, TimeToWaitForAcks})
            after Wait ->
                    case length(Acked) + length(Nacked) of
                        MessageCount ->
                            %% All messages are received
                            % ct:pal("Published ~p~n~p~n~p~n", [Sent1, Acked, Nacked]),
                            Pid ! {published, Sent1, Acked, Nacked},
                            ok;
                        Other ->
                            case TimeToWaitForAcks =< 0 of
                                true ->
                                    ct:pal("Timeout waiting for publisher."),
                                    Pid ! {published, Sent1, Acked, Nacked},
                                    ok;
                                false ->
                                    case Sent1 of
                                        MessageCount ->
                                            ct:pal("Still waiting on publisher ~p~n", [Other]),
                                            Pid ! {publisher_in_progress, Sent1, Acked, Nacked},
                                            print_metrics(erlang_nodes(5)),
                                            %% Keep waiting for acks and nacks
                                            Publisher({Leader, N1, Sent1, Acked, Nacked, TimeToWaitForAcks - Wait});
                                        _ ->
                                            Publisher({Leader, N1, Sent1, Acked, Nacked, TimeToWaitForAcks})
                                    end
                            end
                    end
            end
        end,
        {NodeId, 1, [], [], [], TimeToWaitForAcks0}).


-type wait_time() :: (Exactly :: integer() | infinity)
                     | {random, UpTo :: integer()}
                     | {random, From :: integer(), UpTo :: integer()}.

-type partition_spec() :: (Size :: integer()) | (Nodes :: [node()]).
-spec spawn_nemesis([node()], partition_spec(), wait_time(), wait_time(), inet_tcp_proxy | iptables) -> pid().
spawn_nemesis(Nodes, Partition, TimeForPartition, TimeForHeal, NemesisType) ->
    {UnblockFun, BlockFun} = case NemesisType of
        inet_tcp_proxy ->
            {fun unblock_inet_tcp_proxy/1 ,fun block_random_partition_inet_tcp_proxy/2};
        iptables ->
            {fun unblock_iptables/1, fun block_random_partition_iptables/2}
    end,
    spawn_delayed(
        fun Nemesis(ok) ->
            UnblockFun(Nodes),
            wait(TimeForHeal),
            BlockFun(Partition, Nodes),
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
    ct:pal("Waiting for ~p~n", [Time]),
    timer:sleep(Time),
    ct:pal("Finished waiting for ~p~n", [Time]);
wait({random, UpTo}) when is_integer(UpTo) ->
    Time = rand:uniform(UpTo),
    timer:sleep(Time);
wait({random, From, UpTo}) when is_integer(From), is_integer(UpTo), UpTo =/= From ->
    Time = rand:uniform(UpTo - From),
    timer:sleep(From + Time).

prepare_erlang_cluster(Config) ->
    Nodes = ?config(nodes, Config),
    case ?config(nemesis_type, Config) of
        inet_tcp_proxy ->
            Config0 = tcp_inet_proxy_helpers:enable_dist_proxy_manager(Config),
            erlang_node_helpers:start_erlang_nodes(Nodes, Config0),
            tcp_inet_proxy_helpers:enable_dist_proxy(Nodes, Config0);
        iptables ->
            erlang_node_helpers:start_erlang_nodes(Nodes, Config),
            Config
    end.

setup_ra_cluster(Config) ->
    Nodes = ?config(nodes, Config),
    Name = ?config(name, Config),
    DataDir = data_dir(Config),
    filelib:ensure_dir(DataDir),

    Configs = lists:map(fun(Node) ->
        ct:pal("Start app on ~p~n", [Node]),
        NodeConfig = make_node_ra_config(Name, Nodes, Node, DataDir),
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

make_node_ra_config(Name, Nodes, Node, DataDir) ->
    #{ id => {Name, Node},
       uid => atom_to_binary(Name, utf8),
       initial_nodes => [{Name, N} || N <- Nodes],
       log_module => ra_log_file,
       log_init_args =>
            #{data_dir => filename:join([DataDir, atom_to_list(Node)]),
              uid => atom_to_binary(Name, utf8)},
       machine => {module, ra_fifo}
       }.

print_metrics(Nodes) ->
    [print_node_metrics(Node) || Node <- Nodes].

print_node_metrics(Node) ->
    ct:pal("Node ~p metrics ~p~n", [Node, ct_rpc:call(Node, ets, tab2list, [ra_fifo_metrics])]).


unblock_iptables(Nodes) ->
    ct:pal("Rejoining all nodes"),
    iptables_cmd("-D INPUT -j partitions_test"),
    iptables_cmd("-F partitions_test"),
    iptables_cmd("-X partitions_test").

block_random_partition_iptables(Partition, Nodes) ->
    ensure_iptables_chain(),
    block_random_partition(Partition, Nodes, fun block_traffic_with_iptables/2).

block_traffic_with_iptables(Node1, Node2) ->
    DestPort1 = tcp_inet_proxy_helpers:get_dist_port(Node1),
    DestPort2 = tcp_inet_proxy_helpers:get_dist_port(Node2),
    SourcePort1 = get_outgoing_port(Node1, Node2, DestPort2),
    SourcePort2 = get_outgoing_port(Node2, Node1, DestPort1),
ct:pal(" DestPort1 ~p~n DestPort2 ~p~n SourcePort1 ~p~n SourcePort2 ~p~n", [DestPort1, DestPort2, SourcePort1, SourcePort2]),
    case SourcePort1 of
        undefined -> ok;
        _ ->
            block_ports_iptables(DestPort2, SourcePort1)
    end,
    case SourcePort2 of
        undefined -> ok;
        _ ->
            block_ports_iptables(DestPort1, SourcePort2)
    end,
% timer:sleep(10000000),
    tcp_inet_proxy_helpers:wait_for_blocked(Node1, Node2, 100).

block_ports_iptables(DestPort, SourcePort) ->
ct:pal("Cutting port ~p and ~p~n", [DestPort, SourcePort]),
    iptables_cmd("-A partitions_test -p tcp -j DROP"
                 " --destination-port " ++ integer_to_list(DestPort) ++
                 " --source-port " ++ integer_to_list(SourcePort)),
    iptables_cmd("-A partitions_test -p tcp -j DROP"
                 " --destination-port " ++ integer_to_list(SourcePort) ++
                 " --source-port " ++ integer_to_list(DestPort)).

ensure_iptables_chain() ->
    iptables_cmd("-N partitions_test"),
    iptables_cmd("-A INPUT -j partitions_test").

iptables_cmd(Cmd) ->
    ct:pal("Running iptables " ++ Cmd),
    Res = os:cmd("iptables " ++ Cmd),
    ct:pal("Iptables result: " ++ Res).

get_outgoing_port(Node1, Node2, DestPort) ->
    rpc:call(Node1, rpc, call, [Node2, erlang, self, []]),
    rpc:call(Node2, rpc, call, [Node1, erlang, self, []]),

    rpc:call(Node1, jepsen_like_partitions_SUITE, get_outgoing_port, [Node2, DestPort]).

get_outgoing_port(Node, DestPort) ->
    %% Ensure there is a connection.
    DestPort = rpc:call(Node, tcp_inet_proxy_helpers, get_dist_port, []),
    DistributionSockets = lists:filter(fun(Port) ->
        case erlang:port_info(Port, name) of
            {name, "tcp_inet"} ->
                case inet:peername(Port) of
                    {ok, {_PH, DestPort}} -> true;
                    _ -> false
                end;
            _ -> false
        end
    end,
    erlang:ports()),
    case DistributionSockets of
        [DistributionSocket|_] ->
            {ok, Port} = inet:port(DistributionSocket),
            Port;
        [] -> undefined
    end.

block_random_partition(Partition, Nodes, PartitionFun) ->
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
            [ PartitionFun(Node, OtherNode)
              || OtherNode <- Partition2 ]
        end,
        Partition1).

unblock_inet_tcp_proxy(Nodes) ->
    ct:pal("Rejoining all nodes"),
    [ tcp_inet_proxy_helpers:allow_traffic_between(Node, OtherNode)
      || OtherNode <- Nodes,
         Node <- Nodes,
         OtherNode =/= Node ].

block_random_partition_inet_tcp_proxy(Partition, Nodes) ->
    block_random_partition(Partition, Nodes,
                           fun tcp_inet_proxy_helpers:block_traffic_between/2).

get_random_node(Nodes) ->
    lists:nth(rand:uniform(length(Nodes)), Nodes).

get_random_node(Exceptions, Nodes) ->
    PossibleNodes = Nodes -- Exceptions,
    get_random_node(PossibleNodes).


data_dir(Config) ->
    Cwd = ?config(priv_dir, Config),
    filename:join(Cwd, "jepsen_like_partitions_SUITE").
