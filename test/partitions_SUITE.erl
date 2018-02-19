-module(partitions_SUITE).
-compile(export_all).

-include_lib("common_test/include/ct.hrl").

all() -> [
            publish_ack_no_partition
            ,publish_ack_node_leave
            ,publish_ack_node_restart
            ,publish_ack_inet_tcp_proxy_blocks_single_node
            ,publish_ack_disconnect_node
            % ,publish_ack_inet_tcp_proxy_partition_majority
         ].

init_per_testcase(TestCase, Config) ->
    Config0 = tcp_inet_proxy_helpers:enable_dist_proxy_manager(Config),
    Nodes = erlang_nodes(5),
    erlang_node_helpers:start_erlang_nodes(Nodes, Config0),
    tcp_inet_proxy_helpers:enable_dist_proxy(Nodes, Config0),
    {_, Node} = NodeId = setup_ra_cluster(TestCase, Nodes, Config),
    ok = ra:trigger_election(NodeId),
    Config1 = [{node_id, NodeId}, {nodes, Nodes} | Config0].

end_per_testcase(TestCase, Config) ->
    Nodes = ?config(nodes, Config),
    erlang_node_helpers:stop_erlang_nodes(Nodes),
    Cwd = ?config(priv_dir, Config),
    Dir = filename:join(Cwd, "partitions_SUITE"),
    os:cmd("rm -rf " ++ Dir).

publish_ack_no_partition(Config) ->
    Nodes = ?config(nodes, Config),
    EveryFun = fun() -> print_metrics(Nodes) end,
    publish_and_consume(Config, 1000, 100, EveryFun, EveryFun).

publish_ack_node_leave(Config) ->
    NodeId = ?config(node_id, Config),
    Nodes = ?config(nodes, Config),
    LogFun = fun() -> print_metrics(Nodes) end,
    StopFun = fun() -> print_metrics(Nodes), stop_random_node(NodeId, Nodes) end,
    publish_and_consume(Config, 1000, 400, LogFun, StopFun).

publish_ack_node_restart(Config) ->
    {Name, _} = NodeId = ?config(node_id, Config),
    Nodes = ?config(nodes, Config),
    LogFun = fun() -> print_metrics(Nodes) end,
    RestartFun = fun() ->
                    print_metrics(Nodes),
                    restart_random_node(Name, NodeId, Nodes, Config)
                 end,
    publish_and_consume(Config, 1000, 100, LogFun, RestartFun).

publish_ack_inet_tcp_proxy_blocks_single_node(Config) ->
    NodeId = ?config(node_id, Config),
    Nodes = ?config(nodes, Config),
    LogFun = fun() -> print_metrics(Nodes) end,
    BlockFun = fun() ->
                   print_metrics(Nodes),
                   unblock_inet_tcp_proxy(Nodes),
                   block_random_node_inet_tcp_proxy(NodeId, Nodes)
               end,
    publish_and_consume(Config, 1000, 100, LogFun, BlockFun).

publish_ack_disconnect_node(Config) ->
    NodeId = ?config(node_id, Config),
    Nodes = ?config(nodes, Config),
    LogFun = fun() -> print_metrics(Nodes) end,
    BlockFun = fun() ->
                   print_metrics(Nodes),
                   disconnect_random_node(NodeId, Nodes)
               end,
    publish_and_consume(Config, 1000, 100, LogFun, BlockFun).

% publish_ack_inet_tcp_proxy_partition_majority(Config) ->
%     NodeId = ?config(node_id, Config),
%     Nodes = ?config(nodes, Config),
%     LogFun = fun() -> print_metrics(Nodes) end,
%     BlockFun = fun() ->
%                    print_metrics(Nodes),
%                    unblock_inet_tcp_proxy(Nodes),
%                    block_random_partition_inet_tcp_proxy(NodeId, Nodes)
%                end,
%     publish_and_consume(Config, 1000, 100, LogFun, BlockFun).

publish_and_consume(Config, Total, Every, EveryPublishFun, EveryConsumeFun) ->
    error_logger:tty(false),
    NodeId = ?config(node_id, Config),
    Nodes = ?config(nodes, Config),
    Cid = customer_id(),
    {ok, _Term, Leader} = R = ra:send_and_await_consensus(NodeId, {checkout, {auto, 50}, Cid}, 10000),
    ct:pal("Checked out ~p~n", [R]),
    publish_n_messages(Total, Leader, Every, EveryPublishFun),
    print_metrics(Nodes),
    wait_for_consumption(Total, Leader, Every, EveryConsumeFun).

unblock_inet_tcp_proxy(Nodes) ->
    ct:pal("Unblocking all nodes"),
    [ tcp_inet_proxy_helpers:allow_traffic_between(Node, OtherNode)
      || OtherNode <- Nodes,
         Node <- Nodes,
         OtherNode =/= Node ].

block_random_node_inet_tcp_proxy(NodeId, Nodes) ->
    TargetNode = get_random_node(NodeId, Nodes),
    ct:pal("Blocking node ~p~n", [TargetNode]),
    [ tcp_inet_proxy_helpers:block_traffic_between(TargetNode, OtherNode)
      || OtherNode <- Nodes,
         OtherNode =/= TargetNode].

disconnect_random_node(NodeId, Nodes) ->
    TargetNode = get_random_node(NodeId, Nodes),
    ct:pal("Disconnected node ~p~n", [TargetNode]),
    rpc:call(TargetNode, inet_tcp_proxy, reconnect, [Nodes]).

% block_random_partition_inet_tcp_proxy(NodeId, Nodes) ->
%     Node1 = get_random_node(NodeId, Nodes),
%     Node2 = get_random_node(NodeId, Nodes -- [Node1]),

%     %% Node1 and Node2 will be cut off from the cluster.
%     ct:pal("Cutting off nodes ~p and ~p from the rest of the cluster",
%            [Node1, Node2]),
%     [ tcp_inet_proxy_helpers:block_traffic_between(Node1, OtherNode)
%       || OtherNode <- Nodes,
%          OtherNode =/= Node1,
%          OtherNode =/= Node2 ],
%     [ tcp_inet_proxy_helpers:block_traffic_between(Node2, OtherNode)
%       || OtherNode <- Nodes,
%          OtherNode =/= Node1,
%          OtherNode =/= Node2 ].

get_random_node(no_exceptions, Nodes) ->
    lists:nth(rand:uniform(length(Nodes)), Nodes);
get_random_node({_, Node}, Nodes) ->
    PossibleNodes = Nodes -- [Node],
    lists:nth(rand:uniform(length(PossibleNodes)), PossibleNodes).

stop_random_node(NodeId, Nodes) ->
    TargetNode = get_random_node(NodeId, Nodes),
    erlang_node_helpers:stop_erlang_node(TargetNode).

restart_random_node(Name, NodeId, Nodes, Config) ->
    RestartNode = get_random_node(NodeId, Nodes),
    ct:pal("Restarting node ~p~n", [RestartNode]),
    ct_rpc:call(RestartNode, init, stop, []),
    erlang_node_helpers:wait_for_stop(RestartNode, 100),
    erlang_node_helpers:start_erlang_node(RestartNode, Config),
    setup_ra_cluster(Name, [RestartNode], Config).

setup_ra_cluster(Name, Nodes, Config) ->
    Cwd = ?config(priv_dir, Config),
    filelib:ensure_dir(filename:join(Cwd, "partitions_SUITE")),
    start_ra_cluster(Name, Nodes, Config),
    NodeId = {Name, hd(Nodes)}.

print_metrics(Nodes) ->
    [print_node_metrics(Node) || Node <- Nodes].

print_node_metrics(Node) ->
    ct:pal("Node ~p metrics ~p~n", [Node, ct_rpc:call(Node, ets, tab2list, [ra_fifo_metrics])]).

publish_n_messages(MsgCount, NodeId, EveryN, EveryFun) ->
    spawn(fun() ->
        [begin
            Ref = make_ref(),
            Msg = {msg, N},

            {ok, _Term, _Leader} = ra:send_and_await_consensus(NodeId, {enqueue, Msg}, infinity),
            % ok = ra:send_and_notify(NodeId, {enqueue, Msg}, Ref),
            % receive
                % {ra_event, _Leader, applied, Ref} -> ok
            % after 100000 ->
                    % error(consensus_timeout)
            % end,
            case N rem EveryN of
                0 -> EveryFun();
                _ -> ok
            end
        end
         || N <- lists:seq(1, MsgCount)]
    end).

wait_for_consumption(MsgCount, NodeId, EveryN, EveryFun) ->
    ExpectedMessages = lists:seq(1, MsgCount),
    {_, Received} = lists:foldl(fun(_N, {State, Received0}) ->
        receive
        {ra_event, From, Event} ->
            case length(Received0) rem EveryN of
                0 -> EveryFun();
                _ -> ok
            end,
            case ra_fifo_client:handle_ra_event(From, Event, State) of
                {{delivery, CustomerTag, Msgs}, State1} ->
                    ct:pal("Leader is ~p~n", [From]),

                    [begin
                        ct:pal("Awaiting consensus on: ~p~n with message ~p message Id ~p~n", [From, Msg, MsgId]),
                        {ok, _, _} = R = ra:send_and_await_consensus(From, {settle, MsgId, {CustomerTag, self()}}, 50000),
                        ct:pal("Got consensus ~p~n", [R])
                     end
                     || {MsgId, Msg} <- Msgs],
                     {State1, Msgs ++ Received0};
                Other ->
                    error({unexpected_message, Other})
            end
        after 100000 ->
            {State, Received0}
        end
    end,
    {ra_fifo_client:init(erlang_nodes(5)), []},
    ExpectedMessages),
    ReceivedMsgs = [ Msg || {_, {_, {msg, Msg}}} <- Received ],
    ct:pal("Received ~p~n", [ReceivedMsgs]),
    ct:pal("Expected ~p~n", [ExpectedMessages]),

    [] = ReceivedMsgs -- ExpectedMessages,
    [] = ExpectedMessages -- ReceivedMsgs.

customer_id() ->  {<<"cid">>, self()}.

%% ==================================
%% RA cluster setup

start_ra_cluster(Name, Nodes, Config) ->
    Cwd = ?config(priv_dir, Config),
    Configs = lists:map(fun(Node) ->
        ct:pal("Start app on ~p~n", [Node]),
        NodeConfig = make_node_ra_config(Name, Nodes, Node, Cwd),
        ok = ct_rpc:call(Node, application, load, [ra]),
        ok = ct_rpc:call(Node, application, set_env, [ra, data_dir, filename:join([Cwd, "partitions_SUITE", atom_to_list(Node)])]),
        {ok, _} = ct_rpc:call(Node, application, ensure_all_started, [ra]),
        prepare_node(Node),
        NodeConfig
    end,
    Nodes),
    lists:map(fun(#{id := {_, Node}} = NodeConfig) ->
        ct:pal("Start ra node on ~p~n", [Node]),
        ok = ct_rpc:call(Node, ra, start_node, [NodeConfig]),
        NodeConfig
    end,
    Configs).

make_node_ra_config(Name, Nodes, Node, Cwd) ->
    #{ id => {Name, Node},
       uid => atom_to_binary(Name, utf8),
       initial_nodes => [{Name, N} || N <- Nodes],
       log_module => ra_log_file,
       log_init_args =>
            #{data_dir => filename:join([Cwd, "partitions_SUITE", atom_to_list(Node)]),
              uid => atom_to_binary(Name, utf8)},
       machine => {module, ra_fifo}
       }.

prepare_node(Node) ->
    spawn(Node,
          fun() ->
            ets:new(ra_fifo_metrics, [public, named_table, {write_concurrency, true}]),
            receive stop -> ok end
          end).

%% ==================================
%% Erlang node startup

erlang_nodes(5) ->
    [
     foo1@localhost,
     foo2@localhost,
     foo3@localhost,
     foo4@localhost,
     foo5@localhost
     ].
