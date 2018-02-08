-module(tcp_inet_proxy_helpers).

-export([enable_dist_proxy_manager/1,
         enable_dist_proxy/2,
         start_dist_proxy_on_node/1,
         disconnect_from_other_nodes/1,
         block_traffic_between/2,
         allow_traffic_between/2]).
-export([get_dist_port/0]).

enable_dist_proxy_manager(Config) ->
    inet_tcp_proxy_manager:start(),
    rabbit_ct_helpers:set_config(Config,
      {erlang_dist_module, inet_proxy_dist}).

enable_dist_proxy(Nodes, Config) ->
    ManagerNode = node(),
    %% We first start the proxy process on all nodes, then we close the
    %% existing connection.
    %%
    %% If we do that in a single loop, i.e. start the proxy on node 1
    %% and disconnect it, then, start the proxy on node 2 and disconnect
    %% it, etc., there is a chance that the connection is reopened
    %% by a node where the proxy is still disabled. Therefore, that
    %% connection would bypass the proxy process even though we believe
    %% it to be enabled.
    Map = lists:map(
      fun(Node) ->
          {DistPort, ProxyPort} = ct_rpc:call(Node, ?MODULE, start_dist_proxy_on_node, [ManagerNode])
      end, Nodes),
    ok = lists:foreach(fun(Node) ->
        ok = ct_rpc:call(Node, application, set_env, [kernel, dist_and_proxy_ports_map, Map])
    end,
    Nodes),
    ok = lists:foreach(
      fun(Node) ->
          ok = ct_rpc:call(Node, ?MODULE, disconnect_from_other_nodes, [Nodes -- [Node]])
      end, Nodes),
    Config.

start_dist_proxy_on_node(ManagerNode) ->
    DistPort = get_dist_port(),
    ProxyPort = get_free_port(),
    ok = inet_tcp_proxy:start(ManagerNode, DistPort, ProxyPort),
    {DistPort, ProxyPort}.

disconnect_from_other_nodes(Nodes) ->
    ok = inet_tcp_proxy:reconnect(Nodes).

get_dist_port() ->
    [Self | _] = string:split(atom_to_list(node()), "@"),
    {ok, Names} = net_adm:names(),
    proplists:get_value(Self, Names).

get_free_port() ->
    {ok, Listen} = gen_tcp:listen(0, []),
    {ok, Port} = inet:port(Listen),
    gen_tcp:close(Listen),
    Port.

block_traffic_between(NodeA, NodeB) ->
    true = rpc:call(NodeA, inet_tcp_proxy, block, [NodeB]),
    true = rpc:call(NodeB, inet_tcp_proxy, block, [NodeA]),
    wait_for_blocked(NodeA, NodeB, 10).

allow_traffic_between(NodeA, NodeB) ->
    true = rpc:call(NodeA, inet_tcp_proxy, allow, [NodeB]),
    true = rpc:call(NodeB, inet_tcp_proxy, allow, [NodeA]),
    wait_for_unblocked(NodeA, NodeB, 10).

wait_for_blocked(NodeA, NodeB, 0) ->
    error({failed_to_block, NodeA, NodeB, no_more_attempts});
wait_for_blocked(NodeA, NodeB, Attempts) ->
    case rpc:call(NodeA, rpc, call, [NodeB, erlang, node, []], 1000) of
        {badrpc, _} -> ok;
        _           ->
            timer:sleep(50),
            wait_for_blocked(NodeA, NodeB, Attempts - 1)
    end.

wait_for_unblocked(NodeA, NodeB, 0) ->
    error({failed_to_unblock, NodeA, NodeB, no_more_attempts});
wait_for_unblocked(NodeA, NodeB, Attempts) ->
    case rpc:call(NodeA, rpc, call, [NodeB, erlang, node, []], 1000) of
        NodeB       -> ok;
        {badrpc, _} ->
            timer:sleep(50),
            wait_for_unblocked(NodeA, NodeB, Attempts - 1)
    end.
