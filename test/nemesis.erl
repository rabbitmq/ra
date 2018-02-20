-module(nemesis).

-behaviour(gen_server).

%% API functions
-export([start_link/1,
         wait_on_scenario/2]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-include("ra.hrl").

-type scenario() :: [{wait, non_neg_integer()} |
                     {block, non_neg_integer()} | heal].

-type config() :: #{nodes := [ra_node_id()],
                    scenario := scenario()}.

-record(state, {config :: config(),
                nodes :: [ra_node_id()],
                steps :: scenario(),
                waiting :: term()}).


%%%===================================================================
%%% API functions
%%%===================================================================

-spec start_link(config()) -> {ok, pid()} | ignore | {error, term()}.
start_link(Config) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [Config], []).

wait_on_scenario(Pid, Timeout) ->
    gen_server:call(Pid, wait_on_scenario, Timeout).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([#{scenario := Steps,
        nodes := Nodes} = Config]) ->
    State = handle_step(#state{config = Config,
                               nodes = Nodes,
                               steps = Steps}),
    {ok, State}.

handle_call(wait_on_scenario, _From, #state{steps = []} = State) ->
    {reply, ok, State};
handle_call(wait_on_scenario, From, State) ->
    {noreply, State#state{waiting = From}}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(next_step, State0) ->
    case handle_step(State0) of
        done ->
            case State0#state.waiting of
                undefined ->
                    {noreply, State0};
                From ->
                    gen_server:reply(From, ok),
                    {noreply, State0}
            end;
        State ->
            {noreply, State}
    end.


terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

handle_step(#state{steps = [{wait, Time} | Rem]} = State) ->
    erlang:send_after(Time, self(), next_step),
    State#state{steps = Rem};
handle_step(#state{steps = [{block, Time} | Rem]} = State) ->
    erlang:send_after(Time, self(), next_step),
    block(State#state.nodes),
    State#state{steps = Rem};
handle_step(#state{steps = [heal | Rem]} = State) ->
    unblock_inet_tcp_proxy(State#state.nodes),
    handle_step(State#state{steps = Rem});
handle_step(#state{steps = []} = _State) ->
    done.

unblock_inet_tcp_proxy(Nodes) ->
    ct:pal("Rejoining all nodes"),
    [ tcp_inet_proxy_helpers:allow_traffic_between(Node, OtherNode)
      || OtherNode <- Nodes,
         Node <- Nodes,
         OtherNode =/= Node ].

% block_random_partition_inet_tcp_proxy(Partition, Nodes) ->
%     block_random_partition(Partition, Nodes,
%                            fun tcp_inet_proxy_helpers:block_traffic_between/2).

get_random_node(Nodes) ->
    lists:nth(rand:uniform(length(Nodes)), Nodes).

get_random_node(Exceptions, Nodes) ->
    PossibleNodes = Nodes -- Exceptions,
    get_random_node(PossibleNodes).

block(Nodes) ->
    N = trunc(length(Nodes) / 2),
    block_random_partition(N, Nodes,
                           fun tcp_inet_proxy_helpers:block_traffic_between/2).

block_random_partition(PartitionSpec, Nodes, PartitionFun) ->
    Partition1 = case PartitionSpec of
                     PartitionSize when is_integer(PartitionSpec) ->
                         lists:foldl(fun(_, SelectedNodes) ->
                                             Node = get_random_node(SelectedNodes, Nodes),
                                             [Node | SelectedNodes]
                                     end,
                                     [],
                                     lists:seq(1, PartitionSize))
                     % PartitionNodes when is_list(PartitionNodes) ->
                     %     PartitionNodes
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

% unblock_iptables(_Nodes) ->
%     ct:pal("Rejoining all nodes"),
%     iptables_cmd("-D INPUT -j partitions_test"),
%     iptables_cmd("-F partitions_test"),
%     iptables_cmd("-X partitions_test").

% block_random_partition_iptables(Partition, Nodes) ->
%     ensure_iptables_chain(),
%     block_random_partition(Partition, Nodes, fun block_traffic_with_iptables/2).

% block_traffic_with_iptables(Node1, Node2) ->
%     DestPort1 = tcp_inet_proxy_helpers:get_dist_port(Node1),
%     DestPort2 = tcp_inet_proxy_helpers:get_dist_port(Node2),
%     SourcePort1 = get_outgoing_port(Node1, Node2, DestPort2),
%     SourcePort2 = get_outgoing_port(Node2, Node1, DestPort1),
% ct:pal(" DestPort1 ~p~n DestPort2 ~p~n SourcePort1 ~p~n SourcePort2 ~p~n", [DestPort1, DestPort2, SourcePort1, SourcePort2]),
%     case SourcePort1 of
%         undefined -> ok;
%         _ ->
%             block_ports_iptables(DestPort2, SourcePort1)
%     end,
%     case SourcePort2 of
%         undefined -> ok;
%         _ ->
%             block_ports_iptables(DestPort1, SourcePort2)
%     end,
% % timer:sleep(10000000),
%     tcp_inet_proxy_helpers:wait_for_blocked(Node1, Node2, 100).

% block_ports_iptables(DestPort, SourcePort) ->
% ct:pal("Cutting port ~p and ~p~n", [DestPort, SourcePort]),
%     iptables_cmd("-A partitions_test -p tcp -j DROP"
%                  " --destination-port " ++ integer_to_list(DestPort) ++
%                  " --source-port " ++ integer_to_list(SourcePort)),
%     iptables_cmd("-A partitions_test -p tcp -j DROP"
%                  " --destination-port " ++ integer_to_list(SourcePort) ++
%                  " --source-port " ++ integer_to_list(DestPort)).

% ensure_iptables_chain() ->
%     iptables_cmd("-N partitions_test"),
%     iptables_cmd("-A INPUT -j partitions_test").

% iptables_cmd(Cmd) ->
%     ct:pal("Running iptables " ++ Cmd),
%     Res = os:cmd("iptables " ++ Cmd),
%     ct:pal("Iptables result: " ++ Res).

% get_outgoing_port(Node1, Node2, DestPort) ->
%     rpc:call(Node1, rpc, call, [Node2, erlang, self, []]),
%     rpc:call(Node2, rpc, call, [Node1, erlang, self, []]),

%     rpc:call(Node1, jepsen_like_partitions_SUITE, get_outgoing_port, [Node2, DestPort]).
