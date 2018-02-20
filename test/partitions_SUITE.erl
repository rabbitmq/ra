-module(partitions_SUITE).
-compile(export_all).

-include_lib("common_test/include/ct.hrl").

all() -> [
          {group, tests}
         ].

groups() ->
    Tests = [
             enq_drain
            ],
    [{tests, [], Tests}].

init_per_group(_, Config) -> Config.

end_per_group(_, _Config) -> ok.

init_per_testcase(TestCase, Config0) ->
    Nodes = erlang_nodes(5),
    RaNodes = [{TestCase, N} || N <- Nodes],
    Config1 = prepare_erlang_cluster(Config0, Nodes),
    Config = [{nodes, Nodes}, {ra_nodes, RaNodes},
              {name, TestCase} | Config1],
    NodeId = setup_ra_cluster(Config),
    %% Make sure nodes are synchronised
    ct:pal("Members ~p~n", [ra:members(NodeId)]),
    Config.

end_per_testcase(_, Config) ->
    Nodes = ?config(nodes, Config),
    ct:pal("end_per_testcase: Stopping nodes ~p~n", [Nodes]),
    erlang_node_helpers:stop_erlang_nodes(Nodes),
    ct:pal("end_per_testcase: Stopped nodes ~p~n", [Nodes]),
    ok.

enq_drain(Config) ->
    Nodes = ?config(nodes, Config),
    RaNodes = ?config(ra_nodes, Config),
    Scenario = [{wait, 5000},
                {block, 5000},
                heal,
                {wait, 5000},
                {block, 16000},
                heal,
                {wait, 5000}],
    NemConf = #{nodes => Nodes,
                scenario => Scenario},
    ScenarioTime = scenario_time(Scenario, 5000),
    {ok, Nem} = nemesis:start_link(NemConf),
    EnqInterval = 1000,
    NumMessages = abs(erlang:trunc((ScenarioTime - 5000) / EnqInterval)),
    ct:pal("NumMessages ~p~n", [NumMessages]),
    EnqConf = #{nodes => RaNodes,
                num_messages => NumMessages,
                spec => {EnqInterval, custard}},
    {ok, Enq} = enqueuer:start_link(EnqConf),
    ct:pal("enqueue_checkout wait_on_scenario ~n", []),
    ok = nemesis:wait_on_scenario(Nem, ScenarioTime),
    {applied, Applied, _} = enqueuer:wait(Enq, ScenarioTime),
    ct:pal("enqueuer:wait ~p ~n", [Applied]),
    Received = drain(RaNodes),
    ct:pal("Expected ~p~nApplied ~p~nReceived~p~n",
           [NumMessages, Applied, Received]),
    % assert no messages were lost
    Remaining = Applied -- Received,
    ct:pal("Remaining~p~n", [Remaining]),
    Remaining = [],
    ok.

scenario_time([], Acc) ->
    Acc;
scenario_time([heal | Rest], Acc) ->
    scenario_time(Rest, Acc);
scenario_time([{_, T} | Rest], Acc) ->
    scenario_time(Rest, Acc + T).


drain(Nodes) ->
    F = ra_fifo_client:init(Nodes),
    drain0(F, []).

drain0(S0, Acc) ->
    case ra_fifo_client:dequeue(<<"c1">>, settled, S0) of
        {ok, {_, {_, {custard, Msg}}}, S} ->
            drain0(S, [Msg | Acc]);
        {ok, empty, _} ->
            Acc
    end.

erlang_nodes(5) ->
    [
     foo1@localhost,
     foo2@localhost,
     foo3@localhost,
     foo4@localhost,
     foo5@localhost
     ].

prepare_erlang_cluster(Config, Nodes) ->
    Config0 = tcp_inet_proxy_helpers:enable_dist_proxy_manager(Config),
    erlang_node_helpers:start_erlang_nodes(Nodes, Config0),
    tcp_inet_proxy_helpers:enable_dist_proxy(Nodes, Config0).

setup_ra_cluster(Config) ->
    Nodes = ?config(nodes, Config),
    Name = ?config(name, Config),
    DataDir = data_dir(Config),
    filelib:ensure_dir(DataDir),

    Configs = lists:map(
                fun(Node) ->
                        ct:pal("Start app on ~p~n", [Node]),
                        NodeConfig = make_node_ra_config(Name, Nodes, Node, DataDir),
                        ok = ct_rpc:call(Node, ?MODULE, node_setup, [DataDir]),
                        ok = ct_rpc:call(Node, application, load, [ra]),
                        ok = ct_rpc:call(Node, application, set_env,
                                         [ra, data_dir, filename:join([DataDir, atom_to_list(Node)])]),
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

node_setup(DataDir) ->
    LogFile = filename:join([DataDir, atom_to_list(node()), "ra.log"]),
    SaslFile = filename:join([DataDir, atom_to_list(node()), "ra_sasl.log"]),
    application:load(sasl),
    application:set_env(sasl, sasl_error_logger, {file, SaslFile}),
    application:stop(sasl),
    application:start(sasl),
    filelib:ensure_dir(LogFile),
    _ = error_logger:logfile({open, LogFile}),
    _ = error_logger:tty(false),
    ok.

data_dir(Config) ->
    Cwd = ?config(priv_dir, Config),
    filename:join(Cwd, "part").

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
