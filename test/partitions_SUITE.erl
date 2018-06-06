-module(partitions_SUITE).
-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").

all() -> [
          {group, tests}
         ].

groups() ->
    Tests = [
             enq_drain_basic
             % prop_enq_drain
            ],
    [{tests, [], Tests}].

init_per_group(_, Config) -> Config.

end_per_group(_, _Config) -> ok.

init_per_testcase(print, Config0) ->
    Nodes = erlang_nodes(5),
    RaNodes = [{print, N} || N <- Nodes],
    [{cluster_id, print},
     {nodes, Nodes}, {ra_nodes, RaNodes},
     {name, print} | Config0];
init_per_testcase(TestCase, Config0) ->
    Nodes = erlang_nodes(5),
    RaNodes = [{TestCase, N} || N <- Nodes],
    Config1 = prepare_erlang_cluster(Config0, Nodes),
    Config = [{cluster_id, TestCase},
              {nodes, Nodes}, {ra_nodes, RaNodes},
              {name, TestCase} | Config1],
    Machine = {module, ra_fifo, #{}},
    NodeId = setup_ra_cluster(Config, Machine),
    %% Make sure nodes are synchronised
    ct:pal("Members ~p~n", [ra:members(NodeId)]),
    Config.

end_per_testcase(print, Config) ->
    Config;
end_per_testcase(_, Config) ->
    Nodes = ?config(nodes, Config),
    ct:pal("end_per_testcase: Stopping nodes ~p~n", [Nodes]),
    erlang_node_helpers:stop_erlang_nodes(Nodes),
    ct:pal("end_per_testcase: Stopped nodes ~p~n", [Nodes]),
    ok.

-type nodes5() :: foo1@localhost |
                  foo2@localhost |
                  foo3@localhost |
                  foo4@localhost |
                  foo5@localhost.

-type actions() :: {part, [nodes5()], 1000..20000} |
                   {wait, 1000..20000}.

-type wait_time() :: 1000..20000.

prop_enq_drain(Config) ->
    ClusterId = ?config(cluster_id, Config),
    Nodes = ?config(nodes, Config),
    RaNodes = ?config(ra_nodes, Config),
    run_proper(
      fun () ->
              ?FORALL(S, resize(
                           10,
                           non_empty(
                             list(
                               oneof([{wait, wait_time()},
                                      heal,
                                      {part, vector(2, nodes5()),
                                       wait_time()}])
                              )
                            )),
                      do_enq_drain_scenario(ClusterId,
                                            Nodes, RaNodes,
                                            [{wait, 5000}] ++ S ++
                                            [heal, {wait, 5000}]))
      end, [], 10).

print_scenario(Scenario) ->
    ct:pal("Scenario ~p~n", [Scenario]),
    true.

enq_drain_basic(Config) ->
    ClusterId = ?config(cluster_id, Config),
    Nodes = ?config(nodes, Config),
    RaNodes = ?config(ra_nodes, Config),
    Scenario = [{wait, 5000},
                {part, select_nodes(Nodes), 5000},
                {app_restart, select_nodes(RaNodes)},
                {wait, 5000},
                {part, select_nodes(Nodes), 20000},
                {wait, 5000}],
    true = do_enq_drain_scenario(ClusterId, Nodes, RaNodes, Scenario).

do_enq_drain_scenario(ClusterId, Nodes, RaNodes, Scenario) ->
    ct:pal("Running ~p~n", [Scenario]),
    NemConf = #{nodes => Nodes,
                scenario => Scenario},
    ScenarioTime = scenario_time(Scenario, 5000),
    {ok, Nem} = nemesis:start_link(NemConf),
    EnqInterval = 1000,
    NumMessages = abs(erlang:trunc((ScenarioTime - 5000) / EnqInterval)),
    EnqConf = #{cluster_id => ClusterId,
                nodes => RaNodes,
                num_messages => NumMessages,
                spec => {EnqInterval, custard}},
    EnqConf2 = #{cluster_id => ClusterId,
                 nodes => lists:reverse(RaNodes),
                 num_messages => NumMessages,
                 spec => {EnqInterval, custard}},
    {ok, Enq} = enqueuer:start_link(enq_one, EnqConf),
    {ok, Enq2} = enqueuer:start_link(enq_two, EnqConf2),
    ct:pal("enqueue_checkout wait_on_scenario ~n", []),
    ok = nemesis:wait_on_scenario(Nem, ScenarioTime * 2),
    {applied, Applied, _} = enqueuer:wait(Enq, ScenarioTime * 2),
    {applied, Applied2, _} = enqueuer:wait(Enq2, ScenarioTime * 2),
    ct:pal("enqueuer:wait ~p ~n", [Applied]),
    ct:pal("enqueuer:wait ~p ~n", [Applied2]),
    proc_lib:stop(Nem),
    proc_lib:stop(Enq),
    validate_machine_state(RaNodes),
    Received = drain(ClusterId, RaNodes),
    validate_machine_state(RaNodes),
    ct:pal("Expected ~p~nApplied ~p~nReceived ~p~nScenario: ~p~n",
           [NumMessages, Applied, Received, Scenario]),
    % assert no messages were lost
    Remaining = (Applied ++ Applied2) -- Received,
    ct:pal("Remaining ~p~n", [Remaining]),
    MaxReceived = lists:max(Received),
    Remaining =:= [] andalso NumMessages =:= MaxReceived.

validate_machine_state(Nodes) ->
    % give the cluster a bit of time to settle first
    timer:sleep(1000),
    MacStates = [begin
                     {ok, S, _} = ra:dirty_query(N, fun ra_lib:id/1),
                     S
                 end || N <- Nodes],
    H = hd(MacStates),
    lists:foreach(fun (S) -> ?assertEqual(H, S) end,
                  MacStates),
    ok.

select_nodes(Nodes) ->
    N = trunc(length(Nodes) / 2),
    element(1,
            lists:foldl(fun (_, {Selected, Rem0}) ->
                                {S, Rem} = random_element(Rem0),
                                {[S | Selected], Rem}
                        end, {[], Nodes}, lists:seq(1, N))).

random_element(Nodes) ->
    Selected = lists:nth(rand:uniform(length(Nodes)), Nodes),
    {Selected, lists:delete(Selected, Nodes)}.

scenario_time([], Acc) ->
    Acc;
scenario_time([heal | Rest], Acc) ->
    scenario_time(Rest, Acc);
scenario_time([{app_restart, _} | Rest], Acc) ->
    scenario_time(Rest, Acc + 100);
scenario_time([{_, T} | Rest], Acc) ->
    scenario_time(Rest, Acc + T);
scenario_time([{_, _, T} | Rest], Acc) ->
    scenario_time(Rest, Acc + T).


drain(ClusterId, Nodes) ->
    F = ra_fifo_client:init(ClusterId, Nodes),
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

setup_ra_cluster(Config, Machine) ->
    Nodes = ?config(nodes, Config),
    Name = ?config(name, Config),
    DataDir = data_dir(Config),
    filelib:ensure_dir(DataDir),

    Configs = lists:map(
                fun(Node) ->
                        ct:pal("Start app on ~p~n", [Node]),
                        NodeConfig = make_node_ra_config(Name, Nodes, Node,
                                                         Machine, DataDir),
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

make_node_ra_config(Name, Nodes, Node, Machine, DataDir) ->
    #{cluster_id => Name,
      id => {Name, Node},
      uid => atom_to_binary(Name, utf8),
      initial_nodes => [{Name, N} || N <- Nodes],
      log_module => ra_log_file,
      log_init_args =>
      #{data_dir => filename:join([DataDir, atom_to_list(Node)]),
        uid => atom_to_binary(Name, utf8)},
      machine =>  Machine
     }.

run_proper(Fun, Args, NumTests) ->
    ?assertEqual(
       true,
       proper:counterexample(erlang:apply(Fun, Args),
			     [{numtests, NumTests},
                  noshrink,
			      {on_output, fun(".", _) -> ok; % don't print the '.'s on new lines
					     (F, A) -> ct:pal(?LOW_IMPORTANCE, F, A) end}])).
