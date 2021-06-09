%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2017-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%
-module(partitions_SUITE).
-compile(nowarn_export_all).
-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(SYS, default).

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
    Servers = [{print, N} || N <- Nodes],
    [{cluster_name, print},
     {nodes, Nodes}, {servers, Servers},
     {name, print} | Config0];
init_per_testcase(TestCase, Config0) ->
    Nodes = erlang_nodes(5),
    Servers = [{TestCase, N} || N <- Nodes],
    Config1 = prepare_erlang_cluster(Config0, Nodes),
    Config = [{cluster_name, TestCase},
              {nodes, Nodes}, {servers, Servers},
              {name, TestCase} | Config1],
    Machine = {module, ra_fifo, #{}},
    ServerId = setup_ra_cluster(Config, Machine),
    %% Make sure nodes are synchronised
    ct:pal("Members ~p", [ra:members(ServerId)]),
    Config.

end_per_testcase(print, Config) ->
    Config;
end_per_testcase(_, Config) ->
    Nodes = ?config(nodes, Config),
    ct:pal("end_per_testcase: Stopping nodes ~p", [Nodes]),
    erlang_node_helpers:stop_erlang_nodes(Nodes),
    ct:pal("end_per_testcase: Stopped nodes ~p", [Nodes]),
    ok.

-type nodes5() :: foo1@localhost |
                  foo2@localhost |
                  foo3@localhost |
                  foo4@localhost |
                  foo5@localhost.

-type actions() :: {part, [nodes5()], 500..10000} |
                   {wait, 500..10000}.

-type wait_time() :: 500..10000.

prop_enq_drain(Config) ->
    ClusterName = ?config(cluster_name, Config),
    Nodes = ?config(nodes, Config),
    Servers = ?config(servers, Config),
    run_proper(
      fun () ->
              ?FORALL(S, resize(
                           10,
                           non_empty(
                             list(
                               oneof([{wait, wait_time()},
                                      heal,
                                      {part, vector(2, nodes5()),
                                       wait_time()}])))),
                      do_enq_drain_scenario(ClusterName,
                                            Nodes, Servers,
                                            [{wait, 5000}] ++ S ++
                                            [heal, {wait, 20000}]))
      end, [], 10).

print_scenario(Scenario) ->
    ct:pal("Scenario ~p", [Scenario]),
    true.

enq_drain_basic(Config) ->
    ClusterName = ?config(cluster_name, Config),
    Nodes = ?config(nodes, Config),
    Servers = ?config(servers, Config),
    Scenario = [{wait, 5000},
                {part, select_some(Nodes), 5000},
                {app_restart, select_some(Servers)},
                {wait, 5000},
                {part, select_some(Nodes), 20000},
                {wait, 5000}],
    true = do_enq_drain_scenario(ClusterName, Nodes, Servers, Scenario).

do_enq_drain_scenario(ClusterName, Nodes, Servers, Scenario) ->
    ct:pal("Running ~p", [Scenario]),
    NemConf = #{nodes => Nodes,
                scenario => Scenario},
    ScenarioTime = scenario_time(Scenario, 5000),
    {ok, Nem} = nemesis:start_link(NemConf),
    EnqInterval = 1000,
    NumMessages = abs(erlang:trunc((ScenarioTime - 5000) / EnqInterval)),
    EnqConf = #{cluster_name => ClusterName,
                servers => Servers,
                num_messages => NumMessages,
                spec => {EnqInterval, custard}},
    EnqConf2 = #{cluster_name => ClusterName,
                 servers => lists:reverse(Servers),
                 num_messages => NumMessages,
                 spec => {EnqInterval, custard}},
    {ok, Enq} = enqueuer:start_link(enq_one, EnqConf),
    {ok, Enq2} = enqueuer:start_link(enq_two, EnqConf2),
    ct:pal("enqueue_checkout wait_on_scenario ", []),
    ok = nemesis:wait_on_scenario(Nem, ScenarioTime * 2),
    {applied, Applied, _} = enqueuer:wait(Enq, ScenarioTime * 2),
    {applied, Applied2, _} = enqueuer:wait(Enq2, ScenarioTime * 2),
    ct:pal("enqueuer:wait ~p ", [Applied]),
    ct:pal("enqueuer:wait ~p ", [Applied2]),
    proc_lib:stop(Nem),
    proc_lib:stop(Enq),
    validate_machine_state(Servers),
    Received = drain(ClusterName, Servers),
    validate_machine_state(Servers),
    ct:pal("Expected ~p~nApplied ~p~nReceived ~p~nScenario: ~p",
           [NumMessages, Applied, Received, Scenario]),
    % assert no messages were lost
    Remaining = (Applied ++ Applied2) -- Received,
    ct:pal("Remaining ~p", [Remaining]),
    %% only assert we did not lose any applied entries
    Remaining =:= [].

validate_machine_state(Servers) ->
    validate_machine_state(Servers, 20).

validate_machine_state(Servers, 0) ->
    MacStates = [begin
                     {ok, S, _} = ra:local_query(N, fun ra_lib:id/1),
                     S
                 end || N <- Servers],
    exit({validate_machine_state_failed, MacStates});
validate_machine_state(Servers, Num) ->
    % give the cluster a bit of time to settle first
    timer:sleep(500),
    MacStates = [begin
                     ct:pal("querying ~w", [N]),
                     {ok, {IT, _} = S, _} = ra:local_query(N, fun ra_lib:id/1),
                     ct:pal("validating ~w at ~w", [N, IT]),
                     S
                 end || N <- Servers],
    H = hd(MacStates),
    case lists:all(fun (S) -> H =:= S end, MacStates) of
        true ->
            ct:pal("machine state are valid", []),
            ok;
        false ->
            validate_machine_state(Servers, Num-1)
    end.

select_some(Servers) ->
    N = trunc(length(Servers) / 2),
    element(1,
            lists:foldl(fun (_, {Selected, Rem0}) ->
                                {S, Rem} = random_element(Rem0),
                                {[S | Selected], Rem}
                        end, {[], Servers}, lists:seq(1, N))).

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


drain(ClusterName, Nodes) ->
    ct:pal("draining ~w", [ClusterName]),
    F = ra_fifo_client:init(ClusterName, Nodes),
    drain0(F, []).

drain0(S0, Acc) ->
    case ra_fifo_client:dequeue(<<"c1">>, settled, S0) of
        {ok, {_, {_, {custard, Msg}}}, S} ->
            drain0(S, [Msg | Acc]);
        {ok, empty, _} ->
            Acc;
        Err ->
            %% oh dear
            ct:pal("drain failed after draining ~w~n with ~w", [Acc, Err]),
            exit(Err)
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
    Config0 = tcp_inet_proxy_helpers:configure_dist_proxy(Config),
    erlang_node_helpers:start_erlang_nodes(Nodes, Config0),
    Config0.

setup_ra_cluster(Config, Machine) ->
    Nodes = ?config(nodes, Config),
    Name = ?config(name, Config),
    DataDir = data_dir(Config),
    ok = ra_lib:make_dir(DataDir),

    Configs = lists:map(
                fun(Node) ->
                        ct:pal("Start app on ~p", [Node]),
                        C = make_server_config(Name, Nodes, Node, Machine),
                        ok = ct_rpc:call(Node, ?MODULE, node_setup, [DataDir]),
                        _ = ct_rpc:call(Node, application, stop, [ra]),
                        _ = ct_rpc:call(Node, application, load, [ra]),
                        ok = ct_rpc:call(Node, application, set_env,
                                         [ra, data_dir, [DataDir]]),
                        ok = ct_rpc:call(Node, application, set_env,
                                         [ra, wal_max_bytes, [64000000]]),
                        {ok, _} = ct_rpc:call(Node, ra, start, [[{data_dir, DataDir}]]),
                        ok = ct_rpc:call(Node, logger, set_primary_config,
                                         [level, all]),
                        C
                end,
                Nodes),
    lists:map(fun(#{id := {_, Node}} = ServerConfig) ->
                      ct:pal("Start ra server on ~p", [Node]),
                      ok = ct_rpc:call(Node, ra, start_server, [?SYS, ServerConfig]),
                      ServerConfig
              end,
              Configs),
    ServerId = {Name, hd(Nodes)},
    ok = ra:trigger_election(ServerId),
    ServerId.

node_setup(DataDir) ->
    ok = ra_lib:make_dir(DataDir),
    LogFile = filename:join([DataDir, atom_to_list(node()), "ra.log"]),
    SaslFile = filename:join([DataDir, atom_to_list(node()), "ra_sasl.log"]),
    logger:set_primary_config(level, debug),
    Config = #{config => #{type => {file, LogFile}}, level => debug},
    logger:add_handler(ra_handler, logger_std_h, Config),
    application:load(sasl),
    application:set_env(sasl, sasl_error_logger, {file, SaslFile}),
    application:stop(sasl),
    application:start(sasl),
    _ = error_logger:tty(false),
    ok.

data_dir(Config) ->
    Cwd = ?config(priv_dir, Config),
    filename:join(Cwd, "part").

make_server_config(Name, Nodes, Node, Machine) ->
    #{cluster_name => Name,
      id => {Name, Node},
      uid => atom_to_binary(Name, utf8),
      initial_members => [{Name, N} || N <- Nodes],
      log_init_args =>
      #{uid => atom_to_binary(Name, utf8)},
      machine =>  Machine,
      await_condition_timeout => 5,
      tick_timeout => 500
     }.

run_proper(Fun, Args, NumTests) ->
    ?assertEqual(
       true,
       proper:counterexample(erlang:apply(Fun, Args),
			     [{numtests, NumTests},
                  noshrink,
			      {on_output, fun(".", _) -> ok; % don't print the '.'s on new lines
					     (F, A) -> ct:pal(?LOW_IMPORTANCE, F, A) end}])).
