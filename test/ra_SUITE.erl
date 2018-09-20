-module(ra_SUITE).

-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include("ra.hrl").

-define(SEND_AND_AWAIT_CONSENSUS_TIMEOUT, 6000).

all() ->
    [
     {group, tests}
    ].

all_tests() ->
    [
     single_node,
     stop_node_idemp,
     minority,
     start_nodes,
     node_recovery,
     send_and_await_consensus,
     send_and_notify,
     send_and_notify_reject,
     cast,
     local_query,
     members,
     consistent_query,
     node_catches_up,
     add_member,
     queue_example,
     ramp_up_and_ramp_down,
     start_and_join_then_leave_and_terminate,
     leader_steps_down_after_replicating_new_cluster,
     stop_leader_and_wait_for_elections,
     follower_catchup,
     post_partition_liveness
    ].

groups() ->
    [
     {tests, [], all_tests()}
    ].

suite() -> [{timetrap, {seconds, 30}}].

init_per_suite(Config) ->
    Config.

end_per_suite(Config) ->
    application:stop(ra),
    Config.

restart_ra(DataDir) ->
    application:stop(ra),
    _ = application:load(ra),
    ok = application:set_env(ra, data_dir, DataDir),
    ok = application:set_env(ra, segment_max_entries, 128),
    application:ensure_all_started(ra),
    application:ensure_all_started(sasl),
    ok.

init_per_group(_G, Config) ->
    PrivDir = ?config(priv_dir, Config),
    DataDir = filename:join([PrivDir, "data"]),
    ok = restart_ra(DataDir),
    Config.

end_per_group(_, Config) ->
    Config.

init_per_testcase(TestCase, Config) ->
    [{test_name, ra_lib:to_list(TestCase)} | Config].

end_per_testcase(_TestCase, Config) ->
    ra_nodes_sup:remove_all(),
    Config.

single_node(Config) ->
    Name = ?config(test_name, Config),
    N1 = nn(Config, 1),
    ok = ra:start_node(Name, N1, add_machine(), []),
    ok = ra:trigger_election(N1),
    % index is 2 as leaders commit a noop entry on becoming leaders
    {ok, {2,1}, _} = ra:send_and_await_consensus({N1, node()}, 5, 2000),
    terminate_cluster([N1]).

stop_node_idemp(Config) ->
    Name = ?config(test_name, Config),
    N1 = nn(Config, 1),
    ok = ra:start_node(Name, N1, add_machine(), []),
    ok = ra:trigger_election(N1),
    timer:sleep(100),
    ok = ra:stop_node({N1, node()}),
    % should not raise exception
    ok = ra:stop_node({N1, node()}),
    {error, nodedown} = ra:stop_node({N1, randomnode@bananas}),
    ok.

leader_steps_down_after_replicating_new_cluster(Config) ->
    N1 = nn(Config, 1),
    N2 = nn(Config, 2),
    N3 = nn(Config, 3),
    ok = new_node(N1, Config),
    ok = ra:trigger_election(N1),
    _ = issue_op(N1, 5),
    validate(N1, 5),
    ok = start_and_join(N1, N2),
    _ = issue_op(N1, 5),
    validate(N1, 10),
    ok = start_and_join(N1, N3),
    _ = issue_op(N1, 5),
    validate(N1, 15),
    % allow N3 some time to catch up
    timer:sleep(100),
    % remove leader node
    % the leader should here replicate the new cluster config
    % then step down + shut itself down
    ok = remove_member(N1),
    timer:sleep(500),
    {error, noproc} = ra:send_and_await_consensus(N1, 5, 2000),
    _ = issue_op(N2, 5),
    validate(N2, 20),
    terminate_cluster([N2, N3]).


start_and_join_then_leave_and_terminate(Config) ->
    N1 = nn(Config, 1),
    N2 = nn(Config, 2),
    % safe node removal
    ok = new_node(N1, Config),
    ok = ra:trigger_election(N1),
    _ = issue_op(N1, 5),
    validate(N1, 5),
    ok = start_and_join(N1, N2),
    _ = issue_op(N2, 5),
    validate(N2, 10),
    ok = ra:leave_and_terminate({N1, node()}, {N2, node()}),
    validate(N1, 10),
    terminate_cluster([N1]),
    ok.

ramp_up_and_ramp_down(Config) ->
    N1 = nn(Config, 1),
    N2 = nn(Config, 2),
    N3 = nn(Config, 3),
    ok = new_node(N1, Config),
    ok = ra:trigger_election(N1),
    _ = issue_op(N1, 5),
    validate(N1, 5),

    ok = start_and_join(N1, N2),
    _ = issue_op(N2, 5),
    validate(N2, 10),

    ok = start_and_join(N1, N3),
    _ = issue_op(N3, 5),
    validate(N3, 15),

    ok = ra:leave_and_terminate({N3, node()}),
    _ = issue_op(N2, 5),
    validate(N2, 20),

    % this is dangerous territory
    % we need a quorum from the node that is to be removed for the cluster
    % change. if we stop the node before removing it from the cluster
    % configuration the cluster becomes non-functional
    ok = remove_member(N2),
    % a longish sleep here simulates a node that has been removed but not
    % shut down and thus may start issuing request_vote_rpcs
    timer:sleep(1000),
    ok = stop_node(N2),
    _ = issue_op(N1, 5),
    validate(N1, 25),
    terminate_cluster([N1]).

minority(Config) ->
    Name = ?config(test_name, Config),
    N1 = nn(Config, 1),
    N2 = nn(Config, 2),
    N3 = nn(Config, 3),
    ok = ra:start_node(Name, N1, add_machine(), [{N2, node()}, {N3, node()}]),
    ok = ra:trigger_election(N1),
    {timeout, _} = ra:send_and_await_consensus({N1, node()}, 5, 250),
    terminate_cluster([N1]).

start_nodes(Config) ->
    Name = ?config(test_name, Config),
    % suite unique node names
    N1 = nn(Config, 1),
    N2 = nn(Config, 2),
    N3 = nn(Config, 3),
    % start the first node and wait a bit
    ok = ra:start_node(Name, {N1, node()}, add_machine(),
                       [{N2, node()}, {N3, node()}]),
    % start second node
    ok = ra:start_node(Name, {N2, node()}, add_machine(),
                       [{N1, node()}, {N3, node()}]),
    % trigger election
    ok = ra_node_proc:trigger_election(N1, ?DEFAULT_TIMEOUT),
    % a consensus command tells us there is a functioning cluster
    {ok, _, _Leader} = ra:send_and_await_consensus({N1, node()}, 5,
                                                   ?SEND_AND_AWAIT_CONSENSUS_TIMEOUT),
    % start the 3rd node and issue another command
    ok = ra:start_node(Name, {N3, node()}, add_machine(), [{N1, node()}, {N2, node()}]),
    timer:sleep(100),
    % issue command - this is likely to preceed teh rpc timeout so the node
    % then should stash the command until a leader is known
    {ok, {_, Term}, Leader} = ra:send_and_await_consensus({N3, node()}, 5,
                                                          ?SEND_AND_AWAIT_CONSENSUS_TIMEOUT),
    % shut down non leader
    Target = case Leader of
                 {N1, _} -> {N2, node()};
                 _ -> {N1, node()}
             end,
    ct:pal("shutting down ~p", [Target]),
    gen_statem:stop(Target, normal, 2000),
    % issue command to confirm n3 joined the cluster successfully
    {ok, {4, Term}, _} = ra:send_and_await_consensus({N3, node()}, 5,
                                                     ?SEND_AND_AWAIT_CONSENSUS_TIMEOUT),
    terminate_cluster([N1, N2, N3] -- [element(1, Target)]).



node_recovery(Config) ->
    N1 = nn(Config, 1),
    N2 = nn(Config, 2),
    N3 = nn(Config, 3),

    Name = ?config(test_name, Config),
    % start the first node and wait a bit
    ok = ra:start_node(Name, {N1, node()}, add_machine(),
                       [{N2, node()}, {N3, node()}]),
    ok = ra_node_proc:trigger_election(N1, ?DEFAULT_TIMEOUT),
    % start second node
    ok = ra:start_node(Name, {N2, node()}, add_machine(),
                       [{N1, node()}, {N3, node()}]),
    % a consensus command tells us there is a functioning 2 node cluster
    {ok, {_, _}, Leader} = ra:send_and_await_consensus({N2, node()}, 5,
                                                       ?SEND_AND_AWAIT_CONSENSUS_TIMEOUT),
    % stop leader to trigger restart
    proc_lib:stop(Leader, bad_thing, 5000),
    timer:sleep(1000),
    N = case Leader of
            {N1, _} -> N2;
            _ -> N1
        end,
    % issue command
    {ok, {_, _}, _Leader} = ra:send_and_await_consensus({N, node()}, 5,
                                                        ?SEND_AND_AWAIT_CONSENSUS_TIMEOUT),
    terminate_cluster([N1, N2]).

send_and_await_consensus(Config) ->
    [A, _B, _C] = Cluster =
        start_local_cluster(3, ?config(test_name, Config),
                            {simple, fun erlang:'+'/2, 9}),
    {ok, {_, _}, _Leader} = ra:send_and_await_consensus(A, 5,
                                                        ?SEND_AND_AWAIT_CONSENSUS_TIMEOUT),
    terminate_cluster(Cluster).

send_and_notify(Config) ->
    [A, _B, _C] = Cluster =
        start_local_cluster(3, ?config(test_name, Config),
                            {simple, fun erlang:'+'/2, 9}),
    {ok, _, Leader} = ra:members(A),
    Correlation = my_corr,
    ok = ra:send_and_notify(Leader, 5, Correlation),
    receive
        {ra_event, _, {applied, [Correlation]}} -> ok
    after 2000 ->
              exit(consensus_timeout)
    end,
    terminate_cluster(Cluster).

send_and_notify_reject(Config) ->
    [A, _, _C] = Cluster =
        start_local_cluster(3, ?config(test_name, Config),
                            {simple, fun erlang:'+'/2, 9}),
    Correlation = my_corr,
    {ok, _, Leader} = ra:members(A),
    Followers = Cluster -- [Leader],
    Correlation = my_corr,
    Target = hd(Followers),
    ok = ra:send_and_notify(Target, 5, Correlation),
    receive
        {ra_event, _, {rejected, {not_leader, Leader, Correlation}}} -> ok
    after 2000 ->
              exit(consensus_timeout)
    end,
    terminate_cluster(Cluster).

cast(Config) ->
    [A, B, C] = Cluster =
        start_local_cluster(3, ?config(test_name, Config),
                            {simple, fun erlang:'+'/2, 0}),
    % cast to each node - command should be forwarded
    ok = ra:cast(A, 5),
    ok = ra:cast(B, 5),
    ok = ra:cast(C, 5),
    timer:sleep(50),
    {ok, {_IdxTrm, 15}, _} = ra:consistent_query(A, fun (X) -> X end),
    terminate_cluster(Cluster).

local_query(Config) ->
    [A, B, _C] = Cluster = start_local_cluster(3, ?config(test_name, Config),
                                               {simple, fun erlang:'+'/2, 9}),
    {ok, {{_, _}, 9}, _} = ra:local_query(B, fun(S) -> S end),
    {ok, {_, Term}, Leader} = ra:send_and_await_consensus(A, 5,
                                                          ?SEND_AND_AWAIT_CONSENSUS_TIMEOUT),
    {ok, {{_, Term}, 14}, _} = ra:local_query(Leader, fun(S) -> S end),
    terminate_cluster(Cluster).

members(Config) ->
    Cluster = start_local_cluster(3, ?config(test_name, Config),
                                  {simple, fun erlang:'+'/2, 9}),
    {ok, _, Leader} = ra:send_and_await_consensus(hd(Cluster), 5,
                                                  ?SEND_AND_AWAIT_CONSENSUS_TIMEOUT),
    {ok, Cluster, Leader} = ra:members(Leader),
    terminate_cluster(Cluster).

consistent_query(Config) ->
    [A, _B, _C]  = Cluster = start_local_cluster(3, ?config(test_name, Config),
                                                 add_machine()),
    {ok, {_, Term}, Leader} = ra:send_and_await_consensus(A, 9,
                                                          ?SEND_AND_AWAIT_CONSENSUS_TIMEOUT),
    {ok, {_, Term}, _Leader} = ra:send(Leader, 5),
    {ok, {{_, Term}, 14}, Leader} = ra:consistent_query(A, fun(S) -> S end),
    terminate_cluster(Cluster).

add_member(Config) ->
    Name = ?config(test_name, Config),
    [A, _B] = Cluster = start_local_cluster(2, Name, add_machine()),
    {ok, {_, Term}, Leader} = ra:send_and_await_consensus(A, 9),
    C = ra_node:name(Name, "3"),
    ok = ra:start_node(Name, C, add_machine(), Cluster),
    {ok, {_, Term}, _Leader} = ra:add_member(Leader, {C, node()}),
    {ok, {{_, Term}, 9}, Leader} = ra:consistent_query(C, fun(S) -> S end),
    terminate_cluster([C | Cluster]).

node_catches_up(Config) ->
    N1 = nn(Config, 1),
    N2 = nn(Config, 2),
    N3 = nn(Config, 3),
    Name = ?config(test_name, Config),
    InitialNodes = [{N1, node()}, {N2, node()}],
    %%TODO look into cluster changes WITH INVALID NAMES!!!

    Mac = {module, ra_queue, #{}},
    % start two nodes
    ok = ra:start_node(Name, {N1, node()}, Mac, InitialNodes),
    ok = ra:start_node(Name, {N2, node()}, Mac, InitialNodes),
    ok = ra:trigger_election({N1, node()}),
    DecSink = spawn(fun () -> receive marker_pattern -> ok end end),
    {ok, {_, Term}, Leader} = ra:send(N1, {enq, banana}),
    {ok, {_, Term}, Leader} = ra:send(Leader, {deq, DecSink}),
    {ok, {_, Term}, Leader} = ra:send_and_await_consensus(Leader, {enq, apple},
                                                          ?SEND_AND_AWAIT_CONSENSUS_TIMEOUT),

    ok = ra:start_node(Name, {N3, node()}, Mac, InitialNodes),
    {ok, {_, Term}, _Leader} = ra:add_member(Leader, {N3, node()}),
    timer:sleep(1000),
    % at this point the node should be caught up
    {ok, {_, Res}, _} = ra:local_query(N1, fun ra_lib:id/1),
    {ok, {_, Res}, _} = ra:local_query(N2, fun ra_lib:id/1),
    {ok, {_, Res}, _} = ra:local_query(N3, fun ra_lib:id/1),
    % check that the message isn't delivered multiple times
    terminate_cluster([N3 | InitialNodes]).

stop_leader_and_wait_for_elections(Config) ->
    Name = ?config(test_name, Config),
    % start the first node and wait a bit
    ok = ra:start_node(Name, {n1, node()}, add_machine(),
                       [{n2, node()}, {n3, node()}]),
    ok = ra:trigger_election({n1, node()}),
    % start second node
    ok = ra:start_node(Name, {n2, node()}, add_machine(),
                       [{n1, node()}, {n3, node()}]),
    % a consensus command tells us there is a functioning cluster
    {ok, {2, Term}, _Leader} = ra:send_and_await_consensus({n1, node()}, 5),
    % start the 3rd node and issue another command
    ok = ra:start_node(Name, {n3, node()}, add_machine(),
                       [{n1, node()}, {n2, node()}]),
    % issue command
    {ok, {_, Term}, Leader} = ra:send_and_await_consensus({n3, node()}, 5),
    % shut down the leader
    gen_statem:stop(Leader, normal, 2000),
    timer:sleep(1000),
    % issue command to confirm a new leader is elected
    {ok, {5, NewTerm}, {NewLeader, _}} = ra:send_and_await_consensus({n3, node()}, 5),
    true = (NewTerm > Term),
    true = (NewLeader =/= n1),
    terminate_cluster([n1, n2, n3] -- [element(1, Leader)]).

queue_example(Config) ->
    Self = self(),
    [A, _B, _C] = Cluster = start_local_cluster(3, ?config(test_name, Config),
                                                {module, ra_queue, #{}}),

    {ok, {_, Term}, Leader} = ra:send(A, {enq, test_msg}),
    {ok, {_, Term}, Leader} = ra:send(Leader, {deq, Self}),
    waitfor(test_msg, apply_timeout),
    % check that the message isn't delivered multiple times
    receive
        {ra_queue, _, test_msg} ->
            exit(double_delivery)
    after 500 -> ok
    end,
    terminate_cluster(Cluster).

contains(Match, Entries) ->
    lists:any(fun({_, _, {_, _, Value, _}}) when Value == Match ->
                      true;
                 (_) ->
                      false
              end, Entries).

follower_catchup(Config) ->
    meck:new(ra_node_proc, [passthrough]),
    meck:expect(ra_node_proc, send_rpc,
                fun(P, #append_entries_rpc{entries = Entries} = T) ->
                        case contains(500, Entries) of
                            true ->
                                ct:pal("dropped 500"),
                                ok;
                            false ->
                                meck:passthrough([P, T])
                        end;
                   (P, T) ->
                        meck:passthrough([P, T])
                end),
    Name = ?config(test_name, Config),
    % suite unique node names
    N1 = {nn(Config, 1), node()},
    N2 = {nn(Config, 2), node()},
    % start the first node and wait a bit
    Conf = fun (NodeId, NodeIds, UId) ->
               #{cluster_id => Name,
                 id => NodeId,
                 uid => UId,
                 initial_nodes => NodeIds,
                 log_init_args => #{uid => UId},
                 machine => add_machine(),
                 await_condition_timeout => 1000}
           end,
    ok = ra:start_node(Conf(N1, [N2], <<"N1">>)),
    % start second node
    ok = ra:start_node(Conf(N2, [N1], <<"N2">>)),
    ok = ra:trigger_election(N1),
    _ = ra:members(N1),
    % a consensus command tells us there is a functioning cluster
    {ok, _, _Leader} = ra:send_and_await_consensus(N1, 5,
                                                   ?SEND_AND_AWAIT_CONSENSUS_TIMEOUT),
    % issue command - this will be lost
    ok = ra:send_and_notify(N1, 500, corr_500),
    % issue next command
    {ok, _IdxTerm, Leader0} = ra:send(N1, 501),
    [Follower] = [N1, N2] -- [Leader0],
    receive
        {ra_event, _, {applied, [corr_500]}} ->
            exit(unexpected_consensus)
    after 1000 ->
            case get_gen_statem_status(Follower) of
                await_condition ->
                    ok;
                FollowerStatus0 ->
                    exit({unexpected_follower_status, FollowerStatus0})
            end
    end,
    meck:unload(),
    % we wait for the condition to time out - then the follower will re-issue
    % the aer with the original condition which should trigger a re-wind of of
    % the next_index and a subsequent resend of missing entries
    receive
        {ra_event, _, {applied, [corr_500]}} ->
            case get_gen_statem_status(Follower) of
                follower ->
                    ok;
                FollowerStatus1 ->
                    exit({unexpected_follower_status, FollowerStatus1})
            end,
            ok
    after 6000 ->
              flush(),
              exit(consensus_not_achieved)
    end,
    terminate_cluster([N1, N2]).

flush() ->
    receive
        Any ->
            ct:pal("flush ~p", [Any]),
            flush()
    after 0 ->
              ok
    end.

post_partition_liveness(Config) ->
    meck:new(ra_node_proc, [passthrough]),
    Name = ?config(test_name, Config),
    % suite unique node names
    N1 = nn(Config, 1),
    N2 = nn(Config, 2),
    % start the first node and wait a bit
    ok = ra:start_node(Name, N1, add_machine(), [N2]),
    % start second node
    ok = ra:start_node(Name, N2, add_machine(), [N1]),
    ok = ra:trigger_election(N2),
    % a consensus command tells us there is a functioning cluster
    {ok, _, Leader} = ra:send_and_await_consensus({N1, node()}, 5,
                                                   ?SEND_AND_AWAIT_CONSENSUS_TIMEOUT),

    % simulate partition
    meck:expect(ra_node_proc, send_rpc, fun(_, _) -> ok end),
    % send an entry that will not be replicated
    ok = ra:send_and_notify(Leader, 500, corr_500),
    % assert we don't achieve consensus
    receive
        {ra_event, _, {applied, [corr_500]}} ->
            exit(unexpected_consensus)
    after 1000 ->
              ok
    end,
    % heal partition
    meck:unload(),
    % assert consensus completes after some time
    receive
        {ra_event, _, {applied, [corr_500]}} ->
            ok
    after 6500 ->
            exit(consensus_timeout)
    end,
    ok.

get_gen_statem_status(Ref) ->
    {_, _, _, Items} = sys:get_status(Ref),
    proplists:get_value(raft_state, lists:last(Items)).

% implements a simple queue machine
queue_apply({enqueue, Msg}, State =#{queue := Q0, pending_dequeues := []}) ->
    Q = queue:in(Msg, Q0),
    State#{queue => Q};
queue_apply({enqueue, Msg}, State = #{queue := Q0,
                                      pending_dequeues := [Next | Rest]}) ->
    Q1 = queue:in(Msg, Q0),
    {{value, Item}, Q} = queue:out(Q1),
    {State#{queue => Q, pending_dequeues => Rest}, [{send_msg, Next, Item}]};
queue_apply({dequeue, For}, State = #{queue := Q0, pending_dequeues := []}) ->
    case queue:out(Q0) of
        {empty, Q} ->
            State#{queue => Q, pending_dequeues => [For]};
        {{value, Item}, Q} ->
            {State#{queue => Q}, [{send_msg, For, Item}]}
    end;
queue_apply({dequeue, For},
            State = #{queue := Q0,
                      pending_dequeues := [Next | Rest] = Pending}) ->
    case queue:out(Q0) of
        {empty, Q} ->
            State#{queue => Q, pending_dequeues => Pending ++ [For]};
        {{value, Item}, Q} ->
            {State#{queue => Q, pending_dequeues => Rest ++ [For]},
             [{send_msg, Next, Item}]}
    end.


waitfor(Msg, ExitWith) ->
    receive
        {ra_event, _, {machine, Msg}} -> ok
    after 3000 ->
              exit(ExitWith)
    end.

terminate_cluster(Nodes) ->
    [ra:stop_node(P) || P <- Nodes].

new_node(Name, Config) ->
    ClusterId = ?config(test_name, Config),
    ok = ra:start_node(ClusterId, {Name, node()}, add_machine(), []),
    ok.

stop_node(Name) ->
    ok = ra:stop_node({Name, node()}),
    ok.

add_member(Ref, New) ->
    {ok, _IdxTerm, _Leader} = ra:add_member({Ref, node()}, {New, node()}),
    ok.

start_and_join(Ref, New) ->
    ServerRef = {Ref, node()},
    {ok, _, _} = ra:add_member(ServerRef, {New, node()}),
    ok = ra:start_node(New, {New, node()}, add_machine(), [ServerRef]),
    ok.

start_local_cluster(Num, Name, Machine) ->
    Nodes = [{ra_node:name(Name, integer_to_list(N)), node()}
             || N <- lists:seq(1, Num)],

    {ok, _, Failed} = ra:start_cluster(Name, Machine, Nodes),
    ?assert(length(Failed) == 0),
    Nodes.

remove_member(Name) ->
    {ok, _IdxTerm, _Leader} = ra:remove_member({Name, node()}, {Name, node()}),
    ok.

issue_op(Name, Op) ->
    {ok, IdxTrm, Res} = ra:send_and_await_consensus(Name, Op, 2000),
    {IdxTrm, Res}.

validate(Name, Expected) ->
    {ok, {_, Expected}, _} = ra:consistent_query({Name, node()},
                                                 fun(X) -> X end).

dump(T) ->
    ct:pal("DUMP: ~p~n", [T]),
    T.

nn(Config, N) when is_integer(N) ->
    ra_node:name(?config(test_name, Config), erlang:integer_to_list(N)).

add_machine() ->
    {simple, fun erlang:'+'/2, 0}.
