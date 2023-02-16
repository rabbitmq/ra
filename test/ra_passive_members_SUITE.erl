-module(ra_passive_members_SUITE).
-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include("src/ra.hrl").

-define(PROCESS_COMMAND_TIMEOUT, 6000).
-define(SYS, default).

all() ->
    [
        {group, tests}
    ].

all_tests() ->
    [
        we_did_not_break_simple_things,
        give_3_node_cluster_adding_a_passive_works,
        given_2_node_cluster_with_1_passive,
        given_2_node_cluster_with_1_passive_on_failover_leader_returns_both_stop,
        given_5_node_cluster_with_2_passive_on_failover
    ].

groups() ->
    [
        {tests, [{repeat, 1}], all_tests()}
    ].

init_per_testcase(_TestCase, Config) ->
    rpc_all([node()], ra_groups, set_active_nodes, [undefined]),
    Config.

we_did_not_break_simple_things(Config) ->
    Nodes = start_erlang_nodes(3, Config),

    Cluster = start_ra_cluster(?FUNCTION_NAME, Nodes),
    {ok, _Members, Leader} = ra:members(hd(Cluster)),
    assert_cluster_still_works(Leader),

    terminate_cluster(Cluster),
    terminate_peers(Nodes).

give_3_node_cluster_adding_a_passive_works(Config) -> 
    {Nodes, [PassiveNode]} = lists:split(3, start_erlang_nodes(4, Config)),
    
    Cluster = start_ra_cluster(?FUNCTION_NAME, Nodes),
    {ok, _Members, Leader} = ra:members(hd(Cluster)),
    
    assert_cluster_still_works(Leader),
    {Name, _} = Leader,
    NewMemberId = {Name, PassiveNode},
    start_member(Cluster, PassiveNode),
    {ok, _, _} = ra:add_member(Leader, NewMemberId, passive),
    
    assert_cluster_still_works(Leader),
    {ok, NewMembers, Leader} = ra:members(Leader),
    ?assertEqual(4, erlang:length(NewMembers)),
    ?assert(lists:member(NewMemberId, NewMembers)),
    timer:sleep(500), % TODO: remove this
    Overviews = overviews(NewMembers),
    ct:pal("Overviews: ~p", [Overviews]),
    assert_overviews(Overviews, passive_peers, [NewMemberId]),
    
    {ok, _, Leader} = ra:remove_member(Leader, NewMemberId),

    {ok, Members2, _}  = ra:members(Leader),
    ?assertNot(lists:member(NewMemberId, Members2)),
    Overviews2 = overviews(Members2),
    assert_overviews(Overviews2, passive_peers, []),

    terminate_cluster(Cluster),
    terminate_peers(Nodes).

given_2_node_cluster_with_1_passive(Config) -> 
    {Nodes, [PassiveNode]} = lists:split(1, start_erlang_nodes(2, Config)),
    
    Cluster = start_ra_cluster(?FUNCTION_NAME, Nodes),
    {ok, _Members, OriginalLeader} = ra:members(hd(Cluster)),
    
    {Name, _} = OriginalLeader,
    NewMemberId = {Name, PassiveNode},
    start_member(Cluster, PassiveNode),
    {ok, _, _} = ra:add_member(OriginalLeader, NewMemberId, passive),
    
    assert_cluster_still_works(OriginalLeader),
    {ok, NewCluster, OriginalLeader} = ra:members(OriginalLeader),
    stop_server(OriginalLeader),

    assert_cluster_down(NewCluster),

    _ = ra:force_change_passive_members(NewMemberId, [OriginalLeader], 5000), % todo assert return value
    ct:pal("Forced cluster change"),
    
    {ok, NewCluster, NewMemberId} = ra:members(NewMemberId),
    
    Overviews = overviews([NewMemberId]),
    assert_overviews(Overviews, passive_peers, [OriginalLeader]),

    ra:remove_member(NewMemberId, OriginalLeader),

    {ok, [NewMemberId], NewMemberId} = ra:members(NewMemberId),
    Overviews2 = overviews([NewMemberId]),
    assert_overviews(Overviews2, passive_peers, []),

    assert_cluster_still_works(NewMemberId),
    

    terminate_cluster(Cluster),
    terminate_peers(Nodes).

given_2_node_cluster_with_1_passive_on_failover_leader_returns_both_stop(Config) -> 
    {Nodes, [PassiveNode]} = lists:split(1, start_erlang_nodes(2, Config)),
    
    Cluster = start_ra_cluster(?FUNCTION_NAME, Nodes),
    {ok, _Members, OriginalLeader} = ra:members(hd(Cluster)),
    
    {Name, OriginalNode} = OriginalLeader,
    NewMemberId = {Name, PassiveNode},
    start_member(Cluster, PassiveNode),
    {ok, _, _} = ra:add_member(OriginalLeader, NewMemberId, passive),
    
    assert_cluster_still_works(OriginalLeader),
    {ok, NewCluster, OriginalLeader} = ra:members(OriginalLeader),
    stop_server(OriginalLeader),

    assert_cluster_down(NewCluster),

    _ = ra:force_change_passive_members(NewMemberId, [OriginalLeader], 5000), % todo assert return value
    ct:pal("Forced cluster change"),
    
    start_member(NewCluster, OriginalNode),
    

    assert_cluster_still_works(OriginalLeader), %% TODO: this should fail, but it doesn't
    assert_cluster_still_works(NewMemberId), %% TODO: this should fail, but it doesn't
    {ok, NewCluster, NewMemberId} = ra:members(NewMemberId),
    
    Overviews = overviews([NewMemberId]),
    assert_overviews(Overviews, passive_peers, [OriginalLeader]),

    ra:remove_member(NewMemberId, OriginalLeader),

    {ok, [NewMemberId], NewMemberId} = ra:members(NewMemberId),
    Overviews2 = overviews([NewMemberId]),
    assert_overviews(Overviews2, passive_peers, []),

    assert_cluster_still_works(NewMemberId),
    

    terminate_cluster(Cluster),
    terminate_peers(Nodes).

given_5_node_cluster_with_2_passive_on_failover(Config) -> 
    {ActiveNodes, PassiveNodes} = lists:split(3, start_erlang_nodes(5, Config)),
    
    Cluster = start_ra_cluster(?FUNCTION_NAME, ActiveNodes),
    {ok, ActiveMembers, OriginalLeader} = ra:members(hd(Cluster)),
    
    
    {Name, OriginalNode} = OriginalLeader,
    PassiveServerIds = [begin 
        NewMemberId = {Name, PassiveNode},
        start_member(Cluster, PassiveNode),
        {ok, _, _} = ra:add_member(OriginalLeader, NewMemberId, passive),
        NewMemberId
    end || PassiveNode <- PassiveNodes],
    
    assert_cluster_still_works(OriginalLeader),
    {ok, NewCluster, OriginalLeader} = ra:members(OriginalLeader),
    ?assertEqual(5, erlang:length(NewCluster)),

    Overviews2 = overviews(NewCluster),
    assert_overviews(Overviews2, passive_peers, PassiveServerIds),
    
    [begin 
        stop_server(A),
        ok
    end || A <- ActiveMembers],
    

    assert_cluster_down(NewCluster),
    ct:pal("Configuring passive ~p nodes to be active ~p", [PassiveNodes, ActiveMembers]),
    [begin 
        _ = ra:force_change_passive_members(M, ActiveMembers, 5000), % todo assert return value
        ok
    end || M <- PassiveServerIds],
    
    Overviews = overviews(PassiveServerIds),
    assert_overviews(Overviews, passive_peers, ActiveMembers),

    assert_cluster_still_works(hd(shuffle(PassiveServerIds))), 
    
    {ok, NewCluster, NewLeaderId} = ra:members(hd(shuffle(PassiveServerIds))),
    
    ct:pal("Removing members: ~p", [ActiveMembers]),
    [begin 
        ra:remove_member(NewLeaderId, A), 
        timer:sleep(500) 
    end || A <- ActiveMembers],

    {ok, NewClusterWithoutPastActive, NewLeaderId} = ra:members(hd(shuffle(PassiveServerIds))),
    
    ?assertEqual(2, erlang:length(NewClusterWithoutPastActive)),
    Overviews3 = overviews(NewClusterWithoutPastActive),
    assert_overviews(Overviews3, passive_peers, []),

    terminate_cluster(Cluster),
    terminate_peers(ActiveNodes ++ PassiveNodes).

rpc_all(Nodes, M, F, A) ->
    [
        begin
            ct:pal("Calling ~p:~p(~p) on ~p", [M, F, A, Node]),
            Res = ct_rpc:call(Node, M, F, A),
            ct:pal("Result: ~p", [Res])
        end
     || Node <- Nodes
    ].

start_erlang_nodes(Num, Config) ->
    PrivDir = filename:join(
        ?config(priv_dir, Config),
        ?MODULE
    ),
    Nodes = [
        begin
            Name = "node" ++ erlang:integer_to_list(N),
            Node = start_peer(Name, PrivDir),
            Node
        end
     || N <- lists:seq(1, Num)
    ],
    Nodes.

start_ra_cluster(Name, Nodes) ->
    _Cluster = start_remote_cluster(Name, Nodes, queue_machine()).

start_remote_cluster(ClusterName, Nodes, Machine) ->
    {ok, Started, Failed} = try_start_remote_cluster(ClusterName, Nodes, Machine),
    ?assertEqual([], Failed),
    ?assertEqual(erlang:length(Nodes), erlang:length(Started)),
    Started.

start_member(ExistingServers, TargetNode) ->
    {ClusterName, _} = hd(ExistingServers), 
    ServerId = {ClusterName, TargetNode},
    ok = ra:start_server(default,ClusterName, ServerId, queue_machine(), ExistingServers).

try_start_remote_cluster(ClusterName, Nodes, Machine) ->
    ServerIds = [{ClusterName, Node} || Node <- Nodes],
    ra:start_cluster(default, ClusterName, Machine, ServerIds).

start_peer(Name, PrivDir) ->
    Dir0 = filename:join(PrivDir, Name),
    Dir = "'\"" ++ Dir0 ++ "\"'",
    Host = get_current_host(),
    Pa = string:join(
        ["-pa" | search_paths()] ++ ["-s ra -ra data_dir", Dir],
        " "
    ),
    {ok, S} = slave:start_link(Host, Name, Pa),
    _ = rpc:call(S, ra, start, []),
    ok = ct_rpc:call(
        S,
        logger,
        set_primary_config,
        [level, all]
    ),
    S.

terminate_peers(Nodes) ->
    [slave:stop(N) || N <- Nodes].

terminate_peer_of_server({_, Node}) ->
    ct:pal("Stopping node: ~p", [Node]),
    slave:stop(Node).

get_current_host() ->
    NodeStr = atom_to_list(node()),
    Host = re:replace(NodeStr, "^[^@]+@", "", [{return, list}]),
    list_to_atom(Host).

search_paths() ->
    Ld = code:lib_dir(),
    lists:filter(
        fun(P) -> string:prefix(P, Ld) =:= nomatch end,
        code:get_path()
    ).

queue_machine() ->
    {module, ra_queue, #{}}.

flush(Timeout) ->
    receive
        Msg ->
            ct:pal("Flush: ~p", [Msg]),
            flush(Timeout)
    after Timeout -> ok
    end.

terminate_cluster(Nodes) ->
    [ra:stop_server(?SYS, P) || P <- Nodes].

stop_server({_Name, _Node} = ServerId) ->
    Result = ra:stop_server(?SYS, ServerId),
    ct:pal("Stop server:  ~p: ~p", [ServerId, Result]).

assert_cluster_still_works({_, _} = Member) ->
    Item = {erlang:make_ref(), banana},
    ct:pal("Enqueuing item: ~p on ~p", [Item, Member]),
    {ok, _Members, Leader} = ra:process_command(Member, {enq, Item}),
    {ok, _, _} = ra:process_command(Leader, {deq, self()}),
    receive
        Item ->
            ok
    after 10000 ->
        flush(0),
        ?assert(false)
    end.

enqueue_item(Member, Item) ->
    {ok, _Members, Leader} = ra:process_command(Member, {enq, Item}),
    Leader.

assert_item_dequeued(Member, Item) ->
    {ok, _, _} = ra:process_command(Member, {deq, self()}),
    receive
        Item ->
            ok
    after 10000 ->
        ct:pal("Expected to dequeue ~p", [Item]),
        flush(0),
        ?assert(false)
    end.

random_element(List) ->
    lists:nth(rand:uniform(length(List)), List).

random_element_except(List, Except) ->
    random_element(lists:delete(Except, List)).

leader_of_cluster([H | Tail]) ->
    case ra:members(H, 10000) of
        {ok, _, Leader} ->
            Leader;
        {error, nodedown} ->
            leader_of_cluster(Tail);
        {error, noproc} ->
            leader_of_cluster(Tail)
    end.

assert_cluster_members_running(Cluster) ->
    ?assert(lists:all([assert_server_running(P) || P <- Cluster])).

assert_server_running({Proc, Node}) ->
    rpc:call(Node, erlang, whereis, [Proc]) =/= undefined.

assert_cluster_down(Cluster) ->
    [assert_server_down(P) || P <- Cluster].

assert_server_down({_Proc, _Node} = ServerId) ->
    case ra:consistent_query(ServerId, fun(_) -> working end) of
        {error, nodedown} ->
            ok;
        {timeout, ServerId} ->
            ok;
        {error, noproc} ->
            ok;
        Other ->
            ct:pal("assert_server_down result: ~p", [Other]),
            ?assert(false)
    end.

% we set it on the local node as well as many ra operations are rpc from
% the ct node to the cluster nodes
set_active_nodes(AllNodes, ActiveNodes) ->
    rpc_all([node()] ++ AllNodes, ra_groups, set_active_nodes, [ActiveNodes]).

shuffle(List) ->
    lists:sort(fun(_A, _B) -> rand:uniform(2) =:= 1 end, List).

is_server_process_alive({Proc, Node}) ->
    rpc:call(Node, erlang, whereis, [Proc]) =/= undefined.

node_of({_, N}) -> N.

overviews(Cluster) -> 
    [begin 
        ct:pal("Retrireving overview for ~p", [P]),
        {ok, O, ServerId} = ra:member_overview(P), 
        ct:pal("Overview for ~p: ~p", [P, O]),
        {ServerId, O} 
    end || {_, _} = P <- Cluster].

assert_overviews(Overviews,Key, Expected) ->
    Values = [{ServerId, maps:get(Key, O, {value_not_found, O})} || {ServerId, O} <- Overviews],
    lists:foreach(
        fun({_ServerId, Value}) ->
            case {Value, Expected} of
                {V, V} ->
                    ok;
                {A, B} when is_list(A), is_list(B), Key =:= passive_peers -> 
                    ?assertEqual(lists:sort(A), lists:sort(B));
                _ ->
                    ct:fail("Expected ~p, got ~p. Overviews: ~p", [Expected, Value, Overviews])
            end
        end,
        Values
    ).


