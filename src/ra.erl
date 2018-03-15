-module(ra).

-include("ra.hrl").

-export([
         start/0,
         start_local_cluster/3,
         send/2,
         send/3,
         send_and_await_consensus/2,
         send_and_await_consensus/3,
         send_and_notify/3,
         cast/2,
         dirty_query/2,
         members/1,
         consistent_query/2,
         % cluster management
         start_node/1,
         restart_node/1,
         stop_node/1,
         delete_node/1,
         % cluster operations
         start_cluster/3,
         delete_cluster/1,
         delete_cluster/2,

         add_node/2,
         remove_node/2,
         trigger_election/1,
         leave_and_terminate/1,
         leave_and_terminate/2
        ]).

-type ra_cmd_ret() :: ra_node_proc:ra_cmd_ret().

% starts the ra application
-spec start() -> ok.
start() ->
    {ok, _} = application:ensure_all_started(ra),
    ok.

start_local_cluster(Num, Name, Machine) ->
    [Node1 | _] = Nodes = [{ra_node:name(Name, integer_to_list(N)), node()}
                           || N <- lists:seq(1, Num)],
    Conf0 = #{log_module => ra_log_memory,
              log_init_args => #{},
              initial_nodes => Nodes,
              cluster_id => Name,
              machine => Machine},
    Res = [begin
               UId = atom_to_binary(element(1, Id), utf8),
               {ok, _Pid} = ra_node_proc:start_link(Conf0#{id => Id,
                                                           uid => UId}),
               Id
           end || Id <- Nodes],
    ok = ra:trigger_election(Node1),
    Res.

%% Starts a ra node
-spec start_node(ra_node:ra_node_config()) -> ok | {error, term()}.
start_node(Conf) ->
    % don't match on return value in case it is already running
    case catch ra_nodes_sup:start_node(Conf) of
        {ok, _} -> ok;
        {ok, _, _} -> ok;
        {error, _} = Err -> Err;
        {'EXIT', Err} -> {error, Err}
    end.

-spec restart_node(ra_node_id()) -> ok | {error, term()}.
restart_node(NodeId) ->
    % don't match on return value in case it is already running
    case catch ra_nodes_sup:restart_node(NodeId) of
        {ok, _} -> ok;
        {ok, _, _} -> ok;
        {error, _} = Err -> Err;
        {'EXIT', Err} -> {error, Err}
    end.

-spec stop_node(ra_node_id()) -> ok | {error, nodedown}.
stop_node(NodeId) ->
    try ra_nodes_sup:stop_node(NodeId) of
        ok -> ok;
        {error, not_found} -> ok
    catch
        exit:noproc -> ok;
        exit:{{nodedown, _}, _} -> {error, nodedown}
    end.

-spec delete_node(NodeId :: ra_node_id()) -> ok | {error, term()}.
delete_node(RaNodeId) ->
    ra_nodes_sup:delete_node(RaNodeId).

-spec start_cluster(ra_cluster_id(), ra_machine:machine(), [ra_node_id()]) ->
    {ok, [ra_node_id()], [ra_node_id()]} |
    {error, cluster_not_formed}.
start_cluster(ClusterId, Machine, NodeIds) ->
    % create locally unique id
    % as long as all nodes are on different erlang nodes we can use the same
    % uid for all
    % TODO: validate all nodes are on different erlang nodes
    TS = erlang:system_time(millisecond),
    UId = ra_lib:to_list(ClusterId) ++ ra_lib:to_list(TS),
    Configs = [#{cluster_id => ClusterId,
                 id => N,
                 uid => list_to_binary(UId),
                 initial_nodes => NodeIds,
                 log_module => ra_log_file,
                 log_init_args => #{uid => UId},
                 machine => Machine}
               || N <- NodeIds],
    {Started0, NotStarted0} =
        lists:partition(fun (C) -> ok =:= start_node(C) end, Configs),
    #{id := Node} = hd(Started0),
    ok = trigger_election(Node),
    Started = lists:map(fun (C) -> maps:get(id, C) end, Started0),
    NotStarted = lists:map(fun (C) -> maps:get(id, C) end, NotStarted0),
    case members(Node) of
        {ok, _, _} ->
            % we have a functioning cluster
            {ok, Started, NotStarted};
        _ ->
            [delete_node(N) || N <- Started],
            % we do not have a functioning cluster
            {error, cluster_not_formed}
    end.

-spec delete_cluster(NodeIds :: [ra_node_id()]) -> ok | {error, term()}.
delete_cluster(NodeIds) ->
    delete_cluster(NodeIds, ?DEFAULT_TIMEOUT).

-spec delete_cluster(NodeIds :: [ra_node_id()], timeout()) ->
    ok | {error, term()}.
delete_cluster(NodeIds, Timeout) ->
    delete_cluster0(NodeIds, Timeout, []).

delete_cluster0([NodeId | Rem], Timeout, Errs) ->
    DeleteCmd = {'$ra_cluster', delete, await_consensus},
    case ra_node_proc:command(NodeId, DeleteCmd, Timeout) of
        {ok, _, _} ->
            ok;
        {timeout, _} = E ->
            delete_cluster0(Rem, Timeout, [E | Errs]);
        {error, _} = E ->
            delete_cluster0(Rem, Timeout, [{E, NodeId} | Errs])
    end;
delete_cluster0([], _, Errs) ->
    {error, {no_more_nodes_to_try, Errs}}.


-spec add_node(ra_node_id(), ra_node_id()) ->
    ra_cmd_ret().
add_node(ServerRef, NodeId) ->
    ra_node_proc:command(ServerRef, {'$ra_join', NodeId, after_log_append},
                         ?DEFAULT_TIMEOUT).

-spec remove_node(ra_node_id(), ra_node_id()) -> ra_cmd_ret().
remove_node(ServerRef, NodeId) ->
    ra_node_proc:command(ServerRef, {'$ra_leave', NodeId, after_log_append},
                         ?DEFAULT_TIMEOUT).

-spec trigger_election(ra_node_id()) -> ok.
trigger_election(Id) ->
    ra_node_proc:trigger_election(Id).

% safe way to remove an active node from a cluster
leave_and_terminate(NodeId) ->
    leave_and_terminate(NodeId, NodeId).

-spec leave_and_terminate(ra_node_id(), ra_node_id()) ->
    ok | timeout | {error, no_proc}.
leave_and_terminate(ServerRef, NodeId) ->
    LeaveCmd = {'$ra_leave', NodeId, await_consensus},
    case ra_node_proc:command(ServerRef, LeaveCmd, ?DEFAULT_TIMEOUT) of
        {timeout, Who} ->
            ?ERR("request to ~p timed out trying to leave the cluster", [Who]),
            timeout;
        {error, no_proc} = Err ->
            Err;
        {ok, _, _} ->
            ?ERR("~p has left the building. terminating", [NodeId]),
            stop_node(NodeId)
    end.

-spec send(ra_node_id(), term()) -> ra_cmd_ret().
send(Ref, Data) ->
    send(Ref, Data, ?DEFAULT_TIMEOUT).

-spec send(ra_node_id(), term(), timeout()) -> ra_cmd_ret().
send(Ref, Data, Timeout) ->
    ra_node_proc:command(Ref, usr(Data, after_log_append), Timeout).

-spec send_and_await_consensus(ra_node_id(), term()) -> ra_cmd_ret().
send_and_await_consensus(Ref, Data) ->
    send_and_await_consensus(Ref, Data, ?DEFAULT_TIMEOUT).

-spec send_and_await_consensus(ra_node_id(), term(), timeout()) ->
    ra_cmd_ret().
send_and_await_consensus(Ref, Data, Timeout) ->
    ra_node_proc:command(Ref, usr(Data, await_consensus), Timeout).

-spec send_and_notify(ra_node_id(), term(), term()) -> ok.
send_and_notify(Ref, Data, Correlation) ->
    Cmd = usr(Data, {notify_on_consensus, Correlation, self()}),
    ra_node_proc:cast_command(Ref, Cmd).

-spec cast(ra_node_id(), term()) -> ok.
cast(Ref, Data) ->
    Cmd = usr(Data, noreply),
    ra_node_proc:cast_command(Ref, Cmd).

-spec dirty_query(Node::ra_node_id(), QueryFun::fun((term()) -> term())) ->
    {ok, {ra_idxterm(), term()}, ra_node_id() | not_known}.
dirty_query(ServerRef, QueryFun) ->
    ra_node_proc:query(ServerRef, QueryFun, dirty).

-spec consistent_query(Node::ra_node_id(),
                       QueryFun::fun((term()) -> term())) ->
    {ok, {ra_idxterm(), term()}, ra_node_id() | not_known}.
consistent_query(Node, QueryFun) ->
    ra_node_proc:query(Node, QueryFun, consistent).

-spec members(ra_node_id()) -> ra_node_proc:ra_leader_call_ret([ra_node_id()]).
members(ServerRef) ->
    ra_node_proc:state_query(ServerRef, members).


usr(Data, Mode) ->
    {'$usr', Data, Mode}.
