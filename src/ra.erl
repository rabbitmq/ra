%% @doc The primary module for interacting with ra nodes and clusters.

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
         send_and_notify/4,
         cast/2,
         cast/3,
         committed_query/2,
         members/1,
         consistent_query/2,
         % cluster management
         start_node/1,
         start_node/4,
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

%% @doc Starts the ra application
-spec start() -> ok.
start() ->
    {ok, _} = application:ensure_all_started(ra),
    ok.

%% @doc Starts a ra cluster on the current erlang node.
%% Useful for testing and exploration. Use {@link start_cluster/3} for
%% real use.
%%
%% @param Num the number of nodes in the cluster
%% @param Name The name of the cluster
%% @param Machine The {@link ra_machine:machine/0} configuration.
%% @returns `[ra_node_id()]' after a leader has been elected
-spec start_local_cluster(non_neg_integer(), atom() | string(),
                          ra_machine:machine()) ->
    [ra_node_id()].
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
    _ = ra:members(Node1),
    Res.

%% @doc Starts a ra node
%% @param Conf a ra_node_config() configuration map.
%% @returns `{ok | error, Error}'
-spec start_node(ra_node:ra_node_config()) -> ok | {error, term()}.
start_node(Conf) ->
    % don't match on return value in case it is already running
    case catch ra_nodes_sup:start_node(Conf) of
        {ok, _} -> ok;
        {ok, _, _} -> ok;
        {error, _} = Err -> Err;
        {'EXIT', Err} -> {error, Err}
    end.

%% @doc Restarts a previously succesfully started ra node
%% @param NodeId the ra_node_id() of the node
%% @returns `{ok | error, Error}' when error can be
%% `not_found' or `name_not_registered' when the ra node has never before
%% been started on erlang node.
-spec restart_node(ra_node_id()) -> ok | {error, term()}.
restart_node(NodeId) ->
    % don't match on return value in case it is already running
    case catch ra_nodes_sup:restart_node(NodeId) of
        {ok, _} -> ok;
        {ok, _, _} -> ok;
        {error, _} = Err -> Err;
        {'EXIT', Err} -> {error, Err}
    end.

%% @doc Stops a ra node
%% @param NodeId the ra_node_id() of the node
%% @returns `{ok | error, nodedown}'
-spec stop_node(ra_node_id()) -> ok | {error, nodedown}.
stop_node(NodeId) ->
    try ra_nodes_sup:stop_node(NodeId) of
        ok -> ok;
        {error, not_found} -> ok
    catch
        exit:noproc -> ok;
        exit:{{nodedown, _}, _} -> {error, nodedown}
    end.

%% @doc Deletes a ra node
%% The node is forcefully deleted.
%% @param NodeId the ra_node_id() of the node
%% @returns `{ok | error, nodedown}'
-spec delete_node(NodeId :: ra_node_id()) -> ok | {error, term()}.
delete_node(NodeId) ->
    ra_nodes_sup:delete_node(NodeId).

%% @doc Starts a distributed ra cluster.
%%
%%
%% @param ClusterId the cluster id of the cluster.
%% @param Machine The {@link ra_machine:machine/0} configuration.
%% @param NodeIds The list of ra node ids.
%% @returns
%% `{ok, Started, NotStarted}'  if a cluster could be successfully
%% started. A cluster can be successfully started if more than half of the
%% nodes provided could be started. Nodes that could not be started need to
%% be retried periodically using {@link start_node/1}
%%
%% `{error, cluster_not_formed}' if a cluster could not be started.
%%
%% If a cluster could not be formed any nodes that did manage to start are
%% forcefully deleted.
-spec start_cluster(ra_cluster_id(), ra_machine:machine(), [ra_node_id()]) ->
    {ok, [ra_node_id()], [ra_node_id()]} |
    {error, cluster_not_formed}.
start_cluster(ClusterId, Machine, NodeIds) ->
    % create locally unique id
    % as long as all nodes are on different erlang nodes we can use the same
    % uid for all
    % TODO: validate all nodes are on different erlang nodes
    {Started, NotStarted} =
        lists:partition(fun (N) ->
                                ok =:= start_node(ClusterId, N, Machine, NodeIds)
                        end, NodeIds),
    case Started of
        [] ->
            ?WARN("ra: failed to form new cluster ~w.~n "
                  "No nodes were succesfully started.~n", [ClusterId]),
            {error, cluster_not_formed};
        _ ->
            Node = hd(Started),
            ok = trigger_election(Node),
            case members(Node) of
                {ok, _, _} ->
                    % we have a functioning cluster
                    {ok, Started, NotStarted};
                Err ->
                    ?WARN("ra: failed to form new cluster ~w.~n "
                          "Error: ~w~n", [ClusterId, Err]),
                    [delete_node(N) || N <- Started],
                    % we do not have a functioning cluster
                    {error, cluster_not_formed}
            end
    end.

-spec start_node(ra_cluster_id(), ra_node_id(),
                 ra_machine:machine(), [ra_node_id()]) ->
    ok | {error, term()}.
start_node(ClusterId, NodeId, Machine, NodeIds) ->
    TS = erlang:system_time(millisecond),
    I = erlang:unique_integer(),
    UId = ra_lib:to_list(ClusterId)
          ++ ra_lib:to_list(I)
          ++ ra_lib:to_list(TS),
    U = list_to_binary(UId),
    Conf = #{cluster_id => ClusterId,
             id => NodeId,
             uid => U,
             initial_nodes => NodeIds,
             log_module => ra_log_file,
             log_init_args => #{uid => U},
             machine => Machine},
    start_node(Conf).

%% @doc Deletes a ra cluster in an orderly fashion
%% This function commits and end of life command which after each node applies
%% it will cause that node to shut down and delete all it's data.
%% The leader will stay up until it has successfully replicated the end of life
%% command to all nodes after which it too will shut down and delete all it's
%% data.
%% @param NodeIds the ra_node_ids of the cluster
%% @returns `{ok | error, nodedown}'
-spec delete_cluster(NodeIds :: [ra_node_id()]) -> ok | {error, term()}.
delete_cluster(NodeIds) ->
    delete_cluster(NodeIds, ?DEFAULT_TIMEOUT).

%% @see delete_cluster/1
-spec delete_cluster(NodeIds :: [ra_node_id()], timeout()) ->
    {ok, Leader::ra_node_id()} | {error, term()}.
delete_cluster(NodeIds, Timeout) ->
    delete_cluster0(NodeIds, Timeout, []).

delete_cluster0([NodeId | Rem], Timeout, Errs) ->
    DeleteCmd = {'$ra_cluster', delete, await_consensus},
    case ra_node_proc:command(NodeId, DeleteCmd, Timeout) of
        {ok, _, Leader} ->
            {ok, Leader};
        {timeout, _} = E ->
            delete_cluster0(Rem, Timeout, [E | Errs]);
        {error, _} = E ->
            delete_cluster0(Rem, Timeout, [{E, NodeId} | Errs])
    end;
delete_cluster0([], _, Errs) ->
    {error, {no_more_nodes_to_try, Errs}}.


%% @doc Add a ra node id to a ra cluster's membership configuration
%% This commits a join command to the leader log. After this has been replicated
%% the leader will start replicating entries to the new node.
%% This function returns after appending the command to the log.
%%
%% @param ServerRef the ra node to send the command to
%% @param NodeId the ra node id of the new node
-spec add_node(ra_node_id(), ra_node_id()) -> ra_cmd_ret().
add_node(ServerRef, NodeId) ->
    ra_node_proc:command(ServerRef, {'$ra_join', NodeId, after_log_append},
                         ?DEFAULT_TIMEOUT).

%% @doc Removes a node from the cluster's membership configuration
%% This function returns after appending the command to the log.
%%
%% @param ServerRef the ra node to send the command to
%% @param NodeId the ra node id of the node to remove
-spec remove_node(ra_node_id(), ra_node_id()) -> ra_cmd_ret().
remove_node(ServerRef, NodeId) ->
    ra_node_proc:command(ServerRef, {'$ra_leave', NodeId, after_log_append},
                         ?DEFAULT_TIMEOUT).

%% @doc Causes the node to entre the pre-vote and attempt become leader
%% It is necessary to call this function when starting a new cluster as a
%% branch new ra node will not automatically enter pre-vote by itself.
%% Previously started nodes will however.
%%
%% @param NodeId the ra node id of the node to trigger the election on.
-spec trigger_election(ra_node_id()) -> ok.
trigger_election(NodeId) ->
    ra_node_proc:trigger_election(NodeId).

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

%% @see send/3
-spec send(ra_node_id(), term()) -> ra_cmd_ret().
send(Ref, Data) ->
    send(Ref, Data, ?DEFAULT_TIMEOUT).

%% @doc send a command to the ra node.
%% if the ra node addressed isn't the leader and the leader is known
%% it will automatically redirect the call to the leader node.
%% This function returns after the command has been appended to the leader's
%% raft log.
%%
%% @param ServerRef the ra node id of the node to send the commadn to.
%% @param Command the command, an arbitrary term that the current state
%% machine can understand.
%% @param Timeout a timeout value
%% @returns {@link ra_cmd_ret()}
-spec send(ra_node_id(), term(), timeout()) -> ra_cmd_ret().
send(ServerRef, Command, Timeout) ->
    ra_node_proc:command(ServerRef, usr(Command, after_log_append), Timeout).

-spec send_and_await_consensus(ra_node_id(), term()) -> ra_cmd_ret().
send_and_await_consensus(Ref, Data) ->
    send_and_await_consensus(Ref, Data, ?DEFAULT_TIMEOUT).

%% @doc send a command to the ra node.
%% if the ra node addressed isn't the leader and the leader is known
%% it will automatically redirect the call to the leader node.
%% This function returns after the command has been replicated and applied to
%% the ra state machine. This is a fully synchronous interaction with the
%% ra consensus system.
%% Use this for low throughput actions where simple semantics are needed.
%% if the state machine supports it it may return a result value which will
%% be included in the result tuple.
%%
%% @param ServerRef the ra node id of the node to send the commadn to.
%% @param Command the command, an arbitrary term that the current state
%% machine can understand.
%% @param Timeout a timeout value
%% @returns {@link ra_cmd_ret()}
-spec send_and_await_consensus(ra_node_id(), term(), timeout()) ->
    ra_cmd_ret().
send_and_await_consensus(Ref, Data, Timeout) ->
    ra_node_proc:command(Ref, usr(Data, await_consensus), Timeout).

%% @doc send a command to the ra node using cast.
%% This will send a command to the ra node using a cast.
%% if the node addressed isn't the leader the command will be discarded and
%% and asyncronous notification message returned to the caller of the format:
%% `{ra_event, ra_node_id(), {rejected, {not_leader, Correlation, LeaderId}}'.
%%
%% If the node addressed is the leader the command will be appended to the log
%% and replicated. Once it achieves consensus and asynchronous notification
%% message of the format:
%% `{ra_event, ra_node_id(), {applied, [Correlation]}}'
%%
%% @param ServerRef the ra node id of the node to send the commadn to.
%% @param Command the command, an arbitrary term that the current state
%% machine can understand.
%% @param Timeout a timeout value
%% @returns {@link ra_cmd_ret()}
-spec send_and_notify(ra_node_id(), term(), term()) -> ok.
send_and_notify(ServerRef, Command, Correlation) ->
    Cmd = usr(Command, {notify_on_consensus, Correlation, self()}),
    ra_node_proc:cast_command(ServerRef, Cmd).

-spec send_and_notify(ra_node_id(), high | normal, term(), term()) -> ok.
send_and_notify(ServerRef, Priority, Command, Correlation) ->
    Cmd = usr(Command, {notify_on_consensus, Correlation, self()}),
    ra_node_proc:cast_command(ServerRef, Priority, Cmd).

%% @doc Cast a message to a node
%% This is the least reliable way to interact with a ra node. If the node
%% addressed isn't the leader no notification will be issued.
-spec cast(ra_node_id(), term()) -> ok.
cast(ServerRef, Command) ->
    Cmd = usr(Command, noreply),
    ra_node_proc:cast_command(ServerRef, Cmd).

%% @doc Cast a message to a node with a priority
%% This is the least reliable way to interact with a ra node. If the node
%% addressed isn't the leader no notification will be issued.
-spec cast(ra_node_id(), normal | high, term()) -> ok.
cast(ServerRef, Priority, Command) ->
    Cmd = usr(Command, noreply),
    ra_node_proc:cast_command(ServerRef, Priority, Cmd).

%% @doc query the machine state on any node
%% This allows you to run the QueryFun over the the machine state and
%% return the result. Any ra node can be addressed.
%% This can return infinitely state results.
-spec committed_query(NodeId :: ra_node_id(),
                  QueryFun :: fun((term()) -> term())) ->
    {ok, {ra_idxterm(), term()}, ra_node_id() | not_known}.
committed_query(ServerRef, QueryFun) ->
    ra_node_proc:query(ServerRef, QueryFun, dirty).

%% @doc Query the state machine
%% This allows a caller to query the state machine by appending the query
%% to the log and returning the result once applied. This guarantees the
%% result is consistent.
-spec consistent_query(Node::ra_node_id(),
                       QueryFun::fun((term()) -> term())) ->
    {ok, {ra_idxterm(), term()}, ra_node_id() | not_known}.
consistent_query(Node, QueryFun) ->
    ra_node_proc:query(Node, QueryFun, consistent).

%% @doc Query the members of a cluster
-spec members(ra_node_id()) -> ra_node_proc:ra_leader_call_ret([ra_node_id()]).
members(ServerRef) ->
    ra_node_proc:state_query(ServerRef, members).

%% internal

usr(Data, Mode) ->
    {'$usr', Data, Mode}.
