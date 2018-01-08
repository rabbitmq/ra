-module(ra).

-include("ra.hrl").

-export([
         start_local_cluster/4,
         send/2,
         send/3,
         send_and_await_consensus/2,
         send_and_await_consensus/3,
         send_and_notify/2,
         send_and_notify/3,
         dirty_query/2,
         members/1,
         consistent_query/2,
         start_node/2,
         start_node/4,
         stop_node/1,
         add_node/2,
         remove_node/2,
         start_and_join/5,
         leave_and_terminate/1,
         leave_and_terminate/2
        ]).

-type ra_cmd_ret() :: ra_node_proc:ra_cmd_ret().

start_local_cluster(Num, Name, ApplyFun, InitialState) ->
    Nodes = [{ra_node:name(Name, integer_to_list(N)), node()}
             || N <- lists:seq(1, Num)],
    Conf0 = #{log_module => ra_log_memory,
              log_init_args => #{},
              initial_nodes => Nodes,
              apply_fun => ApplyFun,
              init_fun => fun (_) -> InitialState end},
    [begin
         {ok, _Pid} = ra_node_proc:start_link(Conf0#{id => Id}),
         Id
     end || Id <- Nodes].


-spec start_node(atom(), [ra_node_id()], ra_node:ra_machine_apply_fun(), term()) -> ok.
start_node(Name, Peers, ApplyFun, InitialState) ->
    Conf = #{log_module => ra_log_memory,
             log_init_args => #{},
             apply_fun => ApplyFun,
             init_fun => fun (_) -> InitialState end,
             initial_nodes => Peers},
    start_node(Name, Conf).

%% Starts a ra node on the local erlang node using the provided config.
-spec start_node(atom(), ra_node:ra_node_config()) -> ok.
start_node(Name, Conf0) when is_atom(Name) ->
    This = {Name, node()},
    Conf = maps:update_with(initial_nodes,
                            fun (Peers) ->
                                    lists:usort([This | Peers])
                            end,
                            Conf0#{id => This}),
    {ok, _Pid} = ra_nodes_sup:start_node(Conf),
    ok.

-spec stop_node(ra_node_id()) -> ok.
stop_node(ServerRef) ->
    try ra_nodes_sup:stop_node(ServerRef) of
        ok -> ok;
        {error, not_found} -> ok
    catch
        exit:noproc -> ok;
        exit:{{nodedown, _}, _}  -> ok
    end.

-spec add_node(ra_node_id(), ra_node_id()) ->
    ra_cmd_ret().
add_node(ServerRef, NodeId) ->
    ra_node_proc:command(ServerRef, {'$ra_join', NodeId, after_log_append},
                         ?DEFAULT_TIMEOUT).

-spec remove_node(ra_node_id(), ra_node_id()) -> ra_cmd_ret().
remove_node(ServerRef, NodeId) ->
    ra_node_proc:command(ServerRef, {'$ra_leave', NodeId, after_log_append}, 2000).

start_and_join(ServerRef, Name, Peers, ApplyFun, InitialState) ->
    ok = start_node(Name, Peers, ApplyFun, InitialState),
    NodeId = {Name, node()},
    JoinCmd = {'$ra_join', NodeId, await_consensus},
    case ra_node_proc:command(ServerRef, JoinCmd, ?DEFAULT_TIMEOUT) of
        {ok, _, _} -> ok;
        {timeout, Who} ->
            ?ERR("~p: request to ~p timed out trying to join the cluster", [NodeId, Who]),
            % this is awkward - we don't know if the request was received or not
            % it may still get processed so we have to leave the server up
            timeout;
        {error, _} = Err ->
            ?ERR("~p: request errored whilst ~p tried to join the cluster~n",
                 [NodeId, Err]),
            % shut down server
            stop_node(NodeId),
            Err
    end.

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

-spec send_and_notify(ra_node_id(), term()) -> ra_cmd_ret().
send_and_notify(Ref, Data) ->
    send_and_notify(Ref, Data, ?DEFAULT_TIMEOUT).

-spec send_and_notify(ra_node_id(), term(), timeout()) -> ra_cmd_ret().
send_and_notify(Ref, Data, Timeout) ->
    ra_node_proc:command(Ref, usr(Data, notify_on_consensus), Timeout).

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
