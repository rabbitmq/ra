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
         consistent_query/2,
         start_node/4,
         stop_node/1,
         add_node/2,
         remove_node/2,
         leave_and_terminate/1
        ]).

-type ra_cmd_ret() :: ra_node_proc:ra_cmd_ret().

-define(DEFAULT_TIMEOUT, 5000).

start_local_cluster(Num, Name, ApplyFun, InitialState) ->
    Nodes = [ra_node:name(Name, integer_to_list(N))
             || N <- lists:seq(1, Num)],
    Conf0 = #{log_module => ra_test_log,
              log_init_args => [],
              initial_nodes => Nodes,
              apply_fun => ApplyFun,
              initial_state => InitialState,
              cluster_id => Name},
    [begin
         {ok, _Pid} = ra_node_proc:start_link(Conf0#{id => Id}),
         {Id, node()}
     end || Id <- Nodes].


-spec start_node(atom(), [{atom(), node()}], fun(), term()) -> ok.
start_node(Name, Peers, ApplyFun, InitialState) ->
    Conf0 = #{log_module => ra_test_log,
              log_init_args => [],
              initial_nodes => [{Name, node()} | Peers],
              apply_fun => ApplyFun,
              initial_state => InitialState,
              cluster_id => Name},
    {ok, _Pid} = ra_node_proc:start_link(Conf0#{id => {Name, node()}}),
    ok.

-spec stop_node(ra_node_id()) -> ok.
stop_node(ServerRef) ->
    gen_statem:stop(ServerRef, normal, ?DEFAULT_TIMEOUT).

-spec add_node(ra_node_id(), ra_node_id()) ->
    ra_cmd_ret().
add_node(ServerRef, NodeId) ->
    ra_node_proc:command(ServerRef, {'$ra_join', NodeId, after_log_append}, 2000).

-spec remove_node(ra_node_id(), ra_node_id()) ->
    ra_cmd_ret().
remove_node(ServerRef, NodeId) ->
    ra_node_proc:command(ServerRef, {'$ra_leave', NodeId, after_log_append}, 2000).

% safe way to remove an active node from a cluster
-spec leave_and_terminate(ra_node_id()) -> ok | timeout.
leave_and_terminate(NodeId) ->
    LeaveCmd = {'$ra_leave', NodeId, await_consensus},
    case ra_node_proc:command(NodeId, LeaveCmd, ?DEFAULT_TIMEOUT) of
        {timeout, Who} ->
            ?DBG("request to ~p timed out trying to leave the cluster", [Who]),
            timeout;
        {ok, _, _} ->
            ?DBG("~p has left the building. terminating", [NodeId]),
            stop_node(NodeId)
    end.

-spec send(ra_node_id(), term()) ->
    ra_cmd_ret().
send(Ref, Data) ->
    send(Ref, Data, ?DEFAULT_TIMEOUT).

-spec send(ra_node_id(), term(), timeout()) ->
    ra_cmd_ret().
send(Ref, Data, Timeout) ->
    ra_node_proc:command(Ref, usr(Data, after_log_append), Timeout).

-spec send_and_await_consensus(ra_node_id(), term()) ->
    ra_cmd_ret().
send_and_await_consensus(Ref, Data) ->
    send_and_await_consensus(Ref, Data, ?DEFAULT_TIMEOUT).

-spec send_and_await_consensus(ra_node_id(), term(), timeout()) ->
    ra_cmd_ret().
send_and_await_consensus(Ref, Data, Timeout) ->
    ra_node_proc:command(Ref, usr(Data, await_consensus), Timeout).

-spec send_and_notify(ra_node_id(), term()) ->
    ra_cmd_ret().
send_and_notify(Ref, Data) ->
    send_and_notify(Ref, Data, ?DEFAULT_TIMEOUT).

-spec send_and_notify(ra_node_id(), term(), timeout()) ->
    ra_cmd_ret().
send_and_notify(Ref, Data, Timeout) ->
    ra_node_proc:command(Ref, usr(Data, notify_on_consensus), Timeout).

-spec dirty_query(Node::ra_node_id(),
                  QueryFun::fun((term()) -> term())) ->
    {ok, ra_idxterm(), term()}.
dirty_query(ServerRef, QueryFun) ->
    ra_node_proc:query(ServerRef, QueryFun, dirty).

-spec consistent_query(Node::ra_node_id(),
                       QueryFun::fun((term()) -> term())) ->
    {ok, ra_idxterm(), term()}.
consistent_query(Node, QueryFun) ->
    ra_node_proc:query(Node, QueryFun, consistent).

usr(Data, Mode) ->
    {'$usr', Data, Mode}.
