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
         start_node/4
        ]).

-type ra_idxterm() :: {ra_index(), ra_term()}.

-type ra_sendret() :: {ok, ra_idxterm(), Leader::ra_node_proc:server_ref()} |
                      {timeout, ra_node_proc:server_ref()}.

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


-spec start_node(atom(), [{atom(), node()}], fun(), term()) ->
    ok.
start_node(Name, Peers, ApplyFun, InitialState) ->
    Conf0 = #{log_module => ra_test_log,
              log_init_args => [],
              initial_nodes => [{Name, node()} | Peers],
              apply_fun => ApplyFun,
              initial_state => InitialState,
              cluster_id => Name},
    {ok, _Pid} = ra_node_proc:start_link(Conf0#{id => {Name, node()}}),
    ok.

-spec send(ra_node_proc:server_ref(), term()) -> ra_sendret().
send(Ref, Data) ->
    send(Ref, Data, ?DEFAULT_TIMEOUT).

-spec send(ra_node_proc:server_ref(), term(), timeout()) -> ra_sendret().
send(Ref, Data, Timeout) ->
    ra_node_proc:command(Ref, Data, after_log_append, Timeout).

-spec send_and_await_consensus(ra_node_proc:server_ref(), term()) ->
    ra_sendret().
send_and_await_consensus(Ref, Data) ->
    send_and_await_consensus(Ref, Data, ?DEFAULT_TIMEOUT).

-spec send_and_await_consensus(ra_node_proc:server_ref(), term(), timeout()) ->
    ra_sendret().
send_and_await_consensus(Ref, Data, Timeout) ->
    ra_node_proc:command(Ref, Data, await_consensus, Timeout).

-spec send_and_notify(ra_node_proc:server_ref(), term()) -> ra_sendret().
send_and_notify(Ref, Data) ->
    send_and_notify(Ref, Data, ?DEFAULT_TIMEOUT).

-spec send_and_notify(ra_node_proc:server_ref(), term(), timeout()) ->
    ra_sendret().
send_and_notify(Ref, Data, Timeout) ->
    ra_node_proc:command(Ref, Data, notify_on_consensus, Timeout).

-spec dirty_query(Node::ra_node_proc:server_ref(),
                  QueryFun::fun((term()) -> term())) ->
    {ok, ra_idxterm(), term()}.
dirty_query(ServerRef, QueryFun) ->
    ra_node_proc:query(ServerRef, QueryFun, dirty).

-spec consistent_query(Node::ra_node_proc:server_ref(),
                       QueryFun::fun((term()) -> term())) ->
    {ok, ra_idxterm(), term()}.
consistent_query(Node, QueryFun) ->
    ra_node_proc:query(Node, QueryFun, consistent).
