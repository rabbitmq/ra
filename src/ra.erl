-module(ra).

-include("ra.hrl").

-export([
         start_cluster/4,
         send/2,
         send_and_await_consensus/2,
         send_and_notify/2,
         dirty_query/2
        ]).

start_cluster(Num, Name, ApplyFun, InitialState) ->
    Nodes = [ra_node:name(Name, integer_to_list(N))
             || N <- lists:seq(1, Num)],
    Conf0 = #{log_module => ra_test_log,
              log_init_args => [],
              initial_nodes => Nodes,
              apply_fun => ApplyFun,
              initial_state => InitialState,
              cluster_id => Name},
    [begin
         {ok, Pid} = ra_node_proc:start_link(Conf0#{id => Id}),
         {Pid, Id}
     end || Id <- Nodes].

-spec send(ra_node_proc:server_ref(), term()) ->
    {ok, IdxTerm::{ra_index(), ra_term()}, Leader::ra_node_proc:server_ref()}.
send(Ref, Data) ->
    ra_node_proc:command(Ref, Data, after_log_append).

-spec send_and_await_consensus(ra_node_proc:server_ref(), term()) ->
    {ok, IdxTerm::{ra_index(), ra_term()}, Leader::ra_node_proc:server_ref()}.
send_and_await_consensus(Ref, Data) ->
    ra_node_proc:command(Ref, Data, await_consensus).

-spec send_and_notify(ra_node_proc:server_ref(), term()) ->
    {ok, IdxTerm::{ra_index(), ra_term()}, Leader::ra_node_proc:server_ref()}.
send_and_notify(Ref, Data) ->
    ra_node_proc:command(Ref, Data, notify_on_consensus).

-spec dirty_query(Node::ra_node_proc:server_ref(), QueryFun::fun((term()) -> term())) ->
    {ok, {ra_index(), ra_term()}, term()}.
dirty_query(Node, QueryFun) ->
    ra_node_proc:query(Node, QueryFun, dirty).
