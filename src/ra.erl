-module(ra).

-include("ra.hrl").

-export([
         start_cluster/4,
         cast_command/2,
         call_command/2
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

-spec cast_command(ra_node_proc:server_ref(), term()) ->
    {IdxTerm::{ra_index(), ra_term()}, Leader::ra_node_proc:server_ref()}.
cast_command(Ref, Data) ->
    ra_node_proc:command(Ref, Data, no_wait).

-spec call_command(ra_node_proc:server_ref(), term()) ->
    {IdxTerm::{ra_index(), ra_term()}, Leader::ra_node_proc:server_ref()}.
call_command(Ref, Data) ->
    ra_node_proc:command(Ref, Data, await_consensus).
