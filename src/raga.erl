-module(raga).


-export([
         start_cluster/4
        ]).

% -type node_id() :: reference().
% -opaque cluster_ref() :: {pid(), node_id()}.


% -export_type([
%               cluster_ref/0,
%               node_id/0
%              ]).

start_cluster(Num, Name, ApplyFun, InitialState) ->
    Nodes = [raga_node:name(Name, integer_to_list(N))
             || N <- lists:seq(1, Num)],
    Conf0 = #{log_module => raga_test_log,
              log_init_args => [],
              initial_nodes => Nodes,
              apply_fun => ApplyFun,
              initial_state => InitialState,
              cluster_id => Name},
    [begin
         {ok, Pid} = raga_node_proc:start_link(Conf0#{id => Id}),
         {Pid, Id}
     end || Id <- Nodes].
