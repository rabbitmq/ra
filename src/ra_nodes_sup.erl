-module(ra_nodes_sup).

-behaviour(supervisor).


%% API functions
-export([start_node/1,
         restart_node/1,
         stop_node/1,
         remove_node/2,
         remove_all/0,
         start_link/0]).

%% Supervisor callbacks
-export([init/1]).

-include("ra.hrl").

-spec start_node(ra_node:ra_node_config()) ->
    supervisor:startchild_ret().
start_node(#{id := Id} = Config) ->
    ChildSpec = #{id => Id,
                  type => worker,
                  % needs to be transient as may shut itself down by returning
                  % {stop, normal, State}
                  restart => transient,
                  start => {ra_node_proc, start_link, [Config]}},
    supervisor:start_child(?MODULE, ChildSpec).

restart_node(Id) ->
    supervisor:restart_child(?MODULE, Id).

-spec stop_node(Id :: term()) -> ok | {error, term()}.
stop_node(Id) ->
    supervisor:terminate_child(?MODULE, Id).

-spec remove_node(Id :: term(), DataDir :: file:name()) ->
    ok | {error, term()}.
remove_node(Id, DataDir) ->
    Node = ra_lib:ra_node_id_to_local_name(Id),
    ?INFO("Deleting node ~p and it's data.~n", [Id]),
    case ra_directory:registered_name_from_node_name(Node) of
        undefined ->
            ?WARN("remove_node: ~p registered name not found!~n", [Id]),
            % just stop the node as we cannot resolve a directory
            _ = stop_node(Id),
            supervisor:delete_child(?MODULE, Id);
        Name ->
            % TODO: resolve actual segment writer in use rather than
            % assuming it has a registered name
            ok = ra_log_file_segment_writer:release_segments(
                   ra_log_file_segment_writer, Name),
            _ = stop_node(Id),
            supervisor:delete_child(?MODULE, Id),
            Dir = filename:join(DataDir, binary_to_list(Name)),
            % TODO: catch errors?
            ok = ra_lib:recursive_delete(Dir),
            ok
    end.

remove_all() ->
    [begin
         supervisor:terminate_child(?MODULE, Id),
         supervisor:delete_child(?MODULE, Id)
     end
     || {Id, _, _, _} <- supervisor:which_children(?MODULE)],
    ok.

-spec start_link() ->
    {ok, pid()} | ignore | {error, term()}.
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    SupFlags = #{strategy => one_for_one, intensity => 10, period => 5},
    {ok, {SupFlags, []}}.



