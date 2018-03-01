-module(ra_nodes_sup).

-behaviour(supervisor).


%% API functions
-export([start_node/1,
         restart_node/1,
         stop_node/1,
         delete_node/1,
         remove_all/0,
         start_link/0,
         % for rpcs only
         prepare_restart_rpc/1,
         delete_node_rpc/1]).

%% Supervisor callbacks
-export([init/1]).

-include("ra.hrl").

-spec start_node(ra_node:ra_node_config()) ->
    supervisor:startchild_ret().
start_node(#{uid := UId,
             id := NodeId} = Config) ->
    Node = ra_lib:ra_node_id_node(NodeId),
    ChildSpec = #{id => UId,
                  type => worker,
                  % needs to be transient as may shut itself down by returning
                  % {stop, normal, State}
                  restart => transient,
                  start => {ra_node_proc, start_link, [Config]}},
    supervisor:start_child({?MODULE, Node}, ChildSpec).

-spec restart_node(ra_node_id()) -> supervisor:startchild_ret().
restart_node({RaName, Node}) ->
    {ok, #{uid := UId} = Conf} = rpc:call(Node, ?MODULE,
                                          prepare_restart_rpc,
                                          [RaName]),
    case supervisor:get_childspec({?MODULE, Node}, UId) of
        {ok, _} ->
            supervisor:restart_child({?MODULE, Node}, UId);
        {error, _Err} ->
            start_node(Conf)
    end.

prepare_restart_rpc(RaName) ->
    UId = ra_directory:registered_name_from_node_name(RaName),
    Dir = ra_env:data_dir(UId),
    % TODO this cannot work with another log implementation
    % can it be made generic without already knowing the config state?
    ra_log_file:read_config(Dir).

-spec stop_node(RaNodeId :: ra_node_id()) -> ok | {error, term()}.
stop_node({RaName, Node}) ->
    UId = rpc:call(Node, ra_directory,
                   registered_name_from_node_name, [RaName]),
    supervisor:terminate_child({?MODULE, Node}, UId);
stop_node(RaName) ->
    % local node
    UId = ra_directory:registered_name_from_node_name(RaName),
    supervisor:terminate_child(?MODULE, UId).

-spec delete_node(NodeId :: ra_node_id()) -> ok.
delete_node(NodeId) ->
    Node = ra_lib:ra_node_id_node(NodeId),
    Name = ra_lib:ra_node_id_to_local_name(NodeId),
    ?INFO("Deleting node ~p and it's data.~n", [NodeId]),
    _ = stop_node(NodeId),
    ok = rpc:call(Node, ?MODULE, delete_node_rpc, [Name]),
    ok.

delete_node_rpc(RaName) ->
    UId = ra_directory:registered_name_from_node_name(RaName),
    Dir = ra_env:data_dir(UId),
    ok = ra_log_file_segment_writer:release_segments(
           ra_log_file_segment_writer, UId),
    supervisor:delete_child(?MODULE, UId),
    % TODO: move into separate retrying process
    try ra_lib:recursive_delete(Dir) of
        ok -> ok
    catch
        _:_ = Err ->
            ?WARN("delete_node/2 failed to delete directory ~s~n"
                  "Error: ~p~n", [Dir, Err])
    end,
    ra_directory:unregister_name(UId),
    ok.

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
