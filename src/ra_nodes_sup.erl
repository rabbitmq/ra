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
start_node(#{id := NodeId} = Config) ->
    Node = ra_lib:ra_node_id_node(NodeId),
    supervisor:start_child({?MODULE, Node}, [Config]).

-spec restart_node(ra_node_id()) -> supervisor:startchild_ret().
restart_node({RaName, Node}) ->
    case rpc:call(Node, ?MODULE, prepare_restart_rpc, [RaName]) of
        {ok, Conf} ->
            start_node(Conf);
        Err ->
            {error, Err}
    end.


prepare_restart_rpc(RaName) ->
    case ra_directory:registered_name_from_node_name(RaName) of
        undefined ->
            name_not_registered;
        UId ->
            Dir = ra_env:data_dir(UId),
            % TODO this cannot work with another log implementation
            % can it be made generic without already knowing the config state?
            ra_log:read_config(Dir)
    end.

-spec stop_node(RaNodeId :: ra_node_id()) -> ok | {error, term()}.
stop_node({RaName, Node}) ->
    Pid = rpc:call(Node, ra_directory,
                   whereis_node_name, [RaName]),
    supervisor:terminate_child({?MODULE, Node}, Pid);
stop_node(RaName) ->
    % local node
    case ra_directory:whereis_node_name(RaName) of
        undefined -> ok;
        Pid ->
            supervisor:terminate_child(?MODULE, Pid)
    end.

-spec delete_node(NodeId :: ra_node_id()) -> ok | {error, term()}.
delete_node(NodeId) ->
    Node = ra_lib:ra_node_id_node(NodeId),
    Name = ra_lib:ra_node_id_to_local_name(NodeId),
    case stop_node(NodeId) of
        ok ->
            ?INFO("Deleting node ~p and it's data.~n", [NodeId]),
            rpc:call(Node, ?MODULE, delete_node_rpc, [Name]);
        {error, _} = Err -> Err
    end.

delete_node_rpc(RaName) ->
    %% TODO: better handle and report errors
    UId = ra_directory:registered_name_from_node_name(RaName),
    Dir = ra_env:data_dir(UId),
    ok = ra_log_file_segment_writer:release_segments(
           ra_log_file_segment_writer, UId),
    supervisor:terminate_child(?MODULE, UId),
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
         supervisor:terminate_child(?MODULE, Pid)
     end
     || {_, Pid, _, _} <- supervisor:which_children(?MODULE)],
    ok.

-spec start_link() ->
    {ok, pid()} | ignore | {error, term()}.
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    SupFlags = #{strategy => simple_one_for_one,
                 intensity => 10,
                 period => 5},
    ChildSpec = #{id => undefined,
                  type => worker,
                  % needs to be transient as may shut itself down by returning
                  % {stop, normal, State}
                  restart => transient,
                  start => {ra_node_proc, start_link, []}},
    {ok, {SupFlags, [ChildSpec]}}.
