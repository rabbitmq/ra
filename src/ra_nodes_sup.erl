-module(ra_nodes_sup).

-behaviour(supervisor).


%% API functions
-export([start_node/1,
         restart_node/1,
         stop_node/1,
         delete_node/2,
         remove_all/0,
         start_link/0]).

%% Supervisor callbacks
-export([init/1]).

-include("ra.hrl").

-spec start_node(ra_node:ra_node_config()) ->
    supervisor:startchild_ret().
start_node(#{uid := UId} = Config) ->
    ChildSpec = #{id => UId,
                  type => worker,
                  % needs to be transient as may shut itself down by returning
                  % {stop, normal, State}
                  restart => transient,
                  start => {ra_node_proc, start_link, [Config]}},
    supervisor:start_child(?MODULE, ChildSpec).

-spec restart_node(ra_node:ra_node_config()) -> supervisor:startchild_ret().
restart_node(#{uid := UId} = Config) when is_binary(UId) ->
    case supervisor:get_childspec(?MODULE, UId) of
        {ok, _} ->
            supervisor:restart_child(?MODULE, UId);
        {error, _Err} ->
            start_node(Config)
    end.


-spec stop_node(UId :: ra_uid()) -> ok | {error, term()}.
stop_node(UId) when is_binary(UId) ->
    supervisor:terminate_child(?MODULE, UId).

-spec delete_node(UId :: ra_uid(), DataDir :: file:name()) -> ok.
delete_node(UId, DataDir) ->
    ?INFO("Deleting node ~p and it's data.~n", [UId]),
    % TODO: resolve actual segment writer in use rather than
    % assuming it has a registered name
    ok = ra_log_file_segment_writer:release_segments(
           ra_log_file_segment_writer, UId),
    _ = stop_node(UId),
    supervisor:delete_child(?MODULE, UId),
    Dir = filename:join(DataDir, binary_to_list(UId)),
    % TODO: move into separate retrying process
    try ra_lib:recursive_delete(Dir) of
        ok -> ok
    catch
        _:_ = Err ->
            ?WARN("delete_node/2 failed to delete directory ~s~n"
                  "Error: ~p~n",
                  [Dir, Err])
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



