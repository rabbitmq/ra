-module(ra_nodes_sup).

-behaviour(supervisor).

%% API functions
-export([start_node/1,
         restart_node/1,
         stop_node/1,
         remove_node/1,
         remove_all/0,
         start_link/0]).

%% Supervisor callbacks
-export([init/1]).

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

-spec remove_node(Id :: term()) -> ok | {error, term()}.
remove_node(Id) ->
    _ = stop_node(Id),
    supervisor:delete_child(?MODULE, Id).

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



