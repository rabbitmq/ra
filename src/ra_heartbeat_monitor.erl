-module(ra_heartbeat_monitor).

-include("ra.hrl").

-behaviour(gen_server).

%% API
-export([start_link/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).
-export([register/2, unregister/1, beat/2]).

-define(SERVER, ?MODULE).
-define(DEFAULT_HEARTBEAT_MONITOR_INTERVAL, 5000).
-define(DEFAULT_HEARTBEAT_TIMEOUT, 30000).

-record(state, {nodes :: [node()],
                index :: dict:dict(),
                node_mapping :: dict:dict(),
                interval :: integer(),
                timer_ref :: reference(),
                beat_timeout :: integer()}).

%%%===================================================================
%%% API
%%%===================================================================
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

register(Name, Peers) ->
    gen_server:call(?MODULE, {register, Name, Peers}).

unregister(Name) ->
    gen_server:call(?MODULE, {unregister, Name}).

beat(Node, Timeout) ->
    gen_server:call({?SERVER, Node}, beat, Timeout).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================
init([]) ->
    Interval = application:get_env(ra, heartbeat_monitor_interval,
                                   ?DEFAULT_HEARTBEAT_MONITOR_INTERVAL),
    Timeout = application:get_env(ra, heartbeat_timeout,
                                  ?DEFAULT_HEARTBEAT_TIMEOUT),
    TRef = erlang:send_after(Interval, self(), beat),
    {ok, #state{nodes = [],
                index = dict:new(),
                node_mapping = dict:new(),
                interval = Interval,
                timer_ref = TRef,
                beat_timeout = Timeout}}.

handle_call({register, Name, Peers}, _From, #state{nodes = Nodes,
                                                   index = Index,
                                                   node_mapping = Mapping}
            = State) ->
    NewNodes = add_nodes(Peers, Nodes),
    NewIndex = dict:append_list(Name, Peers, Index),
    NewMapping = add_mapping(Name, Peers, Mapping),
    {reply, ok, State#state{nodes = NewNodes,
                            index = NewIndex,
                            node_mapping = NewMapping}};
handle_call({unregister, Name}, _From, #state{index = Index,
                                              node_mapping = Mapping}
            = State) ->
    %% TODO remove empty nodes if they lose all ra nodes
    {DeregisterFrom, NewIndex} = dict:take(Name, Index),
    NewMapping = lists:foldl(fun(Peer, Acc) ->
                                     dict:update(Peer, fun(List) ->
                                                               lists:delete(Name, List)
                                                       end, Acc)
                             end, Mapping, DeregisterFrom),
    {reply, ok, State#state{index = NewIndex,
                            node_mapping = NewMapping}};
handle_call(beat, _From, State) ->
    {reply, ok, State};
handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(beat, State = #state{interval = Interval,
                                 nodes = Nodes,
                                 node_mapping = Mapping,
                                 beat_timeout = Timeout}) ->
    PangNodes = beat_nodes(Nodes, Timeout),
    notify_ra_nodes(PangNodes, Mapping),
    TRef = erlang:send_after(Interval, self(), beat),
    {noreply, State#state{timer_ref = TRef}};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
add_nodes(New, Existing) ->
    sets:to_list(sets:del_element(node(), sets:from_list(New ++ Existing))).

add_mapping(Name, Peers, Mapping) ->
    lists:foldl(fun(Peer, Acc) ->
                        dict:append(Peer, Name, Acc)
                end, Mapping, Peers).

beat_nodes(Nodes, Timeout) ->
    lists:foldl(fun(Node, Acc) ->
                        try
                            ok = ra_heartbeat_monitor:beat(Node, Timeout),
                            Acc
                        catch
                            exit:{{nodedown, _}, _} ->
                                ?DBG("heartbeat: detected node ~p down", [Node]),
                                [Node | Acc];
                            exit:{timeout, _} ->
                                ?DBG("heartbeat: detected node ~p timeout", [Node]),
                                [Node | Acc];
                            exit:Error ->
                                ?DBG("heartbeat: node ~p failed with: ~p", [Node, Error]),
                                [Node | Acc]
                        end
                end, [], Nodes).

notify_ra_nodes(Nodes, Mapping) ->
    lists:foreach(fun(Node) ->
                          lists:foreach(fun(RaNode) ->
                                                RaNode ! {node_down, Node}
                                        end, dict:fetch(Node, Mapping))
                  end, Nodes).
