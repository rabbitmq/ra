-module(ra_proxy).

-behaviour(gen_server).

-include("ra.hrl").

%% API functions
-export([start_link/3,
         proxy/3]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {appends :: list(),
                parent :: pid(),
                interval = 100 :: non_neg_integer(),
                timer_ref :: maybe(reference()),
                nodes :: #{node() => ok},
                quiesce :: boolean()}).

%%%===================================================================
%%% API functions
%%%===================================================================

start_link(ParentPid, Interval, StopFollowerElection) ->
    gen_server:start_link(?MODULE, [ParentPid, Interval, StopFollowerElection], []).

proxy(Pid, IsUrgent, Appends) ->
    gen_server:cast(Pid, {appends, IsUrgent, Appends}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([Parent, Interval, StopFollowerElection]) ->
    TRef = erlang:send_after(Interval, self(), broadcast),
    ok = net_kernel:monitor_nodes(true),
    Nodes = lists:foldl(fun(N, Acc) ->
                                maps:put(N, ok, Acc)
                        end, #{}, [node() | nodes()]),
    {ok, #state{appends = [],
                parent = Parent,
                interval = Interval,
                timer_ref = TRef,
                nodes = Nodes,
                quiesce = StopFollowerElection}}.

handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast({appends, _, Appends}, #state{appends = Appends,
                                          quiesce = true} = State) ->
    %% Nothing has changed, we can go silent
    {noreply, quiesce(State)};
handle_cast({appends, false, Appends}, State0) ->
    % not urgent just update appends and wait for next interval
    % if the timer had stopped, we must restart it
    {noreply, ensure_timer(State0#state{appends = Appends})};
handle_cast({appends, true, Appends}, State0) ->
    % urgent send append entries now
    State = State0#state{appends = Appends},
    ok = broadcast(State),
    % as we have just broadcast we can reset the timer
    {noreply, reset_timer(State)}.

handle_info(broadcast, State) ->
    ok = broadcast(State),
    {noreply, reset_timer(State)};
handle_info({nodeup, Node}, State = #state{nodes = Nodes}) ->
    ?DBG("proxy: nodeup received x ~p~n", [Node]),
    {noreply, State#state{nodes = maps:put(Node, ok, Nodes)}};
handle_info({nodedown, Node}, State = #state{nodes = Nodes}) ->
    {noreply, State#state{nodes = maps:remove(Node, Nodes)}};
handle_info(Msg, State) ->
    ?DBG("proxy: handle info unknown ~p~n", [Msg]),
    {noreply, State}.

terminate(Reason, State) ->
    ?DBG("proxy: terminate ~p ~p~n", [Reason, State]),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

reset_timer(State) ->
    % should we use the async flag here to ensure minimal blocking
    ensure_timer(quiesce(State)).

ensure_timer(State = #state{timer_ref = undefined, interval = Interval}) ->
    Ref = erlang:send_after(Interval, self(), broadcast),
    State#state{timer_ref = Ref};
ensure_timer(State) ->
    State.

quiesce(State = #state{timer_ref = undefined}) ->
    State;
quiesce(State = #state{timer_ref = Ref}) ->
    _ = erlang:cancel_timer(Ref),
    State#state{timer_ref = undefined}.

broadcast(#state{parent = Parent, appends = Appends, nodes = Nodes}) ->
    [begin
         % use the peer ref as the unique rpc reply reference
         % fake gen_call - reply goes to ra_node process
         try send(Peer, {'$gen_call', {Parent, Peer}, AE}) of
             _ -> ok
         catch
             _:_ = Err ->
                 ?DBG("Peer broadcast error ~p ~p~n", [Peer, Err]),
                 ok
         end
     end || {Peer, AE} <- Appends, is_connected(Peer, Nodes)],
    ok.

send(Dest, Msg) ->
    %% use nosuspend here as we don't want to delay the sending to other peers
    %% due to single overflowed buffer.
    erlang:send(Dest, Msg, [nosuspend]).

is_connected({_Proc, Node}, Nodes) ->
    maps:is_key(Node, Nodes);
is_connected(_, _) ->
    true. % default to true here to ensure we try to send
