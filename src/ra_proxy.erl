-module(ra_proxy).

-behaviour(gen_server).

-include("ra.hrl").

%% API functions
-export([start_link/2,
         proxy/2]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {appends :: map(),
                parent :: pid(),
                interval = 100 :: non_neg_integer(),
                timer_ref :: timer:tref()}).

%%%===================================================================
%%% API functions
%%%===================================================================

start_link(ParentPid, Interval) ->
    gen_server:start_link(?MODULE, [ParentPid, Interval], []).

proxy(Pid, Appends) ->
    gen_server:cast(Pid, {appends, Appends}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([Parent, Interval]) ->
    {ok, TRef} = timer:send_after(Interval, broadcast),
    {ok, #state{appends = #{}, parent = Parent,
                interval = Interval, timer_ref = TRef}}.

handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast({appends, Appends}, State0) ->
    ?DBG("proxy: handle cast appends ~p~n", [State0]),
    AppendsMap = maps:from_list(Appends),
    % TODO reset timer?
    case State0 of
        #state{appends = AppendsMap} ->
            % no change - do nothing
            % TODO check each entry rather than the whole
            {noreply, State0};
        _ ->
            State = State0#state{appends = AppendsMap},
            ok = broadcast(State),
            {noreply, State}
    end.

handle_info(broadcast, State = #state{interval = Interval}) ->
    ?DBG("proxy: handle info broadcast ~p~n", [State]),
    ok = broadcast(State),
    {ok, TRef} = timer:send_after(Interval, broadcast),
    {noreply, State#state{timer_ref = TRef}};
handle_info(Msg, State) ->
    ?DBG("proxy: handle info unknown ~p~n", [Msg]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

broadcast(#state{parent = Parent, appends = Appends, interval = _Interval}) ->
    ?DBG("broadcast ~p~n", [Appends]),
    [begin
         % use the peer ref as the unique rpc reply reference
         % fake gen_call - reply goes to ra_node process
         try Peer ! {'$gen_call', {Parent, Peer}, AE} of
             _ -> ok
         catch
             _:_ ->
                 ?DBG("Peer broadcast error ~p~n", [Peer]),
                 ok
         end
     end || {Peer, AE} <- maps:to_list(Appends)],
    ok.
