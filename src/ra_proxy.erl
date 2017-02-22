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

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link(ParentPid, Interval) ->
    gen_server:start_link(?MODULE, [ParentPid, Interval], []).

proxy(Pid, Appends) ->
    gen_server:cast(Pid, {appends, Appends}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
init([Parent, Interval]) ->
    {ok, TRef} = timer:send_after(Interval, broadcast),
    {ok, #state{appends = #{},
                parent = Parent,
                interval = Interval,
                timer_ref = TRef}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @spec handle_call(Request, From, State) ->
%%                                   {reply, Reply, State} |
%%                                   {reply, Reply, State, Timeout} |
%%                                   {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, Reply, State} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                  {noreply, State, Timeout} |
%%                                  {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_cast({appends, Appends}, State0) ->
    ?DBG("proxy: handle cast appends ~p~n", [State0]),
    AppendsMap = maps:from_list(Appends),
    % TODO reset timer?
    State = State0#state{appends = AppendsMap},
    ok = broadcast(State),
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_info(broadcast, State = #state{interval = Interval}) ->
    ?DBG("proxy: handle info broadcast ~p~n", [State]),
    ok = broadcast(State),
    {ok, TRef} = timer:send_after(Interval, broadcast),
    {noreply, State#state{timer_ref = TRef}};
handle_info(Msg, State) ->
    ?DBG("proxy: handle info unknown ~p~n", [Msg]),
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

broadcast(#state{parent = Parent, appends = Appends, interval = _Interval}) ->
    [begin
         % use the peer ref as the unique rpc reply reference
         Peer ! {'$gen_call', {Parent, Peer}, AE}, % fake gen_call
         % Reply = gen_statem:call(Peer, AE, {dirty_timeout, Interval}),
         % ok = gen_statem:cast(Parent, Reply)
         ok
     end || {Peer, AE} <- maps:to_list(Appends)],
    ok.
