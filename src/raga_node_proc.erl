-module(raga_node_proc).

-behaviour(gen_statem).

-include("raga.hrl").

%% API
-export([start_link/1,
         command/2
        ]).

%% State functions
-export([leader/3,
         follower/3,
         candidate/3]).

%% gen_statem callbacks
-export([
         init/1,
         format_status/2,
         handle_event/4,
         terminate/3,
         code_change/4,
         callback_mode/0
        ]).

-define(SERVER, ?MODULE).
-define(TEST_LOG, raga_test_log).
-define(DEFAULT_BROADCAST_TIME, 100).

-record(state, {node_state :: raga_node:raga_node_state(_),
                broadcast_time :: non_neg_integer(),
                proxy :: maybe(pid())}).

%%%===================================================================
%%% API
%%%===================================================================

start_link(Config = #{id := Id}) ->
    gen_statem:start_link({local, Id}, ?MODULE, [Config], []).

-spec command(gen_statem:server_ref(), term()) -> ok.
command(ServerRef, Data) ->
    gen_statem:cast(ServerRef, {command, Data}).

%%%===================================================================
%%% gen_statem callbacks
%%%===================================================================

init([Config]) ->
    State = #state{node_state = raga_node:init(Config),
                   broadcast_time = ?DEFAULT_BROADCAST_TIME},
    ?DBG("init state ~p~n", [State]),
    {ok, follower, State, election_timeout_action(State)}.

%% callback mode
callback_mode() -> state_functions.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% There should be one instance of this function for each possible
%% state name.  If callback_mode is statefunctions, one of these
%% functions is called when gen_statem receives and event from
%% call/2, cast/2, or as a normal process message.
%%
%% @spec state_name(Event, From, State) ->
%%                   {next_state, NextStateName, NextState} |
%%                   {next_state, NextStateName, NextState, Actions} |
%%                   {stop, Reason, NewState} |
%%    				 stop |
%%                   {stop, Reason :: term()} |
%%                   {stop, Reason :: term(), NewData :: data()} |
%%                   {stop_and_reply, Reason, Replies} |
%%                   {stop_and_reply, Reason, Replies, NewState} |
%%                   {keep_state, NewData :: data()} |
%%                   {keep_state, NewState, Actions} |
%%                   keep_state_and_data |
%%                   {keep_state_and_data, Actions}
%% @end
%%--------------------------------------------------------------------
leader(_EventType, {command, _Cmd}, State = #state{node_state = NodeState0}) ->
    % Persist command into log
    % Return raft index + term to caller so they can wait for apply notifications
    % Send msg to peer proxy with updated state data (so they can replicate)
    NodeState = NodeState0,
    {keep_state, State#state{node_state = NodeState}};
leader(EventType, Msg, State = #state{node_state = NodeState0 = #{id := Id}}) ->
    ?DBG("~p leader: ~p~n", [Id, Msg]),
    From = get_from(EventType),
    case raga_node:handle_leader(Msg, NodeState0) of
        {leader, NodeState, Actions} ->
            interaction(Actions, From, State),
            {keep_state, State#state{node_state = NodeState}};
        {follower, NodeState, Actions} ->
            % TODO kill proxy process
            interaction(Actions, From, State),
            {next_state, follower, State#state{node_state = NodeState},
             [election_timeout_action(State)]}
    end.

candidate(EventType, Msg, State = #state{node_state = NodeState0 = #{id := Id}}) ->
    ?DBG("~p candidate: ~p~n", [Id, Msg]),
    From = get_from(EventType),
    case raga_node:handle_candidate(Msg, NodeState0) of
        {candidate, NodeState, Actions} ->
            interaction(Actions, From, State),
            {keep_state, State#state{node_state = NodeState},
             election_timeout_action(State)};
        {follower, NodeState, Actions} ->
            interaction(Actions, From, State),
            {next_state, follower, State#state{node_state = NodeState},
             election_timeout_action(State)};
        {leader, NodeState, Actions} ->
            interaction(Actions, From, State),
            ?DBG("~p next leader~n", [Id]),
            {next_state, leader, State#state{node_state = NodeState}}
    end.

follower(EventType, Msg, State = #state{node_state = NodeState0 = #{id := Id}}) ->
    ?DBG("~p follower: ~p~n", [Id, Msg]),
    From = get_from(EventType),
    case raga_node:handle_follower(Msg, NodeState0) of
        {follower, NodeState, Actions} ->
            interaction(Actions, From, State),
            ?DBG("~p next follower: ~p~n", [Id, Actions]),
            {keep_state, State#state{node_state = NodeState},
             election_timeout_action(State)};
        {candidate, NodeState, Actions} ->
            interaction(Actions, From, State),
            ?DBG("~p next candidate: ~p ~p~n", [Id, Actions, NodeState]),
            {next_state, candidate, State#state{node_state = NodeState},
             election_timeout_action(State)}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%%
%% If callback_mode is handle_event_function, then whenever a
%% gen_statem receives an event from call/2, cast/2, or as a normal
%% process message, this function is called.
%% @end
% -spec handle_event(event_type(), any(), term(), state()) ->
%                    {next_state, NextStateName, NextState} |
%                    {next_state, NextStateName, NextState, Actions} |
%                    {stop, Reason, NewState} |
%     				 stop |
%                    {stop, Reason :: term()} |
%                    {stop, Reason :: term(), NewData :: data()} |
%                    {stop_and_reply, Reason, Replies} |
%                    {stop_and_reply, Reason, Replies, NewState} |
%                    {keep_state, NewData :: data()} |
%                    {keep_state, NewState, Actions} |
%                    keep_state_and_data |
%                    {keep_state_and_data, Actions}.
handle_event(_EventType, EventContent, StateName, State) ->
    ?DBG("handle_event unknownn ~p~n", [EventContent]),
    {next_state, StateName, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_statem when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_statem terminates with
%% Reason. The return value is ignored.
%%
%% @spec terminate(Reason, StateName, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, _StateName, _State) ->
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, StateName, State, Extra) ->
%%                   {ok, StateName, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Called (1) whenever sys:get_status/1,2 is called by gen_statem or
%% (2) when gen_statem terminates abnormally.
%% This callback is optional.
%%
%% @spec format_status(Opt, [PDict, StateName, State]) -> term()
%% @end
%%--------------------------------------------------------------------
format_status(_Opt, [_PDict, _StateName, _State]) ->
    Status = some_term,
    Status.

%%%===================================================================
%%% Internal functions
%%%===================================================================

interaction(none, _From, _State) ->
    ok;
interaction({reply, _Reply}, undefined, _State) ->
    exit(undefined_reply);
interaction({reply, Reply}, From, _State) ->
    ok = gen_statem:reply(From, Reply);
interaction({vote, Actions}, _From, _State) ->
    % transient election processes
    T = 500,
    Me = self(),
    [begin
         _ = spawn(fun () -> Reply = gen_statem:call(N, M, T),
                             ok = gen_statem:cast(Me, Reply)
                   end)
     end || {N, M} <- Actions],
    ok;
interaction({append, Actions}, _From, #state{proxy = undefined,
                                             broadcast_time = Interval}) ->
    ?DBG("Appends~p ~n", [Actions]),
    {ok, Proxy} = raga_proxy:start_link(self(), Interval),
    ?DBG("Proxy~p ~n", [Proxy]),
    ok = raga_proxy:proxy(Proxy, Actions);
interaction({append, Actions}, _From, #state{proxy = Proxy}) ->
    ok = raga_proxy:proxy(Proxy, Actions).


get_from({call, From}) -> From;
get_from(_) -> undefined.

election_timeout_action(#state{broadcast_time = Timeout}) ->
    T = rand:uniform(Timeout * 3) + (Timeout * 2),
    ?DBG("T: ~p~n", [T]),
    {timeout, T, election_timeout}.
