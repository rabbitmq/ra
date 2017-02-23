-module(ra_node_proc).

-behaviour(gen_statem).

-include("ra.hrl").

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
-define(TEST_LOG, ra_test_log).
-define(DEFAULT_BROADCAST_TIME, 100).

-type server_ref() :: pid() | atom() | {node() | atom()}.

-record(state, {node_state :: ra_node:ra_node_state(_),
                broadcast_time :: non_neg_integer(),
                proxy :: maybe(pid()),
                pending_commands = [] :: [{{pid(), any()}, term()}]}).

%%%===================================================================
%%% API
%%%===================================================================

start_link(Config = #{id := Id}) ->
    gen_statem:start_link({local, Id}, ?MODULE, [Config], []).

-spec command(server_ref(), term()) -> ok.
command(ServerRef, Data) ->
    case gen_statem:call(ServerRef, {command, Data}) of
        {redirect, Leader} ->
            command(Leader, Data);
        Reply -> Reply
    end.

%%%===================================================================
%%% gen_statem callbacks
%%%===================================================================

init([Config]) ->
    State = #state{node_state = ra_node:init(Config),
                   broadcast_time = ?DEFAULT_BROADCAST_TIME},
    ?DBG("init state ~p~n", [State]),
    {ok, follower, State, election_timeout_action(State)}.

%% callback mode
callback_mode() -> state_functions.

%% state functions
leader({call, From}, {command, _Data} = Cmd,
       State = #state{node_state = NodeState0}) ->
    % Persist command into log
    % Return raft index + term to caller so they can wait for apply notifications
    % Send msg to peer proxy with updated state data (so they can replicate)
    {leader, NodeState, Actions} = ra_node:handle_leader(Cmd, NodeState0),
    interact(Actions, From, State),
    {keep_state, State#state{node_state = NodeState}};
leader(EventType, Msg, State = #state{node_state = NodeState0 = #{id := Id}}) ->
    ?DBG("~p leader: ~p~n", [Id, Msg]),
    From = get_from(EventType),
    case ra_node:handle_leader(Msg, NodeState0) of
        {leader, NodeState, Actions} ->
            interact(Actions, From, State),
            {keep_state, State#state{node_state = NodeState}};
        {follower, NodeState, Actions} ->
            ?DBG("~p leader abdicates!~n", [Id]),
            % TODO kill proxy process
            interact(Actions, From, State),
            {next_state, follower, State#state{node_state = NodeState},
             [election_timeout_action(State)]}
    end.

candidate({call, From}, {command, _Data},
          State = #state{node_state = #{leader_id := LeaderId}}) ->
    {keep_state, State, {reply, From, {redirect, LeaderId}}};
candidate({call, From}, {command, _Data} = Cmd,
          State = #state{pending_commands = Pending}) ->
    {keep_state, State#state{pending_commands = [{From, Cmd} | Pending]}};
candidate(EventType, Msg, State = #state{node_state = NodeState0 = #{id := Id},
                                         pending_commands = Pending}) ->
    ?DBG("~p candidate: ~p~n", [Id, Msg]),
    From = get_from(EventType),
    case ra_node:handle_candidate(Msg, NodeState0) of
        {candidate, NodeState, Actions} ->
            interact(Actions, From, State),
            {keep_state, State#state{node_state = NodeState},
             election_timeout_action(State)};
        {follower, NodeState, Actions} ->
            interact(Actions, From, State),
            {next_state, follower, State#state{node_state = NodeState},
             election_timeout_action(State)};
        {leader, NodeState, Actions} ->
            interact(Actions, From, State),
            ?DBG("~p next leader~n", [Id]),
            NextEvents = [{next_event, {call, F}, Cmd} || {F, Cmd} <- Pending],
            {next_state, leader, State#state{node_state = NodeState}, NextEvents}
    end.

follower({call, From}, {command, _Data},
         State = #state{node_state = #{leader_id := LeaderId}}) ->
    {keep_state, State, {reply, From, {redirect, LeaderId}}};
follower({call, From}, {command, _Data} = Cmd,
         State = #state{pending_commands = Pending}) ->
    {keep_state, State#state{pending_commands = [{From, Cmd} | Pending]}};
follower(EventType, Msg, State = #state{node_state = NodeState0 = #{id := Id}}) ->
    ?DBG("~p follower: ~p~n", [Id, Msg]),
    From = get_from(EventType),
    case ra_node:handle_follower(Msg, NodeState0) of
        {follower, NodeState, Actions} ->
            interact(Actions, From, State),
            ?DBG("~p next follower: ~p~n", [Id, Actions]),
            NewState = follower_leader_change(State,
                                              State#state{node_state = NodeState}),
            {keep_state, NewState,
             election_timeout_action(State)};
        {candidate, NodeState, Actions} ->
            interact(Actions, From, State),
            ?DBG("~p next candidate: ~p ~p~n", [Id, Actions, NodeState]),
            {next_state, candidate, State#state{node_state = NodeState},
             election_timeout_action(State)}
    end.


handle_event(_EventType, EventContent, StateName, State) ->
    ?DBG("handle_event unknownn ~p~n", [EventContent]),
    {next_state, StateName, State}.

terminate(Reason, _StateName, _State) ->
    ?DBG("ra terminating with ~p~n", [Reason]),
    ok.

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

format_status(_Opt, [_PDict, _StateName, _State]) ->
    Status = some_term,
    Status.

%%%===================================================================
%%% Internal functions
%%%===================================================================

interact(none, _From, _State) ->
    ok;
interact({reply, _Reply}, undefined, _State) ->
    exit(undefined_reply);
interact({reply, Reply}, From, _State) ->
    ok = gen_statem:reply(From, Reply);
interact({vote, Actions}, _From, _State) ->
    % transient election processes
    T = 500,
    Me = self(),
    [begin
         _ = spawn(fun () -> Reply = gen_statem:call(N, M, T),
                             ok = gen_statem:cast(Me, Reply)
                   end)
     end || {N, M} <- Actions],
    ok;
interact({append, Actions}, _From, #state{proxy = undefined,
                                             broadcast_time = Interval}) ->
    ?DBG("Appends~p ~n", [Actions]),
    {ok, Proxy} = ra_proxy:start_link(self(), Interval),
    ?DBG("Proxy~p ~n", [Proxy]),
    ok = ra_proxy:proxy(Proxy, Actions);
interact({append, Actions}, _From, #state{proxy = Proxy}) ->
    ok = ra_proxy:proxy(Proxy, Actions);
interact([Action | Actions], From, State) ->
    interact(Action, From, State),
    interact(Actions, From, State);
interact([], _From, _State) -> ok.


get_from({call, From}) -> From;
get_from(_) -> undefined.

election_timeout_action(#state{broadcast_time = Timeout}) ->
    T = rand:uniform(Timeout * 3) + (Timeout * 2),
    ?DBG("T: ~p~n", [T]),
    {timeout, T, election_timeout}.

follower_leader_change(#state{node_state = #{leader_id := L}},
                     #state{node_state = #{leader_id := L}} = New) ->
    % no change
    New;
follower_leader_change(_Old, #state{node_state = #{id := Id, leader_id := L},
                                    pending_commands = Pending} = New)
  when L /= undefined ->
    % leader has either changed or just been set
    ?DBG("~p A new leader has been detected: ~p~n", [Id, L]),
    [ok = gen_statem:reply(From, {redirect, L})
     || {From, _Data} <- Pending],
    New#state{pending_commands = []};
follower_leader_change(_Old, New) -> New.

