-module(ra_node_proc).

-behaviour(gen_statem).

-include("ra.hrl").

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

%% API
-export([start_link/1,
         command/3,
         query/3,
         change/2
        ]).

-define(SERVER, ?MODULE).
-define(TEST_LOG, ra_test_log).
-define(DEFAULT_BROADCAST_TIME, 100).


-type command_reply_mode() :: after_log_append | await_consensus
                                             | notify_on_consensus.

-type query_fun() :: fun((term()) -> term()).

-type ra_command_type() :: '$usr' | '$ra_query' | '$ra_join'
                           | '$ra_cluster_change'.

-type ra_command() :: {ra_command_type(), term(), command_reply_mode()}.

-type ra_cmd_ret() :: {ok, IdxTerm::ra_idxterm(), Leader::ra_node_id()}
                      | {error, term()}
                      | {timeout, ra_node_id()}.

-export_type([ra_cmd_ret/0]).

-record(state, {node_state :: ra_node:ra_node_state(),
                broadcast_time :: non_neg_integer(),
                proxy :: maybe(pid()),
                pending_commands = [] :: [{{pid(), any()}, term()}]}).

%%%===================================================================
%%% API
%%%===================================================================

start_link(Config = #{id := Id}) ->
    Name = ra_node_id_to_local_name(Id),
    gen_statem:start_link({local, Name}, ?MODULE, [Config], []).

-spec command(ra_node_id(), ra_command(), timeout()) ->
    ra_cmd_ret().
command(ServerRef, Cmd, Timeout) ->
    case gen_statem_safe_call(ServerRef, {command, Cmd},
                              {dirty_timeout, Timeout}) of
        {redirect, Leader} ->
            command(Leader, Cmd, Timeout);
        {error, _} = E ->
            E;
        timeout ->
            % TODO: formatted error message
            {timeout, ServerRef};
        Reply ->
            {ok, Reply, ServerRef}
    end.

-spec query(ra_node_id(), query_fun(), dirty | consistent) ->
    {ok, IdxTerm::{ra_index(), ra_term()}, term()}.
query(ServerRef, QueryFun, dirty) ->
    gen_statem:call(ServerRef, {dirty_query, QueryFun});
query(ServerRef, QueryFun, consistent) ->
    % TODO: timeout
    command(ServerRef, {'$ra_query', QueryFun, await_consensus}, 1000).

change(ServerRef, {add_node, NodeId}) ->
    command(ServerRef, {'$ra_join', NodeId, await_consensus}, 1000).

%%%===================================================================
%%% gen_statem callbacks
%%%===================================================================

init([Config]) ->
    process_flag(trap_exit, true),
    State = #state{node_state = ra_node:init(Config),
                   broadcast_time = ?DEFAULT_BROADCAST_TIME},
    ?DBG("init state ~p~n", [State]),
    {ok, follower, State, election_timeout_action(follower, State)}.

%% callback mode
callback_mode() -> state_functions.

%%%===================================================================
%%% State functions
%%%===================================================================

leader({call, From} = EventType, {command, {CmdType, Data, ReplyMode}} = C,
       State0 = #state{node_state = NodeState0}) ->
    %% Persist command into log
    %% Return raft index + term to caller so they can wait for apply
    %% notifications
    %% Send msg to peer proxy with updated state data
    %% (so they can replicate)
    ?DBG("leader ra_node_proc command ~p~n", [C]),
    {leader, NodeState, Effects} =
        ra_node:handle_leader({command, {CmdType, From, Data, ReplyMode}},
                              NodeState0),
    {State, Actions} = interact(Effects, EventType, State0),
    {keep_state, State#state{node_state = NodeState}, Actions};
leader({call, From}, {dirty_query, QueryFun},
         State = #state{node_state = NodeState}) ->
    Reply = perform_dirty_query(QueryFun, leader, NodeState),
    {keep_state, State, [{reply, From, Reply}]};
leader(_EventType, {'EXIT', Proxy0, Reason},
       State0 = #state{proxy = Proxy0,
                       broadcast_time = Interval,
                       node_state = NodeState = #{id := Id}}) ->
    ?DBG("~p leader proxy exited with ~p~nrestarting..~n", [Id, Reason]),
    % TODO: this is a bit hacky - refactor
    AppendEntries = ra_node:make_append_entries(NodeState),
    {ok, Proxy} = ra_proxy:start_link(self(), Interval),
    ok = ra_proxy:proxy(Proxy, AppendEntries),
    {keep_state, State0#state{proxy = Proxy}};
leader(EventType, Msg, State0 = #state{node_state = NodeState0,
                                       proxy = Proxy}) ->
    case ra_node:handle_leader(Msg, NodeState0) of
        {leader, NodeState, Effects} ->
            {State, Actions} = interact(Effects, EventType, State0),
            State1 = State#state{node_state = NodeState},
            {keep_state, State1, Actions};
        {follower, NodeState, Effects} ->
            % stop proxy process
            _ = gen_server:stop(Proxy, normal, 100),
            {State, Actions} = interact(Effects, EventType, State0),
            {next_state, follower, State#state{node_state = NodeState},
             [election_timeout_action(follower, State) | Actions]}
    end.

candidate({call, From}, {command, _Cmd},
          State = #state{node_state = #{leader_id := LeaderId}}) ->
    {keep_state, State, {reply, From, {redirect, LeaderId}}};
candidate({call, From}, {command, _Cmd} = Cmd,
          State = #state{pending_commands = Pending}) ->
    % stash commands until a leader is known
    {keep_state, State#state{pending_commands = [{From, Cmd} | Pending]}};
candidate({call, From}, {dirty_query, QueryFun},
         State = #state{node_state = NodeState}) ->
    Reply = perform_dirty_query(QueryFun, candidate, NodeState),
    {keep_state, State, [{reply, From, Reply}]};
candidate(EventType, Msg, State0 = #state{node_state = NodeState0
                                          = #{id := Id,
                                              current_term := Term},
                                          pending_commands = Pending}) ->
    case ra_node:handle_candidate(Msg, NodeState0) of
        {candidate, NodeState, Effects} ->
            {State, Actions} = interact(Effects, EventType, State0),
            {keep_state, State#state{node_state = NodeState},
             [election_timeout_action(candidate, State) | Actions]};
        {follower, NodeState, Effects} ->
            ?DBG("~p candidate -> follower term: ~p~n", [Id, Term]),
            {State, Actions} = interact(Effects, EventType, State0),
            {next_state, follower, State#state{node_state = NodeState},
             [election_timeout_action(follower, State) | Actions]};
        {leader, NodeState, Effects} ->
            {State, Actions} = interact(Effects, EventType, State0),
            ?DBG("~p candidate -> leader term: ~p~n", [Id, Term]),
            % inject a bunch of command events to be processed when node
            % becomes leader
            NextEvents = [{next_event, {call, F}, Cmd} || {F, Cmd} <- Pending],
            {next_state, leader,
             State#state{node_state = NodeState}, Actions ++ NextEvents}
    end.

follower({call, From}, {command, _Cmd},
         State = #state{node_state = #{leader_id := LeaderId}}) ->
    {keep_state, State, {reply, From, {redirect, LeaderId}}};
follower({call, From}, {command, _Cmd} = Msg,
         State = #state{pending_commands = Pending}) ->
    {keep_state, State#state{pending_commands = [{From, Msg} | Pending]}};
follower({call, From}, {dirty_query, QueryFun},
         State = #state{node_state = NodeState}) ->
    Reply = perform_dirty_query(QueryFun, follower, NodeState),
    {keep_state, State, [{reply, From, Reply}]};
follower(EventType, Msg,
         State0 = #state{node_state = NodeState0 = #{id := Id}}) ->
    case ra_node:handle_follower(Msg, NodeState0) of
        {follower, NodeState, Effects} ->
            {State, Actions} = interact(Effects, EventType, State0),
            NewState = follower_leader_change(State,
                                              State#state{node_state = NodeState}),
            {keep_state, NewState,
             [election_timeout_action(follower, State) | Actions]};
        {candidate, NodeState = #{current_term := NewTerm}, Effects} ->
            {State, Actions} = interact(Effects, EventType, State0),
            ?DBG("~p follower -> candidate term: ~p~n", [Id, NewTerm]),
            % TODO: the candidate timer should probably be a state_timeout()
            {next_state, candidate, State#state{node_state = NodeState},
             [election_timeout_action(candidate, State) | Actions]}
    end.

handle_event(_EventType, EventContent, StateName, State) ->
    ?DBG("handle_event unknown ~p~n", [EventContent]),
    {next_state, StateName, State}.

terminate(Reason, _StateName, #state{proxy = undefined}) ->
    ?DBG("ra terminating with ~p~n", [Reason]),
    ok;
terminate(Reason, _StateName, #state{proxy = Proxy}) ->
    ?DBG("ra terminating with ~p~n", [Reason]),
    _ = gen_server:stop(Proxy, Reason, 100),
    ok.

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

format_status(_Opt, [_PDict, _StateName, _State]) ->
    Status = some_term,
    Status.

%%%===================================================================
%%% Internal functions
%%%===================================================================

perform_dirty_query(QueryFun, leader, #{machine_state := MacState,
                                        last_applied := Last,
                                        id := Leader,
                                        current_term := Term}) ->
    {ok, {{Last, Term}, QueryFun(MacState)}, Leader};
perform_dirty_query(QueryFun, _StateName, #{machine_state := MacState,
                                            last_applied := Last,
                                            current_term := Term} = NodeState) ->
    Leader = maps:get(leader_id, NodeState, not_known),
    {ok, {{Last, Term}, QueryFun(MacState)}, Leader}.

interact(Effect, EvtType, State) ->
    interact(Effect, EvtType, State, []).

interact(none, _EvtType, State, Actions) ->
    {State, Actions};
interact({next_event, Evt}, EvtType, State, Actions) ->
    {State, [ {next_event, EvtType, Evt} |  Actions]};
interact({next_event, cast, Evt}, _EvtType, State, Actions) ->
    {State, [ {next_event, cast, Evt} |  Actions]};
interact({send_msg, To, Msg}, _EvtType, State, Actions) ->
    To ! Msg,
    {State, Actions};
interact({notify, Who, Reply}, _EvtType, State, Actions) ->
    ?DBG("notify ~p ~p~n", [Who, Reply]),
    _ = Who ! {consensus, Reply},
    {State, Actions};
interact({reply, _From, _Reply} = Action, _, State, Actions) ->
    {State, [Action | Actions]};
interact({reply, Reply}, {call, From}, State, Actions) ->
    {State, [{reply, From, Reply} | Actions]};
interact({reply, _Reply}, _, _State, _Actions) ->
    exit(undefined_reply);
interact({send_vote_requests, VoteRequests}, _EvtType, State, Actions) ->
    % transient election processes
    T = {dirty_timeout, 500},
    Me = self(),
    [begin
         _ = spawn(fun () -> Reply = gen_statem:call(N, M, T),
                             ok = gen_statem:cast(Me, Reply)
                   end)
     end || {N, M} <- VoteRequests],
    {State, Actions};
interact({send_append_entries, AppendEntries}, _EvtType,
         #state{proxy = undefined, broadcast_time = Interval} = State,
         Actions) ->
    {ok, Proxy} = ra_proxy:start_link(self(), Interval),
    ok = ra_proxy:proxy(Proxy, AppendEntries),
    {State#state{proxy = Proxy}, Actions};
interact({send_append_entries, AppendEntries}, _EvtType,
         #state{proxy = Proxy} = State, Actions) ->
    ok = ra_proxy:proxy(Proxy, AppendEntries),
    {State, Actions};
interact([Effect | Effects], EvtType, State0, Actions0) ->
    {State, Actions} = interact(Effect,  EvtType, State0, Actions0),
    interact(Effects, EvtType, State, Actions);
interact([], _EvtType, State, Actions) -> {State, Actions}.


election_timeout_action(follower, #state{broadcast_time = Timeout}) ->
    T = rand:uniform(Timeout * 3) + (Timeout * 2),
    {timeout, T, election_timeout};
election_timeout_action(candidate, #state{broadcast_time = Timeout}) ->
    T = rand:uniform(Timeout * 3) + (Timeout * 2),
    % candidate should use a state timeout instead of event
    {state_timeout, T, election_timeout}.

follower_leader_change(#state{node_state = #{leader_id := L}},
                       #state{node_state = #{leader_id := L}} = New) ->
    % no change
    New;
follower_leader_change(_Old, #state{node_state = #{id := Id, leader_id := L,
                                                   current_term := Term},
                                    pending_commands = Pending} = New)
  when L /= undefined ->
    % leader has either changed or just been set
    ?DBG("~p detected a new leader ~p in term ~p~n", [Id, L, Term]),
    [ok = gen_statem:reply(From, {redirect, L})
     || {From, _Data} <- Pending],
    New#state{pending_commands = []};
follower_leader_change(_Old, New) -> New.

ra_node_id_to_local_name({Name, _}) -> Name;
ra_node_id_to_local_name(Name) when is_atom(Name) -> Name.

gen_statem_safe_call(ServerRef, Msg, Timeout) ->
    try
        gen_statem:call(ServerRef, Msg, Timeout)
    catch
         exit:{timeout, _} ->
            timeout;
         exit:{noproc, _} ->
            {error, no_proc}
    end.

