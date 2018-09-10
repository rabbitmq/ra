-module(ra_node_proc).

-behaviour(gen_statem).

-include("ra.hrl").

%% State functions
-export([recover/3,
         leader/3,
         follower/3,
         pre_vote/3,
         candidate/3,
         await_condition/3,
         terminating_leader/3,
         terminating_follower/3
        ]).

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
         cast_command/2,
         cast_command/3,
         query/4,
         state_query/3,
         trigger_election/2,
         ping/2,
         log_fold/4
        ]).

-export([send_rpc/2]).

-define(SERVER, ?MODULE).
-define(DEFAULT_BROADCAST_TIME, 50).
-define(DEFAULT_ELECTION_MULT, 3).
-define(TICK_INTERVAL_MS, 1000).
-define(DEFAULT_STOP_FOLLOWER_ELECTION, false).
-define(DEFAULT_AWAIT_CONDITION_TIMEOUT, 30000).
%% Utilisation average calculations are all in μs.
-define(USE_AVG_HALF_LIFE, 1000000.0).

-define(HANDLE_EFFECTS(Effects, EvtType, State0),
        handle_effects(?FUNCTION_NAME, Effects, EvtType, State0)).


-type query_fun() :: fun((term()) -> term()).

-type ra_command() :: {ra_node:command_type(), term(),
                       ra_node:command_reply_mode()}.

-type ra_command_priority() :: normal | high.

-type ra_leader_call_ret(Result) :: {ok, Result, Leader::ra_node_id()} |
                                    {error, term()} |
                                    {timeout, ra_node_id()}.

-type ra_cmd_ret() :: ra_leader_call_ret(ra_idxterm() | term()).

-type gen_statem_start_ret() :: {ok, pid()} | ignore | {error, term()}.

-type safe_call_ret(T) :: timeout | {error, noproc | nodedown} | T.

-type states() :: leader | follower | candidate | await_condition.

%% ra_event types
-type ra_event_reject_detail() :: {not_leader, Leader :: maybe(ra_node_id()),
                                   ra_node:command_correlation()}.

-type ra_event_body() ::
    % used for notifying senders of the ultimate fate of their command
    % sent using ra:send_and_notify
    {applied, [ra_node:command_correlation()]} |
    {rejected, ra_event_reject_detail()} |
    % used to send message side-effects emitted by the state machine
    {machine, term()}.

-type ra_event() :: {ra_event, ra_event_body()}.

-export_type([ra_leader_call_ret/1,
              ra_cmd_ret/0,
              safe_call_ret/1,
              ra_event_reject_detail/0,
              ra_event/0,
              ra_event_body/0]).

-record(state, {node_state :: ra_node:ra_node_state(),
                name :: atom(),
                broadcast_time = ?DEFAULT_BROADCAST_TIME :: non_neg_integer(),
                tick_timeout :: non_neg_integer(),
                monitors = #{} :: #{pid() | node() => maybe(reference())},
                pending_commands = [] :: [{{pid(), any()}, term()}],
                leader_monitor :: reference() | undefined,
                await_condition_timeout :: non_neg_integer(),
                delayed_commands =
                    queue:new() :: queue:queue(ra_node:command())}).

%%%===================================================================
%%% API
%%%===================================================================

-spec start_link(ra_node:ra_node_config()) -> gen_statem_start_ret().
start_link(Config = #{id := Id}) ->
    Name = ra_lib:ra_node_id_to_local_name(Id),
    gen_statem:start_link({local, Name}, ?MODULE, Config, []).

-spec command(ra_node_id(), ra_command(), timeout()) ->
    ra_cmd_ret().
command(ServerRef, Cmd, Timeout) ->
    leader_call(ServerRef, {command, high, Cmd}, Timeout).

-spec cast_command(ra_node_id(), ra_command()) -> ok.
cast_command(ServerRef, Cmd) ->
    gen_statem:cast(ServerRef, {command, normal, Cmd}).

-spec cast_command(ra_node_id(), ra_command_priority(), ra_command()) -> ok.
cast_command(ServerRef, Priority, Cmd) ->
    gen_statem:cast(ServerRef, {command, Priority, Cmd}).

-spec query(ra_node_id(), query_fun(), local | consistent, timeout()) ->
    {ok, {ra_idxterm(), term()}, ra_node_id()}.
query(ServerRef, QueryFun, local, Timeout) ->
    gen_statem:call(ServerRef, {local_query, QueryFun}, Timeout);
query(ServerRef, QueryFun, consistent, Timeout) ->
    % TODO: timeout
    command(ServerRef, {'$ra_query', QueryFun, await_consensus}, Timeout).

-spec log_fold(ra_node_id(), fun(), term(), integer()) -> term().
log_fold(ServerRef, Fun, InitialState, Timeout) ->
    gen_statem:call(ServerRef, {log_fold, Fun, InitialState}, Timeout).

%% used to query the raft state rather than the machine state
-spec state_query(ra_node_id(), all | members | machine, timeout()) ->
    ra_leader_call_ret(term()).
state_query(ServerRef, Spec, Timeout) ->
    leader_call(ServerRef, {state_query, Spec}, Timeout).

-spec trigger_election(ra_node_id(), timeout()) -> ok.
trigger_election(ServerRef, Timeout) ->
    gen_statem:call(ServerRef, trigger_election, Timeout).

-spec ping(ra_node_id(), timeout()) -> safe_call_ret({pong, states()}).
ping(ServerRef, Timeout) ->
    gen_statem_safe_call(ServerRef, ping, Timeout).

leader_call(ServerRef, Msg, Timeout) ->
    case gen_statem_safe_call(ServerRef, {leader_call, Msg},
                              Timeout) of
        {redirect, Leader} ->
            leader_call(Leader, Msg, Timeout);
        {machine_reply, Reply} ->
            {ok, Reply, ServerRef};
        {error, _} = E ->
            E;
        timeout ->
            % TODO: formatted error message
            {timeout, ServerRef};
        Reply ->
            {ok, Reply, ServerRef}
    end.

%%%===================================================================
%%% gen_statem callbacks
%%%===================================================================

init(Config0) when is_map(Config0) ->
    process_flag(trap_exit, true),
    Config = maps:merge(config_defaults(), Config0),
    {#{id := Id, uid := UId,
       cluster := Cluster} = NodeState,
     InitEffects} = ra_node:init(Config),
    Key = ra_lib:ra_node_id_to_local_name(Id),
    % ensure ra_directory has the new pid
    yes = ra_directory:register_name(UId, self(), Key),
    _ = ets:insert(ra_metrics, {Key, 0, 0}),
    % ensure each relevant node is connected
    Peers = maps:keys(maps:remove(Id, Cluster)),
    _ = lists:foreach(fun ({_, Node}) ->
                              net_kernel:connect_node(Node);
                          (_) -> node()
                      end, Peers),
    TickTime = maps:get(tick_timeout, Config),
    AwaitCondTimeout = maps:get(await_condition_timeout, Config),
    State0 = #state{node_state = NodeState, name = Key,
                    tick_timeout = TickTime,
                    await_condition_timeout = AwaitCondTimeout},
    %%?INFO("~w ra_node_proc:init/1:~n~p~n", [Id, ra_node:overview(NodeState)]),
    {State, Actions0} = ?HANDLE_EFFECTS(InitEffects, cast, State0),
    %% TODO: this should really be a {next_event, cast, go} but OTP 20.3
    %% does not support this. it was fixed in 20.3.2
    %% monitor nodes so that we can handle both nodeup and nodedown events
    ok = net_kernel:monitor_nodes(true),
    {ok, recover, State, [{state_timeout, 0, go} | Actions0]}.

%% callback mode
callback_mode() -> [state_functions, state_enter].

%%%===================================================================
%%% State functions
%%%===================================================================
recover(enter, _OldState, State = #state{name = Name}) ->
    ets:insert(ra_state, {Name, recover}),
    {keep_state, State};
recover(_EventType, go, State = #state{node_state = NodeState0}) ->
    NodeState = ra_node:recover(NodeState0),
    true = erlang:garbage_collect(),
    % New cluster starts should be coordinated and elections triggered
    % explicitly hence if this is a new one we wait here.
    % Else we set an election timer
    Actions = case ra_node:is_new(NodeState) of
                  true ->
                      [];
                  false ->
                      ?INFO("~w: is not new, setting election timeout.~n",
                            [id(State)]),
                      [election_timeout_action(short, State)]
              end,
    {next_state, follower, State#state{node_state = NodeState},
     set_tick_timer(State, Actions)};
recover(_, _, State) ->
    % all other events need to be postponed until we can return
    % `next_event` from init
    {keep_state, State, {postpone, true}}.

leader(enter, _, State = #state{name = Name, node_state = NS0}) ->
    ets:insert(ra_state, {Name, leader}),
    {keep_state, State#state{node_state = ra_node:become(leader, NS0)}};
leader(EventType, {leader_call, Msg}, State) ->
    %  no need to redirect
    leader(EventType, Msg, State);
leader(EventType, {leader_cast, Msg}, State) ->
    leader(EventType, Msg, State);
leader(EventType, {command, high, {CmdType, Data, ReplyMode}},
       #state{node_state = NodeState0} = State0) ->
    %% high priority commands are written immediately
    From = case EventType of
               cast -> undefined;
               {call, F} -> F
           end,
    {leader, NodeState, Effects} =
        ra_node:handle_leader({command, {CmdType, #{from => From},
                                         Data, ReplyMode}},
                              NodeState0),
    {State, Actions} = ?HANDLE_EFFECTS(Effects, EventType,
                                      State0#state{node_state = NodeState}),
    {keep_state, State, Actions};
leader(EventType, {command, normal, {CmdType, Data, ReplyMode}},
       #state{delayed_commands = Delayed} = State0) ->
    %% cache the normal command until the flush_commands message arrives
    From = case EventType of
               cast -> undefined;
               {call, F} -> F
           end,
    Cmd = {CmdType, #{from => From}, Data, ReplyMode},
    %% if there are no prior delayed commands
    %% (and thus no action queued to do so)
    %% queue a state timeout to flush them
    %% We use a cast to ourselves instead of a zero timeout as we want to
    %% get onto the back of the erlang mailbox not just the current gen_statem
    %% event buffer.
    case queue:is_empty(Delayed) of
        true ->
            ok = gen_statem:cast(self(), flush_commands);
        false ->
            ok
    end,
    {keep_state, State0#state{delayed_commands = queue:in(Cmd, Delayed)}, []};
leader(EventType, flush_commands,
       #state{node_state = NodeState0,
              delayed_commands = Delayed0} = State0) ->

    {DelQ, Delayed} = queue_take(25, Delayed0),

    % ?INFO("flushing commands ~w~n", [Delayed]),
    %% write a batch of delayed commands
    {leader, NodeState, Effects} =
        ra_node:handle_leader({commands, Delayed}, NodeState0),

    {State, Actions} = ?HANDLE_EFFECTS(Effects, EventType,
                                       State0#state{node_state = NodeState}),
    case queue:is_empty(DelQ) of
        true ->
            ok;
        false ->
            ok = gen_statem:cast(self(), flush_commands)
    end,
    {keep_state, State#state{delayed_commands = DelQ}, Actions};
leader({call, From}, {local_query, QueryFun},
       #state{node_state = NodeState} = State) ->
    Reply = perform_local_query(QueryFun, leader, NodeState),
    {keep_state, State, [{reply, From, Reply}]};
leader({call, From}, {state_query, Spec},
       #state{node_state = NodeState} = State) ->
    Reply = do_state_query(Spec, NodeState),
    {keep_state, State, [{reply, From, Reply}]};
leader({call, From}, ping, State) ->
    {keep_state, State, [{reply, From, {pong, leader}}]};
leader(info, {node_event, _Node, _Evt}, State) ->
    {keep_state, State, []};
leader(info, {'DOWN', MRef, process, Pid, Info},
       #state{monitors = Monitors0,
              node_state = NodeState0} = State0) ->
    case maps:take(Pid, Monitors0) of
        {MRef, Monitors} ->
            % there is a monitor for the ref
            {leader, NodeState, Effects} =
                ra_node:handle_leader({command, {'$usr', #{from => undefined},
                                                 {down, Pid, Info}, noreply}},
                                      NodeState0),
            {State, Actions} =
                ?HANDLE_EFFECTS(Effects, cast,
                                State0#state{node_state = NodeState,
                                             monitors = Monitors}),
            {keep_state, State, Actions};
        error ->
            {keep_state, State0, []}
    end;
leader(info, {NodeEvt, Node},
       #state{monitors = Monitors0,
              node_state = NodeState0} = State0)
  when NodeEvt =:= nodedown orelse NodeEvt =:= nodeup ->
    case Monitors0 of
        #{Node := _} ->
            % there is a monitor for the node
            {leader, NodeState, Effects} =
                ra_node:handle_leader({command,
                                       {'$usr', #{from => undefined},
                                        {NodeEvt, Node}, noreply}},
                                      NodeState0),
            {State, Actions} =
                ?HANDLE_EFFECTS(Effects, cast,
                               State0#state{node_state = NodeState}),
            {keep_state, State, Actions};
        _ ->
            {keep_state, State0, []}
    end;
leader(_, tick_timeout, State0) ->
    State1 = maybe_persist_last_applied(send_rpcs(State0)),
    Effects = ra_node:tick(State1#state.node_state),
    {State, Actions} = ?HANDLE_EFFECTS(Effects, cast, State1),
    true = erlang:garbage_collect(),
    {keep_state, State, set_tick_timer(State, Actions)};
leader({call, From}, trigger_election, State) ->
    {keep_state, State, [{reply, From, ok}]};
leader({call, From}, {log_fold, Fun, Term}, State) ->
    fold_log(From, Fun, Term, State);
leader(EventType, Msg, State0) ->
    case handle_leader(Msg, State0) of
        {leader, State1, Effects} ->
            {State, Actions} = ?HANDLE_EFFECTS(Effects, EventType, State1),
            {keep_state, State, Actions};
        {follower, State1, Effects} ->
            {State, Actions} = ?HANDLE_EFFECTS(Effects, EventType, State1),
            % demonitor when stepping down
            ok = lists:foreach(fun erlang:demonitor/1,
                               maps:values(State#state.monitors)),
            {next_state, follower, State#state{monitors = #{}},
             maybe_set_election_timeout(State, Actions)};
        {stop, State1, Effects} ->
            % interact before shutting down in case followers need
            % to know about the new commit index
            {State, _Actions} = ?HANDLE_EFFECTS(Effects, EventType, State1),
            {stop, normal, State};
        {delete_and_terminate, State1, Effects} ->
            {State2, Actions} = ?HANDLE_EFFECTS(Effects, EventType, State1),
            State = send_rpcs(State2),
            case ra_node:is_fully_replicated(State#state.node_state) of
                true ->
                    {stop, {shutdown, delete}, State};
                false ->
                    ?INFO("~w leader -> terminating_leader term: ~b~n",
                          [id(State), current_term(State)]),
                    {next_state, terminating_leader, State, Actions}
            end
    end.

candidate(enter, _, State = #state{name = Name}) ->
    ets:insert(ra_state, {Name, candidate}),
    {keep_state, State};
candidate({call, From}, {leader_call, Msg},
          #state{pending_commands = Pending} = State) ->
    {keep_state, State#state{pending_commands = [{From, Msg} | Pending]}};
candidate(cast, {command, _Priority,
                 {_CmdType, _Data, {notify_on_consensus, Corr, Pid}}},
         State) ->
    ok = reject_command(Pid, Corr, State),
    {keep_state, State, []};
candidate({call, From}, {local_query, QueryFun},
          #state{node_state = NodeState} = State) ->
    Reply = perform_local_query(QueryFun, candidate, NodeState),
    {keep_state, State, [{reply, From, Reply}]};
candidate({call, From}, ping, State) ->
    {keep_state, State, [{reply, From, {pong, candidate}}]};
candidate(info, {node_event, _Node, _Evt}, State) ->
    {keep_state, State};
candidate(_, tick_timeout, State0) ->
    State = maybe_persist_last_applied(State0),
    {keep_state, State, set_tick_timer(State, [])};
candidate({call, From}, trigger_election, State) ->
    {keep_state, State, [{reply, From, ok}]};
candidate(EventType, Msg, #state{pending_commands = Pending} = State0) ->
    case handle_candidate(Msg, State0) of
        {candidate, State1, Effects} ->
            {State, Actions0} = ?HANDLE_EFFECTS(Effects, EventType, State1),
            Actions = maybe_add_election_timeout(Msg, Actions0, State),
            {keep_state, State, Actions};
        {follower, State1, Effects} ->
            {State, Actions} = ?HANDLE_EFFECTS(Effects, EventType, State1),
            ?INFO("~w candidate -> follower term: ~b~n",
                  [id(State), current_term(State)]),
            {next_state, follower, State,
             % always set an election timeout here to ensure an unelectable
             % node doesn't cause an electable one not to trigger
             % another election when not using follower timeouts
             % TODO: only set this is leader was not detected
             [election_timeout_action(long, State) | Actions]};
        {leader, State1, Effects} ->
            {State2, Actions} = ?HANDLE_EFFECTS(Effects, EventType, State1),
            State = State2#state{pending_commands = []},
            % inject a bunch of command events to be processed when node
            % becomes leader
            NextEvents = [{next_event, {call, F}, Cmd} || {F, Cmd} <- Pending],
            ?INFO("~w candidate -> leader term: ~b~n",
                  [id(State), current_term(State)]),
            {next_state, leader, State, Actions ++ NextEvents}
    end.


pre_vote(enter, _, State = #state{name = Name}) ->
    ets:insert(ra_state, {Name, pre_vote}),
    {keep_state, State};
pre_vote({call, From}, {leader_call, Msg},
          State = #state{pending_commands = Pending}) ->
    {keep_state, State#state{pending_commands = [{From, Msg} | Pending]}};
pre_vote(cast, {command, _Priority,
                {_CmdType, _Data, {notify_on_consensus, Corr, Pid}}},
         State) ->
    ok = reject_command(Pid, Corr, State),
    {keep_state, State, []};
pre_vote({call, From}, {local_query, QueryFun},
          #state{node_state = NodeState} = State) ->
    Reply = perform_local_query(QueryFun, pre_vote, NodeState),
    {keep_state, State, [{reply, From, Reply}]};
pre_vote({call, From}, ping, State) ->
    {keep_state, State, [{reply, From, {pong, pre_vote}}]};
pre_vote(info, {node_event, _Node, _Evt}, State) ->
    {keep_state, State};
pre_vote(_, tick_timeout, State0) ->
    State = maybe_persist_last_applied(State0),
    {keep_state, State, set_tick_timer(State, [])};
pre_vote({call, From}, trigger_election, State) ->
    {keep_state, State, [{reply, From, ok}]};
pre_vote(EventType, Msg, State0) ->
    case handle_pre_vote(Msg, State0) of
        {pre_vote, State1, Effects} ->
            {State, Actions0} = ?HANDLE_EFFECTS(Effects, EventType, State1),
            Actions = maybe_add_election_timeout(Msg, Actions0, State),
            {keep_state, State, Actions};
        {follower, State1, Effects} ->
            {State, Actions} = ?HANDLE_EFFECTS(Effects, EventType, State1),
            ?INFO("~w pre_vote -> follower term: ~b~n",
                  [id(State), current_term(State)]),
            {next_state, follower, State,
             % always set an election timeout here to ensure an unelectable
             % node doesn't cause an electable one not to trigger
             % another election when not using follower timeouts
             [election_timeout_action(long, State) | Actions]};
        {candidate, State1, Effects} ->
            {State, Actions} = ?HANDLE_EFFECTS(Effects, EventType, State1),
            ?INFO("~w pre_vote -> candidate term: ~b~n",
                  [id(State), current_term(State)]),
            {next_state, candidate, State,
             [election_timeout_action(long, State) | Actions]}
    end.

follower(enter, _, State = #state{name = Name,
                                  node_state = NS0}) ->
    ets:insert(ra_state, {Name, follower}),
    {keep_state, State#state{node_state = ra_node:become(follower, NS0)}};
follower({call, From}, {leader_call, Msg}, State) ->
    maybe_redirect(From, Msg, State);
follower(_, {command, _Priority, {_CmdType, Data, noreply}},
         State) ->
    % forward to leader
    case leader_id(State) of
        undefined ->
            ?WARN("~w leader cast - leader not known. "
                  "Command is dropped.~n", [id(State)]),
            {keep_state, State, []};
        LeaderId ->
            ?INFO("~w follower leader cast - redirecting to ~w ~n",
                  [id(State), LeaderId]),
            ok = ra:cast(LeaderId, Data),
            {keep_state, State, []}
    end;
follower(cast, {command, _Priority,
                {_CmdType, _Data, {notify_on_consensus, Corr, Pid}}},
         State) ->
    ok = reject_command(Pid, Corr, State),
    {keep_state, State, []};
follower({call, From}, {local_query, QueryFun},
         #state{node_state = NodeState} = State) ->
    Reply = perform_local_query(QueryFun, follower, NodeState),
    {keep_state, State, [{reply, From, Reply}]};
follower({call, From}, trigger_election, State) ->
    ?INFO("~w: election triggered by ~w", [id(State), element(1, From)]),
    {keep_state, State, [{reply, From, ok},
                         {next_event, cast, election_timeout}]};
follower({call, From}, ping, State) ->
    {keep_state, State, [{reply, From, {pong, follower}}]};
follower(info, {'DOWN', MRef, process, _Pid, Info},
         #state{leader_monitor = MRef} = State) ->
    ?WARN("~w: Leader monitor down with ~W, setting election timeout~n",
          [id(State), Info, 8]),
    case Info of
        noconnection ->
            {keep_state, State#state{leader_monitor = undefined},
             [election_timeout_action(short, State)]};
        _ ->
            {keep_state, State#state{leader_monitor = undefined},
             [election_timeout_action(really_short, State)]}
    end;
follower(info, {node_event, Node, down}, State) ->
    case leader_id(State) of
        {_, Node} ->
            ?WARN("~w: Leader node ~w may be down, setting election timeout",
                  [id(State), Node]),
            {keep_state, State, [election_timeout_action(long, State)]};
        _ ->
            {keep_state, State}
    end;
follower(_, tick_timeout, State0) ->
    State = maybe_persist_last_applied(State0),
    true = erlang:garbage_collect(),
    {keep_state, State, set_tick_timer(State, [])};
follower({call, From}, {log_fold, Fun, Term}, State) ->
    fold_log(From, Fun, Term, State);
follower(EventType, Msg, #state{await_condition_timeout = AwaitCondTimeout,
                                leader_monitor = MRef} = State0) ->
    case handle_follower(Msg, State0) of
        {follower, State1, Effects} ->
            {State2, Actions} = ?HANDLE_EFFECTS(Effects, EventType, State1),
            State = follower_leader_change(State0, State2),
            {keep_state, State, maybe_set_election_timeout(State, Actions)};
        {pre_vote, State1, Effects} ->
            {State, Actions} = ?HANDLE_EFFECTS(Effects, EventType, State1),
            ?INFO("~w follower -> pre_vote in term: ~b~n",
                  [id(State), current_term(State)]),
            _ = stop_monitor(MRef),
            {next_state, pre_vote, State#state{leader_monitor = undefined},
             [election_timeout_action(long, State) | Actions]};
        {await_condition, State1, Effects} ->
            {State2, Actions} = ?HANDLE_EFFECTS(Effects, EventType, State1),
            State = follower_leader_change(State0, State2),
            ?INFO("~w follower -> await_condition term: ~b~n",
                  [id(State), current_term(State)]),
            {next_state, await_condition, State,
             [{state_timeout, AwaitCondTimeout, await_condition_timeout}
              | Actions]};
        {delete_and_terminate, State1, Effects} ->
            ?INFO("~w follower -> terminating_follower term: ~b~n",
                  [id(State1), current_term(State1)]),
            {State, Actions} = ?HANDLE_EFFECTS(Effects, EventType, State1),
            {next_state, terminating_follower, State, Actions}
    end.

terminating_leader(enter, _, State = #state{name = Name}) ->
    ets:insert(ra_state, {Name, terminating_leader}),
    {keep_state, State};
terminating_leader(_EvtType, {command, _, _}, State0) ->
    % do not process any further commands
    {keep_state, State0, []};
terminating_leader(EvtType, Msg, State0) ->
    ?INFO("terminating leader got ~w~n", [Msg]),
    {keep_state, State, Actions} = leader(EvtType, Msg, State0),
    NS = State#state.node_state,
    case ra_node:is_fully_replicated(NS) of
        true ->
            {stop, {shutdown, delete}, State};
        false ->
            ?INFO("~w: is not fully replicated after ~W~n", [id(State),
                                                             Msg, 7]),
            {keep_state, State, Actions}
    end.

terminating_follower(enter, _OldState, State = #state{name = Name}) ->
    ets:insert(ra_state, {Name, terminating_follower}),
    {keep_state, State};
terminating_follower(EvtType, Msg, State0) ->
    % only process ra_log_events
    {keep_state, State, Actions} = follower(EvtType, Msg, State0),
    case ra_node:is_fully_persisted(State#state.node_state) of
        true ->
            {stop, {shutdown, delete}, State};
        false ->
            ?INFO("~w: is not fully persisted after ~W~n", [id(State),
                                                            Msg, 7]),
            {keep_state, State, Actions}
    end.

await_condition({call, From}, {leader_call, Msg}, State) ->
    maybe_redirect(From, Msg, State);
await_condition({call, From}, {local_query, QueryFun},
                #state{node_state = NodeState} = State) ->
    Reply = perform_local_query(QueryFun, follower, NodeState),
    {keep_state, State, [{reply, From, Reply}]};
await_condition({call, From}, ping, State) ->
    {keep_state, State, [{reply, From, {pong, await_condition}}]};
await_condition({call, From}, trigger_election, State) ->
    {keep_state, State, [{reply, From, ok},
                         {next_event, cast, election_timeout}]};
await_condition(info, {'DOWN', MRef, process, _Pid, _Info},
                State = #state{leader_monitor = MRef, name = Name}) ->
    ?WARN("~p: Leader monitor down. Setting election timeout.", [Name]),
    {keep_state, State#state{leader_monitor = undefined},
     [election_timeout_action(short, State)]};
await_condition(info, {node_event, Node, down}, State) ->
    case leader_id(State) of
        {_, Node} ->
            ?WARN("~p: Node ~p might be down. Setting election timeout.",
                  [id(State), Node]),
            {keep_state, State, [election_timeout_action(long, State)]};
        _ ->
            {keep_state, State}
    end;
await_condition(enter, _OldState, State = #state{name = Name}) ->
    ets:insert(ra_state, {Name, await_condition}),
    {keep_state, State};
await_condition(EventType, Msg, #state{leader_monitor = MRef} = State0) ->
    case handle_await_condition(Msg, State0) of
        {follower, State1, Effects} ->
            {State, Actions} = ?HANDLE_EFFECTS(Effects, EventType, State1),
            NewState = follower_leader_change(State0, State),
            ?INFO("~w await_condition -> follower in term: ~b~n",
                  [id(State), current_term(State)]),
            {next_state, follower, NewState,
             [{state_timeout, infinity, await_condition_timeout} |
              maybe_set_election_timeout(State, Actions)]};
        {pre_vote, State1, Effects} ->
            {State, Actions} = ?HANDLE_EFFECTS(Effects, EventType, State1),
            ?INFO("~w await_condition -> pre_vote in term: ~b~n",
                  [id(State), current_term(State1)]),
            _ = stop_monitor(MRef),
            {next_state, pre_vote, State#state{leader_monitor = undefined},
             [election_timeout_action(long, State) | Actions]};
        {await_condition, State1, []} ->
            {keep_state, State1, []}
    end.

handle_event(_EventType, EventContent, StateName, State) ->
    ?WARN("~p: handle_event unknown ~p~n", [id(State), EventContent]),
    {next_state, StateName, State}.

terminate(Reason, StateName,
          #state{name = Key, node_state = NodeState} = State) ->
    ?WARN("ra: ~w terminating with ~w in state ~w~n",
          [id(State), Reason, StateName]),
    UId = uid(State),
    _ = ra_node:terminate(NodeState, Reason),
    case Reason of
        {shutdown, delete} ->
            catch ra_log_segment_writer:release_segments(
                    ra_log_segment_writer, UId),
            catch ra_directory:unregister_name(UId),
            catch ra_log_meta:delete_sync(UId);
        _ -> ok
    end,
    _ = aten:unregister(Key),
    _ = ets:delete(ra_metrics, Key),
    _ = ets:delete(ra_state, Key),
    ok.

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

format_status(Opt, [_PDict, StateName, #state{node_state = NS}]) ->
    [{id, ra_node:id(NS)},
     {opt, Opt},
     {raft_state, StateName},
     {ra_node_state, ra_node:overview(NS)}
    ].

%%%===================================================================
%%% Internal functions
%%%===================================================================

queue_take(N, Q) ->
    queue_take(N, Q, []).

queue_take(0, Q, Acc) ->
    {Q, lists:reverse(Acc)};
queue_take(N, Q0, Acc) ->
    case queue:out(Q0) of
        {{value, I}, Q} ->
            queue_take(N-1, Q, [I | Acc]);
        {empty, _} ->
            {Q0, lists:reverse(Acc)}
    end.

handle_leader(Msg, #state{node_state = NodeState0} = State) ->
    case catch ra_node:handle_leader(Msg, NodeState0) of
        {NextState, NodeState, Effects}  ->
            {NextState, State#state{node_state = NodeState}, Effects}
    end.

handle_candidate(Msg, #state{node_state = NodeState0} = State) ->
    {NextState, NodeState, Effects} = ra_node:handle_candidate(Msg, NodeState0),
    {NextState, State#state{node_state = NodeState}, Effects}.

handle_pre_vote(Msg, #state{node_state = NodeState0} = State) ->
    {NextState, NodeState, Effects} = ra_node:handle_pre_vote(Msg, NodeState0),
    {NextState, State#state{node_state = NodeState}, Effects}.

handle_follower(Msg, #state{node_state = NodeState0} = State) ->
    {NextState, NodeState, Effects} = ra_node:handle_follower(Msg, NodeState0),
    {NextState, State#state{node_state = NodeState}, Effects}.

handle_await_condition(Msg, #state{node_state = NodeState0} = State) ->
    {NextState, NodeState, Effects} =
        ra_node:handle_await_condition(Msg, NodeState0),
    {NextState, State#state{node_state = NodeState}, Effects}.

perform_local_query(QueryFun, leader, #{machine := {machine, MacMod, _},
                                            machine_state := MacState,
                                            last_applied := Last,
                                            id := Leader,
                                            current_term := Term}) ->
    {ok, {{Last, Term}, ra_machine:query(MacMod, QueryFun, MacState)}, Leader};
perform_local_query(QueryFun, _, #{machine := {machine, MacMod, _},
                                       machine_state := MacState,
                                       last_applied := Last,
                                       current_term := Term} = NodeState) ->
    Leader = maps:get(leader_id, NodeState, not_known),
    {ok, {{Last, Term}, ra_machine:query(MacMod, QueryFun, MacState)}, Leader}.

% effect handler: either executes an effect or builds up a list of
% gen_statem 'Actions' to be returned.
handle_effects(RaftState, Effects, EvtType, State0) ->
    lists:foldl(fun(Effect, {State, Actions}) ->
                        handle_effect(RaftState, Effect, EvtType,
                                      State, Actions)
                end, {State0, []}, Effects).

handle_effect(_, {send_rpcs, Rpcs}, _, State0, Actions) ->
    % fully qualified use only so that we can mock it for testing
    % TODO: review / refactor
    [?MODULE:send_rpc(To, Rpc) || {To, Rpc} <- Rpcs],
    % TODO: record metrics for sending times
    {State0, Actions};
handle_effect(_, {next_event, Evt}, EvtType, State, Actions) ->
    {State, [{next_event, EvtType, Evt} |  Actions]};
handle_effect(_, {next_event, _, _} = Next, _, State, Actions) ->
    {State, [Next | Actions]};
handle_effect(_, {send_msg, To, Msg}, _, State, Actions) ->
    ok = send_ra_event(To, Msg, machine, State),
    {State, Actions};
handle_effect(RaftState, {aux, Cmd}, _, State, Actions) ->
    %% TODO: thread through state
    {_, NodeState, []} = ra_node:handle_aux(RaftState, cast, Cmd,
                                            State#state.node_state),

    {State#state{node_state = NodeState}, Actions};
handle_effect(_, {notify, Who, Correlations}, _, State, Actions) ->
    ok = send_ra_event(Who, Correlations, applied, State),
    {State, Actions};
handle_effect(_, {cast, To, Msg}, _, State, Actions) ->
    ok = gen_cast(To, Msg),
    {State, Actions};
handle_effect(_, {reply, From, Reply}, _, State, Actions) ->
    % reply directly
    ok = gen_statem:reply(From, Reply),
    {State, Actions};
handle_effect(_, {reply, Reply}, {call, From}, State, Actions) ->
    % reply directly
    ok = gen_statem:reply(From, Reply),
    {State, Actions};
handle_effect(_, {reply, Reply}, _, _, _) ->
    exit({undefined_reply, Reply});
handle_effect(_, {send_vote_requests, VoteRequests}, _, % EvtType
              State, Actions) ->
    % transient election processes
    T = {dirty_timeout, 500},
    Me = self(),
    [begin
         _ = spawn(fun () -> Reply = gen_statem:call(N, M, T),
                             ok = gen_statem:cast(Me, Reply)
                   end)
     end || {N, M} <- VoteRequests],
    {State, Actions};
handle_effect(_, {release_cursor, Index, MacState}, _EvtType,
              #state{node_state = NodeState0} = State, Actions) ->
    NodeState = ra_node:update_release_cursor(Index, MacState, NodeState0),
    {State#state{node_state = NodeState}, Actions};
handle_effect(_, garbage_collection, _EvtType, State, Actions) ->
    true = erlang:garbage_collect(),
    {State, Actions};
handle_effect(_, {monitor, process, Pid}, _,
              #state{monitors = Monitors} = State, Actions) ->
    case Monitors of
        #{Pid := _MRef} ->
            % monitor is already in place - do nothing
            {State, Actions};
        _ ->
            MRef = erlang:monitor(process, Pid),
            {State#state{monitors = Monitors#{Pid => MRef}}, Actions}
    end;
handle_effect(_, {monitor, node, Node}, _,
              #state{monitors = Monitors} = State, Actions) ->
    case Monitors of
        #{Node := _} ->
            % monitor is already in place - do nothing
            {State, Actions};
        _ ->
            %% no need to actually monitor anything as we've always monitoring
            %% all visible nodes
            {State#state{monitors = Monitors#{Node => undefined}}, Actions}
    end;
handle_effect(_, {demonitor, process, Pid}, _,
              #state{monitors = Monitors0} = State, Actions) ->
    case maps:take(Pid, Monitors0) of
        {MRef, Monitors} ->
            true = erlang:demonitor(MRef),
            {State#state{monitors = Monitors}, Actions};
        error ->
            % ref not known - do nothing
            {State, Actions}
    end;
handle_effect(_, {demonitor, node, Node}, _,
              #state{monitors = Monitors0} = State, Actions) ->
    case maps:take(Node, Monitors0) of
        {_, Monitors} ->
            {State#state{monitors = Monitors}, Actions};
        error ->
            {State, Actions}
    end;
handle_effect(_, {incr_metrics, Table, Ops}, _,
              State = #state{name = Key}, Actions) ->
    _ = ets:update_counter(Table, Key, Ops),
    {State, Actions};
handle_effect(_, {mod_call, Mod, Fun, Args}, _,
              State, Actions) ->
    _ = erlang:apply(Mod, Fun, Args),
    {State, Actions}.

send_rpcs(State0) ->
    {State, Rpcs} = make_rpcs(State0),
    % module call so that we can mock
    % TODO: review
    [ok = ?MODULE:send_rpc(To, Rpc) || {To, Rpc} <-  Rpcs],
    State.

make_rpcs(State) ->
    {NodeState, Rpcs} = ra_node:make_rpcs(State#state.node_state),
    {State#state{node_state = NodeState}, Rpcs}.

send_rpc(To, Msg) ->
    % fake gen cast - need to avoid any blocking delays here
    gen_cast(To, Msg).

gen_cast(To, Msg) ->
    send(To, {'$gen_cast', Msg}).

send_ra_event(To, Msg, EvtType, State) ->
    send(To, {ra_event, id(State), {EvtType, Msg}}).

id(#state{node_state = NodeState}) ->
    ra_node:id(NodeState).

uid(#state{node_state = NodeState}) ->
    ra_node:uid(NodeState).

leader_id(#state{node_state = NodeState}) ->
    ra_node:leader_id(NodeState).

-ifdef(info).
current_term(#state{node_state = NodeState}) ->
    ra_node:current_term(NodeState).
-endif.

maybe_set_election_timeout(#state{leader_monitor = LeaderMon},
                           Actions) when LeaderMon =/= undefined ->
    % only when a leader is known should we cancel the election timeout
    [{state_timeout, infinity, election_timeout} | Actions];
maybe_set_election_timeout(State, Actions) ->
    [election_timeout_action(short, State) | Actions].

election_timeout_action(really_short, #state{broadcast_time = Timeout}) ->
    T = rand:uniform(Timeout * ?DEFAULT_ELECTION_MULT),
    {state_timeout, T, election_timeout};
election_timeout_action(short, #state{broadcast_time = Timeout}) ->
    T = rand:uniform(Timeout * ?DEFAULT_ELECTION_MULT) + (Timeout * 2),
    {state_timeout, T, election_timeout};
election_timeout_action(long, #state{broadcast_time = Timeout}) ->
    T = rand:uniform(Timeout * ?DEFAULT_ELECTION_MULT) + (Timeout * 4),
    {state_timeout, T, election_timeout}.

% sets the tock timer for periodic actions such as sending stale rpcs
% or persisting the last_applied index
set_tick_timer(#state{tick_timeout = TickTimeout}, Actions) ->
    [{{timeout, tick}, TickTimeout, tick_timeout} | Actions].


follower_leader_change(Old, #state{pending_commands = Pending,
                                   leader_monitor = OldMRef} = New) ->
    OldLeader = leader_id(Old),
    case leader_id(New) of
        OldLeader ->
            % no change
            New;
        undefined ->
            New;
        NewLeader ->
            MRef = swap_monitor(OldMRef, NewLeader),
            LeaderNode = ra_lib:ra_node_id_node(NewLeader),
            ok = aten:register(LeaderNode),
            OldLeaderNode = ra_lib:ra_node_id_node(OldLeader),
            ok = aten:unregister(OldLeaderNode),
            % leader has either changed or just been set
            ?INFO("~w detected a new leader ~w in term ~b~n",
                  [id(New), NewLeader, current_term(New)]),
            [ok = gen_statem:reply(From, {redirect, NewLeader})
             || {From, _Data} <- Pending],
            New#state{pending_commands = [],
                      leader_monitor = MRef}
    end.

swap_monitor(MRef, L) ->
    stop_monitor(MRef),
    erlang:monitor(process, L).

stop_monitor(undefined) ->
    ok;
stop_monitor(MRef) ->
    erlang:demonitor(MRef),
    ok.

gen_statem_safe_call(ServerRef, Msg, Timeout) ->
    try
        gen_statem:call(ServerRef, Msg, Timeout)
    catch
         exit:{timeout, _} ->
            timeout;
         exit:{noproc, _} ->
            {error, noproc};
         exit:{{nodedown, _}, _} ->
            {error, nodedown}
    end.

do_state_query(all, State) -> State;
do_state_query(machine, #{machine_state := MacState}) ->
    MacState;
do_state_query(members, #{cluster := Cluster}) ->
    maps:keys(Cluster).

config_defaults() ->
    #{broadcast_time => ?DEFAULT_BROADCAST_TIME,
      tick_timeout => ?TICK_INTERVAL_MS,
      await_condition_timeout => ?DEFAULT_AWAIT_CONDITION_TIMEOUT,
      initial_nodes => []
     }.

maybe_redirect(From, Msg, #state{pending_commands = Pending,
                                 leader_monitor = LeaderMon} = State) ->
    Leader = leader_id(State),
    case LeaderMon of
        undefined ->
            ?WARN("~w leader call - leader not known. "
                  "Command will be forwarded once leader is known.~n",
                  [id(State)]),
            {keep_state,
             State#state{pending_commands = [{From, Msg} | Pending]}};
        _ when Leader =/= undefined ->
            {keep_state, State, {reply, From, {redirect, Leader}}}
    end.

reject_command(Pid, Corr, State) ->
    LeaderId = leader_id(State),
    ?INFO("~w: follower received leader command - rejecting to ~w ~n",
          [id(State), LeaderId]),
    send_ra_event(Pid, {not_leader, LeaderId, Corr}, rejected,
                       State).

maybe_persist_last_applied(#state{node_state = NS} = State) ->
     State#state{node_state = ra_node:persist_last_applied(NS)}.

send(To, Msg) ->
    % we do not want to block the ra node whilst attempting to set up
    % a TCP connection to a potentially down node.
    case erlang:send(To, Msg, [noconnect, nosuspend]) of
        ok -> ok;
        _ -> ok
    end.


fold_log(From, Fun, Term, State) ->
    case ra_node:log_fold(State#state.node_state, Fun, Term) of
        {ok, Result, NodeState} ->
            {keep_state, State#state{node_state = NodeState},
             [{reply, From, {ok, Result}}]};
        {error, Reason, NodeState} ->
            {keep_state, State#state{node_state = NodeState},
             [{reply, From, {error, Reason}}]}
    end.

maybe_add_election_timeout(election_timeout, Actions, State) ->
    [election_timeout_action(long, State) | Actions];
maybe_add_election_timeout(_, Actions, _) ->
    Actions.
