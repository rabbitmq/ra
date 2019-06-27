%% @hidden
-module(ra_server_proc).

-behaviour(gen_statem).

-include("ra.hrl").

%% State functions
-export([
         recover/3,
         recovered/3,
         leader/3,
         pre_vote/3,
         candidate/3,
         follower/3,
         receive_snapshot/3,
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
-define(DEFAULT_BROADCAST_TIME, 100).
-define(DEFAULT_ELECTION_MULT, 5).
-define(TICK_INTERVAL_MS, 1000).
-define(DEFAULT_STOP_FOLLOWER_ELECTION, false).
-define(DEFAULT_AWAIT_CONDITION_TIMEOUT, 30000).
%% Utilisation average calculations are all in μs.
-define(USE_AVG_HALF_LIFE, 1000000.0).
-define(INSTALL_SNAP_RPC_TIMEOUT, 120 * 1000).
-define(DEFAULT_RECEIVE_SNAPSHOT_TIMEOUT, 30000).
-define(DEFAULT_SNAPSHOT_CHUNK_SIZE, 1000000). % 1MB

-define(HANDLE_EFFECTS(Effects, EvtType, State0),
        handle_effects(?FUNCTION_NAME, Effects, EvtType, State0)).

-type query_fun() :: ra:query_fun().

-type ra_command() :: {ra_server:command_type(), term(),
                       ra_server:command_reply_mode()}.

-type ra_command_priority() :: normal | low.

-type ra_leader_call_ret(Result) :: {ok, Result, Leader::ra_server_id()} |
                                    {error, term()} |
                                    {timeout, ra_server_id()}.

-type ra_cmd_ret() :: ra_leader_call_ret(term()).

-type gen_statem_start_ret() :: {ok, pid()} | ignore | {error, term()}.

-type safe_call_ret(T) :: timeout | {error, noproc | nodedown} | T.

-type states() :: leader | follower | candidate | await_condition.

%% ra_event types
-type ra_event_reject_detail() :: {not_leader, Leader :: maybe(ra_server_id()),
                                   ra_server:command_correlation()}.

-type ra_event_body() ::
    % used for notifying senders of the ultimate fate of their command
    % sent using ra:pipeline_command/3|4
    {applied, [{ra_server:command_correlation(), Reply :: term()}]} |
    {rejected, ra_event_reject_detail()} |
    % used to send message side-effects emitted by the state machine
    {machine, term()}.

-type ra_event() :: {ra_event, Sender :: ra_server_id(), ra_event_body()}.
%% the Sender is the ra process that emitted the ra_event.


-type server_loc() :: ra_server_id() | [ra_server_id()].

-export_type([ra_leader_call_ret/1,
              ra_cmd_ret/0,
              safe_call_ret/1,
              ra_event_reject_detail/0,
              ra_event/0,
              ra_event_body/0]).

-type monitors() ::
    #{pid() | node() => {machine | snapshot_sender, maybe(reference())}}.
%% the ra server proc keeps monitors on behalf of different components
%% the state machine, log and server code. The tag is used to determine
%% which component to dispatch the down to

-record(state, {server_state :: ra_server:ra_server_state(),
                log_id :: unicode:chardata(),
                name :: atom(),
                broadcast_time = ?DEFAULT_BROADCAST_TIME :: non_neg_integer(),
                tick_timeout :: non_neg_integer(),
                monitors = #{} :: monitors(),
                pending_commands = [] :: [{{pid(), any()}, term()}],
                leader_monitor :: reference() | undefined,
                await_condition_timeout :: non_neg_integer(),
                delayed_commands =
                    queue:new() :: queue:queue(ra_server:command())}).

%%%===================================================================
%%% API
%%%===================================================================

-spec start_link(ra_server:ra_server_config()) -> gen_statem_start_ret().
start_link(Config = #{id := Id}) ->
    Name = ra_lib:ra_server_id_to_local_name(Id),
    gen_statem:start_link({local, Name}, ?MODULE, Config, []).

-spec command(server_loc(), ra_command(), timeout()) ->
    ra_cmd_ret().
command(ServerLoc, Cmd, Timeout) ->
    leader_call(ServerLoc, {command, normal, Cmd}, Timeout).

-spec cast_command(ra_server_id(), ra_command()) -> ok.
cast_command(ServerId, Cmd) ->
    gen_statem:cast(ServerId, {command, low, Cmd}).

-spec cast_command(ra_server_id(), ra_command_priority(), ra_command()) -> ok.
cast_command(ServerId, Priority, Cmd) ->
    gen_statem:cast(ServerId, {command, Priority, Cmd}).

-spec query(server_loc(), query_fun(),
            local | consistent | leader, timeout()) ->
    ra_server_proc:ra_leader_call_ret(term())
    | {ok, Reply :: term(), ra_server_id() | not_known}.
query(ServerLoc, QueryFun, local, Timeout) ->
    statem_call(ServerLoc, {local_query, QueryFun}, Timeout);
query(ServerLoc, QueryFun, leader, Timeout) ->
    leader_call(ServerLoc, {local_query, QueryFun}, Timeout);
query(ServerLoc, QueryFun, consistent, Timeout) ->
    leader_call(ServerLoc, {consistent_query, QueryFun}, Timeout).

-spec log_fold(ra_server_id(), fun(), term(), integer()) -> term().
log_fold(ServerId, Fun, InitialState, Timeout) ->
    gen_statem:call(ServerId, {log_fold, Fun, InitialState}, Timeout).

%% used to query the raft state rather than the machine state
-spec state_query(server_loc(), all | members | machine, timeout()) ->
    ra_leader_call_ret(term()).
state_query(ServerLoc, Spec, Timeout) ->
    leader_call(ServerLoc, {state_query, Spec}, Timeout).

-spec trigger_election(ra_server_id(), timeout()) -> ok.
trigger_election(ServerId, Timeout) ->
    gen_statem:call(ServerId, trigger_election, Timeout).

-spec ping(ra_server_id(), timeout()) -> safe_call_ret({pong, states()}).
ping(ServerId, Timeout) ->
    gen_statem_safe_call(ServerId, ping, Timeout).

leader_call(ServerLoc, Msg, Timeout) ->
    statem_call(ServerLoc, {leader_call, Msg}, Timeout).

statem_call(ServerIds, Msg, Timeout)
  when is_list(ServerIds) ->
    multi_statem_call(ServerIds, Msg, [], Timeout);
statem_call(ServerId, Msg, Timeout) ->
    case gen_statem_safe_call(ServerId, Msg, Timeout) of
        {redirect, Leader} ->
            statem_call(Leader, Msg, Timeout);
        {wrap_reply, Reply} ->
            {ok, Reply, ServerId};
        {error, _} = E ->
            E;
        timeout ->
            {timeout, ServerId};
        Reply ->
            Reply
    end.

multi_statem_call([ServerId | ServerIds], Msg, Errs, Timeout) ->
    case statem_call(ServerId, Msg, Timeout) of
        {Tag, _} = E
          when Tag == error orelse Tag == timeout ->
            case ServerIds of
                [] ->
                    {error, {no_more_servers_to_try, [E | Errs]}};
                _ ->
                    multi_statem_call(ServerIds, Msg, [E | Errs], Timeout)
            end;
        Reply ->
            Reply
    end.

%%%===================================================================
%%% gen_statem callbacks
%%%===================================================================

init(Config0 = #{id := Id, cluster_name := ClusterName}) ->
    process_flag(trap_exit, true),
    Config = maps:merge(config_defaults(), Config0),
    #{id := {_, UId, LogId},
      cluster := Cluster} = ServerState = ra_server:init(Config),
    Key = ra_lib:ra_server_id_to_local_name(Id),
						% ensure ra_directory has the new pid
    yes = ra_directory:register_name(UId, self(),
                                     maps:get(parent, Config, undefined), Key,
				     ClusterName),

    % ensure each relevant erlang node is connected
    Peers = maps:keys(maps:remove(Id, Cluster)),
    %% as most messages are sent using noconnect we explicitly attempt to
    %% connect to all relevant nodes
    _ = spawn(fun () ->
                      _ = lists:foreach(fun ({_, Node}) ->
                                                net_kernel:connect_node(Node);
                                            (_) -> node()
                                        end, Peers)
              end),
    TickTime = maps:get(tick_timeout, Config),
    AwaitCondTimeout = maps:get(await_condition_timeout, Config),
    State = #state{server_state = ServerState,
                   log_id = LogId,
                   name = Key,
                   tick_timeout = TickTime,
                   await_condition_timeout = AwaitCondTimeout},
    %% monitor nodes so that we can handle both nodeup and nodedown events
    ok = net_kernel:monitor_nodes(true),
    {ok, recover, State, [{next_event, cast, go}]}.

%% callback mode
callback_mode() -> [state_functions, state_enter].

%%%===================================================================
%%% State functions
%%%===================================================================
recover(enter, OldState, State0) ->
    {State, Actions} = handle_enter(?FUNCTION_NAME, OldState, State0),
    {keep_state, State, Actions};
recover(_EventType, go, State = #state{server_state = ServerState0}) ->
    ServerState = ra_server:recover(ServerState0),
    true = erlang:garbage_collect(),
    %% we have to issue the next_event here so that the recovered state is
    %% only passed through very briefly
    {next_state, recovered, State#state{server_state = ServerState},
     [{next_event, internal, next}]};
recover(_, _, State) ->
    % all other events need to be postponed until we can return
    % `next_event` from init
    {keep_state, State, {postpone, true}}.

%% this is a passthrough state to allow state machines to emit node local
%% effects post recovery
recovered(enter, OldState, State0) ->
    {State, Actions} = handle_enter(?FUNCTION_NAME, OldState, State0),
    {keep_state, State, Actions};
recovered(internal, next, #state{server_state = ServerState} = State) ->
    % New cluster starts should be coordinated and elections triggered
    % explicitly hence if this is a new one we wait here.
    % Else we set an election timer
    Actions = case ra_server:is_new(ServerState) of
                  true ->
                      [];
                  false ->
                      ?DEBUG("~s: is not new, setting election timeout.~n",
                             [log_id(State)]),
                      [election_timeout_action(short, State)]
              end,
    _ = ets:insert(ra_metrics, ra_server:metrics(ServerState)),
    {next_state, follower, State, set_tick_timer(State, Actions)}.

leader(enter, OldState, State0) ->
    {State, Actions} = handle_enter(?FUNCTION_NAME, OldState, State0),
    {keep_state, State, Actions};
leader(EventType, {leader_call, Msg}, State) ->
    %  no need to redirect
    leader(EventType, Msg, State);
leader(EventType, {leader_cast, Msg}, State) ->
    leader(EventType, Msg, State);
leader(EventType, {command, normal, {CmdType, Data, ReplyMode}},
       #state{server_state = ServerState0} = State0) ->
    %% normal priority commands are written immediately
    Cmd = make_command(CmdType, EventType, Data, ReplyMode),
    {leader, ServerState, Effects} =
        ra_server:handle_leader({command, Cmd}, ServerState0),
    {State, Actions} =
        ?HANDLE_EFFECTS(Effects, EventType,
                        State0#state{server_state = ServerState}),
    {keep_state, State, Actions};
leader(EventType, {command, low, {CmdType, Data, ReplyMode}},
       #state{delayed_commands = Delayed} = State0) ->
    %% cache the low priority command until the flush_commands message arrives

    Cmd = make_command(CmdType, EventType, Data, ReplyMode),
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
       #state{server_state = ServerState0,
              delayed_commands = Delayed0} = State0) ->

    {DelQ, Delayed} = queue_take(25, Delayed0),
    %% write a batch of delayed commands
    {leader, ServerState, Effects} =
        ra_server:handle_leader({commands, Delayed}, ServerState0),

    {State, Actions} =
        ?HANDLE_EFFECTS(Effects, EventType,
                        State0#state{server_state = ServerState}),
    case queue:is_empty(DelQ) of
        true ->
            ok;
        false ->
            ok = gen_statem:cast(self(), flush_commands)
    end,
    {keep_state, State#state{delayed_commands = DelQ}, Actions};
leader({call, From}, {local_query, QueryFun},
       #state{server_state = ServerState} = State) ->
    Reply = perform_local_query(QueryFun, id(State), ServerState),
    {keep_state, State, [{reply, From, Reply}]};
leader({call, From}, {state_query, Spec},
       #state{server_state = ServerState} = State) ->
    Reply = {ok, do_state_query(Spec, ServerState), id(State)},
    {keep_state, State, [{reply, From, Reply}]};
leader({call, From}, {consistent_query, QueryFun},
       #state{server_state = ServerState0} = State0) ->
    {leader, ServerState1, Effects} =
        ra_server:handle_leader({consistent_query, From, QueryFun},
                                ServerState0),
    {State1, Actions} =
        ?HANDLE_EFFECTS(Effects, {call, From},
                        State0#state{server_state = ServerState1}),
    {keep_state, State1, Actions};
leader({call, From}, ping, State) ->
    {keep_state, State, [{reply, From, {pong, leader}}]};
leader(info, {node_event, _Node, _Evt}, State) ->
    {keep_state, State, []};
leader(info, {'DOWN', MRef, process, Pid, Info},
       #state{monitors = Monitors0,
              server_state = ServerState0} = State0) ->
    case maps:take(Pid, Monitors0) of
        {{Comp, MRef}, Monitors} ->
            {_, ServerState, Effects} =
                ra_server:handle_down(?FUNCTION_NAME, Comp, Pid, Info,
                                      ServerState0),
            {State, Actions} =
                ?HANDLE_EFFECTS(Effects, cast,
                                State0#state{server_state = ServerState,
                                             monitors = Monitors}),
            {keep_state, State, Actions};
        error ->
            {keep_state, State0, []}
    end;
leader(info, {NodeEvt, Node},
       #state{monitors = Monitors0,
              server_state = ServerState0} = State0)
  when NodeEvt =:= nodedown orelse NodeEvt =:= nodeup ->
    case Monitors0 of
        #{Node := _} ->
            % there is a monitor for the node
            Cmd = make_command('$usr', cast,
                               {NodeEvt, Node}, noreply),
            {leader, ServerState, Effects} =
                ra_server:handle_leader({command, Cmd}, ServerState0),
            {State, Actions} =
                ?HANDLE_EFFECTS(Effects, cast,
                                State0#state{server_state = ServerState}),
            {keep_state, State, Actions};
        _ ->
            {keep_state, State0, []}
    end;
leader(_, tick_timeout, State0) ->
    {State1, RpcEffs} = make_rpcs(State0),
    ServerState = State1#state.server_state,
    Effects = ra_server:tick(ServerState),
    {State, Actions} = ?HANDLE_EFFECTS(RpcEffs ++ Effects, cast, State1),
    _ = ets:insert(ra_metrics, ra_server:metrics(ServerState)),
    true = erlang:garbage_collect(),
    {keep_state, State, set_tick_timer(State, Actions)};
leader({timeout, Name}, machine_timeout,
       #state{server_state = ServerState0} = State0) ->
    % the machine timer timed out, add a timeout message
    Cmd = make_command('$usr', cast, {timeout, Name}, noreply),
    {leader, ServerState, Effects} = ra_server:handle_leader({command, Cmd},
                                                             ServerState0),
    {State, Actions} = ?HANDLE_EFFECTS(Effects, cast,
                                       State0#state{server_state =
                                                    ServerState}),
    {keep_state, State, Actions};
leader({call, From}, trigger_election, State) ->
    {keep_state, State, [{reply, From, ok}]};
leader({call, From}, {log_fold, Fun, Term}, State) ->
    fold_log(From, Fun, Term, State);
leader(EventType, Msg, State0) ->
    case handle_leader(Msg, State0) of
        {leader, State1, Effects1} ->
            {State, Actions} = ?HANDLE_EFFECTS(Effects1, EventType, State1),
            {keep_state, State, Actions};
        {follower, State1, Effects1} ->
            {State, Actions} = ?HANDLE_EFFECTS(Effects1,
                                               EventType,
                                               State1),
            % demonitor when stepping down
            ok = lists:foreach(fun ({_, Ref}) when is_reference(Ref) ->
                                       erlang:demonitor(Ref);
                                   (_) ->
                                       %% the monitor is a node
                                       %% all nodes are always monitored
                                       ok
                               end, maps:values(State#state.monitors)),
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
            case ra_server:is_fully_replicated(State#state.server_state) of
                true ->
                    {stop, {shutdown, delete}, State};
                false ->
                    {next_state, terminating_leader, State, Actions}
            end
    end.

candidate(enter, OldState, State0) ->
    {State, Actions} = handle_enter(?FUNCTION_NAME, OldState, State0),
    {keep_state, State, Actions};
candidate({call, From}, {leader_call, Msg},
          #state{pending_commands = Pending} = State) ->
    {keep_state, State#state{pending_commands = [{From, Msg} | Pending]}};
candidate(cast, {command, _Priority,
                 {_CmdType, _Data, {notify, Corr, Pid}}},
          State) ->
    ok = reject_command(Pid, Corr, State),
    {keep_state, State, []};
candidate({call, From}, {local_query, QueryFun},
          #state{server_state = ServerState} = State) ->
    Reply = perform_local_query(QueryFun, not_known, ServerState),
    {keep_state, State, [{reply, From, Reply}]};
candidate({call, From}, ping, State) ->
    {keep_state, State, [{reply, From, {pong, candidate}}]};
candidate(info, {node_event, _Node, _Evt}, State) ->
    {keep_state, State};
candidate(_, tick_timeout, State0) ->
    State = maybe_persist_last_applied(State0),
    _ = ets:insert(ra_metrics, ra_server:metrics(State#state.server_state)),
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
            {next_state, follower, State,
             % set an election timeout here to ensure an unelectable
             % node doesn't cause an electable one not to trigger
             % another election when not using follower timeouts
             maybe_set_election_timeout(State, Actions)};
        {leader, State1, Effects} ->
            {State2, Actions0} = ?HANDLE_EFFECTS(Effects, EventType, State1),
            State = State2#state{pending_commands = []},
            %% reset the tick timer to avoid it triggering early after a leader
            %% change
            Actions = set_tick_timer(State2, Actions0),
            % inject a bunch of command events to be processed when node
            % becomes leader
            NextEvents = [{next_event, {call, F}, Cmd} || {F, Cmd} <- Pending],
            {next_state, leader, State, Actions ++ NextEvents}
    end.


pre_vote(enter, OldState, State0) ->
    {State, Actions} = handle_enter(?FUNCTION_NAME, OldState, State0),
    {keep_state, State, Actions};
pre_vote({call, From}, {leader_call, Msg},
          State = #state{pending_commands = Pending}) ->
    {keep_state, State#state{pending_commands = [{From, Msg} | Pending]}};
pre_vote(cast, {command, _Priority,
                {_CmdType, _Data, {notify, Corr, Pid}}},
         State) ->
    ok = reject_command(Pid, Corr, State),
    {keep_state, State, []};
pre_vote({call, From}, {local_query, QueryFun},
          #state{server_state = ServerState} = State) ->
    Reply = perform_local_query(QueryFun, not_known, ServerState),
    {keep_state, State, [{reply, From, Reply}]};
pre_vote({call, From}, ping, State) ->
    {keep_state, State, [{reply, From, {pong, pre_vote}}]};
pre_vote(info, {node_event, _Node, _Evt}, State) ->
    {keep_state, State};
pre_vote(_, tick_timeout, State0) ->
    State = maybe_persist_last_applied(State0),
    _ = ets:insert(ra_metrics, ra_server:metrics(State#state.server_state)),
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
            {next_state, follower, State,
             % always set an election timeout here to ensure an unelectable
             % node doesn't cause an electable one not to trigger
             % another election when not using follower timeouts
             [election_timeout_action(long, State) | Actions]};
        {candidate, State1, Effects} ->
            {State, Actions} = ?HANDLE_EFFECTS(Effects, EventType, State1),
            {next_state, candidate, State,
             [election_timeout_action(long, State) | Actions]}
    end.

follower(enter, OldState, State0) ->
    {State, Actions} = handle_enter(?FUNCTION_NAME, OldState, State0),
    {keep_state, State, Actions};
follower({call, From}, {leader_call, Msg}, State) ->
    maybe_redirect(From, Msg, State);
follower(_, {command, Priority, {_CmdType, Data, noreply}},
         State) ->
    % forward to leader
    case leader_id(State) of
        undefined ->
            ?WARN("~s: leader cast - leader not known. "
                  "Command is dropped.~n", [log_id(State)]),
            {keep_state, State, []};
        LeaderId ->
            ?INFO("~s: follower leader cast - redirecting to ~w ~n",
                  [log_id(State), LeaderId]),
            ok = ra:pipeline_command(LeaderId, Data, no_correlation, Priority),
            {keep_state, State, []}
    end;
follower(cast, {command, _Priority,
                {_CmdType, _Data, {notify, Corr, Pid}}},
         State) ->
    ok = reject_command(Pid, Corr, State),
    {keep_state, State, []};
follower({call, From}, {local_query, QueryFun},
         #state{server_state = ServerState} = State) ->
    Leader = case ra_server:leader_id(ServerState) of
                 undefined -> not_known;
                 L -> L
             end,
    Reply = perform_local_query(QueryFun, Leader, ServerState),
    {keep_state, State, [{reply, From, Reply}]};
follower({call, From}, trigger_election, State) ->
    ?DEBUG("~s: election triggered by ~w", [log_id(State), element(1, From)]),
    {keep_state, State, [{reply, From, ok},
                         {next_event, cast, election_timeout}]};
follower({call, From}, ping, State) ->
    {keep_state, State, [{reply, From, {pong, follower}}]};
follower(info, {'DOWN', MRef, process, _Pid, Info},
         #state{leader_monitor = MRef} = State) ->
    ?INFO("~s: Leader monitor down with ~W, setting election timeout~n",
          [log_id(State), Info, 8]),
    case Info of
        noconnection ->
            {keep_state, State#state{leader_monitor = undefined},
             [election_timeout_action(short, State)]};
        _ ->
            {keep_state, State#state{leader_monitor = undefined},
             [election_timeout_action(really_short, State)]}
    end;
follower(info, {'DOWN', MRef, process, Pid, Info},
         #state{monitors = Monitors0, server_state = ServerState0} = State0) ->
    case maps:take(Pid, Monitors0) of
        {{Comp, MRef}, Monitors} ->
            {_, ServerState, Effects} =
                ra_server:handle_down(?FUNCTION_NAME, Comp, Pid, Info,
                                      ServerState0),
            {State, Actions} =
                ?HANDLE_EFFECTS(Effects, cast,
                                State0#state{server_state = ServerState,
                                             monitors = Monitors}),
            {keep_state, State, Actions};
        error ->
            {keep_state, State0, []}
    end;
follower(info, {node_event, Node, down}, State) ->
    case leader_id(State) of
        {_, Node} ->
            ?WARN("~s: Leader node ~w may be down, setting pre-vote timeout",
                  [log_id(State), Node]),
            {keep_state, State, [election_timeout_action(long, State)]};
        _ ->
            {keep_state, State}
    end;
follower(info, {node_event, _Node, up}, State) ->
    case leader_id(State) of
        {_, Node} ->
            ?WARN("~s: Leader node ~w is back up, cancelling pre-vote timeout",
                  [log_id(State), Node]),
            {keep_state, State,
             [{state_timeout, infinity, election_timeout}]};
        _ ->
            {keep_state, State}
    end;
follower(_, tick_timeout, State) ->
    true = erlang:garbage_collect(),
    _ = ets:insert(ra_metrics, ra_server:metrics(State#state.server_state)),
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
            _ = stop_monitor(MRef),
            {next_state, pre_vote, State#state{leader_monitor = undefined},
             [election_timeout_action(long, State) | Actions]};
        {await_condition, State1, Effects} ->
            {State2, Actions} = ?HANDLE_EFFECTS(Effects, EventType, State1),
            State = follower_leader_change(State0, State2),
            {next_state, await_condition, State,
             [{state_timeout, AwaitCondTimeout, await_condition_timeout}
              | Actions]};
        {receive_snapshot, State1, Effects} ->
            {State, Actions} = ?HANDLE_EFFECTS(Effects, EventType, State1),
            {next_state, receive_snapshot, State, Actions};
        {delete_and_terminate, State1, Effects} ->
            {State, Actions} = ?HANDLE_EFFECTS(Effects, EventType, State1),
            {next_state, terminating_follower, State, Actions}
    end.

%% TODO: handle leader down abort snapshot and revert to follower
receive_snapshot(enter, OldState, State0) ->
    {State, Actions} = handle_enter(?FUNCTION_NAME, OldState, State0),
    {keep_state, State,
     [{state_timeout, receive_snapshot_timeout(), receive_snapshot_timeout}
      | Actions]};
receive_snapshot(EventType, Msg, State0) ->
    case handle_receive_snapshot(Msg, State0) of
        {receive_snapshot, State1, Effects} ->
            {State, Actions} = ?HANDLE_EFFECTS(Effects, EventType, State1),
            {keep_state, State,
             [{state_timeout, receive_snapshot_timeout(),
               receive_snapshot_timeout} | Actions]};
        {follower, State1, Effects} ->
            {State2, Actions} = ?HANDLE_EFFECTS(Effects, EventType, State1),
            State = follower_leader_change(State0, State2),
            {next_state, follower, State, Actions}
    end.

terminating_leader(enter, OldState, State0) ->
    {State, Actions} = handle_enter(?FUNCTION_NAME, OldState, State0),
    {keep_state, State, Actions};
terminating_leader(_EvtType, {command, _, _}, State0) ->
    % do not process any further commands
    {keep_state, State0, []};
terminating_leader(EvtType, Msg, State0) ->
    LogName = log_id(State0),
    ?DEBUG("~s: terminating leader received ~W~n", [LogName, Msg, 10]),
    {keep_state, State, Actions} = leader(EvtType, Msg, State0),
    NS = State#state.server_state,
    case ra_server:is_fully_replicated(NS) of
        true ->
            {stop, {shutdown, delete}, State};
        false ->
            ?DEBUG("~s: is not fully replicated after ~W~n",
                   [LogName, Msg, 7]),
            {keep_state, send_rpcs(State), Actions}
    end.

terminating_follower(enter, OldState, State0) ->
    {State, Actions} = handle_enter(?FUNCTION_NAME, OldState, State0),
    {keep_state, State, Actions};
terminating_follower(EvtType, Msg, State0) ->
    % only process ra_log_events
    {keep_state, State, Actions} = follower(EvtType, Msg, State0),
    case ra_server:is_fully_persisted(State#state.server_state) of
        true ->
            {stop, {shutdown, delete}, State};
        false ->
            ?DEBUG("~s: is not fully persisted after ~W~n", [log_id(State),
                                                             Msg, 7]),
            {keep_state, State, Actions}
    end.

await_condition({call, From}, {leader_call, Msg}, State) ->
    maybe_redirect(From, Msg, State);
await_condition({call, From}, {local_query, QueryFun},
                #state{server_state = ServerState} = State) ->
    Reply = perform_local_query(QueryFun, follower, ServerState),
    {keep_state, State, [{reply, From, Reply}]};
await_condition({call, From}, ping, State) ->
    {keep_state, State, [{reply, From, {pong, await_condition}}]};
await_condition({call, From}, trigger_election, State) ->
    {keep_state, State, [{reply, From, ok},
                         {next_event, cast, election_timeout}]};
await_condition(info, {'DOWN', MRef, process, _Pid, _Info},
                State = #state{leader_monitor = MRef}) ->
    ?WARN("~s: Leader monitor down. Setting election timeout.",
          [log_id(State)]),
    {keep_state, State#state{leader_monitor = undefined},
     [election_timeout_action(short, State)]};
await_condition(info, {node_event, Node, down}, State) ->
    case leader_id(State) of
        {_, Node} ->
            ?WARN("~s: Node ~w might be down. Setting election timeout.",
                  [log_id(State), Node]),
            {keep_state, State, [election_timeout_action(long, State)]};
        _ ->
            {keep_state, State}
    end;
await_condition(enter, OldState, State0) ->
    {State, Actions} = handle_enter(?FUNCTION_NAME, OldState, State0),
    {keep_state, State, Actions};
await_condition(EventType, Msg, #state{leader_monitor = MRef} = State0) ->
    case handle_await_condition(Msg, State0) of
        {follower, State1, Effects} ->
            {State, Actions} = ?HANDLE_EFFECTS(Effects, EventType, State1),
            NewState = follower_leader_change(State0, State),
            {next_state, follower, NewState,
             [{state_timeout, infinity, await_condition_timeout} |
              maybe_set_election_timeout(State, Actions)]};
        {pre_vote, State1, Effects} ->
            {State, Actions} = ?HANDLE_EFFECTS(Effects, EventType, State1),
            _ = stop_monitor(MRef),
            {next_state, pre_vote, State#state{leader_monitor = undefined},
             [election_timeout_action(long, State) | Actions]};
        {await_condition, State1, []} ->
            {keep_state, State1, []}
    end.

handle_event(_EventType, EventContent, StateName, State) ->
    ?WARN("~s: handle_event unknown ~P~n", [log_id(State), EventContent, 10]),
    {next_state, StateName, State}.

terminate(Reason, StateName,
          #state{name = Key, server_state = ServerState} = State) ->
    ?INFO("~s: terminating with ~w in state ~w~n",
          [log_id(State), Reason, StateName]),
    UId = uid(State),
    _ = ra_server:terminate(ServerState, Reason),
    Parent = ra_directory:where_is_parent(UId),
    case Reason of
        {shutdown, delete} ->
            catch ra_directory:unregister_name(UId),
            catch ra_log_meta:delete_sync(UId),
            catch ets:delete(ra_state, UId),
            Self = self(),
            %% we have to terminate the child spec from the supervisor as it
            %% wont do this automatically, even for transient children
            %% for simple_one_for_one terminate also removes
            _ = spawn(fun () ->
                              Ref = erlang:monitor(process, Self),
                              receive
                                  {'DOWN', Ref, _, _, _} ->
                                      ok = supervisor:terminate_child(
                                             ra_server_sup_sup,
                                             Parent)
                              after 5000 ->
                                        ok
                              end
                      end),
            ok;


        _ -> ok
    end,
    _ = ets:delete(ra_metrics, Key),
    _ = ets:delete(ra_state, Key),
    ok.

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

format_status(Opt, [_PDict, StateName, #state{server_state = NS}]) ->
    [{id, ra_server:id(NS)},
     {opt, Opt},
     {raft_state, StateName},
     {ra_server_state, ra_server:overview(NS)}
    ].

%%%===================================================================
%%% Internal functions
%%%===================================================================

handle_enter(RaftState, OldRaftState,
             #state{name = Name,
                    server_state = ServerState0} = State) ->
    true = ets:insert(ra_state, {Name, RaftState}),
    {ServerState, Effects} = ra_server:handle_state_enter(RaftState,
                                                          ServerState0),
    case RaftState == leader orelse OldRaftState == leader of
        true ->
            %% ensure transitions from and to leader are logged at a higher
            %% level
            ?NOTICE("~s: ~s -> ~s in term: ~b~n",
                    [log_id(State), OldRaftState, RaftState,
                     current_term(State)]);
        false ->
            ?DEBUG("~s: ~s -> ~s in term: ~b~n",
                   [log_id(State), OldRaftState, RaftState,
                    current_term(State)])
    end,
    handle_effects(RaftState, Effects, cast,
                   State#state{server_state = ServerState}).

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

handle_leader(Msg, #state{server_state = ServerState0} = State0) ->
    case catch ra_server:handle_leader(Msg, ServerState0) of
        {NextState, ServerState, Effects}  ->
            State = State0#state{server_state =
                                 ra_server:persist_last_applied(ServerState)},
            {NextState, State, Effects};
        OtherErr ->
            ?ERR("handle_leader err ~p~n", [OtherErr]),
            exit(OtherErr)
    end.

handle_candidate(Msg, #state{server_state = ServerState0} = State) ->
    {NextState, ServerState, Effects} =
        ra_server:handle_candidate(Msg, ServerState0),
    {NextState, State#state{server_state = ServerState}, Effects}.

handle_pre_vote(Msg, #state{server_state = ServerState0} = State) ->
    {NextState, ServerState, Effects} =
        ra_server:handle_pre_vote(Msg, ServerState0),
    {NextState, State#state{server_state = ServerState}, Effects}.

handle_follower(Msg, #state{server_state = ServerState0} = State0) ->
    {NextState, ServerState, Effects} =
        ra_server:handle_follower(Msg, ServerState0),
    State = State0#state{server_state =
                         ra_server:persist_last_applied(ServerState)},
    {NextState, State, Effects}.

handle_receive_snapshot(Msg, #state{server_state = ServerState0} = State) ->
    {NextState, ServerState, Effects} =
        ra_server:handle_receive_snapshot(Msg, ServerState0),
    {NextState, State#state{server_state = ServerState}, Effects}.

handle_await_condition(Msg, #state{server_state = ServerState0} = State) ->
    {NextState, ServerState, Effects} =
        ra_server:handle_await_condition(Msg, ServerState0),
    {NextState, State#state{server_state = ServerState}, Effects}.

perform_local_query(QueryFun, Leader, #{effective_machine_module := MacMod,
                                        machine_state := MacState,
                                        last_applied := Last,
                                        current_term := Term}) ->
    try ra_machine:query(MacMod, QueryFun, MacState) of
        Result ->
            {ok, {{Last, Term}, Result}, Leader}
    catch
        _:_ = Err ->
            {error, Err}
    end.

handle_effects(RaftState, Effects0, EvtType, State0) ->
    handle_effects(RaftState, Effects0, EvtType, State0, []).
% effect handler: either executes an effect or builds up a list of
% gen_statem 'Actions' to be returned.
handle_effects(RaftState, Effects0, EvtType, State0, Actions0) ->
    {State, Actions} = lists:foldl(
                         fun(Effects, {State, Actions}) when is_list(Effects) ->
                                 handle_effects(RaftState, Effects, EvtType,
                                                State, Actions);
                            (Effect, {State, Actions}) ->
                                 handle_effect(RaftState, Effect, EvtType,
                                               State, Actions)
                         end, {State0, Actions0}, Effects0),
    {State, lists:reverse(Actions)}.

handle_effect(_, {send_rpc, To, Rpc}, _, State0, Actions) ->
    % fully qualified use only so that we can mock it for testing
    % TODO: review / refactor to remove the mod call here
    ?MODULE:send_rpc(To, Rpc),
    {State0, Actions};
handle_effect(_, {next_event, Evt}, EvtType, State, Actions) ->
    {State, [{next_event, EvtType, Evt} |  Actions]};
handle_effect(_, {next_event, _, _} = Next, _, State, Actions) ->
    {State, [Next | Actions]};
handle_effect(_, {send_msg, To, Msg}, _, State, Actions) ->
    %% default is to send without any wrapping
    ok = send(To, Msg),
    {State, Actions};
handle_effect(_, {send_msg, To, Msg, Options}, _, State, Actions) ->
    case parse_send_msg_options(Options) of
        {true, true} ->
            gen_cast(To, wrap_ra_event(id(State), machine, Msg));
        {true, false} ->
            send(To, wrap_ra_event(id(State), machine, Msg));
        {false, true} ->
            gen_cast(To, Msg);
        {false, false} ->
            send(To, Msg)
    end,
    {State, Actions};
handle_effect(RaftState, {aux, Cmd}, _, State, Actions) ->
    %% TODO: thread through state
    {_, ServerState, []} = ra_server:handle_aux(RaftState, cast, Cmd,
                                                State#state.server_state),

    {State#state{server_state = ServerState}, Actions};
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
handle_effect(_, {reply, Reply}, EvtType, _, _) ->
    exit({undefined_reply, Reply, EvtType});
handle_effect(leader, {send_snapshot, To, {SnapState, Id, Term}}, _,
              #state{server_state = SS0,
                     monitors = Monitors} = State0, Actions) ->
    ChunkSize = application:get_env(ra, snapshot_chunk_size,
                                    ?DEFAULT_SNAPSHOT_CHUNK_SIZE),
    %% leader effect only
    Me = self(),
    Pid = spawn(fun () ->
                        try send_snapshots(Me, Id, Term, To,
                                           ChunkSize, SnapState) of
                            _ -> ok
                        catch
                            C:timeout:S ->
                                %% timeout is ok as we've already blocked
                                %% for a while
                                erlang:raise(C, timeout, S);
                            C:E:S ->
                                %% insert an arbitrary pause here as a primitive
                                %% throttling operation as certain errors
                                %% happen quickly
                                ok = timer:sleep(5000),
                                erlang:raise(C, E, S)
                        end
                end),
    MRef = erlang:monitor(process, Pid),
    %% update the peer state so that no pipelined entries are sent during
    %% the snapshot sending phase
    SS = ra_server:update_peer_status(To, {sending_snapshot, Pid}, SS0),
    {State0#state{server_state = SS,
                  monitors = Monitors#{Pid => {snapshot_sender, MRef}}},
                  Actions};
handle_effect(_, {delete_snapshot, Dir,  SnapshotRef}, _, State0, Actions) ->
    %% delete snapshots in separate process
    _ = spawn(fun() ->
                      ra_snapshot:delete(Dir, SnapshotRef)
              end),
    {State0, Actions};
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
handle_effect(RaftState, {release_cursor, Index, MacState}, EvtType,
              #state{server_state = ServerState0} = State0, Actions0) ->
    {ServerState, Effects} = ra_server:update_release_cursor(Index, MacState,
                                                             ServerState0),
    State1 = State0#state{server_state = ServerState},
    handle_effects(RaftState, Effects, EvtType, State1, Actions0);
handle_effect(_, garbage_collection, _EvtType, State, Actions) ->
    true = erlang:garbage_collect(),
    {State, Actions};
handle_effect(_, {monitor, process, Pid}, _,
              #state{monitors = Monitors} = State, Actions) ->
    case Monitors of
        #{Pid := _} ->
            % monitor is already in place - do nothing
            {State, Actions};
        _ ->
            MRef = erlang:monitor(process, Pid),
            {State#state{monitors = Monitors#{Pid => {machine, MRef}}},
             Actions}
    end;
handle_effect(_, {monitor, process, Component, Pid}, _,
              #state{monitors = Monitors} = State, Actions) ->
    case Monitors of
        #{Pid := _} ->
            % monitor is already in place - do nothing
            {State, Actions};
        _ ->
            MRef = erlang:monitor(process, Pid),
            {State#state{monitors = Monitors#{Pid => {Component, MRef}}},
             Actions}
    end;
handle_effect(_, {monitor, node, Node}, _,
              #state{monitors = Monitors} = State, Actions0) ->
    case Monitors of
        #{Node := _} ->
            % monitor is already in place - do nothing
            {State, Actions0};
        _ ->
            %% no need to actually monitor anything as we've always monitoring
            %% all visible nodes
            %% Fake a node event if the node is already connected so that the machine
            %% can discover the current status
            case lists:member(Node, nodes()) of
                true ->
                    %% as effects get evaluated on state enter we cannot use
                    %% next_events
                    self() ! {nodeup, Node},
                    ok;
                false ->
                    self() ! {nodedown, Node},
                    ok
            end,
            {State#state{monitors = Monitors#{Node => undefined}}, Actions0}
    end;
handle_effect(_, {demonitor, process, Pid}, _,
              #state{monitors = Monitors0} = State, Actions) ->
    case maps:take(Pid, Monitors0) of
        {{_, MRef}, Monitors} ->
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
handle_effect(_, {timer, Name, T}, _, State, Actions) ->
    {State, [{{timeout, Name}, T, machine_timeout} | Actions]};
handle_effect(RaftState, {log, Idxs, Fun}, EvtType,
              State = #state{server_state = SS0}, Actions) when is_list(Idxs) ->
    %% Useful to implement a batch send of data obtained from the log.
    %% 1) Retrieve all data from the list of indexes
    {Data, SS} = lists:foldl(fun(Idx, {Data0, Acc0}) ->
                                     case ra_server:read_at(Idx, Acc0) of
                                         {ok, D, Acc} ->
                                             {[D | Data0], Acc};
                                         {error, _} ->
                                             %% this is unrecoverable
                                             exit({failed_to_read_index_for_log_effect,
                                                   Idx})
                                     end
                             end, {[], SS0}, Idxs),
    %% 2) Apply the fun to the list of data as a whole and deal with any effects
    case Fun(lists:reverse(Data)) of
        [] ->
            {State#state{server_state = SS}, Actions};
        Effects ->
            %% recurse with the new effects
            handle_effects(RaftState, Effects, EvtType,
                           State#state{server_state = SS}, Actions)
    end;
handle_effect(_, {mod_call, Mod, Fun, Args}, _,
              State, Actions) ->
    _ = erlang:apply(Mod, Fun, Args),
    {State, Actions}.

send_rpcs(State0) ->
    {State, Rpcs} = make_rpcs(State0),
    % module call so that we can mock
    % TODO: review
    [ok = ?MODULE:send_rpc(To, Rpc) || {send_rpc, To, Rpc} <-  Rpcs],
    State.

make_rpcs(State) ->
    {ServerState, Rpcs} = ra_server:make_rpcs(State#state.server_state),
    {State#state{server_state = ServerState}, Rpcs}.

send_rpc(To, Msg) ->
    % fake gen cast - need to avoid any blocking delays here
    gen_cast(To, Msg).

gen_cast(To, Msg) ->
    send(To, {'$gen_cast', Msg}).

send_ra_event(To, Msg, EvtType, State) ->
    send(To, wrap_ra_event(id(State), EvtType, Msg)).

wrap_ra_event(ServerId, EvtType, Evt) ->
    {ra_event, ServerId, {EvtType, Evt}}.

parse_send_msg_options(ra_event) ->
    {true, false};
parse_send_msg_options(cast) ->
    {false, true};
parse_send_msg_options(Options) when is_list(Options) ->
    {lists:member(ra_event, Options), lists:member(cast, Options)}.

id(#state{server_state = ServerState}) ->
    ra_server:id(ServerState).

log_id(#state{log_id = N}) ->
    N.

uid(#state{server_state = ServerState}) ->
    ra_server:uid(ServerState).

leader_id(#state{server_state = ServerState}) ->
    ra_server:leader_id(ServerState).

current_term(#state{server_state = ServerState}) ->
    ra_server:current_term(ServerState).

process_pending_queries(NewLeader, #state{server_state = ServerState0} = State) ->
    {ServerState, Froms} = ra_server:process_new_leader_queries(ServerState0),
    [_ = gen_statem:reply(F, {redirect, NewLeader})
     || F <- Froms],
    State#state{server_state = ServerState}.

maybe_set_election_timeout(#state{leader_monitor = LeaderMon},
                           Actions) when LeaderMon =/= undefined ->
    % only when a leader is known should we cancel the election timeout
    [{state_timeout, infinity, election_timeout} | Actions];
maybe_set_election_timeout(State, Actions) ->
    [election_timeout_action(short, State) | Actions].

election_timeout_action(really_short, #state{broadcast_time = Timeout}) ->
    T = rand:uniform(Timeout),
    {state_timeout, T, election_timeout};
election_timeout_action(short, #state{broadcast_time = Timeout}) ->
    T = rand:uniform(Timeout * ?DEFAULT_ELECTION_MULT) + Timeout,
    {state_timeout, T, election_timeout};
election_timeout_action(long, #state{broadcast_time = Timeout}) ->
    %% this should be longer than aten detection poll interval
    T = rand:uniform(Timeout * ?DEFAULT_ELECTION_MULT * 2) + 1000,
    {state_timeout, T, election_timeout}.

% sets the tick timer for periodic actions such as sending rpcs to servers
% that are stale to ensure liveness
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
            LeaderNode = ra_lib:ra_server_id_node(NewLeader),
            ok = aten_register(LeaderNode),
            OldLeaderNode = ra_lib:ra_server_id_node(OldLeader),
            ok = aten:unregister(OldLeaderNode),
            % leader has either changed or just been set
            ?INFO("~s: detected a new leader ~w in term ~b~n",
                  [log_id(New), NewLeader, current_term(New)]),
            [ok = gen_statem:reply(From, {redirect, NewLeader})
             || {From, _Data} <- Pending],
            process_pending_queries(NewLeader,
                                    New#state{pending_commands = [],
                                              leader_monitor = MRef})
    end.

aten_register(Node) ->
    case node() of
        Node -> ok;
        _ ->
            aten:register(Node)
    end.

swap_monitor(MRef, L) ->
    stop_monitor(MRef),
    erlang:monitor(process, L).

stop_monitor(undefined) ->
    ok;
stop_monitor(MRef) ->
    erlang:demonitor(MRef),
    ok.

gen_statem_safe_call(ServerId, Msg, Timeout) ->
    try
        gen_statem:call(ServerId, Msg, Timeout)
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
      initial_members => []
     }.

maybe_redirect(From, Msg, #state{pending_commands = Pending,
                                 leader_monitor = LeaderMon} = State) ->
    Leader = leader_id(State),
    case LeaderMon of
        undefined ->
            ?INFO("~s: leader call - leader not known. "
                  "Command will be forwarded once leader is known.~n",
                  [log_id(State)]),
            {keep_state,
             State#state{pending_commands = [{From, Msg} | Pending]}};
        _ when Leader =/= undefined ->
            {keep_state, State, {reply, From, {redirect, Leader}}}
    end.

reject_command(Pid, Corr, State) ->
    LeaderId = leader_id(State),
    case LeaderId of
        undefined ->
            %% don't log these as they may never be resolved
            ok;
        _ ->
            ?INFO("~s: follower received leader command from ~w. "
                  "Rejecting to ~w ~n", [log_id(State), Pid, LeaderId])
    end,
    send_ra_event(Pid, {not_leader, LeaderId, Corr}, rejected, State).

maybe_persist_last_applied(#state{server_state = NS} = State) ->
     State#state{server_state = ra_server:persist_last_applied(NS)}.

send(To, Msg) ->
    % we do not want to block the ra server whilst attempting to set up
    % a TCP connection to a potentially down node.
    case erlang:send(To, Msg, [noconnect, nosuspend]) of
        ok -> ok;
        _ -> ok
    end.


fold_log(From, Fun, Term, State) ->
    case ra_server:log_fold(State#state.server_state, Fun, Term) of
        {ok, Result, ServerState} ->
            {keep_state, State#state{server_state = ServerState},
             [{reply, From, {ok, Result}}]};
        {error, Reason, ServerState} ->
            {keep_state, State#state{server_state = ServerState},
             [{reply, From, {error, Reason}}]}
    end.

maybe_add_election_timeout(election_timeout, Actions, State) ->
    [election_timeout_action(long, State) | Actions];
maybe_add_election_timeout(_, Actions, _) ->
    Actions.

receive_snapshot_timeout() ->
    application:get_env(ra, receive_snapshot_timeout,
                        ?DEFAULT_RECEIVE_SNAPSHOT_TIMEOUT).

send_snapshots(Me, Id, Term, To, ChunkSize, SnapState) ->
    {ok,
     Meta,
     ReadState} = ra_snapshot:begin_read(SnapState),

    RPC = #install_snapshot_rpc{term = Term,
                                leader_id = Id,
                                meta = Meta},

    Result = read_chunks_and_send_rpc(RPC, To, ReadState, 1,
                                      ChunkSize, SnapState),
    ok = gen_statem:cast(Me, {To, Result}).

read_chunks_and_send_rpc(RPC0,
                         To, ReadState0, Num, ChunkSize, SnapState) ->
    {ok, Data, ContState} = ra_snapshot:read_chunk(ReadState0, ChunkSize,
                                                   SnapState),
    ChunkFlag = case ContState of
                    {next, _} -> next;
                    last -> last
                end,
    %% TODO: some of the metadata, e.g. the cluster is redundant in subsequent
    %% rpcs
    RPC1 = RPC0#install_snapshot_rpc{chunk_state = {Num, ChunkFlag},
                                     data = Data},
    Res1 = gen_statem:call(To, RPC1,
                           {dirty_timeout, ?INSTALL_SNAP_RPC_TIMEOUT}),
    case ContState of
        {next, ReadState1} ->
            read_chunks_and_send_rpc(RPC0, To, ReadState1, Num + 1,
                                     ChunkSize, SnapState);
        last ->
            Res1
    end.

make_command(Type, {call, From}, Data, Mode) ->
    Ts = os:system_time(millisecond),
    {Type, #{from => From, ts => Ts}, Data, Mode};
make_command(Type, _, Data, Mode) ->
    Ts = os:system_time(millisecond),
    {Type, #{ts => Ts}, Data, Mode}.
