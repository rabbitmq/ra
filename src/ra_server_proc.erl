%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term Broadcom refers to Broadcom Inc. and/or its subsidiaries.
%%
%% @hidden
-module(ra_server_proc).

-behaviour(gen_statem).

-compile({inline, [handle_raft_state/3]}).


-include("ra.hrl").
-include("ra_server.hrl").

%% State functions
-export([
         post_init/3,
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
         format_status/1,
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
         query/5,
         state_query/3,
         local_state_query/3,
         trigger_election/2,
         ping/2,
         log_fold/4,
         transfer_leadership/3,
         force_shrink_members_to_current_member/1
        ]).

-export([send_rpc/3]).

-ifdef(TEST).
-export([leader_call/3,
         local_call/3]).
-endif.

-define(DEFAULT_BROADCAST_TIME, 100).
-define(DEFAULT_ELECTION_MULT, 5).
-define(TICK_INTERVAL_MS, 1000).
-define(DEFAULT_AWAIT_CONDITION_TIMEOUT, 30000).
%% Utilisation average calculations are all in μs.
-define(INSTALL_SNAP_RPC_TIMEOUT, 120 * 1000).

-define(HANDLE_EFFECTS(Effects, EvtType, State0),
        handle_effects(?FUNCTION_NAME, Effects, EvtType, State0)).

-define(ASYNC_DIST(Node, Send),
        case Node == node() of
            true ->
                Send,
                ok;
            false ->
                %% use async_dist for remote sends
                process_flag(async_dist, true),
                Send,
                process_flag(async_dist, false),
                ok
        end).

-type query_fun() :: ra:query_fun().
-type query_options() :: #{condition => ra:query_condition()}.

-type ra_command() :: {ra_server:command_type(), term(),
                       ra_server:command_reply_mode()}.

-type ra_leader_call_ret(Result) :: {ok, Result, Leader::ra_server_id()} |
                                    {error, term()} |
                                    {timeout, ra_server_id()}.

-type ra_local_call_ret(Result) :: {ok, Result, LocalServer::ra_server_id()} |
                                   {error, term()} |
                                   {timeout, ra_server_id()}.

-type ra_cmd_ret() :: ra_leader_call_ret(term()).

-type gen_statem_start_ret() :: {ok, pid()} | ignore | {error, term()}.

%% ra_event types
-type ra_event_reject_detail() :: {not_leader, Leader :: option(ra_server_id()),
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
              ra_local_call_ret/1,
              ra_cmd_ret/0,
              safe_call_ret/1,
              ra_event_reject_detail/0,
              ra_event/0,
              ra_event_body/0,
              query_options/0]).

%% the ra server proc keeps monitors on behalf of different components
%% the state machine, log and server code. The tag is used to determine
%% which component to dispatch the down to

-record(conf, {log_id :: unicode:chardata(),
               name :: atom(),
               cluster_name :: term(),
               broadcast_time = ?DEFAULT_BROADCAST_TIME :: non_neg_integer(),
               tick_timeout :: non_neg_integer(),
               await_condition_timeout :: non_neg_integer(),
               ra_event_formatter :: undefined | {module(), atom(), [term()]},
               flush_commands_size = ?FLUSH_COMMANDS_SIZE :: non_neg_integer(),
               snapshot_chunk_size = ?DEFAULT_SNAPSHOT_CHUNK_SIZE :: non_neg_integer(),
               receive_snapshot_timeout = ?DEFAULT_RECEIVE_SNAPSHOT_TIMEOUT :: non_neg_integer(),
               install_snap_rpc_timeout :: non_neg_integer(),
               aten_poll_interval = 1000 :: non_neg_integer(),
               counter :: undefined | counters:counters_ref()
              }).

-record(state, {conf :: #conf{},
                server_state :: ra_server:ra_server_state(),
                monitors = ra_monitors:init() :: ra_monitors:state(),
                pending_commands = [] :: [{{pid(), any()}, term()}],
                leader_monitor :: reference() | undefined,
                leader_last_seen :: integer() | undefined,
                low_priority_commands :: ra_ets_queue:state(),
                election_timeout_set = false :: boolean(),
                %% the log index last time gc was forced
                pending_notifys = #{} :: #{pid() => [term()]},
                pending_queries = [] :: [{ra:query_condition(),
                                          gen_statem:from(),
                                          query_fun()}]
               }).

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

-spec cast_command(ra_server_id(), ra_server:command_priority(), ra_command()) -> ok.
cast_command(ServerId, Priority, Cmd) ->
    gen_statem:cast(ServerId, {command, Priority, Cmd}).

-spec query(server_loc(), query_fun(),
            local | consistent | leader,
            query_options(),
            timeout()) ->
    ra_server_proc:ra_leader_call_ret({ra_idxterm(), Reply :: term()})
    | ra_server_proc:ra_leader_call_ret(Reply :: term())
    | {ok, {ra_idxterm(), Reply :: term()}, not_known}.
query(ServerLoc, QueryFun, local, Options, Timeout)
  when map_size(Options) =:= 0 ->
    statem_call(ServerLoc, {local_query, QueryFun}, Timeout);
query(ServerLoc, QueryFun, local, Options, Timeout) ->
    statem_call(ServerLoc, {local_query, QueryFun, Options}, Timeout);
query(ServerLoc, QueryFun, leader, Options, Timeout)
  when map_size(Options) =:= 0 ->
    leader_call(ServerLoc, {local_query, QueryFun}, Timeout);
query(ServerLoc, QueryFun, leader, Options, Timeout) ->
    leader_call(ServerLoc, {local_query, QueryFun, Options}, Timeout);
query(ServerLoc, QueryFun, consistent, _Options, Timeout) ->
    leader_call(ServerLoc, {consistent_query, QueryFun}, Timeout).

-spec log_fold(ra_server_id(), fun(), term(), integer()) -> term().
log_fold(ServerId, Fun, InitialState, Timeout) ->
    gen_statem:call(ServerId, {log_fold, Fun, InitialState}, Timeout).

%% used to query the raft state rather than the machine state
-spec state_query(server_loc(),
                  all |
                  overview |
                  voters |
                  members |
                  members_info |
                  initial_members |
                  machine, timeout()) ->
    ra_leader_call_ret(term()).
state_query(ServerLoc, Spec, Timeout) ->
    leader_call(ServerLoc, {state_query, Spec}, Timeout).

-spec local_state_query(server_loc(),
                        all |
                        overview |
                        voters |
                        members |
                        members_info |
                        initial_members |
                        machine, timeout()) ->
    ra_local_call_ret(term()).
local_state_query(ServerLoc, Spec, Timeout) ->
    local_call(ServerLoc, {state_query, Spec}, Timeout).

-spec trigger_election(ra_server_id(), timeout()) -> ok.
trigger_election(ServerId, Timeout) ->
    gen_statem:call(ServerId, trigger_election, Timeout).

-spec transfer_leadership(ra_server_id(), ra_server_id(), timeout()) ->
    ok | already_leader | {error, term()} | {timeout, ra_server_id()}.
transfer_leadership(ServerId, TargetServerId, Timeout) ->
    leader_call(ServerId, {transfer_leadership, TargetServerId}, Timeout).

-spec force_shrink_members_to_current_member(ra_server_id()) -> ok.
force_shrink_members_to_current_member(ServerId) ->
    gen_statem_safe_call(ServerId, force_member_change, 5000).

-spec ping(ra_server_id(), timeout()) -> safe_call_ret({pong, states()}).
ping(ServerId, Timeout) ->
    gen_statem_safe_call(ServerId, ping, Timeout).

leader_call(ServerLoc, Msg, Timeout) ->
    statem_call(ServerLoc, {leader_call, Msg}, Timeout).

local_call(ServerLoc, Msg, Timeout) ->
    statem_call(ServerLoc, {local_call, Msg}, Timeout).

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
        {Tag, Info} = E
          when Tag == timeout orelse
               (Tag == error andalso
                (Info == noproc orelse
                 Info == nodedown orelse
                 Info == shutdown orelse
                 Info == system_not_started)) ->
            %% these are the retryable errors, any others we consider
            %% genuine errors that a retry will not fix
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

init(#{reply_to := ReplyTo} = Config) ->
    %% we have a reply to key, perform init async
    {ok, post_init, maps:remove(reply_to, Config),
     [{next_event, internal, {go, ReplyTo}}]};
init(Config) ->
    %% no reply_to key, must have been started by an older node run synchronous
    %% init
    State = do_init(Config),
    {ok, recover, State, [{next_event, cast, go}]}.

do_init(#{id := Id,
          cluster_name := ClusterName} = Config0) ->
    Key = ra_lib:ra_server_id_to_local_name(Id),
    true = ets:insert(ra_state, {Key, init, unknown}),
    process_flag(trap_exit, true),
    Config = #{counter := Counter,
               system_config := #{names := Names} = SysConf} = maps:merge(config_defaults(Id),
                                                      Config0),
    MsgQData = maps:get(message_queue_data, SysConf, off_heap),
    MinBinVheapSize = maps:get(server_min_bin_vheap_size, SysConf,
                               ?MIN_BIN_VHEAP_SIZE),
    MinHeapSize = maps:get(server_min_heap_size, SysConf, ?MIN_BIN_VHEAP_SIZE),
    process_flag(message_queue_data, MsgQData),
    process_flag(min_bin_vheap_size, MinBinVheapSize),
    process_flag(min_heap_size, MinHeapSize),
    %% wait for wal for a bit before initialising the server state and log
    #{cluster := Cluster} = ServerState = ra_server:init(Config),
    LogId = ra_server:log_id(ServerState),
    UId = ra_server:uid(ServerState),
    % ensure ra_directory has the new pid
    #{names := Names} = SysConf,
    ok = ra_directory:register_name(Names, UId, self(),
                                    maps:get(parent, Config, undefined), Key,
                                    ClusterName),

    % ensure each relevant erlang node is connected
    PeerNodes = [PeerNode ||
                 {_, PeerNode} <- maps:keys(maps:remove(Id, Cluster))],
    case PeerNodes -- nodes() of
        [] ->
            %% all peer nodes are connected
            ok;
        DisconnectedNodes ->
            %% as most messages are sent using noconnect we explicitly attempt to
            %% connect to all relevant nodes
            _ = spawn(fun () ->
                              [net_kernel:connect_node(N)
                               || N <- DisconnectedNodes]
                      end),
            ok
    end,
    TickTime = maps:get(tick_timeout, Config),
    InstallSnapRpcTimeout = maps:get(install_snap_rpc_timeout, Config),
    AwaitCondTimeout = maps:get(await_condition_timeout, Config),
    RaEventFormatterMFA = maps:get(ra_event_formatter, Config, undefined),
    FlushCommandsSize = maps:get(low_priority_commands_flush_size, SysConf,
                                 ?FLUSH_COMMANDS_SIZE),
    SnapshotChunkSize = maps:get(snapshot_chunk_size, SysConf,
                                 ?DEFAULT_SNAPSHOT_CHUNK_SIZE),
    ReceiveSnapshotTimeout = maps:get(receive_snapshot_timeout, SysConf,
                                      ?DEFAULT_RECEIVE_SNAPSHOT_TIMEOUT),
    AtenPollInt = application:get_env(aten, poll_interval, 1000),
    State = #state{conf = #conf{log_id = LogId,
                                cluster_name = ClusterName,
                                name = Key,
                                tick_timeout = TickTime,
                                await_condition_timeout = AwaitCondTimeout,
                                ra_event_formatter = RaEventFormatterMFA,
                                flush_commands_size = FlushCommandsSize,
                                snapshot_chunk_size = SnapshotChunkSize,
                                install_snap_rpc_timeout = InstallSnapRpcTimeout,
                                receive_snapshot_timeout = ReceiveSnapshotTimeout,
                                aten_poll_interval = AtenPollInt,
                                counter = Counter},
                   low_priority_commands = ra_ets_queue:new(),
                   server_state = ServerState},
    ok = net_kernel:monitor_nodes(true, [nodedown_reason]),
    State.

%% callback mode
callback_mode() -> [state_functions, state_enter].

%%%===================================================================
%%% State functions
%%%===================================================================

post_init(enter, _OldState, State) ->
    {keep_state, State, []};
post_init(internal, {go, {ReplyToRef, ReplyToPid}}, Config) ->
    State = do_init(Config),
    ReplyToPid ! {ReplyToRef, ok},
    {next_state, recover, State, [{next_event, internal, go}]}.

recover(enter, OldState, State0) ->
    {State, Actions} = handle_enter(?FUNCTION_NAME, OldState, State0),
    {keep_state, State, Actions};
recover(internal, go, State = #state{server_state = ServerState0}) ->
    ServerState = ra_server:recover(ServerState0),
    incr_counter(State#state.conf, ?C_RA_SRV_GCS, 1),
    %% we have to issue the next_event here so that the recovered state is
    %% only passed through very briefly
    next_state(recovered, State#state{server_state = ServerState},
               [{next_event, internal, next}]);
recover(_, _, State) ->
    % all other events need to be postponed until we can return
    % `next_event` from init
    {keep_state, State, {postpone, true}}.

%% this is a passthrough state to allow state machines to emit node local
%% effects post recovery
recovered(enter, OldState, State0) ->
    {State, Actions} = handle_enter(?FUNCTION_NAME, OldState, State0),
    ok = record_cluster_change(State),
    {keep_state, State, Actions};
recovered(internal, next, #state{server_state = ServerState} = State) ->
    true = erlang:garbage_collect(),
    _ = ets:insert(ra_metrics, ra_server:metrics(ServerState)),
    next_state(follower, State, set_tick_timer(State, [])).

leader(enter, OldState, #state{low_priority_commands = Delayed0} = State0) ->
    {State, Actions} = handle_enter(?FUNCTION_NAME, OldState, State0),

    Delayed = case OldState of
                  await_condition ->
                      %% if we're returning from await_condition we may still
                      %% have valid delayed commands to schedule
                      schedule_command_flush(Delayed0),
                      Delayed0;
                  _ ->
                      %% for any other state it is best to just reset the
                      %% delayed commands
                      ra_ets_queue:reset(Delayed0)
              end,

    ok = record_cluster_change(State),
    {keep_state, State#state{leader_last_seen = undefined,
                             pending_notifys = #{},
                             low_priority_commands = Delayed,
                             election_timeout_set = false}, Actions};
leader(EventType, {leader_call, Msg}, State) ->
    %  no need to redirect
    leader(EventType, Msg, State);
leader(EventType, {local_call, Msg}, State) ->
    leader(EventType, Msg, State);
leader(EventType, {leader_cast, Msg}, State) ->
    leader(EventType, Msg, State);
leader(EventType, {command, normal, {CmdType, Data, ReplyMode}},
       #state{conf = Conf} = State0) ->
    case validate_reply_mode(ReplyMode) of
        ok ->
            %% normal priority commands are written immediately
            Cmd = make_command(CmdType, EventType, Data, ReplyMode),
            {NextState, State1, Effects} = handle_leader({command, Cmd}, State0),
            {State, Actions} = ?HANDLE_EFFECTS(Effects, EventType, State1),
            case NextState of
                leader ->
                    {keep_state, State, Actions};
                _ ->
                    next_state(NextState, State, Actions)
            end;
        Error ->
            ok = incr_counter(Conf, ?C_RA_SRV_INVALID_REPLY_MODE_COMMANDS, 1),
            case EventType of
                {call, From} ->
                    {keep_state, State0, [{reply, From, Error}]};
                _ ->
                    {keep_state, State0, []}
            end
    end;
leader(EventType, {command, low, {'$usr', Data, ReplyMode}},
       #state{conf = Conf,
              low_priority_commands = Delayed} = State0) ->
    %% only user commands can be low priority
    case validate_reply_mode(ReplyMode) of
        ok ->
            %% cache the low priority command until the flush_commands message
            %% arrives
            Cmd = make_command('$usr', EventType, Data, ReplyMode),
            %% if there are no prior delayed commands
            %% (and thus no action queued to do so)
            %% queue a state timeout to flush them
            %% We use a cast to ourselves instead of a zero timeout as we want
            %% to get onto the back of the erlang mailbox not just the current
            %% gen_statem event buffer.
            case ra_ets_queue:len(Delayed) of
                0 ->
                    ok = gen_statem:cast(self(), flush_commands);
                _ ->
                    ok
            end,
            State = State0#state{low_priority_commands =
                                     ra_ets_queue:in(Cmd, Delayed)},
            {keep_state, State, []};
        Error ->
            ok = incr_counter(Conf, ?C_RA_SRV_INVALID_REPLY_MODE_COMMANDS, 1),
            case EventType of
                {call, From} ->
                    {keep_state, State0, [{reply, From, Error}]};
                _ ->
                    {keep_state, State0, []}
            end
    end;
leader(EventType, {command, low, Cmd}, #state{} = State) ->
    %% non user low priority commands are upgraded to normal priority
    leader(EventType, {command, normal, Cmd}, State);
leader(EventType, {aux_command, Cmd}, State0) ->
    {_, ServerState, Effects} = ra_server:handle_aux(?FUNCTION_NAME, EventType,
                                                     Cmd, State0#state.server_state),
    {State, Actions} =
        ?HANDLE_EFFECTS(Effects, EventType,
                        State0#state{server_state = ServerState}),
    {keep_state, State#state{server_state = ServerState}, Actions};
leader(EventType, flush_commands,
       #state{conf = #conf{flush_commands_size = Size},
              low_priority_commands = Delayed0} = State0) ->
    {Commands, Delayed} = ra_ets_queue:take(Size, Delayed0),
    %% write a batch of delayed commands
    {NextState, State1, Effects} = handle_leader({commands, Commands}, State0),
    State2 = State1#state{low_priority_commands = Delayed},
    {State, Actions} = ?HANDLE_EFFECTS(Effects, EventType, State2),

    case NextState of
        leader ->
            schedule_command_flush(Delayed),
            {keep_state, State#state{low_priority_commands = Delayed}, Actions};
        _ ->
            next_state(NextState, State, Actions)
    end;
leader({call, _From} = EventType, {local_query, QueryFun}, State) ->
    leader(EventType, {local_query, QueryFun, #{}}, State);
leader({call, From} = EventType, {local_query, QueryFun, Options}, State) ->
    perform_or_delay_local_query(
      leader, EventType, From, QueryFun, Options, State);
leader({call, From}, {state_query, Spec}, State) ->
    Reply = {ok, do_state_query(Spec, State), id(State)},
    {keep_state, State, [{reply, From, Reply}]};
leader({call, From}, {consistent_query, QueryFun},
       #state{conf = Conf,
              server_state = ServerState0} = State0) ->
    {leader, ServerState1, Effects} =
        ra_server:handle_leader({consistent_query, From, QueryFun},
                                ServerState0),
    incr_counter(Conf, ?C_RA_SRV_CONSISTENT_QUERIES, 1),
    {State1, Actions} =
        ?HANDLE_EFFECTS(Effects, {call, From},
                        State0#state{server_state = ServerState1}),
    {keep_state, State1, Actions};
leader({call, From}, ping, State) ->
    {keep_state, State, [{reply, From, {pong, leader}}]};
leader(info, {node_event, _Node, _Evt}, State) ->
    {keep_state, State, []};
leader(info, {'DOWN', _MRef, process, Pid, Info}, State0) ->
    handle_process_down(Pid, Info, ?FUNCTION_NAME, State0);
leader(info, {Status, Node, InfoList}, State0)
  when Status =:= nodedown orelse
       Status =:= nodeup ->
    handle_node_status_change(Node, Status, InfoList, ?FUNCTION_NAME, State0);
leader(info, {update_peer, PeerId, Update}, State0) ->
    State = update_peer(PeerId, Update, State0),
    {keep_state, State, []};
leader(_, tick_timeout, State0) ->
    {State1, RpcEffs} = make_rpcs(State0),
    ServerState0 = State1#state.server_state,
    Effects = ra_server:tick(ServerState0),
    ServerState = ra_server:log_tick(ServerState0),
    {State2, Actions} = ?HANDLE_EFFECTS(RpcEffs ++ Effects ++ [{aux, tick}],
                                        cast, State1#state{server_state = ServerState}),
    %% try sending any pending applied notifications again
    State = send_applied_notifications(State2, #{}),
    {keep_state, handle_tick_metrics(State),
     set_tick_timer(State, Actions)};
leader({timeout, Name}, machine_timeout, State0) ->
    % the machine timer timed out, add a timeout message
    Cmd = make_command('$usr', cast, {timeout, Name}, noreply),
    {leader, State1, Effects} = handle_leader({command, Cmd}, State0),
    {State, Actions} = ?HANDLE_EFFECTS(Effects, cast, State1),
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
                    next_state(terminating_leader, State, Actions)
            end;
        {await_condition, State1, Effects1} ->
            {State, Actions} = ?HANDLE_EFFECTS(Effects1, EventType, State1),
            ?DEBUG_IF(is_command(Msg), "~ts: postponing ~0P",
                      [log_id(State0), Msg, 10]),
            next_state(await_condition, State,
                       [{postpone, is_command(Msg)} | Actions]);
        {NextState, State1, Effects1} ->
            {State, Actions} = ?HANDLE_EFFECTS(Effects1, EventType, State1),
            next_state(NextState, State, Actions)
    end.

candidate(enter, OldState, State0) ->
    {State1, Actions0} = handle_enter(?FUNCTION_NAME, OldState, State0),
    {State, Actions} = maybe_set_election_timeout(short, State1, Actions0),
    {keep_state, State, Actions};
candidate({call, From}, {leader_call, Msg},
          #state{pending_commands = Pending} = State) ->
    {keep_state, State#state{pending_commands = [{From, Msg} | Pending]}};
candidate(EventType, {local_call, Msg}, State) ->
    candidate(EventType, Msg, State);
candidate(cast, {command, _Priority,
                 {_CmdType, _Data, {notify, Corr, Pid}}},
          State) ->
    _ = reject_command(Pid, Corr, State),
    {keep_state, State, []};
candidate({call, _From} = EventType, {local_query, QueryFun}, State) ->
    candidate(EventType, {local_query, QueryFun, #{}}, State);
candidate({call, From} = EventType, {local_query, QueryFun, Options}, State) ->
    perform_or_delay_local_query(
      candidate, EventType, From, QueryFun, Options, State);
candidate({call, From}, {state_query, Spec}, State) ->
    Reply = {ok, do_state_query(Spec, State), id(State)},
    {keep_state, State, [{reply, From, Reply}]};
candidate({call, From}, ping, State) ->
    {keep_state, State, [{reply, From, {pong, candidate}}]};
candidate(info, {node_event, _Node, _Evt}, State) ->
    {keep_state, State};
candidate(_, tick_timeout, State0) ->
    State = maybe_persist_last_applied(State0),
    {keep_state, handle_tick_metrics(State), set_tick_timer(State, [])};
candidate({call, From}, trigger_election, State) ->
    {keep_state, State, [{reply, From, ok}]};
candidate(EventType, Msg, State0) ->
    case handle_candidate(Msg, State0) of
        {candidate, State1, Effects} ->
            {State2, Actions0} = ?HANDLE_EFFECTS(Effects, EventType, State1),
            {State, Actions} = maybe_set_election_timeout(medium, State2, Actions0),
            {keep_state, State, Actions};
        {follower, State1, Effects} ->
            {State, Actions} = ?HANDLE_EFFECTS(Effects, EventType, State1),
            next_state(follower, State, Actions);
        {leader, State1, Effects} ->
            {State, Actions0} = ?HANDLE_EFFECTS(Effects, EventType, State1),
            %% reset the tick timer to avoid it triggering early after a leader
            %% change
            Actions = set_tick_timer(State, Actions0),
            next_state(leader, State, Actions)
    end.

pre_vote(enter, OldState, #state{leader_monitor = MRef} = State0) ->
    _ = stop_monitor(MRef),
    {State1, Actions0} = handle_enter(?FUNCTION_NAME, OldState, State0),
    {State, Actions} = maybe_set_election_timeout(long, State1, Actions0),
    {keep_state, State#state{leader_monitor = undefined}, Actions};
pre_vote({call, From}, {leader_call, Msg},
          State = #state{pending_commands = Pending}) ->
    {keep_state, State#state{pending_commands = [{From, Msg} | Pending]}};
pre_vote(EventType, {local_call, Msg}, State) ->
    pre_vote(EventType, Msg, State);
pre_vote(cast, {command, _Priority,
                {_CmdType, _Data, {notify, Corr, Pid}}},
         State) ->
    _ = reject_command(Pid, Corr, State),
    {keep_state, State, []};
pre_vote({call, _From} = EventType, {local_query, QueryFun}, State) ->
    pre_vote(EventType, {local_query, QueryFun, #{}}, State);
pre_vote({call, From} = EventType, {local_query, QueryFun, Options}, State) ->
    perform_or_delay_local_query(
      pre_vote, EventType, From, QueryFun, Options, State);
pre_vote({call, From}, {state_query, Spec}, State) ->
    Reply = {ok, do_state_query(Spec, State), id(State)},
    {keep_state, State, [{reply, From, Reply}]};
pre_vote({call, From}, ping, State) ->
    {keep_state, State, [{reply, From, {pong, pre_vote}}]};
pre_vote(info, {node_event, _Node, _Evt}, State) ->
    {keep_state, State};
pre_vote(info, {Status, Node, InfoList}, State0)
  when Status =:= nodedown orelse Status =:= nodeup ->
    handle_node_status_change(Node, Status, InfoList, ?FUNCTION_NAME, State0);
pre_vote(info, {'DOWN', _MRef, process, Pid, Info}, State0) ->
    handle_process_down(Pid, Info, ?FUNCTION_NAME, State0);
pre_vote(_, tick_timeout, State0) ->
    State = maybe_persist_last_applied(State0),
    {keep_state, handle_tick_metrics(State), set_tick_timer(State, [])};
pre_vote({call, From}, trigger_election, State) ->
    {keep_state, State, [{reply, From, ok}]};
pre_vote(EventType, Msg, State0) ->
    case handle_pre_vote(Msg, State0) of
        {pre_vote, State1, Effects} ->
            {State2, Actions0} = ?HANDLE_EFFECTS(Effects, EventType, State1),
            {State, Actions} = maybe_set_election_timeout(long, State2, Actions0),
            {keep_state, State, Actions};
        {follower, State1, Effects} ->
            {State, Actions} = ?HANDLE_EFFECTS(Effects, EventType, State1),
            next_state(follower, State, Actions);
        {candidate, State1, Effects} ->
            {State, Actions} = ?HANDLE_EFFECTS(Effects, EventType, State1),
            next_state(candidate, State, Actions)
    end.

follower(enter, OldState, #state{low_priority_commands = Delayed,
                                 server_state = ServerState} = State0) ->
    %% New cluster starts should be coordinated and elections triggered
    %% explicitly hence if this is a new one we wait here.
    %% Else we set an election timer
    %% Set the timeout length based on the previous state
    TimeoutLen = case OldState of
                     recovered -> short;
                     _ -> long
                 end,
    {State1, Actions0} = handle_enter(?FUNCTION_NAME, OldState, State0),
    {State, Actions} = case ra_server:is_new(ServerState) of
                           true ->
                               {State1, Actions0};
                           false ->
                               ?DEBUG("~ts: is not new, setting "
                                      "election timeout.",
                                      [log_id(State0)]),
                               maybe_set_election_timeout(TimeoutLen, State1,
                                                          Actions0)
                       end,
    Monitors = ra_monitors:remove_all(machine, State#state.monitors),
    {keep_state, State#state{low_priority_commands = ra_ets_queue:reset(Delayed),
                             monitors = Monitors}, Actions};
follower({call, From}, {leader_call, Msg}, State) ->
    maybe_redirect(From, Msg, State);
follower(EventType, {local_call, Msg}, State) ->
    follower(EventType, Msg, State);
follower(_, {command, Priority, {_CmdType, Data, noreply}},
         State) ->
    % forward to leader
    case leader_id(State) of
        undefined ->
            ?WARN("~ts: leader cast - leader not known. "
                  "Command is dropped.", [log_id(State)]),
            {keep_state, State, []};
        LeaderId ->
            ?DEBUG("~ts: follower leader cast - redirecting to ~w ",
                   [log_id(State), LeaderId]),
            ok = ra:pipeline_command(LeaderId, Data, no_correlation, Priority),
            {keep_state, State, []}
    end;
follower(cast, {command, _Priority,
                {_CmdType, _Data, {notify, Corr, Pid}}},
         State) ->
    _ = reject_command(Pid, Corr, State),
    {keep_state, State, []};
follower({call, _From} = EventType, {local_query, QueryFun}, State) ->
    follower(EventType, {local_query, QueryFun, #{}}, State);
follower({call, From} = EventType, {local_query, QueryFun, Options}, State) ->
    perform_or_delay_local_query(
      follower, EventType, From, QueryFun, Options, State);
follower({call, From}, {state_query, Spec}, State) ->
    Reply = {ok, do_state_query(Spec, State), id(State)},
    {keep_state, State, [{reply, From, Reply}]};
follower(EventType, {aux_command, Cmd}, State0) ->
    {_, ServerState, Effects} = ra_server:handle_aux(?FUNCTION_NAME, EventType, Cmd,
                                                     State0#state.server_state),
    {State, Actions} =
        ?HANDLE_EFFECTS(Effects, EventType,
                        State0#state{server_state = ServerState}),
    {keep_state, State#state{server_state = ServerState}, Actions};
follower({call, From}, trigger_election, State) ->
    ?DEBUG("~ts: election triggered by ~w", [log_id(State), element(1, From)]),
    {keep_state, State, [{reply, From, ok},
                         {next_event, cast, election_timeout}]};
follower({call, From}, ping, State) ->
    {keep_state, State, [{reply, From, {pong, follower}}]};
follower(info, {'DOWN', MRef, process, _Pid, Info},
         #state{leader_monitor = MRef} = State0) ->
    ?INFO("~ts: Leader monitor down with ~W, setting election timeout",
          [log_id(State0), Info, 8]),
    %% If the DOWN reason is something else than `noconnection', we know that
    %% the leader process is really gone. We want to clear the leader ID we
    %% know at this point, while a new election is running.
    %%
    %% This is to make sure that `follower_leader_change()' won't create a
    %% useless monitor and more importantly, it won't redirect pending
    %% commands to that old leader. This would cause the commands callers to
    %% get a `noproc' error or a timeout.
    State1 = case Info of
                 noconnection ->
                     State0;
                 _ ->
                     clear_leader_id(State0)
             end,
    TimeoutLen = case Info of
                     noconnection ->
                         short;
                     _ ->
                         %% if it isn't a network related error
                         %% set the shortest timeout
                         really_short
                 end,
    {State, Actions} = maybe_set_election_timeout(TimeoutLen, State1, []),
    {keep_state, State#state{leader_monitor = undefined}, Actions};
follower(info, {'DOWN', _MRef, process, Pid, Info}, State0) ->
    handle_process_down(Pid, Info, ?FUNCTION_NAME, State0);
follower(info, {node_event, Node, down}, State0) ->
    case leader_id(State0) of
        {_, Node} ->
            ?DEBUG("~ts: Leader node ~w may be down, setting pre-vote timeout",
                   [log_id(State0), Node]),
            {State, Actions} = maybe_set_election_timeout(long, State0, []),
            {keep_state, State, Actions};
        _ ->
            {keep_state, State0, []}
    end;
follower(info, {node_event, Node, up}, State) ->
    case leader_id(State) of
        {_, Node} when State#state.election_timeout_set ->
            ?DEBUG("~ts: Leader node ~w is back up, cancelling pre-vote timeout",
                   [log_id(State), Node]),
            {keep_state,
             State#state{election_timeout_set = false},
             [{state_timeout, infinity, election_timeout}]};
        _ ->
            {keep_state, State, []}
    end;
follower(info, {Status, Node, InfoList}, State0)
  when Status =:= nodedown orelse Status =:= nodeup ->
    handle_node_status_change(Node, Status, InfoList, ?FUNCTION_NAME, State0);
follower(_, tick_timeout, #state{server_state = ServerState0} = State0) ->
    ServerState = ra_server:log_tick(ServerState0),
    {State, Actions} = ?HANDLE_EFFECTS([{aux, tick}], cast,
                                       State0#state{server_state = ServerState}),
    {keep_state, handle_tick_metrics(State),
     set_tick_timer(State, Actions)};
follower({call, From}, {log_fold, Fun, Term}, State) ->
    fold_log(From, Fun, Term, State);
follower(EventType, Msg, #state{conf = #conf{name = Name},
                                server_state = SS0} = State0) ->
    case handle_follower(Msg, State0) of
        {follower, State1, Effects} ->
            {State2, Actions} = ?HANDLE_EFFECTS(Effects, EventType, State1),
            State = #state{server_state = SS} = follower_leader_change(State0, State2),
            Membership0 = ra_server:get_membership(SS0),
            case ra_server:get_membership(SS) of
                Membership0 ->
                    ok;
                Membership ->
                    true = ets:update_element(ra_state, Name, {3, Membership})
            end,
            {keep_state, State, Actions};
        {pre_vote, State1, Effects} ->
            {State, Actions} = ?HANDLE_EFFECTS(Effects, EventType, State1),
            next_state(pre_vote, State, Actions);
        {await_condition, State1, Effects} ->
            {State2, Actions} = ?HANDLE_EFFECTS(Effects, EventType, State1),
            State = follower_leader_change(State0, State2),
            next_state(await_condition, State, Actions);
        {receive_snapshot, State1, Effects} ->
            {State, Actions} = ?HANDLE_EFFECTS(Effects, EventType, State1),
            next_state(receive_snapshot, State, Actions);
        {delete_and_terminate, State1, Effects} ->
            {State, Actions} = ?HANDLE_EFFECTS(Effects, EventType, State1),
            next_state(terminating_follower, State, Actions)
    end.

%% TODO: handle leader down abort snapshot and revert to follower
receive_snapshot(enter, OldState, State0 = #state{conf = Conf}) ->
    #conf{receive_snapshot_timeout = ReceiveSnapshotTimeout} = Conf,
    {State, Actions} = handle_enter(?FUNCTION_NAME, OldState, State0),
    {keep_state, State,
     [{state_timeout, ReceiveSnapshotTimeout, receive_snapshot_timeout}
      | Actions]};
receive_snapshot(_, tick_timeout, State0) ->
    {keep_state, State0, set_tick_timer(State0, [])};
receive_snapshot({call, From}, {leader_call, Msg}, State) ->
    maybe_redirect(From, Msg, State);
receive_snapshot(EventType, {local_call, Msg}, State) ->
    receive_snapshot(EventType, Msg, State);
receive_snapshot({call, _From} = EventType, {local_query, QueryFun}, State) ->
    receive_snapshot(EventType, {local_query, QueryFun, #{}}, State);
receive_snapshot({call, From} = EventType, {local_query, QueryFun, Options},
                 State) ->
    perform_or_delay_local_query(
      receive_snapshot, EventType, From, QueryFun, Options, State);
receive_snapshot({call, From}, {state_query, Spec}, State) ->
    Reply = {ok, do_state_query(Spec, State), id(State)},
    {keep_state, State, [{reply, From, Reply}]};
receive_snapshot(EventType, Msg, State0) ->
    case handle_receive_snapshot(Msg, State0) of
        {receive_snapshot, State1, Effects} ->
            {#state{conf = Conf} = State, Actions} =
                ?HANDLE_EFFECTS(Effects, EventType, State1),
            TimeoutActions = case Msg of
                                 #install_snapshot_rpc{} ->
                                     %% Reset timeout only on receive snapshot progress.
                                     [{state_timeout, Conf#conf.receive_snapshot_timeout,
                                                      receive_snapshot_timeout}];
                                 _ ->
                                     []
                             end,
            {keep_state, State, TimeoutActions ++ Actions};
        {follower, State1, Effects} ->
            {State2, Actions} = ?HANDLE_EFFECTS(Effects, EventType, State1),
            State = follower_leader_change(State0, State2),
            next_state(follower, State, Actions)
    end.

terminating_leader(enter, OldState, State0) ->
    {State, Actions} = handle_enter(?FUNCTION_NAME, OldState, State0),
    {keep_state, State, Actions};
terminating_leader(_EvtType, {command, _, _}, State0) ->
    % do not process any further commands
    {keep_state, State0, []};
terminating_leader(EvtType, Msg, State0) ->
    LogName = log_id(State0),
    ?DEBUG("~ts: terminating leader received ~W", [LogName, Msg, 10]),
    {State, Actions} = case leader(EvtType, Msg, State0) of
                           {next_state, terminating_leader, S, A} ->
                               {S, A};
                           {keep_state, S, A} ->
                               {S, A};
                           {stop, {shutdown, delete}, S} ->
                               {S, []}
                       end,
    NS = State#state.server_state,
    case ra_server:is_fully_replicated(NS) of
        true ->
            {stop, {shutdown, delete}, State};
        false ->
            ?DEBUG("~ts: is not fully replicated after ~W",
                   [LogName, Msg, 7]),
            {keep_state, send_rpcs(State), Actions}
    end.

terminating_follower(enter, OldState, State0) ->
    {State, Actions} = handle_enter(?FUNCTION_NAME, OldState, State0),
    {keep_state, State, Actions};
terminating_follower(EvtType, Msg, State0) ->
    % only process ra_log_events
    LogName = log_id(State0),
    ?DEBUG("~ts: terminating follower received ~W", [LogName, Msg, 10]),
    {State, Actions} = case follower(EvtType, Msg, State0) of
                           {next_state, terminating_follower, S, A} ->
                               {S, A};
                           {next_state, NextState, S, A} ->
                               ?DEBUG("~ts: terminating follower requested state '~s'"
                                      " - remaining in current state",
                                      [LogName, NextState]),
                               {S, A};
                           {keep_state, S, A} ->
                               {S, A}
                       end,
    case ra_server:is_fully_persisted(State#state.server_state) of
        true ->
            {stop, {shutdown, delete}, State};
        false ->
            ?DEBUG("~ts: is not fully persisted after ~W",
                   [log_id(State), Msg, 7]),
            {keep_state, State, Actions}
    end.

await_condition(enter, OldState, #state{conf = Conf,
                                       server_state = ServerState} = State0) ->
    {State, Actions0} = handle_enter(?FUNCTION_NAME, OldState, State0),
    Timeout = ra_server:get_condition_timeout(ServerState,
                                              Conf#conf.await_condition_timeout),
    Actions = [{state_timeout, Timeout, await_condition_timeout} | Actions0],
    {keep_state, State, Actions};
await_condition({call, From}, {leader_call, Msg}, State) ->
    maybe_redirect(From, Msg, State);
await_condition(EventType, {local_call, Msg}, State) ->
    await_condition(EventType, Msg, State);
await_condition({call, _From} = EventType, {local_query, QueryFun}, State) ->
    await_condition(EventType, {local_query, QueryFun, #{}}, State);
await_condition({call, From} = EventType, {local_query, QueryFun, Options},
                State) ->
    perform_or_delay_local_query(
      await_condition, EventType, From, QueryFun, Options, State);
await_condition({call, From}, {state_query, Spec}, State) ->
    Reply = {ok, do_state_query(Spec, State), id(State)},
    {keep_state, State, [{reply, From, Reply}]};
await_condition(EventType, {aux_command, Cmd}, State0) ->
    {_, ServerState, Effects} = ra_server:handle_aux(?FUNCTION_NAME, EventType,
                                                     Cmd, State0#state.server_state),
    {State, Actions} =
        ?HANDLE_EFFECTS(Effects, EventType,
                        State0#state{server_state = ServerState}),
    {keep_state, State#state{server_state = ServerState}, Actions};
await_condition({call, From}, ping, State) ->
    {keep_state, State, [{reply, From, {pong, await_condition}}]};
await_condition({call, From}, trigger_election, State) ->
    {keep_state, State, [{reply, From, ok},
                         {next_event, cast, election_timeout}]};
await_condition(info, {'DOWN', MRef, process, _Pid, _Info},
                State = #state{leader_monitor = MRef}) ->
    ?INFO("~ts: await_condition - Leader monitor down. Entering follower state.",
          [log_id(State)]),
    next_state(follower, State#state{leader_monitor = undefined}, []);
await_condition(info, {'DOWN', _MRef, process, Pid, Info}, State0) ->
    handle_process_down(Pid, Info, ?FUNCTION_NAME, State0);
await_condition(info, {node_event, Node, down}, State) ->
    case leader_id(State) of
        {_, Node} ->
            ?WARN("~ts: await_condition - Leader node ~w might be down."
                  " Re-entering follower state.",
                  [log_id(State), Node]),
            next_state(follower, State, []);
        _ ->
            {keep_state, State}
    end;
await_condition(info, {Status, Node, InfoList}, State0)
  when Status =:= nodedown orelse Status =:= nodeup ->
    handle_node_status_change(Node, Status, InfoList, ?FUNCTION_NAME, State0);
await_condition(_, tick_timeout, State0) ->
    {State, Actions} = ?HANDLE_EFFECTS([{aux, tick}], cast, State0),
    {keep_state, State, set_tick_timer(State, Actions)};
await_condition(EventType, Msg, State0) ->
    case handle_await_condition(Msg, State0) of
        {follower, State1, Effects} ->
            {State2, Actions} = ?HANDLE_EFFECTS(Effects, EventType, State1),
            State = follower_leader_change(State0, State2),
            next_state(follower, State, Actions);
        {pre_vote, State1, Effects} ->
            {State, Actions} = ?HANDLE_EFFECTS(Effects, EventType, State1),
            next_state(pre_vote, State, Actions);
        {leader, State1, Effects} ->
            {State, Actions} = ?HANDLE_EFFECTS(Effects, EventType, State1),
            next_state(leader, State, Actions);
        {await_condition, State1, Effects} ->
            {State, Actions} = ?HANDLE_EFFECTS(Effects, EventType, State1),
            %% postpone commands such that they are retried when the
            %% await_condition state is exited. Should help with client
            %% liveness
            ?DEBUG_IF(is_command(Msg), "~ts: await_condition postponing ~0P",
                      [log_id(State0), Msg, 10]),
            {keep_state, State, [{postpone, is_command(Msg)} | Actions]}
    end.

is_command(Msg) when is_tuple(Msg) ->
    element(1, Msg) == command;
is_command(_) ->
    false.


handle_event(_EventType, EventContent, StateName, State) ->
    ?WARN("~ts: handle_event unknown ~P", [log_id(State), EventContent, 10]),
    {next_state, StateName, State}.

terminate(Reason, StateName,
          #state{conf = #conf{name = Key, cluster_name = ClusterName},
                 server_state = ServerState = #{cfg := #cfg{metrics_key = MetricsKey}}} = State) ->
    ?DEBUG("~ts: terminating with ~w in state ~w",
           [log_id(State), Reason, StateName]),
    #{names := #{server_sup := SrvSup,
                 log_meta := MetaName} = Names} =
        ra_server:system_config(ServerState),
    UId = uid(State),
    Id = id(State),
    case Reason of
        {shutdown, delete} ->
            Parent = ra_directory:where_is_parent(Names, UId),
            %% we need to unregister _before_ the log closes
            %% in the ra_server:terminate/2 function
            %% as we want the directory to be deleted
            %% after the server is removed from the ra directory.
            %% This is so that the segment writer can avoid
            %% crashing if it detects a missing key
            catch ra_directory:unregister_name(Names, UId),
            _ = ra_server:terminate(ServerState, Reason),
            catch ra_log_meta:delete_sync(MetaName, UId),
            catch ra_counters:delete(Id),
            Self = self(),
            %% we have to terminate the child spec from the supervisor as it
            %% won't do this automatically, even for transient children
            %% for simple_one_for_one terminate also removes
            _ = spawn(fun () ->
                              Ref = erlang:monitor(process, Self),
                              receive
                                  {'DOWN', Ref, _, _, _} ->
                                      ok = supervisor:terminate_child(
                                             SrvSup, Parent)
                              after 5000 ->
                                        ok
                              end
                      end),
            ok;
        _ ->
            _ = ra_server:terminate(ServerState, Reason),
            ok
    end,
    catch ra_leaderboard:clear(ClusterName),
    _ = ets:delete(ra_metrics, MetricsKey),
    _ = ets:delete(ra_state, Key),
    ok;
%% This occurs if there is a crash in the init callback of the ra_machine,
%% before a state has been built
terminate(Reason, StateName, #{id := Id} = Config) ->
    LogId = maps:get(friendly_name, Config,
                     lists:flatten(io_lib:format("~w", [Id]))),
    ?DEBUG("~ts: terminating with ~w in state ~w",
           [LogId, Reason, StateName]),
    ok;
%% Unknown reason for termination
terminate(Reason, StateName, State) ->
    ?DEBUG("Terminating with ~w in state ~w with state ~w",
           [Reason, StateName, State]),
    ok.

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

format_status(#{state := StateName,
                data := #state{server_state = NS,
                               leader_last_seen = LastSeen,
                               pending_commands = Pending,
                               low_priority_commands = Delayed,
                               pending_notifys = PendingNots,
                               election_timeout_set = ElectionSet
                              }} = FormatStatus) ->
    NumPendingNots = maps:fold(fun (_, Corrs, Acc) -> Acc + length(Corrs) end,
                               0, PendingNots),

    FormatStatus#{data => [{id, ra_server:id(NS)},
                           {raft_state, StateName},
                           {leader_last_seen, LastSeen},
                           {num_pending_commands, length(Pending)},
                           {num_low_priority_commands, ra_ets_queue:len(Delayed)},
                           {num_pending_applied_notifications, NumPendingNots},
                           {election_timeout_set, ElectionSet},
                           {ra_server_state, ra_server:overview(NS)}
                          ]}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

handle_enter(RaftState, OldRaftState,
             #state{conf = #conf{name = Name},
                    server_state = ServerState0} = State) ->
    Membership = ra_server:get_membership(ServerState0),
    true = ets:insert(ra_state, {Name, RaftState, Membership}),
    {ServerState, Effects} = ra_server:handle_state_enter(RaftState,
                                                          OldRaftState,
                                                          ServerState0),
    LastApplied = do_state_query(last_applied, State),
    case RaftState == leader orelse OldRaftState == leader of
        true ->
            %% ensure transitions from and to leader are logged at a higher
            %% level
            ?NOTICE("~ts: ~s -> ~s in term: ~b machine version: ~b, last applied ~b",
                    [log_id(State), OldRaftState, RaftState,
                     current_term(State), machine_version(State),
                     LastApplied]);
        false ->
            ?DEBUG("~ts: ~s -> ~s in term: ~b machine version: ~b, last applied ~b",
                   [log_id(State), OldRaftState, RaftState,
                    current_term(State), machine_version(State),
                    LastApplied])
    end,
    handle_effects(RaftState, Effects, cast,
                   State#state{server_state = ServerState}).

handle_leader(Msg, #state{server_state = ServerState0} = State0) ->
    case catch ra_server:handle_leader(Msg, ServerState0) of
        {NextState, ServerState, Effects}  ->
            State1 = State0#state{server_state =
                                  ra_server:persist_last_applied(ServerState)},
            %% The last applied index made progress. Check if there are
            %% pending queries that wait for this index.
            {State, Actions} = perform_pending_queries(leader, State1),
            maybe_record_cluster_change(State0, State),
            {NextState, State, Effects ++ Actions};
        OtherErr ->
            ?ERR("handle_leader err ~p", [OtherErr]),
            exit(OtherErr)
    end.

handle_raft_state(RaftState, Msg,
                  #state{server_state = ServerState0,
                         election_timeout_set = Set} = State0) ->
    {NextState, ServerState1, Effects} =
        ra_server:RaftState(Msg, ServerState0),
    ElectionTimeoutSet = case Msg of
                             election_timeout -> false;
                             _ -> Set
                         end,
    ServerState = ra_server:persist_last_applied(ServerState1),
    State1 = State0#state{server_state = ServerState,
                          election_timeout_set = ElectionTimeoutSet},
    %% The last applied index made progress. Check if there are pending
    %% queries that wait for this index.
    {State, Actions} = perform_pending_queries(RaftState, State1),
    {NextState, State, Effects ++ Actions}.

handle_candidate(Msg, State) ->
    handle_raft_state(?FUNCTION_NAME, Msg, State).

handle_pre_vote(Msg, State) ->
    handle_raft_state(?FUNCTION_NAME, Msg, State).

handle_follower(Msg, State0) ->
    Ret = handle_raft_state(?FUNCTION_NAME, Msg, State0),
    {_NextState, State, _Effects} = Ret,
    maybe_record_cluster_change(State0, State),
    Ret.

handle_receive_snapshot(Msg, State) ->
    handle_raft_state(?FUNCTION_NAME, Msg, State).

handle_await_condition(Msg, State) ->
    handle_raft_state(?FUNCTION_NAME, Msg, State).

perform_or_delay_local_query(
  RaftState, EventType, From, QueryFun, Options, State0) ->
    {NextState, State1, Effects} = do_perform_or_delay_local_query(
                                     RaftState, From, QueryFun, Options,
                                     State0),
    {State, Actions} = handle_effects(RaftState, Effects, EventType, State1),
    {NextState, State, Actions}.

do_perform_or_delay_local_query(
  RaftState, From, QueryFun, Options,
  #state{conf = Conf,
         server_state = #{cfg := #cfg{id = ThisMember}} = ServerState,
         pending_queries = PendingQueries} = State) ->
    %% The caller might decide it wants the query to be executed only after a
    %% specific index has been applied on the local node. It can specify that
    %% with the `condition' option.
    %%
    %% If the condition is unset or set to `undefined', the query is performed
    %% immediatly. That is the default behavior.
    %%
    %% If the condition is set to `{applied, {Index, Term}}', the query is
    %% added to a list of pending queries. It will be evaluated once that
    %% index is applied locally.
    case maps:get(condition, Options, undefined) of
        undefined ->
            Leader = determine_leader(RaftState, State),
            Reply = perform_local_query(QueryFun, Leader, ServerState, Conf),
            {keep_state, State, [{reply, From, Reply, {member, ThisMember}}]};
        Condition ->
            PendingQuery = {Condition, From, QueryFun},
            PendingQueries1 = [PendingQuery | PendingQueries],
            State1 = State#state{pending_queries = PendingQueries1},
            %% It's possible that the specified index was already applied.
            %% That's why we evaluate pending queries just after adding the
            %% query to the list.
            {State2, Actions} = perform_pending_queries(RaftState, State1),
            {keep_state, State2, Actions}
    end.

perform_pending_queries(_RaftState, #state{pending_queries = []} = State) ->
    {State, []};
perform_pending_queries(RaftState, State) ->
    LastApplied = do_state_query(last_applied, State),
    perform_pending_queries(RaftState, LastApplied, State, []).

perform_pending_queries(RaftState, LastApplied,
                        #state{conf = Conf,
                               server_state = ServerState0,
                               pending_queries = PendingQueries0} = State0,
                        Actions0) ->
    Leader = determine_leader(RaftState, State0),
    {PendingQueries,
     Actions,
     ServerState} = lists:foldr(
                      fun(PendingQuery, Acc) ->
                              perform_pending_queries1(
                                PendingQuery, Acc,
                                #{last_applied => LastApplied,
                                  leader => Leader,
                                  conf => Conf})
                      end, {[], Actions0, ServerState0}, PendingQueries0),
    State = State0#state{server_state = ServerState,
                         pending_queries = PendingQueries},
    {State, Actions}.

perform_pending_queries1(
  {{applied, {TargetIndex, TargetTerm}}, From, QueryFun} = PendingQuery,
  {PendingQueries0, Actions0, #{cfg := #cfg{id = ThisMember}} = ServerState0},
  #{last_applied := LastApplied, leader := Leader, conf := Conf})
  when TargetIndex =< LastApplied ->
    {Term, ServerState} = ra_server:fetch_term(TargetIndex, ServerState0),
    case Term of
        TargetTerm ->
            %% The local node reached or passed the target index. We can
            %% evaluate the query.
            %%
            %% Note that some queries may have timed out from the caller's
            %% point of view. We can't tell that here, so they are still
            %% evaluated. The reply will be discarded by Erlang because the
            %% process alias in `From' is inactive after the timeout.
            Reply = perform_local_query(QueryFun, Leader, ServerState, Conf),
            Actions = [{reply, From, Reply, {member, ThisMember}} | Actions0],
            {PendingQueries0, Actions, ServerState};
        _ ->
            PendingQueries = [PendingQuery | PendingQueries0],
            {PendingQueries, Actions0, ServerState}
    end;
perform_pending_queries1(
  PendingQuery,
  {PendingQueries0, Actions, ServerState}, _Context) ->
    PendingQueries = [PendingQuery | PendingQueries0],
    {PendingQueries, Actions, ServerState}.

determine_leader(RaftState, #state{server_state = ServerState} = State) ->
    case RaftState of
        leader ->
            id(State);
        follower ->
            case ra_server:leader_id(ServerState) of
                undefined -> not_known;
                L -> L
            end;
        _ ->
            not_known
    end.

perform_local_query(QueryFun, Leader, ServerState, Conf) ->
    incr_counter(Conf, ?C_RA_SRV_LOCAL_QUERIES, 1),
    try ra_server:machine_query(QueryFun, ServerState) of
        Result ->
            {ok, Result, Leader}
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

handle_effect(_RaftState, {send_rpc, To, Rpc}, _,
              #state{conf = Conf} = State0, Actions) ->
    % fully qualified use only so that we can mock it for testing
    % TODO: review / refactor to remove the mod call here
    case ?MODULE:send_rpc(To, Rpc, State0) of
        ok ->
            {State0, Actions};
        nosuspend ->
            %% update peer status to suspended and spawn a process
            %% to send the rpc without nosuspend so that it will block until
            %% the data can get through
            Self = self(),
            _Pid = spawn(fun () ->
                                 %% AFAIK none of the below code will throw and
                                 %% exception so we should always end up setting
                                 %% the peer status back to normal
                                 ok = gen_statem:cast(To, Rpc),
                                 incr_counter(Conf, ?C_RA_SRV_MSGS_SENT, 1),
                                 Self ! {update_peer, To, #{status => normal}}
                         end),
            {update_peer(To, #{status => suspended}, State0), Actions};
        noconnect ->
            %% for noconnects just allow it to pipeline and catch up later
            {State0, Actions}
    end;
handle_effect(_, {next_event, Evt}, EvtType, State, Actions) ->
    {State, [{next_event, EvtType, Evt} |  Actions]};
handle_effect(_, {next_event, _, _} = Next, _, State, Actions) ->
    {State, [Next | Actions]};
handle_effect(leader, {send_msg, To, Msg}, _, State, Actions) ->
    %% default is to send without any wrapping
    ToNode = get_node(To),
    ?ASYNC_DIST(ToNode, _ = send(To, Msg, State#state.conf)),
    {State, Actions};
handle_effect(RaftState, {send_msg, To, _Msg, Options} = Eff,
              _, State, Actions) ->
    ToNode = get_node(To),
    case has_local_opt(Options) of
        true ->
            case can_execute_locally(RaftState, ToNode, State) of
                true ->
                    ?ASYNC_DIST(ToNode, send_msg(Eff, State));
                false ->
                    ok
            end;
        false when RaftState == leader ->
            %% the effect got here so we can execute
            ?ASYNC_DIST(ToNode, send_msg(Eff, State));
        false ->
            ok
    end,
    {State, Actions};
handle_effect(leader, {append, Cmd}, _EvtType, State, Actions) ->
    Evt = {command, normal, {'$usr', Cmd, noreply}},
    {State, [{next_event, cast, Evt} | Actions]};
handle_effect(leader, {append, Cmd, ReplyMode}, _EvtType, State, Actions) ->
    Evt = {command, normal, {'$usr', Cmd, ReplyMode}},
    {State, [{next_event, cast, Evt} | Actions]};
handle_effect(_RaftState, {try_append, Cmd, ReplyMode}, _EvtType, State, Actions) ->
    %% this is a special mode to retain the backwards compatibility of
    %% certain prior uses of {append, when it wasn't (accidentally)
    %% limited to the leader
    Evt = {command, normal, {'$usr', Cmd, ReplyMode}},
    {State, [{next_event, cast, Evt} | Actions]};
handle_effect(RaftState, {LogOrLogExt, Idxs, Fun, {local, Node}}, EvtType,
              State0, Actions)
  when LogOrLogExt == log orelse
       LogOrLogExt == log_ext ->
    case can_execute_locally(RaftState, Node, State0) of
        true ->
            {Effects, State} = handle_log_effect(LogOrLogExt, Idxs, Fun,
                                                 State0),
            handle_effects(RaftState, Effects, EvtType,
                           State, Actions);
        false ->
            {State0, Actions}
    end;
handle_effect(leader, {LogOrLogExt, Idxs, Fun}, EvtType, State0, Actions)
  when is_list(Idxs) andalso
       (LogOrLogExt == log orelse
        LogOrLogExt == log_ext) ->
    %% Useful to implement a batch send of data obtained from the log.
    %% 1) Retrieve all data from the list of indexes
    {Effects, State} = handle_log_effect(LogOrLogExt, Idxs, Fun,
                                         State0),
    handle_effects(leader, Effects, EvtType,
                   State, Actions);
handle_effect(RaftState, {aux, Cmd}, EventType, State0, Actions0) ->
    {_, ServerState, Effects} = ra_server:handle_aux(RaftState, cast, Cmd,
                                                     State0#state.server_state),
    {State, Actions} =
        handle_effects(RaftState, Effects, EventType,
                       State0#state{server_state = ServerState}),
    {State, Actions0 ++ Actions};
handle_effect(leader, {notify, Nots}, _, #state{} = State0, Actions) ->
    %% should only be done by leader
    State = send_applied_notifications(State0, Nots),
    {State, Actions};
handle_effect(_AnyState, {cast, To, Msg}, _, State, Actions) ->
    %% TODO: handle send failure
    _ = gen_cast(To, Msg, State),
    {State, Actions};
handle_effect(RaftState, {reply, {Pid, _Tag} = From, Reply, Replier}, _,
              State, Actions) ->
    case Replier of
        leader when RaftState == leader ->
            ok = gen_statem:reply(From, Reply);
        leader ->
            ok;
        local ->
            case can_execute_locally(RaftState, node(Pid), State) of
                true ->
                    ok = gen_statem:reply(From, Reply);
                false ->
                    ok
            end;
        {member, Member} ->
            case can_execute_on_member(RaftState, Member, State) of
                true ->
                    ok = gen_statem:reply(From, Reply);
                false ->
                    ok
            end;
        _ ->
            ok
    end,
    {State, Actions};
handle_effect(leader, {reply, From, Reply}, _, State, Actions) ->
    % reply directly, this is only done from the leader
    % this is like reply/4 above with the Replier=leader
    ok = gen_statem:reply(From, Reply),
    {State, Actions};
handle_effect(_RaftState, {reply, Reply}, {call, From}, State, Actions) ->
    % this is the reply effect for replying to the current call, any state
    % can use this
    ok = gen_statem:reply(From, Reply),
    {State, Actions};
handle_effect(_RaftState, {reply, _From, _Reply}, _EvtType, State, Actions) ->
    {State, Actions};
handle_effect(leader, {send_snapshot, {_, ToNode} = To, {SnapState, Id, Term}}, _,
              #state{server_state = SS0,
                     monitors = Monitors,
                     conf = #conf{snapshot_chunk_size = ChunkSize,
                     install_snap_rpc_timeout = InstallSnapTimeout} = Conf} = State0,
              Actions) ->
    case lists:member(ToNode, [node() | nodes()]) of
        true ->
            %% node is connected
            %% leader effect only
            Self = self(),
            Machine = ra_server:machine(SS0),
            Pid = spawn(fun () ->
                                try send_snapshots(Self, Id, Term, To,
                                                   ChunkSize, InstallSnapTimeout,
                                                   SnapState, Machine) of
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
            ok = incr_counter(Conf, ?C_RA_SRV_SNAPSHOTS_SENT, 1),
            %% update the peer state so that no pipelined entries are sent during
            %% the snapshot sending phase
            SS = ra_server:update_peer(To, #{status => {sending_snapshot, Pid}}, SS0),
            {State0#state{server_state = SS,
                          monitors = ra_monitors:add(Pid, snapshot_sender, Monitors)},
             Actions};
        false ->
            ?DEBUG("~ts: send_snapshot node ~s disconnected",
                   [log_id(State0), ToNode]),
            SS = ra_server:update_peer(To, #{status => disconnected}, SS0),
            {State0#state{server_state = SS}, Actions}
    end;
handle_effect(_, {delete_snapshot, Dir,  SnapshotRef}, _, State0, Actions) ->
    %% delete snapshots in separate process
    _ = spawn(fun() ->
                      ra_snapshot:delete(Dir, SnapshotRef)
              end),
    {State0, Actions};
handle_effect(_, {send_vote_requests, VoteRequests}, _, % EvtType
              #state{conf = #conf{aten_poll_interval = Poll}} = State, Actions) ->
    % transient election processes
    %% set the timeout to the aten poll interval which is the approximate maximum
    %% election timeout value
    Timeout = {dirty_timeout, Poll},
    Me = self(),
    [begin
         _ = spawn(fun () ->
                           Reply = gen_statem:call(ServerId, Request, Timeout),
                           ok = gen_statem:cast(Me, Reply)
                   end)
     end || {ServerId, Request} <- VoteRequests],
    {State, Actions};
handle_effect(RaftState, {release_cursor, Index, MacState}, EvtType,
              #state{server_state = ServerState0} = State0, Actions0) ->
    incr_counter(State0#state.conf, ?C_RA_SRV_RELEASE_CURSORS, 1),
    {ServerState, Effects} = ra_server:update_release_cursor(Index, MacState,
                                                             ServerState0),
    State1 = State0#state{server_state = ServerState},
    handle_effects(RaftState, Effects, EvtType, State1, Actions0);
handle_effect(RaftState, {release_cursor, Index}, EvtType,
              #state{server_state = ServerState0} = State0, Actions0) ->
    incr_counter(State0#state.conf, ?C_RA_SRV_RELEASE_CURSORS, 1),
    {ServerState, Effects} = ra_server:promote_checkpoint(Index, ServerState0),
    State1 = State0#state{server_state = ServerState},
    handle_effects(RaftState, Effects, EvtType, State1, Actions0);
handle_effect(RaftState, {checkpoint, Index, MacState}, EvtType,
              #state{server_state = ServerState0} = State0, Actions0) ->
    incr_counter(State0#state.conf, ?C_RA_SRV_CHECKPOINTS, 1),
    {ServerState, Effects} = ra_server:checkpoint(Index, MacState,
                                                  ServerState0),
    State1 = State0#state{server_state = ServerState},
    handle_effects(RaftState, Effects, EvtType, State1, Actions0);
handle_effect(_, garbage_collection, _EvtType, State, Actions) ->
    true = erlang:garbage_collect(),
    incr_counter(State#state.conf, ?C_RA_SRV_GCS, 1),
    {State, Actions};
handle_effect(leader, {monitor, _ProcOrNode, PidOrNode}, _,
              #state{monitors = Monitors} = State, Actions0) ->
    %% this effect type is only emitted by state machines and thus will
    %% only be monitored from the leader
    {State#state{monitors = ra_monitors:add(PidOrNode, machine, Monitors)},
     Actions0};
handle_effect(leader, {monitor, _ProcOrNode, machine, PidOrNode}, _,
              #state{monitors = Monitors} = State, Actions0) ->
    {State#state{monitors = ra_monitors:add(PidOrNode, machine, Monitors)},
     Actions0};
handle_effect(_RaftState, {monitor, _ProcOrNode, machine, _PidOrNode}, _,
              #state{} = State, Actions0) ->
    %% AFAIK: there is nothing emitting this effect type but we have to
    %% guard against it being actioned on the follower anyway
    {State, Actions0};
handle_effect(_, {monitor, _ProcOrNode, Component, PidOrNode}, _,
              #state{monitors = Monitors} = State, Actions0) ->
    {State#state{monitors = ra_monitors:add(PidOrNode, Component, Monitors)},
     Actions0};
handle_effect(_, {demonitor, _ProcOrNode, PidOrNode}, _,
              #state{monitors = Monitors0} = State, Actions) ->
    Monitors = ra_monitors:remove(PidOrNode, machine, Monitors0),
    {State#state{monitors = Monitors}, Actions};
handle_effect(_, {demonitor, _ProcOrNode, Component, PidOrNode}, _,
              #state{monitors = Monitors0} = State, Actions) ->
    Monitors = ra_monitors:remove(PidOrNode, Component, Monitors0),
    {State#state{monitors = Monitors}, Actions};
handle_effect(leader, {timer, Name, T}, _, State, Actions) ->
    {State, [{{timeout, Name}, T, machine_timeout} | Actions]};
handle_effect(leader, {mod_call, Mod, Fun, Args}, _,
              State, Actions) ->
    %% TODO: catch and log failures or rely on calling function never crashing
    _ = erlang:apply(Mod, Fun, Args),
    {State, Actions};
handle_effect(_RaftState, start_election_timeout, _, State, Actions) ->
    maybe_set_election_timeout(long, State, Actions);
handle_effect(follower, {record_leader_msg, _LeaderId}, _, State0, Actions) ->
    %% record last time leader seen
    State = State0#state{leader_last_seen = erlang:system_time(millisecond),
                         election_timeout_set = false},
    %% always cancel state timeout when a valid leader message has been
    %% received just in case a timeout is currently active
    %% the follower ra_server logic will emit this
    {State, [{state_timeout, infinity, undefined} | Actions]};
handle_effect(_, {record_leader_msg, _LeaderId}, _, State0, Actions) ->
    %% non follower states don't need to reset state timeout after an effect
    {State0, Actions};
handle_effect(_, _, _, State0, Actions) ->
    {State0, Actions}.

send_rpcs(State0) ->
    {State, Rpcs} = make_rpcs(State0),
    % module call so that we can mock
    % We can ignore send failures here as they have not incremented
    % the peer's next index
    [_ = ?MODULE:send_rpc(To, Rpc, State) || {send_rpc, To, Rpc} <- Rpcs],
    State.

make_rpcs(State) ->
    {ServerState, Rpcs} = ra_server:make_rpcs(State#state.server_state),
    {State#state{server_state = ServerState}, Rpcs}.

send_rpc(To, Msg, State) ->
    incr_counter(State#state.conf, ?C_RA_SRV_RPCS_SENT, 1),
    % fake gen cast
    gen_cast(To, Msg, State).

gen_cast(To, Msg, State) ->
    send(To, {'$gen_cast', Msg}, State#state.conf).

send_ra_event(To, Msg, FromId, EvtType, State) ->
    send(To, wrap_ra_event(State, FromId, EvtType, Msg), State#state.conf).

wrap_ra_event(#state{conf = #conf{ra_event_formatter = undefined}},
              FromId, EvtType, Evt) ->
    {ra_event, FromId, {EvtType, Evt}};
wrap_ra_event(#state{conf = #conf{ra_event_formatter = {M, F, A}}},
              LeaderId, EvtType, Evt) ->
    apply(M, F, [LeaderId, {EvtType, Evt} | A]).

parse_send_msg_options(ra_event) ->
    {true, false};
parse_send_msg_options(cast) ->
    {false, true};
parse_send_msg_options(local) ->
    {false, false};
parse_send_msg_options(Options) when is_list(Options) ->
    {lists:member(ra_event, Options), lists:member(cast, Options)}.

id(#state{server_state = ServerState}) ->
    ra_server:id(ServerState).

log_id(#state{conf = #conf{log_id = N}}) ->
    N.

uid(#state{server_state = ServerState}) ->
    ra_server:uid(ServerState).

leader_id(#state{server_state = ServerState}) ->
    ra_server:leader_id(ServerState).

clear_leader_id(#state{server_state = ServerState} = State) ->
    ServerState1 = ra_server:clear_leader_id(ServerState),
    State#state{server_state = ServerState1}.

current_term(#state{server_state = ServerState}) ->
    ra_server:current_term(ServerState).

machine_version(#state{server_state = ServerState}) ->
    ra_server:machine_version(ServerState).

process_pending_queries(NewLeader,
                        #state{server_state = ServerState0} = State) ->
    {ServerState, Froms} = ra_server:process_new_leader_queries(ServerState0),
    [_ = gen_statem:reply(F, {redirect, NewLeader})
     || F <- Froms],
    State#state{server_state = ServerState}.

election_timeout_action(Length, #state{conf = Conf}) ->
    election_timeout_action(Length, Conf);
election_timeout_action(really_short, #conf{broadcast_time = Timeout}) ->
    T = rand:uniform(Timeout),
    {state_timeout, T, election_timeout};
election_timeout_action(short, #conf{broadcast_time = Timeout}) ->
    T = rand:uniform(Timeout * ?DEFAULT_ELECTION_MULT) + Timeout,
    {state_timeout, T, election_timeout};
election_timeout_action(medium, #conf{broadcast_time = Timeout,
                                      aten_poll_interval = Poll}) ->
    %% this is roughly in between broadcast time and aten poll interval
    T = rand:uniform(Poll) + Timeout,
    {state_timeout, T, election_timeout};
election_timeout_action(long, #conf{broadcast_time = Timeout,
                                    aten_poll_interval = Poll}) ->
    %% this should be longer than aten detection poll interval so that
    %% there is a chance a false positive from aten can be reversed without
    %% triggering elections
    T = rand:uniform(Timeout * ?DEFAULT_ELECTION_MULT * 2) + Poll,
    {state_timeout, T, election_timeout}.

% sets the tick timer for periodic actions such as sending rpcs to servers
% that are stale to ensure liveness
set_tick_timer(#state{conf = #conf{tick_timeout = TickTimeout}}, Actions) ->
    [{{timeout, tick}, TickTimeout, tick_timeout} | Actions].


follower_leader_change(Old, #state{pending_commands = Pending,
                                   leader_monitor = OldMRef} = New) ->
    OldLeader = leader_id(Old),
    case leader_id(New) of
        OldLeader when is_reference(OldMRef) ->
            % no change and monitor is still intact
            New;
        undefined ->
            New;
        NewLeader ->
            MRef = swap_monitor(OldMRef, NewLeader),
            LeaderNode = ra_lib:ra_server_id_node(NewLeader),
            ok = aten_register(LeaderNode),
            OldLeaderNode = ra_lib:ra_server_id_node(OldLeader),
            _ = aten:unregister(OldLeaderNode),
            % leader has either changed or just been set
            ?INFO("~ts: detected a new leader ~w in term ~b",
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
            case aten:register(Node) of
                ignore ->
                    ok;
                Res ->
                    Res
            end
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
            {error, nodedown};
         exit:{shutdown, _} ->
            {error, shutdown};
         exit:{Reason, _} ->
            {error, Reason}
    end.

do_state_query(QueryName, #state{server_state = State}) ->
    ra_server:state_query(QueryName, State).

config_defaults(ServerId) ->
    Counter = case ra_counters:fetch(ServerId) of
                  undefined ->
                      ra_counters:new(ServerId,
                                      {persistent_term, ?FIELDSPEC_KEY});
                  C ->
                      C
              end,
    #{broadcast_time => ?DEFAULT_BROADCAST_TIME,
      tick_timeout => ?TICK_INTERVAL_MS,
      install_snap_rpc_timeout => ?INSTALL_SNAP_RPC_TIMEOUT,
      await_condition_timeout => ?DEFAULT_AWAIT_CONDITION_TIMEOUT,
      initial_members => [],
      counter => Counter,
      system_config => ra_system:default_config()
     }.

maybe_redirect(From, Msg, #state{pending_commands = Pending,
                                 leader_monitor = LeaderMon} = State) ->
    Leader = leader_id(State),
    case LeaderMon of
        undefined ->
            ?DEBUG("~ts: leader call - leader not known. "
                   "Command will be forwarded once leader is known.",
                   [log_id(State)]),
            {keep_state,
             State#state{pending_commands = [{From, Msg} | Pending]}};
        _ when Leader =/= undefined ->
            {keep_state, State, {reply, From, {redirect, Leader}}}
    end.

reject_command(Pid, Corr, #state{leader_monitor = _Mon} = State) ->
    Id = id(State),
    LeaderId = leader_id(State),
    case LeaderId of
        undefined ->
            %% don't log these as they may never be resolved
            ok;
        Id ->
            %% this can happen during an explicit leader change
            %% best not rejecting them to oneself!
            ok;
        _ ->
            ?INFO("~ts: follower received leader command from ~w. "
                  "Rejecting to ~w ", [log_id(State), Pid, LeaderId]),
            send_ra_event(Pid, {not_leader, LeaderId, Corr},
                          id(State), rejected, State)
    end.

maybe_persist_last_applied(#state{server_state = NS} = State) ->
     State#state{server_state = ra_server:persist_last_applied(NS)}.

send(To, Msg, Conf) ->
    % we do not want to block the ra server whilst attempting to set up
    % a TCP connection to a potentially down node or when the distribution
    % buffer is full, better to drop and try again later
    case erlang:send(To, Msg, [noconnect, nosuspend]) of
        ok ->
            incr_counter(Conf, ?C_RA_SRV_MSGS_SENT, 1),
            ok;
        Res ->
            incr_counter(Conf, ?C_RA_SRV_DROPPED_SENDS, 1),
            Res
    end.

fold_log(From, Fun, Term, State) ->
    case ra_server:log_fold(State#state.server_state, Fun, Term) of
        {ok, Result, ServerState} ->
            {keep_state, State#state{server_state = ServerState},
             [{reply, From, {ok, Result}}]}
    end.

send_snapshots(Me, Id, Term, {_, ToNode} = To, ChunkSize,
               InstallTimeout, SnapState, Machine) ->
    Context = ra_snapshot:context(SnapState, ToNode),
    {ok, #{machine_version := SnapMacVer} = Meta, ReadState} =
        ra_snapshot:begin_read(SnapState, Context),

    %% only send the snapshot if the target server can accept it
    TheirMacVer = erpc:call(ToNode, ra_machine, version, [Machine]),

    case SnapMacVer > TheirMacVer of
        true ->
            ok;
        false ->
            RPC = #install_snapshot_rpc{term = Term,
                                        leader_id = Id,
                                        meta = Meta},
            Result = read_chunks_and_send_rpc(RPC, To, ReadState, 1,
                                              ChunkSize, InstallTimeout, SnapState),
            ok = gen_statem:cast(Me, {To, Result})
    end.

read_chunks_and_send_rpc(RPC0,
                         To, ReadState0, Num, ChunkSize, InstallTimeout, SnapState) ->
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
                           {dirty_timeout, InstallTimeout}),
    case ContState of
        {next, ReadState1} ->
            read_chunks_and_send_rpc(RPC0, To, ReadState1, Num + 1,
                                     ChunkSize, InstallTimeout, SnapState);
        last ->
            Res1
    end.

validate_reply_mode(after_log_append) ->
    ok;
validate_reply_mode(await_consensus) ->
    ok;
validate_reply_mode({await_consensus, Options}) when is_map(Options) ->
    validate_reply_mode_options(Options);
validate_reply_mode({notify, Correlation, Pid})
  when (is_integer(Correlation) orelse
        is_reference(Correlation)) andalso
       is_pid(Pid) ->
    ok;
validate_reply_mode({notify, Correlation, Pid, Options})
  when (is_integer(Correlation) orelse
        is_reference(Correlation)) andalso
       is_pid(Pid) andalso
       is_map(Options) ->
    validate_reply_mode_options(Options);
validate_reply_mode(noreply) ->
    ok;
validate_reply_mode(ReplyMode) ->
    {error, {invalid_reply_mode, ReplyMode}}.

validate_reply_mode_options(Options) when is_map(Options) ->
    maps:fold(fun (Key, Value, ok) ->
                      case {Key, Value} of
                          {reply_from, local} ->
                              ok;
                          {reply_from, {member, _}} ->
                              ok;
                          {reply_from, leader} ->
                              ok;
                          {_, _} ->
                              {error, {unknown_option, Key, Value}}
                      end;
                  (_Key, _Value, Error) ->
                      Error
              end, ok, Options).

maybe_set_election_timeout(_TimeoutLen,
                           #state{election_timeout_set = true} = State,
                           Actions) ->
    %% already set, don't update
    {State, Actions};
maybe_set_election_timeout(TimeoutLen, State, Actions) ->
    {State#state{election_timeout_set = true},
     [election_timeout_action(TimeoutLen, State) | Actions]}.

next_state(leader, #state{pending_commands = Pending} = State, Actions) ->
    NextEvents = [{next_event, {call, F}, Cmd} || {F, Cmd} <- Pending],
    {next_state, leader, State#state{election_timeout_set = false,
                                     pending_commands = []},
     Actions ++ NextEvents};
next_state(Next, State, Actions) ->
    %% as changing states will always cancel the state timeout we need
    %% to set our own state tracking to false here
    {next_state, Next, State#state{election_timeout_set = false}, Actions}.

send_msg({send_msg, To, Msg, Options}, State) ->
    incr_counter(State#state.conf, ?C_RA_SRV_SEND_MSG_EFFS_SENT, 1),
    case parse_send_msg_options(Options) of
        {true, true} ->
            gen_cast(To, wrap_ra_event(State, leader_id(State), machine, Msg),
                    State);
        {true, false} ->
            send(To, wrap_ra_event(State, leader_id(State), machine, Msg),
                 State#state.conf);
        {false, true} ->
            gen_cast(To, Msg, State);
        {false, false} ->
            send(To, Msg, State#state.conf)
    end.

has_local_opt(local) ->
    true;
has_local_opt(Opt) when is_atom(Opt) ->
    false;
has_local_opt(Opts) ->
    lists:member(local, Opts).

get_node(P) when is_pid(P) ->
    node(P);
get_node({_, Node}) ->
    Node;
get_node(Proc) when is_atom(Proc) ->
    node().

handle_tick_metrics(State) ->
    ServerState = State#state.server_state,
    Metrics = ra_server:metrics(ServerState),
    _ = ets:insert(ra_metrics, Metrics),
    State.

can_execute_locally(RaftState, TargetNode,
                    #state{server_state = ServerState} = State) ->
    case RaftState of
        leader when TargetNode =/= node() ->
            %% We need to evaluate whether to send the message.
            %% Only send if there isn't a local node for the target pid.
            Members = do_state_query(voters, State),
            not lists:any(fun ({_, N}) -> N == TargetNode end, Members);
        leader ->
            true;
        _ when RaftState =/= leader ->
            TargetNode == node() andalso
            voter == ra_server:get_membership(ServerState)
    end.

can_execute_on_member(_RaftState, Member,
                      #state{server_state = #{cfg := #cfg{id = Member}}}) ->
    true;
can_execute_on_member(leader, Member, State) ->
    Members = do_state_query(members, State),
    not lists:member(Member, Members);
can_execute_on_member(_RaftState, _Member, _State) ->
    false.

handle_node_status_change(Node, Status, InfoList, RaftState,
                          #state{monitors = Monitors0,
                                 server_state = ServerState0} = State0) ->
    {Comps, Monitors} = ra_monitors:handle_down(Node, Monitors0),
    {_, ServerState1, Effects} =
        lists:foldl(
          fun (Comp, {R, S0, E0}) ->
                  {R, S, E} = ra_server:handle_node_status(R, Comp, Node,
                                                           Status, InfoList,
                                                           S0),
                  {R, S, E0 ++ E}
          end, {RaftState, ServerState0, []}, Comps),
    ServerState = ra_server:update_disconnected_peers(Node, Status,
                                                      ServerState1),
    {State, Actions} = handle_effects(RaftState, Effects, cast,
                                      State0#state{server_state = ServerState,
                                                   monitors = Monitors}),
    {keep_state, State, Actions}.

handle_process_down(Pid, Info, RaftState,
                    #state{monitors = Monitors0,
                           pending_notifys = Nots,
                           server_state = ServerState0} = State0) ->
    {Comps, Monitors} = ra_monitors:handle_down(Pid, Monitors0),
    {ServerState, Effects} =
        lists:foldl(
          fun(Comp, {S0, E0}) ->
                  {_, S, E} = ra_server:handle_down(RaftState, Comp,
                                                    Pid, Info, S0),
                  {S, E0 ++ E}
          end, {ServerState0, []}, Comps),

    {State, Actions} =
        handle_effects(RaftState, Effects, cast,
                       State0#state{server_state = ServerState,
                                    pending_notifys = maps:remove(Pid, Nots),
                                    monitors = Monitors}),
    {keep_state, State, Actions}.

maybe_record_cluster_change(#state{conf = #conf{cluster_name = ClusterName},
                                   server_state = ServerStateA},
                            #state{server_state = ServerStateB}) ->
    LeaderA = ra_server:leader_id(ServerStateA),
    LeaderB = ra_server:leader_id(ServerStateB),
    if (map_get(cluster_index_term, ServerStateA) =/=
        map_get(cluster_index_term, ServerStateB) orelse
        LeaderA =/= LeaderB) ->
            MembersB = ra_server:state_query(members, ServerStateB),
            ok = ra_leaderboard:record(ClusterName, LeaderB, MembersB);
        true ->
            ok
    end.

record_cluster_change(#state{conf = #conf{cluster_name = ClusterName},
                             server_state = ServerState}) ->
    Leader = ra_server:state_query(leader, ServerState),
    Members = ra_server:state_query(members, ServerState),
    ok = ra_leaderboard:record(ClusterName, Leader, Members).

incr_counter(#conf{counter = Cnt}, Ix, N) when Cnt =/= undefined ->
    counters:add(Cnt, Ix, N);
incr_counter(#conf{counter = undefined}, _Ix, _N) ->
    ok.

update_peer(PeerId, Update,
            #state{server_state = ServerState} = State0)
  when is_map(Update) ->
    State0#state{server_state =
                 ra_server:update_peer(PeerId, Update, ServerState)}.

send_applied_notifications(#state{pending_notifys = PendingNots} = State,
                           Nots0) when map_size(PendingNots) > 0 ->
    Nots = maps:merge_with(fun(_K, V1, V2) ->
                                   V1 ++ V2
                           end, PendingNots, Nots0),
    send_applied_notifications(State#state{pending_notifys = #{}}, Nots);
send_applied_notifications(#state{} = State, Nots) ->
    Id = id(State),
    %% any notifications that could not be sent
    %% will be kept and retried
    RemNots = maps:filter(
                fun(Who, Correlations0) ->
                        %% correlations are build up in reverse order so we need
                        %% to reverse before sending
                        Correlations = lists:reverse(Correlations0),
                        ok =/= send_ra_event(Who, Correlations, Id,
                                             applied, State)
                end, Nots),
    case map_size(RemNots) of
        0  ->
            State;
        _ ->
            State#state{pending_notifys = RemNots}
    end.

make_command(Type, {call, From}, Data, Mode) ->
    Ts = erlang:system_time(millisecond),
    {Type, #{from => From, ts => Ts}, Data, Mode};
make_command(Type, _, Data, Mode) ->
    Ts = erlang:system_time(millisecond),
    {Type, #{ts => Ts}, Data, Mode}.

schedule_command_flush(Delayed) ->
    case ra_ets_queue:len(Delayed) of
        0 ->
            ok;
        _ ->
            ok = gen_statem:cast(self(), flush_commands)
    end.


handle_log_effect(log, Idxs, Fun,
                  #state{server_state = SS0} = State)
  when is_list(Idxs) ->
    %% Useful to implement a batch send of data obtained from the log.
    %% 1) Retrieve all data from the list of indexes
    {ok, Cmds, SS} = ra_server:log_read(Idxs, SS0),
    %% 2) Apply the fun to the list of commands as a whole and deal with any effects
    {Fun(Cmds), State#state{server_state = SS}};
handle_log_effect(log_ext, Idxs, Fun,
                  #state{server_state = SS0} = State)
  when is_list(Idxs) ->
    ReadState = ra_server:log_partial_read(Idxs, SS0),
    {Fun(ReadState), State}.
