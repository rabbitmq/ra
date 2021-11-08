%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2017-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%
-module(ra_server).

-include("ra.hrl").
-include("ra_server.hrl").

-compile(inline_list_funcs).

-export([
         name/2,
         init/1,
         process_new_leader_queries/1,
         handle_leader/2,
         handle_candidate/2,
         handle_pre_vote/2,
         handle_follower/2,
         handle_receive_snapshot/2,
         handle_await_condition/2,
         handle_aux/4,
         handle_state_enter/2,
         tick/1,
         overview/1,
         metrics/1,
         is_new/1,
         is_fully_persisted/1,
         is_fully_replicated/1,
         % properties
         id/1,
         uid/1,
         log_id/1,
         system_config/1,
         leader_id/1,
         current_term/1,
         machine_version/1,
         machine_query/2,
         % TODO: hide behind a handle_leader
         make_rpcs/1,
         update_release_cursor/3,
         persist_last_applied/1,
         update_peer/3,
         register_external_log_reader/2,
         handle_down/5,
         handle_node_status/6,
         terminate/2,
         log_fold/3,
         read_at/2,
         recover/1
        ]).

-type ra_await_condition_fun() ::
    fun((ra_msg(), ra_server_state()) -> {boolean(), ra_server_state()}).

-type ra_server_state() ::
    #{cfg := #cfg{},
      leader_id => maybe(ra_server_id()),
      cluster := ra_cluster(),
      cluster_change_permitted := boolean(),
      cluster_index_term := ra_idxterm(),
      previous_cluster => {ra_index(), ra_term(), ra_cluster()},
      current_term := ra_term(),
      log := term(),
      voted_for => maybe(ra_server_id()), % persistent
      votes => non_neg_integer(),
      commit_index := ra_index(),
      last_applied := ra_index(),
      persisted_last_applied => ra_index(),
      stop_after => ra_index(),
      machine_state := term(),
      aux_state => term(),
      condition => ra_await_condition_fun(),
      condition_timeout_changes => #{transition_to := ra_state(),
                                     effects := [effect()]},
      pre_vote_token => reference(),
      query_index := non_neg_integer(),
      queries_waiting_heartbeats := queue:queue({non_neg_integer(), consistent_query_ref()}),
      pending_consistent_queries := [consistent_query_ref()],
      commit_latency => maybe(non_neg_integer())
     }.

-type ra_state() :: leader | follower | candidate
                    | pre_vote | await_condition | delete_and_terminate
                    | terminating_leader | terminating_follower | recover
                    | recovered | stop | receive_snapshot.

-type command_type() :: '$usr' | '$ra_join' | '$ra_leave' |
                        '$ra_cluster_change' | '$ra_cluster'.

-type command_meta() :: #{from => from(),
                          ts := integer()}.

-type command_correlation() :: integer() | reference().

-type command_reply_mode() :: after_log_append |
                              await_consensus |
                              {notify,
                               command_correlation(), pid()} |
                              noreply.

-type command() :: {command_type(), command_meta(),
                    UserCommand :: term(), command_reply_mode()} |
                   {noop, command_meta(),
                    CurrentMachineVersion :: ra_machine:version()}.

-type ra_msg() :: #append_entries_rpc{} |
                  {ra_server_id(), #append_entries_reply{}} |
                  {ra_server_id(), #install_snapshot_result{}} |
                  #request_vote_rpc{} |
                  #request_vote_result{} |
                  #pre_vote_rpc{} |
                  #pre_vote_result{} |
                  #install_snapshot_rpc{} |
                  election_timeout |
                  await_condition_timeout |
                  {command, command()} |
                  {commands, [command()]} |
                  ra_log:event() |
                  {consistent_query, term(), ra:query_fun()} |
                  #heartbeat_rpc{} |
                  {ra_server_id, #heartbeat_reply{}}.

-type ra_reply_body() :: #append_entries_reply{} |
                         #request_vote_result{} |
                         #install_snapshot_result{} |
                         #pre_vote_result{}.

-type effect() ::
    ra_machine:effect() |
    ra_log:effect() |
    {reply, ra_reply_body()} |
    {reply, term(), ra_reply_body()} |
    {cast, ra_server_id(), term()} |
    {send_vote_requests, [{ra_server_id(),
                           #request_vote_rpc{} | #pre_vote_rpc{}}]} |
    {send_rpc, ra_server_id(), #append_entries_rpc{}} |
    {send_snapshot, To :: ra_server_id(),
     {Module :: module(), Ref :: term(),
      LeaderId :: ra_server_id(), Term :: ra_term()}} |
    {next_event, ra_msg()} |
    {next_event, cast, ra_msg()} |
    {notify, pid(), reference()} |
    %% used for tracking valid leader messages
    {record_leader_msg, ra_server_id()} |
    start_election_timeout.

-type effects() :: [effect()].

-type simple_apply_fun(State) :: fun((term(), State) -> State).
-type ra_event_formatter_fun() ::
    fun((ra_server_id(), Evt :: term()) -> term()).

-type machine_conf() :: {module, module(), InitConfig :: map()} |
                        {simple, simple_apply_fun(term()),
                         InitialState :: term()}.
%% The machine configuration.
%% This is how ra knows which module to use to invoke the ra_machine callbacks
%% and the config to pass to the {@link ra_machine:init/1} implementation.
%% The simple machine config is version that can only be used for simple state
%% machines that cannot access any of the advanced features.

-type ra_server_config() :: #{id := ra_server_id(),
                              uid := ra_uid(),
                              %% a friendly name to refer to a particular
                              %% server - will default to the id formatted
                              %% with `~w'
                              cluster_name := ra_cluster_name(),
                              log_init_args := ra_log:ra_log_init_args(),
                              initial_members := [ra_server_id()],
                              machine := machine_conf(),
                              friendly_name => unicode:chardata(),
                              metrics_key => term(),
                              % TODO: review - only really used for
                              % setting election timeouts
                              broadcast_time => non_neg_integer(), % ms
                              % for periodic actions such as sending stale rpcs
                              % and persisting last_applied index
                              tick_timeout => non_neg_integer(), % ms
                              await_condition_timeout => non_neg_integer(),
                              max_pipeline_count => non_neg_integer(),
                              ra_event_formatter => {module(), atom(), [term()]},
                              counter => counters:counters_ref(),
                              system_config => ra_system:config()}.

-type mutable_config() :: #{cluster_name => ra_cluster_name(),
                            metrics_key => term(),
                            broadcast_time => non_neg_integer(), % ms
                            tick_timeout => non_neg_integer(), % ms
                            await_condition_timeout => non_neg_integer(),
                            max_pipeline_count => non_neg_integer(),
                            ra_event_formatter => {module(), atom(), [term()]}}.

-type config() :: ra_server_config().

-export_type([config/0,
              ra_server_state/0,
              ra_state/0,
              ra_server_config/0,
              mutable_config/0,
              ra_msg/0,
              machine_conf/0,
              command/0,
              command_type/0,
              command_meta/0,
              command_correlation/0,
              command_reply_mode/0,
              ra_event_formatter_fun/0,
              effect/0,
              effects/0
             ]).

-spec name(ClusterName :: ra_cluster_name(), UniqueSuffix::string()) -> atom().
name(ClusterName, UniqueSuffix) ->
    list_to_atom("ra_" ++ ClusterName ++ "_server_" ++ UniqueSuffix).

-spec init(ra_server_config()) -> ra_server_state().
init(#{id := Id,
       uid := UId,
       cluster_name := _ClusterName,
       initial_members := InitialNodes,
       log_init_args := LogInitArgs,
       machine := MachineConf} = Config) ->
    SystemConfig = maps:get(system_config, Config,
                            ra_system:default_config()),
    LogId = maps:get(friendly_name, Config,
                     lists:flatten(io_lib:format("~w", [Id]))),
    MaxPipelineCount = maps:get(max_pipeline_count, Config,
                                ?DEFAULT_MAX_PIPELINE_COUNT),
    MetricKey = case Config of
                    #{metrics_key := K} ->
                        K;
                    _ ->
                        ra_lib:ra_server_id_to_local_name(Id)
                end,
    Name = ra_lib:ra_server_id_to_local_name(Id),
    Machine = case MachineConf of
                  {simple, Fun, S} ->
                      {machine, ra_machine_simple, #{simple_fun => Fun,
                                                     initial_state => S}};
                  {module, Mod, Args} ->
                      {machine, Mod, Args}
              end,

    SnapModule = ra_machine:snapshot_module(Machine),
    Counter = maps:get(counter, Config, undefined),

    Log0 = ra_log:init(LogInitArgs#{snapshot_module => SnapModule,
                                    system_config => SystemConfig,
                                    uid => UId,
                                    counter => Counter,
                                    log_id => LogId,
                                    %% use sequential access pattern during
                                    %% recovery
                                    initial_access_pattern => sequential}),
    %% only write config if it is different from what is already on disk
    case Config of
        #{has_changed := false} ->
            ok;
        _ ->
            ok = ra_log:write_config(Config, Log0)
    end,

    MetaName = meta_name(SystemConfig),
    CurrentTerm = ra_log_meta:fetch(MetaName, UId, current_term, 0),
    LastApplied = ra_log_meta:fetch(MetaName, UId, last_applied, 0),
    VotedFor = ra_log_meta:fetch(MetaName, UId, voted_for, undefined),

    LatestMacVer = ra_machine:version(Machine),

    {FirstIndex, Cluster0, MacVer, MacState,
     {SnapshotIdx, _} = SnapshotIndexTerm} =
        case ra_log:recover_snapshot(Log0) of
            undefined ->
                InitialMachineState = ra_machine:init(Machine, Name),
                {0, make_cluster(Id, InitialNodes),
                 0, InitialMachineState, {0, 0}};
            {#{index := Idx,
               term := Term,
               cluster := ClusterNodes,
               machine_version := MacVersion}, MacSt} ->
                Clu = make_cluster(Id, ClusterNodes),
                %% the snapshot is the last index before the first index
                {Idx, Clu, MacVersion, MacSt, {Idx, Term}}
        end,
    MacMod = ra_machine:which_module(Machine, MacVer),

    CommitIndex = max(LastApplied, FirstIndex),
    Cfg = #cfg{id = Id,
               uid = UId,
               log_id = LogId,
               metrics_key = MetricKey,
               machine = Machine,
               machine_version = LatestMacVer,
               machine_versions = [{SnapshotIdx, MacVer}],
               effective_machine_version = MacVer,
               effective_machine_module = MacMod,
               max_pipeline_count = MaxPipelineCount,
               counter = maps:get(counter, Config, undefined),
               system_config = SystemConfig},

    #{cfg => Cfg,
      current_term => CurrentTerm,
      cluster => Cluster0,
      % There may be scenarios when a single server
      % starts up but hasn't
      % yet re-applied its noop command that we may receive other join
      % commands that can't be applied.
      cluster_change_permitted => false,
      cluster_index_term => SnapshotIndexTerm,
      voted_for => VotedFor,
      commit_index => CommitIndex,
      %% set this to the first index so that we can apply all entries
      %% up to the commit index during recovery
      last_applied => FirstIndex,
      persisted_last_applied => LastApplied,
      log => Log0,
      machine_state => MacState,
      %% aux state is transient and needs to be initialized every time
      aux_state => ra_machine:init_aux(MacMod, Name),
      query_index => 0,
      queries_waiting_heartbeats => queue:new(),
      pending_consistent_queries => []}.

recover(#{cfg := #cfg{log_id = LogId,
                      machine_version = MacVer,
                      effective_machine_version = EffMacVer},
          commit_index := CommitIndex,
          last_applied := LastApplied} = State0) ->
    ?DEBUG("~s: recovering state machine version ~b:~b from index ~b to ~b",
           [LogId,  EffMacVer, MacVer, LastApplied, CommitIndex]),
    Before = erlang:system_time(millisecond),
    {#{log := Log0} = State, _} =
        apply_to(CommitIndex,
                 fun(E, S) ->
                         %% Clear out the effects to avoid building
                         %% up a long list of effects than then
                         %% we throw away
                         %% on server startup (queue recovery)
                         setelement(5, apply_with(E, S), [])
                 end,
                 State0, []),
    After = erlang:system_time(millisecond),
    ?DEBUG("~s: recovery of state machine version ~b:~b "
           "from index ~b to ~b took ~bms",
           [LogId,  EffMacVer, MacVer, LastApplied, CommitIndex, After - Before]),
    %% disable segment read cache by setting random accesss pattern
    Log = ra_log:release_resources(1, random, Log0),
    State#{log => Log,
           %% reset commit latency as recovery may calculate a very old value
           commit_latency => 0}.

-spec handle_leader(ra_msg(), ra_server_state()) ->
    {ra_state(), ra_server_state(), effects()}.
handle_leader({PeerId, #append_entries_reply{term = Term, success = true,
                                             next_index = NextIdx,
                                             last_index = LastIdx}},
              #{current_term := Term,
                cfg := #cfg{id = Id,
                            log_id = LogId} = Cfg} = State0) ->
    ok = incr_counter(Cfg, ?C_RA_SRV_AER_REPLIES_SUCCESS, 1),
    case peer(PeerId, State0) of
        undefined ->
            ?WARN("~s: saw append_entries_reply from unknown peer ~w",
                  [LogId, PeerId]),
            {leader, State0, []};
        Peer0 = #{match_index := MI, next_index := NI} ->
            Peer = Peer0#{match_index => max(MI, LastIdx),
                          next_index => max(NI, NextIdx)},
            State1 = put_peer(PeerId, Peer, State0),
            {State2, Effects0} = evaluate_quorum(State1, []),

            {State3, Effects1} = process_pending_consistent_queries(State2,
                                                                    Effects0),

            {State, More, RpcEffects0} = make_pipelined_rpc_effects(State3, []),
            % rpcs need to be issued _AFTER_ machine effects or there is
            % a chance that effects will never be issued if the leader crashes
            % after sending rpcs but before actioning the machine effects
            RpcEffects = case More of
                             true ->
                                 [{next_event, info, pipeline_rpcs} |
                                  RpcEffects0];
                             false ->
                                 RpcEffects0
                         end,
            Effects = Effects1 ++ RpcEffects,
            case State of
                #{cluster := #{Id := _}} ->
                    % leader is in the cluster
                    {leader, State, Effects};
                #{commit_index := CI,
                  cluster_index_term := {CITIndex, _}}
                  when CI >= CITIndex ->
                    % leader is not in the cluster and the new cluster
                    % config has been committed
                    % time to say goodbye
                    ?INFO("~s: leader not in new cluster - goodbye", [LogId]),
                    {stop, State, Effects};
                _ ->
                    {leader, State, Effects}
            end
    end;
handle_leader({PeerId, #append_entries_reply{term = Term}},
              #{current_term := CurTerm,
                cfg := #cfg{log_id = LogId}} = State0)
  when Term > CurTerm ->
    case peer(PeerId, State0) of
        undefined ->
            ?WARN("~s: saw append_entries_reply from unknown peer ~w",
                  [LogId, PeerId]),
            {leader, State0, []};
        _ ->
            ?NOTICE("~s: leader saw append_entries_reply from ~w for term ~b "
                    "abdicates term: ~b!",
                    [LogId, PeerId, Term, CurTerm]),
            {follower, update_term(Term, State0#{leader_id => undefined}), []}
    end;
handle_leader({PeerId, #append_entries_reply{success = false,
                                             next_index = NextIdx,
                                             last_index = LastIdx,
                                             last_term = LastTerm}},
              State0 = #{cfg := #cfg{log_id = LogId} = Cfg,
                         cluster := Nodes, log := Log0}) ->
    ok = incr_counter(Cfg, ?C_RA_SRV_AER_REPLIES_FAILED, 1),
    #{PeerId := Peer0 = #{match_index := MI,
                          next_index := NI}} = Nodes,
    % if the last_index exists and has a matching term we can forward
    % match_index and update next_index directly
    {Peer, Log} = case ra_log:fetch_term(LastIdx, Log0) of
                      {undefined, L} ->
                          % entry was not found - simply set next index to
                          ?DEBUG("~s: setting next index for ~w ~b",
                                 [LogId, PeerId, NextIdx]),
                          {Peer0#{match_index => LastIdx,
                                  next_index => NextIdx}, L};
                      % entry exists we can forward
                      {LastTerm, L} when LastIdx >= MI ->
                          ?DEBUG("~s: setting last index to ~b, "
                                 " next_index ~b for ~w",
                                 [LogId, LastIdx, NextIdx, PeerId]),
                          {Peer0#{match_index => LastIdx,
                                  next_index => NextIdx}, L};
                      {_Term, L} when LastIdx < MI ->
                          % TODO: this can only really happen when peers are
                          % non-persistent.
                          % should they turn-into non-voters when this sitution
                          % is detected
                          ?WARN("~s: leader saw peer with last_index [~b in ~b]"
                                " lower than recorded match index [~b]."
                                "Resetting peer's state to last_index.",
                                [LogId, LastIdx, LastTerm, MI]),
                          {Peer0#{match_index => LastIdx,
                                  next_index => LastIdx + 1}, L};
                      {_EntryTerm, L} ->
                          NextIndex = max(min(NI-1, LastIdx), MI),
                          ?DEBUG("~s: leader received last_index ~b"
                                 " from ~w with term ~b "
                                 "- expected term ~b. Setting"
                                 "next_index to ~b",
                                 [LogId, LastIdx, PeerId, LastTerm, _EntryTerm,
                                  NextIndex]),
                          % last_index has a different term or entry does not
                          % exist
                          % The peer must have received an entry from a previous
                          % leader and the current leader wrote a different
                          % entry at the same index in a different term.
                          % decrement next_index but don't go lower than
                          % match index.
                          {Peer0#{next_index => NextIndex}, L}
                  end,
    State1 = State0#{cluster => Nodes#{PeerId => Peer}, log => Log},
    {State, _, Effects} = make_pipelined_rpc_effects(State1, []),
    {leader, State, Effects};
handle_leader({command, Cmd}, #{cfg := #cfg{log_id = LogId} = Cfg} = State00) ->
    ok = incr_counter(Cfg, ?C_RA_SRV_COMMANDS, 1),
    case append_log_leader(Cmd, State00) of
        {not_appended, Reason, State} ->
            ?WARN("~s command ~W NOT appended to log. Reason ~w",
                  [LogId, Cmd, 10, Reason]),
            Effects = case Cmd of
                          {_, #{from := From}, _, _} ->
                              [{reply, From, {error, Reason}}];
                          _ ->
                              []
                      end,
            {leader, State, Effects};
        {ok, Idx, Term, State0} ->
            {State, _, Effects0} = make_pipelined_rpc_effects(State0, []),
            % check if a reply is required.
            % TODO: refactor - can this be made a bit nicer/more explicit?
            Effects = case Cmd of
                          {_, #{from := From}, _, after_log_append} ->
                              [{reply, From,
                                {wrap_reply, {Idx, Term}}} | Effects0];
                          _ ->
                              Effects0
                      end,
            {leader, State, Effects}
    end;
handle_leader({commands, Cmds}, #{cfg := Cfg} =  State00) ->
    %% TODO: refactor to use wal batch API?
    Num = length(Cmds),
    {State0, Effects0} =
        lists:foldl(fun(C, {S0, E}) ->
                            {ok, I, T, S} = append_log_leader(C, S0),
                            case C of
                                {_, #{from := From}, _, after_log_append} ->
                                    {S, [{reply, From,
                                          {wrap_reply, {I, T}}} | E]};
                                _ ->
                                    {S, E}
                            end
                    end, {State00, []}, Cmds),
    ok = incr_counter(Cfg, ?C_RA_SRV_COMMAND_FLUSHES, 1),
    ok = incr_counter(Cfg, ?C_RA_SRV_COMMANDS, Num),
    {State, _, Effects} = make_pipelined_rpc_effects(Num, State0, Effects0),

    {leader, State, Effects};
handle_leader({ra_log_event, {written, _} = Evt}, State0 = #{log := Log0}) ->
    {Log, Effects0} = ra_log:handle_event(Evt, Log0),
    {State1, Effects1} = evaluate_quorum(State0#{log => Log}, Effects0),
    {State2, Effects2} = process_pending_consistent_queries(State1, Effects1),

    {State, _, Effects} = make_pipelined_rpc_effects(State2, Effects2),
    {leader, State, Effects};
handle_leader({ra_log_event, Evt}, State = #{log := Log0}) ->
    {Log1, Effects} = ra_log:handle_event(Evt, Log0),
    {leader, State#{log => Log1}, Effects};
handle_leader({aux_command, Type, Cmd}, State0) ->
    handle_aux(leader, Type, Cmd, State0);
handle_leader({PeerId, #install_snapshot_result{term = Term}},
              #{cfg := #cfg{log_id = LogId},
                current_term := CurTerm} = State0)
  when Term > CurTerm ->
    case peer(PeerId, State0) of
        undefined ->
            ?WARN("~s: saw install_snapshot_result from unknown peer ~w",
                  [LogId, PeerId]),
            {leader, State0, []};
        _ ->
            ?DEBUG("~s: leader saw install_snapshot_result from ~w for term ~b"
                  " abdicates term: ~b!", [LogId, PeerId, Term, CurTerm]),
            {follower, update_term(Term, State0#{leader_id => undefined}), []}
    end;
handle_leader({PeerId, #install_snapshot_result{last_index = LastIndex}},
              #{cfg := #cfg{log_id = LogId}} = State0) ->
    case peer(PeerId, State0) of
        undefined ->
            ?WARN("~s: saw install_snapshot_result from unknown peer ~w",
                  [LogId, PeerId]),
            {leader, State0, []};
        Peer0 ->
            State1 = put_peer(PeerId,
                              Peer0#{status => normal,
                                     match_index => LastIndex,
                                     commit_index_sent => LastIndex,
                                     next_index => LastIndex + 1},
                              State0),

            %% we can now demonitor the process
            Effects0 = case Peer0 of
                           #{status := {sending_snapshot, Pid}} ->
                               [{demonitor, process, Pid}];
                           _ -> []
                       end,

            {State, _, Effects} = make_pipelined_rpc_effects(State1, Effects0),
            {leader, State, Effects}
    end;
handle_leader(pipeline_rpcs, State0) ->
    {State, More, Effects0} = make_pipelined_rpc_effects(State0, []),
    Effects = case More of
                  true ->
                      [{next_event, info, pipeline_rpcs} | Effects0];
                  false ->
                      Effects0
              end,
    {leader, State, Effects};
handle_leader(#install_snapshot_rpc{term = Term,
                                    leader_id = Leader} = Evt,
              #{current_term := CurTerm,
                cfg := #cfg{log_id = LogId}} = State0)
  when Term > CurTerm ->
    case peer(Leader, State0) of
        undefined ->
            ?WARN("~s: saw install_snapshot_rpc from unknown leader ~w",
                  [LogId, Leader]),
            {leader, State0, []};
        _ ->
            ?INFO("~s: leader saw install_snapshot_rpc from ~w for term ~b "
                  "abdicates term: ~b!",
                  [LogId, Evt#install_snapshot_rpc.leader_id, Term, CurTerm]),
            {follower, update_term(Term, State0#{leader_id => undefined}),
             [{next_event, Evt}]}
    end;
handle_leader(#append_entries_rpc{term = Term} = Msg,
              #{current_term := CurTerm,
                cfg := #cfg{log_id = LogId}} = State0)
  when Term > CurTerm ->
    ?INFO("~s: leader saw append_entries_rpc from ~w for term ~b "
          "abdicates term: ~b!",
          [LogId, Msg#append_entries_rpc.leader_id,
           Term, CurTerm]),
    {follower, update_term(Term, State0#{leader_id => undefined}),
     [{next_event, Msg}]};
handle_leader(#append_entries_rpc{term = Term}, #{current_term := Term,
                                                  cfg := #cfg{log_id = LogId}}) ->
    ?ERR("~s: leader saw append_entries_rpc for same term ~b"
         " this should not happen!", [LogId, Term]),
    exit(leader_saw_append_entries_rpc_in_same_term);
handle_leader(#append_entries_rpc{leader_id = LeaderId},
              #{current_term := CurTerm,
                cfg := #cfg{id = Id}} = State0) ->
    Reply = append_entries_reply(CurTerm, false, State0),
    {leader, State0, [cast_reply(Id, LeaderId, Reply)]};
handle_leader({consistent_query, From, QueryFun},
              #{commit_index := CommitIndex,
                cluster_change_permitted := true} = State0) ->
    QueryRef = {From, QueryFun, CommitIndex},
    {State1, Effects} = make_heartbeat_rpc_effects(QueryRef, State0),
    {leader, State1, Effects};
handle_leader({consistent_query, From, QueryFun},
              #{commit_index := CommitIndex,
                cluster_change_permitted := false,
                pending_consistent_queries := PQ} = State0) ->
    QueryRef = {From, QueryFun, CommitIndex},
    {leader, State0#{pending_consistent_queries => [QueryRef | PQ]}, []};
%% Lihtweight version of append_entries_rpc
handle_leader(#heartbeat_rpc{term = Term} = Msg,
              #{current_term := CurTerm,
                cfg := #cfg{log_id = LogId}} = State0)
        when CurTerm < Term ->
    ?INFO("~s: leader saw heartbeat_rpc from ~w for term ~b "
          "abdicates term: ~b!",
          [LogId, Msg#heartbeat_rpc.leader_id,
           Term, CurTerm]),
    {follower, update_term(Term, State0#{leader_id => undefined}),
     [{next_event, Msg}]};
handle_leader(#heartbeat_rpc{term = Term, leader_id = LeaderId},
              #{current_term := CurTerm,
                cfg := #cfg{id = Id}} = State)
        when CurTerm > Term ->
    Reply = heartbeat_reply(State),
    {leader, State, [cast_reply(Id, LeaderId, Reply)]};
handle_leader(#heartbeat_rpc{term = Term},
              #{current_term := CurTerm, cfg := #cfg{log_id = LogId}})
  when CurTerm == Term ->
    ?ERR("~s: leader saw heartbeat_rpc for same term ~b"
         " this should not happen!", [LogId, Term]),
    exit(leader_saw_heartbeat_rpc_in_same_term);
handle_leader({PeerId, #heartbeat_reply{query_index = ReplyQueryIndex,
                                        term = Term}},
              #{current_term := CurTerm,
                cfg := #cfg{log_id = LogId}} = State0) ->
    case {CurTerm, Term} of
        {Same, Same} ->
            %% Heartbeat confirmed
            case heartbeat_rpc_quorum(ReplyQueryIndex, PeerId, State0) of
                {[], State} ->
                    {leader, State, []};
                {QueryRefs, State} ->
                    Effects = apply_consistent_queries_effects(QueryRefs, State),
                    {leader, State, Effects}
            end;
        {CurHigher, TermLower} when CurHigher > TermLower ->
            %% Heartbeat reply for lower term. Ignoring
            {leader, State0, []};
        {CurLower, TermHigher} when CurLower < TermHigher ->
            %% A node with higher term confirmed heartbeat. This should not happen
            ?NOTICE("~s leader saw heartbeat_reply from ~w for term ~b "
                    "abdicates term: ~b!",
                    [LogId, PeerId, Term, CurTerm]),
            {follower, update_term(Term, State0#{leader_id => undefined}), []}
    end;
handle_leader(#request_vote_rpc{term = Term, candidate_id = Cand} = Msg,
              #{current_term := CurTerm,
                cfg := #cfg{log_id = LogId}} = State0) when Term > CurTerm ->
    case peer(Cand, State0) of
        undefined ->
            ?WARN("~s: leader saw request_vote_rpc for unknown peer ~w",
                  [LogId, Cand]),
            {leader, State0, []};
        _ ->
            ?INFO("~s: leader saw request_vote_rpc from ~w for term ~b "
                  "abdicates term: ~b!",
                  [LogId, Msg#request_vote_rpc.candidate_id, Term, CurTerm]),
            {follower, update_term(Term, State0#{leader_id => undefined}),
             [{next_event, Msg}]}
    end;
handle_leader(#request_vote_rpc{}, State = #{current_term := Term}) ->
    Reply = #request_vote_result{term = Term, vote_granted = false},
    {leader, State, [{reply, Reply}]};
handle_leader(#pre_vote_rpc{term = Term, candidate_id = Cand} = Msg,
              #{current_term := CurTerm,
                cfg := #cfg{log_id = LogId}} = State0) when Term > CurTerm ->
    case peer(Cand, State0) of
        undefined ->
            ?WARN("~s: leader saw pre_vote_rpc for unknown peer ~w",
                  [LogId, Cand]),
            {leader, State0, []};
        _ ->
            ?INFO("~s: leader saw pre_vote_rpc from ~w for term ~b"
                  " abdicates term: ~b!",
                  [LogId, Msg#pre_vote_rpc.candidate_id, Term, CurTerm]),
            {follower, update_term(Term, State0#{leader_id => undefined}),
             [{next_event, Msg}]}
    end;
handle_leader(#pre_vote_rpc{term = Term},
              #{current_term := CurTerm} = State0)
  when Term =< CurTerm ->
    % enforce leadership
    {State, Effects} = make_all_rpcs(State0),
    {leader, State, Effects};
handle_leader(#request_vote_result{}, State) ->
    %% handle to avoid logging as unhandled
    {leader, State, []};
handle_leader(#pre_vote_result{}, State) ->
    %% handle to avoid logging as unhandled
    {leader, State, []};
handle_leader({transfer_leadership, Leader},
              #{cfg := #cfg{id = Leader, log_id = LogId}} = State) ->
    ?DEBUG("~s: transfer leadership requested but already leader",
           [LogId]),
    {leader, State, [{reply, already_leader}]};
handle_leader({transfer_leadership, Member},
              #{cfg := #cfg{log_id = LogId},
                cluster := Members} = State)
  when not is_map_key(Member, Members) ->
    ?DEBUG("~s: transfer leadership requested but unknown member ~w",
           [LogId, Member]),
    {leader, State, [{reply, {error, unknown_member}}]};
handle_leader({transfer_leadership, ServerId},
              #{cfg := #cfg{log_id = LogId}} = State) ->
    ?DEBUG("~s: transfer leadership to ~w requested",
           [LogId, ServerId]),
    %% TODO find a timeout
    gen_statem:cast(ServerId, try_become_leader),
    {await_condition,
     State#{condition => fun transfer_leadership_condition/2,
            condition_timeout_changes => #{effects => [],
                                           transition_to => leader}},
     [{reply, ok}]};
handle_leader({register_external_log_reader, Pid}, #{log := Log0} = State) ->
    {Log, Effs} = ra_log:register_reader(Pid, Log0),
    {leader, State#{log => Log}, Effs};
handle_leader(Msg, State) ->
    log_unhandled_msg(leader, Msg, State),
    {leader, State, []}.


-spec handle_candidate(ra_msg() | election_timeout, ra_server_state()) ->
    {ra_state(), ra_server_state(), effects()}.
handle_candidate(#request_vote_result{term = Term, vote_granted = true},
                 #{cfg := #cfg{id = Id,
                               log_id = LogId,
                               machine = Mac},
                   current_term := Term,
                   votes := Votes,
                   cluster := Nodes} = State0) ->
    NewVotes = Votes + 1,
    ?DEBUG("~s: vote granted for term ~b votes ~b",
          [LogId, Term, NewVotes]),
    case trunc(maps:size(Nodes) / 2) + 1 of
        NewVotes ->
            {State1, Effects} = make_all_rpcs(initialise_peers(State0)),
            Noop = {noop, #{ts => erlang:system_time(millisecond)},
                    ra_machine:version(Mac)},
            State = State1#{leader_id => Id},
            {leader, maps:without([votes], State),
             [{next_event, cast, {command, Noop}} | Effects]};
        _ ->
            {candidate, State0#{votes => NewVotes}, []}
    end;
handle_candidate(#request_vote_result{term = Term},
                 #{current_term := CurTerm,
                   cfg := #cfg{log_id = LogId}} = State0)
  when Term > CurTerm ->
    ?INFO("~s: candidate request_vote_result with higher term"
           " received ~b -> ~b", [LogId, CurTerm, Term]),
    State = update_term_and_voted_for(Term, undefined, State0),
    {follower, State, []};
handle_candidate(#request_vote_result{vote_granted = false}, State) ->
    {candidate, State, []};
handle_candidate(#append_entries_rpc{term = Term} = Msg,
                 #{current_term := CurTerm} = State0) when Term >= CurTerm ->
    State = update_term_and_voted_for(Term, undefined, State0),
    {follower, State, [{next_event, Msg}]};
handle_candidate(#append_entries_rpc{leader_id = LeaderId},
                 #{current_term := CurTerm} = State) ->
    % term must be older return success=false
    Reply = append_entries_reply(CurTerm, false, State),
    {candidate, State, [{cast, LeaderId, {id(State), Reply}}]};
handle_candidate(#heartbeat_rpc{term = Term} = Msg,
                 #{current_term := CurTerm} = State0) when Term >= CurTerm ->
    State = update_term_and_voted_for(Term, undefined, State0),
    {follower, State, [{next_event, Msg}]};
handle_candidate(#heartbeat_rpc{leader_id = LeaderId}, State) ->
    % term must be older return success=false
    Reply = heartbeat_reply(State),
    {candidate, State, [cast_reply(id(State), LeaderId, Reply)]};
handle_candidate({_PeerId, #heartbeat_reply{term = Term}},
                 #{cfg := #cfg{log_id = LogId},
                   current_term := CurTerm} = State0) when Term > CurTerm ->
    ?INFO("~s: candidate heartbeat_reply with higher"
          " term received ~b -> ~b",
          [LogId, CurTerm, Term]),
    State = update_term_and_voted_for(Term, undefined, State0),
    {follower, State, []};
handle_candidate({_PeerId, #append_entries_reply{term = Term}},
                 #{current_term := CurTerm,
                   cfg := #cfg{log_id = LogId}} = State0)
  when Term > CurTerm ->
    ?INFO("~s: candidate append_entries_reply with higher"
          " term received ~b -> ~b",
          [LogId, CurTerm, Term]),
    State = update_term_and_voted_for(Term, undefined, State0),
    {follower, State, []};
handle_candidate(#request_vote_rpc{term = Term} = Msg,
                 #{current_term := CurTerm,
                   cfg := #cfg{log_id = LogId}} = State0)
  when Term > CurTerm ->
    ?INFO("~s: candidate request_vote_rpc with higher term received ~b -> ~b",
          [LogId, CurTerm, Term]),
    State = update_term_and_voted_for(Term, undefined, State0),
    {follower, State, [{next_event, Msg}]};
handle_candidate(#pre_vote_rpc{term = Term} = Msg,
                 #{current_term := CurTerm,
                   cfg := #cfg{log_id = LogId}} = State0)
  when Term > CurTerm ->
    ?INFO("~s: candidate pre_vote_rpc with higher term received ~b -> ~b",
          [LogId, CurTerm, Term]),
    State = update_term_and_voted_for(Term, undefined, State0),
    {follower, State, [{next_event, Msg}]};
handle_candidate(#request_vote_rpc{}, State = #{current_term := Term}) ->
    Reply = #request_vote_result{term = Term, vote_granted = false},
    {candidate, State, [{reply, Reply}]};
handle_candidate(#pre_vote_rpc{}, State) ->
    %% just ignore pre_votes that aren't of a higher term
    {candidate, State, []};
handle_candidate(#request_vote_result{}, State) ->
    %% handle to avoid logging as unhandled
    {candidate, State, []};
handle_candidate(#pre_vote_result{}, State) ->
    %% handle to avoid logging as unhandled
    {candidate, State, []};
handle_candidate({ra_log_event, Evt}, State = #{log := Log0}) ->
    % simply forward all other events to ra_log
    {Log, Effects} = ra_log:handle_event(Evt, Log0),
    {candidate, State#{log => Log}, Effects};
handle_candidate(election_timeout, State) ->
    call_for_election(candidate, State);
handle_candidate({register_external_log_reader, Pid}, #{log := Log0} = State) ->
    {Log, Effs} = ra_log:register_reader(Pid, Log0),
    {candidate, State#{log => Log}, Effs};
handle_candidate(Msg, State) ->
    log_unhandled_msg(candidate, Msg, State),
    {candidate, State, []}.

-spec handle_pre_vote(ra_msg(), ra_server_state()) ->
    {ra_state(), ra_server_state(), effects()}.
handle_pre_vote(#append_entries_rpc{term = Term} = Msg,
                #{current_term := CurTerm} = State0)
  when Term >= CurTerm ->
    State = update_term(Term, State0),
    % revert to follower state
    {follower, State#{votes => 0}, [{next_event, Msg}]};
handle_pre_vote(#heartbeat_rpc{term = Term} = Msg,
                #{current_term := CurTerm} = State0)
  when Term >= CurTerm ->
    State = update_term(Term, State0),
    % revert to follower state
    {follower, State#{votes => 0}, [{next_event, Msg}]};
handle_pre_vote(#heartbeat_rpc{leader_id = LeaderId}, State) ->
    % term must be older return success=false
    Reply = heartbeat_reply(State),
    {pre_vote, State, [cast_reply(id(State), LeaderId, Reply)]};
handle_pre_vote({_PeerId, #heartbeat_reply{term = Term}},
                #{current_term := CurTerm} = State) when Term > CurTerm ->
    {follower, update_term(Term, State#{votes => 0}), []};
handle_pre_vote(#request_vote_rpc{term = Term} = Msg,
                #{current_term := CurTerm} = State0)
  when Term > CurTerm ->
    State = update_term(Term, State0),
    % revert to follower state
    {follower, State#{votes => 0}, [{next_event, Msg}]};
handle_pre_vote(#pre_vote_result{term = Term},
                #{current_term := CurTerm} = State0)
  when Term > CurTerm ->
    % higher term always reverts?
    State = update_term(Term, State0),
    {follower, State#{votes => 0}, []};
handle_pre_vote(#install_snapshot_rpc{term = Term} = ISR,
                #{current_term := CurTerm} = State0)
  when Term >= CurTerm ->
    {follower, State0#{votes => 0}, [{next_event, ISR}]};
handle_pre_vote(#pre_vote_result{term = Term, vote_granted = true,
                                 token = Token},
                #{current_term := Term,
                  votes := Votes,
                  cfg := #cfg{log_id = LogId},
                  pre_vote_token := Token,
                  cluster := Nodes} = State0) ->
    ?DEBUG("~s: pre_vote granted ~w for term ~b votes ~b",
          [LogId, Token, Term, Votes + 1]),
    NewVotes = Votes + 1,
    State = update_term(Term, State0),
    case trunc(maps:size(Nodes) / 2) + 1 of
        NewVotes ->
            call_for_election(candidate, State);
        _ ->
            {pre_vote, State#{votes => NewVotes}, []}
    end;
handle_pre_vote(#pre_vote_result{vote_granted = false}, State) ->
    %% just handle negative results to avoid printing an unhandled message log
    {pre_vote, State, []};
handle_pre_vote(#pre_vote_rpc{} = PreVote, State) ->
    process_pre_vote(pre_vote, PreVote, State);
handle_pre_vote(#request_vote_result{}, State) ->
    %% handle to avoid logging as unhandled
    {pre_vote, State, []};
handle_pre_vote(#pre_vote_result{}, State) ->
    %% handle to avoid logging as unhandled
    {pre_vote, State, []};
handle_pre_vote(election_timeout, State) ->
    call_for_election(pre_vote, State);
handle_pre_vote({ra_log_event, Evt}, State = #{log := Log0}) ->
    % simply forward all other events to ra_log
    {Log, Effects} = ra_log:handle_event(Evt, Log0),
    {pre_vote, State#{log => Log}, Effects};
handle_pre_vote({register_external_log_reader, Pid}, #{log := Log0} = State) ->
    {Log, Effs} = ra_log:register_reader(Pid, Log0),
    {pre_vote, State#{log => Log}, Effs};
handle_pre_vote(Msg, State) ->
    log_unhandled_msg(pre_vote, Msg, State),
    {pre_vote, State, []}.


-spec handle_follower(ra_msg(), ra_server_state()) ->
    {ra_state(), ra_server_state(), effects()}.
handle_follower(#append_entries_rpc{term = Term,
                                    leader_id = LeaderId,
                                    leader_commit = LeaderCommit,
                                    prev_log_index = PLIdx,
                                    prev_log_term = PLTerm,
                                    entries = Entries0},
                State00 = #{cfg := #cfg{log_id = LogId,
                                        id = Id} = Cfg,
                            log := Log00,
                            current_term := CurTerm})
  when Term >= CurTerm ->
    ok = incr_counter(Cfg, ?C_RA_SRV_AER_RECEIVED_FOLLOWER, 1),
    %% this is a valid leader, append entries message
    Effects0 = [{record_leader_msg, LeaderId}],
    State0 = update_term(Term, State00#{leader_id => LeaderId,
                                        commit_index => LeaderCommit}),
    case has_log_entry_or_snapshot(PLIdx, PLTerm, Log00) of
        {entry_ok, Log0} ->
            % filter entries already seen
            {Log1, Entries} = drop_existing({Log0, Entries0}),
            case Entries of
                [] ->
                    ok = incr_counter(Cfg, ?C_RA_SRV_AER_RECEIVED_FOLLOWER_EMPTY, 1),
                    LastIdx = ra_log:last_index_term(Log1),
                    Log2 = case Entries0 of
                               [] when element(1, LastIdx) > PLIdx ->
                                   %% if no entries were sent we need to reset
                                   %% last index to match the leader
                                   ?DEBUG("~s: resetting last index to ~b",
                                         [LogId, PLIdx]),
                                   {ok, L} = ra_log:set_last_index(PLIdx, Log1),
                                   L;
                               _ ->
                                   Log1
                           end,
                    %% if nothing was appended we need to send a reply here
                    State1 = State0#{log => Log2},
                    % evaluate commit index as we may have received an updated
                    % commit_index for previously written entries
                    {NextState, State, Effects} =
                         evaluate_commit_index_follower(State1, Effects0),
                    % TODO: only send a reply if there is no pending write
                    % between the follower and the wal as the written event
                    % will trigger a reply anyway
                    Reply = append_entries_reply(Term, true, State),
                    {NextState, State,
                     [cast_reply(Id, LeaderId, Reply) | Effects]};
                _ ->
                    State = lists:foldl(fun pre_append_log_follower/2,
                                        State0, Entries),
                    case ra_log:write(Entries, Log1) of
                        {ok, Log} ->
                            evaluate_commit_index_follower(State#{log => Log},
                                                           Effects0);
                        {error, wal_down} ->
                            {await_condition,
                             State#{log => Log1,
                                    condition => fun wal_down_condition/2},
                             Effects0};
                        {error, _} = Err ->
                            exit(Err)
                    end
            end;
        {missing, Log0} ->
            State = State0#{log => Log0},
            Reply = append_entries_reply(Term, false, State),
            ?INFO("~s: follower did not have entry at ~b in ~b."
                  " Requesting ~w from ~b",
                  [LogId, PLIdx, PLTerm, LeaderId,
                   Reply#append_entries_reply.next_index]),
            Effects = [cast_reply(Id, LeaderId, Reply) | Effects0],
            {await_condition,
             State#{condition => follower_catchup_cond_fun(missing),
                    % repeat reply effect on condition timeout
                    condition_timeout_changes => #{effects => Effects,
                                                   transition_to => follower}},
             Effects};
        {term_mismatch, OtherTerm, Log0} ->
            %% NB: this is the commit index before update
            LastApplied = maps:get(last_applied, State00),
            ?INFO("~s: term mismatch - follower had entry at ~b with term ~b "
                  "but not with term ~b~n"
                  "Asking leader ~w to resend from ~b",
                  [LogId, PLIdx, OtherTerm, PLTerm, LeaderId, LastApplied + 1]),
            % This situation arises when a minority leader replicates entries
            % that it cannot commit then gets replaced by a majority leader
            % that also has made progress
            % As the follower is responsible for telling the leader
            % which their next expected entry is the best we can do here
            % is rewind back and use the commit index as the last index
            % and commit_index + 1 as the next expected.
            % This _may_ overwrite some valid entries but is probably the
            % simplest way to proceed
            {Reply, State} = mismatch_append_entries_reply(Term, LastApplied,
                                                           State0),
            Effects = [cast_reply(Id, LeaderId, Reply) | Effects0],
            {await_condition,
             State#{log => Log0,
                    condition => follower_catchup_cond_fun(term_mismatch),
                    % repeat reply effect on condition timeout
                    condition_timeout_changes => #{effects => Effects,
                                                   transition_to => follower}},
             Effects}
    end;
handle_follower(#append_entries_rpc{term = _Term, leader_id = LeaderId},
                #{cfg := #cfg{id = Id, log_id = LogId} = Cfg,
                  current_term := CurTerm} = State) ->
    ok = incr_counter(Cfg, ?C_RA_SRV_AER_RECEIVED_FOLLOWER, 1),
    % the term is lower than current term
    Reply = append_entries_reply(CurTerm, false, State),
    ?DEBUG("~s: follower got append_entries_rpc from ~w in"
           " ~b but current term is: ~b",
          [LogId, LeaderId, _Term, CurTerm]),
    {follower, State, [cast_reply(Id, LeaderId, Reply)]};
handle_follower(#heartbeat_rpc{query_index = RpcQueryIndex, term = Term,
                               leader_id = LeaderId},
                #{current_term := CurTerm,
                  cfg := #cfg{id = Id}} = State0)
  when Term >= CurTerm ->
    State1 = update_term(Term, State0),
    #{query_index := QueryIndex} = State1,
    NewQueryIndex = max(RpcQueryIndex, QueryIndex),
    State2 = update_query_index(State1#{leader_id => LeaderId}, NewQueryIndex),
    Reply = heartbeat_reply(State2),
    {follower, State2, [cast_reply(Id, LeaderId, Reply)]};
handle_follower(#heartbeat_rpc{leader_id = LeaderId},
                #{cfg := #cfg{id = Id}} = State)->
    Reply = heartbeat_reply(State),
    {follower, State, [cast_reply(Id, LeaderId, Reply)]};
handle_follower({ra_log_event, {written, _} = Evt},
                State0 = #{log := Log0,
                           cfg := #cfg{id = Id},
                           leader_id := LeaderId,
                           current_term := Term})
  when LeaderId =/= undefined ->
    {Log, Effects} = ra_log:handle_event(Evt, Log0),
    State = State0#{log => Log},
    Reply = append_entries_reply(Term, true, State),
    {follower, State, [cast_reply(Id, LeaderId, Reply) | Effects]};
handle_follower({ra_log_event, Evt}, State = #{log := Log0}) ->
    % simply forward all other events to ra_log
    {Log, Effects} = ra_log:handle_event(Evt, Log0),
    {follower, State#{log => Log}, Effects};
handle_follower(#pre_vote_rpc{} = PreVote, State) ->
    process_pre_vote(follower, PreVote, State);
handle_follower(#request_vote_rpc{candidate_id = Cand, term = Term},
                #{current_term := Term, voted_for := VotedFor,
                  cfg := #cfg{log_id = LogId}} = State)
  when VotedFor /= undefined andalso VotedFor /= Cand ->
    % already voted for another in this term
    ?DEBUG("~w: follower request_vote_rpc for ~w already voted for ~w in ~b",
           [LogId, Cand, VotedFor, Term]),
    Reply = #request_vote_result{term = Term, vote_granted = false},
    {follower, State, [{reply, Reply}]};
handle_follower(#request_vote_rpc{term = Term, candidate_id = Cand,
                                  last_log_index = LLIdx,
                                  last_log_term = LLTerm},
                #{current_term := CurTerm,
                  cfg := #cfg{log_id = LogId}} = State0)
  when Term >= CurTerm ->
    State1 = update_term(Term, State0),
    LastIdxTerm = last_idx_term(State1),
    case is_candidate_log_up_to_date(LLIdx, LLTerm, LastIdxTerm) of
        true ->
            ?INFO("~s: granting vote for ~w with last indexterm ~w"
                  " for term ~b previous term was ~b",
                  [LogId, Cand, {LLIdx, LLTerm}, Term, CurTerm]),
            Reply = #request_vote_result{term = Term, vote_granted = true},
            State = update_term_and_voted_for(Term, Cand, State1),
            {follower, State#{voted_for => Cand, current_term => Term},
             [{reply, Reply}]};
        false ->
            ?INFO("~s: declining vote for ~w for term ~b,"
                  " candidate last log index term was: ~w~n"
                  " last log entry idxterm seen was: ~w",
                  [LogId, Cand, Term, {LLIdx, LLTerm}, {LastIdxTerm}]),
            Reply = #request_vote_result{term = Term, vote_granted = false},
            {follower, State1#{current_term => Term}, [{reply, Reply}]}
    end;
handle_follower(#request_vote_rpc{term = Term, candidate_id = _Cand},
                State = #{current_term := CurTerm,
                          cfg := #cfg{log_id = LogId}})
  when Term < CurTerm ->
    ?INFO("~s: declining vote to ~w for term ~b, current term ~b",
          [LogId, _Cand, Term, CurTerm]),
    Reply = #request_vote_result{term = CurTerm, vote_granted = false},
    {follower, State, [{reply, Reply}]};
handle_follower({_PeerId, #append_entries_reply{term = TheirTerm}},
                State = #{current_term := CurTerm}) ->
    Term = max(TheirTerm, CurTerm),
    {follower, update_term(Term, State), []};
handle_follower({_PeerId, #heartbeat_reply{term = TheirTerm}},
                State = #{current_term := CurTerm}) ->
    Term = max(TheirTerm, CurTerm),
    {follower, update_term(Term, State), []};
handle_follower(#install_snapshot_rpc{term = Term,
                                      meta = #{index := LastIndex,
                                               term := LastTerm}},
                State = #{cfg := #cfg{log_id = LogId}, current_term := CurTerm})
  when Term < CurTerm ->
    ?DEBUG("~s: install_snapshot old term ~b in ~b",
          [LogId, LastIndex, LastTerm]),
    % follower receives a snapshot from an old term
    Reply = #install_snapshot_result{term = CurTerm,
                                     last_term = LastTerm,
                                     last_index = LastIndex},
    {follower, State, [{reply, Reply}]};
%% need to check if it's the first or last rpc
%% TODO: must abort pending if for some reason we need to do so
handle_follower(#install_snapshot_rpc{term = Term,
                                      meta = #{index := SnapIdx} = Meta,
                                      leader_id = LeaderId,
                                      chunk_state = {1, _ChunkFlag}} = Rpc,
                #{cfg := #cfg{log_id = LogId}, log := Log0,
                  last_applied := LastApplied,
                  current_term := CurTerm} = State0)
  when Term >= CurTerm andalso SnapIdx > LastApplied ->
    %% only begin snapshot procedure if Idx is higher than the last_applied
    %% index.
    ?DEBUG("~s: begin_accept snapshot at index ~b in term ~b",
           [LogId, SnapIdx, Term]),
    SnapState0 = ra_log:snapshot_state(Log0),
    {ok, SS} = ra_snapshot:begin_accept(Meta, SnapState0),
    Log = ra_log:set_snapshot_state(SS, Log0),
    {receive_snapshot, State0#{log => Log,
                               leader_id => LeaderId},
     [{next_event, Rpc}, {record_leader_msg, LeaderId}]};
handle_follower(#request_vote_result{}, State) ->
    %% handle to avoid logging as unhandled
    {follower, State, []};
handle_follower(#pre_vote_result{}, State) ->
    %% handle to avoid logging as unhandled
    {follower, State, []};
handle_follower(#append_entries_reply{}, State) ->
    %% handle to avoid logging as unhandled
    %% could receive a lot of these shortly after standing down as leader
    {follower, State, []};
handle_follower(election_timeout, State) ->
    call_for_election(pre_vote, State);
handle_follower(try_become_leader, State) ->
    call_for_election(pre_vote, State);
handle_follower({register_external_log_reader, Pid}, #{log := Log0} = State) ->
    {Log, Effs} = ra_log:register_reader(Pid, Log0),
    {follower, State#{log => Log}, Effs};
handle_follower(Msg, State) ->
    log_unhandled_msg(follower, Msg, State),
    {follower, State, []}.

handle_receive_snapshot(#install_snapshot_rpc{term = Term,
                                              meta = #{index := LastIndex,
                                                       term := LastTerm},
                                              chunk_state = {Num, ChunkFlag},
                                              data = Data},
                        #{cfg := #cfg{id = Id, log_id = LogId},
                          log := Log0,
                          current_term := CurTerm} = State0)
  when Term >= CurTerm ->
    ?DEBUG("~s: receiving snapshot chunk: ~b / ~w",
           [LogId, Num, ChunkFlag]),
    SnapState0 = ra_log:snapshot_state(Log0),
    {ok, SnapState} = ra_snapshot:accept_chunk(Data, Num, ChunkFlag,
                                               SnapState0),
    Reply = #install_snapshot_result{term = CurTerm,
                                     last_term = LastTerm,
                                     last_index = LastIndex},
    case ChunkFlag of
        last ->
            %% this is the last chunk so we can "install" it
            {Log, Effs} = ra_log:install_snapshot({LastIndex, LastTerm},
                                                  SnapState, Log0),
            {#{cluster := ClusterIds}, MacState} = ra_log:recover_snapshot(Log),
            State = State0#{log => Log,
                            current_term => Term,
                            commit_index => LastIndex,
                            last_applied => LastIndex,
                            cluster => make_cluster(Id, ClusterIds),
                            machine_state => MacState},
            %% it was the last snapshot chunk so we can revert back to
            %% follower status
            {follower, persist_last_applied(State), [{reply, Reply} | Effs]};
        next ->
            Log = ra_log:set_snapshot_state(SnapState, Log0),
            State = State0#{log => Log},
            {receive_snapshot, State, [{reply, Reply}]}
    end;
handle_receive_snapshot({ra_log_event, Evt}, State = #{log := Log0}) ->
    % simply forward all other events to ra_log
    % whilst the snapshot is being written
    {Log, Effects} = ra_log:handle_event(Evt, Log0),
    {receive_snapshot, State#{log => Log}, Effects};
handle_receive_snapshot(receive_snapshot_timeout, #{log := Log0} = State) ->
    SnapState0 = ra_log:snapshot_state(Log0),
    SnapState = ra_snapshot:abort_accept(SnapState0),
    Log = ra_log:set_snapshot_state(SnapState, Log0),
    {follower, State#{log => Log}, []};
handle_receive_snapshot({register_external_log_reader, Pid}, #{log := Log0} = State) ->
    {Log, Effs} = ra_log:register_reader(Pid, Log0),
    {receive_snapshot, State#{log => Log}, Effs};
handle_receive_snapshot(Msg, State) ->
    log_unhandled_msg(receive_snapshot, Msg, State),
    %% drop all other events??
    %% TODO: work out what else to handle
    {receive_snapshot, State, []}.

-spec handle_await_condition(ra_msg(), ra_server_state()) ->
    {ra_state(), ra_server_state(), effects()}.
handle_await_condition(#request_vote_rpc{} = Msg, State) ->
    {follower, State, [{next_event, Msg}]};
handle_await_condition(election_timeout, State) ->
    call_for_election(pre_vote, State);
handle_await_condition(await_condition_timeout,
                       #{condition_timeout_changes := #{effects := Effects,
                                                        transition_to := TransitionTo}} = State) ->
    {TransitionTo, State#{condition_timeout_changes => #{effects => [],
                                                         transition_to => TransitionTo}}, Effects};
handle_await_condition({ra_log_event, Evt}, State = #{log := Log0}) ->
    % simply forward all other events to ra_log
    {Log, Effects} = ra_log:handle_event(Evt, Log0),
    {await_condition, State#{log => Log}, Effects};
handle_await_condition({register_external_log_reader, Pid}, #{log := Log0} = State) ->
    {Log, Effs} = ra_log:register_reader(Pid, Log0),
    {await_condition, State#{log => Log}, Effs};
handle_await_condition(Msg, #{condition := Cond} = State0) ->
    case Cond(Msg, State0) of
        {true, State} ->
            {follower, State, [{next_event, Msg}]};
        {false, State} ->
            % log_unhandled_msg(await_condition, Msg, State),
            {await_condition, State, []}
    end.

-spec process_new_leader_queries(ra_server_state()) ->
    {ra_server_state(), [from()]}.
process_new_leader_queries(#{pending_consistent_queries := Pending,
                             queries_waiting_heartbeats := Waiting} = State0) ->
    From0 = lists:map(fun({From, _, _}) -> From end, Pending),

    From1 = lists:map(fun({_, {From, _, _}}) -> From end,
                      queue:to_list(Waiting)),

    {State0#{pending_consistent_queries => [],
             queries_waiting_heartbeats => queue:new()},
     From0 ++ From1}.

-spec tick(ra_server_state()) -> effects().
tick(#{cfg := #cfg{effective_machine_module = MacMod},
       machine_state := MacState}) ->
    Now = erlang:system_time(millisecond),
    ra_machine:tick(MacMod, Now, MacState).

-spec handle_state_enter(ra_state() | eol, ra_server_state()) ->
    {ra_server_state() | eol, effects()}.
handle_state_enter(RaftState, #{cfg := #cfg{effective_machine_module = MacMod},
                                machine_state := MacState} = State) ->
    {become(RaftState, State),
     ra_machine:state_enter(MacMod, RaftState, MacState)}.


-spec overview(ra_server_state()) -> map().
overview(#{cfg := #cfg{effective_machine_module = MacMod} = Cfg,
           log := Log,
           machine_state := MacState,
           aux_state := Aux
          } = State) ->
    O0 = maps:with([current_term, commit_index, last_applied,
                    cluster, leader_id, voted_for], State),
    O = maps:merge(O0, cfg_to_map(Cfg)),
    LogOverview = ra_log:overview(Log),
    MacOverview = ra_machine:overview(MacMod, MacState),
    O#{log => LogOverview,
       aux => Aux,
       machine => MacOverview}.

cfg_to_map(Cfg) ->
    element(2, lists:foldl(
                 fun (F, {N, Acc}) ->
                         {N + 1, Acc#{F => element(N, Cfg)}}
                 end, {2, #{}}, record_info(fields, cfg))).

-spec metrics(ra_server_state()) ->
    {atom(), ra_term(),
     ra_index(), ra_index(),
     ra_index(), ra_index(), non_neg_integer()}.
metrics(#{cfg := #cfg{metrics_key = Key},
          commit_index := CI,
          last_applied := LA,
          current_term := CT,
          log := Log} = State) ->
    SnapIdx = case ra_log:snapshot_index_term(Log) of
                  undefined -> 0;
                  {I, _} -> I
              end,
    CL = case  State of
             #{commit_latency := L} ->
                 L;
             _ ->
                 0
         end,
    {LW, _} = ra_log:last_index_term(Log),
    {Key, CT, SnapIdx, LA, CI, LW, CL}.

-spec is_new(ra_server_state()) -> boolean().
is_new(#{log := Log}) ->
    ra_log:next_index(Log) =:= 1.

-spec is_fully_persisted(ra_server_state()) -> boolean().
is_fully_persisted(#{log := Log}) ->
    LastWritten = ra_log:last_written(Log),
    LastIdxTerm = ra_log:last_index_term(Log),
    LastWritten =:= LastIdxTerm.

-spec is_fully_replicated(ra_server_state()) -> boolean().
is_fully_replicated(#{commit_index := CI} = State) ->
    case maps:values(peers(State)) of
        [] -> true; % there is only one server
        Peers ->
            MinMI = lists:min([M || #{match_index := M} <- Peers]),
            MinCI = lists:min([M || #{commit_index_sent := M} <- Peers]),
            MinMI >= CI andalso MinCI >= CI
    end.

handle_aux(RaftState, Type, Cmd, #{cfg := #cfg{effective_machine_module = MacMod},
                                   aux_state := Aux0, log := Log0,
                                   machine_state := MacState0} = State0) ->
    case ra_machine:handle_aux(MacMod, RaftState, Type, Cmd, Aux0,
                               Log0, MacState0) of
        {reply, Reply, Aux, Log} ->
            {RaftState, State0#{log => Log, aux_state => Aux},
             [{reply, Reply}]};
        {reply, Reply, Aux, Log, Effects} ->
            {RaftState, State0#{log => Log, aux_state => Aux},
             [{reply, Reply} | Effects]};
        {no_reply, Aux, Log} ->
            {RaftState, State0#{log => Log, aux_state => Aux}, []};
        {no_reply, Aux, Log, Effects} ->
            {RaftState, State0#{log => Log, aux_state => Aux}, Effects};
        undefined ->
            {RaftState, State0, []}
    end.

% property helpers

-spec id(ra_server_state()) -> ra_server_id().
id(#{cfg := #cfg{id = Id}}) -> Id.

-spec log_id(ra_server_state()) -> unicode:chardata().
log_id(#{cfg := #cfg{log_id = LogId}}) -> LogId.

-spec uid(ra_server_state()) -> ra_uid().
uid(#{cfg := #cfg{uid = UId}}) -> UId.

-spec system_config(ra_server_state()) -> ra_system:config().
system_config(#{cfg := #cfg{system_config = SC}}) -> SC.

-spec leader_id(ra_server_state()) -> maybe(ra_server_id()).
leader_id(State) ->
    maps:get(leader_id, State, undefined).

-spec current_term(ra_server_state()) -> maybe(ra_term()).
current_term(State) ->
    maps:get(current_term, State).

-spec machine_version(ra_server_state()) -> non_neg_integer().
machine_version(#{cfg := #cfg{machine_version = MacVer}}) ->
    MacVer.

-spec machine_query(fun((term()) -> term()), ra_server_state()) ->
    {ra_idxterm(), term()}.
machine_query(QueryFun, #{cfg := #cfg{effective_machine_module = MacMod},
                          machine_state := MacState,
                          last_applied := Last,
                          current_term := Term
                         }) ->
    Res = ra_machine:query(MacMod, QueryFun, MacState),
    {{Last, Term}, Res}.



% Internal

become(leader, #{cluster := Cluster, log := Log0} = State) ->
    Log = ra_log:release_resources(maps:size(Cluster) + 2, random, Log0),
    State#{log => Log};
become(follower, #{log := Log0} = State) ->
    %% followers should only ever need a single segment open at any one
    %% time
    State#{log => ra_log:release_resources(1, random, Log0)};
become(_RaftState, State) ->
    State.

follower_catchup_cond_fun(OriginalReason) ->
    fun (Entry, State) ->
            follower_catchup_cond(OriginalReason, Entry, State)
    end.

follower_catchup_cond(OriginalReason,
                      #append_entries_rpc{term = Term,
                                          prev_log_index = PLIdx,
                                          prev_log_term = PLTerm},
                      State0 = #{current_term := CurTerm,
                                 log := Log0})
  when Term >= CurTerm ->
    case has_log_entry_or_snapshot(PLIdx, PLTerm, Log0) of
        {entry_ok, Log} ->
            {true, State0#{log => Log}};
        {term_mismatch, _, Log} ->
            %% if the original reason to enter catch-up was a missing entry
            %% the next entry _could_ result in a term_mismatch if so we
            %% exit await_condition temporarily to process the AppendEntriesRpc
            %% that resulted in the term_mismatch
            {OriginalReason == missing, State0#{log => Log}};
        {missing, Log} ->
            {false, State0#{log => Log}}
    end;
follower_catchup_cond(_,
                      #install_snapshot_rpc{term = Term,
                                            meta = #{index := PLIdx}},
                      #{current_term := CurTerm,
                        log := Log} = State)
  when Term >= CurTerm ->
    % term is ok - check if the snapshot index is greater than the last
    % index seen
    {PLIdx >= ra_log:next_index(Log), State};
follower_catchup_cond(_, _Msg, State) ->
    {false, State}.

wal_down_condition(_Msg, #{log := Log} = State) ->
    {ra_log:can_write(Log), State}.

transfer_leadership_condition(#append_entries_rpc{term = Term},
                              State = #{current_term := CurTerm})
  when Term > CurTerm ->
    {true, State};
transfer_leadership_condition(#install_snapshot_rpc{term = Term},
                              State = #{current_term := CurTerm})
  when Term > CurTerm ->
    {true, State};
transfer_leadership_condition(_Msg, State) ->
    {false, State}.

evaluate_commit_index_follower(#{commit_index := CommitIndex,
                                 cfg := #cfg{id = Id},
                                 leader_id := LeaderId,
                                 last_applied := LastApplied0,
                                 current_term := Term,
                                 log := Log} = State0, Effects0)
  when LeaderId =/= undefined ->
    %% take the minimum of the last index seen and the commit index
    %% This may mean we apply entries that have not yet been fsynced locally.
    %% This is ok as the append_entries_rpc with the updated commit index would
    %% ensure no uncommitted entries from a previous term have been truncated
    %% from the log
    {Idx, _} = ra_log:last_index_term(Log),
    ApplyTo = min(Idx, CommitIndex),

    % need to catch a termination throw
    case catch apply_to(ApplyTo, State0, Effects0) of
        {delete_and_terminate, State1, Effects} ->
            Reply = append_entries_reply(Term, true, State1),
            {delete_and_terminate, State1,
             [cast_reply(Id, LeaderId, Reply) |
              filter_follower_effects(Effects)]};
        {#{last_applied := LastApplied} = State, Effects1} ->
            Effects = filter_follower_effects(Effects1),
            case LastApplied > LastApplied0 of
                true ->
                    %% entries were applied, append eval_aux effect
                    {follower, State, [{aux, eval} | Effects]};
                false ->
                    %% no entries were applied
                    {follower, State, Effects}
            end
    end;
evaluate_commit_index_follower(State, Effects) ->
    %% when no leader is known
    {follower, State, Effects}.

filter_follower_effects(Effects) ->
    lists:foldr(fun ({release_cursor, _, _} = C, Acc) ->
                        [C | Acc];
                    ({record_leader_msg, _} = C, Acc) ->
                        [C | Acc];
                    ({aux, _} = C, Acc) ->
                        [C | Acc];
                    (garbage_collection = C, Acc) ->
                        [C | Acc];
                    ({delete_snapshot, _} = C, Acc) ->
                        [C | Acc];
                    ({send_msg, _, _, _Opts} = C, Acc) ->
                        %% send_msg effects _may_ have the local option
                        %% and will be evaluated properly during
                        %% effect processing
                        [C | Acc];
                    ({log, _, _, _Opts} = C, Acc) ->
                        [C | Acc];
                    ({monitor, _ProcOrNode, Comp, _} = C, Acc)
                      when Comp =/= machine ->
                        %% only machine monitors should not be emitted
                        %% by followers
                        [C | Acc];
                    (L, Acc) when is_list(L) ->
                        %% nested case - recurse
                        case filter_follower_effects(L) of
                            [] -> Acc;
                            Filtered ->
                                [Filtered | Acc]
                        end;
                    (_, Acc) ->
                        Acc
                end, [], Effects).

make_pipelined_rpc_effects(State, Effects) ->
    make_pipelined_rpc_effects(?AER_CHUNK_SIZE, State, Effects).

make_pipelined_rpc_effects(MaxBatchSize,
                           #{cfg := #cfg{id = Id,
                                         max_pipeline_count = MaxPipelineCount},
                             commit_index := CommitIndex,
                             log := Log,
                             cluster := Cluster} = State,
                           Effects) ->
    NextLogIdx = ra_log:next_index(Log),
    maps:fold(
      fun (I, _, Acc) when I =:= Id ->
              %% oneself
              Acc;
          (_, #{status := suspended}, Acc) ->
              Acc;
          (_, #{status := {sending_snapshot, _}}, Acc) ->
              %% if a peers is currently receiving a snapshot
              %% we should not pipeline
              Acc;
          (PeerId, #{next_index := NI,
                     commit_index_sent := CI,
                     match_index := MI} = Peer0,
           {S0, More0, Effs} = Acc)
            when NI < NextLogIdx orelse CI < CommitIndex ->
              % there are unsent items or a new commit index
              % check if the match index isn't too far behind the
              % next index
              case NI - MI < MaxPipelineCount of
                  true ->
                      {NextIdx, Eff, S} =
                          make_rpc_effect(PeerId, Peer0, MaxBatchSize, S0),
                      Peer = Peer0#{next_index => NextIdx,
                                    commit_index_sent => CommitIndex},
                      %% is there more potentially pipelining
                      More = More0 orelse (NextIdx < NextLogIdx andalso
                                           NextIdx - MI < MaxPipelineCount),
                      {put_peer(PeerId, Peer, S), More, [Eff | Effs]};
                  false ->
                      Acc
              end;
          (_, _, Acc) ->
              Acc
      end, {State, false, Effects}, Cluster).

make_rpcs(State) ->
    {State1, EffectsHR} = update_heartbeat_rpc_effects(State),
    {State2, EffectsAER} = make_rpcs_for(stale_peers(State1), State1),
    {State2, EffectsAER ++ EffectsHR}.

% makes empty append entries for peers that aren't pipelineable
make_all_rpcs(State0) ->
    {State1, EffectsHR} = update_heartbeat_rpc_effects(State0),
    {State2, EffectsAER} = make_rpcs_for(peers_with_normal_status(State1), State1),
    {State2, EffectsAER ++ EffectsHR}.

make_rpcs_for(Peers, State) ->
    maps:fold(fun(PeerId, Peer, {S0, Effs}) ->
                      {_, Eff, S} =
                          make_rpc_effect(PeerId, Peer, ?AER_CHUNK_SIZE, S0),
                      {S, [Eff | Effs]}
              end, {State, []}, Peers).

make_rpc_effect(PeerId, #{next_index := Next}, MaxBatchSize,
                #{cfg := #cfg{id = Id}, log := Log0,
                  current_term := Term} = State) ->
    PrevIdx = Next - 1,
    case ra_log:fetch_term(PrevIdx, Log0) of
        {PrevTerm, Log} when is_integer(PrevTerm) ->
            make_append_entries_rpc(PeerId, PrevIdx,
                                    PrevTerm, MaxBatchSize,
                                    State#{log => Log});
        {undefined, Log} ->
            % The assumption here is that a missing entry means we need
            % to send a snapshot.
            case ra_log:snapshot_index_term(Log) of
                {PrevIdx, PrevTerm} ->
                    % Previous index is the same as snapshot index
                    make_append_entries_rpc(PeerId, PrevIdx,
                                            PrevTerm, MaxBatchSize,
                                            State#{log => Log});
                {LastIdx, _} ->
                    SnapState = ra_log:snapshot_state(Log),
                    %% don't increment the next index here as we will do
                    %% that once the snapshot is fully replicated
                    %% and we don't pipeline entries until after snapshot
                    {LastIdx,
                     {send_snapshot, PeerId, {SnapState, Id, Term}},
                     State#{log => Log}}
            end
    end.

make_append_entries_rpc(PeerId, PrevIdx, PrevTerm, Num,
                        #{log := Log0, current_term := Term,
                          cfg := #cfg{id = Id},
                          commit_index := CommitIndex} = State) ->
    Next = PrevIdx + 1,
    {Entries, NumRead, Log} = ra_log:take(Next, Num, Log0),
    NextIndex = Next + NumRead,
    {NextIndex,
     {send_rpc, PeerId,
      #append_entries_rpc{entries = Entries,
                          term = Term,
                          leader_id = Id,
                          prev_log_index = PrevIdx,
                          prev_log_term = PrevTerm,
                          leader_commit = CommitIndex}},
     State#{log => Log}}.

% stores the cluster config at an index such that we can later snapshot
% at this index.
-spec update_release_cursor(ra_index(),
                            term(), ra_server_state()) ->
    {ra_server_state(), effects()}.
update_release_cursor(Index, MacState,
                      State = #{log := Log0, cluster := Cluster}) ->
    MacVersion = index_machine_version(Index, State),
    % simply pass on release cursor index to log
    {Log, Effects} = ra_log:update_release_cursor(Index, Cluster,
                                                  MacVersion,
                                                  MacState, Log0),
    {State#{log => Log}, Effects}.

% Persist last_applied - as there is an inherent race we cannot
% always guarantee that side effects won't be re-issued when a
% follower that has seen an entry but not the commit_index
% takes over and this
% This is done on a schedule
-spec persist_last_applied(ra_server_state()) -> ra_server_state().
persist_last_applied(#{persisted_last_applied := PLA,
                       last_applied := LA} = State) when LA =< PLA ->
    % if last applied is less than PL for some reason do nothing
    State;
persist_last_applied(#{last_applied := LastApplied,
                       cfg := #cfg{uid = UId} = Cfg} = State) ->
    ok = ra_log_meta:store(meta_name(Cfg), UId, last_applied, LastApplied),
    State#{persisted_last_applied => LastApplied}.


-spec update_peer(ra_server_id(),
                  #{next_index => non_neg_integer(),
                    query_index => non_neg_integer(),
                    commit_index_sent => non_neg_integer(),
                    status => ra_peer_status()},
                  ra_server_state()) -> ra_server_state().
update_peer(PeerId, Update, #{cluster := Peers} = State)
  when is_map(Update) ->
    Peer = maps:merge(maps:get(PeerId, Peers), Update),
    put_peer(PeerId, Peer, State).

-spec register_external_log_reader(pid(), ra_server_state()) ->
    {ra_server_state(), effects()}.
register_external_log_reader(Pid, #{log := Log0} = State) ->
    {Log, Effs} = ra_log:register_reader(Pid, Log0),
    {State#{log => Log}, Effs}.

peer_snapshot_process_exited(SnapshotPid, #{cluster := Peers} = State) ->
     PeerKv =
         maps:to_list(
           maps:filter(fun(_, #{status := {sending_snapshot, Pid}})
                             when Pid =:= SnapshotPid ->
                               true;
                          (_, _) -> false
                       end, Peers)),
     case PeerKv of
         [{PeerId, Peer}] ->
             put_peer(PeerId, Peer#{status => normal}, State);
         _ ->
             State
     end.

-spec handle_down(ra_state(),
                  machine | snapshot_sender | snapshot_writer | aux,
                  pid(), term(), ra_server_state()) ->
    {ra_state(), ra_server_state(), effects()}.
handle_down(leader, machine, Pid, Info, State)
  when is_pid(Pid) ->
    %% commit command to be processed by state machine
    handle_leader({command, {'$usr', #{ts => erlang:system_time(millisecond)},
                            {down, Pid, Info}, noreply}},
                  State);
handle_down(leader, snapshot_sender, Pid, Info,
            #{cfg := #cfg{log_id = LogId}} = State)
  when is_pid(Pid) ->
    ?DEBUG("~s: Snapshot sender process ~w exited with ~W",
          [LogId, Pid, Info, 10]),
    {leader, peer_snapshot_process_exited(Pid, State), []};
handle_down(RaftState, snapshot_writer, Pid, Info,
            #{cfg := #cfg{log_id = LogId}, log := Log0} = State)
  when is_pid(Pid) ->
    case Info of
        noproc -> ok;
        normal -> ok;
        _ ->
            ?WARN("~s: Snapshot write process ~w exited with ~w",
                  [LogId, Pid, Info])
    end,
    SnapState0 = ra_log:snapshot_state(Log0),
    SnapState = ra_snapshot:handle_down(Pid, Info, SnapState0),
    Log = ra_log:set_snapshot_state(SnapState, Log0),
    {RaftState, State#{log => Log}, []};
handle_down(RaftState, log, Pid, Info, #{log := Log0} = State) ->
    {Log, Effects} = ra_log:handle_event({down, Pid, Info}, Log0),
    {RaftState, State#{log => Log}, Effects};
handle_down(RaftState, aux, Pid, Info, State)
  when is_pid(Pid) ->
    handle_aux(RaftState, cast, {down, Pid, Info}, State);
handle_down(RaftState, Type, Pid, Info, #{cfg := #cfg{log_id = LogId}} = State) ->
    ?INFO("~s: handle_down: unexpected ~w ~w exited with ~W",
          [LogId, Type, Pid, Info, 10]),
    {RaftState, State, []}.

-spec handle_node_status(ra_state(), machine | aux,
                         node(), nodeup | nodedown,
                         term(), ra_server_state()) ->
    {ra_state(), ra_server_state(), effects()}.
handle_node_status(leader, machine, Node, Status, _Infos, State)
  when is_atom(Node) ->
    %% commit command to be processed by state machine
    %% TODO: provide an option where the machine or aux can be provided with
    %% the node down reason
    Meta = #{ts => erlang:system_time(millisecond)},
    handle_leader({command, {'$usr', Meta, {Status, Node}, noreply}}, State);
handle_node_status(RaftState, aux, Node, Status, _Infos, State)
  when is_atom(Node) ->
    handle_aux(RaftState, cast, {Status, Node}, State);
handle_node_status(RaftState, Type, Node, Status, _Info,
                   #{cfg := #cfg{log_id = LogId}} = State) ->
    ?DEBUG("~s: handle_node_status: unexpected ~w ~w status change ~w",
          [LogId, Type, Node, Status]),
    {RaftState, State, []}.

-spec terminate(ra_server_state(), Reason :: {shutdown, delete} | term()) -> ok.
terminate(#{log := Log,
            cfg := #cfg{log_id = LogId}} = _State, {shutdown, delete}) ->
    ?NOTICE("~s: terminating with reason 'delete'", [LogId]),
    catch ra_log:delete_everything(Log),
    ok;
terminate(#{cfg := #cfg{log_id = LogId}} = State, Reason) ->
    ?DEBUG("~s: terminating with reason '~w'", [LogId, Reason]),
    #{log := Log} = persist_last_applied(State),
    catch ra_log:close(Log),
    ok.

-spec log_fold(ra_server_state(), fun((term(), State) -> State), State) ->
    {ok, State, ra_server_state()} |
    {error, term(), ra_server_state()}.
log_fold(#{log := Log} = RaState, Fun, State) ->
    Idx = case ra_log:snapshot_index_term(Log) of
              {PrevIdx, _PrevTerm} ->
                  PrevIdx;
              undefined ->
                  1
          end,
    case fold_log_from(Idx, Fun, {State, Log}) of
        {ok, {State1, Log1}} ->
            {ok, State1, RaState#{log => Log1}};
        {error, Reason, Log1} ->
            {error, Reason, RaState#{log => Log1}}
    end.

%% reads user commands at the specified index
-spec read_at(ra_index(), ra_server_state()) ->
    {ok, term(), ra_server_state()} |
    {error, ra_server_state()}.
read_at(Idx, #{log := Log0,
               cfg := #cfg{log_id = LogId}} = RaState) ->
    case ra_log:fetch(Idx, Log0) of
        {{Idx, _, {'$usr', _, Data, _}}, Log} ->
            {ok, Data, RaState#{log => Log}};
        {Cmd, Log} ->
            ?ERROR("~s: failed to read user command at ~b. Got ~w",
                   [LogId, Idx, Cmd]),
            {error, RaState#{log => Log}}
    end.
%%%===================================================================
%%% Internal functions
%%%===================================================================

call_for_election(candidate, #{cfg := #cfg{id = Id, log_id = LogId} = Cfg,
                               current_term := CurrentTerm} = State0) ->
    ok = incr_counter(Cfg, ?C_RA_SRV_ELECTIONS, 1),
    NewTerm = CurrentTerm + 1,
    ?DEBUG("~s: election called for in term ~b", [LogId, NewTerm]),
    PeerIds = peer_ids(State0),
    % increment current term
    {LastIdx, LastTerm} = last_idx_term(State0),
    Reqs = [{PeerId, #request_vote_rpc{term = NewTerm,
                                       candidate_id = Id,
                                       last_log_index = LastIdx,
                                       last_log_term = LastTerm}}
            || PeerId <- PeerIds],
    % vote for self
    VoteForSelf = #request_vote_result{term = NewTerm, vote_granted = true},
    State = update_term_and_voted_for(NewTerm, Id, State0),
    {candidate, State#{leader_id => undefined, votes => 0},
     [{next_event, cast, VoteForSelf}, {send_vote_requests, Reqs}]};
call_for_election(pre_vote, #{cfg := #cfg{id = Id,
                                          log_id = LogId,
                                          machine_version = MacVer} = Cfg,
                              current_term := Term} = State0) ->
    ok = incr_counter(Cfg, ?C_RA_SRV_PRE_VOTE_ELECTIONS, 1),
    ?DEBUG("~s: pre_vote election called for in term ~b", [LogId, Term]),
    Token = make_ref(),
    PeerIds = peer_ids(State0),
    {LastIdx, LastTerm} = last_idx_term(State0),
    Reqs = [{PeerId, #pre_vote_rpc{term = Term,
                                   token = Token,
                                   machine_version = MacVer,
                                   candidate_id = Id,
                                   last_log_index = LastIdx,
                                   last_log_term = LastTerm}}
            || PeerId <- PeerIds],
    % vote for self
    VoteForSelf = #pre_vote_result{term = Term, token = Token,
                                   vote_granted = true},
    State = update_term_and_voted_for(Term, Id, State0),
    {pre_vote, State#{leader_id => undefined, votes => 0,
                      pre_vote_token => Token},
     [{next_event, cast, VoteForSelf}, {send_vote_requests, Reqs}]}.

process_pre_vote(FsmState, #pre_vote_rpc{term = Term, candidate_id = Cand,
                                         version = Version,
                                         machine_version = TheirMacVer,
                                         token = Token,
                                         last_log_index = LLIdx,
                                         last_log_term = LLTerm},
                 #{cfg := #cfg{machine_version = OurMacVer,
                               effective_machine_version = EffMacVer},
                   current_term := CurTerm} = State0)
  when Term >= CurTerm  ->
    State = update_term(Term, State0),
    LastIdxTerm = last_idx_term(State),
    case is_candidate_log_up_to_date(LLIdx, LLTerm, LastIdxTerm) of
        true when Version > ?RA_PROTO_VERSION->
            ?DEBUG("~s: declining pre-vote for ~w for protocol version ~b",
                   [log_id(State0), Cand, Version]),
            {FsmState, State, [{reply, pre_vote_result(Term, Token, false)}]};
        true when TheirMacVer >= EffMacVer andalso
                  TheirMacVer =< OurMacVer ->
            ?DEBUG("~s: granting pre-vote for ~w"
                   " machine version (their:ours) ~b:~b"
                   " with last indexterm ~w"
                   " for term ~b previous term ~b",
                   [log_id(State0), Cand, TheirMacVer, OurMacVer,
                    {LLIdx, LLTerm}, Term, CurTerm]),
            {FsmState, State#{voted_for => Cand},
             [{reply, pre_vote_result(Term, Token, true)}]};
        true ->
            ?DEBUG("~s: declining pre-vote for ~w their machine version ~b"
                   " ours is ~b",
                   [log_id(State0), Cand, TheirMacVer, OurMacVer]),
            {FsmState, State, [{reply, pre_vote_result(Term, Token, false)},
                               start_election_timeout]};
        false ->
            ?DEBUG("~s: declining pre-vote for ~w for term ~b,"
                   " candidate last log index term was: ~w~n"
                   "Last log entry idxterm seen was: ~w",
                   [log_id(State0), Cand, Term, {LLIdx, LLTerm}, LastIdxTerm]),
            case FsmState of
                follower ->
                    {FsmState, State, [start_election_timeout]};
                pre_vote ->
                    {FsmState, State,
                     [{reply, pre_vote_result(Term, Token, false)}]}
            end
    end;
process_pre_vote(FsmState, #pre_vote_rpc{term = Term,
                                         token = Token,
                                         candidate_id = _Cand},
                #{current_term := CurTerm} = State)
  when Term < CurTerm ->
    ?DEBUG("~s declining pre-vote to ~w for term ~b, current term ~b",
           [log_id(State), _Cand, Term, CurTerm]),
    {FsmState, State,
     [{reply, pre_vote_result(CurTerm, Token, false)}]}.

pre_vote_result(Term, Token, Success) ->
    #pre_vote_result{term = Term,
                     token = Token,
                     vote_granted = Success}.

new_peer() ->
    #{next_index => 1,
      match_index => 0,
      commit_index_sent => 0,
      query_index => 0,
      status => normal}.

new_peer_with(Map) ->
    maps:merge(new_peer(), Map).

peers(#{cfg := #cfg{id = Id}, cluster := Peers}) ->
    maps:remove(Id, Peers).

%% remove any peers that are currently receiving a snapshot
peers_with_normal_status(State) ->
    maps:filter(fun (_, #{status := normal}) -> true;
                    (_, _) -> false
                end, peers(State)).

% peers that could need an update
stale_peers(#{commit_index := CommitIndex,
              cfg := #cfg{id = ThisId},
              cluster := Cluster}) ->
    maps:filter(fun (Id , _) when Id == ThisId ->
                        false;
                    (_, #{status := normal,
                          next_index := NI,
                          match_index := MI})
                      when MI < NI - 1 ->
                        % there are unconfirmed items
                        true;
                    (_, #{status := normal,
                          commit_index_sent := CI})
                      when CI < CommitIndex ->
                        % the commit index has been updated
                        true;
                    (_, _Peer) ->
                        false
                end, Cluster).

peer_ids(State) ->
    maps:keys(peers(State)).

peer(PeerId, #{cluster := Nodes}) ->
    maps:get(PeerId, Nodes, undefined).

put_peer(PeerId, Peer, #{cluster := Peers} = State) ->
    State#{cluster => Peers#{PeerId => Peer}}.

update_term_and_voted_for(Term, VotedFor, #{cfg := #cfg{uid = UId} = Cfg,
                                            current_term := CurTerm} = State) ->
    CurVotedFor = maps:get(voted_for, State, undefined),
    case Term =:= CurTerm andalso VotedFor =:= CurVotedFor of
        true ->
            %% no update needed
            State;
        false ->
            MetaName = meta_name(Cfg),
            %% as this is a rare event it is ok to go sync here
            ok = ra_log_meta:store(MetaName, UId, current_term, Term),
            ok = ra_log_meta:store_sync(MetaName, UId, voted_for, VotedFor),
            incr_counter(Cfg, ?C_RA_SRV_TERM_AND_VOTED_FOR_UPDATES, 1),
            reset_query_index(State#{current_term => Term,
                                     voted_for => VotedFor})
    end.

update_term(Term, State = #{current_term := CurTerm})
  when Term =/= undefined andalso Term > CurTerm ->
        update_term_and_voted_for(Term, undefined, State);
update_term(_, State) ->
    State.

last_idx_term(#{log := Log}) ->
    ra_log:last_index_term(Log).

%% § 5.4.1 Raft determines which of two logs is more up-to-date by comparing
%% the index and term of the last entries in the logs. If the logs have last
%% entries with different terms, then the log with the later term is more
%% up-to-date. If the logs end with the same term, then whichever log is
%% longer is more up-to-dat
-spec is_candidate_log_up_to_date(ra_index(), ra_term(), ra_idxterm()) ->
    boolean().
is_candidate_log_up_to_date(_, Term, {_, LastTerm})
  when Term > LastTerm ->
    true;
is_candidate_log_up_to_date(Idx, Term, {LastIdx, Term})
  when Idx >= LastIdx ->
    true;
is_candidate_log_up_to_date(_, _, {_, _}) ->
    false.

has_log_entry_or_snapshot(Idx, Term, Log0) ->
    case ra_log:fetch_term(Idx, Log0) of
        {undefined, Log} ->
            case ra_log:snapshot_index_term(Log) of
                {Idx, Term} ->
                    {entry_ok, Log};
                {Idx, OtherTerm} ->
                    {term_mismatch, OtherTerm, Log};
                _ ->
                    {missing, Log}
            end;
        {Term, Log} ->
            {entry_ok, Log};
        {OtherTerm, Log} ->
            {term_mismatch, OtherTerm, Log}
    end.

fetch_term(Idx, #{log := Log0} = State) ->
    case ra_log:fetch_term(Idx, Log0) of
        {undefined, Log} ->
            case ra_log:snapshot_index_term(Log) of
                {Idx, Term} ->
                    {Term, State#{log => Log}};
                _ ->
                    {undefined, State#{log => Log}}
            end;
        {Term, Log} ->
            {Term, State#{log => Log}}
    end.

fetch_entries(From, To, #{log := Log0} = State) ->
    {Entries, _, Log} = ra_log:take(From, To - From + 1, Log0),
    {Entries, State#{log => Log}}.

make_cluster(Self, Nodes) ->
    case lists:foldl(fun(N, Acc) ->
                             Acc#{N => new_peer()}
                     end, #{}, Nodes) of
        #{Self := _} = Cluster ->
            % current server is already in cluster - do nothing
            Cluster;
        Cluster ->
            % add current server to cluster
            Cluster#{Self => new_peer()}
    end.

initialise_peers(State = #{log := Log, cluster := Cluster0}) ->
    PeerIds = peer_ids(State),
    NextIdx = ra_log:next_index(Log),
    Cluster = lists:foldl(fun(PeerId, Acc) ->
                                  Acc#{PeerId =>
                                       new_peer_with(#{next_index => NextIdx})}
                          end, Cluster0, PeerIds),
    State#{cluster => Cluster}.

apply_to(ApplyTo, State, Effs) ->
    apply_to(ApplyTo, fun apply_with/2, #{}, Effs, State).

apply_to(ApplyTo, ApplyFun, State, Effs) ->
    apply_to(ApplyTo, ApplyFun, #{}, Effs, State).

apply_to(ApplyTo, ApplyFun, Notifys0, Effects0,
         #{last_applied := LastApplied,
           cfg := #cfg{machine_version = MacVer,
                       effective_machine_module = MacMod,
                       effective_machine_version = EffMacVer},
           machine_state := MacState0} = State0)
  when ApplyTo > LastApplied andalso MacVer >= EffMacVer ->
    From = LastApplied + 1,
    To = min(From + ?MAX_FETCH_ENTRIES, ApplyTo),
    case fetch_entries(From, To, State0) of
        {[], State} ->
            %% reverse list before consing the notifications to ensure
            %% notifications are processed first
            FinalEffs = make_notify_effects(Notifys0, lists:reverse(Effects0)),
            {State, FinalEffs};
        %% assert first item read is from
        {[{From, _, _} | _] = Entries, State1} ->
            {_, AppliedTo, State, MacState, Effects, Notifys, LastTs} =
                lists:foldl(ApplyFun, {MacMod, LastApplied, State1, MacState0,
                                       Effects0, Notifys0, undefined},
                            Entries),
            CommitLatency = case LastTs of
                                undefined ->
                                    0;
                                _ when is_integer(LastTs) ->
                                    erlang:system_time(millisecond) - LastTs
                            end,
            %% due to machine versioning all entries may not have been applied
            apply_to(ApplyTo, ApplyFun, Notifys, Effects,
                     State#{last_applied => AppliedTo,
                            commit_latency => CommitLatency,
                            machine_state => MacState})
    end;
apply_to(_ApplyTo, _, Notifys, Effects, State)
  when is_list(Effects) ->
    %% reverse list before consing the notifications to ensure
    %% notifications are processed first
    FinalEffs = make_notify_effects(Notifys, lists:reverse(Effects)),
    {State, FinalEffs}.

make_notify_effects(Nots, Prior) ->
    maps:fold(fun (Pid, Corrs, Acc) ->
                      [{notify, Pid, lists:reverse(Corrs)} | Acc]
              end, Prior, Nots).

apply_with(_Cmd,
           {Mod, LastAppliedIdx,
            #{cfg := #cfg{machine_version = MacVer,
                          effective_machine_version = Effective}} = State,
            MacSt, Effects, Notifys, LastTs})
      when MacVer < Effective ->
    %% we cannot apply any further entries
    {Mod, LastAppliedIdx, State, MacSt, Effects, Notifys, LastTs};
apply_with({Idx, Term, {'$usr', CmdMeta, Cmd, ReplyType}},
           {Module, _LastAppliedIdx,
            State = #{cfg := #cfg{effective_machine_version = MacVer}},
            MacSt, Effects, Notifys0, LastTs}) ->
    %% augment the meta data structure
    Meta = augment_command_meta(Idx, Term, MacVer, CmdMeta),
    Ts = maps:get(ts, CmdMeta, LastTs),
    case ra_machine:apply(Module, Meta, Cmd, MacSt) of
        {NextMacSt, Reply, AppEffs} ->
            {ReplyEffs, Notifys} = add_reply(CmdMeta, Reply, ReplyType,
                                             Effects, Notifys0),
            {Module, Idx, State, NextMacSt,
             [AppEffs | ReplyEffs], Notifys, Ts};
        {NextMacSt, Reply} ->
            {ReplyEffs, Notifys} = add_reply(CmdMeta, Reply, ReplyType,
                                             Effects, Notifys0),
            {Module, Idx, State, NextMacSt,
             ReplyEffs, Notifys, Ts}
    end;
apply_with({Idx, Term, {'$ra_cluster_change', CmdMeta, NewCluster, ReplyType}},
           {Mod, _, State0, MacSt, Effects0, Notifys0, LastTs}) ->
    {Effects, Notifys} = add_reply(CmdMeta, ok, ReplyType,
                                   Effects0, Notifys0),
    State = case State0 of
                #{cluster_index_term := {CI, CT}}
                  when Idx > CI andalso Term >= CT ->
                    ?DEBUG("~s: applying ra cluster change to ~w",
                           [log_id(State0), maps:keys(NewCluster)]),
                    %% we are recovering and should apply the cluster change
                    State0#{cluster => NewCluster,
                            cluster_change_permitted => true,
                            cluster_index_term => {Idx, Term}};
                _  ->
                    ?DEBUG("~s: committing ra cluster change to ~w",
                           [log_id(State0), maps:keys(NewCluster)]),
                    %% else just enable further cluster changes again
                    State0#{cluster_change_permitted => true}
            end,
    {Mod, Idx, State, MacSt, Effects, Notifys, LastTs};
apply_with({Idx, Term, {noop, CmdMeta, NextMacVer}},
           {CurModule, LastAppliedIdx,
            #{cfg := #cfg{log_id = LogId,
                          machine_version = MacVer,
                          %% active machine versions and their index
                          %% (from last snapshot)
                          machine = Machine,
                          machine_versions = MacVersions,
                          effective_machine_version = OldMacVer
                         } = Cfg0,
              current_term := CurrentTerm,
              cluster_change_permitted := ClusterChangePerm0} = State0,
            MacSt, Effects, Notifys, LastTs}) ->
    ClusterChangePerm = case CurrentTerm of
                            Term ->
                                ?DEBUG("~s: enabling ra cluster changes in"
                                       " ~b", [LogId, Term]),
                                true;
                            _ -> ClusterChangePerm0
                        end,
    %% can we understand the next machine version
    IsOk = MacVer >= NextMacVer,
    case NextMacVer > OldMacVer of
        true when IsOk ->
            %% discover the next module to use
            Module = ra_machine:which_module(Machine, NextMacVer),
            %% enable cluster change if the noop command is for the current term
            Cfg = Cfg0#cfg{effective_machine_version = NextMacVer,
                           %% record this machine version "term"
                           machine_versions = [{Idx, MacVer} | MacVersions],
                           effective_machine_module = Module},
            State = State0#{cfg => Cfg,
                            cluster_change_permitted => ClusterChangePerm},
            Meta = augment_command_meta(Idx, Term, MacVer, CmdMeta),
            ?DEBUG("~s: applying new machine version ~b current ~b",
                   [LogId, NextMacVer, OldMacVer]),
            apply_with({Idx, Term,
                        {'$usr', Meta,
                         {machine_version, OldMacVer, NextMacVer}, none}},
                       {Module, LastAppliedIdx, State, MacSt,
                        Effects, Notifys, LastTs});
        true ->
            %% we cannot make progress as we don't understand the new
            %% machine version so we
            %% update the effective machine version to stop any further entries
            %% being applied. This is ok as a restart will be needed to
            %% learn the new machine version which will reset it
            ?DEBUG("~s: unknown machine version ~b current ~b"
                   " cannot apply any further entries",
                   [LogId, NextMacVer, MacVer]),
            Cfg = Cfg0#cfg{effective_machine_version = NextMacVer},
            State = State0#{cfg => Cfg},
            {CurModule, LastAppliedIdx, State,
             MacSt, Effects, Notifys, LastTs};
        false ->
            State = State0#{cluster_change_permitted => ClusterChangePerm},
            {CurModule, Idx, State, MacSt, Effects, Notifys, LastTs}
    end;
apply_with({Idx, _, {'$ra_cluster', CmdMeta, delete, ReplyType}},
           {Module, _, State0, MacSt, Effects0, Notifys0, _LastTs}) ->
    % cluster deletion
    {Effects1, Notifys} = add_reply(CmdMeta, ok, ReplyType, Effects0, Notifys0),
    NotEffs = make_notify_effects(Notifys, []),
    %% virtual "eol" state
    EOLEffects = ra_machine:state_enter(Module, eol, MacSt),
    % non-local return to be caught by ra_server_proc
    % need to update the state before throw
    State = State0#{last_applied => Idx, machine_state => MacSt},
    throw({delete_and_terminate, State, EOLEffects ++ NotEffs ++ Effects1});
apply_with({Idx, _, _} = Cmd, Acc) ->
    % TODO: remove to make more strict, ideally we should not need a catch all
    ?WARN("~s: apply_with: unhandled command: ~W",
          [log_id(element(2, Acc)), Cmd, 10]),
    setelement(2, Acc, Idx).

augment_command_meta(Idx, Term, MacVer, CmdMeta) ->
    maps:fold(fun (ts, V, Acc) ->
                      %% rename from compact key name
                      Acc#{system_time => V};
                  (K, V, Acc) ->
                      Acc#{K => V}
              end, #{index => Idx,
                     machine_version => MacVer,
                     term => Term},
              CmdMeta).

add_reply(_, '$ra_no_reply', _, Effects, Notifys) ->
    {Effects, Notifys};
add_reply(#{from := From}, Reply, await_consensus, Effects, Notifys) ->
    {[{reply, From, {wrap_reply, Reply}} | Effects], Notifys};
add_reply(_, Reply, {notify, Corr, Pid},
          Effects, Notifys) ->
    % notify are casts and thus have to include their own pid()
    % reply with the supplied correlation so that the sending can do their
    % own bookkeeping
    CorrData = {Corr, Reply},
    case Notifys of
        #{Pid := T} ->
            {Effects, Notifys#{Pid => [CorrData | T]}};
        _ ->
            {Effects, Notifys#{Pid => [CorrData]}}
    end;
add_reply(_, _, _, % From, Reply, Mode
          Effects, Notifys) ->
    {Effects, Notifys}.

append_log_leader({CmdTag, _, _, _},
                  State = #{cluster_change_permitted := false})
  when CmdTag == '$ra_join' orelse
       CmdTag == '$ra_leave' ->
    {not_appended, cluster_change_not_permitted, State};
append_log_leader({'$ra_join', From, JoiningNode, ReplyMode},
                  State = #{cluster := OldCluster}) ->
    case OldCluster of
        #{JoiningNode := _} ->
            % already a member do nothing
            % TODO: reply? If we don't reply the caller may block until timeout
            {not_appended, already_member, State};
        _ ->
            Cluster = OldCluster#{JoiningNode => new_peer()},
            append_cluster_change(Cluster, From, ReplyMode, State)
    end;
append_log_leader({'$ra_leave', From, LeavingServer, ReplyMode},
                  State = #{cfg := #cfg{log_id = LogId},
                            cluster := OldCluster}) ->
    case OldCluster of
        #{LeavingServer := _} ->
            Cluster = maps:remove(LeavingServer, OldCluster),
            append_cluster_change(Cluster, From, ReplyMode, State);
        _ ->
            ?DEBUG("~s: member ~w requested to leave but was not a member. "
                   "Members: ~w",
                   [LogId, LeavingServer, maps:keys(OldCluster)]),
            % not a member - do nothing
            {not_appended, not_member, State}
    end;
append_log_leader(Cmd, State = #{log := Log0, current_term := Term}) ->
    NextIdx = ra_log:next_index(Log0),
    Log = ra_log:append({NextIdx, Term, Cmd}, Log0),
    {ok, NextIdx, Term, State#{log => Log}}.

pre_append_log_follower({Idx, Term, Cmd} = Entry,
                        State = #{cluster_index_term := {Idx, CITTerm}})
  when Term /= CITTerm ->
    % the index for the cluster config entry has a different term, i.e.
    % it has been overwritten by a new leader. Unless it is another cluster
    % change (can this even happen?) we should revert back to the last known
    % cluster
    case Cmd of
        {'$ra_cluster_change', _, Cluster, _} ->
            State#{cluster => Cluster,
                   cluster_index_term => {Idx, Term}};
        _ ->
            % revert back to previous cluster
            {PrevIdx, PrevTerm, PrevCluster} = maps:get(previous_cluster, State),
            State1 = State#{cluster => PrevCluster,
                            cluster_index_term => {PrevIdx, PrevTerm}},
            pre_append_log_follower(Entry, State1)
    end;
pre_append_log_follower({Idx, Term, {'$ra_cluster_change', _, Cluster, _}},
                        State) ->
    State#{cluster => Cluster,
           cluster_index_term => {Idx, Term}};
pre_append_log_follower(_, State) ->
    State.

append_cluster_change(Cluster, From, ReplyMode,
                      State = #{log := Log0,
                                cluster := PrevCluster,
                                cluster_index_term := {PrevCITIdx, PrevCITTerm},
                                current_term := Term}) ->
    % turn join command into a generic cluster change command
    % that include the new cluster configuration
    Command = {'$ra_cluster_change', From, Cluster, ReplyMode},
    NextIdx = ra_log:next_index(Log0),
    IdxTerm = {NextIdx, Term},
    % TODO: is it safe to do change the cluster config with an async write?
    % what happens if the write fails?
    Log = ra_log:append({NextIdx, Term, Command}, Log0),
    {ok, NextIdx, Term,
     State#{log => Log,
            cluster => Cluster,
            cluster_change_permitted => false,
            cluster_index_term => IdxTerm,
            previous_cluster => {PrevCITIdx, PrevCITTerm, PrevCluster}}}.

mismatch_append_entries_reply(Term, CommitIndex, State0) ->
    {CITerm, State} = fetch_term(CommitIndex, State0),
    % assert CITerm is found
    false = CITerm =:= undefined,
    {#append_entries_reply{term = Term, success = false,
                           next_index = CommitIndex + 1,
                           last_index = CommitIndex,
                           last_term = CITerm},
     State}.

append_entries_reply(Term, Success, State = #{log := Log}) ->
    % we can't use the the last received idx
    % as it may not have been persisted yet
    % also we can't use the last writted Idx as then
    % the follower may resent items that are currently waiting to
    % be written.
    {LWIdx, LWTerm} = ra_log:last_written(Log),
    {LastIdx, _} = last_idx_term(State),
    #append_entries_reply{term = Term,
                          success = Success,
                          next_index = LastIdx + 1,
                          last_index = LWIdx,
                          last_term = LWTerm}.

evaluate_quorum(#{commit_index := CI0} = State0, Effects0) ->
    % TODO: shortcut function if commit index was not incremented
    State = #{commit_index := CI} = increment_commit_index(State0),

    Effects = case CI > CI0 of
                  true ->
                      [{aux, eval} | Effects0];
                  false ->
                      Effects0
              end,
    apply_to(CI, State, Effects).

increment_commit_index(State0 = #{current_term := CurrentTerm}) ->
    PotentialNewCommitIndex = agreed_commit(match_indexes(State0)),
    % leaders can only increment their commit index if the corresponding
    % log entry term matches the current term. See (§5.4.2)
    case fetch_term(PotentialNewCommitIndex, State0) of
        {CurrentTerm, State} ->
            State#{commit_index => PotentialNewCommitIndex};
        {_, State} ->
            State
    end.

query_indexes(#{cfg := #cfg{id = Id},
                cluster := Cluster,
                query_index := QueryIndex}) ->
    maps:fold(fun (PeerId, _, Acc) when PeerId == Id ->
                      Acc;
                  (_K, #{query_index := Idx}, Acc) ->
                      [Idx | Acc]
              end, [QueryIndex], Cluster).

match_indexes(#{cfg := #cfg{id = Id},
                cluster := Cluster,
                log := Log}) ->
    {LWIdx, _} = ra_log:last_written(Log),
    maps:fold(fun (PeerId, _, Acc) when PeerId == Id ->
                      Acc;
                  (_K, #{match_index := Idx}, Acc) ->
                      [Idx | Acc]
              end, [LWIdx], Cluster).

-spec agreed_commit(list()) -> ra_index().
agreed_commit(Indexes) ->
    SortedIdxs = lists:sort(fun erlang:'>'/2, Indexes),
    Nth = trunc(length(SortedIdxs) / 2) + 1,
    lists:nth(Nth, SortedIdxs).

log_unhandled_msg(RaState, Msg, #{cfg := #cfg{log_id = LogId}}) ->
    ?DEBUG("~s: ~w received unhandled msg: ~W", [LogId, RaState, Msg, 6]).

fold_log_from(From, Folder, {St, Log0}) ->
    case ra_log:take(From, ?FOLD_LOG_BATCH_SIZE, Log0) of
        {[], _, Log} ->
            {ok, {St, Log}};
        {Entries, _, Log}  ->
            try
                St1 = lists:foldl(Folder, St, Entries),
                fold_log_from(From + ?FOLD_LOG_BATCH_SIZE, Folder, {St1, Log})
            catch
                _:Reason ->
                    {error, Reason, Log}
            end
    end.

drop_existing({Log0, []}) ->
    {Log0, []};
drop_existing({Log0, [{Idx, Trm, _} | Tail] = Entries}) ->
    case ra_log:exists({Idx, Trm}, Log0) of
        {true, Log} ->
            drop_existing({Log, Tail});
        {false, Log} ->
            {Log, Entries}
    end.

cast_reply(From, To, Msg) ->
    {cast, To, {From, Msg}}.


index_machine_version(Idx, #{cfg := #cfg{machine_versions = Versions}}) ->
    %% scan for versions
    index_machine_version0(Idx, Versions).

index_machine_version0(Idx, []) ->
    %% this _should_ never happen as you should never get a release cursor
    %% for an index that is lower than the last snapshot index
    exit({machine_version_for_index_not_known, {index, Idx}});
index_machine_version0(Idx, [{MIdx, V} | _])
  when Idx >= MIdx -> V;
index_machine_version0(Idx, [_ | Rem]) ->
    index_machine_version0(Idx, Rem).

heartbeat_reply(#{current_term := CurTerm, query_index := QueryIndex}) ->
    #heartbeat_reply{term = CurTerm, query_index = QueryIndex}.

update_heartbeat_rpc_effects(#{query_index := QueryIndex,
                               queries_waiting_heartbeats := Waiting,
                               current_term := Term,
                               cfg := #cfg{id = Id}} = State) ->
    Peers = peers(State),
    %% TODO: do a quorum evaluation to find a queries to apply and apply all
    %% queries until that point
    case maps:size(Peers) of
        0 ->
            %% Apply all if there are no peers.
            {_, QueryRefs} = lists:unzip(queue:to_list(Waiting)),
            Effects = apply_consistent_queries_effects(QueryRefs, State),
            {State#{queries_waiting_heartbeats => queue:new()}, Effects};
        _ ->
            Effects = heartbeat_rpc_effects(Peers, Id, Term, QueryIndex),
            {State, Effects}
    end.

make_heartbeat_rpc_effects(QueryRef,
                           #{query_index := QueryIndex,
                             queries_waiting_heartbeats := Waiting0,
                             current_term := Term,
                             cfg := #cfg{id = Id}} = State0) ->
    Peers = peers(State0),
    %% TODO: do a quorum evaluation to find a queries to apply and apply all
    %% queries until that point
    case maps:size(Peers) of
        0 ->
            Effects = apply_consistent_queries_effects([QueryRef], State0),
            {State0, Effects};
        _ ->
            NewQueryIndex = QueryIndex + 1,
            State = update_query_index(State0, NewQueryIndex),
            Effects = heartbeat_rpc_effects(Peers, Id, Term, NewQueryIndex),
            Waiting1 = queue:in({NewQueryIndex, QueryRef}, Waiting0),
            {State#{queries_waiting_heartbeats => Waiting1}, Effects}
    end.

update_query_index(State, NewQueryIndex) ->
    State#{query_index => NewQueryIndex}.

reset_query_index(#{cluster := Cluster} = State) ->
    State#{cluster =>
            maps:map(fun(_PeerId, Peer) -> Peer#{query_index => 0} end,
                     Cluster)}.


heartbeat_rpc_effects(Peers, Id, Term, QueryIndex) ->
    lists:filtermap(fun({PeerId, Peer}) ->
                            heartbeat_rpc_effect_for_peer(PeerId, Peer, Id,
                                                          Term, QueryIndex)
                    end,
                    maps:to_list(Peers)).

heartbeat_rpc_effect_for_peer(PeerId, Peer, Id, Term, QueryIndex) ->
    case maps:get(query_index, Peer, 0) < QueryIndex of
        true ->
            {true,
             {send_rpc, PeerId,
              #heartbeat_rpc{query_index = QueryIndex,
                             term = Term,
                             leader_id = Id}}};
        false ->
            false
    end.

heartbeat_rpc_quorum(NewQueryIndex, PeerId,
                     #{queries_waiting_heartbeats := Waiting0} = State) ->
    State1 = update_peer_query_index(PeerId, NewQueryIndex, State),
    ConsensusQueryIndex = get_current_query_quorum(State1),
    {QueryRefs, Waiting1} = take_from_queue_while(
                              fun({QueryIndex, QueryRef}) ->
                                      case QueryIndex > ConsensusQueryIndex of
                                          true  -> false;
                                          false -> {true, QueryRef}
                                      end
                              end,
                              Waiting0),
    case QueryRefs of
        [] -> {[], State1};
        _  -> {QueryRefs, State1#{queries_waiting_heartbeats := Waiting1}}
    end.

update_peer_query_index(PeerId, QueryIndex, #{cluster := Cluster} = State0) ->
    case maps:get(PeerId, Cluster, undefined) of
        undefined ->
            State0;
        #{query_index := PeerQueryIndex} = Peer ->
            case QueryIndex > PeerQueryIndex of
                true  ->
                    put_peer(PeerId,
                             Peer#{query_index => QueryIndex},
                             State0);
                false ->
                    State0
            end
    end.

get_current_query_quorum(State) ->
    agreed_commit(query_indexes(State)).

-spec take_from_queue_while(fun((El) -> {true, Res} | false), queue:queue(El)) ->
    {[Res], queue:queue(El)}.
take_from_queue_while(Fun, Queue) ->
    take_from_queue_while(Fun, Queue, []).

take_from_queue_while(Fun, Queue, Result) ->
    case queue:peek(Queue) of
        {value, El} ->
            case Fun(El) of
                {true, ResVal} ->
                    take_from_queue_while(Fun, queue:drop(Queue),
                                          [ResVal | Result]);
                false ->
                    {Result, Queue}
            end;
        empty ->
            {Result, Queue}
    end.

-spec apply_consistent_queries_effects([consistent_query_ref()],
                                       ra_server_state()) ->
    effects().
apply_consistent_queries_effects(QueryRefs,
                                 #{last_applied := LastApplied} = State) ->
    lists:map(fun({_, _, ReadCommitIndex} = QueryRef) ->
                      true = LastApplied >= ReadCommitIndex,
                      consistent_query_reply(QueryRef, State)
              end, QueryRefs).

-spec consistent_query_reply(consistent_query_ref(), ra_server_state()) -> effect().
consistent_query_reply({From, QueryFun, _ReadCommitIndex},
                       #{cfg := #cfg{id = Id,
                                     machine = {machine, MacMod, _}},
                         machine_state := MacState
                         }) ->
    Result = ra_machine:query(MacMod, QueryFun, MacState),
    {reply, From, {ok, Result, Id}}.

process_pending_consistent_queries(#{cluster_change_permitted := false} = State0, Effects0) ->
    {State0, Effects0};
process_pending_consistent_queries(#{cluster_change_permitted := true,
                                     pending_consistent_queries := Pending} = State0,
                                   Effects0) ->
    %% TODO: submit all pending queries with a single query index.
    lists:foldl(
        fun(QueryRef, {State, Effects}) ->
            {NewState, NewEffects} = make_heartbeat_rpc_effects(QueryRef, State),
            {NewState, NewEffects ++ Effects}
        end,
        {State0#{pending_consistent_queries => []}, Effects0},
        Pending).

incr_counter(#cfg{counter = Cnt}, Ix, N) when Cnt =/= undefined ->
    counters:add(Cnt, Ix, N);
incr_counter(#cfg{counter = undefined}, _Ix, _N) ->
    ok.

meta_name(#cfg{system_config = #{names := #{log_meta := Name}}}) ->
    Name;
meta_name(#{names := #{log_meta := Name}}) ->
    Name.
%%% ===================
%%% Internal unit tests
%%% ===================

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

index_machine_version0_test() ->
    S0 = [{0, 0}],
    ?assertEqual(0, index_machine_version0(0, S0)),
    ?assertEqual(0, index_machine_version0(1123456, S0)),

    S1 = [{100, 4}, {50, 3}, {25, 2}],
    ?assertEqual(4, index_machine_version0(101, S1)),
    ?assertEqual(4, index_machine_version0(100, S1)),
    ?assertEqual(3, index_machine_version0(99, S1)),
    ?assertEqual(2, index_machine_version0(49, S1)),
    ?assertEqual(2, index_machine_version0(25, S1)),
    ?assertExit({machine_version_for_index_not_known, _},
                index_machine_version0(24, S1)),
    ok.

agreed_commit_test() ->
    % one server
    4 = agreed_commit([4]),
    % 2 servers - only leader has seen new commit
    3 = agreed_commit([4, 3]),
    % 2 servers - all servers have seen new commit
    4 = agreed_commit([4, 4, 4]),
    % 3 servers - leader + 1 server has seen new commit
    4 = agreed_commit([4, 4, 3]),
    % only other servers have seen new commit
    4 = agreed_commit([3, 4, 4]),
    % 3 servers - only leader has seen new commit
    3 = agreed_commit([4, 2, 3]),
    ok.

-endif.
