-module(ra_server).

-include("ra.hrl").

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
         leader_id/1,
         current_term/1,
         % TODO: hide behind a handle_leader
         make_rpcs/1,
         update_release_cursor/3,
         persist_last_applied/1,
         update_peer_status/3,
         handle_down/5,
         terminate/2,
         log_fold/3,
         read_at/2,
         recover/1
        ]).

-type ra_await_condition_fun() ::
    fun((ra_msg(), ra_server_state()) -> {boolean(), ra_server_state()}).

-type ra_server_state() ::
    #{id := {ra_server_id(), ra_uid(), unicode:chardata()},
      leader_id => maybe(ra_server_id()),
      cluster := ra_cluster(),
      cluster_change_permitted := boolean(),
      cluster_index_term := ra_idxterm(),
      pending_cluster_changes := [term()],
      previous_cluster => {ra_index(), ra_term(), ra_cluster()},
      current_term := ra_term(),
      log := term(),
      voted_for => maybe(ra_server_id()), % persistent
      votes => non_neg_integer(),
      commit_index := ra_index(),
      last_applied := ra_index(),
      persisted_last_applied => ra_index(),
      stop_after => ra_index(),
      machine := ra_machine:machine(),
      machine_state := term(),
      machine_version := ra_machine:version(),
      machine_versions := [{ra_index(), ra_machine:version()}, ...],
      metrics_key := term(),
      effective_machine_version := ra_machine:version(),
      effective_machine_module := module(),
      aux_state => term(),
      condition => ra_await_condition_fun(),
      condition_timeout_effects => [ra_effect()],
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

-type ra_effect() ::
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
    {incr_metrics, Table :: atom(),
     [{Pos :: non_neg_integer(), Incr :: integer()}]}.

-type ra_effects() :: [ra_effect()].

-type simple_apply_fun(State) :: fun((term(), State) -> State).

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
                              await_condition_timeout => non_neg_integer()}.

-type config() :: ra_server_config().

-export_type([config/0,
              ra_server_state/0,
              ra_state/0,
              ra_server_config/0,
              ra_msg/0,
              machine_conf/0,
              command/0,
              command_type/0,
              command_meta/0,
              command_correlation/0,
              command_reply_mode/0
             ]).

-define(AER_CHUNK_SIZE, 25).
-define(FOLD_LOG_BATCH_SIZE, 25).
% TODO: test what is a good defult here
% TODO: make configurable
-define(MAX_PIPELINE_DISTANCE, 10000).

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
    LogId = maps:get(friendly_name, Config,
                     lists:flatten(io_lib:format("~w", [Id]))),
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

    Log0 = ra_log:init(LogInitArgs#{snapshot_module => SnapModule,
                                    uid => UId,
                                    log_id => LogId}),
    ok = ra_log:write_config(Config, Log0),
    CurrentTerm = ra_log_meta:fetch(UId, current_term, 0),
    LastApplied = ra_log_meta:fetch(UId, last_applied, 0),
    VotedFor = ra_log_meta:fetch(UId, voted_for, undefined),

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

    #{id => {Id, UId, LogId},
      cluster => Cluster0,
      % There may be scenarios when a single server
      % starts up but hasn't
      % yet re-applied its noop command that we may receive other join
      % commands that can't be applied.
      cluster_change_permitted => false,
      cluster_index_term => SnapshotIndexTerm,
      pending_cluster_changes => [],
      current_term => CurrentTerm,
      voted_for => VotedFor,
      commit_index => CommitIndex,
      last_applied => FirstIndex,
      persisted_last_applied => LastApplied,
      log => Log0,
      machine => Machine,
      machine_state => MacState,
      machine_version => LatestMacVer,
      machine_versions => [{SnapshotIdx, MacVer}],
      metrics_key => MetricKey,
      effective_machine_version => MacVer,
      effective_machine_module => MacMod,
      %% aux state is transient and needs to be initialized every time
      aux_state => ra_machine:init_aux(MacMod, Name),
      condition_timeout_effects => [],
      query_index => 0,
      queries_waiting_heartbeats => queue:new(),
      pending_consistent_queries => []}.

recover(#{id := {_, _, LogId},
          commit_index := CommitIndex,
          machine_version := MacVer,
          effective_machine_version := EffMacVer,
          last_applied := LastApplied} = State0) ->
    ?DEBUG("~s: recovering state machine version ~b:~b from index ~b to ~b~n",
           [LogId,  EffMacVer, MacVer, LastApplied, CommitIndex]),
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
    Log = ra_log:release_resources(1, Log0),
    State#{log => Log}.

-spec handle_leader(ra_msg(), ra_server_state()) ->
    {ra_state(), ra_server_state(), ra_effects()}.
handle_leader({PeerId, #append_entries_reply{term = Term, success = true,
                                             next_index = NextIdx,
                                             last_index = LastIdx}},
              State0 = #{current_term := Term, id := {Id, _, LogId}}) ->
    case peer(PeerId, State0) of
        undefined ->
            ?WARN("~s: saw append_entries_reply from unknown peer ~w~n",
                  [LogId, PeerId]),
            {leader, State0, []};
        Peer0 = #{match_index := MI, next_index := NI} ->
            Peer = Peer0#{match_index => max(MI, LastIdx),
                          next_index => max(NI, NextIdx)},
            State1 = update_peer(PeerId, Peer, State0),
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
                #{id := {Id, _, _}, cluster := #{Id := _}} ->
                    % leader is in the cluster
                    {leader, State, Effects};
                #{commit_index := CI, cluster_index_term := {CITIndex, _},
                  id := {_, _, LogId}}
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
                id := {_, _, LogId}} = State0) when Term > CurTerm ->
    case peer(PeerId, State0) of
        undefined ->
            ?WARN("~s saw append_entries_reply from unknown peer ~w~n",
                  [LogId, PeerId]),
            {leader, State0, []};
        _ ->
            ?NOTICE("~s leader saw append_entries_reply for term ~b "
                    "abdicates term: ~b!~n",
                    [LogId, Term, CurTerm]),
            {follower, update_term(Term, State0), []}
    end;
handle_leader({PeerId, #append_entries_reply{success = false,
                                             next_index = NextIdx,
                                             last_index = LastIdx,
                                             last_term = LastTerm}},
              State0 = #{id := {_, _, LogId},
                         cluster := Nodes, log := Log0}) ->
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
                                "Resetting peer's state to last_index.~n",
                                [LogId, LastIdx, LastTerm, MI]),
                          {Peer0#{match_index => LastIdx,
                                  next_index => LastIdx + 1}, L};
                      {_EntryTerm, L} ->
                          NextIndex = max(min(NI-1, LastIdx), MI),
                          ?DEBUG("~s: leader received last_index ~b"
                                 " from ~w with term ~b "
                                 "- expected term ~b. Setting"
                                 "next_index to ~b~n",
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
handle_leader({command, Cmd}, State00 = #{id := {_, _, LogId}}) ->
    case append_log_leader(Cmd, State00) of
        {not_appended, State = #{cluster_change_permitted := CCP}} ->
            ?WARN("~s command ~W NOT appended to log, "
                  "cluster_change_permitted ~w~n", [LogId, Cmd, 5, CCP]),
            {leader, State, []};
        {ok, Idx, Term, State0} ->
            {State, _, Effects0} = make_pipelined_rpc_effects(State0, []),
            % check if a reply is required.
            % TODO: refactor - can this be made a bit nicer/more explicit?
            Effects = case Cmd of
                          {_, _, _, await_consensus} ->
                              Effects0;
                          {_, #{from := From}, _, _} ->
                              [{reply, From,
                                {wrap_reply, {Idx, Term}}} | Effects0];
                          _ ->
                              Effects0
                      end,
            {leader, State, Effects}
    end;
handle_leader({commands, Cmds}, State00) ->
    %% TODO: refactor to use wal batch API?
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

    {State, _, Effects} = make_pipelined_rpc_effects(length(Cmds), State0,
                                                  Effects0),
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
              #{id := {_, _, LogId}, current_term := CurTerm} = State0)
  when Term > CurTerm ->
    case peer(PeerId, State0) of
        undefined ->
            ?WARN("~s: saw install_snapshot_result from unknown peer ~w~n",
                  [LogId, PeerId]),
            {leader, State0, []};
        _ ->
            ?DEBUG("~s: leader saw install_snapshot_result for term ~b"
                  " abdicates term: ~b!~n", [LogId, Term, CurTerm]),
            {follower, update_term(Term, State0), []}
    end;
handle_leader({PeerId, #install_snapshot_result{last_index = LastIndex}},
              #{id := {_, _, LogId}} = State0) ->
    case peer(PeerId, State0) of
        undefined ->
            ?WARN("~s: saw install_snapshot_result from unknown peer ~w~n",
                  [LogId, PeerId]),
            {leader, State0, []};
        Peer0 ->
            State1 = update_peer(PeerId,
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
                id := {_, _, LogId}} = State0) when Term > CurTerm ->
    case peer(Leader, State0) of
        undefined ->
            ?WARN("~s: saw install_snapshot_rpc from unknown leader ~w~n",
                  [LogId, Leader]),
            {leader, State0, []};
        _ ->
            ?INFO("~s: leader saw install_snapshot_rpc from ~w for term ~b "
                  "abdicates term: ~b!~n",
                  [LogId, Evt#install_snapshot_rpc.leader_id,
                   Term, CurTerm]),
            {follower, update_term(Term, State0), [{next_event, Evt}]}
    end;
handle_leader(#append_entries_rpc{term = Term} = Msg,
              #{current_term := CurTerm,
                id := {_, _, LogId}} = State0) when Term > CurTerm ->
    ?INFO("~s: leader saw append_entries_rpc from ~w for term ~b "
          "abdicates term: ~b!~n",
          [LogId, Msg#append_entries_rpc.leader_id,
           Term, CurTerm]),
    {follower, update_term(Term, State0), [{next_event, Msg}]};
handle_leader(#append_entries_rpc{term = Term}, #{current_term := Term,
                                                  id := {_, _, LogId}}) ->
    ?ERR("~s: leader saw append_entries_rpc for same term ~b"
         " this should not happen!~n", [LogId, Term]),
    exit(leader_saw_append_entries_rpc_in_same_term);
handle_leader(#append_entries_rpc{leader_id = LeaderId},
              #{current_term := CurTerm,
                id := {Id, _, _}} = State0) ->
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
                id := {_, _, LogId}} = State0)
        when CurTerm < Term ->
    ?INFO("~s: leader saw heartbeat_rpc from ~w for term ~b "
          "abdicates term: ~b!~n",
          [LogId, Msg#heartbeat_rpc.leader_id,
           Term, CurTerm]),
    {follower, update_term(Term, State0), [{next_event, Msg}]};
handle_leader(#heartbeat_rpc{term = Term, leader_id = LeaderId},
              #{current_term := CurTerm, id := {Id, _, _}} = State)
        when CurTerm > Term ->
    Reply = heartbeat_reply(State),
    {leader, State, [cast_reply(Id, LeaderId, Reply)]};
handle_leader(#heartbeat_rpc{term = Term},
              #{current_term := CurTerm, id := {_, _, LogId}})
  when CurTerm == Term ->
    ?ERR("~s: leader saw heartbeat_rpc for same term ~b"
         " this should not happen!~n", [LogId, Term]),
    exit(leader_saw_heartbeat_rpc_in_same_term);
handle_leader({PeerId, #heartbeat_reply{query_index = ReplyQueryIndex, term = Term}},
              #{current_term := CurTerm, id := {_, _, LogId}} = State0) ->
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
            ?NOTICE("~s leader saw heartbeat_reply for term ~b "
                    "abdicates term: ~b!~n",
                    [LogId, Term, CurTerm]),
            {follower, update_term(Term, State0), []}
    end;
handle_leader(#request_vote_rpc{term = Term, candidate_id = Cand} = Msg,
              #{current_term := CurTerm,
                id := {_, _, LogId}} = State0) when Term > CurTerm ->
    case peer(Cand, State0) of
        undefined ->
            ?WARN("~s: leader saw request_vote_rpc for unknown peer ~w~n",
                  [LogId, Cand]),
            {leader, State0, []};
        _ ->
            ?INFO("~s: leader saw request_vote_rpc from ~w for term ~b "
                  "abdicates term: ~b!~n",
                  [LogId, Msg#request_vote_rpc.candidate_id,
                   Term, CurTerm]),
            {follower, update_term(Term, State0), [{next_event, Msg}]}
    end;
handle_leader(#request_vote_rpc{}, State = #{current_term := Term}) ->
    Reply = #request_vote_result{term = Term, vote_granted = false},
    {leader, State, [{reply, Reply}]};
handle_leader(#pre_vote_rpc{term = Term, candidate_id = Cand} = Msg,
              #{current_term := CurTerm,
                id := {_, _, LogId}} = State0) when Term > CurTerm ->
    case peer(Cand, State0) of
        undefined ->
            ?WARN("~s: leader saw pre_vote_rpc for unknown peer ~w~n",
                  [LogId, Cand]),
            {leader, State0, []};
        _ ->
            ?INFO("~s: leader saw pre_vote_rpc for term ~b"
                  " abdicates term: ~b!~n", [LogId, Term, CurTerm]),
            {follower, update_term(Term, State0), [{next_event, Msg}]}
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
handle_leader(Msg, State) ->
    log_unhandled_msg(leader, Msg, State),
    {leader, State, []}.


-spec handle_candidate(ra_msg() | election_timeout, ra_server_state()) ->
    {ra_state(), ra_server_state(), ra_effects()}.
handle_candidate(#request_vote_result{term = Term, vote_granted = true},
                 #{current_term := Term,
                   votes := Votes,
                   machine := Mac,
                   cluster := Nodes} = State0) ->
    NewVotes = Votes + 1,
    case trunc(maps:size(Nodes) / 2) + 1 of
        NewVotes ->
            {State, Effects} = make_all_rpcs(initialise_peers(State0)),
            Noop = {noop, #{ts => os:system_time(millisecond)},
                    ra_machine:version(Mac)},
            {leader, maps:without([votes, leader_id], State),
             [{next_event, cast, {command, Noop}} | Effects]};
        _ ->
            {candidate, State0#{votes => NewVotes}, []}
    end;
handle_candidate(#request_vote_result{term = Term},
                 #{current_term := CurTerm,
                   id := {_, _, LogId}} = State0)
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
                 #{id := {_, _, LogId},
                   current_term := CurTerm} = State0) when Term > CurTerm ->
    ?INFO("~s: candidate heartbeat_reply with higher"
          " term received ~b -> ~b~n",
          [LogId, CurTerm, Term]),
    State = update_term_and_voted_for(Term, undefined, State0),
    {follower, State, []};
handle_candidate({_PeerId, #append_entries_reply{term = Term}},
                 #{current_term := CurTerm,
                   id := {_, _, LogId}} = State0)
  when Term > CurTerm ->
    ?INFO("~s: candidate append_entries_reply with higher"
          " term received ~b -> ~b~n",
          [LogId, CurTerm, Term]),
    State = update_term_and_voted_for(Term, undefined, State0),
    {follower, State, []};
handle_candidate(#request_vote_rpc{term = Term} = Msg,
                 #{current_term := CurTerm,
                   id := {_, _, LogId}} = State0)
  when Term > CurTerm ->
    ?INFO("~s: candidate request_vote_rpc with higher term received ~b -> ~b~n",
          [LogId, CurTerm, Term]),
    State = update_term_and_voted_for(Term, undefined, State0),
    {follower, State, [{next_event, Msg}]};
handle_candidate(#pre_vote_rpc{term = Term} = Msg,
                 #{current_term := CurTerm,
                   id := {_, _, LogId}} = State0)
  when Term > CurTerm ->
    ?INFO("~s: candidate pre_vote_rpc with higher term received ~b -> ~b~n",
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
    {pre_vote, State#{log => Log}, Effects};
handle_candidate(election_timeout, State) ->
    call_for_election(candidate, State);
handle_candidate(Msg, State) ->
    log_unhandled_msg(candidate, Msg, State),
    {candidate, State, []}.

-spec handle_pre_vote(ra_msg(), ra_server_state()) ->
    {ra_state(), ra_server_state(), ra_effects()}.
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
                #{current_term := Term, votes := Votes,
                  pre_vote_token := Token,
                  cluster := Nodes} = State0) ->
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
handle_pre_vote(Msg, State) ->
    log_unhandled_msg(pre_vote, Msg, State),
    {pre_vote, State, []}.


-spec handle_follower(ra_msg(), ra_server_state()) ->
    {ra_state(), ra_server_state(), ra_effects()}.
handle_follower(#append_entries_rpc{term = Term,
                                    leader_id = LeaderId,
                                    leader_commit = LeaderCommit,
                                    prev_log_index = PLIdx,
                                    prev_log_term = PLTerm,
                                    entries = Entries0},
                State00 = #{log := Log00,
                            id := {Id, _, LogId}, current_term := CurTerm})
  when Term >= CurTerm ->
    State0 = update_term(Term, State00),
    case has_log_entry_or_snapshot(PLIdx, PLTerm, Log00) of
        {entry_ok, Log0} ->
            % filter entries already seen
            {Log1, Entries} = drop_existing({Log0, Entries0}),
            case Entries of
                [] ->
                    LastIdx = ra_log:last_index_term(Log1),
                    Log2 = case Entries0 of
                               [] when element(1, LastIdx) > PLIdx ->
                                   %% if no entries were sent we need to reset
                                   %% last index to match the leader
                                   ?DEBUG("~s: resetting last index to ~b~n",
                                         [LogId, PLIdx]),
                                   {ok, L} = ra_log:set_last_index(PLIdx, Log1),
                                   L;
                               _ ->
                                   Log1
                           end,
                    % update commit index to be the min of the last
                    % entry seen (but not necessarily written)
                    % and the leader commit
                    {Idx, _} = ra_log:last_index_term(Log2),
                    State1 = State0#{commit_index => min(Idx, LeaderCommit),
                                     log => Log2,
                                     leader_id => LeaderId},
                    % evaluate commit index as we may have received an updated
                    % commit index for previously written entries
                    evaluate_commit_index_follower(State1, []);
                [{FirstIdx, _, _} | _] -> % FirstTerm

                    {LastIdx, State1} = lists:foldl(
                                          fun pre_append_log_follower/2,
                                          {FirstIdx, State0}, Entries),
                    % Increment only commit_index here as we are not applying
                    % anything at this point.
                    % last_applied will be incremented when the written event is
                    % processed
                    State = State1#{commit_index => min(LeaderCommit, LastIdx),
                                    leader_id => LeaderId},
                    case ra_log:write(Entries, Log1) of
                        {ok, Log} ->
                            {follower, State#{log => Log}, []};
                        {error, wal_down} ->
                            {await_condition,
                             State#{condition => fun wal_down_condition/2}, []};
                        {error, _} = Err ->
                            exit(Err)
                    end
            end;
        {missing, Log0} ->
            Reply = append_entries_reply(Term, false, State0),
            ?INFO("~s: follower did not have entry at ~b in ~b."
                  " Requesting ~w from ~b~n",
                  [LogId, PLIdx, PLTerm, LeaderId, Reply#append_entries_reply.next_index]),
            Effects = [cast_reply(Id, LeaderId, Reply)],
            {await_condition,
             State0#{leader_id => LeaderId,
                     log => Log0,
                     condition => follower_catchup_cond_fun(missing),
                     % repeat reply effect on condition timeout
                     condition_timeout_effects => Effects}, Effects};
        {term_mismatch, OtherTerm, Log0} ->
            CommitIndex = maps:get(commit_index, State0),
            ?INFO("~s: term mismatch - follower had entry at ~b with term ~b "
                  "but not with term ~b~n"
                  "Asking leader ~w to resend from ~b~n",
                  [LogId, PLIdx, OtherTerm, PLTerm, LeaderId, CommitIndex + 1]),
            % This situation arises when a minority leader replicates entries
            % that it cannot commit then gets replaced by a majority leader
            % that also has made progress
            % As the follower is responsible for telling the leader
            % which their next expected entry is the best we can do here
            % is rewind back and use the commit index as the last index
            % and commit_index + 1 as the next expected.
            % This _may_ overwrite some valid entries but is probably the
            % simplest way to proceed
            {Reply, State} = mismatch_append_entries_reply(Term, CommitIndex,
                                                           State0),
            Effects = [cast_reply(Id, LeaderId, Reply)],
            {await_condition,
             State#{leader_id => LeaderId,
                    log => Log0,
                    condition => follower_catchup_cond_fun(term_mismatch),
                    % repeat reply effect on condition timeout
                    condition_timeout_effects => Effects}, Effects}
    end;
handle_follower(#append_entries_rpc{term = _Term, leader_id = LeaderId},
                #{id := {Id, _, LogId},
                  current_term := CurTerm} = State) ->
    % the term is lower than current term
    Reply = append_entries_reply(CurTerm, false, State),
    ?DEBUG("~s: follower got append_entries_rpc from ~w in"
           " ~b but current term is: ~b~n",
          [LogId, LeaderId, _Term, CurTerm]),
    {follower, State, [cast_reply(Id, LeaderId, Reply)]};
handle_follower(#heartbeat_rpc{query_index = RpcQueryIndex, term = Term,
                               leader_id = LeaderId},
                #{current_term := CurTerm, id := {Id, _, _}} = State0)
  when Term >= CurTerm ->
    State1 = update_term(Term, State0),
    #{query_index := QueryIndex} = State1,
    NewQueryIndex = max(RpcQueryIndex, QueryIndex),
    State2 = update_query_index(State1#{leader_id => LeaderId}, NewQueryIndex),
    Reply = heartbeat_reply(State2),
    {follower, State2, [cast_reply(Id, LeaderId, Reply)]};
handle_follower(#heartbeat_rpc{leader_id = LeaderId},
                #{id := {Id, _, _}} = State)->
    Reply = heartbeat_reply(State),
    {follower, State, [cast_reply(Id, LeaderId, Reply)]};
handle_follower({ra_log_event, {written, _} = Evt},
                State0 = #{log := Log0}) ->
    {Log, Effects} = ra_log:handle_event(Evt, Log0),
    State = State0#{log => Log},
    evaluate_commit_index_follower(State, Effects);
handle_follower({ra_log_event, Evt}, State = #{log := Log0}) ->
    % simply forward all other events to ra_log
    {Log, Effects} = ra_log:handle_event(Evt, Log0),
    {follower, State#{log => Log}, Effects};
handle_follower(#pre_vote_rpc{} = PreVote, State) ->
    process_pre_vote(follower, PreVote, State);
handle_follower(#request_vote_rpc{candidate_id = Cand, term = Term},
                #{current_term := Term, voted_for := VotedFor,
                  id := {_, _, LogId}} = State)
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
                  id := {_, _, LogId}} = State0)
  when Term >= CurTerm ->
    State = update_term(Term, State0),
    LastIdxTerm = last_idx_term(State),
    case is_candidate_log_up_to_date(LLIdx, LLTerm, LastIdxTerm) of
        true ->
            ?INFO("~s: granting vote for ~w with last indexterm ~w"
                  " for term ~b previous term was ~b~n",
                  [LogId, Cand, {LLIdx, LLTerm}, Term, CurTerm]),
            Reply = #request_vote_result{term = Term, vote_granted = true},
            {follower, State#{voted_for => Cand, current_term => Term},
             [{reply, Reply}]};
        false ->
            ?INFO("~s: declining vote for ~w for term ~b,"
                  " candidate last log index term was: ~w~n"
                  " last log entry idxterm seen was: ~w~n",
                  [LogId, Cand, Term, {LLIdx, LLTerm}, {LastIdxTerm}]),
            Reply = #request_vote_result{term = Term, vote_granted = false},
            {follower, State#{current_term => Term}, [{reply, Reply}]}
    end;
handle_follower(#request_vote_rpc{term = Term, candidate_id = _Cand},
                State = #{current_term := CurTerm,
                          id := {_, _, LogId}})
  when Term < CurTerm ->
    ?INFO("~s: declining vote to ~w for term ~b, current term ~b~n",
          [LogId, _Cand, Term, CurTerm]),
    Reply = #request_vote_result{term = CurTerm, vote_granted = false},
    {follower, State, [{reply, Reply}]};
handle_follower({_PeerId, #append_entries_reply{term = Term}},
                State = #{current_term := CurTerm}) when Term > CurTerm ->
    {follower, update_term(Term, State), []};
handle_follower({_PeerId, #heartbeat_reply{term = Term}},
                State = #{current_term := CurTerm}) when Term > CurTerm ->
    {follower, update_term(Term, State), []};
handle_follower(#install_snapshot_rpc{term = Term,
                                      meta = #{index := LastIndex,
                                               term := LastTerm}},
                State = #{id := {_, _, LogId}, current_term := CurTerm})
  when Term < CurTerm ->
    ?DEBUG("~s: install_snapshot old term ~b in ~b~n",
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
                #{id := {_, _, LogId}, log := Log0,
                  last_applied := LastApplied,
                  current_term := CurTerm} = State0)
  when Term >= CurTerm andalso SnapIdx > LastApplied ->
    %% only begin snapshot procedure if Idx is higher than the last_applied
    %% index.
    ?DEBUG("~s: begin_accept snapshot at index ~b in term ~b~n",
           [LogId, SnapIdx, Term]),
    SnapState0 = ra_log:snapshot_state(Log0),
    {ok, SS} = ra_snapshot:begin_accept(Meta, SnapState0),
    Log = ra_log:set_snapshot_state(SS, Log0),
    {receive_snapshot, State0#{log => Log,
                               leader_id => LeaderId}, [{next_event, Rpc}]};
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
handle_follower(Msg, State) ->
    log_unhandled_msg(follower, Msg, State),
    {follower, State, []}.

handle_receive_snapshot(#install_snapshot_rpc{term = Term,
                                              meta = #{index := LastIndex,
                                                       term := LastTerm},
                                              chunk_state = {Num, ChunkFlag},
                                              data = Data},
                        #{id := {Id, _, LogId}, log := Log0,
                          current_term := CurTerm} = State0)
  when Term >= CurTerm ->
    ?DEBUG("~s: receiving snapshot chunk: ~b / ~w~n",
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
            Log = ra_log:install_snapshot({LastIndex, LastTerm},
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
            {follower, persist_last_applied(State), [{reply, Reply}]};
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
handle_receive_snapshot(Msg, State) ->
    log_unhandled_msg(receive_snapshot, Msg, State),
    %% drop all other events??
    %% TODO: work out what else to handle
    {receive_snapshot, State, []}.

-spec handle_await_condition(ra_msg(), ra_server_state()) ->
    {ra_state(), ra_server_state(), ra_effects()}.
handle_await_condition(#request_vote_rpc{} = Msg, State) ->
    {follower, State, [{next_event, Msg}]};
handle_await_condition(election_timeout, State) ->
    call_for_election(pre_vote, State);
handle_await_condition(await_condition_timeout,
                       #{condition_timeout_effects := Effects} = State) ->
    {follower, State#{condition_timeout_effects => []}, Effects};
handle_await_condition({ra_log_event, Evt}, State = #{log := Log0}) ->
    % simply forward all other events to ra_log
    {Log, Effects} = ra_log:handle_event(Evt, Log0),
    {await_condition, State#{log => Log}, Effects};
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

-spec tick(ra_server_state()) -> ra_effects().
tick(#{effective_machine_module := MacMod,
       machine_state := MacState}) ->
    Now = os:system_time(millisecond),
    ra_machine:tick(MacMod, Now, MacState).

-spec handle_state_enter(ra_state() | eol, ra_server_state()) ->
    {ra_server_state() | eol, ra_effects()}.
handle_state_enter(RaftState, #{effective_machine_module := MacMod,
                                machine_state := MacState} = State) ->
    {become(RaftState, State),
     ra_machine:state_enter(MacMod, RaftState, MacState)}.


-spec overview(ra_server_state()) -> map().
overview(#{log := Log, effective_machine_module := MacMod,
           machine_state := MacState} = State) ->
    O = maps:with([uid, current_term, commit_index, last_applied,
                   cluster, leader_id, voted_for,
                   machine_version, effective_machine_version], State),
    LogOverview = ra_log:overview(Log),
    MacOverview = ra_machine:overview(MacMod, MacState),
    O#{log => LogOverview,
       machine => MacOverview}.

-spec metrics(ra_server_state()) ->
    {atom(), ra_term(),
     ra_index(), ra_index(),
     ra_index(), ra_index(), non_neg_integer()}.
metrics(#{metrics_key := Key,
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
                 -1
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
            MinMI >= CI
    end.

handle_aux(RaftState, Type, Cmd, #{aux_state := Aux0, log := Log0,
                                   effective_machine_module := MacMod,
                                   machine_state := MacState0} = State0) ->
    case ra_machine:handle_aux(MacMod, RaftState, Type, Cmd, Aux0,
                               Log0, MacState0) of
        {reply, Reply, Aux, Log} ->
            {RaftState, State0#{log => Log, aux_state => Aux},
             [{reply, Reply}]};
        {no_reply, Aux, Log} ->
            {RaftState, State0#{log => Log, aux_state => Aux}, []};
        undefined ->
            {RaftState, State0, []}
    end.

% property helpers

-spec id(ra_server_state()) -> ra_server_id().
id(#{id := {Id, _, _}}) -> Id.

log_id(#{id := {_, _, LogId}}) -> LogId.

-spec uid(ra_server_state()) -> ra_uid().
uid(#{id := {_, UId, _}}) -> UId.

-spec leader_id(ra_server_state()) -> maybe(ra_server_id()).
leader_id(State) ->
    maps:get(leader_id, State, undefined).

-spec current_term(ra_server_state()) -> maybe(ra_term()).
current_term(State) ->
    maps:get(current_term, State).

% Internal

become(leader, #{cluster := Cluster, log := Log0} = State) ->
    Log = ra_log:release_resources(maps:size(Cluster) + 1, Log0),
    State#{log => Log};
become(follower, #{log := Log0} = State) ->
    %% followers should only ever need a single segment open at any one
    %% time
    State#{log => ra_log:release_resources(1, Log0)};
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

evaluate_commit_index_follower(#{commit_index := CommitIndex,
                                 id := {Id, _, _},
                                 leader_id := LeaderId,
                                 current_term := Term,
                                 log := Log} = State0, Effects0)
  when LeaderId =/= undefined ->
    % as writes are async we can't use the index of the last available entry
    % in the log as they may not have been fully persisted yet
    % Take the smaller of the two values as commit index may be higher
    % than the last entry received
    {Idx, _} = ra_log:last_written(Log),
    EffectiveCommitIndex = min(Idx, CommitIndex),
    % neet catch termination throw
    case catch apply_to(EffectiveCommitIndex, State0, Effects0) of
        {delete_and_terminate, State1, Effects} ->
            Reply = append_entries_reply(Term, true, State1),
            {delete_and_terminate, State1,
             [cast_reply(Id, LeaderId, Reply) |
              filter_follower_effects(Effects)]};
        {State, Effects1} ->
            % filter the effects that should be applied on a follower
            Effects = filter_follower_effects(Effects1),
            Reply = append_entries_reply(Term, true, State),

            {follower, State, [cast_reply(Id, LeaderId, Reply) | Effects]}
    end;
evaluate_commit_index_follower(State, Effects) ->
    %% when no leader is known
    {follower, State, Effects}.

filter_follower_effects(Effects) ->
    lists:reverse(lists:foldl(
                    fun ({release_cursor, _, _} = C, Acc) ->
                            [C | Acc];
                        ({incr_metrics, _, _} = C, Acc) ->
                            [C | Acc];
                        ({aux, _} = C, Acc) ->
                            [C | Acc];
                        (garbage_collection = C, Acc) ->
                            [C | Acc];
                        ({delete_snapshot, _} = C, Acc) ->
                            [C | Acc];
                        ({monitor, process, Comp, _} = C, Acc)
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
                    end, [], Effects)).

make_pipelined_rpc_effects(State, Effects) ->
    make_pipelined_rpc_effects(?AER_CHUNK_SIZE, State, Effects).

make_pipelined_rpc_effects(MaxBatchSize, #{id := {Id, _, _},
                                           commit_index := CommitIndex,
                                           log := Log,
                                           cluster := Cluster} = State,
                           Effects) ->
    NextLogIdx = ra_log:next_index(Log),
    maps:fold(
      fun (I, _, Acc) when I =:= Id ->
              %% oneself
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
              case NI - MI < ?MAX_PIPELINE_DISTANCE of
                  true ->
                      {NextIdx, Eff, S} =
                      make_rpc_effect(PeerId, NI,
                                      MaxBatchSize, S0),
                      Peer = Peer0#{next_index => NextIdx,
                                    commit_index_sent => CommitIndex},
                      %% is there more potentially pipelining
                      More = More0 orelse (NextIdx < NextLogIdx andalso
                                           NextIdx - MI < ?MAX_PIPELINE_DISTANCE),
                      {update_peer(PeerId, Peer, S),
                       More,
                       [Eff | Effs]};
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
    {State2, EffectsAER} = make_rpcs_for(peers_not_sending_snapshots(State1), State1),
    {State2, EffectsAER ++ EffectsHR}.

make_rpcs_for(Peers, State) ->
    maps:fold(fun(PeerId, #{next_index := Next}, {S0, Effs}) ->
                      {_, Eff, S} =
                          make_rpc_effect(PeerId, Next, ?AER_CHUNK_SIZE, S0),
                      {S, [Eff | Effs]}
              end, {State, []}, Peers).

make_rpc_effect(PeerId, Next, MaxBatchSize,
                #{id := {Id, _, _}, log := Log0,
                  current_term := Term} = State) ->
    PrevIdx = Next - 1,
    case ra_log:fetch_term(PrevIdx, Log0) of
        {PrevTerm, Log} when is_integer(PrevTerm) ->
            make_append_entries_rpc(PeerId, PrevIdx, PrevTerm, MaxBatchSize,
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
                          id := {Id, _, _},
                          commit_index := CommitIndex} = State) ->
    Next = PrevIdx + 1,
    %% TODO: refactor to avoid lists:last call later
    %% ra_log:take should be able to return the actual number of entries
    %% read at fixed cost
    {Entries, Log} = ra_log:take(Next, Num, Log0),
    NextIndex = case Entries of
                    [] -> Next;
                    _ ->
                        {LastIdx, _, _} = lists:last(Entries),
                        %% assertion
                        {Next, _, _} = hd(Entries),
                        LastIdx + 1
                end,
    {NextIndex,
     {send_rpc, PeerId, #append_entries_rpc{entries = Entries,
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
    {ra_server_state(), ra_effects()}.
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
                       id := {_, UId, _}} = State) ->
    ok = ra_log_meta:store(UId, last_applied, LastApplied),
    State#{persisted_last_applied => LastApplied}.


-spec update_peer_status(ra_server_id(), ra_peer_status(),
                         ra_server_state()) -> ra_server_state().
update_peer_status(PeerId, Status, #{cluster := Peers} = State) ->
    Peer = maps:put(status, Status, maps:get(PeerId, Peers)),
    State#{cluster => maps:put(PeerId, Peer, Peers)}.

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
             update_peer(PeerId, Peer#{status => normal}, State);
         _ ->
             State
     end.

-spec handle_down(ra_state(), machine | snapshot_sender | snapshot_writer,
                  pid(), term(), ra_server_state()) ->
    {ra_state(), ra_server_state(), ra_effects()}.
handle_down(leader, machine, Pid, Info, State) ->
    %% commit command to be processed by state machine
    handle_leader({command,  {'$usr', #{ts => os:system_time(millisecond)},
                             {down, Pid, Info}, noreply}},
                  State);
handle_down(leader, snapshot_sender, Pid, Info, #{id := {_, _, LogId}} = State) ->
    ?DEBUG("~s: Snapshot sender process ~w exited with ~W~n",
          [LogId, Pid, Info, 10]),
    {leader, peer_snapshot_process_exited(Pid, State), []};
handle_down(RaftState, snapshot_writer, Pid, Info,
            #{id := {_, _, LogId}, log := Log0} = State) ->
    case Info of
        noproc -> ok;
        normal -> ok;
        _ ->
            ?WARN("~s: Snapshot write process ~w exited with ~w~n",
                  [LogId, Pid, Info])
    end,
    SnapState0 = ra_log:snapshot_state(Log0),
    SnapState = ra_snapshot:handle_down(Pid, Info, SnapState0),
    Log = ra_log:set_snapshot_state(SnapState, Log0),
    {RaftState, State#{log => Log}, []}.


-spec terminate(ra_server_state(), Reason :: {shutdown, delete} | term()) -> ok.
terminate(#{log := Log,
            id := {_, _, LogId}} = _State, {shutdown, delete}) ->
    ?NOTICE("~s: terminating with reason 'delete'~n", [LogId]),
    catch ra_log:delete_everything(Log),
    ok;
terminate(#{id := {_, _, LogId}} = State, Reason) ->
    ?DEBUG("~s: terminating with reason '~w'~n", [LogId, Reason]),
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
               id := {_, _, LogId}} = RaState) ->
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

call_for_election(candidate, #{id := {Id, _, LogId},
                               current_term := CurrentTerm} = State0) ->
    NewTerm = CurrentTerm + 1,
    ?DEBUG("~s: election called for in term ~b~n", [LogId, NewTerm]),
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
call_for_election(pre_vote, #{id := {Id, _, LogId},
                              machine_version := MacVer,
                              current_term := Term} = State0) ->
    ?DEBUG("~s: pre_vote election called for in term ~b~n", [LogId, Term]),
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
                 #{current_term := CurTerm,
                   machine_version := OurMacVer}= State0)
  when Term >= CurTerm  ->
    State = update_term(Term, State0),
    LastIdxTerm = last_idx_term(State),
    case is_candidate_log_up_to_date(LLIdx, LLTerm, LastIdxTerm) of
        true when Version > ?RA_PROTO_VERSION->
            ?DEBUG("~s: declining pre-vote for ~w for protocol version ~b~n",
                   [log_id(State0), Cand, Version]),
            {FsmState, State, [{reply, pre_vote_result(Term, Token, false)}]};
        true when OurMacVer =/= TheirMacVer->
            ?DEBUG("~s: declining pre-vote for ~w their machine version ~b"
                   " ours is ~b~n",
                   [log_id(State0), Cand, TheirMacVer, OurMacVer]),
            {FsmState, State, [{reply, pre_vote_result(Term, Token, false)}]};
        true ->
            ?DEBUG("~s: granting pre-vote for ~w"
                   " machine version (their:ours) ~b:~b"
                   " with last indexterm ~w"
                   " for term ~b previous term ~b~n",
                   [log_id(State0), Cand, TheirMacVer, OurMacVer,
                    {LLIdx, LLTerm}, Term, CurTerm]),
            {FsmState, State#{voted_for => Cand},
             [{reply, pre_vote_result(Term, Token, true)}]};
        false ->
            ?DEBUG("~s: declining pre-vote for ~w for term ~b,"
                   " candidate last log index term was: ~w~n"
                   "Last log entry idxterm seen was: ~w~n",
                   [log_id(State0), Cand, Term, {LLIdx, LLTerm}, LastIdxTerm]),
            case FsmState of
                follower ->
                    %% immediately enter pre_vote election as this node is more
                    %% likely to win but could be held back by a persistent
                    %% stale pre voter
                    call_for_election(pre_vote, State);
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
    ?DEBUG("~s declining pre-vote to ~w for term ~b, current term ~b~n",
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
      query_index => 0}.

new_peer_with(Map) ->
    maps:merge(new_peer(), Map).

peers(#{id := {Id, _, _}, cluster := Peers}) ->
    maps:remove(Id, Peers).

%% remove any peers that are currently receiving a snapshot
peers_not_sending_snapshots(State) ->
    maps:filter(fun (_, #{status := {sending_snapshot, _}}) -> false;
                    (_, _) -> true
                end, peers(State)).

% peers that could need an update
stale_peers(#{commit_index := CommitIndex} = State) ->
    maps:filter(fun (_, #{status := {sending_snapshot, _}}) ->
                        false;
                    (_, #{next_index := NI,
                          match_index := MI})
                      when MI < NI - 1 ->
                        % there are unconfirmed items
                        true;
                    (_, #{commit_index_sent := CI})
                      when CI < CommitIndex ->
                        % the commit index has been updated
                        true;
                    (_, _Peer) ->
                        false
                end, peers(State)).

peer_ids(State) ->
    maps:keys(peers(State)).

peer(PeerId, #{cluster := Nodes}) ->
    maps:get(PeerId, Nodes, undefined).

update_peer(PeerId, Peer, #{cluster := Nodes} = State) ->
    State#{cluster => Nodes#{PeerId => Peer}}.

update_term_and_voted_for(Term, VotedFor, #{id := {_, UId, _},
                                            current_term := CurTerm} = State) ->
    CurVotedFor = maps:get(voted_for, State, undefined),
    case Term =:= CurTerm andalso VotedFor =:= CurVotedFor of
        true ->
            %% no update needed
            State;
        false ->
            ok = ra_log_meta:store(UId, current_term, Term),
            ok = ra_log_meta:store_sync(UId, voted_for, VotedFor),
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

fetch_term(Idx, #{log := Log}) ->
    ra_log:fetch_term(Idx, Log).

fetch_entries(From, To, #{log := Log0} = State) ->
    {Entries, Log} = ra_log:take(From, To - From + 1, Log0),
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
           machine_version := MacVer,
           effective_machine_module := MacMod,
           effective_machine_version := EffMacVer,
           machine_state := MacState0} = State0)
  when ApplyTo > LastApplied andalso MacVer >= EffMacVer ->
    From = LastApplied + 1,
    To = min(From + 1024, ApplyTo),
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
                                    undefined;
                                _ when is_integer(LastTs) ->
                                    os:system_time(millisecond) - LastTs
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
            #{machine_version := MacVer,
              effective_machine_version := Effective} = State,
            MacSt, Effects, Notifys, LastTs})
      when MacVer < Effective ->
    %% we cannot apply any further entries
    {Mod, LastAppliedIdx, State, MacSt, Effects, Notifys, LastTs};
apply_with({Idx, Term, {'$usr', CmdMeta, Cmd, ReplyType}},
           {Module, _LastAppliedIdx,
            State = #{effective_machine_version := MacVer},
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
                    ?DEBUG("~s: applying ra cluster change to ~w~n",
                           [log_id(State0), maps:keys(NewCluster)]),
                    %% we are recovering and should apply the cluster change
                    State0#{cluster => NewCluster,
                            cluster_change_permitted => true,
                            cluster_index_term => {Idx, Term}};
                _  ->
                    ?DEBUG("~s: committing ra cluster change to ~w~n",
                           [log_id(State0), maps:keys(NewCluster)]),
                    %% else just enable further cluster changes again
                    State0#{cluster_change_permitted => true}
            end,
    % add pending cluster change as next event
    {Effects1, State1} = add_next_cluster_change(Effects, State),
    {Mod, Idx, State1, MacSt, Effects1, Notifys, LastTs};
apply_with({Idx, Term, {noop, CmdMeta, NextMacVer}},
           {CurModule, LastAppliedIdx,
            #{current_term := CurrentTerm,
              machine := Machine,
              machine_version := MacVer,
              %% active machine versions and their index (from last snapshot)
              machine_versions := MacVersions,
              cluster_change_permitted := ClusterChangePerm0,
              effective_machine_version := OldMacVer,
              id := {_, _, LogId}} = State0,
            MacSt, Effects, Notifys, LastTs}) ->
    ClusterChangePerm = case CurrentTerm of
                            Term ->
                                ?DEBUG("~s: enabling ra cluster changes in"
                                       " ~b~n", [LogId, Term]),
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
            State = State0#{cluster_change_permitted => ClusterChangePerm,
                            effective_machine_version => NextMacVer,
                            %% record this machine version "term"
                            machine_versions => [{Idx, MacVer} | MacVersions],
                            effective_machine_module => Module},
            Meta = augment_command_meta(Idx, Term, MacVer, CmdMeta),
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
                   " cannot apply any further entries~n",
                   [LogId, NextMacVer, MacVer]),
            State = State0#{effective_machine_version => NextMacVer},
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
    % TODO: remove to make more strics, ideally we should not need a catch all
    ?WARN("~s: apply_with: unhandled command: ~W~n",
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

add_next_cluster_change(Effects,
                        #{pending_cluster_changes := [C | Rest]} = State) ->
    {_, #{from := From} , _, _} = C,
    {[{next_event, {call, From}, {command, C}} | Effects],
     State#{pending_cluster_changes => Rest}};
add_next_cluster_change(Effects, State) ->
    {Effects, State}.

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

append_log_leader({CmdTag, _, _, _} = Cmd,
                  State = #{cluster_change_permitted := false,
                            pending_cluster_changes := Pending})
  when CmdTag == '$ra_join' orelse
       CmdTag == '$ra_leave' ->
    % cluster change is in progress or leader has not yet committed anything
    % in this term - stash the request
    {not_appended, State#{pending_cluster_changes => Pending ++ [Cmd]}};
append_log_leader({'$ra_join', From, JoiningNode, ReplyMode},
                  State = #{cluster := OldCluster}) ->
    case OldCluster of
        #{JoiningNode := _} ->
            % already a member do nothing
            % TODO: reply? If we don't reply the caller may block until timeout
            {not_appended, State};
        _ ->
            Cluster = OldCluster#{JoiningNode => new_peer()},
            append_cluster_change(Cluster, From, ReplyMode, State)
    end;
append_log_leader({'$ra_leave', From, LeavingNode, ReplyMode},
                  State = #{cluster := OldCluster}) ->
    case OldCluster of
        #{LeavingNode := _} ->
            Cluster = maps:remove(LeavingNode, OldCluster),
            append_cluster_change(Cluster, From, ReplyMode, State);
        _ ->
            % not a member - do nothing
            {not_appended, State}
    end;
append_log_leader(Cmd, State = #{log := Log0, current_term := Term}) ->
    NextIdx = ra_log:next_index(Log0),
    Log = ra_log:append({NextIdx, Term, Cmd}, Log0),
    {ok, NextIdx, Term, State#{log => Log}}.

pre_append_log_follower({Idx, Term, Cmd} = Entry,
                        {_, State = #{cluster_index_term := {Idx, CITTerm}}})
  when Term /= CITTerm ->
    % the index for the cluster config entry has a different term, i.e.
    % it has been overwritten by a new leader. Unless it is another cluster
    % change (can this even happen?) we should revert back to the last known
    % cluster
    case Cmd of
        {'$ra_cluster_change', _, Cluster, _} ->
            {Idx, State#{cluster => Cluster,
                         cluster_index_term => {Idx, Term}}};
        _ ->
            % revert back to previous cluster
            {PrevIdx, PrevTerm, PrevCluster} = maps:get(previous_cluster,
                                                        State),
            State1 = State#{cluster => PrevCluster,
                            cluster_index_term => {PrevIdx, PrevTerm}},
            pre_append_log_follower(Entry, {Idx, State1})
    end;
pre_append_log_follower({Idx, Term, {'$ra_cluster_change', _, Cluster, _}},
                    {_, State}) ->
    {{Idx, Term}, State#{cluster => Cluster,
                         cluster_index_term => {Idx, Term}}};
pre_append_log_follower({Idx, _, _}, {_, State}) ->
    {Idx, State}.

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

mismatch_append_entries_reply(Term, CommitIndex, State = #{log := Log0}) ->
    {CITerm, Log} = ra_log:fetch_term(CommitIndex, Log0),
    % assert CITerm is found
    false = CITerm =:= undefined,
    {#append_entries_reply{term = Term, success = false,
                           next_index = CommitIndex + 1,
                           last_index = CommitIndex,
                           last_term = CITerm},
     State#{log => Log}}.

append_entries_reply(Term, Success, State = #{log := Log}) ->
    % ah - we can't use the the last received idx
    % as it may not have been persisted yet
    % also we can't use the last writted Idx as then
    % the follower may resent items that are currently waiting to
    % be written.
    {LWIdx, LWTerm} = ra_log:last_written(Log),
    {LastIdx, _} = last_idx_term(State),
    #append_entries_reply{term = Term, success = Success,
                          next_index = LastIdx + 1,
                          last_index = LWIdx,
                          last_term = LWTerm}.

evaluate_quorum(State0, Effects) ->
    % TODO: shortcut function if commit index was not incremented
    State = #{commit_index := CI} = increment_commit_index(State0),
    apply_to(CI, State, Effects).

increment_commit_index(State = #{current_term := CurrentTerm}) ->
    PotentialNewCommitIndex = agreed_commit(match_indexes(State)),
    % leaders can only increment their commit index if the corresponding
    % log entry term matches the current term. See (§5.4.2)
    case fetch_term(PotentialNewCommitIndex, State) of
        {CurrentTerm, Log} ->
            State#{commit_index => PotentialNewCommitIndex,
                   log => Log};
        {_, Log} ->
            State#{log => Log}
    end.


match_indexes(#{log := Log} = State) ->
    {LWIdx, _} = ra_log:last_written(Log),
    maps:fold(fun(_K, #{match_index := Idx}, Acc) ->
                      [Idx | Acc]
              end, [LWIdx], peers(State)).

-spec agreed_commit(list()) -> ra_index().
agreed_commit(Indexes) ->
    SortedIdxs = lists:sort(fun erlang:'>'/2, Indexes),
    Nth = trunc(length(SortedIdxs) / 2) + 1,
    lists:nth(Nth, SortedIdxs).

log_unhandled_msg(RaState, Msg, #{id := {_, _, LogId}}) ->
    ?WARN("~s: ~w received unhandled msg: ~W~n", [LogId, RaState, Msg, 6]).

fold_log_from(From, Folder, {St, Log0}) ->
    case ra_log:take(From, ?FOLD_LOG_BATCH_SIZE, Log0) of
        {[], Log} ->
            {ok, {St, Log}};
        {Entries, Log}  ->
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


index_machine_version(Idx, #{machine_versions := Versions}) ->
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
                               id := {Id, _, _}} = State) ->
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
                             id := {Id, _, _}} = State0) ->
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

update_query_index(#{cluster := Cluster, id := {Id, _, _}} = State, NewQueryIndex) ->
    Self = maps:get(Id, Cluster),
    State#{cluster => Cluster#{Id => Self#{query_index => NewQueryIndex}},
           query_index => NewQueryIndex}.

reset_query_index(#{cluster := Cluster} = State) ->
    State#{
        cluster =>
            maps:map(fun(_PeerId, Peer) -> Peer#{query_index => 0} end,
                     Cluster)
    }.


heartbeat_rpc_effects(Peers, Id, Term, QueryIndex) ->
    lists:filtermap(fun({PeerId, Peer}) ->
        heartbeat_rpc_effect_for_peer(PeerId, Peer, Id, Term, QueryIndex)
    end,
    maps:to_list(Peers)).

heartbeat_rpc_effect_for_peer(PeerId, Peer, Id, Term, QueryIndex) ->
    case maps:get(query_index, Peer, 0) < QueryIndex of
        true ->
            {true,
                {send_rpc, PeerId, #heartbeat_rpc{query_index = QueryIndex,
                                                  term = Term,
                                                  leader_id = Id}}};
        false ->
            false
    end.

heartbeat_rpc_quorum(NewQueryIndex, PeerId, #{queries_waiting_heartbeats := Waiting0} = State) ->
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
    case maps:get(PeerId, Cluster, none) of
        none -> State0;
        #{query_index := PeerQueryIndex} = Peer ->
            case QueryIndex > PeerQueryIndex of
                true  ->
                    update_peer(PeerId,
                                Peer#{query_index => QueryIndex},
                                State0);
                false ->
                    State0
            end
    end.

get_current_query_quorum(#{cluster := Cluster}) ->
    SortedQueryIndexes =
        lists:sort(
            fun erlang:'>'/2,
            lists:map(
                fun(#{query_index := PeerQueryIndex}) ->
                    PeerQueryIndex
                end,
                maps:values(Cluster))),

    lists:nth(maps:size(Cluster) div 2 + 1, SortedQueryIndexes).

-spec take_from_queue_while(fun((El) -> {true, Res} | false), queue:queue(El)) ->
    {[Res], queue:queue(El)}.
take_from_queue_while(Fun, Queue) ->
    take_from_queue_while(Fun, Queue, []).

take_from_queue_while(Fun, Queue, Result) ->
    case queue:peek(Queue) of
        {value, El} ->
            case Fun(El) of
                {true, ResVal} ->
                    take_from_queue_while(Fun, queue:drop(Queue), [ResVal | Result]);
                false ->
                    {Result, Queue}
            end;
        empty ->
            {Result, Queue}
    end.

-spec apply_consistent_queries_effects([consistent_query_ref()], ra_server_state()) ->
    ra_effects().
apply_consistent_queries_effects(QueryRefs, #{last_applied := ApplyIndex} = State) ->
    lists:map(fun({_, _, ReadCommitIndex} = QueryRef) ->
        true = ApplyIndex >= ReadCommitIndex,
        consistent_query_reply(QueryRef, State)
    end,
    QueryRefs).

-spec consistent_query_reply(consistent_query_ref(), ra_server_state()) -> ra_effect().
consistent_query_reply({From, QueryFun, _ReadCommitIndex},
                       #{id := {Id, _, _},
                         machine_state := MacState,
                         machine := {machine, MacMod, _}}) ->
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

%%% ===================
%%% Internal unit tests
%%% ===================

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

index_machine_version_test() ->
    S0 = #{machine_versions => [{0, 0}]},
    ?assertEqual(0, index_machine_version(0, S0)),
    ?assertEqual(0, index_machine_version(1123456, S0)),

    S1 = #{machine_versions => [{100, 4}, {50, 3}, {25, 2}]},
    ?assertEqual(4, index_machine_version(101, S1)),
    ?assertEqual(4, index_machine_version(100, S1)),
    ?assertEqual(3, index_machine_version(99, S1)),
    ?assertEqual(2, index_machine_version(49, S1)),
    ?assertEqual(2, index_machine_version(25, S1)),
    ?assertExit({machine_version_for_index_not_known, _},
                index_machine_version(24, S1)),
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
