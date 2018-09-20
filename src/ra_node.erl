-module(ra_node).

-include("ra.hrl").

-compile(inline_list_funcs).

-export([
         name/2,
         init/1,
         handle_leader/2,
         handle_candidate/2,
         handle_pre_vote/2,
         handle_follower/2,
         handle_await_condition/2,
         handle_aux/4,
         become/2,
         tick/1,
         overview/1,
         is_new/1,
         is_fully_persisted/1,
         is_fully_replicated/1,
         % properties
         id/1,
         uid/1,
         leader_id/1,
         machine/1,
         current_term/1,
         % TODO: hide behind a handle_leader
         make_rpcs/1,
         update_release_cursor/3,
         persist_last_applied/1,
         terminate/2,
         log_fold/3,
         recover/1
        ]).

-type ra_await_condition_fun() :: fun((ra_msg(), ra_node_state()) -> boolean()).

-type ra_node_state() ::
    #{id => ra_node_id(),
      uid => ra_uid(),
      leader_id => maybe(ra_node_id()),
      cluster => ra_cluster(),
      cluster_change_permitted => boolean(),
      cluster_index_term => ra_idxterm(),
      pending_cluster_changes => [term()],
      previous_cluster => {ra_index(), ra_term(), ra_cluster()},
      current_term => ra_term(),
      log => term(),
      voted_for => maybe(ra_node_id()), % persistent
      votes => non_neg_integer(),
      commit_index => ra_index(),
      last_applied => ra_index(),
      persisted_last_applied => ra_index(),
      stop_after => ra_index(),
      machine => ra_machine:machine(),
      machine_state => term(),
      aux_state => term(),
      condition => ra_await_condition_fun(),
      condition_timeout_effects => [ra_effect()],
      pre_vote_token => reference()
     }.

-type ra_state() :: leader | follower | candidate
                    | pre_vote | await_condition | delete_and_terminate | stop.

-type command_type() :: '$usr' | '$ra_query' | '$ra_join' | '$ra_leave' |
                        '$ra_cluster_change' | '$ra_cluster'.

-type command_meta() :: #{from := maybe(from())}.

-type command_correlation() :: non_neg_integer().

-type command_reply_mode() :: after_log_append |
                              await_consensus |
                              {notify_on_consensus, pid(),
                               command_correlation()} |
                              noreply.

-type command() :: {command_type(), command_meta(),
                    UserCommand :: term(), command_reply_mode()} | noop.

-type ra_msg() :: #append_entries_rpc{} |
                  {ra_node_id(), #append_entries_reply{}} |
                  {ra_node_id(), #install_snapshot_result{}} |
                  #request_vote_rpc{} |
                  #request_vote_result{} |
                  #pre_vote_rpc{} |
                  #pre_vote_result{} |
                  #install_snapshot_rpc{} |
                  election_timeout |
                  await_condition_timeout |
                  {command, command()} |
                  {commands, [command()]} |
                  ra_log:event().

-type ra_reply_body() :: #append_entries_reply{} |
                         #request_vote_result{} |
                         #pre_vote_result{}.

-type ra_effect() ::
    ra_machine:effect() |
    {reply, ra_reply_body()} |
    {reply, term(), ra_reply_body()} |
    {cast, ra_node_id(), term()} |
    {send_vote_requests, [{ra_node_id(),
                           #request_vote_rpc{} | #pre_vote_rpc{}}]} |
    {send_rpcs, [{ra_node_id(), #append_entries_rpc{}}]} |
    {next_event, ra_msg()} |
    {next_event, cast, ra_msg()} |
    {notify, pid(), reference()} |
    {incr_metrics, Table :: atom(),
     [{Pos :: non_neg_integer(), Incr :: integer()}]}.

-type ra_effects() :: [ra_effect()].

-type simple_apply_fun(State) :: fun((term(), State) -> State).

-type machine_conf() :: {simple, simple_apply_fun(term()), State :: term()}
                        | {module, module(), map()}.

-type ra_node_config() :: #{id := ra_node_id(),
                            uid := ra_uid(),
                            cluster_id := ra_cluster_id(),
                            log_init_args := ra_log:ra_log_init_args(),
                            initial_nodes := [ra_node_id()],
                            machine := machine_conf(),
                            % TODO: review - only really used for
                            % setting election timeouts
                            broadcast_time => non_neg_integer(), % ms
                            % for periodic actions such as sending stale rpcs
                            % and persisting last_applied index
                            tick_timeout => non_neg_integer(), % ms
                            await_condition_timeout => non_neg_integer()}.

-export_type([ra_node_state/0,
              ra_state/0,
              ra_node_config/0,
              ra_msg/0,
              machine_conf/0,
              command/0,
              command_type/0,
              command_meta/0,
              command_correlation/0,
              command_reply_mode/0
             ]).

-define(AER_CHUNK_SIZE, 25).
% TODO: test what is a good defult here
% TODO: make configurable
-define(MAX_PIPELINE_DISTANCE, 10000).

-spec name(ClusterId::string(), UniqueSuffix::string()) -> atom().
name(ClusterId, UniqueSuffix) ->
    list_to_atom("ra_" ++ ClusterId ++ "_node_" ++ UniqueSuffix).

-spec init(ra_node_config()) -> {ra_node_state(), ra_effects()}.
init(#{id := Id,
       uid := UId,
       cluster_id := _ClusterId,
       initial_nodes := InitialNodes,
       log_init_args := LogInitArgs,
       machine := MachineConf} = Config) ->
    Name = ra_lib:ra_node_id_to_local_name(Id),
    Log0 = ra_log:init(LogInitArgs),
    ok = ra_log:write_config(Config, Log0),
    CurrentTerm = ra_log:read_meta(current_term, Log0, 0),
    LastApplied = ra_log:read_meta(last_applied, Log0, 0),
    VotedFor = ra_log:read_meta(voted_for, Log0, undefined),
    Machine = case MachineConf of
                  {simple, Fun, S} ->
                      {machine, ra_machine_simple, #{simple_fun => Fun,
                                                     initial_state => S}};
                  {module, Mod, Args} ->
                      {machine, Mod, Args}
              end,
    {InitialMachineState, InitEffects} = ra_machine:init(Machine, Name),
    {FirstIndex, Cluster0, MacState, SnapshotIndexTerm} =
        case ra_log:read_snapshot(Log0) of
            undefined ->
                {0, make_cluster(Id, InitialNodes),
                 InitialMachineState, {0, 0}};
            {Idx, Term, ClusterNodes, MacSt} ->
                Clu = make_cluster(Id, ClusterNodes),
                %% the snapshot is the last index before the first index
                {Idx, Clu, MacSt, {Idx, Term}}
        end,

    CommitIndex = max(LastApplied, FirstIndex),

    State0 = #{id => Id,
               uid => UId,
               cluster => Cluster0,
               % There may be scenarios when a single node
               % starts up but hasn't
               % yet re-applied its noop command that we may receive other join
               % commands that can't be applied.
               % TODO: what if we have snapshotted and
               % there is no `noop` command
               % to be applied in the current term?
               cluster_change_permitted => false,
               cluster_index_term => {0, 0},
               pending_cluster_changes => [],
               current_term => CurrentTerm,
               voted_for => VotedFor,
               commit_index => CommitIndex,
               last_applied => FirstIndex,
               persisted_last_applied => LastApplied,
               log => Log0,
               machine => Machine,
               machine_state => MacState,
               aux_state => ra_machine:init_aux(Machine, Name),
               condition_timeout_effects => []},
    % Find last cluster change and idxterm and use as initial cluster
    % This is required as otherwise a node could restart without any known
    % peers and become a leader
    {ok, {{ClusterIndexTerm, Cluster}, Log}} =
    fold_log_from(CommitIndex,
                  fun({Idx, Term, {'$ra_cluster_change', _, Cluster, _}}, _) ->
                          {{Idx, Term}, Cluster};
                     (_, Acc) ->
                          Acc
                  end, {{SnapshotIndexTerm, Cluster0}, Log0}),
    % TODO: do we need to set previous cluster here?
    {State0#{log => Log,
             cluster => Cluster,
             cluster_index_term => ClusterIndexTerm}, InitEffects}.


recover(#{id := Id,
          commit_index := CommitIndex,
          last_applied := _LastApplied,
          log := Log0,
          machine := Machine} = State0) ->
    ?INFO("~w: recovering state machine from ~b to ~b~n",
          [Id, _LastApplied, CommitIndex]),
    {State, _, _} = apply_to(CommitIndex,
                             fun(E, S) ->
                                     %% Clear out the effects to avoid building
                                     %% up a long list of effects than then
                                     %% we throw away
                                     %% on node startup (queue recovery)
                                     setelement(4,
                                                apply_with(Machine, E, S), [])
                             end,
                             State0),
    % close and re-open log to ensure segments aren't unnecessarily kept
    % open
    Log = ra_log:release_resources(1, Log0),
    State#{log => Log}.

% the peer id in the append_entries_reply message is an artifact of
% the "fake" rpc call in ra_proxy as when using reply the unique reference
% is joined with the msg itself. In this instance it is treated as an info
% message.
-spec handle_leader(ra_msg(), ra_node_state()) ->
    {ra_state(), ra_node_state(), ra_effects()}.
handle_leader({PeerId, #append_entries_reply{term = Term, success = true,
                                             next_index = NextIdx,
                                             last_index = LastIdx}},
              State0 = #{current_term := Term, id := Id}) ->
    case peer(PeerId, State0) of
        undefined ->
            ?WARN("~w saw append_entries_reply from unknown peer ~w~n",
                  [Id, PeerId]),
            {leader, State0, []};
        Peer0 = #{match_index := MI, next_index := NI} ->
            Peer = Peer0#{match_index => max(MI, LastIdx),
                          next_index => max(NI, NextIdx)},
            State1 = update_peer(PeerId, Peer, State0),
            {State2, Effects0, Applied} = evaluate_quorum(State1),
            {State, Rpcs} = make_pipelined_rpcs(State2),
            % rpcs need to be issued _AFTER_ machine effects or there is
            % a chance that effects will never be issued if the leader crashes
            % after sending rpcs but before actioning the machine effects
            Effects = Effects0 ++ [{send_rpcs, Rpcs},
                                   {incr_metrics, ra_metrics, [{3, Applied}]}],
            case State of
                #{id := Id, cluster := #{Id := _}} ->
                    % leader is in the cluster
                    {leader, State, Effects};
                #{commit_index := CI, cluster_index_term := {CITIndex, _}}
                  when CI >= CITIndex ->
                    % leader is not in the cluster and the new cluster
                    % config has been committed
                    % time to say goodbye
                    ?INFO("~w leader not in new cluster - goodbye", [Id]),
                    {stop, State, Effects};
                _ ->
                    {leader, State, Effects}
            end
    end;
handle_leader({PeerId, #append_entries_reply{term = Term}},
              #{current_term := CurTerm,
                id := Id} = State0) when Term > CurTerm ->
    case peer(PeerId, State0) of
        undefined ->
            ?WARN("~w saw append_entries_reply from unknown peer ~w~n",
                  [Id, PeerId]),
            {leader, State0, []};
        _ ->
            ?INFO("~w leader saw append_entries_reply for term ~b "
                  "abdicates term: ~b!~n",
                  [Id, Term, CurTerm]),
            {follower, update_term(Term, State0), []}
    end;
handle_leader({PeerId, #append_entries_reply{success = false,
                                             next_index = NextIdx,
                                             last_index = LastIdx,
                                             last_term = LastTerm}},
              State0 = #{id := Id, cluster := Nodes, log := Log0}) ->
    #{PeerId := Peer0 = #{match_index := MI,
                          next_index := NI}} = Nodes,
    % if the last_index exists and has a matching term we can forward
    % match_index and update next_index directly
    {Peer, Log} = case ra_log:fetch_term(LastIdx, Log0) of
                      {undefined, L} ->
                          % entry was not found - simply set next index to
                          ?INFO("~w: setting next index for ~w ~b",
                                [Id, PeerId, NextIdx]),
                          {Peer0#{match_index => LastIdx,
                                  next_index => NextIdx}, L};
                      % entry exists we can forward
                      {LastTerm, L} when LastIdx >= MI ->
                          ?INFO("~w: setting last index to ~b, next_index ~b"
                                " for ~w", [Id, LastIdx, NextIdx, PeerId]),
                          {Peer0#{match_index => LastIdx,
                                  next_index => NextIdx}, L};
                      {_Term, L} when LastIdx < MI ->
                          % TODO: this can only really happen when peers are
                          % non-persistent.
                          % should they turn-into non-voters when this sitution
                          % is detected
                          ?ERR("~w: leader saw peer with last_index [~b in ~b]"
                               " lower than recorded match index [~b]."
                                "Resetting peer's state to last_index.~n",
                               [Id, LastIdx, LastTerm, MI]),
                          {Peer0#{match_index => LastIdx,
                                  next_index => LastIdx + 1}, L};
                      {_EntryTerm, L} ->
                          ?INFO("~w: leader received last_index ~b"
                                " from ~w with "
                                "term ~b different term ~b~n",
                                [Id, LastIdx, PeerId, LastTerm, _EntryTerm]),
                          % last_index has a different term or entry does not
                          % exist
                          % The peer must have received an entry from a previous
                          % leader and the current leader wrote a different
                          % entry at the same index in a different term.
                          % decrement next_index but don't go lower than
                          % match index.
                          {Peer0#{next_index => max(min(NI-1, LastIdx), MI)}, L}
                  end,
    State1 = State0#{cluster => Nodes#{PeerId => Peer}, log => Log},
    {State, Rpcs} = make_pipelined_rpcs(State1),
    {leader, State, [{send_rpcs, Rpcs}]};
handle_leader({command, Cmd}, State00 = #{id := Id}) ->
    case append_log_leader(Cmd, State00) of
        {not_appended, State = #{cluster_change_permitted := CCP}} ->
            ?WARN("~w command ~W NOT appended to log, "
                  "cluster_change_permitted ~w~n", [Id, Cmd, 5, CCP]),
            {leader, State, []};
        {ok, Idx, Term, State0} ->
            % ?INFO("~p ~p command appended to log at ~p term ~p~n",
            %      [Id, Cmd, Idx, Term]),
            % Only "pipeline" in response to a command
            {State, Rpcs} = make_pipelined_rpcs(State0),
            Effects1 = [{send_rpcs, Rpcs},
                        {incr_metrics, ra_metrics, [{2, 1}]}],
            % check if a reply is required.
            % TODO: refactor - can this be made a bit nicer/more explicit?
            Effects = case Cmd of
                          {_, _, _, await_consensus} ->
                              Effects1;
                          {_, #{from := undefined}, _, _} ->
                              Effects1;
                          {_, #{from := From}, _, _} ->
                              [{reply, From, {Idx, Term}} | Effects1];
                          _ ->
                              Effects1
                      end,
            {leader, State, Effects}
    end;
handle_leader({commands, Cmds}, State00 = #{id := _Id}) ->
    {State0, Effects0} =
        lists:foldl( fun(C, {S0, E}) ->
                             {ok, I, T, S} = append_log_leader(C, S0),
                             case C of
                                 {_, #{from := From}, _, after_log_append} ->
                                     {S, [{reply, From , {I, T}} | E]};
                                 _ ->
                                     {S, E}
                             end
                     end, {State00, []}, Cmds),

    {State, Rpcs} = make_pipelined_rpcs(length(Cmds), State0),
    %% TOOD: ra_metrics
    {leader, State, [{send_rpcs, Rpcs} | Effects0]};
handle_leader({ra_log_event, {written, _} = Evt}, State0 = #{log := Log0}) ->
    Log = ra_log:handle_event(Evt, Log0),
    {State1, Effects, Applied} = evaluate_quorum(State0#{log => Log}),
    {State, Rpcs} = make_pipelined_rpcs(State1),
    {leader, State, [{send_rpcs, Rpcs},
                     {incr_metrics, ra_metrics, [{3, Applied}]} | Effects]};
handle_leader({ra_log_event, Evt}, State = #{log := Log0}) ->
    % simply forward all other events to ra_log
    {leader, State#{log => ra_log:handle_event(Evt, Log0)}, []};
handle_leader({aux_command, Type, Cmd}, State0) ->
    handle_aux(leader, Type, Cmd, State0);
handle_leader({PeerId, #install_snapshot_result{term = Term}},
              #{id := Id, current_term := CurTerm} = State0)
  when Term > CurTerm ->
    case peer(PeerId, State0) of
        undefined ->
            ?WARN("~w: saw install_snapshot_result from unknown peer ~w~n",
                  [Id, PeerId]),
            {leader, State0, []};
        _ ->
            ?INFO("~w: leader saw install_snapshot_result for term ~b"
                  " abdicates term: ~b!~n", [Id, Term, CurTerm]),
            {follower, update_term(Term, State0), []}
    end;
handle_leader({PeerId, #install_snapshot_result{last_index = LastIndex}},
              #{id := Id} = State0) ->
    case peer(PeerId, State0) of
        undefined ->
            ?WARN("~w saw install_snapshot_result from unknown peer ~w~n",
                  [Id, PeerId]),
            {leader, State0, []};
        Peer0 = #{next_index := NI} ->
            State1 = update_peer(PeerId,
                                 Peer0#{match_index => LastIndex,
                                        commit_index => LastIndex,
                                        % leader might have pipelined
                                        % append entries
                                        % since snapshot was sent
                                        % need to ensure next index is at least
                                        % LastIndex + 1 though
                                        next_index => max(NI, LastIndex+1) },
                                 State0),

            {State, Rpcs} = make_pipelined_rpcs(State1),
            Effects = [{send_rpcs, Rpcs}],
            {leader, State, Effects}
    end;
handle_leader(#install_snapshot_rpc{term = Term,
                                    leader_id = Leader} = Evt,
              #{current_term := CurTerm,
                id := Id} = State0) when Term > CurTerm ->
    case peer(Leader, State0) of
        undefined ->
            ?WARN("~w saw install_snapshot_rpc from unknown leader ~w~n",
                  [Id, Leader]),
            {leader, State0, []};
        _ ->
            ?INFO("~w leader saw install_snapshot_rpc for term ~b "
                  "abdicates term: ~b!~n",
                  [Id, Term, CurTerm]),
            {follower, update_term(Term, State0), [{next_event, Evt}]}
    end;
handle_leader(#append_entries_rpc{term = Term} = Msg,
              #{current_term := CurTerm} = State0) when Term > CurTerm ->
    ?INFO("~w leader saw append_entries_rpc for term ~b abdicates term: ~b!~n",
         [id(State0), Term, CurTerm]),
    {follower, update_term(Term, State0), [{next_event, Msg}]};
handle_leader(#append_entries_rpc{term = Term}, #{current_term := Term,
                                                  id := Id}) ->
    ?ERR("~w leader saw append_entries_rpc for same term ~b"
         " this should not happen!~n", [Id, Term]),
    exit(leader_saw_append_entries_rpc_in_same_term);
% TODO: reply to append_entries_rpcs that have lower term?
handle_leader(#request_vote_rpc{term = Term, candidate_id = Cand} = Msg,
              #{current_term := CurTerm,
                id := Id} = State0) when Term > CurTerm ->
    case peer(Cand, State0) of
        undefined ->
            ?WARN("~w leader saw request_vote_rpc for unknown peer ~w~n",
                  [Id, Cand]),
            {leader, State0, []};
        _ ->
            ?INFO("~w leader saw request_vote_rpc for term ~b"
                  " abdicates term: ~b!~n", [Id, Term, CurTerm]),
            {follower, update_term(Term, State0), [{next_event, Msg}]}
    end;
handle_leader(#request_vote_rpc{}, State = #{current_term := Term}) ->
    Reply = #request_vote_result{term = Term, vote_granted = false},
    {leader, State, [{reply, Reply}]};
handle_leader(#pre_vote_rpc{term = Term, candidate_id = Cand} = Msg,
              #{current_term := CurTerm,
                id := Id} = State0) when Term > CurTerm ->
    case peer(Cand, State0) of
        undefined ->
            ?WARN("~w leader saw pre_vote_rpc for unknown peer ~w~n",
                  [Id, Cand]),
            {leader, State0, []};
        _ ->
            ?INFO("~w leader saw pre_vote_rpc for term ~b"
                  " abdicates term: ~b!~n", [Id, Term, CurTerm]),
            {follower, update_term(Term, State0), [{next_event, Msg}]}
    end;
handle_leader(#pre_vote_rpc{term = Term},
              #{current_term := CurTerm} = State0)
  when Term =< CurTerm ->
    % enforce leadership
    {State, Rpcs} = make_all_rpcs(State0),
    Effects = [{send_rpcs, Rpcs}],
    {leader, State, Effects};
handle_leader(Msg, State) ->
    log_unhandled_msg(leader, Msg, State),
    {leader, State, []}.


-spec handle_candidate(ra_msg() | election_timeout, ra_node_state()) ->
    {ra_state(), ra_node_state(), ra_effects()}.
handle_candidate(#request_vote_result{term = Term, vote_granted = true},
                 #{current_term := Term, votes := Votes,
                   cluster := Nodes, machine := Machine,
                   machine_state := MacState} = State0) ->
    NewVotes = Votes + 1,
    case trunc(maps:size(Nodes) / 2) + 1 of
        NewVotes ->
            {State, Rpcs} = make_all_rpcs(initialise_peers(State0)),
            Effects = ra_machine:leader_effects(Machine, MacState),
            {leader, maps:without([votes, leader_id], State),
             [{send_rpcs, Rpcs},
              {next_event, cast, {command, noop}} | Effects]};
        _ ->
            {candidate, State0#{votes => NewVotes}, []}
    end;
handle_candidate(#request_vote_result{term = Term},
                 #{current_term := CurTerm} = State0)
  when Term > CurTerm ->
    ?INFO("~w: candidate request_vote_result with higher term"
          " received ~b -> ~b", [id(State0), CurTerm, Term]),
    State = update_meta(Term, undefined, State0),
    {follower, State, []};
handle_candidate(#request_vote_result{vote_granted = false}, State) ->
    {candidate, State, []};
handle_candidate(#append_entries_rpc{term = Term} = Msg,
                 #{current_term := CurTerm} = State0) when Term >= CurTerm ->
    State = update_meta(Term, undefined, State0),
    {follower, State, [{next_event, Msg}]};
handle_candidate(#append_entries_rpc{leader_id = LeaderId},
                 #{current_term := CurTerm} = State) ->
    % term must be older return success=false
    Reply = append_entries_reply(CurTerm, false, State),
    {candidate, State, [{cast, LeaderId, {id(State), Reply}}]};
handle_candidate({_PeerId, #append_entries_reply{term = Term}},
                 #{current_term := CurTerm} = State0)
  when Term > CurTerm ->
    ?INFO("~w: candidate append_entries_reply with higher"
          " term received ~b -> ~b~n",
          [id(State0), CurTerm, Term]),
    State = update_meta(Term, undefined, State0),
    {follower, State, []};
handle_candidate(#request_vote_rpc{term = Term} = Msg,
                 #{current_term := CurTerm} = State0)
  when Term > CurTerm ->
    ?INFO("~w: candidate request_vote_rpc with higher term received ~b -> ~b~n",
          [id(State0), CurTerm, Term]),
    State = update_meta(Term, undefined, State0),
    {follower, State, [{next_event, Msg}]};
handle_candidate(#request_vote_rpc{}, State = #{current_term := Term}) ->
    Reply = #request_vote_result{term = Term, vote_granted = false},
    {candidate, State, [{reply, Reply}]};
handle_candidate(election_timeout, State) ->
    call_for_election(candidate, State);
handle_candidate(Msg, State) ->
    log_unhandled_msg(candidate, Msg, State),
    {candidate, State, []}.

-spec handle_pre_vote(ra_msg(), ra_node_state()) ->
    {ra_state(), ra_node_state(), ra_effects()}.
handle_pre_vote(#append_entries_rpc{term = Term} = Msg,
                #{current_term := CurTerm} = State0)
  when Term >= CurTerm ->
    State = update_term(Term, State0),
    % revert to follower state
    {follower, State#{votes => 0}, [{next_event, Msg}]};
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
handle_pre_vote(#pre_vote_rpc{} = PreVote, State) ->
    process_pre_vote(pre_vote, PreVote, State);
handle_pre_vote(election_timeout, State) ->
    call_for_election(pre_vote, State);
handle_pre_vote(Msg, State) ->
    log_unhandled_msg(pre_vote, Msg, State),
    {pre_vote, State, []}.


-spec handle_follower(ra_msg(), ra_node_state()) ->
    {ra_state(), ra_node_state(), ra_effects()}.
handle_follower(#append_entries_rpc{term = Term,
                                    leader_id = LeaderId,
                                    leader_commit = LeaderCommit,
                                    prev_log_index = PLIdx,
                                    prev_log_term = PLTerm,
                                    entries = Entries0},
                State000 = #{id := Id, log := Log0, current_term := CurTerm})
  when Term >= CurTerm ->
    State00 = update_term(Term, State000),
    case has_log_entry_or_snapshot(PLIdx, PLTerm, State00) of
        {entry_ok, State0} ->
            % filter entries already seen
            {Log1, Entries} = drop_existing({Log0, Entries0}),
            case Entries of
                [] ->
                    LastIdx = ra_log:last_index_term(Log1),
                    Log2 = case Entries0 of
                               [] when element(1, LastIdx) > PLIdx ->
                                   %% if no entries were sent we need to reset
                                   %% last index to match the leader
                                   ?INFO("ra: resetting last index to ~b~n",
                                         [PLIdx]),
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
                    evaluate_commit_index_follower(State1);
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
        {missing, State0} ->
            Reply = append_entries_reply(Term, false, State0),
            ?INFO("~w: follower did not have entry at ~b in ~b."
                  " Requesting from ~b~n",
                  [Id, PLIdx, PLTerm, Reply#append_entries_reply.next_index]),
            Effects = [cast_reply(Id, LeaderId, Reply)],
            {await_condition,
             State0#{leader_id => LeaderId,
                     condition => fun follower_catchup_cond/2,
                     % repeat reply effect on condition timeout
                     condition_timeout_effects => Effects}, Effects};
        {term_mismatch, OtherTerm, State0} ->
            CommitIndex = maps:get(commit_index, State0),
            ?INFO("~w: term mismatch - follower had entry at ~b with term ~b "
                  "but not with term ~b~n"
                  "Asking leader to resend from ~b~n",
                  [Id, PLIdx, OtherTerm, PLTerm, CommitIndex + 1]),
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
                    condition => fun follower_catchup_cond/2,
                    % repeat reply effect on condition timeout
                    condition_timeout_effects => Effects}, Effects}
    end;
handle_follower(#append_entries_rpc{term = _Term, leader_id = LeaderId},
                #{id := Id, current_term := CurTerm} = State) ->
    % the term is lower than current term
    Reply = append_entries_reply(CurTerm, false, State),
    ?INFO("~w: follower request_vote_rpc in ~b but current term ~b~n",
          [Id, _Term, CurTerm]),
    {follower, State, [cast_reply(Id, LeaderId, Reply)]};
handle_follower({ra_log_event, {written, _} = Evt},
                State00 = #{log := Log0}) ->
    State0 = State00#{log => ra_log:handle_event(Evt, Log0)},
    evaluate_commit_index_follower(State0);
handle_follower({ra_log_event, Evt}, State = #{log := Log0}) ->
    % simply forward all other events to ra_log
    {follower, State#{log => ra_log:handle_event(Evt, Log0)}, []};
handle_follower(#pre_vote_rpc{} = PreVote, State) ->
    process_pre_vote(follower, PreVote, State);
handle_follower(#request_vote_rpc{candidate_id = Cand, term = Term},
                #{current_term := Term, voted_for := VotedFor} = State)
  when VotedFor /= undefined andalso VotedFor /= Cand ->
    % already voted for another in this term
    ?INFO("~w: follower request_vote_rpc for ~w already voted for ~w in ~b",
          [id(State), Cand, VotedFor, Term]),
    Reply = #request_vote_result{term = Term, vote_granted = false},
    {follower, State, [{reply, Reply}]};
handle_follower(#request_vote_rpc{term = Term, candidate_id = Cand,
                                  last_log_index = LLIdx,
                                  last_log_term = LLTerm},
                #{current_term := CurTerm} = State0)
  when Term >= CurTerm ->
    State = update_term(Term, State0),
    LastIdxTerm = last_idx_term(State),
    case is_candidate_log_up_to_date(LLIdx, LLTerm, LastIdxTerm) of
        true ->
            ?INFO("~w: granting vote for ~w with last indexterm ~w"
                  "for term ~b previous term was ~b~n",
                  [id(State0), Cand, {LLIdx, LLTerm}, Term, CurTerm]),
            Reply = #request_vote_result{term = Term, vote_granted = true},
            {follower, State#{voted_for => Cand, current_term => Term},
             [{reply, Reply}]};
        false ->
            ?INFO("~w: declining vote for ~w for term ~b,"
                  " candidate last log index term was: ~w~n"
                  " last log entry idxterm seen was: ~w~n",
                  [id(State0), Cand, Term, {LLIdx, LLTerm}, {LastIdxTerm}]),
            Reply = #request_vote_result{term = Term, vote_granted = false},
            {follower, State#{current_term => Term}, [{reply, Reply}]}
    end;
handle_follower(#request_vote_rpc{term = Term, candidate_id = _Cand},
                State = #{current_term := CurTerm})
  when Term < CurTerm ->
    ?INFO("~w declining vote to ~w for term ~b, current term ~b~n",
          [id(State), _Cand, Term, CurTerm]),
    Reply = #request_vote_result{term = CurTerm, vote_granted = false},
    {follower, State, [{reply, Reply}]};
handle_follower({_PeerId, #append_entries_reply{term = Term}},
                State = #{current_term := CurTerm}) when Term > CurTerm ->
    {follower, update_term(Term, State), []};
handle_follower(#install_snapshot_rpc{term = Term,
                                      leader_id = LeaderId,
                                      last_index = LastIndex,
                                      last_term = LastTerm},
                State = #{id := Id, current_term := CurTerm})
  when Term < CurTerm ->
    ?INFO("~w: install_snapshot old term ~b in ~b~n",
          [Id, LastIndex, LastTerm]),
    % follower receives a snapshot from an old term
    Reply = #install_snapshot_result{term = CurTerm,
                                     last_term = LastTerm,
                                     last_index = LastIndex},
    {follower, State, [cast_reply(Id, LeaderId, Reply)]};
handle_follower(#install_snapshot_rpc{term = Term,
                                      leader_id = LeaderId,
                                      last_term = LastTerm,
                                      last_index = LastIndex,
                                      last_config = ClusterNodes,
                                      data = Data},
                State0 = #{id := Id, log := Log0,
                           current_term := CurTerm})
  when Term >= CurTerm ->
    ?INFO("~w: installing snapshot at index ~b in term ~b~n",
          [Id, LastIndex, LastTerm]),
    % follower receives a snapshot to be installed
    Log = ra_log:install_snapshot({LastIndex, LastTerm, ClusterNodes, Data}, Log0),
    State1 = State0#{log => Log,
                     current_term => Term,
                     commit_index => LastIndex,
                     last_applied => LastIndex,
                     cluster => make_cluster(Id, ClusterNodes),
                     machine_state => Data,
                     leader_id => LeaderId},
    State = persist_last_applied(State1),

    Reply = #install_snapshot_result{term = CurTerm,
                                     last_term = LastTerm,
                                     last_index = LastIndex},
    {follower, State, [cast_reply(Id, LeaderId, Reply)]};
handle_follower(election_timeout, State) ->
    call_for_election(pre_vote, State);
handle_follower(Msg, State) ->
    log_unhandled_msg(follower, Msg, State),
    {follower, State, []}.

-spec handle_await_condition(ra_msg(), ra_node_state()) ->
    {ra_state(), ra_node_state(), ra_effects()}.
handle_await_condition(#request_vote_rpc{} = Msg, State) ->
    {follower, State, [{next_event, cast, Msg}]};
handle_await_condition(election_timeout, State) ->
    call_for_election(pre_vote, State);
handle_await_condition(await_condition_timeout,
                       #{condition_timeout_effects := Effects} = State) ->
    {follower, State#{condition_timeout_effects => []}, Effects};
handle_await_condition(Msg, #{condition := Cond} = State) ->
    case Cond(Msg, State) of
        true ->
            {follower, State, [{next_event, cast, Msg}]};
        false ->
            % log_unhandled_msg(await_condition, Msg, State),
            {await_condition, State, []}
    end.

-spec tick(ra_node_state()) -> ra_effects().
tick(#{machine := Machine, machine_state := MacState}) ->
    Now = os:system_time(millisecond),
    ra_machine:tick(Machine, Now, MacState).


-spec become(ra_state(), ra_node_state()) -> ra_node_state().
become(leader, #{cluster := Cluster, log := Log0} = State) ->
    Log = ra_log:release_resources(maps:size(Cluster), Log0),
    State#{log => Log};
become(follower, #{log := Log0} = State) ->
    %% followers should only ever need a single segment open at any one
    %% time
    State#{log => ra_log:release_resources(1, Log0)};
become(_RaftState, State) ->
    State.


-spec overview(ra_node_state()) -> map().
overview(#{log := Log, machine := Machine,
           machine_state := MacState} = State) ->
    O = maps:with([uid, current_term, commit_index, last_applied,
                   cluster, leader_id, voted_for], State),
    LogOverview = ra_log:overview(Log),
    MacOverview = ra_machine:overview(Machine, MacState),
    O#{log => LogOverview,
       machine => MacOverview}.

-spec is_new(ra_node_state()) -> boolean().
is_new(#{log := Log}) ->
    ra_log:next_index(Log) =:= 1.

-spec is_fully_persisted(ra_node_state()) -> boolean().
is_fully_persisted(#{log := Log}) ->
    LastWritten = ra_log:last_written(Log),
    LastIdxTerm = ra_log:last_index_term(Log),
    LastWritten =:= LastIdxTerm.

-spec is_fully_replicated(ra_node_state()) -> boolean().
is_fully_replicated(#{commit_index := CI} = State) ->
    case maps:values(peers(State)) of
        [] -> true; % there is only one node
        Peers ->
            MinMI = lists:min([M || #{match_index := M} <- Peers]),
            MinMI >= CI
    end.

handle_aux(RaftState, Type, Cmd, #{aux_state := Aux0, log := Log0,
                                   machine := Machine,
                                   machine_state := MacState0} = State0) ->
    case ra_machine:handle_aux(Machine, RaftState, Type, Cmd, Aux0,
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

-spec id(ra_node_state()) -> ra_node_id().
id(#{id := Id}) -> Id.

-spec uid(ra_node_state()) -> ra_uid().
uid(#{uid := UId}) -> UId.

-spec machine(ra_node_state()) -> module().
machine(#{machine := Machine}) ->
    ra_machine:module(Machine).

-spec leader_id(ra_node_state()) -> maybe(ra_node_id()).
leader_id(State) ->
    maps:get(leader_id, State, undefined).

-spec current_term(ra_node_state()) -> maybe(ra_term()).
current_term(State) ->
    maps:get(current_term, State).
% Internal

follower_catchup_cond(#append_entries_rpc{term = Term,
                                          prev_log_index = PLIdx,
                                          prev_log_term = PLTerm},
                      State0 = #{current_term := CurTerm})
  when Term >= CurTerm ->
    case has_log_entry_or_snapshot(PLIdx, PLTerm, State0) of
        {entry_ok, _State} ->
            true;
        _ ->
            false
    end;
follower_catchup_cond(#install_snapshot_rpc{term = Term,
                                            last_index = PLIdx},
                      #{current_term := CurTerm,
                        log := Log})
  when Term >= CurTerm ->
    % term is ok - check if the snapshot index is greater than the last
    % index seen
    PLIdx >= ra_log:next_index(Log);
follower_catchup_cond(_Msg, _State) ->
    false.

wal_down_condition(_Msg, #{log := Log}) ->
    ra_log:can_write(Log).

evaluate_commit_index_follower(#{commit_index := CommitIndex,
                                 id := Id, leader_id := LeaderId,
                                 current_term := Term,
                                 log := Log} = State0)
  when LeaderId =/= undefined ->
    % as writes are async we can't use the index of the last available entry
    % in the log as they may not have been fully persisted yet
    % Take the smaller of the two values as commit index may be higher
    % than the last entry received
    {Idx, _} = ra_log:last_written(Log),
    EffectiveCommitIndex = min(Idx, CommitIndex),
    % neet catch termination throw
    case catch apply_to(EffectiveCommitIndex, State0) of
        {delete_and_terminate, State1, Effects} ->
            Reply = append_entries_reply(Term, true, State1),
            {delete_and_terminate, State1,
             [cast_reply(Id, LeaderId, Reply) |
              filter_follower_effects(Effects)]};
        {State, Effects0, Applied} ->
            % filter the effects that should be applied on a follower
            Effects = filter_follower_effects(Effects0),
            Reply = append_entries_reply(Term, true, State),
            {follower, State, [cast_reply(Id, LeaderId, Reply),
                               {incr_metrics, ra_metrics, [{3, Applied}]}
                               | Effects]}
    end;
evaluate_commit_index_follower(State) ->
    %% when no leader is known
    {follower, State, []}.

filter_follower_effects(Effects) ->
    lists:filter(fun ({release_cursor, _, _}) -> true;
                     ({incr_metrics, _, _}) -> true;
                     ({aux, _}) -> true;
                     (garbage_collection) -> true;
                     (_) -> false
                 end, Effects).

make_pipelined_rpcs(State) ->
    make_pipelined_rpcs(?AER_CHUNK_SIZE, State).

make_pipelined_rpcs(MaxBatchSize, #{commit_index := CommitIndex} = State0) ->
    maps:fold(fun(PeerId, Peer0 = #{next_index := Next}, {S0, Entries}) ->
                      {LastIdx, Entry, S} =
                          append_entries_or_snapshot(PeerId, Next,
                                                     MaxBatchSize, S0),
                      Peer = Peer0#{next_index => LastIdx+1,
                                    commit_index => CommitIndex},
                      {update_peer(PeerId, Peer, S), [Entry | Entries]}
              end, {State0, []}, pipelineable_peers(State0)).

make_rpcs(State) ->
    make_rpcs_for(stale_peers(State), State).

% makes empty append entries for peers that aren't pipelineable
make_all_rpcs(State) ->
    make_rpcs_for(peers(State), State).

make_rpcs_for(Peers, State) ->
    maps:fold(fun(PeerId, #{next_index := Next}, {S0, Entries}) ->
                      {_, Entry, S} =
                          append_entries_or_snapshot(PeerId, Next,
                                                     ?AER_CHUNK_SIZE,
                                                     S0),
                      {S, [Entry | Entries]}
              end, {State, []}, Peers).

append_entries_or_snapshot(PeerId, Next, MaxBatchSize,
                           #{id := Id, log := Log0,
                             current_term := Term} = State) ->
    PrevIdx = Next - 1,
    case ra_log:fetch_term(PrevIdx, Log0) of
        {PrevTerm, Log} when PrevTerm =/= undefined ->
            make_aer_chunk(PeerId, PrevIdx, PrevTerm, MaxBatchSize,
                           State#{log => Log});
        {undefined, Log} ->
            % The assumption here is that a missing entry means we need
            % to send a snapshot.
            case ra_log:snapshot_index_term(Log) of
                {PrevIdx, PrevTerm} ->
                    % Previous index is the same as snapshot index
                    make_aer_chunk(PeerId, PrevIdx, PrevTerm, MaxBatchSize,
                                   State#{log => Log});
                _ ->
                    {LastIndex, LastTerm, Config, MacState} =
                        ra_log:read_snapshot(Log),
                    {LastIndex,
                     {PeerId, #install_snapshot_rpc{term = Term,
                                                    leader_id = Id,
                                                    last_index = LastIndex,
                                                    last_term = LastTerm,
                                                    last_config = Config,
                                                    data = MacState}},
                     State#{log => Log}}
            end
    end.

make_aer_chunk(PeerId, PrevIdx, PrevTerm, Num,
               #{log := Log0, current_term := Term, id := Id,
                 commit_index := CommitIndex} = State) ->
    Next = PrevIdx + 1,
    %% TODO: refactor to avoid lists:last call later
    %% ra_log:take should be able to return the actual number of entries
    %% read at fixed cost
    {Entries, Log} = ra_log:take(Next, Num, Log0),
    LastIndex = case Entries of
                    [] -> PrevIdx;
                    _ ->
                        {LastIdx, _, _} = lists:last(Entries),
                        %% assertion
                        {Next, _, _} = hd(Entries),
                        LastIdx
                end,
    {LastIndex,
     {PeerId, #append_entries_rpc{entries = Entries,
                                  term = Term,
                                  leader_id = Id,
                                  prev_log_index = PrevIdx,
                                  prev_log_term = PrevTerm,
                                  leader_commit = CommitIndex}},
     State#{log => Log}}.

% stores the cluster config at an index such that we can later snapshot
% at this index.
-spec update_release_cursor(ra_index(), term(), ra_node_state()) ->
    ra_node_state().
update_release_cursor(Index, MacState,
                      State = #{log := Log0, cluster := Cluster}) ->
    % simply pass on release cursor index to log
    Log = ra_log:update_release_cursor(Index, Cluster, MacState, Log0),
    State#{log => Log}.

% Persist last_applied - as there is an inherent race we cannot
% always guarantee that side effects won't be re-issued when a
% follower that has seen an entry but not the commit_index
% takes over and this
% This is done on a schedule
-spec persist_last_applied(ra_node_state()) -> ra_node_state().
persist_last_applied(#{persisted_last_applied := L,
                       last_applied := L} = State) ->
    % do nothing
    State;
persist_last_applied(#{last_applied := L, log := Log0} = State) ->
    ok = ra_log:write_meta(last_applied, L, Log0, false),
    State#{persisted_last_applied => L}.


-spec terminate(ra_node_state(), Reason :: {shutdown, delete} | term()) -> ok.
terminate(#{log := Log} = _State, {shutdown, delete}) ->
    ?INFO("~w: terminating and deleting all data~n", [id(_State)]),
    catch ra_log:delete_everything(Log),
    ok;
terminate(State, _Reason) ->
    #{log := Log} = persist_last_applied(State),
    catch ra_log:close(Log),
    ok.

-spec log_fold(ra_node_state(), fun((term(), State) -> State), State) ->
    {ok, State, ra_node_state()} |
    {error, term(), ra_node_state()}.
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

%%%===================================================================
%%% Internal functions
%%%===================================================================

call_for_election(candidate, #{id := Id,
                               current_term := CurrentTerm} = State0) ->
    NewTerm = CurrentTerm + 1,
    ?INFO("~w: election called for in term ~b~n", [Id, NewTerm]),
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
    State = update_meta(NewTerm, Id, State0),
    {candidate, State#{leader_id => undefined, votes => 0},
     [{next_event, cast, VoteForSelf}, {send_vote_requests, Reqs}]};
call_for_election(pre_vote, #{id := Id,
                              current_term := Term} = State0) ->
    ?INFO("~w: pre_vote election called for in term ~b~n", [Id, Term]),
    Token = make_ref(),
    PeerIds = peer_ids(State0),
    {LastIdx, LastTerm} = last_idx_term(State0),
    Reqs = [{PeerId, #pre_vote_rpc{term = Term,
                                   token = Token,
                                   candidate_id = Id,
                                   last_log_index = LastIdx,
                                   last_log_term = LastTerm}}
            || PeerId <- PeerIds],
    % vote for self
    VoteForSelf = #pre_vote_result{term = Term, token = Token,
                                   vote_granted = true},
    State = update_meta(Term, Id, State0),
    {pre_vote, State#{leader_id => undefined, votes => 0,
                      pre_vote_token => Token},
     [{next_event, cast, VoteForSelf}, {send_vote_requests, Reqs}]}.

process_pre_vote(FsmState, #pre_vote_rpc{term = Term, candidate_id = Cand,
                                         version = Version,
                                         token = Token,
                                         last_log_index = LLIdx,
                                         last_log_term = LLTerm},
                 #{current_term := CurTerm}= State0)
  when Term >= CurTerm  ->
    State = update_term(Term, State0),
    LastIdxTerm = last_idx_term(State),
    case is_candidate_log_up_to_date(LLIdx, LLTerm, LastIdxTerm) of
        true when Version =< ?RA_PROTO_VERSION ->
            ?INFO("~w: granting pre-vote for ~w with last indexterm ~w"
                  "for term ~b previous term was ~b~n",
                  [id(State0), Cand, {LLIdx, LLTerm}, Term, CurTerm]),
            {FsmState, State#{voted_for => Cand},
             [{reply, pre_vote_result(Term, Token, true)}]};
        true ->
            ?INFO("~w: declining pre-vote for ~w for protocol version ~b~n",
                  [id(State0), Cand, Version]),
            {FsmState, State, [{reply, pre_vote_result(Term, Token, false)}]};
        false ->
            ?INFO("~w: declining pre-vote for ~w for term ~b,"
                  " candidate last log index term was: ~w~n"
                  "Last log entry idxterm seen was: ~w~n",
                  [id(State0), Cand, Term, {LLIdx, LLTerm}, LastIdxTerm]),
            {FsmState, State,
             [{reply, pre_vote_result(Term, Token, false)}]}
    end;
process_pre_vote(FsmState, #pre_vote_rpc{term = Term,
                                         token = Token,
                                         candidate_id = _Cand},
                #{current_term := CurTerm} = State)
  when Term < CurTerm ->
    ?INFO("~w declining pre-vote to ~w for term ~b, current term ~b~n",
          [id(State), _Cand, Term, CurTerm]),
    {FsmState, State,
     [{reply, pre_vote_result(CurTerm, Token, false)}]}.

pre_vote_result(Term, Token, Success) ->
    #pre_vote_result{term = Term,
                     token = Token,
                     vote_granted = Success}.

new_peer() ->
    #{next_index => 1,
      match_index => 0,
      commit_index => 0}.

new_peer_with(Map) ->
    maps:merge(new_peer(), Map).

peers(#{id := Id, cluster := Nodes}) ->
    maps:remove(Id, Nodes).

% returns the peers that should receive piplined entries
pipelineable_peers(#{commit_index := CommitIndex,
                     log := Log} = State) ->
    NextIdx = ra_log:next_index(Log),
    maps:filter(fun (_, #{next_index := NI,
                          match_index := MI}) when NI < NextIdx ->
                        % there are unsent items
                        NI - MI < ?MAX_PIPELINE_DISTANCE;
                    (_, #{commit_index := CI,
                          next_index := NI,
                          match_index := MI}) when CI < CommitIndex ->
                        % the commit index has been updated
                        NI - MI < ?MAX_PIPELINE_DISTANCE;
                    (_, _) ->
                        false
                end, peers(State)).

% peers that could need an update
stale_peers(#{commit_index := CommitIndex} = State) ->
    maps:filter(fun (_Id, #{next_index := NI,
                            match_index := MI}) when MI < NI - 1 ->
                        % there are unconfirmed items
                        true;
                    (_Id, #{commit_index := CI}) when CI < CommitIndex ->
                        % the commit index has been updated
                        true;
                    (_Id, _Peer) ->
                        false
                end, peers(State)).

peer_ids(State) ->
    maps:keys(peers(State)).

peer(PeerId, #{cluster := Nodes}) ->
    maps:get(PeerId, Nodes, undefined).

update_peer(PeerId, Peer, #{cluster := Nodes} = State) ->
    State#{cluster => Nodes#{PeerId => Peer}}.

update_meta(Term, VotedFor, #{log := Log} = State) ->
    ok = ra_log:write_meta(current_term, Term, Log, false),
    ok = ra_log:write_meta(voted_for, VotedFor, Log, true),
    State#{current_term => Term,
           voted_for => VotedFor}.

update_term(Term, State = #{current_term := CurTerm})
  when Term =/= undefined andalso Term > CurTerm ->
        update_meta(Term, undefined, State);
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

has_log_entry_or_snapshot(Idx, Term, #{log := Log0} = State) ->
    case ra_log:fetch_term(Idx, Log0) of
        {undefined, Log} ->
            case ra_log:snapshot_index_term(Log) of
                {Idx, Term} ->
                    {entry_ok, State#{log => Log}};
                {Idx, OtherTerm} ->
                    {term_mismatch, OtherTerm, State#{log => Log}};
                _ ->
                    {missing, State#{log => Log}}
            end;
        {Term, Log} ->
            {entry_ok, State#{log => Log}};
        {OtherTerm, Log} ->
            {term_mismatch, OtherTerm, State#{log => Log}}
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
            % current node is already in cluster - do nothing
            Cluster;
        Cluster ->
            % add current node to cluster
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

apply_to(ApplyTo, #{machine := Machine} = State) ->
    apply_to(ApplyTo, fun(E, S) -> apply_with(Machine, E, S) end, State).

apply_to(ApplyTo, ApplyFun, State) ->
    apply_to(ApplyTo, ApplyFun, 0, #{}, [], State).

apply_to(ApplyTo, ApplyFun, NumApplied, Notifys0, Effects0,
         #{last_applied := LastApplied,
           machine_state := MacState0} = State0)
  when ApplyTo > LastApplied ->
    From = LastApplied + 1,
    To = min(From + 1024, ApplyTo),
    case fetch_entries(From, To, State0) of
        {[], State} ->
            %% reverse list before consing the notifications to ensure
            %% notifications are processed first
            Effects = make_notify_effects(Notifys0, lists:reverse(Effects0)),
            {State, Effects, NumApplied};
        %% assert first item read is from
        {[{From, _, _} | _] = Entries, State1} ->
            {AppliedTo, State, MacState, Effects, Notifys} =
                lists:foldl(ApplyFun, {LastApplied, State1, MacState0,
                                       Effects0, Notifys0}, Entries),
            % {AppliedTo,_, _} = lists:last(Entries),
            % ?INFO("applied: ~p ~nAfter: ~p", [Entries, MacState]),
            apply_to(ApplyTo, ApplyFun, NumApplied + length(Entries),
                     Notifys, Effects, State#{last_applied => AppliedTo,
                                              machine_state => MacState})
    end;
apply_to(_, _, NumApplied, Notifys, Effects0, State) -> % ApplyTo
    %% reverse list before consing the notifications to ensure
    %% notifications are processed first
    Effects = make_notify_effects(Notifys, lists:reverse(Effects0)),
    {State, Effects, NumApplied}.

make_notify_effects(Nots, Effects) ->
    maps:fold(fun (Pid, Corrs, Acc) ->
                      [{notify, Pid, lists:reverse(Corrs)} | Acc]
              end, Effects, Nots).

apply_with(Machine,
           {Idx, Term, {'$usr', #{from := From} = Meta0, Cmd, ReplyType}},
           {_, State, MacSt, Effects0, Notifys0}) ->
    Meta = Meta0#{index => Idx, term => Term},
    case ra_machine:apply(Machine, Meta, Cmd, Effects0, MacSt) of
        {NextMacSt, Effects1} ->
            % apply returned no reply so use IdxTerm as reply value
            {Effects, Notifys} = add_reply(From, {Idx, Term}, ReplyType,
                                           Effects1, Notifys0),
            {Idx, State, NextMacSt, Effects, Notifys};
        {NextMacSt, Effects1, Reply} ->
            % apply returned a return value
            {Effects, Notifys} = add_reply(From, Reply, ReplyType,
                                           Effects1, Notifys0),
            {Idx, State, NextMacSt, Effects, Notifys}
    end;
apply_with({machine, MacMod, _}, % Machine
           {Idx, Term, {'$ra_query', #{from := From}, QueryFun, ReplyType}},
           {_, State, MacSt, Effects0, Notifys0}) ->
    Result = ra_machine:query(MacMod, QueryFun, MacSt),
    {Effects, Notifys} = add_reply(From, {{Idx, Term}, Result},
                                   ReplyType, Effects0, Notifys0),
    {Idx, State, MacSt, Effects, Notifys};
apply_with(_Machine,
           {Idx, Term, {'$ra_cluster_change', #{from := From}, _New,
                        ReplyType}},
           {_, State0, MacSt, Effects0, Notifys0}) ->
    ?INFO("~w: applying ra cluster change to ~w~n",
          [id(State0), maps:keys(_New)]),
    {Effects, Notifys} = add_reply(From, {Idx, Term}, ReplyType,
                                   Effects0, Notifys0),
    State = State0#{cluster_change_permitted => true},
    % add pending cluster change as next event
    {Effects1, State1} = add_next_cluster_change(Effects, State),
    {Idx, State1, MacSt, Effects1, Notifys};
apply_with(_, % Machine
           {Idx, Term, noop}, % Idx
           {_, State0 = #{current_term := Term}, MacSt, Effects, Notifys}) ->
    ?INFO("~w: enabling ra cluster changes in ~b~n", [id(State0), Term]),
    State = State0#{cluster_change_permitted => true},
    {Idx, State, MacSt, Effects, Notifys};
apply_with(Machine,
           {Idx, _, {'$ra_cluster', #{from := From}, delete, ReplyType}},
           {_, State0, MacSt, Effects0, Notifys0}) ->
    % cluster deletion
    {Effects1, Notifys} = add_reply(From, ok, ReplyType, Effects0, Notifys0),
    Effects = make_notify_effects(Notifys, Effects1),
    EOLEffects = ra_machine:eol_effects(Machine, MacSt),
    % non-local return to be caught by ra_node_proc
    % need to update the state before throw
    State = State0#{last_applied => Idx, machine_state => MacSt},
    throw({delete_and_terminate, State, EOLEffects ++ Effects});
apply_with(_, {Idx, _, _} = Cmd, Acc) ->
    % TODO: remove to make more strics, ideally we should not need a catch all
    ?WARN("~p: apply_with: unhandled command: ~W~n",
          [id(element(2, Acc)), Cmd, 10]),
    setelement(1, Acc, Idx).

add_next_cluster_change(Effects,
                        #{pending_cluster_changes := [C | Rest]} = State) ->
    {_, From , _, _} = C,
    {[{next_event, {call, From}, {command, C}} | Effects],
     State#{pending_cluster_changes => Rest}};
add_next_cluster_change(Effects, State) ->
    {Effects, State}.


add_reply(From, Reply, await_consensus, Effects, Notifys) ->
    {[{reply, From, {machine_reply, Reply}} | Effects], Notifys};
add_reply(undefined, _, {notify_on_consensus, Corr, Pid}, % _ IdxTerm
          Effects, Notifys0) ->
    % notify are casts and thus have to include their own pid()
    % reply with the supplied correlation so that the sending can do their
    % own bookkeeping
    Notifys = maps:update_with(Pid, fun (T) -> [Corr | T] end,
                               [Corr], Notifys0),
    {Effects, Notifys};
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

evaluate_quorum(State0) ->
    State = #{commit_index := CI} = increment_commit_index(State0),
    apply_to(CI, State).

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

log_unhandled_msg(RaState, Msg, #{id := Id}) ->
    ?WARN("~w ~w received unhandled msg: ~W~n", [Id, RaState, Msg, 6]).

fold_log_from(From, Folder, {St, Log0}) ->
    case ra_log:take(From, 5, Log0) of
        {[], Log} ->
            {ok, {St, Log}};
        {Entries, Log}  ->
            try
                St1 = lists:foldl(Folder, St, Entries),
                fold_log_from(From + 5, Folder, {St1, Log})
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

%%% ===================
%%% Internal unit tests
%%% ===================

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

agreed_commit_test() ->
    % one node
    4 = agreed_commit([4]),
    % 2 nodes - only leader has seen new commit
    3 = agreed_commit([4, 3]),
    % 2 nodes - all nodes have seen new commit
    4 = agreed_commit([4, 4, 4]),
    % 3 nodes - leader + 1 node has seen new commit
    4 = agreed_commit([4, 4, 3]),
    % only other nodes have seen new commit
    4 = agreed_commit([3, 4, 4]),
    % 3 nodes - only leader has seen new commit
    3 = agreed_commit([4, 2, 3]),
    ok.

-endif.
