-module(ra_node).

-include("ra.hrl").

-compile(inline_list_funcs).

-export([
         name/2,
         init/1,
         handle_leader/2,
         handle_candidate/2,
         handle_follower/2,
         handle_await_condition/2,
         tick/1,
         overview/1,
         is_new/1,
         % properties
         id/1,
         leader_id/1,
         machine/1,
         current_term/1,
         % TODO: hide behind a handle_leader
         make_rpcs/1,
         update_release_cursor/3,
         persist_last_applied/1,
         terminate/1
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
      condition => ra_await_condition_fun(),
      condition_timeout_effects => [ra_effect()]
     }.

-type ra_state() :: leader | follower | candidate.

-type ra_msg() :: #append_entries_rpc{} |
                  {ra_node_id(), #append_entries_reply{}} |
                  #request_vote_rpc{} |
                  #request_vote_result{} |
                  {command, term()}.

-type ra_effect() ::
    ra_machine:effect() |
    {reply, #append_entries_reply{} | #request_vote_result{}} |
    {cast, ra_node_id(), term()} |
    {send_vote_requests, [{ra_node_id(), #request_vote_rpc{}}]} |
    {send_rpcs, IsUrgent :: boolean(), [{ra_node_id(), #append_entries_rpc{}}]} |
    {next_event, ra_msg()} |
    {next_event, cast, ra_msg()} |
    {notify, pid(), reference()} |
    {incr_metrics, Table :: atom(),
     [{Pos :: non_neg_integer(), Incr :: integer()}]}.

-type ra_effects() :: [ra_effect()].

-type ra_node_config() :: #{id => ra_node_id(),
                            uid => ra_uid(),
                            log_module => ra_log_memory | ra_log_file,
                            log_init_args => ra_log:ra_log_init_args(),
                            initial_nodes => [ra_node_id()],
                            machine => ra_machine:machine(),
                            % TODO: review - only really used for
                            % setting election timeouts
                            broadcast_time => non_neg_integer(), % ms
                            % for periodic actions such as sending stale rpcs
                            % and persisting last_applied index
                            tick_timeout => non_neg_integer(), % ms
                            await_condition_timeout => non_neg_integer()}.

-export_type([ra_node_state/0,
              ra_node_config/0,
              ra_msg/0
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
       initial_nodes := InitialNodes,
       log_module := LogMod,
       log_init_args := LogInitArgs,
       machine := Machine} = Config) ->
    Name = ra_lib:ra_node_id_to_local_name(Id),
    Log0 = ra_log:init(LogMod, LogInitArgs),
    ok = ra_log:write_config(Config, Log0),
    CurrentTerm = ra_log:read_meta(current_term, Log0, 0),
    LastApplied = ra_log:read_meta(last_applied, Log0, 0),
    VotedFor = ra_log:read_meta(voted_for, Log0, undefined),
    {InitialMachineState, InitEffects} = ra_machine:init(Machine, Name),
    {FirstIndex, Cluster0, MacState, SnapshotIndexTerm} =
        case ra_log:read_snapshot(Log0) of
            undefined ->
                {0,
                 make_cluster(Id, InitialNodes), InitialMachineState, {0, 0}};
            {Idx, Term, Clu, MacSt} ->
                {Idx, Clu, MacSt, {Idx, Term}}
        end,

    CommitIndex = max(LastApplied, FirstIndex),

    State0 = #{id => Id,
               uid => UId,
               cluster => Cluster0,
               % TODO: there may be scenarios when a single node starts up but hasn't
               % yet re-applied its noop command that we may receive other join
               % commands that can't be applied.
               % TODO: what if we have snapshotted and there is no `noop` command
               % to be applied in the current term?
               cluster_change_permitted => false,
               cluster_index_term => {0, 0},
               pending_cluster_changes => [],
               current_term => CurrentTerm,
               voted_for => VotedFor,
               commit_index => CommitIndex,
               last_applied => FirstIndex - 1,
               persisted_last_applied => LastApplied,
               log => Log0,
               machine => Machine,
               machine_state => MacState,
               condition_timeout_effects => []},
    % Find last cluster change and idxterm and use as initial cluster
    % This is required as otherwise a node could restart without any known
    % peers and become a leader
    {{ClusterIndexTerm, Cluster}, Log1} =
    fold_log_from(CommitIndex,
                  fun({Idx, Term, {'$ra_cluster_change', _, Cluster, _}}, _Acc) ->
                          {{Idx, Term}, Cluster};
                     (_, Acc) ->
                          Acc
                  end, {{SnapshotIndexTerm, Cluster0}, Log0}),
    % TODO: do we need to set previous cluster here?
    % apply entries to the statemachine and
    % throw away the effects as they have already been issued
    {State, _, _} = apply_to(CommitIndex,
                             State0#{cluster => Cluster,
                                     cluster_index_term => ClusterIndexTerm,
                                     log => Log1}),

    % close and re-open log to ensure segments aren't unnecessarily kept
    % open
    ok = ra_log:close(maps:get(log, State)),
    Log = ra_log:init(LogMod, LogInitArgs),
    {State#{log => Log}, InitEffects}.


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
            ?WARN("~p saw command from unknown peer ~p~n", [Id, PeerId]),
            {leader, State0, []};
        Peer0 = #{match_index := MI, next_index := NI} ->
            % TODO: strictly speaking we should not need to take a max here?
            % assert?
            Peer = Peer0#{match_index => max(MI, LastIdx),
                          next_index => max(NI, NextIdx)},
            State1 = update_peer(PeerId, Peer, State0),
            {State2, Effects0, Applied} = evaluate_quorum(State1),
            {State, Rpcs} = make_pipelined_rpcs(State2),
            % TODO: rpcs need to be issued _AFTER_ machine effects or there is
            % a chance that effects will never be issued if the leader crashes
            % after sending rpcs but before actioning the machine effects
            Effects = Effects0 ++ [{send_rpcs, true, Rpcs},
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
            ?WARN("~w saw command from unknown peer ~w~n", [Id, PeerId]),
            {leader, State0, []};
        _ ->
            ?INFO("~w leader saw append_entries_reply for term ~b abdicates term: ~b!~n",
                 [Id, Term, CurTerm]),
            {follower, update_term(Term, State0), []}
    end;
handle_leader({PeerId, #append_entries_reply{success = false,
                                             next_index = NextIdx,
                                             last_index = LastIdx,
                                             last_term = LastTerm}} = _Reply ,
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
                          ?ERR("~p: leader saw peer return last_index [~b in ~b]"
                               " lower than recorded match index [~b]."
                                "Resetting peer's state to last_index.~n",
                               [Id, LastIdx, LastTerm, MI]),
                          {Peer0#{match_index => LastIdx,
                                  next_index => LastIdx + 1}, L};
                      {EntryTerm, L} ->
                          ?INFO("~w: leader received last_index from ~p with "
                                "different term ~p~n",
                                [Id, PeerId, EntryTerm]),
                          % last_index has a different term or entry does not
                          % exist
                          % The peer must have received an entry from a previous
                          % leader
                          % and the current leader wrote a different entry at the same
                          % index in a different term.
                          % decrement next_index but don't go lower than match index.
                          {Peer0#{next_index => max(min(NI-1, LastIdx), MI)}, L}
                  end,
    State1 = State0#{cluster => Nodes#{PeerId => Peer}, log => Log},
    {State, Rpcs} = make_pipelined_rpcs(State1),
    {leader, State, [{send_rpcs, true, Rpcs}]};
handle_leader({command, Cmd}, State00 = #{id := Id}) ->
    case append_log_leader(Cmd, State00) of
        {not_appended, State = #{cluster_change_permitted := CCP}} ->
            ?WARN("~w command ~W NOT appended to log, cluster_change_permitted ~w~n",
                 [Id, Cmd, 5, CCP]),
            {leader, State, []};
        {Status, Idx, Term, State0}  ->
            % ?INFO("~p ~p command appended to log at ~p term ~p~n",
            %      [Id, Cmd, Idx, Term]),
            {State1, Effects0} =
                case Status of
                    written ->
                        % fake written event
                        {State0,
                         [{next_event, {ra_log_event, {written, {Idx, Idx, Term}}}}]};
                    queued ->
                        {State0, []}
                end,
            % Only "pipeline" in response to a command
            % Observation: pipelining and "urgent" flag go together?
            {State, Rpcs} = make_pipelined_rpcs(State1),
            Effects1 = [{send_rpcs, true, Rpcs},
                        {incr_metrics, ra_metrics, [{2, 1}]}
                        | Effects0],
            % check if a reply is required.
            % TODO: refactor - can this be made a bit nicer/more explicit?
            Effects = case Cmd of
                          {_, _, _, await_consensus} ->
                              Effects1;
                          {_, undefined, _, _} ->
                              Effects1;
                          {_, From, _, _} ->
                              [{reply, From, {Idx, Term}} | Effects1];
                          _ ->
                              Effects1
                      end,
            {leader, State, Effects}
    end;
handle_leader({ra_log_event, {written, _} = Evt}, State0 = #{log := Log0}) ->
    Log = ra_log:handle_event(Evt, Log0),
    {State, Effects, Applied} = evaluate_quorum(State0#{log => Log}),
    % TODO: should we send rpcs in case commit_index was incremented?
    % {State, Rpcs} = make_pipelined_rpcs(State1),
    {leader, State, [{incr_metrics, ra_metrics, [{3, Applied}]} | Effects]};
handle_leader({ra_log_event, Evt}, State = #{log := Log0}) ->
    % simply forward all other events to ra_log
    {leader, State#{log => ra_log:handle_event(Evt, Log0)}, []};
handle_leader({PeerId, #install_snapshot_result{term = Term}},
              #{id := Id, current_term := CurTerm} = State0)
  when Term > CurTerm ->
    case peer(PeerId, State0) of
        undefined ->
            ?WARN("~w: saw command from unknown peer ~w~n", [Id, PeerId]),
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
                                        % leader might have pipelined
                                        % append entries
                                        % since snapshot was sent
                                        % need to ensure next index is at least
                                        % LastIndex + 1 though
                                        next_index => max(NI, LastIndex+1) },
                                 State0),

            {State, Rpcs} = make_pipelined_rpcs(State1),
            Effects = [{send_rpcs, true, Rpcs}],
            {leader, State, Effects}
    end;
handle_leader(#append_entries_rpc{term = Term} = Msg,
              #{current_term := CurTerm,
                id := Id} = State0) when Term > CurTerm ->
    ?INFO("~w leader saw append_entries_rpc for term ~b abdicates term: ~b!~n",
         [Id, Term, CurTerm]),
    {follower, update_term(Term, State0), [{next_event, Msg}]};
handle_leader(#append_entries_rpc{term = Term}, #{current_term := Term,
                                                  id := Id}) ->
    ?ERR("~p leader saw append_entries_rpc for same term ~p"
         " this should not happen: ~p!~n", [Id, Term]),
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
            {State, Rpcs} = make_all_rpcs(
                              initialise_peers(State0)),
            Effects = ra_machine:leader_effects(Machine, MacState),
            {leader, maps:without([votes, leader_id], State),
             [{send_rpcs, true, Rpcs},
              {next_event, cast, {command, noop}}
             | Effects]};
        _ ->
            {candidate, State0#{votes => NewVotes}, []}
    end;
handle_candidate(#request_vote_result{term = Term},
                 State0 = #{current_term := CurTerm, id := Id})
  when Term > CurTerm ->
    ?INFO("~p: candidate request_vote_result with higher term"
          " received ~p -> ~p", [Id, CurTerm, Term]),
    State = update_meta([{current_term, Term}, {voted_for, undefined}],
                        State0),
    {follower, State, []};
handle_candidate(#request_vote_result{vote_granted = false}, State) ->
    {candidate, State, []};
handle_candidate(#append_entries_rpc{term = Term} = Msg,
                 State0 = #{current_term := CurTerm}) when Term >= CurTerm ->
    State = update_meta([{current_term, Term}, {voted_for, undefined}],
                        State0),
    {follower, State, [{next_event, Msg}]};
handle_candidate(#append_entries_rpc{leader_id = LeaderId},
                 State = #{current_term := CurTerm}) ->
    % term must be older return success=false
    Reply = append_entries_reply(CurTerm, false, State),
    {candidate, State, [{cast, LeaderId, {id(State), Reply}}]};
handle_candidate({_PeerId, #append_entries_reply{term = Term}},
                 #{id := Id, current_term := CurTerm} = State0)
  when Term > CurTerm ->
    ?INFO("~w: candidate append_entries_reply with higher"
          " term received ~b -> ~b~n",
          [Id, CurTerm, Term]),
    State = update_meta([{current_term, Term}, {voted_for, undefined}], State0),
    {follower, State, []};
handle_candidate(#request_vote_rpc{term = Term} = Msg,
                 State0 = #{current_term := CurTerm, id := Id})
  when Term > CurTerm ->
    ?INFO("~w: candidate request_vote_rpc with higher term received ~b -> ~b~n",
          [Id, CurTerm, Term]),
    State = update_meta([{current_term, Term}, {voted_for, undefined}], State0),
    {follower, State, [{next_event, Msg}]};
handle_candidate(#request_vote_rpc{}, State = #{current_term := Term}) ->
    Reply = #request_vote_result{term = Term, vote_granted = false},
    {candidate, State, [{reply, Reply}]};
handle_candidate(election_timeout, State) ->
    handle_election_timeout(State);
handle_candidate(Msg, State) ->
    log_unhandled_msg(candidate, Msg, State),
    {candidate, State, []}.

-spec handle_follower(ra_msg(), ra_node_state()) ->
    {ra_state(), ra_node_state(), ra_effects()}.
handle_follower(#append_entries_rpc{term = Term, leader_id = LeaderId,
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
                    % update commit index to be the min of the last
                    % entry seen (but not necessarily written)
                    % and the leader commit
                    {Idx, _} = ra_log:last_index_term(Log1),
                    State1 = State0#{commit_index => min(Idx, LeaderCommit),
                                     leader_id => LeaderId},
                    % evaluate commit index as we may have received an updated
                    % commit index for previously written entries
                    {State, Effects} = evaluate_commit_index_follower(State1),
                    Reply = append_entries_reply(Term, true, State),
                    {follower, State, [{cast, LeaderId, {Id, Reply}} | Effects]};
                [{FirstIdx, _FirstTerm, _} | _] ->

                    {LastIdx, State1} = lists:foldl(fun pre_append_log_follower/2,
                                                    {FirstIdx, State0},
                                                    Entries),
                    % Increment only commit_index here as we are not applying anything
                    % at this point.
                    % last_applied will be incremented when the written event is
                    % processed
                    State = State1#{commit_index => min(LeaderCommit, LastIdx),
                                    leader_id => LeaderId},
                    % ?INFO("~p: follower received ~p append_entries in ~p.~nEffects ~p",
                    %      [Id, {PLIdx, PLTerm, length(Entries)}, Term, Effects]),
                    case ra_log:write(Entries, Log1) of
                        {written, Log} ->
                            % schedule a written next_event
                            % we can use last idx here as the log store
                            % is now fullly up to date.
                            FinalState = State#{log => Log},
                            {LIdx, LTerm} = last_idx_term(FinalState),
                            {follower, FinalState,
                             [{next_event, {ra_log_event,
                                            {written, {LIdx, LIdx, LTerm}}}}]};
                        {queued, Log} ->
                            {follower, State#{log => Log}, []};
                        {error, wal_down} ->
                            {await_condition,
                             State#{condition => fun wal_down_condition/2}, []};
                        {error, _} = Err ->
                            exit(Err)
                    end
            end;
        {missing, State0} ->
            ?INFO("~w: follower did not have entry at ~b in ~b~n",
                  [Id, PLIdx, PLTerm]),
            Reply = append_entries_reply(Term, false, State0),
            Effects = [cast_reply(Id, LeaderId, Reply)],
            {await_condition,
             State0#{leader_id => LeaderId,
                     condition => fun follower_catchup_cond/2,
                     % repeat reply effect on condition timeout
                     condition_timeout_effects => Effects}, Effects};
        {term_mismatch, State0} ->
            ?INFO("~w: term mismatch - follower had entry at ~b "
                  "but not with term ~b~n", [Id, PLIdx, PLTerm]),
            % This situation arises when a minority leader replicates entries
            % that it cannot commit then gets replaced by a majority leader
            % that also has made progress
            % As the follower is responsible for telling the leader
            % which their next expected entry is the best we can do here
            % is rewind back and use the commit index as the last index
            % and commit_index + 1 as the next expected.
            % This _may_ overwrite some valid entries but is probably the
            % simplest way to proceed
            CommitIndex = maps:get(commit_index, State0),
            {Reply, State} = mismatch_append_entries_reply(Term, CommitIndex,
                                                           State0),
            Effects = [cast_reply(Id, LeaderId, Reply)],
            {await_condition,
             State#{leader_id => LeaderId,
                    condition => fun follower_catchup_cond/2,
                    % repeat reply effect on condition timeout
                    condition_timeout_effects => Effects}, Effects}
    end;
handle_follower(#append_entries_rpc{term = Term, leader_id = LeaderId},
                State = #{id := Id, current_term := CurTerm}) ->
    % the term is lower than current term
    Reply = append_entries_reply(CurTerm, false, State),
    ?INFO("~w: follower request_vote_rpc in ~b but current term ~b~n",
         [Id, Term, CurTerm]),
    {follower, State, [cast_reply(Id, LeaderId, Reply)]};
handle_follower({ra_log_event, {written, _} = Evt},
                State00 = #{current_term := Term, id := Id,
                            log := Log0, leader_id := LeaderId}) ->

    State0 = State00#{log => ra_log:handle_event(Evt, Log0)},
    {State, Effects} = evaluate_commit_index_follower(State0),
    Reply = append_entries_reply(Term, true, State),
    {follower, State, [cast_reply(Id, LeaderId, Reply) | Effects]};
handle_follower({ra_log_event, Evt}, State = #{log := Log0}) ->
    % simply forward all other events to ra_log
    {follower, State#{log => ra_log:handle_event(Evt, Log0)}, []};
handle_follower(#request_vote_rpc{candidate_id = Cand, term = Term},
                State = #{id := Id, current_term := Term,
                          voted_for := VotedFor})
  when VotedFor /= undefined andalso VotedFor /= Cand ->
    % already voted for another in this term
    ?INFO("~w: follower request_vote_rpc for ~w already voted for ~b in ~b",
          [Id, Cand, VotedFor, Term]),
    Reply = #request_vote_result{term = Term, vote_granted = false},
    {follower, maps:without([leader_id], State), [{reply, Reply}]};
handle_follower(#request_vote_rpc{term = Term, candidate_id = Cand,
                                  last_log_index = LLIdx,
                                  last_log_term = LLTerm},
                State0 = #{current_term := CurTerm, id := Id})
  when Term >= CurTerm ->
    State = update_term(Term, State0),
    LastIdxTerm = last_idx_term(State),
    case is_candidate_log_up_to_date(LLIdx, LLTerm, LastIdxTerm) of
        true ->
            ?INFO("~w: granting vote for ~w with last indexterm ~w"
                  "for term ~b previous term was ~b~n",
                  [Id, Cand, {LLIdx, LLTerm}, Term, CurTerm]),
            Reply = #request_vote_result{term = Term, vote_granted = true},
            {follower, State#{voted_for => Cand, current_term => Term},
             [{reply, Reply}]};
        false ->
            ?INFO("~w: declining vote for ~w for term ~b,"
                  " candidate last log index term was: ~w~n"
                  " last log entry idxterm seen was: ~w~n",
                  [Id, Cand, Term, {LLIdx, LLTerm}, {LastIdxTerm}]),
            Reply = #request_vote_result{term = Term, vote_granted = false},
            {follower, State#{current_term => Term}, [{reply, Reply}]}
    end;
handle_follower(#request_vote_rpc{term = Term, candidate_id = Cand},
                State = #{current_term := CurTerm, id := Id})
  when Term < CurTerm ->
    ?INFO("~w declining vote to ~w for term ~b, current term ~b~n",
          [Id, Cand, Term, CurTerm]),
    Reply = #request_vote_result{term = CurTerm, vote_granted = false},
    {follower, State, [{reply, Reply}]};
handle_follower({_PeerId, #append_entries_reply{term = Term}},
                State = #{current_term := CurTerm}) when Term > CurTerm ->
    {follower, update_term(Term, State), []};
handle_follower(#install_snapshot_rpc{term = Term,
                                      leader_id = LeaderId,
                                      last_index = LastIndex,
                                      last_term = LastTerm},
                State = #{id := Id, current_term := CurTerm}) when Term < CurTerm ->
    ?INFO("~w: install_snapshot old term ~b in ~b~n", [Id, LastIndex, LastTerm]),
    % follower receives a snapshot from an old term
    Reply = #install_snapshot_result{term = CurTerm,
                                     last_term = LastTerm,
                                     last_index = LastIndex},
    {follower, State, [cast_reply(Id, LeaderId, Reply)]};
handle_follower(#install_snapshot_rpc{term = Term,
                                      leader_id = LeaderId,
                                      last_term = LastTerm,
                                      last_index = LastIndex,
                                      last_config = Cluster,
                                      data = Data},
                State0 = #{id := Id, log := Log0,
                           current_term := CurTerm}) when Term >= CurTerm ->
    ?INFO("~w: installing snapshot at index ~b in term ~b~n",
          [Id, LastIndex, LastTerm]),
    % follower receives a snapshot to be installed
    Log = ra_log:install_snapshot({LastIndex, LastTerm, Cluster, Data}, Log0),
    % TODO: should we also update metadata?
    State = State0#{log => Log,
                    current_term => Term,
                    commit_index => LastIndex,
                    last_applied => LastIndex,
                    cluster => Cluster,
                    machine_state => Data,
                    leader_id => LeaderId},

    % TODO: reply on snapshot written confirmation?
    Reply = #install_snapshot_result{term = CurTerm,
                                     last_term = LastTerm,
                                     last_index = LastIndex},
    {follower, State, [cast_reply(Id, LeaderId, Reply)]};
handle_follower(election_timeout, State) ->
    handle_election_timeout(State);
handle_follower(Msg, State) ->
    log_unhandled_msg(follower, Msg, State),
    {follower, State, []}.

-spec handle_await_condition(ra_msg(), ra_node_state()) ->
    {ra_state(), ra_node_state(), ra_effects()}.
handle_await_condition(#request_vote_rpc{} = Msg, State) ->
    {follower, State, [{next_event, cast, Msg}]};
handle_await_condition(election_timeout, State) ->
    handle_election_timeout(State);
handle_await_condition(await_condition_timeout,
                       #{condition_timeout_effects := Effects} = State) ->
    {follower, State#{condition_timeout_effects => []}, Effects};
handle_await_condition(Msg,#{condition := Cond} = State) ->
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

% property helpers

-spec id(ra_node_state()) -> ra_node_id().
id(#{id := Id}) -> Id.

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
        {_, _State} ->
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

evaluate_commit_index_follower(State0 = #{commit_index := CommitIndex,
                                          log := Log}) ->
    % as writes are async we can't use the index of the last available entry
    % in the log as they may not have been fully persisted yet
    % Take the smaller of the two values as commit index may be higher
    % than the last entry received
    {Idx, _} = ra_log:last_written(Log),
    EffectiveCommitIndex = min(Idx, CommitIndex),
    {State, Effects0, Applied} =
        apply_to(EffectiveCommitIndex, State0),
    % filter the effects that should be applied on a follower
    Effects1 = lists:filter(fun ({release_cursor, _, _}) -> true;
                                ({incr_metrics, _, _}) -> true;
                                (_) -> false
                            end, Effects0),
    Effects = [{incr_metrics, ra_metrics, [{3, Applied}]} | Effects1],
    {State, Effects}.

make_pipelined_rpcs(#{commit_index := CommitIndex} = State0) ->
    maps:fold(fun(PeerId, Peer0 = #{next_index := Next}, {S0, Entries}) ->
                      {LastIdx, Entry, S} =
                          append_entries_or_snapshot(PeerId, Next, S0),
                      Peer = Peer0#{next_index => LastIdx+1,
                                    commit_index => CommitIndex},
                      {update_peer(PeerId, Peer,S), [Entry | Entries]}
              end, {State0, []}, pipelineable_peers(State0)).

% makes empty append entries for peers that aren't pipelineable 
make_rpcs(State) ->
    maps:fold(fun(PeerId, #{next_index := Next}, {S0, Entries}) ->
                      {_, Entry, S} = append_entries_or_snapshot(PeerId, Next, S0),
                      {S, [Entry | Entries]}
              end, {State, []}, stale_peers(State)).

make_all_rpcs(State) ->
    maps:fold(fun(PeerId, #{next_index := Next}, {S0, Entries}) ->
                      {_, Entry, S} = append_entries_or_snapshot(PeerId, Next, S0),
                      {S, [Entry | Entries]}
              end, {State, []}, peers(State)).

append_entries_or_snapshot(PeerId, Next, #{id := Id, log := Log0,
                                           current_term := Term} = State) ->
    PrevIdx = Next - 1,
    case ra_log:fetch_term(PrevIdx, Log0) of
        {PrevTerm, Log} when PrevTerm =/= undefined ->
            make_aer_chunk(PeerId, PrevIdx, PrevTerm, ?AER_CHUNK_SIZE,
                           State#{log => Log});
        {undefined, Log} ->
            % The assumption here is that a missing entry means we need
            % to send a snapshot.
            case ra_log:snapshot_index_term(Log) of
                {PrevIdx, PrevTerm} ->
                    % Previous index is the same as snapshot index
                    make_aer_chunk(PeerId, PrevIdx, PrevTerm, ?AER_CHUNK_SIZE,
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
    {Entries, Log} = ra_log:take(Next, Num, Log0),
    LastIndex = case Entries of
                    [] -> PrevIdx;
                    _ ->
                        {LastIdx, _, _} = lists:last(Entries),
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

    % 1. CHK A
    % 2. ENQ (allocated to A[1])
    % SNAPSHOT: A[1] (no messages)
    %
    % 3. ENQ (allocated to A[1, 3])
    % 4. SET 2 (release cursor is 2) A[3]
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
    {ok, Log} = ra_log:write_meta(last_applied, L, Log0, false),
    State#{log => Log,
           persisted_last_applied => L}.


-spec terminate(ra_node_state()) -> ok.
terminate(State) ->
    #{log := Log} = persist_last_applied(State),
    catch ra_log:close(Log),
    ok.

%%%===================================================================
%%% Internal functions
%%%===================================================================

handle_election_timeout(State0 = #{id := Id, current_term := CurrentTerm}) ->
    ?INFO("~w: election timeout in term ~b~n", [Id, CurrentTerm]),
    PeerIds = peer_ids(State0),
    % increment current term
    NewTerm = CurrentTerm + 1,
    {LastIdx, LastTerm} = last_idx_term(State0),
    VoteRequests = [{PeerId, #request_vote_rpc{term = NewTerm,
                                               candidate_id = Id,
                                               last_log_index = LastIdx,
                                               last_log_term = LastTerm}}
                    || PeerId <- PeerIds],
    % vote for self
    VoteForSelf = #request_vote_result{term = NewTerm, vote_granted = true},
    State = update_meta([{current_term, NewTerm}, {voted_for, Id}], State0),
    {candidate, State#{leader_id => undefined,
                       votes => 0},
     [{next_event, cast, VoteForSelf},
      {send_vote_requests, VoteRequests}]}.

new_peer() ->
    #{next_index => 1,
      match_index => 0,
      commit_index => 0}.

% is_new_peer(#{next_index := 1,
%               match_index := 0,
%               commit_index := 0}) ->
%     true;
% is_new_peer(_Peer) ->
%     false.


new_peer_with(Map) ->
    maps:merge(new_peer(), Map).

peers(#{id := Id, cluster := Nodes}) ->
    maps:remove(Id, Nodes).

% returns the peers that should receive piplined entries
pipelineable_peers(#{commit_index := CommitIndex,
                     log := Log} = State) ->
    NextIdx  = ra_log:next_index(Log),
    maps:filter(fun (_Id, #{next_index := NI,
                            match_index := MI}) when NI < NextIdx ->
                        % there are unsent items
                        NI - MI < ?MAX_PIPELINE_DISTANCE;
                    (_Id, #{commit_index := CI,
                            next_index := NI,
                            match_index := MI}) when CI < CommitIndex ->
                        % the commit index has been updated
                        NI - MI < ?MAX_PIPELINE_DISTANCE;
                    (_Id, _) ->
                        false
                end, peers(State)).

% peers that could need an update
% TODO: introduce a last_seen timestamp to avoid sending additional message
% in high-load scenarios
stale_peers(State) ->
    maps:filter(fun (_Id, #{next_index := NI,
                            match_index := MI}) when MI < NI - 1 ->
                        % there are unconfirmed items
                        % TODO: now() - last_seen > ?RCP_INTERVAL_MS
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

update_meta(Updates, #{log := Log0} = State) ->
    {State1, Log} = lists:foldl(fun({K, V}, {State0, Acc0}) ->
                              {ok, Acc} = ra_log:write_meta(K, V, Acc0, false),
                              {maps:put(K, V, State0), Acc}
                      end, {State, Log0}, Updates),
    ok = ra_log:sync_meta(Log),
    State1#{log => Log}.

update_term(Term, State = #{current_term := CurTerm})
  when Term =/= undefined andalso Term > CurTerm ->
        update_meta([{current_term, Term},
                     {voted_for, undefined}], State);
update_term(_, State) ->
    State.

last_idx_term(#{log := Log}) ->
    case ra_log:last_index_term(Log) of
        {Idx, Term} ->
            {Idx, Term};
        undefined ->
            ra_log:snapshot_index_term(Log)
    end.

%% § 5.4.1 Raft determines which of two logs is more up-to-date by comparing
%% the index and term of the last entries in the logs. If the logs have last
%% entries with different terms, then the log with the later term is more
%% up-to-date. If the logs end with the same term, then whichever log is
%% longer is more up-to-dat
is_candidate_log_up_to_date(_Idx, Term, {_LastIdx, LastTerm})
  when Term > LastTerm ->
    true;
is_candidate_log_up_to_date(Idx, Term, {LastIdx, Term})
  when Idx >= LastIdx ->
    true;
is_candidate_log_up_to_date(_Idx, _Term, {_LastIdx, _LastTerm}) ->
    false.

has_log_entry_or_snapshot(Idx, Term, #{log := Log0} = State) ->
    case ra_log:fetch_term(Idx, Log0) of
        {undefined, Log} ->
            case ra_log:snapshot_index_term(Log) of
                {Idx, Term} ->
                    {entry_ok, State#{log => Log}};
                {Idx, _OtherTerm} ->
                    {term_mismatch, State#{log => Log}};
                _ ->
                    {missing, State#{log => Log}}
            end;
        {Term, Log} ->
            {entry_ok, State#{log => Log}};
        {_OtherTerm, Log} ->
            {term_mismatch, State#{log => Log}}
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


apply_to(ApplyTo, #{id := Id,
                    last_applied := LastApplied,
                    machine := Machine,
                    machine_state := MacState0} = State0)
  when ApplyTo > LastApplied ->
    % TODO: fetch and apply batches to reduce peak memory usage
    case fetch_entries(LastApplied + 1, ApplyTo, State0) of
        {[], State} ->
            {State, [], 0};
        {Entries, State1} ->
            {State, MacState, NewEffects} =
                lists:foldl(fun(E, St) -> apply_with(Id, Machine, E, St) end,
                            {State1, MacState0, []}, Entries),
            {AppliedTo, _LastEntryTerm, _} = lists:last(Entries),
            % ?INFO("~p: applied to: ~b in ~b", [Id,  LastEntryIdx, LastEntryTerm]),

            {State#{last_applied => AppliedTo,
                    machine_state => MacState}, NewEffects,
             AppliedTo - LastApplied}
    end;
apply_to(_ApplyTo, State) ->
    {State, [], 0}.


apply_with(_Id, Machine,
           {Idx, Term, {'$usr', From, Cmd, ReplyType}},
           {State, MacSt, Effects0}) ->
    case ra_machine:apply(Machine, Idx, Cmd, MacSt) of
        {NextMacSt, Efx} ->

            % apply returned no reply so use IdxTerm as reply value
            Effects = add_reply(From, {Idx, Term}, ReplyType, Effects0),
            {State, NextMacSt, Effects ++ Efx};
        {NextMacSt, Efx, Reply} ->
            % apply returned a return value
            Effects = add_reply(From, Reply, ReplyType, Effects0),
            {State, NextMacSt, Effects ++ Efx}
    end;
apply_with(_Id, _Machine,
           {Idx, Term, {'$ra_query', From, QueryFun, ReplyType}},
           {State, MacSt, Effects0}) ->
    Effects = add_reply(From, {{Idx, Term}, QueryFun(MacSt)},
                        ReplyType, Effects0),
    {State, MacSt, Effects};
apply_with(Id, _Machine,
           {Idx, Term, {'$ra_cluster_change', From, New, ReplyType}},
           {State0, MacSt, Effects0}) ->
    ?INFO("~w: applying ra cluster change to ~w~n", [Id, maps:keys(New)]),
    Effects = add_reply(From, {Idx, Term}, ReplyType, Effects0),
    State = State0#{cluster_change_permitted => true},
    % add pending cluster change as next event
    {Effects1, State1} = add_next_cluster_change(Effects, State),
    {State1, MacSt, Effects1};
apply_with(Id, _Machine,
           {_Idx, Term, noop},
           {State0 = #{current_term := Term}, MacSt, Effects}) ->
    ?INFO("~w: enabling ra cluster changes in ~b~n", [Id, Term]),
    State = State0#{cluster_change_permitted => true},
    {State, MacSt, Effects};
apply_with(Id, _Machine, Cmd, Acc) ->
    % TODO: uh why a catch all here? try to remove this and see if it breaks
    ?WARN("~p: apply_with: unhandled command: ~p~n", [Id, Cmd]),
    Acc.

add_next_cluster_change(Effects,
                        State = #{pending_cluster_changes := [C | Rest]}) ->
    {_, From , _, _} = C,
    {[{next_event, {call, From}, {command, C}} | Effects],
     State#{pending_cluster_changes => Rest}};
add_next_cluster_change(Effects, State) ->
    {Effects, State}.


add_reply(From, Reply, await_consensus, Effects) ->
    [{reply, From, Reply} | Effects];
add_reply(undefined, _IdxTerm, {notify_on_consensus, Corr, Pid}, Effects) ->
    % notify are casts and thus have to include their own pid()
    % reply with the supplied correlation so that the sending can do their
    % own bookkeeping
    Effects ++ [{notify, Pid, Corr}];
add_reply(_From, _Reply, _Mode, Effects) ->
    Effects.

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
            % TODO: reply?
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
    case ra_log:append({NextIdx, Term, Cmd}, Log0) of
        {queued, Log} ->
            {queued, NextIdx, Term, State#{log => Log}};
        {written, Log} ->
            {written, NextIdx, Term, State#{log => Log}}
    end.

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
            {PrevIdx, PrevTerm, PrevCluster} = maps:get(previous_cluster, State),
            State1 = State#{cluster => PrevCluster,
                            cluster_index_term => {PrevIdx, PrevTerm}},
            pre_append_log_follower(Entry, {Idx, State1})
    end;
pre_append_log_follower({Idx, Term, {'$ra_cluster_change', _, Cluster, _}},
                    {_, State}) ->
    {{Idx, Term}, State#{cluster => Cluster, cluster_index_term => {Idx, Term}}};
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
    % TODO: can we even do this async?
    Log = ra_log:append_sync({NextIdx, Term, Command}, Log0),
    {written, NextIdx, Term,
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
        {CurrentTerm, Log}  ->
            State#{commit_index => PotentialNewCommitIndex,
                   log => Log};
        _ ->
            State
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
    ?WARN("~w ~w received unhandled msg: ~W~n", [Id, RaState, Msg, 3]).

fold_log_from(From, Folder, {St, Log0}) ->
    case ra_log:take(From, 5, Log0) of
        {[], Log} ->
            {St, Log};
        {Entries, Log}  ->
            St1 = lists:foldl(Folder, St, Entries),
            fold_log_from(From + 5, Folder, {St1, Log})
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
