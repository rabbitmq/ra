-module(ra_node).

-include("ra.hrl").

-export([
         name/2,
         init/1,
         handle_leader/2,
         handle_candidate/2,
         handle_follower/2,
         make_rpcs/1,
         record_snapshot_point/2,
         maybe_snapshot/2,
         terminate/1
        ]).

-type ra_machine_effect() ::
    {send_msg, pid() | atom() | {atom(), atom()}, term()} |
    {monitor, process, pid()} |
    {demonitor, pid()} |
    % indicates that none of the preceeding entries contribute to the
    % current machine state
    {release_cursor, ra_index()} |
    % instruct ra to record a snapshot point at the current index
    {snapshot_point, ra_index()}.

-type ra_machine_command() :: {down, pid()} | term().

-type ra_machine_apply_fun_return() :: term() | {effects, term(), [ra_machine_effect()]}.
-type ra_machine_apply_fun() ::
        fun((ra_index(), Command :: ra_machine_command(), term()) -> ra_machine_apply_fun_return()) |
        fun((term(), term()) -> ra_machine_apply_fun_return()).

-type ra_node_state() ::
    #{id => ra_node_id(),
      leader_id => maybe(ra_node_id()),
      cluster_id => binary(),
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
      stop_after => ra_index(),
      % fun implementing ra machine
      machine_apply_fun => ra_machine_apply_fun(),
      machine_state => term(),
      initial_machine_state => term(),
      snapshot_index_term => ra_idxterm(),
      snapshot_points => #{ra_index() => {ra_term(), ra_cluster()}},
      sync_strategy => always | except_usr
      }.

-type ra_state() :: leader | follower | candidate.

-type ra_msg() :: #append_entries_rpc{} |
                  {ra_node_id(), #append_entries_reply{}} |
                  #request_vote_rpc{} |
                  #request_vote_result{} |
                  {command, term()} |
                  sync. % time to fsync

-type ra_effect() ::
    ra_machine_effect() |
    {reply, ra_msg()} |
    {send_vote_requests, [{ra_node_id(), #request_vote_rpc{}}]} |
    {send_rpcs, IsUrgent :: boolean(), [{ra_node_id(), #append_entries_rpc{}}]} |
    {next_event, ra_msg()} |
    {incr_metrics, Table :: atom(), [{Pos :: non_neg_integer(), Incr :: integer()}]} |
    schedule_sync.

-type ra_effects() :: [ra_effect()].

-type ra_node_config() :: #{id => ra_node_id(),
                            log_module => ra_log_memory | ra_log_file,
                            log_init_args => ra_log:ra_log_init_args(),
                            initial_nodes => [ra_node_id()],
                            apply_fun => ra_machine_apply_fun(),
                            init_fun => fun((atom()) -> term()),
                            cluster_id => atom()}.

-export_type([ra_node_state/0,
              ra_node_config/0,
              ra_machine_apply_fun/0,
              ra_msg/0,
              ra_effect/0,
              ra_effects/0]).

-spec name(ClusterId::string(), UniqueSuffix::string()) -> atom().
name(ClusterId, UniqueSuffix) ->
    list_to_atom("ra_" ++ ClusterId ++ "_node_" ++ UniqueSuffix).


% TODO: needs more tests around recovery / restart
-spec init(ra_node_config()) -> ra_node_state().
init(#{id := Id,
       initial_nodes := InitialNodes,
       cluster_id := ClusterId,
       log_module := LogMod,
       log_init_args := LogInitArgs,
       apply_fun := MachineApplyFun,
       init_fun := MachineInitFun}) ->
    Name = ra_lib:ra_node_id_to_local_name(Id),
    Log0 = ra_log:init(LogMod, LogInitArgs),
    CurrentTerm = ra_log:read_meta(current_term, Log0, 0),
    VotedFor = ra_log:read_meta(voted_for, Log0, undefined),
    {ok, Log} = ra_log:write_meta(current_term, CurrentTerm, Log0),
    InitialMachineState = MachineInitFun(Name),
    {CommitIndex, Cluster0, MacState, SnapshotIndexTerm} =
        case ra_log:read_snapshot(Log) of
            undefined ->
                {0, make_cluster(Id, InitialNodes), InitialMachineState, {0, 0}};
            {Idx, Term, Clu, MacSt} ->
                {Idx, Clu, MacSt, {Idx, Term}}
        end,

    State = #{id => Id,
              cluster_id => ClusterId,
              cluster => Cluster0,
              % TODO: there may be scenarios when a single node starts up but hasn't
              % yet re-applied it's noop command that we may receive other join
              % commands that can't be applied.
              cluster_change_permitted => false,
              cluster_index_term => {0, 0},
              pending_cluster_changes => [],
              current_term => CurrentTerm,
              voted_for => VotedFor,
              commit_index => CommitIndex,
              last_applied => CommitIndex,
              log => Log,
              machine_apply_fun => wrap_machine_fun(MachineApplyFun),
              machine_state => MacState,
              % for snapshots
              initial_machine_state => InitialMachineState,
              snapshot_index_term => SnapshotIndexTerm,
              snapshot_points => #{},
              sync_strategy => except_usr},
    % Find last cluster change and idxterm and use as initial cluster
    % This is required as otherwise a node could restart without any known
    % peers and become a leader
    {ClusterIndexTerm, Cluster} =
        fold_log_from(CommitIndex,
                      fun({Idx, Term, {'$ra_cluster_change', _, Cluster, _}}, _Acc) ->
                              {{Idx, Term}, Cluster};
                         (_, Acc) ->
                              Acc
                      end, {SnapshotIndexTerm, Cluster0}, Log),
    State#{cluster => Cluster,
           cluster_index_term => ClusterIndexTerm}.

% the peer id in the append_entries_reply message is an artifact of
% the "fake" rpc call in ra_proxy as when using reply the unique reference
% is joined with the msg itself. In this instance it is treated as an info
% message.
% TODO: we probably cannot rely on this behaviour. This may need to
% change when the peer proxy gets refactored.
-spec handle_leader(ra_msg(), ra_node_state()) ->
    {ra_state(), ra_node_state(), ra_effects()}.
handle_leader({PeerId, #append_entries_reply{term = Term, success = true,
                                             last_index = LastIdx}},
              State0 = #{current_term := Term, id := Id}) ->
    case peer(PeerId, State0) of
        undefined ->
            ?DBG("~p saw command from unknown peer ~p~n", [Id, PeerId]),
            {leader, State0, []};
        Peer0 = #{match_index := MI, next_index := NI} ->
            Peer = Peer0#{match_index => max(MI, LastIdx),
                          next_index => max(NI, LastIdx+1)},
            State1 = update_peer(PeerId, Peer, State0),
            {State, Effects0, Applied} = evaluate_quorum(State1),
            Rpcs = make_rpcs(State),
            Effects = [{send_rpcs, false, Rpcs},
                       {incr_metrics, ra_metrics, [{3, Applied}]} | Effects0],
            case State of
                #{id := Id, cluster := #{Id := _}} ->
                    % leader is in the cluster
                    {leader, State, Effects};
                #{commit_index := CI, cluster_index_term := {CITIndex, _}}
                  when CI >= CITIndex ->
                    % leader is not in the cluster and the new cluster
                    % config has been committed
                    % time to say goodbye
                    ?DBG("~p leader not in new cluster - goodbye", [Id]),
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
            ?DBG("~p saw command from unknown peer ~p~n", [Id, PeerId]),
            {leader, State0, []};
        _ ->
            ?DBG("~p leader saw append_entries_reply for term ~p abdicates term: ~p!~n",
                 [Id, Term, CurTerm]),
            {follower, update_term(Term, State0), []}
    end;
handle_leader({PeerId, #append_entries_reply{success = false,
                                             last_index = LastIdx,
                                             last_term = LastTerm}},
              State0 = #{id := Id, cluster := Nodes, log := Log}) ->
    #{PeerId := Peer0 = #{match_index := MI,
                          next_index := NI}} = Nodes,
    % if the last_index exists and has a matching term we can forward
    % match_index and update next_index directly
    Peer = case ra_log:fetch_term(LastIdx, Log) of
               LastTerm when LastIdx >= MI -> % entry exists we can forward
                   Peer0#{match_index => LastIdx,
                          next_index => LastIdx + 1};
               _Term when LastIdx < MI ->
                   % TODO: this can only really happen when peers are non-persistent.
                   % should they turn-into non-voters when this sitution is detected
                   error_logger:warning_msg(
                     "~p leader: peer returned last_index [~p in ~p] lower than recorded "
                     "match index [~p]. Resetting peers state to last_index.~n",
                     [Id, LastIdx, LastTerm, MI]),
                   Peer0#{match_index => LastIdx,
                          next_index => LastIdx + 1};
               EntryTerm ->
                   ?DBG("~p leader received last_index with different term ~p~n",
                        [Id, EntryTerm]),
                   % last_index has a different term
                   % The peer must have received an entry from a previous leader
                   % and the current leader wrote a different entry at the same
                   % index in a different term.
                   % decrement next_index but don't go lower than match index.
                   Peer0#{next_index => max(min(NI-1, LastIdx), MI)}
           end,
    State = State0#{cluster => Nodes#{PeerId => Peer}},
    Rpcs = make_rpcs(State),
    {leader, State, [{send_rpcs, false, Rpcs}]};
handle_leader({command, Cmd}, State00 = #{id := Id}) ->
    case append_log_leader(Cmd, State00) of
        {not_appended, State} ->
            ?DBG("~p command ~p NOT appended to log ~p~n", [Id, Cmd, State]),
            {leader, State, []};
        {Sync, {Idx, _} = IdxTerm, State0}  ->
            ?DBG("~p ~p command appended to log at ~p sync ~p~n",
                 [Id, first_or_atom(Cmd), IdxTerm, Sync]),
            {State1, Effects0, Applied} =
                case Sync of
                    sync ->
                        % we have synced - forward leader match_index
                        evaluate_quorum(update_match_index(Id, Idx, State0));
                    no_sync ->
                        % ask ra_node_proc to schedule a sync depending on
                        % sync_strategy
                        case State0 of
                            #{sync_strategy := always} ->
                                {State0, [schedule_sync], 0};
                            #{sync_strategy := except_usr} ->
                                evaluate_quorum(update_match_index(Id, Idx, State0))
                        end
                end,
            % Only "pipeline" in response to a command
            % Observation: pipelining and "urgent" flag go together?
            {State, Rpcs} = make_pipelined_rpcs(State1),
            Effects1 = [{send_rpcs, true, Rpcs},
                        {incr_metrics, ra_metrics, [{2, 1}, {3, Applied}]}
                        | Effects0],
            % check if a reply is required.
            % TODO: refactor - can this be made a bit nicer/more explicit?
            Effects = case Cmd of
                          {_, _, _, await_consensus} ->
                              Effects1;
                          {_, undefined, _, _} ->
                              Effects1;
                          {_, From, _, _} ->
                              [{reply, From, IdxTerm} | Effects1];
                          _ ->
                              Effects1
                      end,
            {leader, State, Effects}
    end;
handle_leader(sync, State0 = #{id := Id, log := Log}) ->
    {Idx, _} = ra_log:last_index_term(Log),
    ?DBG("~p: leader sync at ~b~n", [Id, Idx]),
    ok = ra_log:sync(Log),
    {State, Effects, Applied} =
        evaluate_quorum(update_match_index(Id, Idx, State0)),
    {leader, State, [{incr_metrics, ra_metrics, [{3, Applied}]} | Effects]};
handle_leader({PeerId, #install_snapshot_result{term = Term}},
              #{id := Id, current_term := CurTerm} = State0)
  when Term > CurTerm ->
    case peer(PeerId, State0) of
        undefined ->
            ?DBG("~p saw command from unknown peer ~p~n", [Id, PeerId]),
            {leader, State0, []};
        _ ->
            ?DBG("~p leader saw install_snapshot_result for term ~p abdicates term: ~p!~n",
                 [Id, Term, CurTerm]),
            {follower, update_term(Term, State0), []}
    end;
handle_leader({PeerId, #install_snapshot_result{last_index = LastIndex}},
              #{id := Id} = State0) ->
    case peer(PeerId, State0) of
        undefined ->
            ?DBG("~p saw install_snapshot_result from unknown peer ~p~n", [Id, PeerId]),
            {leader, State0, []};
        Peer0 ->
            State = update_peer(PeerId, Peer0#{match_index => LastIndex,
                                               next_index => LastIndex + 1},
                                State0),

            Rpcs = make_rpcs(State),
            Effects = [{send_rpcs, false, Rpcs}],
            {leader, State, Effects}
    end;
handle_leader(#append_entries_rpc{term = Term} = Msg,
              #{current_term := CurTerm,
                id := Id} = State0) when Term > CurTerm ->
    ?DBG("~p leader saw append_entries_rpc for term ~p abdicates term: ~p!~n",
         [Id, Term, CurTerm]),
    {follower, update_term(Term, State0), [{next_event, Msg}]};
handle_leader(#append_entries_rpc{term = Term}, #{current_term := Term,
                                                  id := Id}) ->
    ?DBG("~p leader saw append_entries_rpc for same term ~p this should not happen: ~p!~n",
         [Id, Term]),
         exit(leader_saw_append_entries_rpc_in_same_term);
% TODO: reply to append_entries_rpcs that have lower term?
handle_leader(#request_vote_rpc{term = Term, candidate_id = Cand} = Msg,
              #{current_term := CurTerm,
                id := Id} = State0) when Term > CurTerm ->
    case peer(Cand, State0) of
        undefined ->
            ?DBG("~p leader saw request_vote_rpc for unknown peer ~p~n",
                 [Id, Cand]),
            {leader, State0, []};
        _ ->
            ?DBG("~p leader saw request_vote_rpc for term ~p abdicates term: ~p!~n",
                 [Id, Term, CurTerm]),
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
                 State0 = #{current_term := Term, votes := Votes,
                            cluster := Nodes}) ->
    NewVotes = Votes+1,
    case trunc(maps:size(Nodes) / 2) + 1 of
        NewVotes ->
            State = initialise_peers(State0),
            {leader, maps:without([votes, leader_id], State),
             [{next_event, cast, {command, noop}}]};
        _ ->
            {candidate, State0#{votes => NewVotes}, []}
    end;
handle_candidate(#request_vote_result{term = Term},
                 State0 = #{current_term := CurTerm, id := Id}) when Term > CurTerm ->
    ?DBG("~p candidate request_vote_result with higher term recieved ~p -> ~p",
         [Id, CurTerm, Term]),
    State = update_meta([{current_term, Term}, {voted_for, undefined}], State0),
    {follower, State, []};
handle_candidate(#request_vote_result{vote_granted = false}, State) ->
    {candidate, State, []};
handle_candidate(#append_entries_rpc{term = Term} = Msg,
                 State0 = #{current_term := CurTerm}) when Term >= CurTerm ->
    State = update_meta([{current_term, Term}, {voted_for, undefined}], State0),
    {follower, State, [{next_event, Msg}]};
handle_candidate(#append_entries_rpc{},
                 State = #{current_term := CurTerm}) ->
    % term must be older return success=false
    Reply = append_entries_reply(CurTerm, false, State),
    {candidate, State, [{reply, Reply}]};
handle_candidate({_PeerId, #append_entries_reply{term = Term}},
                 State0 = #{current_term := CurTerm}) when Term > CurTerm ->
    State = update_meta([{current_term, Term}, {voted_for, undefined}], State0),
    {follower, State, []};
handle_candidate(#request_vote_rpc{term = Term} = Msg,
                 State0 = #{current_term := CurTerm, id := Id})
  when Term > CurTerm ->
    ?DBG("~p candidate request_vote_rpc with higher term recieved ~p -> ~p",
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
handle_follower(#append_entries_rpc{term = Term,
                                    leader_id = LeaderId,
                                    leader_commit = LeaderCommit,
                                    prev_log_index = PLIdx,
                                    prev_log_term = PLTerm,
                                    entries = Entries},
                State00 = #{id := Id,
                            current_term := CurTerm})
  when Term >= CurTerm ->
    State0 = update_term(Term, State00),
    case has_log_entry_or_snapshot(PLIdx, PLTerm, State0) of
        true ->
            % append_log_follower doesn't fsync each entry
            State1 = lists:foldl(fun append_log_follower/2,
                                 State0, Entries),
            % only sync if we have received entries
            ok = maybe_sync_log(Entries, State1),

            % ?DBG("~p: follower received ~p append_entries in ~p.",
            %      [Id, {PLIdx, PLTerm, length(Entries)}, Term]),

            % only apply log related effects on non-leader
            % monitor effects need to be done as well in case a follower is required
            % to take over as leader.
            {State, Effects0, Applied} =
                apply_to(LeaderCommit, State1#{leader_id => LeaderId}),
            Effects = lists:filter(fun ({release_cursor, _}) -> true;
                                       ({snapshot_point, _}) -> true;
                                       ({monitor, process, _}) -> true;
                                       ({demonitor, _}) -> true;
                                       ({incr_metrics, _, _}) -> true;
                                       (_) -> false
                                   end, Effects0),
            Reply = append_entries_reply(Term, true, State),
            {follower, State, [{reply, Reply},
                               {incr_metrics, ra_metrics, [{3, Applied}]} | Effects]};
        false ->
            ?DBG("~p: follower did not have entry at ~b in ~b~n",
                 [Id, PLIdx, PLTerm]),
            Reply = append_entries_reply(Term, false, State0),
            {follower, State0#{leader_id => LeaderId}, [{reply, Reply}]}
    end;
handle_follower(#append_entries_rpc{term = Term},
                State = #{id := Id, current_term := CurTerm}) ->
    Reply = append_entries_reply(CurTerm, false, State),
    ?DBG("~p: follower request_vote_rpc in ~b but current term ~b",
         [Id, Term, CurTerm]),
    {follower, State, [{reply, Reply}]};
handle_follower(#request_vote_rpc{candidate_id = Cand, term = Term},
                State = #{id := Id, current_term := Term,
                          voted_for := VotedFor})
  when VotedFor /= undefined andalso VotedFor /= Cand ->
    % already voted for another in this term
    ?DBG("~p: follower request_vote_rpc for ~p already voted for ~p in ~p",
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
            ?DBG("~p granting vote for ~p for term ~p previous term was ~p",
                 [Id, Cand, Term, CurTerm]),
            Reply = #request_vote_result{term = Term, vote_granted = true},
            {follower, State#{voted_for => Cand, current_term => Term},
             [{reply, Reply}]};
        false ->
            ?DBG("~p declining vote for ~p for term ~p, last log index ~p",
                 [Id, Cand, Term, LLIdx]),
            Reply = #request_vote_result{term = Term, vote_granted = false},
            {follower, State#{current_term => Term}, [{reply, Reply}]}
    end;
handle_follower(#request_vote_rpc{term = Term, candidate_id = Cand},
                State = #{current_term := CurTerm, id := Id})
  when Term < CurTerm ->
    ?DBG("~p declining vote to ~p for term ~p, current term ~p",
         [Id, Cand, Term, CurTerm]),
    Reply = #request_vote_result{term = CurTerm, vote_granted = false},
    {follower, State, [{reply, Reply}]};
handle_follower({_PeerId, #append_entries_reply{term = Term}},
                State = #{current_term := CurTerm}) when Term > CurTerm ->
    {follower, update_term(Term, State), []};
handle_follower(#install_snapshot_rpc{term = Term,
                                      last_index = LastIndex,
                                      last_term = LastTerm},
                State = #{current_term := CurTerm}) when Term < CurTerm ->
    % follower receives a snapshot from an old term
    Reply = #install_snapshot_result{term = CurTerm,
                                     last_term = LastTerm,
                                     last_index = LastIndex},
    {follower, State, [{reply, Reply}]};
handle_follower(#install_snapshot_rpc{term = Term,
                                      leader_id = LeaderId,
                                      last_term = LastTerm,
                                      last_index = LastIndex,
                                      last_config = Cluster,
                                      data = Data},
                State0 = #{id := Id, log := Log0,
                           current_term := CurTerm}) when Term >= CurTerm ->
    ?DBG("~p: installing snapshot at index ~p in ~p", [Id, LastIndex, LastTerm]),
    % follower receives a snapshot to be installed
    Log = ra_log:write_snapshot({LastIndex, LastTerm, Cluster, Data}, Log0),
    % TODO: should we also update metadata?
    State = State0#{log => Log,
                    current_term => Term,
                    commit_index => LastIndex,
                    last_applied => LastIndex,
                    cluster => Cluster,
                    machine_state => Data,
                    leader_id => LeaderId,
                    snapshot_index_term => {LastIndex, LastTerm}},

    Reply = #install_snapshot_result{term = CurTerm,
                                     last_term = LastTerm,
                                     last_index = LastIndex},
    {follower, State, [{reply, Reply}]};
handle_follower(election_timeout, State) ->
    handle_election_timeout(State);
handle_follower(Msg, State) ->
    log_unhandled_msg(follower, Msg, State),
    {follower, State, []}.


make_pipelined_rpcs(State) ->
    maps:fold(fun(PeerId, Peer = #{next_index := Next}, {S, Entries}) ->
                      {LastIdx, Entry} =
                          append_entries_or_snapshot(PeerId, Next, State),
                      {update_peer(PeerId, Peer#{next_index => LastIdx+1}, S),
                       [Entry | Entries]}
              end, {State, []}, peers(State)).

make_rpcs(State) ->
    maps:fold(fun(PeerId, #{next_index := Next}, Acc) ->
                      [element(2, append_entries_or_snapshot(PeerId, Next, State)) | Acc]
              end, [], peers(State)).

append_entries_or_snapshot(PeerId, Next,
                           #{id := Id, log := Log, current_term := Term,
                             commit_index := CommitIndex,
                             snapshot_index_term := {SIdx, STerm}}) ->
    PrevIdx = Next - 1,
    case ra_log:fetch_term(PrevIdx, Log) of
        PrevTerm when PrevTerm =/= undefined ->
            % The log backend implementation will be responsible for
            % keeping a cache of recently accessed entries.
            Entries = ra_log:take(Next, 5, Log),
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
                                          leader_commit = CommitIndex}}};
        undefined when PrevIdx =:= SIdx ->
            % Previous index is the same as snapshot index
            Entries = ra_log:take(Next, 5, Log),
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
                                          prev_log_index = SIdx,
                                          prev_log_term = STerm,
                                          leader_commit = CommitIndex}}};
        undefined  ->
            % TODO: The assumption here is that a missing entry means we need
            % to send a snapshot. This may not be the case in a sparse log and would
            % require changes if using incremental cleaning in combination with
            % snapshotting.
            {LastIndex, LastTerm, Config, MacState} = ra_log:read_snapshot(Log),
            %% TODO: if last index/term is same as Next-1 we should send
            %% append entries not snapshot
            {LastIndex, {PeerId, #install_snapshot_rpc{term = Term,
                                                       leader_id = Id,
                                                       last_index = LastIndex,
                                                       last_term = LastTerm,
                                                       last_config = Config,
                                                       data = MacState}}}

    end.

% stores the cluster config at an index such that we can later snapshot
% at this index.
record_snapshot_point(Index, State = #{id := Id,
                                       current_term := Term,
                                       cluster := Cluster,
                                       snapshot_points := Points0}) ->
    ?DBG("~p: recording snapshot point at index ~p~n", [Id, Index]),
    State#{snapshot_points => Points0#{Index => {Term, Cluster}}}.

% takes a snapshot if a snapshot point for the given index has been
% recorded.
maybe_snapshot(Index, State = #{id := Id,
                                log := Log0,
                                snapshot_points := Points0,
                                initial_machine_state := MachineState}) ->
    case Points0 of
        #{Index := {Term, Cluster}} ->
            ?DBG("~p: writing snapshot at index ~p~n", [Id, Index]),
            Snapshot = {Index, Term, Cluster, MachineState},
            Log = ra_log:write_snapshot(Snapshot, Log0),
            % TODO: remove all points below index
            Points = maps:without([Index], Points0),
            State#{log => Log,
                   snapshot_index_term => {Index, Term},
                   snapshot_points => Points};
        _ ->
            ?DBG("~p: snapshot point not found at index ~p~n", [Id, Index]),
            State
    end.

-spec terminate(ra_node_state()) -> ok.
terminate(#{log := Log}) ->
    catch ra_log:close(Log),
    ok.

%%%===================================================================
%%% Internal functions
%%%===================================================================

handle_election_timeout(State0 = #{id := Id, current_term := CurrentTerm}) ->
    ?DBG("~p election timeout in term ~p~n", [Id, CurrentTerm]),
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
    {candidate, State#{votes => 0}, [{next_event, cast, VoteForSelf},
                                     {send_vote_requests, VoteRequests}]}.

peers(#{id := Id, cluster := Nodes}) ->
    maps:remove(Id, Nodes).

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
  when Term > CurTerm ->
        update_meta([{current_term, Term}], State);
update_term(_, State) ->
    State.

last_idx_term(#{log := Log} = State) ->
    case ra_log:last_index_term(Log) of
        {Idx, Term} ->
            {Idx, Term};
        undefined ->
            maps:get(snapshot_index_term, State)
    end.

is_candidate_log_up_to_date(_Idx, Term, {_LastIdx, LastTerm})
  when Term > LastTerm ->
    true;
is_candidate_log_up_to_date(Idx, Term, {LastIdx, Term})
  when Idx >= LastIdx ->
    true;
is_candidate_log_up_to_date(_Idx, _Term, {_LastIdx, _LastTerm}) ->
    false.

has_log_entry_or_snapshot(Idx, Term,
                          #{log := Log,
                            snapshot_index_term := {SIdx, STerm}}) ->
    case ra_log:fetch_term(Idx, Log) of
        Term -> true;
        _ ->
            SIdx =:= Idx andalso STerm =:= Term
    end.

fetch_term(Idx, #{log := Log}) ->
    ra_log:fetch_term(Idx, Log).

fetch_entries(From, To, #{log := Log}) ->
    ra_log:take(From, To - From + 1, Log).

make_cluster(Self, Nodes) ->
    case lists:foldl(fun(N, Acc) ->
                                  Acc#{N => #{match_index => 0}}
                          end, #{}, Nodes) of
        #{Self := _} = Cluster ->
            % current node is already in cluster - do nothing
            Cluster;
        Cluster ->
            % add current node to cluster
            Cluster#{Self => #{match_index => 0}}
    end.

initialise_peers(State = #{log := Log, cluster := Cluster0}) ->
    PeerIds = peer_ids(State),
    NextIdx = ra_log:next_index(Log),
    Cluster = lists:foldl(fun(PeerId, Acc) ->
                                  Acc#{PeerId => #{match_index => 0,
                                                   next_index => NextIdx}}
                          end, Cluster0, PeerIds),
    State#{cluster => Cluster}.


apply_to(Commit, State0 = #{id := _Id,
                            last_applied := LastApplied,
                            machine_apply_fun := ApplyFun0,
                            machine_state := MacState0})
  when Commit > LastApplied ->
    case fetch_entries(LastApplied + 1, Commit, State0) of
        [] ->
            {State0, [], 0};
        Entries ->
            {State, MacState, NewEffects} =
                lists:foldl(fun(E, St) -> apply_with(ApplyFun0, E, St) end,
                            {State0, MacState0, []}, Entries),
            {LastEntryIdx, _LastEntryTerm, _} = lists:last(Entries),
            NewCommit = min(Commit, LastEntryIdx),
            % ?DBG("~p: applied to: ~b in ~b", [Id,  LastEntryIdx, LastEntryTerm]),
            {State#{last_applied => NewCommit,
                    commit_index => NewCommit,
                    machine_state => MacState}, NewEffects, Commit - LastApplied}
    end;
apply_to(_Commit, State) ->
    {State, [], 0}.

apply_with(ApplyFun, {Idx, Term, {'$usr', From, Cmd, ReplyType}},
        {State, MacSt, Effects0}) ->
            case ApplyFun(Idx, Cmd, MacSt) of
                {effects, NextMacSt, Efx} ->
                    Effects = add_reply(From, {Idx, Term}, ReplyType, Effects0),
                    {State, NextMacSt, Effects ++ Efx};
                NextMacSt ->
                    Effects = add_reply(From, {Idx, Term}, ReplyType, Effects0),
                    {State, NextMacSt, Effects}
            end;
apply_with(_ApplyFun, {Idx, Term, {'$ra_query', From, QueryFun, ReplyType}},
        {State, MacSt, Effects0}) ->
            Effects = add_reply(From, {{Idx, Term}, QueryFun(MacSt)},
                                ReplyType, Effects0),
            {State, MacSt, Effects};
apply_with(_ApplyFun, {Idx, Term, {'$ra_cluster_change', From, New, ReplyType}},
         {State0, MacSt, Effects0}) ->
            ?DBG("applying ra cluster change to ~p~n", [New]),
            Effects = add_reply(From, {Idx, Term}, ReplyType, Effects0),
            State = State0#{cluster_change_permitted => true},
            % add pending cluster change as next event
            {Effects1, State1} = add_next_cluster_change(Effects, State),
            {State1, MacSt, Effects1};
apply_with(_ApplyFun, {_Idx, Term, noop}, {State0 = #{current_term := Term}, MacSt, Effects}) ->
            State = State0#{cluster_change_permitted => true},
            {State, MacSt, Effects};
apply_with(_ApplyFun, _, Acc) ->
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
add_reply({FromPid, _}, Reply, notify_on_consensus, Effects) ->
    [{notify, FromPid, Reply} | Effects];
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
            {not_appended, State};
        _ ->
            Cluster = OldCluster#{JoiningNode => #{next_index => 1,
                                                   match_index => 0}},
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
    {ok, Log} = ra_log:append({NextIdx, Term, Cmd}, no_overwrite, no_sync, Log0),
    {no_sync, {NextIdx, Term}, State#{log => Log}}.

append_log_follower({Idx, Term, Cmd} = Entry,
                    State = #{log := Log0,
                              cluster_index_term := {Idx, CITTerm}})
  when Term /= CITTerm ->
    % the index for the cluster config entry has a different term, i.e.
    % it has been overwritten by a new leader. Unless it is another cluster
    % change (can this even happen?) we should revert back to the last known
    % cluster
    case Cmd of
        {'$ra_cluster_change', _, Cluster, _} ->
            {ok, Log} = ra_log:append(Entry, overwrite, sync, Log0),
            State#{log => Log, cluster => Cluster,
                   cluster_index_term => {Idx, Term}};
        _ ->
            % revert back to previous cluster
            {PrevIdx, PrevTerm, PrevCluster} = maps:get(previous_cluster, State),
            State1 = State#{cluster => PrevCluster,
                            cluster_index_term => {PrevIdx, PrevTerm}},
            append_log_follower(Entry, State1)
    end;
append_log_follower({Idx, Term, {'$ra_cluster_change', _, Cluster, _}} = Entry,
                    State = #{log := Log0}) ->
    % TODO: for the always sync_strategy we may want to delay sync until after
    % all entries have been appended.
    {ok, Log} = ra_log:append(Entry, overwrite, sync, Log0),
    State#{log => Log, cluster => Cluster, cluster_index_term => {Idx, Term}};
append_log_follower(Entry, State = #{log := Log0}) ->
    {ok, Log} = ra_log:append(Entry, overwrite, no_sync, Log0),
    State#{log => Log}.

maybe_sync_log([], _State) ->
    ok;
maybe_sync_log(_Entries, #{sync_strategy := except_usr}) ->
    ok;
maybe_sync_log(_Entries, #{log := Log0, sync_strategy := always}) ->
    ok = ra_log:sync(Log0),
    ok.

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
    {ok, Log} = ra_log:append({NextIdx, Term, Command}, no_overwrite, Log0),
    {sync, IdxTerm,
     State#{log => Log,
            cluster => Cluster,
            cluster_change_permitted => false,
            cluster_index_term => IdxTerm,
            previous_cluster => {PrevCITIdx, PrevCITTerm, PrevCluster}}}.

append_entries_reply(Term, Success, State) ->
    %% TODO: use snapshot index/term if not found
    {LastIdx, LastTerm} = last_idx_term(State),
    #append_entries_reply{term = Term, success = Success,
                          last_index = LastIdx,
                          last_term = LastTerm}.

evaluate_quorum(State) ->
    NewCommitIndex = increment_commit_index(State),
    apply_to(NewCommitIndex, State).

increment_commit_index(State = #{current_term := CurrentTerm,
                                 commit_index := CommitIndex,
                                 cluster := Nodes}) ->
    PotentialNewCommitIndex = agreed_commit(Nodes),
    % leaders can only increment their commit index if the corresponding
    % log entry term matches the current term. See (§5.4.2)
    case fetch_term(PotentialNewCommitIndex, State) of
        CurrentTerm -> PotentialNewCommitIndex;
        _ -> CommitIndex
    end.

-spec agreed_commit(map()) -> ra_index().
agreed_commit(Nodes) ->
    Idxs = maps:fold(fun(_K, #{match_index := Idx}, Acc) ->
                             [Idx | Acc]
                     end, [], Nodes),
    SortedIdxs = lists:sort(fun erlang:'>'/2, Idxs),
    Nth = trunc(length(Idxs) / 2) + 1,
    lists:nth(Nth, SortedIdxs).

log_unhandled_msg(RaState, Msg, #{id := Id}) ->
    ?DBG("~p ~p received unhandled msg: ~p~n", [Id, RaState, Msg]).

first_or_atom(T) when is_tuple(T) ->
    element(1, T);
first_or_atom(A) when is_atom(A) ->
    A;
first_or_atom(X) ->
    X.

update_match_index(Id, Idx, State = #{cluster := Cluster}) ->
    case Cluster of
        #{Id := Node} ->
            State#{cluster := Cluster#{Id := Node#{match_index => Idx}}};
        _ -> State
    end.

fold_log_from(From, Folder, St, Log) ->
    case ra_log:take(From, 5, Log) of
        [] ->
            St;
        Entries ->
            St1 = lists:foldl(Folder, St, Entries),
            fold_log_from(From+5, Folder, St1, Log)
    end.

wrap_machine_fun(Fun) ->
    case erlang:fun_info(Fun, arity) of
        {arity, 2} ->
            % user is not insterested in the index
            % of the entry
            fun(_Idx, Cmd, State) -> Fun(Cmd, State) end;
        {arity, 3} -> Fun
    end.

%%% ===================
%%% Internal unit tests
%%% ===================

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

agreed_commit_test() ->
    Nodes = #{n1 => #{match_index => 4}, % leader
              n2 => #{match_index => 3},
              n3 => #{match_index => 3}},
    % one node
    4 = agreed_commit(#{n1 => #{match_index => 4}}),
    % 2 nodes - only leader has seen new commit
    3 = agreed_commit(#{n2 => #{match_index => 3}}),
    % 2 nodes - all nodes have seen new commit
    4 = agreed_commit(Nodes#{n2 => #{match_index => 4}}),
    % 3 nodes - leader + 1 node has seen new commit
    4 = agreed_commit(#{n2 => #{match_index => 4}}),
    % only other nodes have seen new commit
    4 = agreed_commit(#{ n1 => #{match_index => 3},
                         n2 => #{match_index => 4},
                         n3 => #{match_index => 4}}),
    % 3 nodes - only leader has seen new commit
    3 = agreed_commit(Nodes),
    ok.

-endif.
