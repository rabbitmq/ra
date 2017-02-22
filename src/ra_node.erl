-module(ra_node).

-include("ra.hrl").

-export([
         name/2,
         init/1,
         handle_leader/2,
         handle_follower/2,
         handle_candidate/2
        ]).

% TODO should this really be a shared record?
% Perhaps as it may be persisted to the log
-type ra_peer_state() :: #{next_index => non_neg_integer(),
                             match_index => non_neg_integer()}.

-type ra_cluster() :: #{ra_node_id() => ra_peer_state()}.

% TODO: some of this state needs to be persisted
-type ra_node_state(MacState) ::
    #{id => ra_node_id(),
      cluster_id => binary(),
      cluster => {normal, ra_cluster()} |
                 {joint, ra_cluster(), ra_cluster()},
      current_term => ra_term(),
      log => term(),
      log_mod => atom(), % if only this could be narrowed down to a behavour - sigh
      voted_for => maybe(ra_node_id()),
      votes => non_neg_integer(),
      commit_index => ra_index(),
      last_applied => ra_index(),
      machine_apply_fun => fun((term(), MacState) -> MacState), % Module implementing ra machine
      machine_state => MacState}.

-type ra_state() :: leader | follower | candidate.

-export_type([ra_node_state/1]).

-spec name(ClusterId::string(), UniqueSuffix::string()) -> atom().
name(ClusterId, UniqueSuffix) ->
    list_to_atom("ra_" ++ ClusterId ++ "_node_" ++ UniqueSuffix).


-spec init(map()) -> ra_node_state(_).
init(#{id := Id,
       initial_nodes := InitialNodes,
       cluster_id := ClusterId,
       log_module := LogMod,
       log_init_args := LogInitArgs,
       apply_fun := MachineApplyFun,
       initial_state := InitialMachineState}) ->
    #{id => Id,
      cluster_id => ClusterId,
      cluster => {normal, make_cluster(InitialNodes)},
      current_term => 0,
      commit_index => 0,
      last_applied => 0,
      log => LogMod:init(LogInitArgs),
      log_mod => LogMod,
      machine_apply_fun => MachineApplyFun,
      machine_state => InitialMachineState}.

-spec handle_leader(ra_msg(), ra_node_state(M)) ->
    {ra_state(), ra_node_state(M), ra_action()}.
handle_leader({_Peer, #append_entries_reply{term = Term}},
                       State = #{current_term := Term}) ->
    % Reply = #append_entries_reply{term = Term, success = true},
    {leader, State, none};
handle_leader(_Msg, State) ->
    {leader, State, none}.

-spec handle_follower(ra_msg(), ra_node_state(M)) ->
    {ra_state(), ra_node_state(M), ra_action()}.
handle_follower(election_timeout, State) ->
    handle_election_timeout(State);
handle_follower(#append_entries_rpc{term = Term,
                                    leader_commit = LeaderCommit,
                                    prev_log_index = PLIdx,
                                    prev_log_term = PLTerm,
                                    entries = Entries},
                       State = #{current_term := CurTerm,
                                 log_mod := LogMod,
                                 log := Log0})
  when Term >= CurTerm ->
    case has_log_entry(PLIdx, PLTerm, State) of
        true ->
            Reply = #append_entries_reply{term = Term, success = true},
            Log = lists:foldl(fun(E, Acc) ->
                                      {ok, L} = LogMod:append(E, true, Acc),
                                      L
                              end, Log0, Entries),
            State1 = apply_to(LeaderCommit,
                              State#{current_term => Term,
                                     log => Log}),
            {follower, State1, {reply, Reply}};
        false ->
            Reply = #append_entries_reply{term = Term, success = false},
            {follower, State#{current_term => Term}, {reply, Reply}}
    end;
handle_follower(#append_entries_rpc{},
                       State = #{current_term := CurTerm}) ->
    Reply = #append_entries_reply{term = CurTerm, success = false},
    {follower, State, {reply, Reply}};
handle_follower(#request_vote_rpc{candidate_id = Cand,
                                  term = Term},
                State = #{current_term := Term, voted_for := VotedFor})
  when VotedFor /= Cand ->
    % already voted in this term
    Reply = #request_vote_result{term = Term, vote_granted = false},
    {follower, State, {reply, Reply}};
handle_follower(#request_vote_rpc{candidate_id = Cand,
                                  term = Term,
                                  last_log_index = LLIdx,
                                  last_log_term = LLTerm},
                State = #{current_term := CurTerm})
  when Term >= CurTerm ->
    case has_log_entry(LLIdx, LLTerm, State) of
        true ->
            Reply = #request_vote_result{term = Term, vote_granted = true},
            {follower, State#{voted_for => Cand, current_term => Term},
             {reply, Reply}};
        false ->
            Reply = #request_vote_result{term = Term, vote_granted = false},
            {follower, State#{current_term => Term}, {reply, Reply}}
    end;
handle_follower(#request_vote_rpc{term = Term},
                State = #{current_term := CurTerm})
  when Term < CurTerm ->
    Reply = #request_vote_result{term = CurTerm, vote_granted = false},
    {follower, State, {reply, Reply}};

handle_follower(_Msg, State) ->
    {follower, State, none}.

-spec handle_candidate(ra_msg() | election_timeout, ra_node_state(M)) ->
    {ra_state(), ra_node_state(M), ra_action()}.
handle_candidate(#request_vote_result{term = Term, vote_granted = true},
                 State = #{current_term := Term, votes := Votes,
                           cluster := {normal, Nodes}}) ->
    NewVotes = Votes+1,
    case trunc(maps:size(Nodes) / 2) + 1 of
        NewVotes ->
            {Cluster, Actions} = make_empty_append_entries(State),
            {leader, maps:remove(votes, State#{cluster => Cluster}),
             {append, Actions}};
        _ ->
            {candidate, State#{votes => NewVotes}, none}
    end;
handle_candidate(#request_vote_result{term = Term},
                 State = #{current_term := CurTerm}) when Term > CurTerm ->
    {follower, State#{current_term => Term}, none};
handle_candidate(#request_vote_result{vote_granted = false}, State) ->
    {candidate, State, none};
handle_candidate(election_timeout, State) ->
    handle_election_timeout(State).


%% util
%%


handle_election_timeout(State = #{id := Id, current_term := CurrentTerm}) ->
    PeerIds = peers(State),
    % increment current term
    NewTerm = CurrentTerm + 1,
    {LastIdx, LastTerm, _Data} = last_entry(State),
    Actions = [{PeerId, #request_vote_rpc{term = NewTerm, candidate_id = Id,
                                          last_log_index = LastIdx,
                                          last_log_term = LastTerm}}
               || PeerId <- PeerIds],
    % vote for self
    {candidate, State#{current_term => NewTerm,
                       votes => 1}, {vote, Actions}}.

peers(#{id := Id, cluster := {normal, Nodes}}) ->
    maps:keys(maps:remove(Id, Nodes)).

last_entry(#{log := Log, log_mod := Mod}) ->
    Mod:last(Log).

has_log_entry(Idx, Term, #{log := Log, log_mod := Mod}) ->
    case Mod:fetch(Idx, Log) of
        {Idx, Term, _} -> true;
        _ -> false
    end.

fetch_entries(From, To, #{log := Log, log_mod := Mod}) ->
    Mod:take(From, To - From, Log).

make_cluster(Nodes) ->
    lists:foldl(fun(N, Acc) ->
                        Acc#{N => #{match_index => 0}}
                end, #{}, Nodes).

make_empty_append_entries(State = #{id := Id, current_term := Term,
                                    commit_index := CommitIdx,
                                    cluster := {normal, Cluster0}}) ->
    PeerIds = peers(State),
    {LastIdx, LastTerm, _} = last_entry(State),
    Cluster = lists:foldl(fun(PeerId, Acc) ->
                                  Acc#{PeerId => #{match_index => 0,
                                                   next_index => LastIdx+1}}
                          end, Cluster0, PeerIds),

    {{normal, Cluster},
     [{PeerId, #append_entries_rpc{term = Term,
                                   leader_id = Id,
                                   prev_log_index = LastIdx,
                                   prev_log_term = LastTerm,
                                   leader_commit = CommitIdx}}
      || PeerId <- peers(State)]}.

apply_to(Commit, State = #{last_applied := LastApplied,
                           machine_state := MacState0,
                           machine_apply_fun := ApplyFun})
  when Commit > LastApplied ->
    case fetch_entries(LastApplied, Commit, State) of
        [] -> State;
        Entries ->
            MacState = lists:foldl(ApplyFun, MacState0,
                                   [E || {_I, _T, E} <- Entries]),

            {LastEntryIdx, _, _} = lists:last(Entries),
            NewCommit = min(Commit, LastEntryIdx),
            State#{last_applied => NewCommit,
                   commit_index => NewCommit,
                   machine_state => MacState}
    end;
apply_to(_Commit, State) -> State.




