-module(ra_node).

-include("ra.hrl").

-export([
         name/2,
         init/1,
         handle_leader/2,
         handle_follower/2,
         handle_candidate/2
        ]).

% Perhaps as it may be persisted to the log
-type ra_peer_state() :: #{next_index => non_neg_integer(),
                           match_index => non_neg_integer()}.

-type ra_cluster() :: #{ra_node_id() => ra_peer_state()}.

% TODO: some of this state needs to be persisted
-type ra_node_state() ::
    #{id => ra_node_id(),
      leader_id => maybe(ra_node_id()),
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
      % Module implementing ra machine
      machine_apply_fun => fun((term(), term()) -> term()),
      machine_state => term()}.

-type ra_state() :: leader | follower | candidate.

-type ra_msg() :: #append_entries_rpc{} | #append_entries_reply{} |
                  #request_vote_rpc{} | #request_vote_result{}.

-type ra_action() :: {reply, ra_msg()} |
                     {send_vote_requests, [{ra_node_id(), #request_vote_rpc{}}]} |
                     {send_append_entries, [{ra_node_id(), #append_entries_rpc{}}]} |
                     {next_event, ra_msg()} |
                     [ra_action()] |
                     none.

-export_type([ra_node_state/0,
              ra_msg/0,
              ra_action/0]).

-spec name(ClusterId::string(), UniqueSuffix::string()) -> atom().
name(ClusterId, UniqueSuffix) ->
    list_to_atom("ra_" ++ ClusterId ++ "_node_" ++ UniqueSuffix).


-spec init(map()) -> ra_node_state().
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

-spec handle_leader(ra_msg(), ra_node_state()) ->
    {ra_state(), ra_node_state(), ra_action()}.
handle_leader({PeerId, #append_entries_reply{term = Term, success = true,
                                             last_index = LastIdx}},
              State0 = #{current_term := Term,
                         cluster := {normal, Nodes}}) ->
    #{PeerId := Peer0 = #{match_index := MI, next_index := NI}} = Nodes,
    Peer = Peer0#{match_index => max(MI, LastIdx),
                  next_index => max(NI, LastIdx+1)},
    State1 = State0#{cluster => {normal, Nodes#{PeerId => Peer}}},
    NewCommitIndex = increment_commit_index(State1),
    State = apply_to(NewCommitIndex, State1),
    AEs = append_entries(State),
    {leader, State, {send_append_entries, AEs}};
handle_leader({_PeerId, #append_entries_reply{term = Term}},
              #{current_term := CurTerm} = State) when Term > CurTerm ->
    {follower, State#{current_term => Term}, none};
handle_leader({PeerId, #append_entries_reply{success = false,
                                             last_index = LastIdx,
                                             last_term = LastTerm}},
              State0 = #{cluster := {normal, Nodes},
                         log := Log, log_mod := LogMod}) ->
    #{PeerId := Peer0 = #{match_index := MI, next_index := NI}} = Nodes,
    % if the last_index exists and has a matching term we can forward match_index
    % and update next_index directly
    Peer = case LogMod:fetch(LastIdx, Log) of
               {_, LastTerm, _} -> % entry exists forward all things
                   Peer0#{match_index => LastIdx,
                          next_index => LastIdx+1};
               _ ->
                   % take the smallest next_index as long as it is
                   % not smaller than the last known match index
                   Peer0#{next_index => max(min(NI-1, LastIdx), MI)}
           end,

    State = State0#{cluster => {normal, Nodes#{PeerId => Peer}}},
    AEs = append_entries(State),
    {leader, State, {send_append_entries, AEs}};
handle_leader(#append_entries_rpc{term = Term} = Msg,
              #{current_term := CurTerm} = State) when Term > CurTerm ->
    {follower, State#{current_term => Term}, {next_event, Msg}};
handle_leader(#request_vote_rpc{term = Term} = Msg,
              #{current_term := CurTerm} = State) when Term > CurTerm ->
    {follower, State#{current_term => Term}, {next_event, Msg}};
handle_leader({command, Data}, State0) ->
    {IdxTerm, State} = append_log(Data, State0),
    AEs = append_entries(State),
    {leader, State, [{reply, IdxTerm}, {send_append_entries, AEs}]};

handle_leader(Msg, State) ->
    log_unhandled_msg(leader, Msg, State),
    {leader, State, none}.

-spec handle_follower(ra_msg(), ra_node_state()) ->
    {ra_state(), ra_node_state(), ra_action()}.
handle_follower(#append_entries_rpc{term = Term,
                                    leader_id = LeaderId,
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
            Log = lists:foldl(fun(E, Acc) ->
                                      {ok, L} = LogMod:append(E, true, Acc),
                                      L
                              end, Log0, Entries),

            State1 = apply_to(LeaderCommit, State#{current_term => Term,
                                                   leader_id => LeaderId,
                                                   log => Log}),
            Reply = append_entries_reply(CurTerm, true, State1),
            {follower, State1, {reply, Reply}};
        false ->
            Reply = append_entries_reply(CurTerm, false, State),
            {follower, State#{current_term => Term,
                              leader_id => LeaderId}, {reply, Reply}}
    end;
handle_follower(#append_entries_rpc{}, State = #{current_term := CurTerm}) ->
    Reply = append_entries_reply(CurTerm, false, State),
    {follower, maps:without([leader_id], State), {reply, Reply}};
handle_follower(#request_vote_rpc{candidate_id = Cand, term = Term},
                State = #{current_term := Term, voted_for := VotedFor})
  when VotedFor /= Cand ->
    % already voted for another in this term
    Reply = #request_vote_result{term = Term, vote_granted = false},
    {follower, maps:without([leader_id], State), {reply, Reply}};
handle_follower(#request_vote_rpc{candidate_id = Cand, term = Term,
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
handle_follower({_PeerId, #append_entries_reply{term = Term}},
                State = #{current_term := CurTerm}) when Term > CurTerm ->
    {follower, State#{current_term => Term}, none};
handle_follower(election_timeout, State) ->
    handle_election_timeout(State);
handle_follower(Msg, State) ->
    log_unhandled_msg(follower, Msg, State),
    {follower, State, none}.

-spec handle_candidate(ra_msg() | election_timeout, ra_node_state()) ->
    {ra_state(), ra_node_state(), ra_action()}.
handle_candidate(#request_vote_result{term = Term, vote_granted = true},
                 State = #{current_term := Term, votes := Votes,
                           cluster := {normal, Nodes}}) ->
    NewVotes = Votes+1,
    case trunc(maps:size(Nodes) / 2) + 1 of
        NewVotes ->
            {Cluster, Actions} = make_empty_append_entries(State),
            {leader, maps:without([votes, leader_id],
                                  State#{cluster => Cluster}),
             {send_append_entries, Actions}};
        _ ->
            {candidate, State#{votes => NewVotes}, none}
    end;
handle_candidate(#request_vote_result{term = Term},
                 State = #{current_term := CurTerm}) when Term > CurTerm ->
    {follower, State#{current_term => Term}, none};
handle_candidate(#request_vote_result{vote_granted = false}, State) ->
    {candidate, State, none};
handle_candidate(#append_entries_rpc{term = Term} = Msg,
                 State = #{current_term := CurTerm}) when Term > CurTerm ->
    {follower, State#{current_term => Term}, {next_event, Msg}};
handle_candidate({_PeerId, #append_entries_reply{term = Term}},
                 State = #{current_term := CurTerm}) when Term > CurTerm ->
    {follower, State#{current_term => Term}, none};
handle_candidate(#request_vote_rpc{term = Term} = Msg,
                 State = #{current_term := CurTerm})
  when Term > CurTerm ->
    {follower, State#{current_term => Term}, {next_event, Msg}};
handle_candidate(election_timeout, State) ->
    handle_election_timeout(State);
handle_candidate(Msg, State) ->
    log_unhandled_msg(candidate, Msg, State),
    {candidate, State, none}.


%%%===================================================================
%%% Internal functions
%%%===================================================================

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
                       votes => 1}, {send_vote_requests, Actions}}.

peers(#{id := Id, cluster := {normal, Nodes}}) ->
    maps:keys(maps:remove(Id, Nodes)).

last_entry(#{log := Log, log_mod := Mod}) ->
    Mod:last(Log).

has_log_entry(Idx, Term, #{log := Log, log_mod := Mod}) ->
    case Mod:fetch(Idx, Log) of
        {Idx, Term, _} -> true;
        _ -> false
    end.

fetch_entry(Idx, #{log := Log, log_mod := Mod}) ->
    Mod:fetch(Idx, Log).

fetch_entries(From, To, #{log := Log, log_mod := Mod}) ->
    Mod:take(From, To - From + 1, Log).

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
    case fetch_entries(LastApplied+1, Commit, State) of
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


append_log(Cmd, State = #{log := Log0, log_mod := Mod, current_term := Term}) ->
    {LastIdx, _, _} = Mod:last(Log0),
    {ok, Log} = Mod:append({LastIdx+1, Term, Cmd}, false, Log0),
    {{LastIdx+1, Term}, State#{log => Log}}.


append_entries(#{id := Id, cluster := {normal, Nodes},
                 log := Log, log_mod := Mod,
                 current_term := Term, commit_index := CommitIndex}) ->
    Peers = maps:remove(Id, Nodes),
    [begin
         {PrevIdx, PrevTerm, _} = Mod:fetch(Next-1, Log),
         {PeerId, #append_entries_rpc{entries = Mod:take(Next, 5, Log),
                                      term = Term,
                                      leader_id = Id,
                                      prev_log_index = PrevIdx,
                                      prev_log_term = PrevTerm,
                                      leader_commit = CommitIndex }}
     end
     || {PeerId, #{next_index := Next}} <- maps:to_list(Peers)].

append_entries_reply(Term, Success, State) ->
    {LastIdx, LastTerm, _} = last_entry(State),
    #append_entries_reply{term = Term, success = Success,
                          last_index = LastIdx,
                          last_term = LastTerm}.

increment_commit_index(State = #{cluster := {normal, Nodes}, id := Id,
                                 current_term := CurrentTerm,
                                 commit_index := CommitIndex}) ->
    {LeaderIdx, _, _} = last_entry(State),
    Idxs = lists:sort(
             [LeaderIdx |
              [Idx || {_, #{match_index := Idx}} <- maps:to_list(maps:remove(Id, Nodes))]]),
    Nth = trunc(length(Idxs) / 2) + 1,
    PotentialNewCommitIndex = lists:nth(Nth, Idxs),
    % leaders can only increment their commit index if the corresponding
    % log entry term matches the current term
    case fetch_entry(PotentialNewCommitIndex, State) of
        {_, CurrentTerm, _} ->
             PotentialNewCommitIndex;
        _ -> CommitIndex
    end.

log_unhandled_msg(RaState, Msg, State) ->
    ?DBG("~p received unhandled msg: ~p~nState was~p~n", [RaState, Msg, State]).
