-module(ra_node).

-include("ra.hrl").

-export([
         name/2,
         init/1,
         handle_leader/2,
         handle_follower/2,
         handle_candidate/2,
         append_entries/1
        ]).

% Perhaps as it may be persisted to the log
-type ra_peer_state() :: #{next_index => non_neg_integer(),
                           match_index => non_neg_integer()}.

-type ra_cluster() :: #{ra_node_id() => ra_peer_state()}.

-type ra_machine_effect() :: {send_msg,
                              pid() | atom() | {atom(), atom()}, term()}.

% TODO: some of this state needs to be persisted
-type ra_node_state() ::
    #{id => ra_node_id(),
      leader_id => maybe(ra_node_id()),
      cluster_id => binary(),
      cluster => {normal, ra_cluster()} |
                 {joint, ra_cluster(), ra_cluster()},
      current_term => ra_term(),
      log => term(),
      voted_for => maybe(ra_node_id()),
      votes => non_neg_integer(),
      commit_index => ra_index(),
      last_applied => ra_index(),
      % Module implementing ra machine
      machine_apply_fun => fun((term(), term()) ->
                              term() | {term(), [ra_machine_effect()]}),
      machine_state => term()}.

-type ra_state() :: leader | follower | candidate.

-type ra_msg() :: #append_entries_rpc{} | #append_entries_reply{} |
                  #request_vote_rpc{} | #request_vote_result{}.

-type ra_effect() ::
    {reply, ra_msg()} |
    {send_vote_requests, [{ra_node_id(), #request_vote_rpc{}}]} |
    {send_append_entries, [{ra_node_id(), #append_entries_rpc{}}]} |
    {next_event, ra_msg()} |
    [ra_effect()] |
    {send_msg, pid(), term()} |
    none.

-export_type([ra_node_state/0,
              ra_msg/0,
              ra_effect/0]).

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
      log => ra_log:init(LogMod, LogInitArgs),
      machine_apply_fun => MachineApplyFun,
      machine_state => InitialMachineState}.


-spec handle_leader(ra_msg(), ra_node_state()) ->
    {ra_state(), ra_node_state(), ra_effect()}.
handle_leader({PeerId, #append_entries_reply{term = Term, success = true,
                                             last_index = LastIdx}},
              State0 = #{current_term := Term}) ->

    Peer0 = #{match_index := MI, next_index := NI} = peer(PeerId, State0),
    Peer = Peer0#{match_index => max(MI, LastIdx),
                  next_index => max(NI, LastIdx+1)},
    State1 = update_peer(PeerId, Peer, State0),
    NewCommitIndex = increment_commit_index(State1),
    {State, Effects} = apply_to(NewCommitIndex, State1),
    AEs = append_entries(State),
    Actions = case Effects  of
                  [] -> {send_append_entries, AEs};
                  E -> [{send_append_entries, AEs} | E]
              end,
    {leader, State, Actions};
handle_leader({_PeerId, #append_entries_reply{term = Term}},
              #{current_term := CurTerm,
                id := Id} = State) when Term > CurTerm ->
    ?DBG("~p leader saw append_entries_reply for term ~p abdicates term: ~p!~n",
         [Id, Term, CurTerm]),
    {follower, State#{current_term => Term}, none};
handle_leader({PeerId, #append_entries_reply{success = false,
                                             last_index = LastIdx,
                                             last_term = LastTerm}},
              State0 = #{cluster := {normal, Nodes},
                         log := Log}) ->
    #{PeerId := Peer0 = #{match_index := MI, next_index := NI}} = Nodes,
    % if the last_index exists and has a matching term we can forward
    % match_index and update next_index directly
    Peer = case ra_log:fetch(LastIdx, Log) of
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
handle_leader({command, Cmd}, State0 = #{id := Id}) ->
    {IdxTerm, State} = append_log_leader(Cmd, State0),
    ?DBG("~p command appended to log at ~p~n", [Id, IdxTerm]),
    AEs = append_entries(State),
    Actions = case Cmd of
                  {'$usr', From, _Data, ReplyMode} when
                       ReplyMode =:= after_log_append orelse
                       ReplyMode =:= notify_on_consensus ->
                      [{reply, From, IdxTerm},
                       {send_append_entries, AEs}];
                  _ ->
                      [{send_append_entries, AEs}]
              end,

    {leader, State, Actions};
handle_leader(#append_entries_rpc{term = Term} = Msg,
              #{current_term := CurTerm,
                id := Id} = State) when Term > CurTerm ->
    ?DBG("~p leader saw append_entries_rpc for term ~p abdicates term: ~p!~n",
         [Id, Term, CurTerm]),
    {follower, State#{current_term => Term}, {next_event, Msg}};
handle_leader(#append_entries_rpc{term = Term}, #{current_term := Term,
                                                  id := Id}) ->
    ?DBG("~p leader saw append_entries_rpc for same term ~p this should not happen: ~p!~n",
         [Id, Term]),
         exit(leader_saw_append_entries_rpc_in_same_term);
% TODO: reply to append_entries_rpcs that have lower term?
handle_leader(#request_vote_rpc{term = Term} = Msg,
              #{current_term := CurTerm,
                id := Id} = State) when Term > CurTerm ->
    ?DBG("~p leader saw request_vote_rpc for term ~p abdicates term: ~p!~n",
         [Id, Term, CurTerm]),
    {follower, State#{current_term => Term}, {next_event, Msg}};
handle_leader(#request_vote_rpc{}, State = #{current_term := Term}) ->
    Reply = #request_vote_result{term = Term, vote_granted = false},
    {leader, State, {reply, Reply}};
handle_leader(Msg, State) ->
    log_unhandled_msg(leader, Msg, State),
    {leader, State, none}.


-spec handle_follower(ra_msg(), ra_node_state()) ->
    {ra_state(), ra_node_state(), ra_effect()}.
handle_follower(#append_entries_rpc{term = Term,
                                    leader_id = LeaderId,
                                    leader_commit = LeaderCommit,
                                    prev_log_index = PLIdx,
                                    prev_log_term = PLTerm,
                                    entries = Entries},
                State0 = #{current_term := CurTerm})
  when Term >= CurTerm ->
    case has_log_entry(PLIdx, PLTerm, State0) of
        true ->
            #{log := Log} = State = lists:foldl(fun append_log_follower/2,
                                                State0, Entries),

            % do not apply Effects from the machine on a non leader
            {State1, _Effects} = apply_to(LeaderCommit,
                                          State#{current_term => Term,
                                                 leader_id => LeaderId,
                                                 log => Log}),
            Reply = append_entries_reply(Term, true, State1),
            {follower, State1, {reply, Reply}};
        false ->
            Reply = append_entries_reply(Term, false, State0),
            {follower, State0#{current_term => Term,
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
                State = #{current_term := CurTerm, id := Id,
                          log := Log})
  when Term >= CurTerm ->
    LastEntry = ra_log:last(Log),
    case is_candidate_log_up_to_date(LLIdx, LLTerm, LastEntry) of
        true ->
            ?DBG("~p granting vote to ~p for term ~p previous term was ~p",
                 [Id, Cand, Term, CurTerm]),
            Reply = #request_vote_result{term = Term, vote_granted = true},
            {follower, State#{voted_for => Cand, current_term => Term},
             {reply, Reply}};
        false ->
            ?DBG("~p declining vote to ~p for term ~p", [Id, Cand, Term]),
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
    {ra_state(), ra_node_state(), ra_effect()}.
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
                 State = #{current_term := CurTerm}) when Term >= CurTerm ->
    {follower, State#{current_term => Term}, {next_event, Msg}};
handle_candidate({_PeerId, #append_entries_reply{term = Term}},
                 State = #{current_term := CurTerm}) when Term > CurTerm ->
    {follower, State#{current_term => Term}, none};
handle_candidate(#request_vote_rpc{term = Term} = Msg,
                 State = #{current_term := CurTerm})
  when Term >= CurTerm ->
    {follower, State#{current_term => Term}, {next_event, Msg}};
handle_candidate(#request_vote_rpc{}, State = #{current_term := Term}) ->
    Reply = #request_vote_result{term = Term, vote_granted = false},
    {candidate, State, {reply, Reply}};
handle_candidate(election_timeout, State) ->
    handle_election_timeout(State);
handle_candidate(Msg, State) ->
    log_unhandled_msg(candidate, Msg, State),
    {candidate, State, none}.


%%%===================================================================
%%% Internal functions
%%%===================================================================

handle_election_timeout(State = #{id := Id, current_term := CurrentTerm}) ->
    PeerIds = peer_ids(State),
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
    maps:remove(Id, Nodes);
peers(#{id := Id, cluster := {joint, Old, New}}) ->
    Nodes = maps:merge(Old, New),
    maps:remove(Id, Nodes).

peer_ids(State) ->
    maps:keys(peers(State)).

peer(PeerId, #{cluster := {normal, Nodes}}) ->
    maps:get(PeerId, Nodes);
peer(PeerId, #{cluster := {joint, Old, New}}) ->
    case New of
        #{PeerId := Peer} -> Peer;
        _ -> maps:get(PeerId, Old)
    end.

update_peer(PeerId, Peer, #{cluster := {normal, Nodes}} = State) ->
    State#{cluster => {normal, Nodes#{PeerId => Peer}}};
update_peer(PeerId, Peer, #{cluster := {joint, Old, New}} = State) ->
    case New of
        #{PeerId := _} ->
            State#{cluster => {joint, Old, New#{PeerId => Peer}}};
        _ ->
            State#{cluster => {joint, Old#{PeerId := Peer}, New}}
    end.

last_entry(#{log := Log}) ->
    ra_log:last(Log).

is_candidate_log_up_to_date(_Idx, Term, {_LastIdx, LastTerm, _})
  when Term > LastTerm ->
    true;
is_candidate_log_up_to_date(Idx, Term, {LastIdx, Term, _})
  when Idx >= LastIdx ->
    true;
is_candidate_log_up_to_date(_Idx, _Term, {_LastIdx, _LastTerm, _}) ->
    false.

has_log_entry(Idx, Term, #{log := Log}) ->
    case ra_log:fetch(Idx, Log) of
        {Idx, Term, _} -> true;
        _ -> false
    end.

fetch_entry(Idx, #{log := Log}) ->
    ra_log:fetch(Idx, Log).

fetch_entries(From, To, #{log := Log}) ->
    ra_log:take(From, To - From + 1, Log).

make_cluster(Nodes) ->
    lists:foldl(fun(N, Acc) ->
                        Acc#{N => #{match_index => 0}}
                end, #{}, Nodes).

make_empty_append_entries(State = #{id := Id, current_term := Term,
                                    commit_index := CommitIdx,
                                    cluster := {normal, Cluster0}}) ->
    PeerIds = peer_ids(State),
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
      || PeerId <- peer_ids(State)]}.

apply_to(Commit, State0 = #{last_applied := LastApplied,
                            machine_apply_fun := ApplyFun0,
                            machine_state := MacState0,
                            cluster := Cluster})
  when Commit > LastApplied ->
    case fetch_entries(LastApplied + 1, Commit, State0) of
        [] -> {State0, []};
        Entries ->
            ApplyFun = wrap_apply_fun(ApplyFun0, Cluster),
            {MacState, NewEffects} = lists:foldl(ApplyFun, {MacState0, []},
                                                 Entries),
            {LastEntryIdx, _, _} = lists:last(Entries),
            NewCommit = min(Commit, LastEntryIdx),
            {State0#{last_applied => NewCommit,
                     commit_index => NewCommit,
                     machine_state => MacState}, NewEffects}
    end;
apply_to(_Commit, State) -> {State, []}.

wrap_apply_fun(ApplyFun, _Cluster) ->
    fun({Idx, Term, {'$ra_query', From, QueryFun, ReplyType}},
        {MacSt, Effects0}) ->
            Effects = add_reply(From, {{Idx, Term}, QueryFun(MacSt)},
                                ReplyType, Effects0),
            {MacSt, Effects};
       ({Idx, Term, {'$usr', From, Cmd, ReplyType}},
        {MacSt, Effects0}) ->
            case ApplyFun(Cmd, MacSt) of
                {NextSt, Efx} ->
                    Effects = add_reply(From, {Idx, Term}, ReplyType, Effects0),
                    {NextSt, Effects ++ Efx};
                NextSt ->
                    Effects = add_reply(From, {Idx, Term}, ReplyType, Effects0),
                    {NextSt, Effects}
            end;
       ({Idx, Term, {'$ra_cluster_change', From, {joint, _, New}, ReplyType}},
         {MacSt, Effects0}) ->
            ?DBG("ra cluster change ~p~n", [New]),
            % create effect to append new cluster to log
            Effects = [{next_event, cast,
                        {command, {'$ra_cluster_change', undefined, {normal, New},
                                   none}}} | Effects0],
            Effects1 = add_reply(From, {Idx, Term}, ReplyType, Effects),
            {MacSt, Effects1};
       (_, Acc) ->
            Acc
    end.

add_reply(From, Reply,  await_consensus, Effects) ->
    [{reply, From, Reply} | Effects];
add_reply({FromPid, _}, Reply, notify_on_consensus, Effects) ->
    [{notify, FromPid, Reply} | Effects];
add_reply(_From, _Reply, _Mode, Effects) ->
    Effects.

append_log_leader({'$ra_join', From, JoiningNode, ReplyMode},
           State = #{cluster := {normal, OldCluster},
                     log := Log0, current_term := Term}) ->
    Cluster = {joint, OldCluster,
               OldCluster#{JoiningNode => #{next_index => 1,
                                            match_index => 0}}},
    {LastIdx, _, _} = ra_log:last(Log0),
    % turn join command into a generic cluster change command
    % that include the new cluster configuration
    Command = {'$ra_cluster_change', From, Cluster, ReplyMode},
    {ok, Log} = ra_log:append({LastIdx+1, Term, Command}, false, Log0),
    {{LastIdx+1, Term}, State#{log => Log, cluster => Cluster}};
append_log_leader(Cmd, State = #{log := Log0, current_term := Term}) ->
    % TODO: optimise. ra_log:last returns the full entry which is not
    % needed here - add ra_log:lastidx.
    {LastIdx, _, _} = ra_log:last(Log0),
    {ok, Log} = ra_log:append({LastIdx+1, Term, Cmd}, false, Log0),
    {{LastIdx+1, Term}, State#{log => Log}}.

append_log_follower({_Idx, _Term, Cmd} = Entry, State = #{log := Log0,
                                                cluster := Cluster}) ->
    NextCluster = case Cmd of
                      {'$ra_cluster_change', _, New, _} ->
                         New;
                     _ -> Cluster
                  end,
    {ok, Log} = ra_log:append(Entry, true, Log0),
    State#{log => Log, cluster => NextCluster}.

append_entries(#{id := Id, log := Log, current_term := Term,
                 commit_index := CommitIndex} = State) ->
    Peers = maps:to_list(peers(State)),
    [begin
         {PrevIdx, PrevTerm, _} = ra_log:fetch(Next-1, Log),
         Entries = ra_log:take(Next, 5, Log),
         {PeerId, #append_entries_rpc{entries = Entries,
                                      term = Term,
                                      leader_id = Id,
                                      prev_log_index = PrevIdx,
                                      prev_log_term = PrevTerm,
                                      leader_commit = CommitIndex }}
     end
     || {PeerId, #{next_index := Next}} <- Peers].

append_entries_reply(Term, Success, State) ->
    {LastIdx, LastTerm, _} = last_entry(State),
    #append_entries_reply{term = Term, success = Success,
                          last_index = LastIdx,
                          last_term = LastTerm}.

increment_commit_index(State = #{current_term := CurrentTerm,
                                 commit_index := CommitIndex}) ->
    {LeaderIdx, _, _} = last_entry(State),
    Idxs = lists:sort(
             [LeaderIdx | [Idx || {_, #{match_index := Idx}} <-
                                  maps:to_list(peers(State))]]),
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
