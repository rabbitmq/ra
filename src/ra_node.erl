-module(ra_node).

-include("ra.hrl").

-export([
         name/2,
         init/1,
         handle_leader/2,
         handle_candidate/2,
         handle_follower/2,
         make_append_entries/1
        ]).

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
      % Module implementing ra machine
      machine_apply_fun => fun((ra_index(), term(), term()) ->
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
    {send_msg, pid(), term()} |
    [ra_effect()] |
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
      cluster => make_cluster(InitialNodes),
      cluster_change_permitted => false,
      cluster_index_term => {0, 0},
      pending_cluster_changes => [],
      current_term => 0,
      commit_index => 0,
      last_applied => 0,
      log => ra_log:init(LogMod, LogInitArgs),
      machine_apply_fun => wrap_machine(MachineApplyFun),
      machine_state => InitialMachineState}.

wrap_machine(Fun) ->
    case erlang:fun_info(Fun, arity) of
        {arity, 2} ->
            % user is not insterested in the index
            % of the entry
            fun(_Idx, Cmd, State) -> Fun(Cmd, State) end;
        _ -> Fun
    end.

-spec handle_leader(ra_msg(), ra_node_state()) ->
    {ra_state(), ra_node_state(), ra_effect()}.
handle_leader({PeerId, #append_entries_reply{term = Term, success = true,
                                             last_index = LastIdx}},
              State0 = #{current_term := Term, id := Id}) ->
    case peer(PeerId, State0) of
        undefined ->
            ?DBG("~p saw command from unknown peer ~p~n", [Id, PeerId]),
            {leader, State0, none};
        Peer0 = #{match_index := MI, next_index := NI} ->
            Peer = Peer0#{match_index => max(MI, LastIdx),
                          next_index => max(NI, LastIdx+1)},
            State1 = update_peer(PeerId, Peer, State0),
            {State, Effects} = evaluate_quorum(State1),
            AEs = make_append_entries(State),
            Actions = case Effects  of
                          [] -> {send_append_entries, AEs};
                          E -> [{send_append_entries, AEs} | E]
                      end,
            case State of
                #{id := Id, cluster := #{Id := _}} ->
                    % leader is in the cluster
                    {leader, State, Actions};
                #{commit_index := CI, cluster_index_term := {CITIndex, _}}
                  when CI >= CITIndex ->
                    % leader is not in the cluster and the new cluster
                    % config has been committed
                    % time to say goodbye
                    ?DBG("~p leader stopping - goodbye", [Id]),
                    {stop, State, Actions};
                _ ->
                    {leader, State, Actions}
            end
    end;
handle_leader({PeerId, #append_entries_reply{term = Term}},
              #{current_term := CurTerm,
                id := Id} = State) when Term > CurTerm ->
    case peer(PeerId, State) of
        undefined ->
            ?DBG("~p saw command from unknown peer ~p~n", [Id, PeerId]),
            {leader, State, none};
        _ ->
            ?DBG("~p leader saw append_entries_reply for term ~p abdicates term: ~p!~n",
                 [Id, Term, CurTerm]),
            {follower, State#{current_term => Term}, none}
    end;
handle_leader({PeerId, #append_entries_reply{success = false,
                                             last_index = LastIdx,
                                             last_term = LastTerm}},
              State0 = #{cluster := Nodes,
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
    State = State0#{cluster => Nodes#{PeerId => Peer}},
    AEs = make_append_entries(State),
    {leader, State, {send_append_entries, AEs}};
handle_leader({command, Cmd}, State0 = #{id := Id}) ->
    case append_log_leader(Cmd, State0) of
        {not_appended, State} ->
            {leader, State, []};
        {IdxTerm, State}  ->
            ?DBG("~p command ~p appended to log at ~p~n", [Id, Cmd, IdxTerm]),
            {State1, Effects0} = evaluate_quorum(State),
            Effects = [{send_append_entries, make_append_entries(State1)} | Effects0],
            Actions = case Cmd of
                          {_, _, _, await_consensus} ->
                              Effects;
                          {_, undefined, _, _} ->
                              Effects;
                          {_, From, _, _} ->
                              [{reply, From, IdxTerm} | Effects];
                          _ ->
                              Effects
                      end,
            {leader, State1, Actions}
    end;
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
handle_leader(#request_vote_rpc{term = Term, candidate_id = Cand} = Msg,
              #{current_term := CurTerm,
                id := Id} = State) when Term > CurTerm ->
    case peer(Cand, State) of
        undefined ->
            ?DBG("~p leader saw request_vote_rpc for unknown peer ~p~n",
                 [Id, Cand]),
            {leader, State, none};
        _ ->
            ?DBG("~p leader saw request_vote_rpc for term ~p abdicates term: ~p!~n",
                 [Id, Term, CurTerm]),
            {follower, State#{current_term => Term}, {next_event, Msg}}
    end;
handle_leader(#request_vote_rpc{}, State = #{current_term := Term}) ->
    Reply = #request_vote_result{term = Term, vote_granted = false},
    {leader, State, {reply, Reply}};
handle_leader(Msg, State) ->
    log_unhandled_msg(leader, Msg, State),
    {leader, State, none}.


-spec handle_candidate(ra_msg() | election_timeout, ra_node_state()) ->
    {ra_state(), ra_node_state(), ra_effect()}.
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
            {candidate, State0#{votes => NewVotes}, none}
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
            State = lists:foldl(fun append_log_follower/2,
                                                State0, Entries),

            % do not apply Effects from the machine on a non leader
            {State1, _Effects} = apply_to(LeaderCommit,
                                          State#{current_term => Term,
                                                 leader_id => LeaderId}),
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
                State = #{current_term := CurTerm, id := Id})
  when Term >= CurTerm ->
    LastEntry = last_entry(State),
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



%%%===================================================================
%%% Internal functions
%%%===================================================================

handle_election_timeout(State = #{id := Id, current_term := CurrentTerm}) ->
    ?DBG("~p election timeout in term ~p~n", [Id, CurrentTerm]),
    PeerIds = peer_ids(State),
    % increment current term
    NewTerm = CurrentTerm + 1,
    {LastIdx, LastTerm, _Data} = last_entry(State),
    Actions = [{PeerId, #request_vote_rpc{term = NewTerm, candidate_id = Id,
                                          last_log_index = LastIdx,
                                          last_log_term = LastTerm}}
               || PeerId <- PeerIds],
    % vote for self
    VoteForSelf = #request_vote_result{term = NewTerm, vote_granted = true},
    {candidate, State#{current_term => NewTerm,
                       votes => 0}, [{next_event, cast, VoteForSelf},
                                     {send_vote_requests, Actions}]}.

peers(#{id := Id, cluster := Nodes}) ->
    maps:remove(Id, Nodes).

peer_ids(State) ->
    maps:keys(peers(State)).

peer(PeerId, #{cluster := Nodes}) ->
    maps:get(PeerId, Nodes, undefined).

update_peer(PeerId, Peer, #{cluster := Nodes} = State) ->
    State#{cluster => Nodes#{PeerId => Peer}}.

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

initialise_peers(State = #{log := Log, cluster := Cluster0}) ->
    PeerIds = peer_ids(State),
    NextIdx = ra_log:next_index(Log),
    Cluster = lists:foldl(fun(PeerId, Acc) ->
                                  Acc#{PeerId => #{match_index => 0,
                                                   next_index => NextIdx}}
                          end, Cluster0, PeerIds),
    State#{cluster => Cluster}.


apply_to(Commit, State0 = #{last_applied := LastApplied,
                            machine_apply_fun := ApplyFun0,
                            machine_state := MacState0})
  when Commit > LastApplied ->
    case fetch_entries(LastApplied + 1, Commit, State0) of
        [] -> {State0, []};
        Entries ->
            ?DBG("applying entrries: ~p", [Entries]),
            ApplyFun = wrap_apply_fun(ApplyFun0),
            {State, MacState, NewEffects} = lists:foldl(ApplyFun,
                                                        {State0, MacState0, []},
                                                        Entries),
            {LastEntryIdx, _, _} = lists:last(Entries),
            NewCommit = min(Commit, LastEntryIdx),
            {State#{last_applied => NewCommit,
                    commit_index => NewCommit,
                    machine_state => MacState}, NewEffects}
    end;
apply_to(_Commit, State) -> {State, []}.

wrap_apply_fun(ApplyFun) ->
    fun({Idx, Term, {'$ra_query', From, QueryFun, ReplyType}},
        {State, MacSt, Effects0}) ->
            Effects = add_reply(From, {{Idx, Term}, QueryFun(MacSt)},
                                ReplyType, Effects0),
            {State, MacSt, Effects};
       ({Idx, Term, {'$usr', From, Cmd, ReplyType}},
        {State, MacSt, Effects0}) ->
            case ApplyFun(Idx, Cmd, MacSt) of
                {NextMacSt, Efx} ->
                    Effects = add_reply(From, {Idx, Term}, ReplyType, Effects0),
                    {State, NextMacSt, Effects ++ Efx};
                NextMacSt ->
                    Effects = add_reply(From, {Idx, Term}, ReplyType, Effects0),
                    {State, NextMacSt, Effects}
            end;
       ({Idx, Term, {'$ra_cluster_change', From, New, ReplyType}},
         {State0, MacSt, Effects0}) ->
            ?DBG("ra cluster change to ~p~n", [New]),
            Effects = add_reply(From, {Idx, Term}, ReplyType, Effects0),
            State = State0#{cluster_change_permitted => true},
            {State, MacSt, Effects};
       ({_Idx, Term, noop}, {State0 = #{current_term := Term}, MacSt, Effects}) ->
            State = State0#{cluster_change_permitted => true},
            {State, MacSt, Effects};
       (_, Acc) ->
            Acc
    end.

add_reply(From, Reply,  await_consensus, Effects) ->
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
    Cluster = OldCluster#{JoiningNode => #{next_index => 1,
                                           match_index => 0}},
    append_cluster_change(Cluster, From, ReplyMode, State);
append_log_leader({'$ra_leave', From, LeavingNode, ReplyMode},
                  State = #{cluster := OldCluster}) ->
    Cluster = maps:remove(LeavingNode, OldCluster),
    append_cluster_change(Cluster, From, ReplyMode, State);
append_log_leader(Cmd, State = #{log := Log0, current_term := Term}) ->
    NextIdx = ra_log:next_index(Log0),
    {ok, Log} = ra_log:append({NextIdx, Term, Cmd}, false, Log0),
    {{NextIdx, Term}, State#{log => Log}}.

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
            {ok, Log} = ra_log:append(Entry, true, Log0),
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
    {ok, Log} = ra_log:append(Entry, true, Log0),
    State#{log => Log, cluster => Cluster, cluster_index_term => {Idx, Term}};
append_log_follower(Entry, State = #{log := Log0}) ->
    {ok, Log} = ra_log:append(Entry, true, Log0),
    State#{log => Log}.

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
    {ok, Log} = ra_log:append({NextIdx, Term, Command}, false, Log0),
    {IdxTerm, State#{log => Log, cluster => Cluster,
                     cluster_change_permitted => false,
                     cluster_index_term => IdxTerm,
                     previous_cluster => {PrevCITIdx, PrevCITTerm, PrevCluster}}}.

make_append_entries(#{id := Id, log := Log, current_term := Term,
                      commit_index := CommitIndex} = State) ->
    maps:fold(
      fun(PeerId, #{next_index := Next}, Acc) ->
              {PrevIdx, PrevTerm, _} = ra_log:fetch(Next-1, Log),
              Entries = ra_log:take(Next, 5, Log),
              [{PeerId, #append_entries_rpc{entries = Entries,
                                            term = Term,
                                            leader_id = Id,
                                            prev_log_index = PrevIdx,
                                            prev_log_term = PrevTerm,
                                            leader_commit = CommitIndex}} | Acc]
      end, [], peers(State)).

append_entries_reply(Term, Success, State) ->
    {LastIdx, LastTerm, _} = last_entry(State),
    #append_entries_reply{term = Term, success = Success,
                          last_index = LastIdx,
                          last_term = LastTerm}.

evaluate_quorum(State) ->
    NewCommitIndex = increment_commit_index(State),
    apply_to(NewCommitIndex, State).

increment_commit_index(State = #{current_term := CurrentTerm,
                                 commit_index := CommitIndex}) ->
    {LeaderIdx, _, _} = last_entry(State),
    PotentialNewCommitIndex = agreed_commit(LeaderIdx, peers(State)),
    % leaders can only increment their commit index if the corresponding
    % log entry term matches the current term. See (§5.4.2)
    case fetch_entry(PotentialNewCommitIndex, State) of
        {_, CurrentTerm, _} ->
             PotentialNewCommitIndex;
        _ -> CommitIndex
    end.

-spec agreed_commit(ra_index(), map()) -> ra_index().
agreed_commit(LeaderIdx, Peers) ->
    Idxs = maps:fold(fun(_K, #{match_index := Idx}, Acc) ->
                             [Idx | Acc]
                     end, [LeaderIdx], Peers),
    SortedIdxs = lists:sort(fun erlang:'>'/2, Idxs),
    Nth = trunc(length(Idxs) / 2) + 1,
    lists:nth(Nth, SortedIdxs).

log_unhandled_msg(RaState, Msg, State) ->
    ?DBG("~p received unhandled msg: ~p~nState was~p~n", [RaState, Msg, State]).

%%% ===================
%%% Internal unit tests
%%% ===================

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

agreed_commit_test() ->
    Peers = #{n2 => #{match_index => 3},
              n3 => #{match_index => 3}},
    % one node
    4 = agreed_commit(4, #{}),
    % 2 nodes - only leader has seen new commit
    3 = agreed_commit(4, #{n2 => #{match_index => 3}}),
    % 2 nodes - all nodes have seen new commit
    4 = agreed_commit(4, Peers#{n2 => #{match_index => 4}}),
    % 3 nodes - leader + 1 node has seen new commit
    4 = agreed_commit(4, #{n2 => #{match_index => 4}}),
    % 3 nodes - only leader has seen new commit
    3 = agreed_commit(4, Peers),
    ok.

-endif.
