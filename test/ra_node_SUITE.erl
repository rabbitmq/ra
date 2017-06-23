-module(ra_node_SUITE).

-compile(export_all).

-include("ra.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

all() ->
    [
     election_timeout,
     follower_handleds_append_entries_rpc,
     append_entries_reply_success,
     append_entries_reply_no_success,
     follower_vote,
     request_vote_rpc_with_lower_term,
     leader_does_not_abdicate_to_unknown_peer,
     higher_term_detected,
     quorum,
     command,
     consistent_query,
     leader_noop_operation_enables_cluster_change,
     leader_node_join,
     leader_node_leave,
     leader_is_removed,
     follower_cluster_change,
     leader_applies_new_cluster,
     leader_appends_cluster_change_then_steps_before_applying_it
    ].

groups() ->
    [ {tests, [], all()} ].

id(X) -> X.

election_timeout(_Config) ->
    State = base_state(3),
    Msg = election_timeout,
    Term = 6,
    VoteRpc = #request_vote_rpc{term = Term, candidate_id = n1, last_log_index = 3,
                                last_log_term = 5},
    VoteForSelfEvent = {next_event, cast,
                        #request_vote_result{term = Term, vote_granted = true}},
    {candidate, #{current_term := Term, votes := 0},
     [VoteForSelfEvent, {send_vote_requests, [{n2, VoteRpc}, {n3, VoteRpc}]}]} =
        ra_node:handle_follower(Msg, State),
    {candidate, #{current_term := Term, votes := 0},
     [VoteForSelfEvent, {send_vote_requests, [{n2, VoteRpc}, {n3, VoteRpc}]}]} =
        ra_node:handle_candidate(Msg, State),
    ok.

follower_handleds_append_entries_rpc(_Config) ->
    Self = self(),
    State = (base_state(3))#{commit_index => 1},
    EmptyAE = #append_entries_rpc{term = 5,
                                  leader_id = n1,
                                  prev_log_index = 3,
                                  prev_log_term = 5,
                                  leader_commit = 3},
    % success case - everything is up to date leader id got updated
    {follower, #{leader_id := n1},
     {reply, #append_entries_reply{term = 5, success = true,
                                   last_index = 3, last_term = 5}}}
        = ra_node:handle_follower(EmptyAE, State),

    % success case when leader term is higher
    % reply term should be updated
    {follower, #{leader_id := n1},
     {reply, #append_entries_reply{term = 6, success = true,
                                   last_index = 3, last_term = 5}}}
        = ra_node:handle_follower(EmptyAE#append_entries_rpc{term = 6}, State),

    % reply false if term < current_term (5.1)
    {follower, _, {reply, #append_entries_reply{term = 5, success = false}}}
        = ra_node:handle_follower(EmptyAE#append_entries_rpc{term = 4}, State),

    % reply false if log doesn't contain a term matching entry at prev_log_index
    {follower, _, {reply, #append_entries_reply{term = 5, success = false}}}
        = ra_node:handle_follower(EmptyAE#append_entries_rpc{prev_log_index = 4},
                                  State),
    {follower, _, {reply, #append_entries_reply{term = 5, success = false}}}
        = ra_node:handle_follower(EmptyAE#append_entries_rpc{prev_log_term = 4},
                                  State),

    % truncate/overwrite if a existing entry conflicts (diff term) with
    % a new one (5.3)
    AE = #append_entries_rpc{term = 5, leader_id = n1,
                             prev_log_index = 1, prev_log_term = 1,
                             leader_commit = 2,
                             entries = [{2, 4, {'$usr', Self, <<"hi">>,
                                                after_log_append}}]},

    ExpectedLog = {2, #{0 => {0, undefined}, 1 => {1, usr(<<"hi1">>)},
                        2 => {4, usr(<<"hi">>)}}, #{}},
    {follower,  #{log := {ra_log_memory, ExpectedLog}},
     {reply, #append_entries_reply{term = 5, success = true,
                                   last_index = 2, last_term = 4}}}
        = ra_node:handle_follower(AE, State#{last_applied => 1}),

    % append new entries not in the log
    % if leader_commit > commit_index set commit_index = min(leader_commit, index of last new entry)
    ExpectedLogEntry = usr(<<"hi4">>),
    {follower, #{log := {ra_log_memory, {4, #{4 := {5, ExpectedLogEntry}}, #{}}},
                 commit_index := 4, last_applied := 4,
                 machine_state := <<"hi4">>},
     {reply, #append_entries_reply{term = 5, success = true,
                                   last_index = 4, last_term = 5}}}
        = ra_node:handle_follower(
            EmptyAE#append_entries_rpc{entries = [{4, 5, usr(<<"hi4">>)}],
                                       leader_commit = 5},
            State#{commit_index => 1, last_applied => 1,
                   machine_state => usr(<<"hi1">>)}),
    ok.

append_entries_reply_success(_Config) ->
    Cluster = #{n1 => #{},
                n2 => #{next_index => 1, match_index => 0},
                n3 => #{next_index => 2, match_index => 1}},
    State = (base_state(3))#{commit_index => 1,
                             last_applied => 1,
                             cluster => Cluster,
                             machine_state => <<"hi1">>},
    Msg = {n2, #append_entries_reply{term = 5, success = true,
                                     last_index = 3, last_term = 5}},
    ExpectedActions =
        {send_append_entries,
         [ {n3, #append_entries_rpc{term = 5, leader_id = n1,
                                    prev_log_index = 1,
                                    prev_log_term = 1,
                                    leader_commit = 3,
                                    entries = [{2, 3, usr(<<"hi2">>)},
                                               {3, 5, usr(<<"hi3">>)}]}},
           {n2, #append_entries_rpc{term = 5, leader_id = n1,
                                    prev_log_index = 3,
                                    prev_log_term = 5,
                                    leader_commit = 3}}]},
    % update match index
    {leader, #{cluster := #{n2 := #{next_index := 4, match_index := 3}},
               commit_index := 3,
               last_applied := 3,
               machine_state := <<"hi3">>}, ExpectedActions} =
        ra_node:handle_leader(Msg, State),

    Msg1 = {n2, #append_entries_reply{term = 7, success = true,
                                      last_index = 3, last_term = 5}},
    {leader, #{cluster := #{n2 := #{next_index := 4,
                                             match_index := 3}},
               commit_index := 1,
               last_applied := 1,
               current_term := 7,
               machine_state := <<"hi1">>}, _} =
        ra_node:handle_leader(Msg1, State#{current_term := 7}),
    ok.

append_entries_reply_no_success(_Config) ->
    % decrement next_index for peer if success = false
    Cluster = #{n1 => #{},
                n2 => #{next_index => 3, match_index => 0},
                n3 => #{next_index => 2, match_index => 1}},
    State = (base_state(3))#{commit_index => 1,
                             last_applied => 1,
                             cluster => Cluster,
                             machine_state => <<"hi1">>},
    % n2 has only seen index 1
    Msg = {n2, #append_entries_reply{term = 5, success = false,
                                     last_index = 1, last_term = 1}},
    AE = #append_entries_rpc{term = 5, leader_id = n1,
                             prev_log_index = 1,
                             prev_log_term = 1,
                             leader_commit = 1,
                             entries = [{2, 3, usr(<<"hi2">>)},
                                        {3, 5, usr(<<"hi3">>)}]},
    ExpectedActions = {send_append_entries, [{n3, AE}, {n2, AE}]},
    % new peers state is updated
    {leader, #{cluster := #{n2 := #{next_index := 2, match_index := 1}},
               commit_index := 1,
               last_applied := 1,
               machine_state := <<"hi1">>}, ExpectedActions} =
        ra_node:handle_leader(Msg, State),
    ok.

follower_vote(_Config) ->
    State = base_state(3),
    Msg = #request_vote_rpc{candidate_id = n2, term = 6, last_log_index = 3,
                            last_log_term = 5},
    % success
    {follower, #{voted_for := n2, current_term := 6} = State1,
     {reply, #request_vote_result{term = 6, vote_granted = true}}} =
     ra_node:handle_follower(Msg, State),

    % we can vote again for the same candidate and term
    {follower, #{voted_for := n2, current_term := 6},
     {reply, #request_vote_result{term = 6, vote_granted = true}}} =
     ra_node:handle_follower(Msg, State1),

    % but not for a different candidate
    {follower, #{voted_for := n2, current_term := 6},
     {reply, #request_vote_result{term = 6, vote_granted = false}}} =
     ra_node:handle_follower(Msg#request_vote_rpc{candidate_id = n3}, State1),

   % fail due to lower term
    {follower, #{current_term := 5},
     {reply, #request_vote_result{term = 5, vote_granted = false}}} =
     ra_node:handle_follower(Msg#request_vote_rpc{term = 4}, State),

     % Raft determines which of two logs is more up-to-date by comparing the
     % index and term of the last entries in the logs. If the logs have last
     % entries with different terms, then the log with the later term is more
     % up-to-date. If the logs end with the same term, then whichever log is
     % longer is more up-to-date. (§5.4.1)

     % reject when candidate last log entry has a lower term
     % still update current term if incoming is higher
    {follower, #{current_term := 6},
     {reply, #request_vote_result{term = 6, vote_granted = false}}} =
         ra_node:handle_follower(Msg#request_vote_rpc{last_log_term = 4},
                                 State),

    % grant vote when candidate last log entry has same term but is longer
    {follower, #{current_term := 6},
     {reply, #request_vote_result{term = 6, vote_granted = true}}} =
         ra_node:handle_follower(Msg#request_vote_rpc{last_log_index = 4},
                                 State),
     ok.

request_vote_rpc_with_lower_term(_Config) ->
    State = (base_state(3))#{current_term => 6,
                             voted_for => n1},
    Msg = #request_vote_rpc{candidate_id = n2, term = 5, last_log_index = 3,
                            last_log_term = 5},
    % term is lower than candidate term
    {candidate, #{voted_for := n1, current_term := 6},
     {reply, #request_vote_result{term = 6, vote_granted = false}}} =
     ra_node:handle_candidate(Msg, State),
    % term is lower than candidate term
    {leader, #{current_term := 6},
     {reply, #request_vote_result{term = 6, vote_granted = false}}} =
     ra_node:handle_leader(Msg, State).

leader_does_not_abdicate_to_unknown_peer(_Config) ->
    State = base_state(3),
    Vote = #request_vote_rpc{candidate_id = uknown_peer, term = 6,
                             last_log_index = 3,
                             last_log_term = 5},
    {leader, State, none} = ra_node:handle_leader(Vote, State),

    AEReply = {unknown_peer, #append_entries_reply{term = 6, success = false,
                                                   last_index = 3,
                                                   last_term = 5}},
    {leader, State, none} = ra_node:handle_leader(AEReply , State),
    ok.

higher_term_detected(_Config) ->
    % Any message received with a higher term should result in the
    % node reverting to follower state
    State = #{log := Log} = base_state(3),
    IncomingTerm = 6,
    AEReply = {n2, #append_entries_reply{term = IncomingTerm, success = false,
                                         last_index = 3, last_term = 5}},
    {ok, ExpectLog} = ra_log:write_meta(current_term, IncomingTerm, Log),
    {ok, ExpectLogWithVotedFor} = ra_log:write_meta(voted_for, undefined,
                                                    ExpectLog),
    {follower, #{current_term := IncomingTerm,
                 log := ExpectLog}, none} = ra_node:handle_leader(AEReply, State),
    {follower, #{current_term := IncomingTerm,
                 log := ExpectLog}, none} = ra_node:handle_follower(AEReply, State),
    {follower, #{current_term := IncomingTerm,
                 log := ExpectLogWithVotedFor}, none}
    = ra_node:handle_candidate(AEReply, State),

    AERpc = #append_entries_rpc{term = IncomingTerm, leader_id = n3,
                                prev_log_index = 3, prev_log_term = 5,
                                leader_commit = 3},
    {follower, #{current_term := IncomingTerm},
     {next_event, AERpc}} = ra_node:handle_leader(AERpc, State),
    {follower, #{current_term := IncomingTerm},
     {next_event, AERpc}} = ra_node:handle_candidate(AERpc, State),
    % follower will handle this properly and is tested elsewhere

    Vote = #request_vote_rpc{candidate_id = n2, term = 6, last_log_index = 3,
                             last_log_term = 5},
    {follower, #{current_term := IncomingTerm,
                 log := ExpectLog},
     {next_event, Vote}} = ra_node:handle_leader(Vote, State),
    {follower, #{current_term := IncomingTerm,
                 log := ExpectLogWithVotedFor},
     {next_event, Vote}} = ra_node:handle_candidate(Vote, State).

consistent_query(_Config) ->
    Cluster = #{n1 => #{},
                         n2 => #{next_index => 4, match_index => 3},
                         n3 => #{next_index => 4, match_index => 3}},
    State = (base_state(3))#{cluster => Cluster},
    {leader, State1, _} =
    ra_node:handle_leader({command, {'$ra_query', self(),
                                     fun id/1, await_consensus}}, State),
    AEReply = {n2, #append_entries_reply{term = 5, success = true,
                                         last_index = 4, last_term = 5}},
    {leader, _State2, Effects} = ra_node:handle_leader(AEReply, State1),
    ?assert(lists:any(fun({reply, _, {{4, 5}, <<"hi3">>}}) -> true;
                         (_) -> false
                      end, Effects)),
    ok.

leader_noop_operation_enables_cluster_change(_Config) ->
    State = (base_state(3))#{cluster_change_permitted => false},
    {leader, #{cluster_change_permitted := false} = State1, _} =
        ra_node:handle_leader({command, noop}, State),
    AEReply = {n2, #append_entries_reply{term = 5, success = true,
                                         last_index = 4, last_term = 5}},
    % noop consensus
    {leader, #{cluster_change_permitted := true}, _} =
        ra_node:handle_leader(AEReply, State1),
    ok.



leader_node_join(_Config) ->
    OldCluster = #{n1 => #{},
                   n2 => #{next_index => 4, match_index => 3},
                   n3 => #{next_index => 4, match_index => 3}},
    State = (base_state(3))#{cluster => OldCluster},
    NewCluster = #{n1 => #{},
                   n2 => #{next_index => 4, match_index => 3},
                   n3 => #{next_index => 4, match_index => 3},
                   n4 => #{next_index => 1, match_index => 0}},
    % raft nodes should switch to the new configuration after log append
    % and further cluster changes should be disallowed
    {leader, #{cluster := NewCluster,
               cluster_change_permitted := false}, Effects} =
        ra_node:handle_leader({command, {'$ra_join', self(),
                                         n4, await_consensus}}, State),
    JoinEntry = {4, 5, {'$ra_cluster_change', self(),
                        NewCluster, await_consensus}},
    AE = #append_entries_rpc{term = 5, leader_id = n1,
                             prev_log_index = 3,
                             prev_log_term = 5,
                             leader_commit = 3,
                             entries = [JoinEntry]},
    [{send_append_entries, [{n4, #append_entries_rpc{entries =
                                                     [_, _, _, JoinEntry]}},
                            {n3, AE}, {n2, AE}]}] = Effects,
    ok.

leader_node_leave(_Config) ->
    OldCluster = #{n1 => #{},
                   n2 => #{next_index => 4, match_index => 3},
                   n3 => #{next_index => 4, match_index => 3},
                   n4 => #{next_index => 1, match_index => 0}},
    State = (base_state(3))#{cluster => OldCluster},
    NewCluster = #{n1 => #{},
                   n2 => #{next_index => 4, match_index => 3},
                   n3 => #{next_index => 4, match_index => 3}},
    % raft nodes should switch to the new configuration after log append
    {leader, #{cluster := NewCluster}, Effects} =
        ra_node:handle_leader({command, {'$ra_leave', self(), n4, await_consensus}},
                              State),
    JoinEntry = {4, 5, {'$ra_cluster_change', self(), NewCluster, await_consensus}},
    AE = #append_entries_rpc{term = 5, leader_id = n1,
                             prev_log_index = 3,
                             prev_log_term = 5,
                             leader_commit = 3,
                             entries = [JoinEntry]},
    % the leaving node is no longer included
    [{send_append_entries, [{n3, AE}, {n2, AE}]}] = Effects,
    ok.

leader_is_removed(_Config) ->
    OldCluster = #{n1 => #{},
                   n2 => #{next_index => 4, match_index => 3},
                   n3 => #{next_index => 4, match_index => 3},
                   n4 => #{next_index => 1, match_index => 0}},
    State = (base_state(3))#{cluster => OldCluster},

    {leader, State1, _} =
        ra_node:handle_leader({command, {'$ra_leave', self(), n1, await_consensus}},
                              State),

    % replies coming in
    AEReply = #append_entries_reply{term = 5, success = true,
                                    last_index = 4, last_term = 5},
    {leader, State2, _} = ra_node:handle_leader({n2, AEReply}, State1),
    % after committing the new entry the leader steps down
    {stop, #{commit_index := 4}, _} = ra_node:handle_leader({n3, AEReply}, State2),
    ok.

follower_cluster_change(_Config) ->
    OldCluster = #{n1 => #{},
                   n2 => #{next_index => 4, match_index => 3},
                   n3 => #{next_index => 4, match_index => 3}},
    State = (base_state(3))#{id => n2,
                             cluster => OldCluster},
    NewCluster = #{n1 => #{},
                   n2 => #{next_index => 4, match_index => 3},
                   n3 => #{next_index => 4, match_index => 3},
                   n4 => #{next_index => 1}},
    JoinEntry = {4, 5, {'$ra_cluster_change', self(), NewCluster, await_consensus}},
    AE = #append_entries_rpc{term = 5, leader_id = n1,
                             prev_log_index = 3,
                             prev_log_term = 5,
                             leader_commit = 3,
                             entries = [JoinEntry]},
    {follower, #{cluster := NewCluster,
                 cluster_index_term := {4, 5}},
     {reply, #append_entries_reply{}}} = ra_node:handle_follower(AE, State),
    ok.

leader_applies_new_cluster(_Config) ->
    OldCluster = #{n1 => #{},
                   n2 => #{next_index => 4, match_index => 3},
                   n3 => #{next_index => 4, match_index => 3}},
    NewCluster = #{n1 => #{},
                   n2 => #{next_index => 4, match_index => 3},
                   n3 => #{next_index => 4, match_index => 3},
                   n4 => #{next_index => 1, match_index => 0}},

    State = (base_state(3))#{id => n1, cluster => OldCluster},
    Command = {command, {'$ra_join', self(), n4, await_consensus}},
    % cluster records index and term it was applied to determine whether it has
    % been applied
    {leader, #{cluster_index_term := {4, 5},
               cluster := NewCluster} = State1, _} =
        ra_node:handle_leader(Command, State),

    Command2 = {command, {'$ra_join', self(), n5, await_consensus}},
    % additional cluster change commands are not applied whilst
    % cluster change is being committed
    {leader, #{cluster_index_term := {4, 5},
               cluster := NewCluster,
               pending_cluster_changes := [_]} = State2, _} =
        ra_node:handle_leader(Command2, State1),


    % replies coming in
    AEReply = #append_entries_reply{term = 5, success = true,
                                    last_index = 4, last_term = 5},
    % leader does not yet have consensus as will need at least 3 votes
    {leader, State3 = #{commit_index := 3,
                        cluster_index_term := {4, 5},
                        cluster := #{n2 := #{next_index := 5,
                                             match_index := 4}}},
     _} = ra_node:handle_leader({n2, AEReply}, State2#{votes => 1}),

    % leader has consensus
    {leader, _State4 = #{commit_index := 4,
                         cluster := #{n3 := #{next_index := 5,
                                              match_index := 4}}},
     Effects} = ra_node:handle_leader({n3, AEReply}, State3),

    % the pending cluster change can now be processed as the
    % next event
    ?assert(lists:any(fun({next_event, {call, _}, {command, _} = C2}) ->
                              C2 =:= Command2;
                         (_) -> false
                      end, Effects)),

    ok.

leader_appends_cluster_change_then_steps_before_applying_it(_Config) ->
    OldCluster = #{n1 => #{},
                   n2 => #{next_index => 4, match_index => 3},
                   n3 => #{next_index => 4, match_index => 3}},
    NewCluster = #{n1 => #{},
                   n2 => #{next_index => 4, match_index => 3},
                   n3 => #{next_index => 4, match_index => 3},
                   n4 => #{next_index => 1, match_index => 0}},

    State = (base_state(3))#{id => n1, cluster => OldCluster},
    Command = {command, {'$ra_join', self(), n4, await_consensus}},
    % cluster records index and term it was applied to determine whether it has
    % been applied
    {leader, #{cluster_index_term := {4, 5},
               cluster := NewCluster} = State1, _} = ra_node:handle_leader(
                                                       Command, State),

    % leader has committed the entry but n2 and n3 have not yet seen it and
    % n2 has been elected leader and is replicating a different entry for
    % index 4 with a higher term
    AE = #append_entries_rpc{entries = [{4, 6, noop}],
                             leader_id = n2,
                             term = 6,
                             prev_log_index = 3,
                             prev_log_term = 5,
                             leader_commit = 3},
    {follower, #{cluster := Cluster}, _} = ra_node:handle_follower(AE, State1),
    % assert n1 has switched back to the old cluster config
    #{n1 := _, n2 := _, n3 := _} = Cluster,
    3 = maps:size(Cluster),
    ok.


command(_Config) ->
    State = base_state(3),
    Self = self(),
    Cmd = {'$usr', Self, <<"hi4">>, after_log_append},
    AE = #append_entries_rpc{entries = [{4, 5, Cmd}],
                             leader_id = n1,
                             term = 5,
                             prev_log_index = 3,
                             prev_log_term = 5,
                             leader_commit = 3
                            },
    {leader, _, [{reply, Self, {4, 5}}, {send_append_entries,
                                         [{n3, AE}, {n2, AE}]}]} =
        ra_node:handle_leader({command, Cmd}, State),
    ok.

quorum(_Config) ->
    State = (base_state(5))#{current_term => 6, votes => 1},
    Reply = #request_vote_result{term = 6, vote_granted = true},
    {candidate, #{votes := 2} = State1, none}
        = ra_node:handle_candidate(Reply, State),
    % denied
    NegResult = #request_vote_result{term = 6, vote_granted = false},
    {candidate, #{votes := 2}, none}
        = ra_node:handle_candidate(NegResult, State1),
    % newer term should make candidate stand down
    HighTermResult = #request_vote_result{term = 7, vote_granted = false},
    {follower, #{current_term := 7}, none}
        = ra_node:handle_candidate(HighTermResult, State1),

    % quorum has been achieved - candidate becomes leader
    PeerState = #{next_index => 3+1, % leaders last log index + 1
                  match_index => 0}, % initd to 0

    % when candidate becomes leader the next operation should be a noop
    {leader, #{cluster := #{n1 := #{next_index := 4},
                            n2 := PeerState,
                            n3 := PeerState,
                            n4 := PeerState,
                            n5 := PeerState}},
     [{next_event, cast, {command, noop}}]}
        = ra_node:handle_candidate(Reply, State1).

%%% helpers

base_state(NumNodes) ->
    Log = {3, #{0 => {0, undefined},
                1 => {1, usr(<<"hi1">>)},
                2 => {3, usr(<<"hi2">>)},
                3 => {5, usr(<<"hi3">>)}},
           #{}},
    Nodes = lists:foldl(fun(N, Acc) ->
                                Name = list_to_atom("n" ++ integer_to_list(N)),
                                Acc#{Name => #{next_index => 4,
                                               match_index => 3}}
                        end, #{}, lists:seq(1, NumNodes)),
    #{id => n1,
      cluster => Nodes,
      cluster_index_term => {0, 0},
      cluster_change_permitted => true,
      pending_cluster_changes => [],
      current_term => 5,
      commit_index => 3,
      last_applied => 3,
      machine_apply_fun => fun (_, E, _) -> E end, % just keep last applied value
      machine_state => <<"hi3">>, % last entry has been applied
      log => {ra_log_memory, Log}}.

usr(Data) ->
    {'$usr', self(), Data, after_log_append}.
