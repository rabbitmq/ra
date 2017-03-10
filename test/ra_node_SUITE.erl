-module(ra_node_SUITE).

-compile(export_all).

-include("ra.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

all() ->
    [
     election_timeout,
     quorum,
     follower_vote,
     follower_append_entries,
     command,
     append_entries_reply_success,
     append_entries_reply_no_success,
     node_steps_down,
     consistent_query
    ].

groups() ->
    [ {tests, [], all()} ].

id(X) -> X.

consistent_query(_Config) ->
    Cluster = {normal, #{n1 => #{},
                         n2 => #{next_index => 4, match_index => 3},
                         n3 => #{next_index => 4, match_index => 3}}},
    State = (base_state(3))#{cluster => Cluster},
    {leader, State1, _} =
    ra_node:handle_leader({command, {'$ra_query', self(), fun id/1, await_consensus}}, State),
    AEReply = {n2, #append_entries_reply{term = 5, success = true,
                                         last_index = 4, last_term = 5}},
    {leader, _State2, Effects} = ra_node:handle_leader(AEReply, State1),
    ct:pal("Effects ~p~n", [Effects]),
    ?assert(lists:any(fun({reply, _, {{4, 5}, <<"hi3">>}}) -> true;
                         (_) -> false
                      end, Effects)),
    ok.

    % ct:pal("~p ~p~n", [State2, MacState]).


node_steps_down(_Config) ->
    State = base_state(3),
    % Any message received with a higher term should result in the
    % node reverting to follower state
    IncomingTerm = 6,
    AEReply = {n2, #append_entries_reply{term = IncomingTerm, success = false,
                                         last_index = 3, last_term = 5}},
    AEReplyExpect = {follower, State#{current_term := IncomingTerm}, none},
    AEReplyExpect = ra_node:handle_leader(AEReply, State),
    AEReplyExpect = ra_node:handle_candidate(AEReply, State),
    AEReplyExpect = ra_node:handle_follower(AEReply, State),

    AERpc = #append_entries_rpc{term = IncomingTerm, leader_id = n3,
                                prev_log_index = 3, prev_log_term = 5,
                                leader_commit = 3},
    AERpcExpect = {follower, State#{current_term := IncomingTerm}, {next_event, AERpc}},
    AERpcExpect = ra_node:handle_leader(AERpc, State),
    AERpcExpect = ra_node:handle_candidate(AERpc, State),
    % follower will handle this properly and is tested elsewhere

    Vote = #request_vote_rpc{candidate_id = n2, term = 6, last_log_index = 3,
                             last_log_term = 5},
    VoteExpect = {follower, State#{current_term := IncomingTerm}, {next_event, Vote}},
    VoteExpect = ra_node:handle_leader(Vote, State),
    VoteExpect = ra_node:handle_candidate(Vote, State).



append_entries_reply_success(_Config) ->
    Cluster = #{n1 => #{},
                n2 => #{next_index => 1, match_index => 0},
                n3 => #{next_index => 2, match_index => 1}},
    State = (base_state(3))#{commit_index => 1,
                             last_applied => 1,
                             cluster => {normal, Cluster},
                             machine_state => <<"hi1">>},
    Msg = {n2, #append_entries_reply{term = 5, success = true,
                                     last_index = 3, last_term = 5}},
    ExpectedActions =
    {send_append_entries,
     [{n2, #append_entries_rpc{term = 5, leader_id = n1,
                               prev_log_index = 3,
                               prev_log_term = 5,
                               leader_commit = 3}},
      {n3, #append_entries_rpc{term = 5, leader_id = n1,
                               prev_log_index = 1,
                               prev_log_term = 1,
                               leader_commit = 3,
                               entries = [{2, 3, usr(<<"hi2">>)},
                                          {3, 5, usr(<<"hi3">>)}]}}]},
    % update match index
    {leader, #{cluster := {normal, #{n2 := #{next_index := 4,
                                             match_index := 3}}},
               commit_index := 3,
               last_applied := 3,
               machine_state := <<"hi3">>}, ExpectedActions} =
        ra_node:handle_leader(Msg, State),

    % TODO: do not increment commit index when
    % the term of the log entry /= current_term (§5.3, §5.4)
    Msg1 = {n2, #append_entries_reply{term = 7, success = true,
                                      last_index = 3, last_term = 5}},
    {leader, #{cluster := {normal, #{n2 := #{next_index := 4,
                                             match_index := 3}}},
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
                             cluster => {normal, Cluster},
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
    ExpectedActions = {send_append_entries, [{n2, AE}, {n3, AE}]},
    % new peers state is updated
    {leader, #{cluster := {normal, #{n2 := #{next_index := 2,
                                             match_index := 1}}},
               commit_index := 1,
               last_applied := 1,
               machine_state := <<"hi1">>}, ExpectedActions} =
        ra_node:handle_leader(Msg, State),
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
    {leader, _, [{reply, Self, {4, 5}}, {send_append_entries, [{n2, AE}, {n3, AE}]}]} =
        ra_node:handle_leader({command, Cmd}, State),
    ok.

follower_append_entries(_Config) ->
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
        = ra_node:handle_follower(EmptyAE#append_entries_rpc{prev_log_index = 4}, State),
    {follower, _, {reply, #append_entries_reply{term = 5, success = false}}}
        = ra_node:handle_follower(EmptyAE#append_entries_rpc{prev_log_term = 4}, State),

    % truncate/overwrite if a existing entry conflicts (diff term) with a new one (5.3)
    AE = #append_entries_rpc{term = 5, leader_id = n1,
                             prev_log_index = 1, prev_log_term = 1,
                             leader_commit = 2,
                             entries = [{2, 4, {'$usr', Self, <<"hi">>, after_log_append}}]},

    ExpectedLog = {2, #{1 => {1, usr(<<"hi1">>)}, 2 => {4, usr(<<"hi">>)}}},
    {follower,  #{log := {ra_test_log, ExpectedLog}},
     {reply, #append_entries_reply{term = 5, success = true,
                                   last_index = 2, last_term = 4}}}
        = ra_node:handle_follower(AE, State#{last_applied => 1}),

    % append new entries not in the log
    % if leader_commit > commit_index set commit_index = min(leader_commit, index of last new entry)
    ExpectedLogEntry = usr(<<"hi4">>),
    {follower, #{log := {ra_test_log, {4, #{4 := {5, ExpectedLogEntry}}}},
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
    AE = #append_entries_rpc{term = 6,
                             leader_id = n1,
                             prev_log_index = 3,
                             prev_log_term = 5,
                             entries = [],
                             leader_commit = 3
                             },
    PeerState = #{next_index => 3+1, % leaders last log index + 1
                  match_index => 0}, % initd to 0

    Cluster = {normal, #{n1 => #{next_index => 4},
                         n2 => PeerState,
                         n3 => PeerState,
                         n4 => PeerState,
                         n5 => PeerState}},
    {leader, #{cluster := Cluster},
     {send_append_entries, [{n2, AE} | _ ]}}
               = ra_node:handle_candidate(Reply, State1).

election_timeout(_Config) ->
    State = base_state(3),
    Msg = election_timeout,
    VoteRpc = #request_vote_rpc{term = 6, candidate_id = n1, last_log_index = 3,
                                last_log_term = 5},
    {candidate, #{current_term := 6, votes := 1},
     {send_vote_requests, [{n2, VoteRpc}, {n3, VoteRpc}]}} = ra_node:handle_follower(Msg, State),
    {candidate, #{current_term := 6, votes := 1},
     {send_vote_requests, [{n2, VoteRpc}, {n3, VoteRpc}]}} = ra_node:handle_candidate(Msg, State),
    ok.

base_state(NumNodes) ->
    Log = {3, #{1 => {1, usr(<<"hi1">>)},
                2 => {3, usr(<<"hi2">>)},
                3 => {5, usr(<<"hi3">>)}}},
    Nodes = lists:foldl(fun(N, Acc) ->
                                Name = list_to_atom("n" ++ integer_to_list(N)),
                                Acc#{Name => #{next_index => 4}}
                        end, #{}, lists:seq(1, NumNodes)),
    #{id => n1,
      cluster => {normal, Nodes},
      current_term => 5,
      commit_index => 3,
      last_applied => 3,
      machine_apply_fun => fun (E, _) -> E end, % just keep last applied value
      machine_state => <<"hi3">>, % last entry has been applied
      log => {ra_test_log, Log}}.

usr(Data) ->
    {'$usr', self(), Data, after_log_append}.
