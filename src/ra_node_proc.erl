-module(ra_node_proc).

-behaviour(gen_statem).

-include("ra.hrl").

%% API
-export([start_link/1,
         command/3
        ]).

%% State functions
-export([leader/3,
         follower/3,
         candidate/3]).

%% gen_statem callbacks
-export([
         init/1,
         format_status/2,
         handle_event/4,
         terminate/3,
         code_change/4,
         callback_mode/0
        ]).

-define(SERVER, ?MODULE).
-define(TEST_LOG, ra_test_log).
-define(DEFAULT_BROADCAST_TIME, 100).

-type server_ref() :: pid() | atom() | {node() | atom()}.

-export_type([server_ref/0]).

-record(state, {node_state :: ra_node:ra_node_state(),
                broadcast_time :: non_neg_integer(),
                proxy :: maybe(pid()),
                pending_replies = [] :: [{{ra_index(), ra_term()}, term()}],
                pending_commands = [] :: [{{pid(), any()}, term()}]}).

%%%===================================================================
%%% API
%%%===================================================================

start_link(Config = #{id := Id}) ->
    gen_statem:start_link({local, Id}, ?MODULE, [Config], []).

-spec command(ra_node_proc:server_ref(), term(), no_wait | await_consensus) ->
    {ok, IdxTerm::{ra_index(), ra_term()}, Leader::ra_node_proc:server_ref()}
    | {error, term()}.
command(ServerRef, Data, Flag) ->
    % TODO: use dirty timeouts
    case gen_statem:call(ServerRef, {command, Data, Flag}) of
        {redirect, Leader} ->
            command(Leader, Data, Flag);
        {error, _} = E -> E;
        Reply -> {ok, Reply, ServerRef}
    end.

%%%===================================================================
%%% gen_statem callbacks
%%%===================================================================

init([Config]) ->
    State = #state{node_state = ra_node:init(Config),
                   broadcast_time = ?DEFAULT_BROADCAST_TIME},
    ?DBG("init state ~p~n", [State]),
    {ok, follower, State, election_timeout_action(State)}.

%% callback mode
callback_mode() -> state_functions.

%% state functions
leader({call, From}, {command, Data, no_wait},
       State0 = #state{node_state = NodeState0}) ->
    % Persist command into log
    % Return raft index + term to caller so they can wait for apply notifications
    % Send msg to peer proxy with updated state data (so they can replicate)
    {leader, NodeState, Actions} = ra_node:handle_leader({command, Data},
                                                         NodeState0),
    State = interact(Actions, From, State0),
    {keep_state, State#state{node_state = NodeState}};
leader({call, From}, {command, Data, await_consensus},
       State0 = #state{node_state = NodeState0,
                       pending_replies = PendingReplies0}) ->
    {leader, NodeState, [{reply, IdxTerm} | Actions]} =
     ra_node:handle_leader({command, Data}, NodeState0),
    State = interact(Actions, From, State0),
    PendingReplies = [{IdxTerm, From} | PendingReplies0],
    {keep_state, State#state{node_state = NodeState,
                             pending_replies = PendingReplies}};
leader(EventType, Msg,
       State0 = #state{node_state = NodeState0 = #{id := Id}}) ->
    From = get_from(EventType),
    case ra_node:handle_leader(Msg, NodeState0) of
        {leader, NodeState, Actions} ->
            State = interact(Actions, From, State0),
            State1 = State#state{node_state = NodeState},
            {State2, ReplyActions} = make_caller_reply_actions(State1),
            {keep_state, State2, ReplyActions};
        {follower, NodeState, Actions} ->
            ?DBG("~p leader abdicates!~n", [Id]),
            % TODO kill proxy process
            State = interact(Actions, From, State0),
            {next_state, follower, State#state{node_state = NodeState},
             [election_timeout_action(State)]}
    end.

candidate({call, From}, {command, _Data, _Flag},
          State = #state{node_state = #{leader_id := LeaderId}}) ->
    {keep_state, State, {reply, From, {redirect, LeaderId}}};
candidate({call, From}, {command, _Data, _Flag} = Cmd,
          State = #state{pending_commands = Pending}) ->
    % stash commands until a leader is known
    {keep_state, State#state{pending_commands = [{From, Cmd} | Pending]}};
candidate(EventType, Msg, State0 = #state{node_state = NodeState0 = #{id := Id},
                                          pending_commands = Pending}) ->
    ?DBG("~p candidate: ~p~n", [Id, Msg]),
    From = get_from(EventType),
    case ra_node:handle_candidate(Msg, NodeState0) of
        {candidate, NodeState, Actions} ->
            State = interact(Actions, From, State0),
            {keep_state, State#state{node_state = NodeState},
             election_timeout_action(State)};
        {follower, NodeState, Actions} ->
            State = interact(Actions, From, State0),
            {next_state, follower, State#state{node_state = NodeState},
             election_timeout_action(State)};
        {leader, NodeState, Actions} ->
            State = interact(Actions, From, State0),
            ?DBG("~p next leader~n", [Id]),
            % inject a bunch of command events to be processed when node
            % becomes leader
            NextEvents = [{next_event, {call, F}, Cmd} || {F, Cmd} <- Pending],
            {next_state, leader, State#state{node_state = NodeState}, NextEvents}
    end.

follower({call, From}, {command, _Data, _Flag},
         State = #state{node_state = #{leader_id := LeaderId}}) ->
    {keep_state, State, {reply, From, {redirect, LeaderId}}};
follower({call, From}, {command, _Data, _Flag} = Cmd,
         State = #state{pending_commands = Pending}) ->
    {keep_state, State#state{pending_commands = [{From, Cmd} | Pending]}};
follower(EventType, Msg,
         State0 = #state{node_state = NodeState0 = #{id := Id}}) ->
    ?DBG("~p follower: ~p~n", [Id, Msg]),
    From = get_from(EventType),
    case ra_node:handle_follower(Msg, NodeState0) of
        {follower, NodeState, Actions} ->
            State = interact(Actions, From, State0),
            NewState = follower_leader_change(State,
                                              State#state{node_state = NodeState}),
            {keep_state, NewState, election_timeout_action(State)};
        {candidate, NodeState, Actions} ->
            State = interact(Actions, From, State0),
            ?DBG("~p next candidate: ~p ~p~n", [Id, Actions, NodeState]),
            {next_state, candidate, State#state{node_state = NodeState},
             election_timeout_action(State)}
    end.


handle_event(_EventType, EventContent, StateName, State) ->
    ?DBG("handle_event unknownn ~p~n", [EventContent]),
    {next_state, StateName, State}.

terminate(Reason, _StateName, #state{proxy = undefined}) ->
    ?DBG("ra terminating with ~p~n", [Reason]),
    ok;
terminate(Reason, _StateName, #state{proxy = Proxy}) ->
    ?DBG("ra terminating with ~p~n", [Reason]),
    _ = gen_server:stop(Proxy, Reason, 100),
    ok.

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

format_status(_Opt, [_PDict, _StateName, _State]) ->
    Status = some_term,
    Status.

%%%===================================================================
%%% Internal functions
%%%===================================================================

make_caller_reply_actions(State = #state{pending_replies = []}) ->
    {State, []};
make_caller_reply_actions(State = #state{pending_replies = Pending,
                                node_state = #{commit_index := CommitIndex,
                                               log := Log, log_mod := Mod}}) ->
    % There are code paths here that are difficult to test externally
    % TODO: add internal unit tests
    {Replies, Pending1} =
    lists:foldl(fun({{Idx, Term} = IdxTerm, From}, {Act, Rem})
                      when Idx =< CommitIndex ->
                        Reply = case Mod:fetch(Idx, Log) of
                                    undefined ->
                                        % should never happen so exit
                                        exit(corrupted_log);
                                    {Idx, Term, _} ->
                                        % the term of the index is correct
                                        IdxTerm;
                                    {Idx, OthTerm, _} ->
                                        % can this happen given the pending
                                        % callers are only stored for the leader
                                        {error, {entry_term_mismatch, Term,
                                                 OthTerm}}
                                end,
                        {[{reply, From, Reply} | Act], Rem};
                   (_, {Act, Rem}) ->
                        {Act, Rem}
                end, {[], []}, Pending),
    {State#state{pending_replies = Pending1}, Replies}.

interact(none, _From, State) ->
    State;
interact({send_msg, To, Msg}, _From, State) ->
    To ! Msg,
    State;
interact({reply, _Reply}, undefined, _State) ->
    exit(undefined_reply);
interact({reply, Reply}, From, State) ->
    ok = gen_statem:reply(From, Reply),
    State;
interact({send_vote_requests, VoteRequests}, _From, State) ->
    % transient election processes
    T = {dirty_timeout, 500},
    Me = self(),
    [begin
         _ = spawn(fun () -> Reply = gen_statem:call(N, M, T),
                             ok = gen_statem:cast(Me, Reply)
                   end)
     end || {N, M} <- VoteRequests],
    State;
interact({send_append_entries, AppendEntries}, _From,
         #state{proxy = undefined, broadcast_time = Interval} = State) ->
    ?DBG("Appends Entries ~p ~n", [AppendEntries]),
    {ok, Proxy} = ra_proxy:start_link(self(), Interval),
    ok = ra_proxy:proxy(Proxy, AppendEntries),
    State#state{proxy = Proxy};
interact({send_append_entries, AppendEntries}, _From,
         #state{proxy = Proxy} = State) ->
    ok = ra_proxy:proxy(Proxy, AppendEntries),
    State;
interact([Action | Actions], From, State0) ->
    State = interact(Action, From, State0),
    interact(Actions, From, State);
interact([], _From, State) -> State.


get_from({call, From}) -> From;
get_from(_) -> undefined.

election_timeout_action(#state{broadcast_time = Timeout}) ->
    T = rand:uniform(Timeout * 3) + (Timeout * 2),
    ?DBG("T: ~p~n", [T]),
    {timeout, T, election_timeout}.

follower_leader_change(#state{node_state = #{leader_id := L}},
                     #state{node_state = #{leader_id := L}} = New) ->
    % no change
    New;
follower_leader_change(_Old, #state{node_state = #{id := Id, leader_id := L},
                                    pending_commands = Pending} = New)
  when L /= undefined ->
    % leader has either changed or just been set
    ?DBG("~p A new leader has been detected: ~p~n", [Id, L]),
    [ok = gen_statem:reply(From, {redirect, L})
     || {From, _Data} <- Pending],
    New#state{pending_commands = []};
follower_leader_change(_Old, New) -> New.

