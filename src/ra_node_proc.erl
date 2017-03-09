-module(ra_node_proc).

-behaviour(gen_statem).

-include("ra.hrl").

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

%% API
-export([start_link/1,
         command/4,
         query/3
        ]).

-define(SERVER, ?MODULE).
-define(TEST_LOG, ra_test_log).
-define(DEFAULT_BROADCAST_TIME, 100).

-type server_ref() :: pid() | atom() | {Name::atom(), node()}.

-type command_reply_mode() :: after_log_append | await_consensus
                              | notify_on_consensus.

-type pending_reply() :: {reply | notify, {ra_index(), ra_term()},
                          From::term()}.

-type query_fun() :: fun((term()) -> term()).

-export_type([server_ref/0]).

-record(state, {node_state :: ra_node:ra_node_state(),
                broadcast_time :: non_neg_integer(),
                proxy :: maybe(pid()),
                pending_replies = [] :: [pending_reply()],
                pending_commands = [] :: [{{pid(), any()}, term()}]}).

%%%===================================================================
%%% API
%%%===================================================================

start_link(Config = #{id := Id}) ->
    Name = server_ref_to_local_name(Id),
    gen_statem:start_link({local, Name}, ?MODULE, [Config], []).

-spec command(ra_node_proc:server_ref(), term(), command_reply_mode(),
              timeout()) ->
    {ok, IdxTerm::{ra_index(), ra_term()}, Leader::ra_node_proc:server_ref()}
    | {error, term()}.
command(ServerRef, Data, ReplyMode, Timeout) ->
    case gen_statem_safe_call(ServerRef, {command, Data, ReplyMode},
                              {dirty_timeout, Timeout}) of
        {redirect, Leader} ->
            command(Leader, Data, ReplyMode, Timeout);
        {error, _} = E ->
            E;
        timeout ->
            % TODO: formatted error message
            {timeout, ServerRef};
        Reply ->
            {ok, Reply, ServerRef}
    end.

gen_statem_safe_call(ServerRef, Msg, Timeout) ->
    try
        gen_statem:call(ServerRef, Msg, Timeout)
    catch
         exit:{timeout, _} ->
            timeout;
         exit:{no_proc, _} ->
            {error, no_proc}
    end.

-spec query(ra_node_proc:server_ref(), query_fun(), dirty | consistent) ->
    {ok, IdxTerm::{ra_index(), ra_term()}, term()}.
query(ServerRef, QueryFun, QueryMode) ->
    gen_statem:call(ServerRef, {query, QueryFun, QueryMode}).


%%%===================================================================
%%% gen_statem callbacks
%%%===================================================================

init([Config]) ->
    process_flag(trap_exit, true),
    State = #state{node_state = ra_node:init(Config),
                   broadcast_time = ?DEFAULT_BROADCAST_TIME},
    ?DBG("init state ~p~n", [State]),
    {ok, follower, State, election_timeout_action(State)}.

%% callback mode
callback_mode() -> state_functions.

%% state functions
leader({call, From} = EventType, {command, Data, await_consensus},
       State0 = #state{node_state = NodeState0,
                       pending_replies = PendingReplies0}) ->
    {leader, NodeState, [{reply, IdxTerm} | Effects]} =
        ra_node:handle_leader({command, Data}, NodeState0),
    % State = interact(Actions, EventType, State0),
    {State, Actions} = interact(Effects, EventType, State0),
    PendingReplies = [{reply, IdxTerm, From} | PendingReplies0],
    {keep_state, State#state{node_state = NodeState,
                             pending_replies = PendingReplies}, Actions};
leader({call, From} = EventType, {command, Data, ReplyMode},
       State0 = #state{node_state = NodeState0,
                       pending_replies = PendingReplies0}) ->
    %% Persist command into log
    %% Return raft index + term to caller so they can wait for apply
    %% notifications
    %% Send msg to peer proxy with updated state data
    %% (so they can replicate)
    {leader, NodeState, [{reply, IdxTerm} | _] = Effects} =
        ra_node:handle_leader({command, Data}, NodeState0),
    PendingReplies = case ReplyMode of
                         notify_on_consensus ->
                             [{notify, IdxTerm, From} | PendingReplies0];
                         after_log_append ->
                             PendingReplies0
                     end,

    {State, Actions} = interact(Effects, EventType, State0),
    % State = interact(Actions, EventType, State0),
    {keep_state, State#state{node_state = NodeState,
                             pending_replies = PendingReplies}, Actions};
leader({call, From}, {query, QueryFun, dirty},
         State = #state{node_state = NodeState}) ->
    Reply = perform_query(QueryFun, dirty, NodeState),
    {keep_state, State, [{reply, From, Reply}]};
leader(_EventType, {'EXIT', Proxy0, Reason},
       State0 = #state{proxy = Proxy0,
                       broadcast_time = Interval,
                       node_state = NodeState = #{id := Id}}) ->
    ?DBG("~p leader proxy exited with ~p~nrestarting..~n", [Id, Reason]),
    % TODO: this is a bit hacky - refactor
    AppendEntries = ra_node:append_entries(NodeState),
    {ok, Proxy} = ra_proxy:start_link(self(), Interval),
    ok = ra_proxy:proxy(Proxy, AppendEntries),
    {keep_state, State0#state{proxy = Proxy}};
leader(EventType, Msg,
       State0 = #state{node_state = NodeState0 = #{id := Id,
                                                   current_term := Term},
                       proxy = Proxy}) ->
    case ra_node:handle_leader(Msg, NodeState0) of
        {leader, NodeState, Effects} ->
            % State = interact(Actions, EventType, State0),
            {State, Actions} = interact(Effects, EventType, State0),
            State1 = State#state{node_state = NodeState},
            {State2, ReplyActions, Notifications} =
                make_caller_reply_actions(State1),
                _ = [FromPid ! {consensus, Reply}
                     || {FromPid, Reply} <- Notifications],
            {keep_state, State2, ReplyActions ++ Actions};
        {follower, NodeState, Effects} ->
            ?DBG("~p leader abdicates term: ~p!~n", [Id, Term]),
            % stop proxy process - TODO: would this be safer sync?
            _ = spawn(fun () ->
                              _ = gen_server:stop(Proxy, normal, 100)
                      end),
            {State, Actions} = interact(Effects, EventType, State0),
            % State = interact(Actions, EventType, State0),
            {next_state, follower, State#state{node_state = NodeState},
             [election_timeout_action(State) | Actions]}
    end.

candidate({call, From}, {command, _Data, _Flag},
          State = #state{node_state = #{leader_id := LeaderId}}) ->
    {keep_state, State, {reply, From, {redirect, LeaderId}}};
candidate({call, From}, {command, _Data, _Flag} = Cmd,
          State = #state{pending_commands = Pending}) ->
    % stash commands until a leader is known
    {keep_state, State#state{pending_commands = [{From, Cmd} | Pending]}};
candidate({call, From}, {query, QueryFun, dirty},
         State = #state{node_state = NodeState}) ->
    Reply = perform_query(QueryFun, dirty, NodeState),
    {keep_state, State, [{reply, From, Reply}]};
candidate(EventType, Msg, State0 = #state{node_state = NodeState0
                                          = #{id := Id,
                                              current_term := Term},
                                          pending_commands = Pending}) ->
    case ra_node:handle_candidate(Msg, NodeState0) of
        {candidate, NodeState, Effects} ->
            {State, Actions} = interact(Effects, EventType, State0),
            {keep_state, State#state{node_state = NodeState},
             [election_timeout_action(State) | Actions]};
        {follower, NodeState, Effects} ->
            ?DBG("~p candidate -> follower term: ~p~n", [Id, Term]),
            {State, Actions} = interact(Effects, EventType, State0),
            {next_state, follower, State#state{node_state = NodeState},
             [election_timeout_action(State) | Actions]};
        {leader, NodeState, Effects} ->
            {State, Actions} = interact(Effects, EventType, State0),
            ?DBG("~p candidate -> leader term: ~p~n", [Id, Term]),
            % inject a bunch of command events to be processed when node
            % becomes leader
            NextEvents = [{next_event, {call, F}, Cmd} || {F, Cmd} <- Pending],
            {next_state, leader,
             State#state{node_state = NodeState}, NextEvents ++ Actions}
    end.

follower({call, From}, {command, _Data, _Flag},
         State = #state{node_state = #{leader_id := LeaderId}}) ->
    {keep_state, State, {reply, From, {redirect, LeaderId}}};
follower({call, From}, {command, _Data, _Flag} = Cmd,
         State = #state{pending_commands = Pending}) ->
    {keep_state, State#state{pending_commands = [{From, Cmd} | Pending]}};
follower({call, From}, {query, QueryFun, dirty},
         State = #state{node_state = NodeState}) ->
    Reply = perform_query(QueryFun, dirty, NodeState),
    {keep_state, State, [{reply, From, Reply}]};
follower(EventType, Msg,
         State0 = #state{node_state = NodeState0 =
                         #{id := Id, current_term := CurTerm}}) ->
    case ra_node:handle_follower(Msg, NodeState0) of
        {follower, NodeState, Effects} ->
            {State, Actions} = interact(Effects, EventType, State0),
            NewState = follower_leader_change(State,
                                              State#state{node_state = NodeState}),
            {keep_state, NewState,
             [election_timeout_action(State) | Actions]};
        {candidate, NodeState, Effects} ->
            {State, Actions} = interact(Effects, EventType, State0),
            ?DBG("~p follower -> candidate term: ~p~n", [Id, CurTerm]),
            {next_state, candidate, State#state{node_state = NodeState},
             [election_timeout_action(State) | Actions]}
    end.

handle_event(_EventType, EventContent, StateName, State) ->
    ?DBG("handle_event unknown ~p~n", [EventContent]),
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

perform_query(QueryFun, dirty, #{machine_state := MacState,
                                 last_applied := Last,
                                 current_term := Term}) ->
     {ok, {Last, Term}, QueryFun(MacState)}.

make_caller_reply_actions(State = #state{pending_replies = []}) ->
    {State, [], []};
make_caller_reply_actions(State = #state{pending_replies = Pending,
                                         node_state = NodeState
                                         = #{commit_index := CommitIndex}}) ->
    % There are code paths here that are difficult to test externally
    % TODO: add internal unit tests
    {Replies, Pending1, Notifications} =
    lists:foldl(fun({Mode, {Idx, Term}, {FromPid, _} = From}, {Act, Rem, Not})
                      when Idx =< CommitIndex ->
                        Reply = check_idx_term(Idx, Term, NodeState),
                        case Mode of
                            reply ->
                                {[{reply, From, Reply} | Act], Rem, Not};
                            notify ->
                                {Act, Rem, [{FromPid, Reply} | Act]}
                        end;
                   (P, {Act, Rem, Not}) ->
                        {Act, [P | Rem], Not}
                end, {[], [], []}, Pending),
    {State#state{pending_replies = Pending1}, Replies, Notifications}.

check_idx_term(Idx, Term, #{log := Log}) ->
    case ra_log:fetch(Idx, Log) of
        undefined ->
            % should never happen so exit
            exit(corrupted_log);
        {Idx, Term, _} ->
            % the term of the index is correct
            {Idx, Term};
        {Idx, OthTerm, _} ->
            % can this happen given the pending
            % callers are only stored for the leader
            {error, {entry_term_mismatch, Term,
                     OthTerm}}
    end.

interact(Effect, EvtType, State) ->
    interact(Effect, EvtType, State, []).

interact(none, _EvtType, State, Actions) ->
    {State, Actions};
interact({next_event, Evt}, EvtType, State, Actions) ->
    {State, [ {next_event, EvtType, Evt} |  Actions]};
interact({send_msg, To, Msg}, _EvtType, State, Actions) ->
    To ! Msg,
    {State, Actions};
interact({reply, Reply}, {call, From}, State, Actions) ->
    % ok = gen_statem:reply(From, Reply),
    {State, [{reply, From, Reply} | Actions]};
interact({reply, _Reply}, _, _State, _Actions) ->
    exit(undefined_reply);
interact({send_vote_requests, VoteRequests}, _EvtType, State, Actions) ->
    % transient election processes
    T = {dirty_timeout, 500},
    Me = self(),
    [begin
         _ = spawn(fun () -> Reply = gen_statem:call(N, M, T),
                             ok = gen_statem:cast(Me, Reply)
                   end)
     end || {N, M} <- VoteRequests],
    {State, Actions};
interact({send_append_entries, AppendEntries}, _EvtType,
         #state{proxy = undefined, broadcast_time = Interval} = State,
         Actions) ->
    {ok, Proxy} = ra_proxy:start_link(self(), Interval),
    ok = ra_proxy:proxy(Proxy, AppendEntries),
    {State#state{proxy = Proxy}, Actions};
interact({send_append_entries, AppendEntries}, _EvtType,
         #state{proxy = Proxy} = State, Actions) ->
    ok = ra_proxy:proxy(Proxy, AppendEntries),
    {State, Actions};
interact([Effect | Effects], EvtType, State0, Actions0) ->
    {State, Actions} = interact(Effect,  EvtType, State0, Actions0),
    interact(Effects, EvtType, State, Actions);
interact([], _EvtType, State, Actions) -> {State, Actions}.


election_timeout_action(#state{broadcast_time = Timeout}) ->
    T = rand:uniform(Timeout * 3) + (Timeout * 2),
    {timeout, T, election_timeout}.

follower_leader_change(#state{node_state = #{leader_id := L}},
                       #state{node_state = #{leader_id := L}} = New) ->
    % no change
    New;
follower_leader_change(_Old, #state{node_state = #{id := Id, leader_id := L,
                                                   current_term := Term},
                                    pending_commands = Pending} = New)
  when L /= undefined ->
    % leader has either changed or just been set
    ?DBG("~p detected a new leader ~p in term ~p~n", [Id, L, Term]),
    [ok = gen_statem:reply(From, {redirect, L})
     || {From, _Data} <- Pending],
    New#state{pending_commands = []};
follower_leader_change(_Old, New) -> New.

server_ref_to_local_name({Name, _}) -> Name;
server_ref_to_local_name(Name) when is_atom(Name) -> Name.

