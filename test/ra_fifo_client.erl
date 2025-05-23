%% @doc Provides an easy to consume API for interacting with the {@link ra_fifo.}
%% state machine implementation running inside a `ra' raft system.
%%
%% Handles command tracking and other non-functional concerns.
-module(ra_fifo_client).

-export([
         init/2,
         init/3,
         init/5,
         checkout/3,
         cancel_checkout/2,
         enqueue/2,
         enqueue/3,
         dequeue/3,
         settle/3,
         return/3,
         discard/3,
         handle_ra_event/3,
         untracked_enqueue/3,
         purge/1,
         cluster_name/1
         ]).

-include("src/ra.hrl").

-define(SOFT_LIMIT, 256).

-type seq() :: non_neg_integer().

-record(state, {cluster_name :: ra_cluster_name(),
                servers = [] :: [ra:server_id()],
                leader :: option(ra:server_id()),
                next_seq = 0 :: seq(),
                last_applied :: option(seq()),
                next_enqueue_seq = 1 :: seq(),
                %% indicates that we've exceeded the soft limit
                slow = false :: boolean(),
                unsent_commands = #{} :: #{ra_fifo:customer_id() =>
                                           {[seq()], [seq()], [seq()]}},
                soft_limit = ?SOFT_LIMIT :: non_neg_integer(),
                pending = #{} :: #{seq() =>
                                   {option(term()), ra_fifo:command()}},
                customer_deliveries = #{} :: #{ra_fifo:customer_tag() =>
                                               seq()},
                priority = normal :: normal | low,
                block_handler = fun() -> ok end :: fun(() -> ok),
                unblock_handler = fun() -> ok end :: fun(() -> ok),
                timeout :: non_neg_integer()
               }).

-opaque state() :: #state{}.

-export_type([
              state/0
             ]).


%% @doc Create the initial state for a new ra_fifo sessions. A state is needed
%% to interact with a ra_fifo queue using @module.
%% @param ClusterName the id of the cluster to interact with
%% @param Nodes The known servers of the queue. If the current leader is known
%% ensure the leader node is at the head of the list.
-spec init(ra_cluster_name(), [ra:server_id()]) -> state().
init(ClusterName, Nodes) ->
    init(ClusterName, Nodes, ?SOFT_LIMIT).

%% @doc Create the initial state for a new ra_fifo sessions. A state is needed
%% to interact with a ra_fifo queue using @module.
%% @param ClusterName the id of the cluster to interact with
%% @param Nodes The known servers of the queue. If the current leader is known
%% ensure the leader node is at the head of the list.
%% @param MaxPending size defining the max number of pending commands.
-spec init(ra_cluster_name(), [ra:server_id()], non_neg_integer()) -> state().
init(ClusterName, Nodes, SoftLimit) ->
    Timeout = application:get_env(kernel, net_ticktime, 60) + 30,
    #state{cluster_name = ClusterName,
           servers = Nodes,
           soft_limit = SoftLimit,
           timeout = Timeout * 1000}.

-spec init(ra_cluster_name(), [ra:server_id()], non_neg_integer(), fun(() -> ok),
           fun(() -> ok)) -> state().
init(ClusterName, Nodes, SoftLimit, BlockFun, UnblockFun) ->
    Timeout = application:get_env(kernel, net_ticktime, 60) + 30,
    #state{cluster_name = ClusterName,
           servers = Nodes,
           block_handler = BlockFun,
           unblock_handler = UnblockFun,
           soft_limit = SoftLimit,
           timeout = Timeout * 1000}.

%% @doc Enqueues a message.
%% @param Correlation an arbitrary erlang term used to correlate this
%% command when it has been applied.
%% @param Msg an arbitrary erlang term representing the message.
%% @param State the current {@module} state.
%% @returns
%% `{ok | slow, State}' if the command was successfully sent. If the return
%% tag is `slow' it means the limit is approaching and it is time to slow down
%% the sending rate.
%% {@module} assigns a sequence number to every raft command it issues. The
%% SequenceNumber can be correlated to the applied sequence numbers returned
%% by the {@link handle_ra_event/2. handle_ra_event/2} function.
%%
%% `{error, stop_sending}' if the number of message not yet known to
%% have been successfully applied by ra has reached the maximum limit.
%% If this happens the caller should either discard or cache the requested
%% enqueue until at least one <code>ra_event</code> has been processes.
-spec enqueue(Correlation :: term(), Msg :: term(), State :: state()) ->
    {ok | slow, state()} | {error, stop_sending}.
enqueue(Correlation, Msg, State0 = #state{slow = Slow,
                                          block_handler = BlockFun}) ->
    Node = pick_node(State0),
    {Next, State1} = next_enqueue_seq(State0),
    % by default there is no correlation id
    Cmd = {enqueue, self(), Next, Msg},
    case send_command(Node, Correlation, Cmd, low, State1) of
        {slow, _} = Ret when not Slow ->
            BlockFun(),
            Ret;
        Any ->
            Any
    end.

%% @doc Enqueues a message.
%% @param Msg an arbitrary erlang term representing the message.
%% @param State the current {@module} state.
%% @returns
%% `{ok | slow, State}' if the command was successfully sent. If the return
%% tag is `slow' it means the limit is approaching and it is time to slow down
%% the sending rate.
%% {@module} assigns a sequence number to every raft command it issues. The
%% SequenceNumber can be correlated to the applied sequence numbers returned
%% by the {@link handle_ra_event/2. handle_ra_event/2} function.
%%
%% `{error, stop_sending}' if the number of message not yet known to
%% have been successfully applied by ra has reached the maximum limit.
%% If this happens the caller should either discard or cache the requested
%% enqueue until at least one <code>ra_event</code> has been processes.
-spec enqueue(Msg :: term(), State :: state()) ->
    {ok | slow, state()} | {error, stop_sending}.
enqueue(Msg, State) ->
    enqueue(undefined, Msg, State).

%% @doc Dequeue a message from the queue.
%%
%% This is a synchronous call. I.e. the call will block until the command
%% has been accepted by the ra process or it times out.
%%
%% @param CustomerTag a unique tag to identify this particular customer.
%% @param Settlement either `settled' or `unsettled'. When `settled' no
%% further settlement needs to be done.
%% @param State The {@module} state.
%%
%% @returns `{ok, IdMsg, State}' or `{error | timeout, term()}'
-spec dequeue(ra_fifo:customer_tag(),
              Settlement :: settled | unsettled, state()) ->
    {ok, ra_fifo:delivery_msg() | empty, state()} | {error | timeout, term()}.
dequeue(CustomerTag, Settlement, #state{timeout = Timeout} = State0) ->
    Node = pick_node(State0),
    CustomerId = customer_id(CustomerTag),
    case ra:process_command(Node, {checkout, {dequeue, Settlement},
                                            CustomerId}, Timeout) of
        {ok, {dequeue, Reply}, Leader} ->
            {ok, Reply, State0#state{leader = Leader}};
        {ok, Err, _} ->
            Err;
        Err ->
            Err
    end.

%% @doc Settle a message. Permanently removes message from the queue.
%% @param CustomerTag the tag uniquely identifying the customer.
%% @param MsgIds the message ids received with the {@link ra_fifo:delivery/0.}
%% @param State the {@module} state
%% @returns
%% `{ok | slow, State}' if the command was successfully sent. If the return
%% tag is `slow' it means the limit is approaching and it is time to slow down
%% the sending rate.
%%
%% `{error, stop_sending}' if the number of commands not yet known to
%% have been successfully applied by ra has reached the maximum limit.
%% If this happens the caller should either discard or cache the requested
%% enqueue until at least one <code>ra_event</code> has been processes.
-spec settle(ra_fifo:customer_tag(), [ra_fifo:msg_id()], state()) ->
    {ok, state()}.
settle(CustomerTag, [_|_] = MsgIds, #state{slow = false} = State0) ->
    Node = pick_node(State0),
    Cmd = {settle, MsgIds, customer_id(CustomerTag)},
    case send_command(Node, undefined, Cmd, normal, State0) of
        {slow, S} ->
            % turn slow into ok for this function
            {ok, S};
        {ok, _} = Ret ->
            Ret
    end;
settle(CustomerTag, [_|_] = MsgIds,
       #state{unsent_commands = Unsent0} = State0) ->
    CustomerId = customer_id(CustomerTag),
    %% we've reached the soft limit so will stash the command to be
    %% sent once we have seen enough notifications
    Unsent = maps:update_with(CustomerId,
                              fun ({Settles, Returns, Discards}) ->
                                      {Settles ++ MsgIds, Returns, Discards}
                              end, {MsgIds, [], []}, Unsent0),
    {ok, State0#state{unsent_commands = Unsent}}.

%% @doc Return a message to the queue.
%% @param CustomerTag the tag uniquely identifying the customer.
%% @param MsgIds the message ids to return received
%% from {@link ra_fifo:delivery/0.}
%% @param State the {@module} state
%% @returns
%% `{ok | slow, State}' if the command was successfully sent. If the return
%% tag is `slow' it means the limit is approaching and it is time to slow down
%% the sending rate.
%%
%% `{error, stop_sending}' if the number of commands not yet known to
%% have been successfully applied by ra has reached the maximum limit.
%% If this happens the caller should either discard or cache the requested
%% enqueue until at least one <code>ra_event</code> has been processes.
-spec return(ra_fifo:customer_tag(), [ra_fifo:msg_id()], state()) ->
    {ok, state()}.
return(CustomerTag, [_|_] = MsgIds, #state{slow = false} = State0) ->
    Node = pick_node(State0),
    % TODO: make ra_fifo return support lists of message ids
    Cmd = {return, MsgIds, customer_id(CustomerTag)},
    case send_command(Node, undefined, Cmd, normal, State0) of
        {slow, S} ->
            % turn slow into ok for this function
            {ok, S};
        {ok, _} = Ret ->
            Ret
    end;
return(CustomerTag, [_|_] = MsgIds,
       #state{unsent_commands = Unsent0} = State0) ->
    CustomerId = customer_id(CustomerTag),
    %% we've reached the soft limit so will stash the command to be
    %% sent once we have seen enough notifications
    Unsent = maps:update_with(CustomerId,
                              fun ({Settles, Returns, Discards}) ->
                                      {Settles, Returns ++ MsgIds, Discards}
                              end, {[], MsgIds, []}, Unsent0),
    {ok, State0#state{unsent_commands = Unsent}}.

%% @doc Discards a checked out message.
%% If the queue has a dead_letter_handler configured this will be called.
%% @param CustomerTag the tag uniquely identifying the customer.
%% @param MsgIds the message ids to discard
%% from {@link ra_fifo:delivery/0.}
%% @param State the {@module} state
%% @returns
%% `{ok | slow, State}' if the command was successfully sent. If the return
%% tag is `slow' it means the limit is approaching and it is time to slow down
%% the sending rate.
%%
%% `{error, stop_sending}' if the number of commands not yet known to
%% have been successfully applied by ra has reached the maximum limit.
%% If this happens the caller should either discard or cache the requested
%% enqueue until at least one <code>ra_event</code> has been processes.
-spec discard(ra_fifo:customer_tag(), [ra_fifo:msg_id()], state()) ->
    {ok | slow, state()} | {error, stop_sending}.
discard(CustomerTag, [_|_] = MsgIds, #state{slow = false} = State0) ->
    Node = pick_node(State0),
    Cmd = {discard, MsgIds, customer_id(CustomerTag)},
    case send_command(Node, undefined, Cmd, normal, State0) of
        {slow, S} ->
            % turn slow into ok for this function
            {ok, S};
        {ok, _} = Ret ->
            Ret
    end;
discard(CustomerTag, [_|_] = MsgIds,
        #state{unsent_commands = Unsent0} = State0) ->
    CustomerId = customer_id(CustomerTag),
    %% we've reached the soft limit so will stash the command to be
    %% sent once we have seen enough notifications
    Unsent = maps:update_with(CustomerId,
                              fun ({Settles, Returns, Discards}) ->
                                      {Settles, Returns, Discards ++ MsgIds}
                              end, {[], [], MsgIds}, Unsent0),
    {ok, State0#state{unsent_commands = Unsent}}.


%% @doc Register with the ra_fifo queue to "checkout" messages as they
%% become available.
%%
%% This is a synchronous call. I.e. the call will block until the command
%% has been accepted by the ra process or it times out.
%%
%% @param CustomerTag a unique tag to identify this particular customer.
%% @param NumUnsettled the maximum number of in-flight messages. Once this
%% number of messages has been received but not settled no further messages
%% will be delivered to the customer.
%% @param State The {@module} state.
%%
%% @returns `{ok, State}' or `{error | timeout, term()}'
-spec checkout(ra_fifo:customer_tag(), NumUnsettled :: non_neg_integer(),
               state()) -> {ok, state()} | {error | timeout, term()}.
checkout(CustomerTag, NumUnsettled, State0) ->
    Nodes = sorted_servers(State0),
    CustomerId = {CustomerTag, self()},
    Cmd = {checkout, {auto, NumUnsettled}, CustomerId},
    try_process_command(Nodes, Cmd, State0).

%% @doc Cancels a checkout with the ra_fifo queue  for the customer tag
%%
%% This is a synchronous call. I.e. the call will block until the command
%% has been accepted by the ra process or it times out.
%%
%% @param CustomerTag a unique tag to identify this particular customer.
%% @param State The {@module} state.
%%
%% @returns `{ok, State}' or `{error | timeout, term()}'
-spec cancel_checkout(ra_fifo:customer_tag(), state()) ->
    {ok, state()} | {error | timeout, term()}.
cancel_checkout(CustomerTag, #state{customer_deliveries = CDels} = State0) ->
    Nodes = sorted_servers(State0),
    CustomerId = {CustomerTag, self()},
    Cmd = {checkout, cancel, CustomerId},
    State = State0#state{customer_deliveries = maps:remove(CustomerTag, CDels)},
    try_process_command(Nodes, Cmd, State).

%% @doc Purges all the messages from a ra_fifo queue and returns the number
%% of messages purged.
-spec purge(ra:server_id()) -> {ok, non_neg_integer()} | {error | timeout, term()}.
purge(Node) ->
    case ra:process_command(Node, purge) of
        {ok, {reply, {purge, Reply}}, _} ->
            {ok, Reply};
        Err ->
            Err
    end.

%% @doc returns the cluster name
-spec cluster_name(state()) -> ra_cluster_name().
cluster_name(#state{cluster_name = ClusterName}) ->
    ClusterName.

%% @doc Handles incoming `ra_events'. Events carry both internal "bookkeeping"
%% events emitted by the `ra' leader as well as `ra_fifo' emitted events such
%% as message deliveries. All ra events need to be handled by {@module}
%% to ensure bookkeeping, resends and flow control is correctly handled.
%%
%% If the `ra_event' contains a `ra_fifo' generated message it will be returned
%% for further processing.
%%
%% Example:
%%
%% ```
%%  receive
%%     {ra_event, From, Evt} ->
%%         case ra_fifo_client:handle_ra_event(From, Evt, State0) of
%%             {internal, _Seq, State} -> State;
%%             {{delivery, _CustomerTag, Msgs}, State} ->
%%                  handle_messages(Msgs),
%%                  ...
%%         end
%%  end
%% '''
%%
%% @param From the {@link ra:server_id().} of the sending process.
%% @param Event the body of the `ra_event'.
%% @param State the current {@module} state.
%%
%% @returns
%% `{internal, AppliedCorrelations, State}' if the event contained an internally
%% handled event such as a notification and a correlation was included with
%% the command (e.g. in a call to `enqueue/3' the correlation terms are returned
%% here.
%%
%% `{RaFifoEvent, State}' if the event contained a client message generated by
%% the `ra_fifo' state machine such as a delivery.
%%
%% The type of `ra_fifo' client messages that can be received are:
%%
%% `{delivery, CustomerTag, [{MsgId, {MsgHeader, Msg}}]}'
%%
%% <li>`CustomerTag' the binary tag passed to {@link checkout/3.}</li>
%% <li>`MsgId' is a customer scoped monotonically incrementing id that can be
%% used to {@link settle/3.} (roughly: AMQP 0.9.1 ack) message once finished
%% with them.</li>
-spec handle_ra_event(ra:server_id(), ra_server_proc:ra_event_body(), state()) ->
    {internal, Correlators :: [term()], state()} |
    {ra_fifo:client_msg(), state()} | eol.
handle_ra_event(From, {applied, Seqs},
                #state{soft_limit = SftLmt,
                       unblock_handler = UnblockFun} = State0) ->
    {Corrs, State1} = lists:foldl(fun seq_applied/2,
                                  {[], State0#state{leader = From}},
                                  Seqs),
    %% TODO check if we have moved from softlimit to safe zone
    case maps:size(State1#state.pending) < SftLmt of
        true when State1#state.slow == true ->
            % we have exited soft limit state
            % send any unsent commands
            State2 = State1#state{slow = false,
                                  unsent_commands = #{}},
            % build up a list of commands to issue
            Commands = maps:fold(
                         fun (Cid, {Settled, Returns, Discards}, Acc) ->
                                 add_command(Cid, settle, Settled,
                                             add_command(Cid, return, Returns,
                                                         add_command(Cid, discard, Discards, Acc)))
                         end, [], State1#state.unsent_commands),
            Node = pick_node(State2),
            %% send all the settlements and returns
            State = lists:foldl(fun (C, S0) ->
                                        case send_command(Node, undefined,
                                                          C, normal, S0) of
                                            {T, S} when T =/= error ->
                                                S
                                        end
                                end, State2, Commands),
            UnblockFun(),
            {internal, lists:reverse(Corrs), State};
        _ ->
            {internal, lists:reverse(Corrs), State1}
    end;
handle_ra_event(Leader, {machine, {delivery, _CustomerTag, _} = Del}, State0) ->
    handle_delivery(Leader, Del, State0);
handle_ra_event(_From, {rejected, {not_leader, undefined, _Seq}}, State0) ->
    % TODO: how should these be handled? re-sent on timer or try random
    {internal, [], State0};
handle_ra_event(_From, {rejected, {not_leader, Leader, Seq}}, State0) ->
    State1 = State0#state{leader = Leader},
    State = resend(Seq, State1),
    {internal, [], State};
handle_ra_event(_Leader, {machine, eol}, _State0) ->
    eol.

%% @doc Attempts to enqueue a message using cast semantics. This provides no
%% guarantees or retries if the message fails to achieve consensus or if the
%% servers sent to happens not to be available. If the message is sent to a
%% follower it will attempt the deliver it to the leader, if known. Else it will
%% drop the messages.
%%
%% NB: only use this for non-critical enqueues where a full ra_fifo_client state
%% cannot be maintained.
%%
%% @param ClusterName  the name of the cluster.
%% @param Nodes the known servers in the cluster.
%% @param Msg the message to enqueue.
%%
%% @returns `ok'
-spec untracked_enqueue(ra_cluster_name(), [ra:server_id()], term()) ->
    ok.
untracked_enqueue(_ClusterName, [Node | _], Msg) ->
    Cmd = {enqueue, undefined, undefined, Msg},
    ok = ra:pipeline_command(Node, Cmd),
    ok.

%% Internal

try_process_command([Node | Rem], Cmd, State) ->
    case ra:process_command(Node, Cmd, 30000) of
        {ok, _, Leader} ->
            {ok, State#state{leader = Leader}};
        Err when length(Rem) =:= 0 ->
            Err;
        _ ->
            try_process_command(Rem, Cmd, State)
    end.

seq_applied({Seq, _}, {Corrs, #state{last_applied = Last} = State0})
  when Seq > Last orelse Last =:= undefined ->
    State = case Last of
                undefined -> State0;
                _ ->
                    do_resends(Last+1, Seq-1, State0)
            end,
    case maps:take(Seq, State#state.pending) of
        {{undefined, _}, Pending} ->
            {Corrs, State#state{pending = Pending,
                                last_applied = Seq}};
        {{Corr, _}, Pending} ->
            {[Corr | Corrs], State#state{pending = Pending,
                                         last_applied = Seq}};
        error ->
            % must have already been resent or removed for some other reason
            {Corrs, State}
    end;
seq_applied(_Seq, Acc) ->
    Acc.

do_resends(From, To, State) when From =< To ->
    ?INFO("doing resends From ~w  To ~w", [From, To]),
    lists:foldl(fun resend/2, State, lists:seq(From, To));
do_resends(_, _, State) ->
    State.

% resends a command with a new sequence number
resend(OldSeq, #state{pending = Pending0, leader = Leader} = State) ->
    case maps:take(OldSeq, Pending0) of
        {{Corr, Cmd}, Pending} ->
            %% resends aren't subject to flow control here
            resend_command(Leader, Corr, Cmd, State#state{pending = Pending});
        error ->
            State
    end.

handle_delivery(Leader, {delivery, Tag, [{FstId, _} | _] = IdMsgs} = Del0,
                #state{customer_deliveries = CDels0} = State0) ->
    {LastId, _} = lists:last(IdMsgs),
    case maps:get(Tag, CDels0, -1) of
        Prev when FstId =:= Prev+1 ->
            {Del0, State0#state{customer_deliveries =
                                maps:put(Tag, LastId, CDels0)}};
        Prev when FstId > Prev+1 ->
            Missing = get_missing_deliveries(Leader, Prev+1, FstId-1, Tag),
            Del = {delivery, Tag, Missing ++ IdMsgs},
            {Del, State0#state{customer_deliveries =
                               maps:put(Tag, LastId, CDels0)}};
        Prev when FstId =< Prev ->
            case lists:dropwhile(fun({Id, _}) -> Id =< Prev end, IdMsgs) of
                [] ->
                    {internal, [], State0};
                IdMsgs2 ->
                    handle_delivery(Leader, {delivery, Tag, IdMsgs2}, State0)
            end;
        _ when FstId =:= 0 ->
            % the very first delivery
            {Del0, State0#state{customer_deliveries =
                                maps:put(Tag, LastId, CDels0)}}
    end.

get_missing_deliveries(Leader, From, To, CustomerTag) ->
    CustomerId = customer_id(CustomerTag),
    % ?INFO("get_missing_deliveries for ~w from ~b to ~b",
    %       [CustomerId, From, To]),
    Query = fun (State) ->
                    ra_fifo:get_checked_out(CustomerId, From, To, State)
            end,
    {ok, {_, Missing}, _} = ra:local_query(Leader, Query),
    Missing.

pick_node(#state{leader = undefined, servers = [N | _]}) ->
    N;
pick_node(#state{leader = Leader}) ->
    Leader.

% servers sorted by last known leader
sorted_servers(#state{leader = undefined, servers = Nodes}) ->
    Nodes;
sorted_servers(#state{leader = Leader, servers = Nodes}) ->
    [Leader | lists:delete(Leader, Nodes)].

next_seq(#state{next_seq = Seq} = State) ->
    {Seq, State#state{next_seq = Seq + 1}}.

next_enqueue_seq(#state{next_enqueue_seq = Seq} = State) ->
    {Seq, State#state{next_enqueue_seq = Seq + 1}}.

customer_id(CustomerTag) ->
    {CustomerTag, self()}.

send_command(Node, Correlation, Command, Priority,
             #state{pending = Pending,
                    priority = Priority,
                    soft_limit = SftLmt} = State0) ->
    {Seq, State} = next_seq(State0),
    ok = ra:pipeline_command(Node, Command, Seq, Priority),
    Tag = case maps:size(Pending) >= SftLmt of
              true -> slow;
              false -> ok
          end,
    {Tag, State#state{pending = Pending#{Seq => {Correlation, Command}},
                      priority = Priority,
                      slow = Tag == slow}};
send_command(Node, Correlation, Command, normal,
             #state{priority = low} = State) ->
    send_command(Node, Correlation, Command, low, State);
send_command(Node, Correlation, Command, low,
             #state{priority = normal} = State) ->
    send_command(Node, Correlation, Command, low,
                 State#state{priority = low}).

resend_command(Node, Correlation, Command,
               #state{pending = Pending} = State0) ->
    {Seq, State} = next_seq(State0),
    ok = ra:pipeline_command(Node, Command, Seq),
    State#state{pending = Pending#{Seq => {Correlation, Command}}}.

add_command(_Cid, _Tag, [], Acc) ->
    Acc;
add_command(Cid, Tag, MsgIds, Acc) ->
    [{Tag, MsgIds, Cid} | Acc].
