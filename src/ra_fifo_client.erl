%% @doc Provides an easy to consume API for interacting with the {@link ra_fifo.}
%% state machine implementation running inside a `ra' raft system.
%%
%% Handles command tracking and other non-functional concerns.
-module(ra_fifo_client).

-export([
         init/1,
         checkout/3,
         enqueue/2,
         enqueue/3,
         dequeue/3,
         settle/3,
         return/3,
         discard/3,
         handle_ra_event/3
         ]).

-include("ra.hrl").

-type seq() :: non_neg_integer().

-record(state, {nodes = [] :: [ra_node_id()],
                leader :: maybe(ra_node_id()),
                next_seq = 0 :: seq(),
                last_applied :: maybe(seq()),
                next_enqueue_seq = 1 :: seq(),
                pending = #{} :: #{seq() => {maybe(term()), ra_fifo:command()}},
                customer_deliveries = #{} :: #{ra_fifo:customer_tag() =>
                                               seq()}}).

-opaque state() :: #state{}.

-export_type([
              state/0
             ]).

-define(MAX_PENDING, 1024).

%% @doc Create the initial state for a new ra_fifo sessions. A state is needed
%% to interact with a ra_fifo queue using @module.
%% @param Nodes The known nodes of the queue. If the current leader is known
%% ensure the leader node is at the head of the list.
-spec init([ra_node_id()]) -> state().
init(Nodes) ->
    #state{nodes = Nodes}.

%% @doc Enqueues a message.
%% @param Correlation an arbitrary erlang term used to correlate this
%% command when it has been applied.
%% @param Msg an arbitrary erlang term representing the message.
%% @param State the current {@module} state.
%% @returns
%% `{ok, State}' if the command was successfully sent.
%% {@module} assigns a sequence number to every raft command it issues. The
%% SequenceNumber can be correlated to the applied sequence numbers returned
%% by the {@link handle_ra_event/2. handle_ra_event/2} function.
%%
%% `{error, stop_sending}' if the number of message not yet known to
%% have been successfully applied by ra has reached the maximum limit.
%% If this happens the caller should either discard or cache the requested
%% enqueue until at least one <code>ra_event</code> has been processes.
-spec enqueue(Correlation :: term(), Msg :: term(), State :: state()) ->
    {ok, state()} | {error, stop_sending}.
enqueue(Correlation, Msg, State0) ->
    Node = pick_node(State0),
    {Next, State1} = next_enqueue_seq(State0),
    % by default there is no correlation id
    Cmd = {enqueue, self(), Next, Msg},
    State = send_command(Node, Correlation, Cmd, State1),
    {ok, State}.

%% @doc Enqueues a message.
%% @param Msg an arbitrary erlang term representing the message.
%% @param State the current {@module} state.
%% @returns
%% `{ok, State}' if the command was successfully sent.
%% {@module} assigns a sequence number to every raft command it issues. The
%% SequenceNumber can be correlated to the applied sequence numbers returned
%% by the {@link handle_ra_event/2. handle_ra_event/2} function.
%%
%% `{error, stop_sending}' if the number of message not yet known to
%% have been successfully applied by ra has reached the maximum limit.
%% If this happens the caller should either discard or cache the requested
%% enqueue until at least one <code>ra_event</code> has been processes.
-spec enqueue(Msg :: term(), State :: state()) ->
    {ok, state()} | {error, stop_sending}.
enqueue(Msg, State) ->
    enqueue(undefined, Msg, State).

%% @doc Dequeue a message from the queue.
%%
%% This is a syncronous call. I.e. the call will block until the command
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
dequeue(CustomerTag, Settlement, State0) ->
    Node = pick_node(State0),
    CustomerId = customer_id(CustomerTag),
    case ra:send_and_await_consensus(Node, {checkout, {dequeue, Settlement},
                                            CustomerId}) of
        {ok, {dequeue, Reply}, Leader} ->
            {ok, Reply, State0#state{leader = Leader}};
        Err ->
            Err
    end.

%% @doc Settle a message. Permanently removes message from the queue.
%% @param CustomerTag the tag uniquely identifying the customer.
%% @param MsgIds the message ids received with the {@link ra_fifo:delivery/0.}
%% @param State the {@module} state
%% @returns
%% `{ok, State}' if the command was successfully sent.
%%
%% `{error, stop_sending}' if the number of commands not yet known to
%% have been successfully applied by ra has reached the maximum limit.
%% If this happens the caller should either discard or cache the requested
%% enqueue until at least one <code>ra_event</code> has been processes.
-spec settle(ra_fifo:customer_tag(), [ra_fifo:msg_id()], state()) ->
    {ok, state()} | {error, stop_sending}.
settle(CustomerTag, [_|_] = MsgIds, State0) ->
    Node = pick_node(State0),
    % TODO: make ra_fifo settle support lists of message ids
    Cmd = {settle, MsgIds, customer_id(CustomerTag)},
    State = send_command(Node, undefined, Cmd, State0),
    {ok, State}.

%% @doc Return a message to the queue.
%% @param CustomerTag the tag uniquely identifying the customer.
%% @param MsgIds the message ids to return received
%% from {@link ra_fifo:delivery/0.}
%% @param State the {@module} state
%% @returns
%% `{ok, State}' if the command was successfully sent.
%%
%% `{error, stop_sending}' if the number of commands not yet known to
%% have been successfully applied by ra has reached the maximum limit.
%% If this happens the caller should either discard or cache the requested
%% enqueue until at least one <code>ra_event</code> has been processes.
-spec return(ra_fifo:customer_tag(), [ra_fifo:msg_id()], state()) ->
    {ok, state()} | {error, stop_sending}.
return(CustomerTag, [_|_] = MsgIds, State0) ->
    Node = pick_node(State0),
    % TODO: make ra_fifo return support lists of message ids
    Cmd = {return, MsgIds, customer_id(CustomerTag)},
    State = send_command(Node, undefined, Cmd, State0),
    {ok, State}.

%% @doc Discards a checked out message.
%% If the queue has a dead_letter_handler configured this will be called.
%% @param CustomerTag the tag uniquely identifying the customer.
%% @param MsgIds the message ids to discard
%% from {@link ra_fifo:delivery/0.}
%% @param State the {@module} state
%% @returns
%% `{ok, State}' if the command was successfully sent.
%%
%% `{error, stop_sending}' if the number of commands not yet known to
%% have been successfully applied by ra has reached the maximum limit.
%% If this happens the caller should either discard or cache the requested
%% enqueue until at least one <code>ra_event</code> has been processes.
-spec discard(ra_fifo:customer_tag(), [ra_fifo:msg_id()], state()) ->
    {ok, state()} | {error, stop_sending}.
discard(CustomerTag, [_|_] = MsgIds, State0) ->
    Node = pick_node(State0),
    Cmd = {discard, MsgIds, customer_id(CustomerTag)},
    State = send_command(Node, undefined, Cmd, State0),
    {ok, State}.

%% @doc Register with the ra_fifo queue to "checkout" messages as they
%% become available.
%%
%% This is a syncronous call. I.e. the call will block until the command
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
checkout(CustomerTag, NumUnsettled, State) ->
    Node = pick_node(State),
    CustomerId = {CustomerTag, self()},
    case ra:send_and_await_consensus(Node, {checkout, {auto, NumUnsettled},
                                            CustomerId}) of
        {ok, _, Leader} ->
            {ok, State#state{leader = Leader}};
        Err ->
            Err
    end.

%% @doc Handles incoming `ra_events'. Events carry both internal "bookeeping"
%% events emitted by the `ra' leader as well as `ra_fifo' emitted events such
%% as message deliveries. All ra events need to be handled by {@module}
%% to ensure bookeeping, resends and flow control is correctly handled.
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
%% @param From the {@link ra_node_id().} of the sending process.
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
-spec handle_ra_event(ra_node_id(), ra_node_proc:ra_event_body(), state()) ->
    {internal, Correlators :: [term()], state()} |
    {ra_fifo:client_msg(), state()}.
handle_ra_event(From, {applied, Seqs}, State0) ->
    {Corrs, State} = lists:foldl(fun seq_applied/2,
                                 {[], State0#state{leader = From}}, Seqs),
    {internal, lists:reverse(Corrs), State};
handle_ra_event(_From, {rejected, {not_leader, undefined, _Seq}}, State0) ->
    % TODO: how should these be handled? re-sent on timer or try random
    {internal, [], State0};
handle_ra_event(_From, {rejected, {not_leader, Leader, Seq}}, State0) ->
    State1 = State0#state{leader = Leader},
    State = resend(Seq, State1),
    {internal, [], State};
handle_ra_event(Leader, {machine, {delivery, _CustomerTag, _} = Del}, State0) ->
    handle_delivery(Leader, Del, State0).

%% Internal

seq_applied(Seq, {Corrs, #state{last_applied = Last} = State0})
  when Seq > Last orelse Last =:= undefined ->
    State = case Last of
                undefined ->
                    State0;
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

do_resends(From, To, State) ->
    lists:foldl(fun resend/2, State, lists:seq(From, To)).

% resends a command with a new sequence number
resend(OldSeq, #state{pending = Pending0, leader = Leader} = State) ->
    case maps:take(OldSeq, Pending0) of
        {{Corr, Cmd}, Pending} ->
            send_command(Leader, Corr, Cmd, State#state{pending = Pending});
        error ->
            State
    end.

handle_delivery(Leader, {delivery, Tag, [{FstId, _} | _] = IdMsgs} = Del0,
                #state{customer_deliveries = CDels0} = State0) ->
    {LastId, _} = lists:last(IdMsgs),
    case CDels0 of
        #{Tag := Last} when FstId =:= Last+1 ->
            {Del0, State0#state{customer_deliveries =
                                maps:put(Tag, LastId, CDels0)}};
        #{Tag := Last} when FstId > Last+1 ->
            Missing = get_missing_deliveries(Leader, Last+1, FstId-1, Tag),
            Del = {delivery, Tag, Missing ++ IdMsgs},
            {Del, State0#state{customer_deliveries =
                               maps:put(Tag, LastId, CDels0)}};
        #{Tag := Last} when FstId =< Last ->
            exit(duplicate_delivery_not_impl);
        _ when FstId =:= 0 ->
            % the very first delivery
            {Del0, State0#state{customer_deliveries =
                                maps:put(Tag, LastId, CDels0)}};
        _ ->
            % not seen before and not initial msg id
            Missing = get_missing_deliveries(Leader, 0, FstId-1, Tag),
            Del = {delivery, Tag, Missing ++ IdMsgs},
            {Del, State0#state{customer_deliveries =
                               maps:put(Tag, LastId, CDels0)}}

    end.

get_missing_deliveries(Leader, From, To, CustomerTag) ->
    CustomerId = customer_id(CustomerTag),
    Query = fun (State) ->
                    ra_fifo:get_checked_out(CustomerId,
                                            From, To, State)
            end,
    {ok, {_, Missing}, _} = ra:dirty_query(Leader, Query),
    Missing.

pick_node(#state{leader = undefined, nodes = [N | _]}) ->
    N;
pick_node(#state{leader = Leader}) ->
    Leader.

next_seq(#state{next_seq = Seq} = State) ->
    {Seq, State#state{next_seq = Seq + 1}}.

next_enqueue_seq(#state{next_enqueue_seq = Seq} = State) ->
    {Seq, State#state{next_enqueue_seq = Seq + 1}}.

customer_id(CustomerTag) ->
    {CustomerTag, self()}.

send_command(Node, Correlation, Command, #state{pending = Pending} = State0) ->
    {Seq, State} = next_seq(State0),
    ok = ra:send_and_notify(Node, Command, Seq),
    State#state{pending = Pending#{Seq => {Correlation, Command}}}.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.
