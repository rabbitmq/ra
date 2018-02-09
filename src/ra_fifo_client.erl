%% @doc Provides an easy to consume API for interacting with the {@link ra_fifo.}
%% state machine implementation running inside a `ra' raft system.
%%
%% Handles command tracking and other non-functional concerns.
-module(ra_fifo_client).

-export([
         init/1,
         checkout/3,
         enqueue/2,
         dequeue/3,
         settle/3,
         handle_ra_event/2
         ]).

-include("ra.hrl").

-record(state, {nodes = [] :: [ra_node_id()],
                leader :: maybe(ra_node_id()),
                next_seq = 0 :: non_neg_integer(),
                customer_deliveries = #{} :: #{ra_fifo:customer_tag() =>
                                               non_neg_integer()}}).

-opaque state() :: #state{}.

-export_type([
              state/0
              ]).

%% @doc Create the initial state for a new ra_fifo sessions. A state is needed
%% to interact with a ra_fifo queue using @module.
%% @param Nodes The known nodes of the queue. If the current leader is known
%% ensure the leader node is at the head of the list.
-spec init([ra_node_id()]) -> state().
init(Nodes) ->
    #state{nodes = Nodes}.

%% @doc Enqueues a message.
%% @param Msg an arbitrary erlang term representing the message.
%% @param State the current {@module} state.
%% @returns
%% `{ok, SequenceNumber, State}' if the command was successfully sent.
%% {@module} assigns a sequence number to every raft command it issues. The
%% SequenceNumber can be correlated to the applied sequence numbers returned
%% by the {@link handle_ra_event/2. handle_ra_event/2} function.
%%
%% `{error, stop_sending}' if the number of message not yet known to
%% have been successfully applied by ra has reached the maximum limit.
%% If this happens the caller should either discard or cache the requested
%% enqueue until at least one <code>ra_event</code> has been processes.
-spec enqueue(Msg :: term(), State :: state()) ->
    {ok, Seq :: non_neg_integer(), state()} | {error, stop_sending}.
enqueue(Msg, State0) ->
    Node = pick_node(State0),
    {Seq, State} = next_seq(State0),
    ok = ra:send_and_notify(Node, {enqueue, Msg}, Seq),
    {ok, Seq, State}.

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
    CustomerId = {CustomerTag, self()},
    case ra:send_and_await_consensus(Node, {checkout, {get, Settlement},
                                            CustomerId}) of
        {ok, {get, Reply}, Leader} ->
            {ok, Reply, State0#state{leader = Leader}};
        Err ->
            Err
    end.

%% @doc Settle a message. Permanently removes message from the queue.
%% @param CustomerTag the tag uniquely identifying the customer.
%% @param MsgId the message id received with the {@link ra_fifo:delivery/0.}
%% @param State the {@module} state
%% @returns
%% `{ok, SequenceNumber, State}' if the command was successfully sent.
%% {@module} assigns a sequence number to every raft command it issues. The
%% SequenceNumber can be correlated to the applied sequence numbers returned
%% by the {@link handle_ra_event/2. handle_ra_event/2} function.
%%
%% `{error, stop_sending}' if the number of message not yet known to
%% have been successfully applied by ra has reached the maximum limit.
%% If this happens the caller should either discard or cache the requested
%% enqueue until at least one <code>ra_event</code> has been processes.
-spec settle(ra_fifo:customer_tag(), non_neg_integer(), state()) ->
    {ok, Seq :: non_neg_integer(), state()} | {error, stop_sending}.
settle(CustomerTag, MsgId, State0) ->
    Node = pick_node(State0),
    {Seq, State} = next_seq(State0),
    ok = ra:send_and_notify(Node, {settle, MsgId, CustomerTag}, Seq),
    {ok, Seq, State}.

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
%%     {ra_event, Evt} ->
%%         case ra_fifo_client:handle_ra_event(Evt, State0) of
%%             {internal, _Seq, State} -> State;
%%             {{delivery, _CustomerTag, Msgs}, State} ->
%%                  handle_messages(Msgs),
%%                  ...
%%         end
%%  end
%% '''
%%
%% @param Event the body of the `ra_event'.
%% @param State the current {@module} state.
%%
%% @returns
%% `{internal, AppliedSeqs, State}' if the event contained an internally
%% handled event such as a notification confirming that a sequence number
%% has been applied to the `ra_fifo' state machine.
%%
%% `{RaFifoEvent, State}' if the event contained a client message generated by
%% the `ra_fifo' state machine such as a delivery.
%%
%% The type of `ra_fifo' client messages that can be received are:
%%
%% `{delivery, CustomerTag, [{MsgId, {MsgHeader, Msg}}]}'
%%
%% <li>`CustomerTag' the binary tag passed to {@link checkout/3.}</li>
%% <li>`MsgId' is a customer scoped monotonically incrementing id that can be used
%% to {@link settle/3.} (roughly: AMQP 0.9.1 ack) message once finished with
%% them.</li>
-spec handle_ra_event(ra_node_proc:ra_event_body(), state()) ->
    {internal, AppliedSeqs :: [non_neg_integer()], state()} |
    {ra_fifo:client_msg(), state()}.
handle_ra_event({applied, _From, Seq}, State) ->
    % applied notifications should arrive in order
    % here we can detect if a sequence number was missed and resend it
    % TODO: bookkeeping
    {internal, [Seq], State};
handle_ra_event({rejected, _From, {not_leader, _Leader, _Seq} = Det}, _State) ->
    % need to resend to leader if not undefined
    exit({rejected_not_impl, Det});
handle_ra_event({machine, Leader, {delivery, _, _} = Del}, State0) ->
    State = record_delivery(Leader, Del, State0),
    {Del, State}.

%% internal

record_delivery(Leader, {delivery, CustomerTag, IdMsgs},
                #state{customer_deliveries = CDels} = State0) ->
    lists:foldl(
      fun ({MsgId, _}, S) ->
              case CDels of
                  #{CustomerTag := Last} when MsgId =:= Last+1 ->
                      S#state{customer_deliveries =
                              maps:put(CustomerTag, MsgId, CDels)};
                  #{CustomerTag := Last} when MsgId =:= Last+1 ->
                      % TODO for now just exit if we get an out of order
                      % delivery in the future we need to be perform something
                      % akin to selective ARQ or simply reset the subscription
                      exit({ra_fifo_client, out_of_order_delivery, MsgId,
                            State0});
                  _ ->
                      S#state{customer_deliveries =
                              maps:put(CustomerTag, MsgId, CDels)}
              end
      end, State0#state{leader = Leader}, IdMsgs).

pick_node(#state{leader = undefined, nodes = [N | _]}) ->
    N;
pick_node(#state{leader = Leader}) ->
    Leader.

next_seq(#state{next_seq = Seq} = State) ->
    {Seq, State#state{next_seq = Seq + 1}}.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.
