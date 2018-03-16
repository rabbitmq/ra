-module(ra_fifo).

-behaviour(ra_machine).

-compile({no_auto_import, [apply/3]}).

-include("ra.hrl").

-export([
         init/1,
         apply/3,
         leader_effects/1,
         tick/2,
         overview/1,
         get_checked_out/4,
         % queries
         query_messages_ready/1,
         query_messages_checked_out/1,
         query_processes/1,
         shadow_copy/1,
         size_test/2,
         perf_test/2
         % profile/1
         %
        ]).

-type raw_msg() :: term().
%% The raw message. It is opaque to ra_fifo.

-type msg_in_id() :: non_neg_integer().
% a queue scoped monotonically incrementing integer used to enforce order
% in the unassigned messages map

-type msg_id() :: non_neg_integer().
%% A customer-scoped monotonically incrementing integer included with a
%% {@link delivery/0.}. Used to settle deliveries using
%% {@link ra_fifo_client:settle/3.}

-type msg_seqno() :: non_neg_integer().
%% A sender process scoped monotonically incrementing integer included
%% in enqueue messages. Used to ensure ordering of messages send from the
%% same process

-type msg_header() :: #{delivery_count => non_neg_integer()}.
%% The message header map:
%% delivery_count: the number of unsuccessful delivery attempts.
%%                 A non-zero value indicates a previous attempt.

-type msg() :: {msg_header(), raw_msg()}.
%% message with a header map.

-type indexed_msg() :: {ra_index(), msg()}.

-type delivery_msg() :: {msg_id(), msg()}.
%% A tuple consisting of the message id and the headered message.

-type customer_tag() :: binary().
%% An arbitrary binary tag used to distinguish between different customers
%% set up by the same process. See: {@link ra_fifo_client:checkout/3.}

-type delivery() :: {delivery, customer_tag(), [delivery_msg()]}.
%% Represents the delivery of one or more ra_fifo messages.

-type customer_id() :: {customer_tag(), pid()}.
%% The entity that receives messages. Uniquely identifies a customer.

-type checkout_spec() :: {once | auto, Num :: non_neg_integer()} |
                         {dequeue, settled | unsettled}.

-type protocol() ::
    {enqueue, Sender :: pid(), MsgSeq :: msg_seqno(), Msg :: raw_msg()} |
    {checkout, Spec :: checkout_spec(), Customer :: customer_id()} |
    {settle, MsgIds :: [msg_id()], Customer :: customer_id()} |
    {return, MsgIds :: [msg_id()], Customer :: customer_id()} |
    {discard, MsgIds :: [msg_id()], Customer :: customer_id()}.

-type command() :: protocol() | ra_machine:builtin_command().
%% all the command types suppored by ra fifo

-type metrics() :: {Name :: atom(),
                    Enqueued :: non_neg_integer(),
                    CheckedOut :: non_neg_integer(),
                    Settled :: non_neg_integer(),
                    Returned :: non_neg_integer()}.

-type client_msg() :: delivery().
%% the messages `ra_fifo' can send to customers.

-type applied_mfa() :: {module(), function(), [term()]}.
% represents a partially applied module call

-define(METRICS_TABLE, ra_fifo_metrics).
-define(SHADOW_COPY_INTERVAL, 128).
% metrics tuple format:
% {Key, Enqueues, Checkouts, Settlements, Returns}

-record(customer,
        {checked_out = #{} :: #{msg_id() => {msg_in_id(), indexed_msg()}},
         next_msg_id = 0 :: msg_id(), % part of snapshot data
         num = 0 :: non_neg_integer(), % part of snapshot data
         % number of allocated messages
         % part of snapshot data
         seen = 0 :: non_neg_integer(),
         lifetime = once :: once | auto,
         suspected_down = false :: boolean()
        }).

-record(enqueuer,
        {next_seqno = 1 :: msg_seqno(),
         % out of order enqueues - sorted list
         pending = [] :: [{msg_seqno(), ra_index(), raw_msg()}],
         suspected_down = false :: boolean()
        }).

-record(state,
        {name :: atom(),
         % unassigned messages
         messages = #{} :: #{msg_in_id() => indexed_msg()},
         % defines the lowest message in id available in the messages map
         low_msg_num :: msg_in_id() | undefined,
         % defines the next message in id to be added to the messages map
         next_msg_num = 1 :: msg_in_id(),
         % a counter of enqueues - used to trigger shadow copy points
         enqueue_count = 0 :: non_neg_integer(),
         % a map containing all the live processes that have ever enqueued
         % a message to this queue as well as a cached value of the smallest
         % ra_index of all pending enqueues
         enqueuers = #{} :: #{pid() => #enqueuer{}},
         % master index of all enqueue raft indexes including pending
         % enqueues
         % ra_fifo_index can be slow when calculating the smallest
         % index when there are large gaps but should be faster than gb_trees
         % for normal appending operations - backed by a map
         ra_indexes = ra_fifo_index:empty() :: ra_fifo_index:state(),
         % customers need to reflect customer state at time of snapshot
         % needs to be part of snapshot
         customers = #{} :: #{customer_id() => #customer{}},
         % customers that require further service are queued here
         % needs to be part of snapshot
         service_queue = queue:new() :: queue:queue(customer_id()),
         dead_letter_handler :: maybe(applied_mfa()),
         metrics :: metrics()
        }).

-opaque state() :: #state{}.

-type config() :: #{name := atom(),
                    dead_letter_handler => maybe(applied_mfa())}.

-export_type([protocol/0,
              delivery/0,
              command/0,
              customer_tag/0,
              client_msg/0,
              msg/0,
              msg_id/0,
              msg_seqno/0,
              delivery_msg/0,
              state/0,
              config/0]).

-spec init(config()) -> {state(), ra_machine:effects()}.
init(#{name := Name} = Conf) ->
    DLH = maps:get(dead_letter_handler, Conf, undefined),
    {#state{name = Name,
            dead_letter_handler = DLH,
            metrics = {Name, 0, 0, 0, 0}},
     [{metrics_table, ra_fifo_metrics, {Name, 0, 0, 0, 0}}]}.



% msg_ids are scoped per customer
% ra_indexes holds all raft indexes for enqueues currently on queue
-spec apply(ra_index(), command(), state()) ->
    {state(), ra_machine:effects()}.
apply(RaftIdx, {enqueue, From, Seq, RawMsg}, State00) ->
    State0 = append_to_master_index(RaftIdx, State00),
    {State1, Effects0} = maybe_enqueue(RaftIdx, From, Seq, RawMsg, State0),
    {State2, Effects, Num} = checkout(State1, Effects0),
    State = incr_metrics(State2, {1, Num, 0, 0}),
    {State, Effects};
apply(RaftIdx, {settle, MsgIds, CustomerId},
      #state{customers = Custs0} = State0) ->
    case Custs0 of
        #{CustomerId := Cust0 = #customer{checked_out = Checked0}} ->
            Checked = maps:without(MsgIds, Checked0),
            Discarded = maps:with(MsgIds, Checked0),
            MsgRaftIdxs = [RIdx || {_MsgInId, {RIdx, _}}
                                   <- maps:values(Discarded)],
            % need to increment metrics before completing as any snapshot
            % states taken need to includ them
            State = incr_metrics(State0, {0, 0, length(MsgIds), 0}),
            complete(RaftIdx, CustomerId,
                     MsgRaftIdxs, Cust0, Checked,
                     State);
        _ ->
            {State0, []}
    end;
apply(RaftIdx, {discard, MsgIds, CustomerId},
      #state{customers = Custs0} = State0) ->
    case Custs0 of
        #{CustomerId := Cust0 = #customer{checked_out = Checked0}} ->
            Checked = maps:without(MsgIds, Checked0),
            Discarded = maps:with(MsgIds, Checked0),
            MsgRaftIdxs = [RIdx || {_MsgInId, {RIdx, _}}
                                   <- maps:values(Discarded)],
            State1 = incr_metrics(State0, {0, 0, length(MsgIds), 0}),
            {State, Effects} = complete(RaftIdx, CustomerId,
                                        MsgRaftIdxs, Cust0, Checked,
                                        State1),
            {State, dead_letter_effects(Discarded, State, Effects)};
        _ ->
            {State0, []}
    end;
apply(_RaftId, {return, MsgIds, CustomerId},
      #state{customers = Custs0} = State) ->
    case Custs0 of
        #{CustomerId := Cust0 = #customer{checked_out = Checked0}} ->
            Checked = maps:without(MsgIds, Checked0),
            Returned = maps:with(MsgIds, Checked0),
            MsgNumMsgs = [M || M <- maps:values(Returned)],
            return(CustomerId, MsgNumMsgs, Cust0, Checked, State);
        _ ->
            {State, []}
    end;
apply(_RaftIdx, {checkout, {dequeue, _}, {_Tag, _Pid}},
      #state{messages = M} = State0) when map_size(M) == 0 ->
    %% TODO do we need metric visibility of empty get requests?
    {State0, [], {dequeue, empty}};
apply(RaftIdx, {checkout, {dequeue, settled}, CustomerId}, State0) ->
    % TODO: this clause could probably be optimised
    State1 = update_customer(CustomerId, {once, 1}, State0),
    % turn send msg effect into reply
    {State2, [{send_msg, _, {_, _, [{MsgId, _} = M]}}]} = checkout_one(State1),
    State3 = incr_metrics(State2, {0, 1, 0, 0}),
    % immediately settle
    {State, Effects} = apply(RaftIdx, {settle, [MsgId], CustomerId}, State3),
    {State, Effects, {dequeue, M}};
apply(_RaftIdx, {checkout, {dequeue, unsettled}, {_Tag, Pid} = Customer},
      State0) ->
    State1 = update_customer(Customer, {once, 1}, State0),
    {State2, Reply} = case checkout_one(State1) of
                          {S, [{send_msg, _, {_, _, [M]}}]} ->
                              {S, M};
                          {S, []} ->
                              {S, empty}
                      end,
    State = incr_metrics(State2, {0, 1, 0, 0}),
    {State, [{monitor, process, Pid}], {dequeue, Reply}};
apply(_RaftIdx, {checkout, Spec, {_Tag, Pid} = Customer}, State0) ->
    State1 = update_customer(Customer, Spec, State0),
    {State2, Effects, Num} = checkout(State1, []),
    State = incr_metrics(State2, {0, Num, 0, 0}),
    {State, [{monitor, process, Pid} | Effects]};
apply(_RaftId, {down, CustomerPid, noconnection},
      #state{customers = Custs0,
             enqueuers = Enqs0} = State0) ->
    Node = node(CustomerPid),
    % mark all customers and enqueuers as suspect
    % and monitor the node
    Custs = maps:map(fun({_, P}, C) when node(P) =:= Node ->
                             C#customer{suspected_down = true};
                        (_, C) -> C
                     end, Custs0),
    Enqs = maps:map(fun(P, E) when node(P) =:= Node ->
                            E#enqueuer{suspected_down = true};
                       (_, E) -> E
                    end, Enqs0),
    {State0#state{customers = Custs,
                  enqueuers = Enqs}, [{monitor, node, Node}]};
apply(_RaftId, {down, Pid, _Info},
      #state{customers = Custs0,
             enqueuers = Enqs0} = State0) ->
    % remove any enqueuer for the same pid
    % TODO: if there are any pending enqueuers these should be enqueued
    % This should be ok as we won't see any more enqueues from this pid
    State1 = case maps:take(Pid, Enqs0) of
                 {_E, Enqs} ->
                    State0#state{enqueuers = Enqs};
                 error ->
                     State0
             end,
    % return checked out messages to main queue
    % Find the customers for the down pid
    DownCustomers = maps:keys(
                      maps:filter(fun({_, P}, _) -> P =:= Pid end, Custs0)),
    State = lists:foldl(
              fun(CustomerId, #state{customers = C0} = S0) ->
                      case maps:take(CustomerId, C0) of
                          {#customer{checked_out = Checked0}, Custs} ->
                              S1 = maps:fold(fun (_MsgId, {MsgNum, Msg}, S) ->
                                                     return_one(MsgNum, Msg, S)
                                             end, S0, Checked0),
                              S = incr_metrics(S1, {0, 0, 0,
                                                    maps:size(Checked0)}),
                              S#state{customers = Custs};
                          error ->
                              % already removed - do nothing
                              S0
                      end
              end, State1, DownCustomers),
    {State, []};
apply(_RaftId, {nodeup, Node},
      #state{customers = Custs0,
             enqueuers = Enqs0} = State0) ->
    Custs = maps:fold(fun({_, P}, #customer{suspected_down = true}, Acc)
                            when node(P) =:= Node ->
                              [P | Acc];
                         (_, _, Acc) -> Acc
                      end, [], Custs0),
    Enqs = maps:fold(fun(P, #enqueuer{suspected_down = true}, Acc)
                           when node(P) =:= Node ->
                             [P | Acc];
                        (_, _, Acc) -> Acc
                     end, [], Enqs0),
    Monitors = [{monitor, process, P} || P <- Custs ++ Enqs],
    % TODO: should we unsuspect these processes here?
    {State0, Monitors};
apply(_RaftId, {nodedown, _Node}, State) ->
    {State, []}.



-spec leader_effects(state()) -> ra_machine:effects().
leader_effects(#state{customers = Custs}) ->
    % return effects to monitor all current customers
    [{monitor, process, P} || {_, P} <- maps:keys(Custs)].


-spec tick(non_neg_integer(), state()) -> ra_machine:effects().
tick(_Ts, #state{metrics = Metrics}) ->
    [{mod_call, ets, insert, [?METRICS_TABLE, Metrics]}].

overview(#state{customers = Custs,
                ra_indexes = Indexes}) ->
    #{type => ?MODULE,
      num_customers => maps:size(Custs),
      num_messages => ra_fifo_index:size(Indexes)}.

-spec get_checked_out(customer_id(), msg_id(), msg_id(), state()) ->
    [delivery_msg()].
get_checked_out(Cid, From, To, #state{customers = Customers}) ->
    case Customers of
        #{Cid := #customer{checked_out = Checked}} ->
            [{K, snd(snd(maps:get(K, Checked)))} || K <- lists:seq(From, To)];
        _ ->
            []
    end.


%%% Queries

query_messages_ready(#state{messages = M}) ->
    M.

query_messages_checked_out(#state{customers = Customers}) ->
    maps:fold(fun (_, #customer{checked_out = C}, S) ->
                      maps:merge(S, maps:from_list(maps:values(C)))
              end, #{}, Customers).

query_processes(#state{enqueuers = Enqs, customers = Custs0}) ->
    Custs = maps:fold(fun({_, P}, V, S) -> S#{P => V} end, #{}, Custs0),
    maps:keys(maps:merge(Enqs, Custs)).


%%% Internal

incr_enqueue_count(#state{enqueue_count = C} = State)
 when C =:= ?SHADOW_COPY_INTERVAL ->
    {State#state{enqueue_count = 1}, shadow_copy(State)};
incr_enqueue_count(#state{enqueue_count = C} = State) ->
    {State#state{enqueue_count = C + 1}, undefined}.

enqueue(RaftIdx, RawMsg, #state{messages = Messages,
                                low_msg_num = LowMsgNum,
                                next_msg_num = NextMsgNum} = State0) ->
    Msg = {RaftIdx, {#{}, RawMsg}}, % indexed message with header map
    State0#state{messages = Messages#{NextMsgNum => Msg},
                 low_msg_num = min(LowMsgNum, NextMsgNum),
                 next_msg_num = NextMsgNum + 1}.

append_to_master_index(RaftIdx,
                       #state{ra_indexes = Indexes0} = State0) ->
    {State, Shadow} = incr_enqueue_count(State0),
    Indexes = ra_fifo_index:append(RaftIdx, Shadow, Indexes0),
    State#state{ra_indexes = Indexes}.

enqueue_pending(From,
                #enqueuer{next_seqno = Next,
                          pending = [{Next, RaftIdx, RawMsg} | Pending]} = Enq0,
                State0) ->
            State = enqueue(RaftIdx, RawMsg, State0),
            Enq = Enq0#enqueuer{next_seqno = Next + 1, pending = Pending},
            enqueue_pending(From, Enq, State);
enqueue_pending(From, Enq, #state{enqueuers = Enqueuers0} = State) ->
    State#state{enqueuers = Enqueuers0#{From => Enq}}.

maybe_enqueue(RaftIdx, undefined, undefined, RawMsg,
              State0) ->
    % direct enqueue without tracking
    {enqueue(RaftIdx, RawMsg, State0), []};
maybe_enqueue(RaftIdx, From, MsgSeqNo, RawMsg,
              #state{enqueuers = Enqueuers0} = State0) ->
    case maps:get(From, Enqueuers0, undefined) of
        undefined ->
            State1 = State0#state{enqueuers = Enqueuers0#{From => #enqueuer{}}},
            {State, Effects} = maybe_enqueue(RaftIdx, From, MsgSeqNo,
                                             RawMsg, State1),
            {State, [{monitor, process, From} | Effects]};
        #enqueuer{next_seqno = MsgSeqNo,
                  pending = _Pending} = Enq0 ->
            % it is the next expected seqno
            State1 = enqueue(RaftIdx, RawMsg, State0),
            Enq = Enq0#enqueuer{next_seqno = MsgSeqNo + 1},
            State = enqueue_pending(From, Enq, State1),
            {State, []};
        #enqueuer{next_seqno = Next,
                  pending = Pending0} = Enq0
          when MsgSeqNo > Next ->
            % out of order delivery
            Pending = [{MsgSeqNo, RaftIdx, RawMsg} | Pending0],
            Enq = Enq0#enqueuer{pending = lists:sort(Pending)},
            {State0#state{enqueuers = Enqueuers0#{From => Enq}}, []};
        #enqueuer{next_seqno = Next, pending = _Pending} = _Enq0
          when MsgSeqNo =< Next ->
            % duplicate delivery
            {State0, []}
    end.
snd(T) ->
    element(2, T).

incr_metrics(#state{metrics = {N, E0, C0, S0, R0}} = State, {E, C, S, R}) ->
    State#state{metrics = {N, E0 + E, C0 + C, S0 + S, R0 + R}}.

return(CustomerId, MsgNumMsgs, Cust0, Checked,
       #state{customers = Custs0, service_queue = SQ0} = State0) ->
    Num = length(MsgNumMsgs),
    Cust = Cust0#customer{checked_out = Checked,
                          seen = Cust0#customer.seen - Num},
    {Custs, SQ, Effects0} = update_or_remove_sub(CustomerId, Cust, Custs0, SQ0),
    State1 = lists:foldl(fun({MsgNum, Msg}, S0) ->
                                 return_one(MsgNum, Msg, S0)
                         end, State0, MsgNumMsgs),
    {State2, Effects, NumChecked} = checkout(State1#state{customers = Custs,
                                                          service_queue = SQ},
                                             Effects0),
    State = incr_metrics(State2, {0, NumChecked, 0, Num}),
    {State, Effects}.

% used to processes messages that are finished
complete(IncomingRaftIdx, CustomerId, MsgRaftIdxs, Cust0, Checked,
       #state{customers = Custs0, service_queue = SQ0,
              ra_indexes = Indexes0} = State0) ->
    Cust = Cust0#customer{checked_out = Checked},
    {Custs, SQ, Effects0} = update_or_remove_sub(CustomerId, Cust, Custs0, SQ0),
    Indexes = lists:foldl(fun ra_fifo_index:delete/2, Indexes0, MsgRaftIdxs),
    {State1, Effects, NumChecked} =
        checkout(State0#state{customers = Custs,
                              ra_indexes = Indexes,
                              service_queue = SQ},
                 Effects0),
    % settle metrics are incremented separately
    State = incr_metrics(State1, {0, NumChecked, 0, 0}),
    % TODO this could probably be made more efficient
    update_smallest_raft_index(IncomingRaftIdx, {State, Effects}).
    % lists:foldl(
    %   fun (MsgRaftIdx, Acc) ->
    %           update_smallest_raft_index(IncomingRaftIdx, MsgRaftIdx, Acc)
    %   end, {State, Effects}, MsgRaftIdxs).

dead_letter_effects(_Discarded,
                    #state{dead_letter_handler = undefined},
                    Effects) ->
    Effects;
dead_letter_effects(Discarded,
                    #state{dead_letter_handler = {Mod, Fun, Args}}, Effects) ->
    DeadLetters = maps:fold(fun(_MsgId, {_MsgIdId, {_RaftId, {_Header, Msg}}},
                          Acc) ->
                      [{rejected, Msg} | Acc]
              end, [], Discarded),
    [{mod_call, Mod, Fun, Args ++ [DeadLetters]} | Effects].

update_smallest_raft_index(IncomingRaftIdx,
                           {#state{ra_indexes = Indexes} = State, Effects}) ->
    case ra_fifo_index:size(Indexes) of
        0 ->
            % there are no messages on queue anymore and no pending enqueues
            % we can forward release_cursor all the way until
            % the last received command
            {State,
             [{release_cursor, IncomingRaftIdx, shadow_copy(State)} | Effects]};
        _ ->
            % TODO: for simplicity can we just always take the smallest here?
            % Then we won't need the MsgRaftIdx
            case ra_fifo_index:smallest(Indexes) of
                 {_Smallest, undefined} ->
                    % no shadow taken for this index,
                    % no release cursor increase
                    {State, Effects};
                 {Smallest, Shadow} ->
                    % we emit the last index _not_ to contribute to the
                    % current state - hence the -1
                    {State, [{release_cursor, Smallest - 1, Shadow} | Effects]}
            end
    end.

return_one(MsgNum, {RaftId, {Header0, RawMsg}},
           #state{messages = Messages, low_msg_num = Low0} = State0) ->

    Header = maps:update_with(delivery_count,
                              fun (C) -> C+1 end,
                              1, Header0),
    Msg = {RaftId, {Header, RawMsg}},
    % this should not affect the release cursor in any way
    State0#state{messages = maps:put(MsgNum, Msg, Messages),
                 low_msg_num = min(MsgNum, Low0)}.


checkout(State, Effects) ->
    checkout0(checkout_one(State), Effects, 0).

checkout0({State, []}, Effects, Num) ->
    {State, lists:reverse(Effects), Num};
checkout0({State, Efxs}, Effects, Num) ->
    checkout0(checkout_one(State), Efxs ++ Effects, Num + 1).

checkout_one(#state{messages = Messages0,
                    low_msg_num = Low0,
                    next_msg_num = NextMsgNum,
                    service_queue = SQ0,
                    customers = Custs0} = State0) ->
    % messages are available
    case maps:take(Low0, Messages0) of
        {{_, Msg} = IdxMsg, Messages} ->
            % there are customers waiting to be serviced
            case queue:out(SQ0) of
                {{value, {CTag, CPid} = CustomerId}, SQ1} ->
                    % process customer checkout
                    case maps:get(CustomerId, Custs0, undefined) of
                        #customer{checked_out = Checked0,
                                  next_msg_id = Next,
                                  seen = Seen} = Cust0 ->
                            Checked = maps:put(Next, {Low0, IdxMsg}, Checked0),
                            Cust = Cust0#customer{checked_out = Checked,
                                                  next_msg_id = Next+1,
                                                  seen = Seen+1},
                            {Custs, SQ, []} = % we expect no effects
                                update_or_remove_sub(CustomerId, Cust, Custs0, SQ1),
                            Low = new_low(Low0, NextMsgNum, Messages),
                            State = State0#state{service_queue = SQ,
                                                 low_msg_num = Low, %/ra_fifo_index:next_key_after(LowIdx, Indexes),
                                                 messages = Messages,
                                                 customers = Custs},
                            {State, [{send_msg, CPid, {delivery, CTag, [{Next, Msg}]}}]};
                        undefined ->
                            % customer did not exist but was queued, recurse
                            checkout_one(State0#state{service_queue = SQ1})
                    end;
                _ ->
                    {State0, []}
            end;
        error ->
            {State0, []}
    end.

new_low(_Prev, _Max, Messages) when map_size(Messages) =:= 0 ->
    undefined;
new_low(_Prev, _Max, Messages) when map_size(Messages) < 100 ->
    % guesstimate value - needs measuring
    lists:min(maps:keys(Messages));
new_low(Prev, Max, Messages) ->
    walk_map(Prev+1, Max, Messages).

walk_map(N, Max, Map) when N =< Max ->
    case maps:is_key(N, Map) of
        true -> N;
        false ->
            walk_map(N+1, Max, Map)
    end;
walk_map(_, _, _) ->
    undefined.


update_or_remove_sub(CustomerId, #customer{lifetime = once,
                                           checked_out = Checked,
                                           num = N, seen = N} = Cust,
                     Custs, ServiceQueue) ->
    case maps:size(Checked)  of
        0 ->
            % we're done with this customer
            {maps:remove(CustomerId, Custs), ServiceQueue,
             [{demonitor, CustomerId}]};
        _ ->
            % there are unsettled items so need to keep around
            {maps:update(CustomerId, Cust, Custs), ServiceQueue, []}
    end;
update_or_remove_sub(CustomerId, #customer{lifetime = once} = Cust,
                     Custs, ServiceQueue) ->
    {maps:update(CustomerId, Cust, Custs),
     uniq_queue_in(CustomerId, ServiceQueue), []};
update_or_remove_sub(CustomerId, #customer{lifetime = auto,
                                           checked_out = Checked,
                                           num = Num} = Cust,
                     Custs, ServiceQueue) ->
    case maps:size(Checked) < Num of
        true ->
            {maps:update(CustomerId, Cust, Custs),
             uniq_queue_in(CustomerId, ServiceQueue), []};
        false ->
            {maps:update(CustomerId, Cust, Custs), ServiceQueue, []}
    end.

uniq_queue_in(Key, Queue) ->
    % TODO: queue:member could surely be quite expensive, however the practical
    % number of unique customers may not be large enough for it to matter
    case queue:member(Key, Queue) of
        true ->
            Queue;
        false ->
            queue:in(Key, Queue)
    end.


update_customer(CustomerId, {Life, Num},
                #state{customers = Custs0,
                       service_queue = ServiceQueue0} = State0) ->
    Init = #customer{lifetime = Life, num = Num},
    Custs = maps:update_with(CustomerId,
                             fun(S) ->
                                     S#customer{lifetime = Life, num = Num}
                             end, Init, Custs0),
    ServiceQueue = maybe_queue_customer(CustomerId, maps:get(CustomerId, Custs),
                                        ServiceQueue0),

    State0#state{customers = Custs, service_queue = ServiceQueue}.

maybe_queue_customer(CustomerId, #customer{checked_out = Checked, num = Num},
                     ServiceQueue0) ->
    case maps:size(Checked) of
        Size when Size < Num ->
            % customerect needs service - check if already on service queue
            case queue:member(CustomerId, ServiceQueue0) of
                true ->
                    ServiceQueue0;
                false ->
                    queue:in(CustomerId, ServiceQueue0)
            end;
        _ ->
            ServiceQueue0
    end.


size_test(NumMsg, NumCust) ->
    EnqGen = fun(N) -> {N, {enqueue, N}} end,
    CustGen = fun(N) -> {N, {checkout, {auto, 100}, spawn(fun() -> ok end)}} end,
    S0 = run_log(1, NumMsg, EnqGen, init(#{name => size_test})),
    S = run_log(NumMsg, NumMsg + NumCust, CustGen, S0),
    S2 = S#state{ra_indexes = ra_fifo_index:map(fun(_, _) -> undefined end,
                                                S#state.ra_indexes)},
    {erts_debug:size(S), erts_debug:size(S2)}.

perf_test(NumMsg, NumCust) ->
    timer:tc(
      fun () ->
              EnqGen = fun(N) -> {N, {enqueue, self(), N, N}} end,
              Pid = spawn(fun() -> ok end),
              CustGen = fun(N) -> {N, {checkout, {auto, NumMsg}, Pid}} end,
              SetlGen = fun(N) -> {N, {settle, N - NumMsg - NumCust - 1, Pid}} end,
              S0 = run_log(1, NumMsg, EnqGen,
                           element(1, init(#{name => size_test}))),
              S1 = run_log(NumMsg, NumMsg + NumCust, CustGen, S0),
              _ = run_log(NumMsg, NumMsg + NumCust + NumMsg, SetlGen, S1),
              ok
             end).

% profile(File) ->
%     GzFile = atom_to_list(File) ++ ".gz",
%     lg:trace([ra_fifo, maps, queue, ra_fifo_index], lg_file_tracer,
%              GzFile, #{running => false, mode => profile}),
%     NumMsg = 10000,
%     NumCust = 500,
%     EnqGen = fun(N) -> {N, {enqueue, N}} end,
%     Pid = spawn(fun() -> ok end),
%     CustGen = fun(N) -> {N, {checkout, {auto, NumMsg}, Pid}} end,
%     SetlGen = fun(N) -> {N, {settle, N - NumMsg - NumCust - 1, Pid}} end,
%     S0 = run_log(1, NumMsg, EnqGen, element(1, init(size_test))),
%     S1 = run_log(NumMsg, NumMsg + NumCust, CustGen, S0),
%     _ = run_log(NumMsg, NumMsg + NumCust + NumMsg, SetlGen, S1),
%     lg:stop().


run_log(Num, Num, _Gen, State) ->
    State;
run_log(Num, Max, Gen, State0) ->
    {_, E} = Gen(Num),
    run_log(Num+1, Max, Gen, element(1, apply(Num, E, State0))).

shadow_copy(#state{customers = Customers,
                   enqueuers = Enqueuers0} = State) ->
    Enqueuers = maps:map(fun (_, E) -> E#enqueuer{pending = []}
                         end, Enqueuers0),
    % creates a copy of the current state suitable for snapshotting
    State#state{messages = #{},
                ra_indexes = ra_fifo_index:empty(),
                low_msg_num = undefined,
                % TODO: optimise
                % this is inefficient (from a memory use point of view)
                % as it creates a new tuple for every customer
                % even if they haven't changed instead we could just update a copy
                % of the last dehydrated state with the difference
                customers = maps:map(fun (_, V) ->
                                             V#customer{checked_out = #{}}
                                     end, Customers),
                enqueuers = Enqueuers
               }.


-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

-define(assertEffect(EfxPat, Effects),
        ?assertEffect(EfxPat, true, Effects)).

-define(assertEffect(EfxPat, Guard, Effects),
    ?assert(lists:any(fun (EfxPat) when Guard -> true;
                          (_) -> false
                      end, Effects))).

-define(assertNoEffect(EfxPat, Effects),
    ?assert(not lists:any(fun (EfxPat) -> true;
                          (_) -> false
                      end, Effects))).

ensure_ets() ->
    case ets:info(ra_fifo_metrics) of
        undefined ->
            _ = ets:new(ra_fifo_metrics,
                        [public, named_table, {write_concurrency, true}]);
        _ ->
            ok
    end.

test_init(Name) ->
    element(1, init(#{name => Name})).

enq_enq_checkout_test() ->
    Cid = {<<"enq_enq_checkout_test">>, self()},
    {State1, _} = enq(1, 1, first, test_init(test)),
    {State2, _} = enq(2, 2, second, State1),
    {_State3, Effects} =
        apply(3, {checkout, {once, 2}, Cid}, State2),
    ?assertEffect({monitor, _, _}, Effects),
    ?assertEffect({send_msg, _, {delivery, _, _}}, Effects),
    ok.

enq_enq_deq_test() ->
    Cid = {<<"enq_enq_checkout_get_test">>, self()},
    {State1, _} = enq(1, 1, first, test_init(test)),
    {State2, _} = enq(2, 2, second, State1),
    % get returns a reply value
    {_State3, [{monitor, _, _}], {dequeue, {0, {_, first}}}} =
        apply(3, {checkout, {dequeue, unsettled}, Cid}, State2),
    ok.

enq_enq_deq_deq_settle_test() ->
    Cid = {<<"enq_enq_checkout_get_test">>, self()},
    {State1, _} = enq(1, 1, first, test_init(test)),
    {State2, _} = enq(2, 2, second, State1),
    % get returns a reply value
    {State3, [{monitor, _, _}], {dequeue, {0, {_, first}}}} =
        apply(3, {checkout, {dequeue, unsettled}, Cid}, State2),
    {_State4, _Effects4, {dequeue, empty}} =
        apply(3, {checkout, {dequeue, unsettled}, Cid}, State3),
    ok.

enq_enq_checkout_get_settled_test() ->
    ensure_ets(),
    Cid = {<<"enq_enq_checkout_get_test">>, self()},
    {State1, _} = enq(1, 1, first, test_init(test)),
    % get returns a reply value
    {_State2, _Effects, {dequeue, {0, {_, first}}}} =
        apply(3, {checkout, {dequeue, settled}, Cid}, State1),
    ok.

checkout_get_empty_test() ->
    ensure_ets(),
    Cid = {<<"checkout_get_empty_test">>, self()},
    State = test_init(test),
    {_State2, [], {dequeue, empty}} =
        apply(1, {checkout, {dequeue, unsettled}, Cid}, State),
    ok.

untracked_enq_deq_test() ->
    Cid = {<<"untracked_enq_deq_test">>, self()},
    State0 = test_init(test),
    {State1, _} = apply(1, {enqueue, undefined, undefined, first}, State0),
    {_State2, _, {dequeue, {0, {_, first}}}} =
        apply(3, {checkout, {dequeue, settled}, Cid}, State1),
    ok.
release_cursor_test() ->
    ensure_ets(),
    Cid = {<<"release_cursor_test">>, self()},
    {State1, _} = enq(1, 1, first,  test_init(test)),
    {State2, _} = enq(2, 2, second, State1),
    {State3, _} = check(Cid, 3, 10, State2),
    % no release cursor effect at this point
    {State4, []} = settle(Cid, 4, 1, State3),
    {_Final, Effects1} = settle(Cid, 5, 0, State4),
    % empty queue forwards release cursor all the way
    ?assertEffect({release_cursor, 5, _}, Effects1),
    ok.

checkout_enq_settle_test() ->
    ensure_ets(),
    Cid = {<<"checkout_enq_settle_test">>, self()},
    {State1, [{monitor, _, _}]} = check(Cid, 1, test_init(test)),
    {State2, Effects0} = enq(2, 1,  first, State1),
    ?assertEffect({send_msg, _,
                   {delivery, <<"checkout_enq_settle_test">>,
                    [{0, {_, first}}]}},
                  Effects0),
    {State3, []} = enq(3, 2, second, State2),
    {_, _Effects} = settle(Cid, 4, 0, State3),
    % the release cursor is the smallest raft index that does not
    % contribute to the state of the application
    % ?assertEffect({release_cursor, 2, _}, Effects),
    ok.

out_of_order_enqueue_test() ->
    Cid = {<<"out_of_order_enqueue_test">>, self()},
    {State1, [{monitor, _, _}]} = check_n(Cid, 5, 5, test_init(test)),
    {State2, Effects2} = enq(2, 1, first, State1),
    ?assertEffect({send_msg, _, {delivery, _, [{_, {_, first}}]}}, Effects2),
    % assert monitor was set up
    ?assertEffect({monitor, _, _}, Effects2),
    % enqueue seq num 3 and 4 before 2
    {State3, Effects3} = enq(3, 3, third, State2),
    ?assertNoEffect({send_msg, _, {delivery, _, _}}, Effects3),
    {State4, Effects4} = enq(4, 4, fourth, State3),
    % assert no further deliveries where made
    ?assertNoEffect({send_msg, _, {delivery, _, _}}, Effects4),
    {_State5, Effects5} = enq(5, 2, second, State4),
    % assert two deliveries were now made
    ?assertEffect({send_msg, _, {delivery, _, [{_, {_, second}}]}}, Effects5),
    ?assertEffect({send_msg, _, {delivery, _, [{_, {_, third}}]}}, Effects5),
    % assert order of deliviers
    Deliveries = lists:filtermap(
                   fun({send_msg, _, {delivery,_, [{_, {_, M}}]}}) ->
                           {true, M};
                      (_) ->
                           false
                   end, Effects5),
    [second, third, fourth] = Deliveries,

    ok.

out_of_order_first_enqueue_test() ->
    Cid = {<<"out_of_order_enqueue_test">>, self()},
    {State1, _} = check_n(Cid, 5, 5, test_init(test)),
    {_State2, Effects2} = enq(2, 10, first, State1),
    ?assertEffect({monitor, process, _}, Effects2),
    ?assertNoEffect({send_msg, _, {delivery, _, [{_, {_, first}}]}}, Effects2),
    ok.

duplicate_enqueue_test() ->
    Cid = {<<"duplicate_enqueue_test">>, self()},
    {State1, [{monitor, _, _}]} = check_n(Cid, 5, 5, test_init(test)),
    {State2, Effects2} = enq(2, 1, first, State1),
    ?assertEffect({send_msg, _, {delivery, _, [{_, {_, first}}]}}, Effects2),
    {_State3, Effects3} = enq(3, 1, first, State2),
    ?assertNoEffect({send_msg, _, {delivery, _, [{_, {_, first}}]}}, Effects3),
    ok.

return_non_existent_test() ->
    Cid = {<<"cid">>, self()},
    {State0, [_]} = enq(1, 1, second, test_init(test)),
    % return non-existent
    {_State2, []} = apply(3, {return, [99], Cid}, State0),
    ok.

return_checked_out_test() ->
    Cid = {<<"cid">>, self()},
    {State0, [_]} = enq(1, 1, first, test_init(test)),
    {State1, [_Monitor, {send_msg, _, {delivery, _, [{MsgId, _}]}}]} =
        check(Cid, 2, State0),
    % return
    {_State2, [_]} = apply(3, {return, [MsgId], Cid}, State1),
    % {_, _, {get, {0, first}}} = deq(Cid, 4, State2),
    ok.

return_auto_checked_out_test() ->
    Cid = {<<"cid">>, self()},
    {State00, [_]} = enq(1, 1, first, test_init(test)),
    {State0, []} = enq(2, 2, second, State00),
    {State1, [_Monitor, {send_msg, _, {delivery, _, [{MsgId, _}]}}]} =
        check_auto(Cid, 2, State0),
    % return should include another delivery
    {_State2, Effects} = apply(3, {return, [MsgId], Cid}, State1),
    ?assertEffect({send_msg, _,
                   {delivery, _, [{_, {#{delivery_count := 1}, first}}]}},
                  Effects),
    ok.

down_with_noproc_customer_returns_unsettled_test() ->
    Cid = {<<"down_customer_returns_unsettled_test">>, self()},
    {State0, [_]} = enq(1, 1, second, test_init(test)),
    {State1, [{monitor, process, Pid}, _Del]} = check(Cid, 2, State0),
    {State2, []} = apply(3, {down, Pid, noproc}, State1),
    {_State, Effects} = check(Cid, 4, State2),
    ?assertEffect({monitor, process, _}, Effects),
    ok.

down_with_noconnection_marks_suspect_and_node_is_monitored_test() ->
    Pid = spawn(fun() -> ok end),
    Cid = {<<"down_with_noconnect">>, Pid},
    Self = self(),
    Node = node(Pid),
    {State0, Effects0} = enq(1, 1, second, test_init(test)),
    ?assertEffect({monitor, process, P}, P =:= Self, Effects0),
    {State1, Effects1} = check(Cid, 2, State0),
    ?assertEffect({monitor, process, P}, P =:= Pid, Effects1),
    % monitor both enqueuer and customer
    % because we received a noconnection we now need to monitor the node
    {State2a, _Effects2a} = apply(3, {down, Pid, noconnection}, State1),
    {State2, Effects2} = apply(3, {down, Self, noconnection}, State2a),
    ?assertEffect({monitor, node, _}, Effects2),
    ?assertNoEffect({demonitor, process, _}, Effects2),
    % when the node comes up we need to retry the process monitors for the
    % disconnected processes
    {_State3, Effects3} = apply(3, {nodeup, Node}, State2),
    % try to re-monitor the suspect processes
    ?assertEffect({monitor, process, P}, P =:= Pid, Effects3),
    ?assertEffect({monitor, process, P}, P =:= Self, Effects3),
    ok.

down_with_noproc_enqueuer_is_cleaned_up_test() ->
    State00 = test_init(test),
    Pid = spawn(fun() -> ok end),
    {State0, Effects0} = apply(1, {enqueue, Pid, 1, first}, State00),
    ?assertEffect({monitor, process, _}, Effects0),
    {State1, _Effects1} = apply(3, {down, Pid, noproc}, State0),
    % ensure there are no enqueuers
    ?assert(0 =:= maps:size(State1#state.enqueuers)),
    ok.

completed_customer_yields_demonitor_effect_test() ->
    Cid = {<<"completed_customer_yields_demonitor_effect_test">>, self()},
    {State0, [_]} = enq(1, 1, second, test_init(test)),
    {State1, [{monitor, process, _}, _Msg]} = check(Cid, 2, State0),
    {_, Effects} = settle(Cid, 3, 0, State1),
    ?assertEffect({demonitor, _}, Effects),
    % release cursor for empty queue
    ?assertEffect({release_cursor, 3, _}, Effects),
    ok.

discarded_message_without_dead_letter_handler_is_removed_test() ->
    Cid = {<<"completed_customer_yields_demonitor_effect_test">>, self()},
    {State0, [_]} = enq(1, 1, first, test_init(test)),
    {State1, Effects1} = check_n(Cid, 2, 10, State0),
    ?assertEffect({send_msg, _,
                   {delivery, _, [{0, {#{}, first}}]}},
                  Effects1),
    {_State2, Effects2} = apply(1, {discard, [0], Cid}, State1),
    ?assertNoEffect({send_msg, _,
                     {delivery, _, [{0, {#{}, first}}]}},
                    Effects2),
    ok.

discarded_message_with_dead_letter_handler_emits_mod_call_effect_test() ->
    Cid = {<<"completed_customer_yields_demonitor_effect_test">>, self()},
    {State00, _} = init(#{name => test,
                          dead_letter_handler =>
                          {somemod, somefun, [somearg]}}),
    {State0, [_]} = enq(1, 1, first, State00),
    {State1, Effects1} = check_n(Cid, 2, 10, State0),
    ?assertEffect({send_msg, _,
                   {delivery, _, [{0, {#{}, first}}]}},
                  Effects1),
    {_State2, Effects2} = apply(1, {discard, [0], Cid}, State1),
    % assert mod call effect with appended reason and message
    ?assertEffect({mod_call, somemod, somefun, [somearg, [{rejected, first}]]},
                  Effects2),
    ok.

tick_test() ->
    {State0, [_]} = enq(1, 1, second, test_init(test)),
    [{mod_call, ets, insert, [?METRICS_TABLE, {test, 1, 0, 0, 0}]}] =
        tick(1, State0),
    ok.

release_cursor_snapshot_state_test() ->
    Tag = <<"release_cursor_snapshot_state_test">>,
    Cid = {Tag, self()},
    OthPid = spawn(fun () -> ok end),
    Oth = {<<"oth">>, OthPid},
    Commands = [
                {checkout, {auto, 5}, Cid},
                {enqueue, self(), 1, 0},
                {enqueue, self(), 2, 1},
                {settle, [0], Cid},
                {enqueue, self(), 3, 2},
                {settle, [1], Cid},
                {checkout, {auto, 4}, Oth},
                {enqueue, self(), 4, 3},
                {enqueue, self(), 5, 4},
                {settle, [2, 3], Cid},
                {enqueue, self(), 6, 5},
                {settle, [0], Oth},
                {enqueue, self(), 7, 6},
                {settle, [1], Oth},
                {settle, [4], Cid},
                {checkout, {once, 0}, Oth}
              ],
    Indexes = lists:seq(1, length(Commands)),
    Entries = lists:zip(Indexes, Commands),
    {State, Effects} = run_log(test_init(help), Entries),

    [begin
         Filtered = lists:dropwhile(fun({X, _}) when X =< SnapIdx -> true;
                                       (_) -> false
                                    end, Entries),
         {S, _} = run_log(SnapState, Filtered),
         % assert log can be restored from any release cursor index
         % ?debugFmt("Idx ~p S~p~nState~p~nSnapState ~p~nFiltered ~p~n",
         %           [SnapIdx, S, State, SnapState, Filtered]),
         ?assertEqual(State, S)
     end || {release_cursor, SnapIdx, SnapState} <- Effects],
    ok.

delivery_query_returns_deliveries_test() ->
    Tag = <<"release_cursor_snapshot_state_test">>,
    Cid = {Tag, self()},
    Commands = [
                {checkout, {auto, 5}, Cid},
                {enqueue, self(), 1, one},
                {enqueue, self(), 2, two},
                {enqueue, self(), 3, tre},
                {enqueue, self(), 4, for}
              ],
    Indexes = lists:seq(1, length(Commands)),
    Entries = lists:zip(Indexes, Commands),
    {State, _Effects} = run_log(test_init(help), Entries),
    % 3 deliveries are returned
    [{0, {#{}, one}}] = get_checked_out(Cid, 0, 0, State),
    [_, _, _] = get_checked_out(Cid, 1, 3, State),
    ok.

performance_test() ->
    ensure_ets(),
    % just under ~200ms on my machine [Karl]
    NumMsgs = 100000,
    {Taken, _} = perf_test(NumMsgs, 0),
    ?debugFmt("performance_test took ~p ms for ~p messages",
              [Taken / 1000, NumMsgs]),
    ok.

enq(Idx, MsgSeq, Msg, State) ->
    apply(Idx, {enqueue, self(), MsgSeq, Msg}, State).

% deq(Cid, Idx, State) ->
%     apply(Idx, {checkout, {get, settled}, Cid}, State).

check_n(Cid, Idx, N, State) ->
    apply(Idx, {checkout, {auto, N}, Cid}, State).

check(Cid, Idx, State) ->
    apply(Idx, {checkout, {once, 1}, Cid}, State).

check_auto(Cid, Idx, State) ->
    apply(Idx, {checkout, {auto, 1}, Cid}, State).

check(Cid, Idx, Num, State) ->
    apply(Idx, {checkout, {once, Num}, Cid}, State).

settle(Cid, Idx, MsgId, State) ->
    apply(Idx, {settle, [MsgId], Cid}, State).

run_log(InitState, Entries) ->
    lists:foldl(fun ({Idx, E}, {Acc0, Efx0}) ->
                        {Acc, Efx} = apply(Idx, E, Acc0),
                        {Acc, Efx0 ++ Efx}
                end, {InitState, []}, Entries).
-endif.

