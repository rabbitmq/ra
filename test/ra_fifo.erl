-module(ra_fifo).

-behaviour(ra_machine).

-compile(inline_list_funcs).
-compile(inline).
-compile({no_auto_import, [apply/3]}).

-include("src/ra.hrl").

-include_lib("eunit/include/eunit.hrl").
-export([
         init/1,
         apply/3,
         state_enter/2,
         tick/2,
         overview/1,
         get_checked_out/4,
         %% aux
         init_aux/1,
         handle_aux/6,
         % queries
         query_messages_ready/1,
         query_messages_checked_out/1,
         query_processes/1,
         query_ra_indexes/1,
         query_customer_count/1,

         usage/1,

         %% misc
         dehydrate_state/1,
         size_test/2,
         perf_test/2,
         read_log/0

         % profile/1
         %
        ]).

-ifdef(TEST).
-export([
         metrics_handler/1
        ]).
-endif.

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

-type indexed_msg() :: {ra:index(), msg()}.

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
                         {dequeue, settled | unsettled} |
                         cancel.

-type protocol() ::
    {enqueue, Sender :: option(pid()), MsgSeq :: option(msg_seqno()),
     Msg :: raw_msg()} |
    {checkout, Spec :: checkout_spec(), Customer :: customer_id()} |
    {settle, MsgIds :: [msg_id()], Customer :: customer_id()} |
    {return, MsgIds :: [msg_id()], Customer :: customer_id()} |
    {discard, MsgIds :: [msg_id()], Customer :: customer_id()} |
    purge.

-type command() :: protocol() | ra_machine:builtin_command().
%% all the command types supported by ra fifo

-type client_msg() :: delivery().
%% the messages `ra_fifo' can send to customers.

-type applied_mfa() :: {module(), atom(), list()}.
% represents a partially applied module call

-define(SHADOW_COPY_INTERVAL, 4096).
% metrics tuple format:
% {Key, Ready, Pending, Total}
-define(USE_AVG_HALF_LIFE, 10000.0).

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
         pending = [] :: [{msg_seqno(), ra:index(), raw_msg()}],
         suspected_down = false :: boolean()
        }).

-record(state,
        {name :: atom(),
         shadow_copy_interval = ?SHADOW_COPY_INTERVAL :: non_neg_integer(),
         % unassigned messages
         messages = #{} :: #{msg_in_id() => indexed_msg()},
         % defines the lowest message in id available in the messages map
         % that isn't a return
         low_msg_num :: msg_in_id() | undefined,
         % defines the next message in id to be added to the messages map
         next_msg_num = 1 :: msg_in_id(),
         % list of returned msg_in_ids - when checking out it picks from
         % this list first before taking low_msg_num
         returns = queue:new() :: queue:queue(msg_in_id()),
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
         dead_letter_handler :: option(applied_mfa()),
         cancel_customer_handler :: option(applied_mfa()),
         become_leader_handler :: option(applied_mfa()),
         metrics_handler :: option(applied_mfa()),
         prefix_msg_count = 0 :: non_neg_integer()
        }).

-opaque state() :: #state{}.

-type config() :: #{name := atom(),
                    machine_version := ra_machine:version(),
                    dead_letter_handler => applied_mfa(),
                    become_leader_handler => applied_mfa(),
                    cancel_customer_handler => applied_mfa(),
                    metrics_handler => applied_mfa(),
                    shadow_copy_interval => non_neg_integer()}.

-export_type([protocol/0,
              delivery/0,
              command/0,
              customer_tag/0,
              customer_id/0,
              client_msg/0,
              msg/0,
              msg_id/0,
              msg_seqno/0,
              delivery_msg/0,
              state/0,
              config/0]).

-spec init(config()) -> state().
init(#{name := Name} = Conf) ->
    DLH = maps:get(dead_letter_handler, Conf, undefined),
    CCH = maps:get(cancel_customer_handler, Conf, undefined),
    BLH = maps:get(become_leader_handler, Conf, undefined),
    MH = maps:get(metrics_handler, Conf, undefined),
    SHI = maps:get(shadow_copy_interval, Conf, ?SHADOW_COPY_INTERVAL),
    #state{name = Name,
            dead_letter_handler = DLH,
            cancel_customer_handler = CCH,
            become_leader_handler = BLH,
            metrics_handler = MH,
            shadow_copy_interval = SHI}.



% msg_ids are scoped per customer
% ra_indexes holds all raft indexes for enqueues currently on queue
-spec apply(ra_machine:command_meta_data(), command(), state()) ->
    {state(), term(), ra_machine:effects()} | {state(), term()}.
apply(#{index := RaftIdx}, {enqueue, From, Seq, RawMsg}, State00) ->
    case maybe_enqueue(RaftIdx, From, Seq, RawMsg, [], State00) of
        {ok, State0, Effects} ->
            State = append_to_master_index(RaftIdx, State0),
            checkout(State, Effects);
        {duplicate, State, Effects} ->
            {State, ok, Effects}
    end;
apply(#{index := RaftIdx}, {settle, MsgIds, CustomerId},
      #state{customers = Custs0} = State) ->
    case Custs0 of
        #{CustomerId := Cust0} ->
            % need to increment metrics before completing as any snapshot
            % states taken need to include them
            complete_and_checkout(RaftIdx, MsgIds, CustomerId,
                                  Cust0, [], State);
        _ ->
            {State, ok}
    end;
apply(#{index := RaftIdx}, {discard, MsgIds, CustomerId},
      #state{customers = Custs0} = State0) ->
    case Custs0 of
        #{CustomerId := Cust0} ->
            Discarded = maps:with(MsgIds, Cust0#customer.checked_out),
            Effects0 = dead_letter_effects(Discarded, State0, []),
            complete_and_checkout(RaftIdx, MsgIds, CustomerId, Cust0,
                                  Effects0, State0);
        _ ->
            {State0, ok}
    end;
apply(_, {return, MsgIds, CustomerId},
      #state{customers = Custs0} = State) ->
    case Custs0 of
        #{CustomerId := Cust0 = #customer{checked_out = Checked0}} ->
            Checked = maps:without(MsgIds, Checked0),
            Returned = maps:with(MsgIds, Checked0),
            MsgNumMsgs = [M || M <- maps:values(Returned)],
            return(CustomerId, MsgNumMsgs, Cust0, Checked, [], State);
        _ ->
            {State, ok}

    end;
apply(_, {checkout, {dequeue, _}, {_Tag, _Pid}},
      #state{messages = M,
             prefix_msg_count = 0} = State0) when map_size(M) == 0 ->
    %% TODO do we need metric visibility of empty get requests?
    {State0, {dequeue, empty}};
apply(Meta, {checkout, {dequeue, settled}, CustomerId}, State0) ->
    % TODO: this clause could probably be optimised
    State1 = update_customer(CustomerId, {once, 1}, State0),
    % turn send msg effect into reply
    {success, _, MsgId, Msg, State2} = checkout_one(State1),
    % immediately settle
    {State, _, _} = apply(Meta, {settle, [MsgId], CustomerId},
                          State2),
    {State, {dequeue, {MsgId, Msg}}};
apply(_, {checkout, {dequeue, unsettled}, {_Tag, Pid} = Customer}, State0) ->
    State1 = update_customer(Customer, {once, 1}, State0),
    Effects1 = [{monitor, process, Pid}],
    {State, Reply, Effects} = case checkout_one(State1) of
                                  {success, _, MsgId, Msg, S} ->
                                      {S, {MsgId, Msg}, Effects1};
                                  {inactive, S} ->
                                      {S, empty, [{aux, inactive} | Effects1]};
                                  S ->
                                      {S, empty, Effects1}
                              end,
    {State, {dequeue, Reply}, Effects};
apply(_, {checkout, cancel, CustomerId}, State0) ->
    {CancelEffects, State1} = cancel_customer(CustomerId, {[], State0}),
    {State, _, Effects} = checkout(State1, CancelEffects),
    % TODO: here we should really demonitor the pid but _only_ if it has no
    % other customers or enqueuers.
    {State, ok, Effects};
apply(_, {checkout, Spec, {_Tag, Pid} = CustomerId}, State0) ->
    State1 = update_customer(CustomerId, Spec, State0),
    checkout(State1, [{monitor, process, Pid}]);
apply(#{index := RaftIdx}, purge,
      #state{customers = Custs0, ra_indexes = Indexes } = State0) ->
    Total = ra_fifo_index:size(Indexes),
    {State1, Effects1} =
        maps:fold(
          fun(CustomerId, C = #customer{checked_out = Checked0},
              {StateAcc0, EffectsAcc0}) ->
                  MsgRaftIdxs = [RIdx || {_MsgInId, {RIdx, _}}
                                             <- maps:values(Checked0)],
                  complete(CustomerId, MsgRaftIdxs, C,
                           #{}, EffectsAcc0, StateAcc0)
          end, {State0, []}, Custs0),
    {State, ok, Effects} = update_smallest_raft_index(
                             RaftIdx, Indexes,
                             State1#state{ra_indexes = ra_fifo_index:empty(),
                                          messages = #{},
                                          returns = queue:new(),
                                          low_msg_num = undefined}, Effects1),
    {State, {purge, Total}, lists:reverse([garbage_collection | Effects])};
apply(_, {down, CustomerPid, noconnection},
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
    Effects = case maps:size(Custs) of
                  0 ->
                      [{aux, inactive}, {monitor, node, Node}];
                  _ ->
                      {monitor, node, Node}
              end,
    {State0#state{customers = Custs, enqueuers = Enqs}, ok, Effects};
apply(_, {down, Pid, _Info},
      #state{customers = Custs0,
             enqueuers = Enqs0} = State0) ->
    % remove any enqueuer for the same pid
    % TODO: if there are any pending enqueuers these should be enqueued
    % This should be ok as we won't see any more enqueues from this pid
    State1 = case maps:take(Pid, Enqs0) of
                 {#enqueuer{pending = Pend}, Enqs} ->
                     lists:foldl(fun ({_, RIdx, RawMsg}, S) ->
                                         enqueue(RIdx, RawMsg, S)
                                 end, State0#state{enqueuers = Enqs}, Pend);
                 error ->
                     State0
             end,
    % return checked out messages to main queue
    % Find the customers for the down pid
    DownCustomers = maps:keys(
                      maps:filter(fun({_, P}, _) -> P =:= Pid end, Custs0)),
    {Effects1, State2} = lists:foldl(fun cancel_customer/2, {[], State1},
                                     DownCustomers),
    checkout(State2, Effects1);
apply(_, {nodeup, Node}, #state{customers = Custs0,
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
    % TODO: avoid list concat
    {State0, ok, Monitors};
apply(_, {nodedown, _Node}, State) ->
    {State, ok}.


state_enter(leader, #state{customers = Custs,
                           name = Name,
                           become_leader_handler = BLH}) ->
    % return effects to monitor all current customers
    Effects = [{monitor, process, P} || {_, P} <- maps:keys(Custs)],
    case BLH of
        undefined ->
            Effects;
        {Mod, Fun, Args} ->
            [{mod_call, Mod, Fun, Args ++ [Name]} | Effects]
    end;
state_enter(eol, #state{enqueuers = Enqs, customers = Custs0}) ->
    Custs = maps:fold(fun({_, P}, V, S) -> S#{P => V} end, #{}, Custs0),
    [{send_msg, P, eol, ra_event} || P <- maps:keys(maps:merge(Enqs, Custs))];
state_enter(_, _) ->
    %% catch all as not handling all states
    [].


-spec tick(non_neg_integer(), state()) -> ra_machine:effects().
tick(_Ts, #state{name = Name,
                 messages = Messages,
                 ra_indexes = Indexes,
                 metrics_handler = MH,
                 customers = Custs} = State) ->
    Metrics = {Name,
               maps:size(Messages), % Ready
               num_checked_out(State), % checked out
               ra_fifo_index:size(Indexes), %% Total
               maps:size(Custs)}, % Customers
    case MH of
        undefined ->
            [{aux, emit}];
        {Mod, Fun, Args} ->
            [{mod_call, Mod, Fun, Args ++ [Metrics]}, {aux, emit}]
    end.

-spec overview(state()) -> map().
overview(#state{customers = Custs,
                enqueuers = Enqs,
                messages = Messages,
                ra_indexes = Indexes} = State) ->
    #{type => ?MODULE,
      num_customers => maps:size(Custs),
      num_checked_out => num_checked_out(State),
      num_enqueuers => maps:size(Enqs),
      num_ready_messages => maps:size(Messages),
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

init_aux(Name) when is_atom(Name) ->
    %% TODO: catch specific exception throw if table already exists
    ok = ra_machine_ets:create_table(ra_fifo_usage,
                                     [named_table, set, public,
                                      {write_concurrency, true}]),
    Now = erlang:monotonic_time(micro_seconds),
    {Name, {inactive, Now, 1, 1.0}}.

handle_aux(_, cast, Cmd, {Name, Use0}, Log, _) ->
    Use = case Cmd of
              _ when Cmd == active orelse Cmd == inactive ->
                  update_use(Use0, Cmd);
              _ ->
                  true = ets:insert(ra_fifo_usage,
                                    {Name, utilisation(Use0)}),
                  Use0
          end,
    {no_reply, {Name, Use}, Log}.

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


query_ra_indexes(#state{ra_indexes = RaIndexes}) ->
    RaIndexes.

query_customer_count(#state{customers = Customers}) ->
    maps:size(Customers).

%% other

-spec usage(atom()) -> float().
usage(Name) when is_atom(Name) ->
    case ets:lookup(ra_fifo_usage, Name) of
        [] -> 0.0;
        [{_, Use}] -> Use
    end.

read_log() ->
    fun({_, _, {'$usr', _, {enqueue, _, _, _}, _}}, {E, C, S, D, R}) ->
            {E + 1, C, S, D, R};
       ({_, _, {'$usr', _, {settle, _, _}, _}}, {E, C, S, D, R}) ->
            {E, C, S + 1, D, R};
       ({_, _, {'$usr', _, {discard, _, _}, _}}, {E, C, S, D, R}) ->
            {E, C, S, D + 1, R};
       ({_, _, {'$usr', _, {return, _, _}, _}}, {E, C, S, D, R}) ->
            {E, C, S, D, R + 1};
       ({_, _, {'$usr', _, {checkout, _, _}, _}}, {E, C, S, D, R}) ->
            {E, C + 1, S, D, R};
       (_, Acc) ->
            Acc
    end.

%%% Internal

update_use({inactive, _, _, _} = CUInfo, inactive) ->
    CUInfo;
update_use({active, _, _} = CUInfo, active) ->
    CUInfo;
update_use({active, Since, Avg}, inactive) ->
    Now = erlang:monotonic_time(micro_seconds),
    {inactive, Now, Now - Since, Avg};
update_use({inactive, Since, Active, Avg},   active) ->
    Now = erlang:monotonic_time(micro_seconds),
    {active, Now, use_avg(Active, Now - Since, Avg)}.

utilisation({active, Since, Avg}) ->
    use_avg(erlang:monotonic_time(micro_seconds) - Since, 0, Avg);
utilisation({inactive, Since, Active, Avg}) ->
    use_avg(Active, erlang:monotonic_time(micro_seconds) - Since, Avg).

use_avg(0, 0, Avg) ->
    Avg;
use_avg(Active, Inactive, Avg) ->
    Time = Inactive + Active,
    moving_average(Time, ?USE_AVG_HALF_LIFE, Active / Time, Avg).

moving_average(_Time, _, Next, undefined) ->
    Next;
moving_average(Time, HalfLife, Next, Current) ->
    Weight = math:exp(Time * math:log(0.5) / HalfLife),
    Next * (1 - Weight) + Current * Weight.

num_checked_out(#state{customers = Custs}) ->
    lists:foldl(fun (#customer{checked_out = C}, Acc) ->
                        maps:size(C) + Acc
                end, 0, maps:values(Custs)).

cancel_customer(CustomerId,
                {Effects0, #state{customers = C0, name = Name} = S0}) ->
    case maps:take(CustomerId, C0) of
        {#customer{checked_out = Checked0}, Custs} ->
            S = maps:fold(fun (_, {MsgNum, Msg}, S) ->
                                  return_one(MsgNum, Msg, S)
                          end, S0, Checked0),
            Effects = cancel_customer_effects(CustomerId, Name, S, Effects0),
            case maps:size(Custs) of
                0 ->
                    {[{aux, inactive} | Effects], S#state{customers = Custs}};
                _ ->
                    {Effects, S#state{customers = Custs}}
                end;
        error ->
            % already removed - do nothing
            {Effects0, S0}
    end.

incr_enqueue_count(#state{enqueue_count = C,
                          shadow_copy_interval = C} = State0) ->
    % time to stash a dehydrated state version
    State = State0#state{enqueue_count = 0},
    {State, dehydrate_state(State)};
incr_enqueue_count(#state{enqueue_count = C} = State) ->
    {State#state{enqueue_count = C + 1}, undefined}.

enqueue(RaftIdx, RawMsg, #state{messages = Messages,
                                low_msg_num = LowMsgNum,
                                next_msg_num = NextMsgNum} = State0) ->
    Msg = {RaftIdx, {#{}, RawMsg}}, % indexed message with header map
    State0#state{messages = Messages#{NextMsgNum => Msg},
                 % this is probably only done to record it when low_msg_num
                 % is undefined
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

maybe_enqueue(RaftIdx, undefined, undefined, RawMsg, Effects,
              State0) ->
    % direct enqueue without tracking
    {ok, enqueue(RaftIdx, RawMsg, State0), Effects};
maybe_enqueue(RaftIdx, From, MsgSeqNo, RawMsg, Effects0,
              #state{enqueuers = Enqueuers0} = State0) ->
    case maps:get(From, Enqueuers0, undefined) of
        undefined ->
            State1 = State0#state{enqueuers = Enqueuers0#{From => #enqueuer{}}},
            {ok, State, Effects} = maybe_enqueue(RaftIdx, From, MsgSeqNo,
                                                 RawMsg, Effects0, State1),
            {ok, State, [{monitor, process, From} | Effects]};
        #enqueuer{next_seqno = MsgSeqNo} = Enq0 ->
            % it is the next expected seqno
            State1 = enqueue(RaftIdx, RawMsg, State0),
            Enq = Enq0#enqueuer{next_seqno = MsgSeqNo + 1},
            State = enqueue_pending(From, Enq, State1),
            {ok, State, Effects0};
        #enqueuer{next_seqno = Next,
                  pending = Pending0} = Enq0
          when MsgSeqNo > Next ->
            % out of order delivery
            Pending = [{MsgSeqNo, RaftIdx, RawMsg} | Pending0],
            Enq = Enq0#enqueuer{pending = lists:sort(Pending)},
            {ok, State0#state{enqueuers = Enqueuers0#{From => Enq}}, Effects0};
        #enqueuer{next_seqno = Next} when MsgSeqNo =< Next ->
            % duplicate delivery - remove the raft index from the ra_indexes
            % map as it was added earlier
            {duplicate, State0, Effects0}
    end.

snd(T) ->
    element(2, T).

return(CustomerId, MsgNumMsgs, #customer{lifetime = Life} = Cust0, Checked,
       Effects0, #state{customers = Custs0, service_queue = SQ0} = State0) ->
    Cust = case Life of
              auto ->
                   Num = length(MsgNumMsgs),
                   Cust0#customer{checked_out = Checked,
                          seen = Cust0#customer.seen - Num};
               once ->
                   Cust0#customer{checked_out = Checked}
           end,
    {Custs, SQ, Effects} = update_or_remove_sub(CustomerId, Cust, Custs0,
                                                 SQ0, Effects0),
    State1 = lists:foldl(fun(dummy, #state{prefix_msg_count = MsgCount} = S0) ->
                                 S0#state{prefix_msg_count = MsgCount + 1};
                            ({MsgNum, Msg}, S0) ->
                                 return_one(MsgNum, Msg, S0)
                         end, State0, MsgNumMsgs),
    checkout(State1#state{customers = Custs,
                          service_queue = SQ},
             Effects).

% used to processes messages that are finished
complete(CustomerId, MsgRaftIdxs, Cust0, Checked, Effects0,
       #state{customers = Custs0, service_queue = SQ0,
              ra_indexes = Indexes0} = State0) ->
    Cust = Cust0#customer{checked_out = Checked},
    {Custs, SQ, Effects} = update_or_remove_sub(CustomerId, Cust, Custs0,
                                                SQ0, Effects0),
    Indexes = lists:foldl(fun ra_fifo_index:delete/2, Indexes0, MsgRaftIdxs),
    {State0#state{customers = Custs,
                  ra_indexes = Indexes,
                  service_queue = SQ}, Effects}.

complete_and_checkout(IncomingRaftIdx, MsgIds, CustomerId,
                      #customer{checked_out = Checked0} = Cust0,
                      Effects0, #state{ra_indexes = Indexes0} = State0) ->
    Checked = maps:without(MsgIds, Checked0),
    Discarded = maps:with(MsgIds, Checked0),
    MsgRaftIdxs = [RIdx || {_, {RIdx, _}} <- maps:values(Discarded)],
    {State1, Effects1}  = complete(CustomerId, MsgRaftIdxs,
                                   Cust0, Checked, Effects0, State0),
    {State, ok, Effects} = checkout(State1, Effects1),
    % settle metrics are incremented separately
    update_smallest_raft_index(IncomingRaftIdx, Indexes0, State, Effects).

dead_letter_effects(_,
                    #state{dead_letter_handler = undefined},
                    Effects) ->
    Effects;
dead_letter_effects(Discarded,
                    #state{dead_letter_handler = {Mod, Fun, Args}}, Effects) ->
    DeadLetters = maps:fold(fun(_, {_, {_, {_, Msg}}},
                                % MsgId, MsgIdID, RaftId, Header
                                Acc) -> [{rejected, Msg} | Acc]
                            end, [], Discarded),
    [{mod_call, Mod, Fun, Args ++ [DeadLetters]} | Effects].

cancel_customer_effects(_, _, #state{cancel_customer_handler = undefined},
                        Effects) ->
    Effects;
cancel_customer_effects(Pid, Name,
                        #state{cancel_customer_handler = {Mod, Fun, Args}},
                        Effects) ->
    [{mod_call, Mod, Fun, Args ++ [Pid, Name]} | Effects].

update_smallest_raft_index(IncomingRaftIdx, OldIndexes,
                           #state{ra_indexes = Indexes,
                                  messages = Messages} = State, Effects) ->
    case ra_fifo_index:size(Indexes) of
        0 when map_size(Messages) =:= 0 ->
            % there are no messages on queue anymore and no pending enqueues
            % we can forward release_cursor all the way until
            % the last received command
            {State, ok,
             [{release_cursor, IncomingRaftIdx, State} | Effects]};
        _ ->
            NewSmallest = ra_fifo_index:smallest(Indexes),
            % Take the smallest raft index available in the index when starting
            % to process this command
            case {NewSmallest, ra_fifo_index:smallest(OldIndexes)} of
                {{Smallest, _}, {Smallest, _}} ->
                    % smallest has not changed, do not issue release cursor
                    % effects
                    {State, ok, Effects};
                {_, {Smallest, Shadow}} when Shadow =/= undefined ->
                    % ?INFO("RELEASE ~w ~w ~w", [IncomingRaftIdx, Smallest,
                    %                              Shadow]),
                    {State, ok, [{release_cursor, Smallest, Shadow} | Effects]};
                 _ -> % smallest
                    % no shadow taken for this index,
                    % no release cursor increase
                    {State, ok, Effects}
            end
    end.

% TODO update message then update messages and returns in single operations
return_one(MsgNum, {RaftId, {Header0, RawMsg}},
           #state{messages = Messages,
                  returns = Returns} = State0) ->
    Header = maps:update_with(delivery_count,
                              fun (C) -> C+1 end,
                              1, Header0),
    Msg = {RaftId, {Header, RawMsg}},
    % this should not affect the release cursor in any way
    State0#state{messages = maps:put(MsgNum, Msg, Messages),
                 returns = queue:in(MsgNum, Returns)}.


checkout(State, Effects) ->
    checkout0(checkout_one(State), Effects, #{}).

checkout0({success, CustomerId, MsgId, Msg, State}, Effects, Acc0) ->
    DelMsg = {MsgId, Msg},
    Acc = maps:update_with(CustomerId,
                           fun (M) -> [DelMsg | M] end,
                           [DelMsg], Acc0),
    checkout0(checkout_one(State), Effects, Acc);
checkout0({inactive, State}, Effects0, Acc) ->
    Effects = append_send_msg_effects(Effects0, Acc),
    {State, ok, lists:reverse([{aux, inactive} | Effects])};
checkout0(State, Effects0, Acc) ->
    Effects = append_send_msg_effects(Effects0, Acc),
    {State, ok, lists:reverse(Effects)}.

append_send_msg_effects(Effects, AccMap) when map_size(AccMap) == 0 ->
    Effects;
append_send_msg_effects(Effects0, AccMap) ->
    Effects = maps:fold(fun (C, Msgs, Ef) ->
                                [send_msg_effect(C, lists:reverse(Msgs)) | Ef]
                        end, Effects0, AccMap),
    [{aux, active} | Effects].

next_checkout_message(#state{returns = Returns,
                             low_msg_num = Low0,
                             next_msg_num = NextMsgNum} = State) ->
    %% use peek rather than out there as the most likely case is an empty
    %% queue
    case queue:peek(Returns) of
        empty ->
            case Low0 of
                undefined ->
                    {undefined, State};
                _ ->
                    case Low0 + 1 of
                        NextMsgNum ->
                            %% the map will be empty after this item is removed
                            {Low0, State#state{low_msg_num = undefined}};
                        Low ->
                            {Low0, State#state{low_msg_num = Low}}
                    end
            end;
        {value, Next} ->
            {Next, State#state{returns = queue:drop(Returns)}}
    end.

take_next_msg(#state{prefix_msg_count = 0,
                     messages = Messages0} = State0) ->
    {NextMsgInId, State} = next_checkout_message(State0),
    %% messages are available
    case maps:take(NextMsgInId, Messages0) of
        {IdxMsg, Messages} ->
            {{NextMsgInId, IdxMsg}, State, Messages, 0};
        error ->
            error
    end;
take_next_msg(#state{prefix_msg_count = MsgCount,
                     messages = Messages} = State) ->
    {dummy, State, Messages, MsgCount - 1}.

send_msg_effect({CTag, CPid}, Msgs) ->
    {send_msg, CPid, {delivery, CTag, Msgs}, ra_event}.

checkout_one(#state{service_queue = SQ0,
                    messages = Messages0,
                    customers = Custs0} = InitState) ->
    case queue:peek(SQ0) of
        {value, CustomerId} ->
            case take_next_msg(InitState) of
                {CustomerMsg, State0, Messages, PrefMsgC} ->
                    SQ1 = queue:drop(SQ0),
                    %% there are customers waiting to be serviced
                    %% process customer checkout
                    case maps:find(CustomerId, Custs0) of
                        {ok, #customer{checked_out = Checked0,
                                       next_msg_id = Next,
                                       seen = Seen} = Cust0} ->
                            Checked = maps:put(Next, CustomerMsg, Checked0),
                            Cust = Cust0#customer{checked_out = Checked,
                                                  next_msg_id = Next+1,
                                                  seen = Seen+1},
                            {Custs, SQ, []} = % we expect no effects
                                update_or_remove_sub(CustomerId, Cust,
                                                     Custs0, SQ1, []),
                            State = State0#state{service_queue = SQ,
                                                 messages = Messages,
                                                 prefix_msg_count = PrefMsgC,
                                                 customers = Custs},
                            Msg = case CustomerMsg of
                                      dummy -> dummy;
                                      {_, {_, M}} -> M
                                  end,
                            {success, CustomerId, Next, Msg, State};
                        error ->
                            %% customer did not exist but was queued, recurse
                            checkout_one(InitState#state{service_queue = SQ1})
                    end;
                error ->
                    InitState
            end;
        empty ->
            case maps:size(Messages0) of
                0 -> InitState;
                _ -> {inactive, InitState}
            end
    end.


update_or_remove_sub(CustomerId, #customer{lifetime = auto,
                                           checked_out = Checked,
                                           num = Num} = Cust,
                     Custs, ServiceQueue, Effects) ->
    case maps:size(Checked) < Num of
        true ->
            {maps:put(CustomerId, Cust, Custs),
             uniq_queue_in(CustomerId, ServiceQueue), Effects};
        false ->
            {maps:put(CustomerId, Cust, Custs), ServiceQueue, Effects}
    end;
update_or_remove_sub(CustomerId, #customer{lifetime = once,
                                           checked_out = Checked,
                                           num = N, seen = N} = Cust,
                     Custs, ServiceQueue, Effects) ->
    case maps:size(Checked)  of
        0 ->
            % we're done with this customer
            {maps:remove(CustomerId, Custs), ServiceQueue,
             [{demonitor, process, element(2, CustomerId)} | Effects]};
        _ ->
            % there are unsettled items so need to keep around
            {maps:put(CustomerId, Cust, Custs), ServiceQueue, Effects}
    end;
update_or_remove_sub(CustomerId, #customer{lifetime = once} = Cust,
                     Custs, ServiceQueue, Effects) ->
    {maps:put(CustomerId, Cust, Custs),
     uniq_queue_in(CustomerId, ServiceQueue), Effects}.

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
    CustGen = fun(N) -> {N, {checkout, {auto, 100},
                             spawn(fun() -> ok end)}} end,
    S0 = run_log(1, NumMsg, EnqGen, init(#{name => size_test,
                                           machine_version => 0})),
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
              SetlGen = fun(N) ->
                                {N, {settle, N - NumMsg - NumCust - 1, Pid}}
                        end,
              S0 = run_log(1, NumMsg, EnqGen,
                           init(#{name => size_test,
                                  machine_version => 0})),
              S1 = run_log(NumMsg, NumMsg + NumCust, CustGen, S0),
              _ = run_log(NumMsg, NumMsg + NumCust + NumMsg, SetlGen, S1),
              ok
     end).

run_log(Num, Num, _Gen, State) ->
    State;
run_log(Num, Max, Gen, State0) ->
    {_, E} = Gen(Num),
    run_log(Num+1, Max, Gen, element(1, apply(meta(Num), E, State0))).

dehydrate_state(#state{messages = Messages0,
                       customers = Customers,
                       prefix_msg_count = MsgCount} = State) ->
    State#state{messages = #{},
                ra_indexes = ra_fifo_index:empty(),
                low_msg_num = undefined,
                customers = maps:map(fun (_, C) ->
                                             C#customer{checked_out = #{}}
                                     end, Customers),
                returns = queue:new(),
                prefix_msg_count = maps:size(Messages0) + MsgCount}.


-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

-define(ASSERT_EFF(EfxPat, Effects),
        begin
            case Effects  of
                _ when is_list(Effects) ->
                    ?ASSERT_EFF(EfxPat, true, Effects);
                _ ->
                    ?ASSERT_EFF(EfxPat, true, [Effects])
            end
        end).


-define(ASSERT_EFF(EfxPat, Guard, Effects),
    ?assert(lists:any(fun (EfxPat) when Guard -> true;
                          (_) -> false
                      end, Effects))).

-define(assertNoEffect(EfxPat, Effects),
        begin
            case Effects of
                _ when is_list(Effects) ->
                    ?assert(not lists:any(fun (EfxPat) -> true;
                                              (_) -> false
                                          end, Effects));
                _ ->
                    ?assert(not lists:any(fun (EfxPat) -> true;
                                              (_) -> false
                                          end, [Effects]))
            end
        end).

test_init(Name) ->
    init(#{name => Name,
           machine_version => 0,
           shadow_copy_interval => 0,
           metrics_handler => {?MODULE, metrics_handler, []}}).

metrics_handler(_) ->
    ok.

enq_enq_checkout_test() ->
    Cid = {<<"enq_enq_checkout_test">>, self()},
    {State1, _, _} = enq(1, 1, first, test_init(test)),
    {State2, _, _} = enq(2, 2, second, State1),
    {_State3, _, Effects} =
        apply(meta(3), {checkout, {once, 2}, Cid}, State2),
    ?ASSERT_EFF({monitor, _, _}, Effects),
    ?ASSERT_EFF({send_msg, _, {delivery, _, _}, ra_event}, Effects),
    ok.

enq_enq_deq_test() ->
    Cid = {<<"enq_enq_checkout_get_test">>, self()},
    {State1, _, _} = enq(1, 1, first, test_init(test)),
    {State2, _, _} = enq(2, 2, second, State1),
    % get returns a reply value
    {_State3, {dequeue, {0, {_, first}}}, [{monitor, _, _}]} =
        apply(meta(3), {checkout, {dequeue, unsettled}, Cid}, State2),
    ok.

enq_enq_deq_deq_settle_test() ->
    Cid = {<<"enq_enq_checkout_get_test">>, self()},
    {State1, _, _} = enq(1, 1, first, test_init(test)),
    {State2, _, _} = enq(2, 2, second, State1),
    % get returns a reply value
    {State3, {dequeue, {0, {_, first}}}, [{monitor, _, _}]} =
        apply(meta(3), {checkout, {dequeue, unsettled}, Cid}, State2),
    {_State4, {dequeue, empty}, _} =
        apply(meta(3), {checkout, {dequeue, unsettled}, Cid}, State3),
    ok.

enq_enq_checkout_get_settled_test() ->
    Cid = {<<"enq_enq_checkout_get_test">>, self()},
    {State1, _, _} = enq(1, 1, first, test_init(test)),
    % get returns a reply value
    {_State2, {dequeue, {0, {_, first}}}} =
        apply(meta(3), {checkout, {dequeue, settled}, Cid}, State1),
    ok.

checkout_get_empty_test() ->
    Cid = {<<"checkout_get_empty_test">>, self()},
    State = test_init(test),
    {_State2, {dequeue, empty}} =
        apply(meta(1), {checkout, {dequeue, unsettled}, Cid}, State),
    ok.

untracked_enq_deq_test() ->
    Cid = {<<"untracked_enq_deq_test">>, self()},
    State0 = test_init(test),
    {State1, _, _} = apply(meta(1), {enqueue, undefined, undefined, first}, State0),
    {_State2, {dequeue, {0, {_, first}}}} =
        apply(meta(3), {checkout, {dequeue, settled}, Cid}, State1),
    ok.
release_cursor_test() ->
    Cid = {<<"release_cursor_test">>, self()},
    {State1, _, _} = enq(1, 1, first,  test_init(test)),
    {State2, _, _} = enq(2, 2, second, State1),
    {State3, _, _} = check(Cid, 3, 10, State2),
    % no release cursor effect at this point
    {State4, _, _} = settle(Cid, 4, 1, State3),
    {_Final, _, Effects1} = settle(Cid, 5, 0, State4),
    % empty queue forwards release cursor all the way
    ?ASSERT_EFF({release_cursor, 5, _}, Effects1),
    ok.

checkout_enq_settle_test() ->
    Cid = {<<"checkout_enq_settle_test">>, self()},
    {State1, _, [{monitor, _, _}]} = check(Cid, 1, test_init(test)),
    {State2, _, Effects0} = enq(2, 1,  first, State1),
    ?ASSERT_EFF({send_msg, _,
                 {delivery, <<"checkout_enq_settle_test">>,
                  [{0, {_, first}}]},
                 ra_event},
                Effects0),
    {State3, _, [_Inactive]} = enq(3, 2, second, State2),
    {_, _, _} = settle(Cid, 4, 0, State3),
    % the release cursor is the smallest raft index that does not
    % contribute to the state of the application
    % ?ASSERT_EFF({release_cursor, 2, _}, Effects),
    ok.

out_of_order_enqueue_test() ->
    Cid = {<<"out_of_order_enqueue_test">>, self()},
    {State1, _, [{monitor, _, _}]} = check_n(Cid, 5, 5, test_init(test)),
    {State2, _, Effects2} = enq(2, 1, first, State1),
    ?ASSERT_EFF({send_msg, _, {delivery, _, [{_, {_, first}}]}, ra_event},
                Effects2),
    % assert monitor was set up
    ?ASSERT_EFF({monitor, _, _}, Effects2),
    % enqueue seq num 3 and 4 before 2
    {State3, _, Effects3} = enq(3, 3, third, State2),
    ?assertNoEffect({send_msg, _, {delivery, _, _}, ra_event}, Effects3),
    {State4, _, Effects4} = enq(4, 4, fourth, State3),
    % assert no further deliveries where made
    ?assertNoEffect({send_msg, _, {delivery, _, _}, ra_event}, Effects4),
    {_State5, _, Effects5} = enq(5, 2, second, State4),
    % assert two deliveries were now made
    ?debugFmt("Effects5 ~n~p", [Effects5]),
    ?ASSERT_EFF({send_msg, _, {delivery, _, [{_, {_, second}},
                                             {_, {_, third}},
                                             {_, {_, fourth}}]},
                ra_event}, Effects5),
    ok.

out_of_order_first_enqueue_test() ->
    Cid = {<<"out_of_order_enqueue_test">>, self()},
    {State1, _, _} = check_n(Cid, 5, 5, test_init(test)),
    {_State2, _, Effects2} = enq(2, 10, first, State1),
    ?ASSERT_EFF({monitor, process, _}, Effects2),
    ?assertNoEffect({send_msg, _, {delivery, _, [{_, {_, first}}]},
                     ra_event}, Effects2),
    ok.

duplicate_enqueue_test() ->
    Cid = {<<"duplicate_enqueue_test">>, self()},
    {State1, _, [{monitor, _, _}]} = check_n(Cid, 5, 5, test_init(test)),
    {State2, _, Effects2} = enq(2, 1, first, State1),
    ?ASSERT_EFF({send_msg, _, {delivery, _, [{_, {_, first}}]},
                 ra_event}, Effects2),
    {_State3, _, Effects3} = enq(3, 1, first, State2),
    ?assertNoEffect({send_msg, _, {delivery, _, [{_, {_, first}}]},
                     ra_event}, Effects3),
    ok.

return_non_existent_test() ->
    Cid = {<<"cid">>, self()},
    {State0, _, [_, _Inactive]} = enq(1, 1, second, test_init(test)),
    % return non-existent
    {_State2, ok} = apply(meta(3), {return, [99], Cid}, State0),
    ok.

return_checked_out_test() ->
    Cid = {<<"cid">>, self()},
    {State0, _, [_, _]} = enq(1, 1, first, test_init(test)),
    {State1, _, [_Monitor,
                 {send_msg, _, {delivery, _, [{MsgId, _}]}, _},
                 {aux, active}]} =
        check(Cid, 2, State0),
    % return
    {_State2, _, [_, _]} = apply(meta(3), {return, [MsgId], Cid}, State1),
    ok.

return_auto_checked_out_test() ->
    Cid = {<<"cid">>, self()},
    {State00, _, [_, _]} = enq(1, 1, first, test_init(test)),
    {State0, _, [_]} = enq(2, 2, second, State00),
    % it first active then inactive as the consumer took on but cannot take
    % any more
    {State1, _, [_Monitor,
                 {send_msg, _, {delivery, _, [{MsgId, _}]}, ra_event},
                 {aux, active},
                 {aux, inactive}]} = check_auto(Cid, 2, State0),

    % return should include another delivery
    {_State2, _, Effects} = apply(meta(3), {return, [MsgId], Cid}, State1),
    ?ASSERT_EFF({send_msg, _,
                 {delivery, _, [{_, {#{delivery_count := 1}, first}}]}, _},
                Effects),
    ok.


cancelled_checkout_out_test() ->
    Cid = {<<"cid">>, self()},
    {State00, _, [_, _]} = enq(1, 1, first, test_init(test)),
    {State0, _, [_]} = enq(2, 2, second, State00),
    {State1, _, _} = check_auto(Cid, 2, State0),
    % cancelled checkout should return all pending messages to queue
    {State2, _, _} = apply(meta(3), {checkout, cancel, Cid}, State1),

    {State3, {dequeue, {0, {_, first}}}} =
        apply(meta(3), {checkout, {dequeue, settled}, Cid}, State2),
    {_State, {dequeue, {_, {_, second}}}} =
        apply(meta(3), {checkout, {dequeue, settled}, Cid}, State3),
    ok.

down_with_noproc_customer_returns_unsettled_test() ->
    Cid = {<<"down_customer_returns_unsettled_test">>, self()},
    {State0, _, [_, _]} = enq(1, 1, second, test_init(test)),
    {State1, _, [{monitor, process, Pid} | _]} = check(Cid, 2, State0),
    {State2, _, [_, _]} = apply(meta(3), {down, Pid, noproc}, State1),
    {_State, _, Effects} = check(Cid, 4, State2),
    ?ASSERT_EFF({monitor, process, _}, Effects),
    ok.

down_with_noconnection_marks_suspect_and_node_is_monitored_test() ->
    Pid = spawn(fun() -> ok end),
    Cid = {<<"down_with_noconnect">>, Pid},
    Self = self(),
    Node = node(Pid),
    {State0, _, Effects0} = enq(1, 1, second, test_init(test)),
    ?ASSERT_EFF({monitor, process, P}, P =:= Self, Effects0),
    {State1, _, Effects1} = check(Cid, 2, State0),
    ?ASSERT_EFF({monitor, process, P}, P =:= Pid, Effects1),
    % monitor both enqueuer and customer
    % because we received a noconnection we now need to monitor the node
    {State2a, _, _Effects2a} = apply(meta(3), {down, Pid, noconnection}, State1),
    {State2, _, Effects2} = apply(meta(3), {down, Self, noconnection}, State2a),
    ?ASSERT_EFF({monitor, node, _}, Effects2),
    ?assertNoEffect({demonitor, process, _}, Effects2),
    % when the node comes up we need to retry the process monitors for the
    % disconnected processes
    {_State3, _, Effects3} = apply(meta(3), {nodeup, Node}, State2),
    % try to re-monitor the suspect processes
    ?ASSERT_EFF({monitor, process, P}, P =:= Pid, Effects3),
    ?ASSERT_EFF({monitor, process, P}, P =:= Self, Effects3),
    ok.

down_with_noproc_enqueuer_is_cleaned_up_test() ->
    State00 = test_init(test),
    Pid = spawn(fun() -> ok end),
    {State0, _, Effects0} = apply(meta(1), {enqueue, Pid, 1, first}, State00),
    ?ASSERT_EFF({monitor, process, _}, Effects0),
    {State1, _, _Effects1} = apply(meta(3), {down, Pid, noproc}, State0),
    % ensure there are no enqueuers
    ?assert(0 =:= maps:size(State1#state.enqueuers)),
    ok.

completed_customer_yields_demonitor_effect_test() ->
    Cid = {<<"completed_customer_yields_demonitor_effect_test">>, self()},
    {State0, _, [_, _]} = enq(1, 1, second, test_init(test)),
    {State1, _, [{monitor, process, _} |  _]} = check(Cid, 2, State0),
    {_, _, Effects} = settle(Cid, 3, 0, State1),
    ?ASSERT_EFF({demonitor, _, _}, Effects),
    % release cursor for empty queue
    ?ASSERT_EFF({release_cursor, 3, _}, Effects),
    ok.

discarded_message_without_dead_letter_handler_is_removed_test() ->
    Cid = {<<"completed_customer_yields_demonitor_effect_test">>, self()},
    {State0, _, [_, _]} = enq(1, 1, first, test_init(test)),
    {State1, _, Effects1} = check_n(Cid, 2, 10, State0),
    ?ASSERT_EFF({send_msg, _,
                 {delivery, _, [{0, {#{}, first}}]}, _},
                Effects1),
    {_State2, _, Effects2} = apply(meta(1), {discard, [0], Cid}, State1),
    ?assertNoEffect({send_msg, _,
                     {delivery, _, [{0, {#{}, first}}]}, _},
                    Effects2),
    ok.

discarded_message_with_dead_letter_handler_emits_mod_call_effect_test() ->
    Cid = {<<"completed_customer_yields_demonitor_effect_test">>, self()},
    State00 = init(#{name => test,
                     machine_version => 0,
                     dead_letter_handler =>
                     {somemod, somefun, [somearg]}}),
    {State0, _, [_, _]} = enq(1, 1, first, State00),
    {State1, _, Effects1} = check_n(Cid, 2, 10, State0),
    ?ASSERT_EFF({send_msg, _,
                 {delivery, _, [{0, {#{}, first}}]}, _},
                Effects1),
    {_State2, _, Effects2} = apply(meta(1), {discard, [0], Cid}, State1),
    ?debugFmt("Effects2 ~w", [Effects2]),
    % assert mod call effect with appended reason and message
    ?ASSERT_EFF({mod_call, somemod, somefun, [somearg, [{rejected, first}]]},
                Effects2),
    ok.

tick_test() ->
    Cid = {<<"c">>, self()},
    Cid2 = {<<"c2">>, self()},
    {S0, _, _} = enq(1, 1, fst, test_init(test)),
    {S1, _, _} = enq(2, 2, snd, S0),
    {S2, {MsgId, _}, _} = deq(3, Cid, unsettled, S1),
    {S3, {_, _}, _} = deq(4, Cid2, unsettled, S2),
    {S4, _, _} = apply(meta(5), {return, [MsgId], Cid}, S3),

    [{mod_call, _, _, [{test, 1, 1, 2, 1}]}, {aux, emit}] = tick(1, S4),
    ok.

enq_deq_snapshot_recover_test() ->
    Tag = <<"release_cursor_snapshot_state_test">>,
    Cid = {Tag, self()},
    % OthPid = spawn(fun () -> ok end),
    % Oth = {<<"oth">>, OthPid},
    Commands = [
                {enqueue, self(), 1, one},
                {enqueue, self(), 2, two},
                {checkout, {dequeue, settled}, Cid},
                {enqueue, self(), 3, three},
                {enqueue, self(), 4, four},
                {checkout, {dequeue, settled}, Cid},
                {enqueue, self(), 5, five},
                {checkout, {dequeue, settled}, Cid}
              ],
    run_snapshot_test(?FUNCTION_NAME, Commands).

enq_deq_settle_snapshot_recover_test() ->
    Tag = atom_to_binary(?FUNCTION_NAME, utf8),
    Cid = {Tag, self()},
    % OthPid = spawn(fun () -> ok end),
    % Oth = {<<"oth">>, OthPid},
    Commands = [
                {enqueue, self(), 1, one},
                {enqueue, self(), 2, two},
                {checkout, {dequeue, unsettled}, Cid},
                {settle, [0], Cid}
              ],
    run_snapshot_test(?FUNCTION_NAME, Commands).

enq_deq_settle_snapshot_recover_2_test() ->
    Tag = atom_to_binary(?FUNCTION_NAME, utf8),
    Cid = {Tag, self()},
    OthPid = spawn(fun () -> ok end),
    Oth = {<<"oth">>, OthPid},
    Commands = [
                {enqueue, self(), 1, one},
                {enqueue, self(), 2, two},
                {checkout, {dequeue, unsettled}, Cid},
                {settle, [0], Cid},
                {enqueue, self(), 3, two},
                {checkout, {dequeue, unsettled}, Oth},
                {settle, [0], Oth}
              ],
    run_snapshot_test(?FUNCTION_NAME, Commands).

snapshot_recover_test() ->
    Tag = atom_to_binary(?FUNCTION_NAME, utf8),
    Cid = {Tag, self()},
    Commands = [
                {checkout, {auto, 2}, Cid},
                {enqueue, self(), 1, one},
                {enqueue, self(), 2, two},
                {enqueue, self(), 3, three},
                purge
              ],
    run_snapshot_test(?FUNCTION_NAME, Commands).

enq_deq_return_snapshot_recover_test() ->
    Tag = atom_to_binary(?FUNCTION_NAME, utf8),
    Cid = {Tag, self()},
    OthPid = spawn(fun () -> ok end),
    Oth = {<<"oth">>, OthPid},
    Commands = [
                {enqueue, self(), 1, one},
                {enqueue, self(), 2, two},
                {checkout, {dequeue, unsettled}, Oth},
                {checkout, {dequeue, unsettled}, Cid},
                {settle, [0], Oth},
                {return, [0], Cid},
                {enqueue, self(), 3, three},
                purge
              ],
    run_snapshot_test(?FUNCTION_NAME, Commands).

enq_check_settle_snapshot_recover_test() ->
    Tag = atom_to_binary(?FUNCTION_NAME, utf8),
    Cid = {Tag, self()},
    Commands = [
                {checkout, {auto, 2}, Cid},
                {enqueue, self(), 1, one},
                {enqueue, self(), 2, two},
                {settle, [1], Cid},
                {settle, [0], Cid},
                {enqueue, self(), 3, three},
                {settle, [2], Cid}

              ],
         % ?debugFmt("~w running commands ~w", [?FUNCTION_NAME, C]),
    run_snapshot_test(?FUNCTION_NAME, Commands).


run_snapshot_test(Name, Commands) ->
    %% create every incremental permutation of the commands lists
    %% and run the snapshot tests against that
    [begin
         % ?debugFmt("~w running commands ~w", [?FUNCTION_NAME, C]),
         run_snapshot_test0(Name, C)
     end || C <- prefixes(Commands, 1, [])].

run_snapshot_test0(Name, Commands) ->
    Indexes = lists:seq(1, length(Commands)),
    Entries = lists:zip(Indexes, Commands),
    {State, Effects} = run_log(test_init(Name), Entries),

    [begin
         Filtered = lists:dropwhile(fun({X, _}) when X =< SnapIdx -> true;
                                       (_) -> false
                                    end, Entries),
         {S, _} = run_log(SnapState, Filtered),
         % assert log can be restored from any release cursor index
         % ?debugFmt("Name ~p Idx ~p S~p~nState~p~nSnapState ~p~nFiltered ~p",
         %           [Name, SnapIdx, S, State, SnapState, Filtered]),
         ?assertEqual(State, S)
     end || {release_cursor, SnapIdx, SnapState} <- Effects],
    ok.

prefixes(Source, N, Acc) when N > length(Source) ->
    lists:reverse(Acc);
prefixes(Source, N, Acc) ->
    {X, _} = lists:split(N, Source),
    prefixes(Source, N+1, [X | Acc]).

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

pending_enqueue_is_enqueued_on_down_test() ->
    Cid = {<<"cid">>, self()},
    Pid = self(),
    {State0, _, _} = enq(1, 2, first, test_init(test)),
    {State1, _, _} = apply(meta(2), {down, Pid, noproc}, State0),
    {_State2, {dequeue, {0, {_, first}}}} =
        apply(meta(3), {checkout, {dequeue, settled}, Cid}, State1),
    ok.

duplicate_delivery_test() ->
    {State0, _, _} = enq(1, 1, first, test_init(test)),
    {#state{ra_indexes = RaIdxs,
            messages = Messages}, _, _} = enq(2, 1, first, State0),
    ?assertEqual(1, ra_fifo_index:size(RaIdxs)),
    ?assertEqual(1, maps:size(Messages)),
    ok.

state_enter_test() ->

    S0 = init(#{name => the_name,
                machine_version => 0,
                become_leader_handler => {m, f, [a]}}),
    [{mod_call, m, f, [a, the_name]}] = state_enter(leader, S0),
    ok.

performance_test() ->
    % just under ~200ms on my machine [Karl]
    NumMsgs = 100000,
    {Taken, _} = perf_test(NumMsgs, 0),
    ?debugFmt("performance_test took ~p ms for ~p messages",
              [Taken / 1000, NumMsgs]),
    ok.

purge_test() ->
    Cid = {<<"purge_test">>, self()},
    {State1, _, _} = enq(1, 1, first, test_init(test)),
    {State2, {purge, 1}, _} = apply(meta(2), purge, State1),
    {State3, _, _} = enq(3, 2, second, State2),
    % get returns a reply value
    {_State4, {dequeue, {0, {_, second}}}, [{monitor, _, _}]} =
        apply(meta(4), {checkout, {dequeue, unsettled}, Cid}, State3),
    ok.

purge_with_checkout_test() ->
    Cid = {<<"purge_test">>, self()},
    {State0, _, _} = check_auto(Cid, 1, test_init(?FUNCTION_NAME)),
    {State1, _, _} = enq(2, 1, first, State0),
    {State2, _, _} = enq(3, 2, second, State1),
    {State3, {purge, 2}, _} = apply(meta(2), purge, State2),
    #customer{checked_out = Checked} = maps:get(Cid, State3#state.customers),
    ?assertEqual(0, maps:size(Checked)),
    ok.

meta(Idx) ->
    #{index => Idx, term => 1,
      system_time => os:system_time(millisecond)}.

enq(Idx, MsgSeq, Msg, State) ->
    apply(meta(Idx), {enqueue, self(), MsgSeq, Msg}, State).

deq(Idx, Cid, Settlement, State0) ->
    {State, {dequeue, Msg}, _} =
        apply(meta(Idx), {checkout, {dequeue,  Settlement}, Cid}, State0),
    {State, Msg, ok}.

check_n(Cid, Idx, N, State) ->
    apply(meta(Idx), {checkout, {auto, N}, Cid}, State).

check(Cid, Idx, State) ->
    apply(meta(Idx), {checkout, {once, 1}, Cid}, State).

check_auto(Cid, Idx, State) ->
    apply(meta(Idx), {checkout, {auto, 1}, Cid}, State).

check(Cid, Idx, Num, State) ->
    apply(meta(Idx), {checkout, {once, Num}, Cid}, State).

settle(Cid, Idx, MsgId, State) ->
    apply(meta(Idx), {settle, [MsgId], Cid}, State).

run_log(InitState, Entries) ->
    lists:foldl(fun ({Idx, E}, {Acc0, Efx0}) ->
                        case apply(meta(Idx), E, Acc0) of
                            {Acc, Efx, _} ->
                                {Acc, [Efx | Efx0]};
                            {Acc, _} ->
                                {Acc, Efx0}
                        end
                end, {InitState, []}, Entries).


%% AUX Tests

aux_test() ->
    _ = ra_machine_ets:start_link(),
    Aux0 = init_aux(aux_test),
    MacState = init(#{name => aux_test, machine_version => 0}),
    Log = undefined,
    {no_reply, Aux, undefined} = handle_aux(leader, cast, active, Aux0,
                                            Log, MacState),
    {no_reply, _Aux, undefined} = handle_aux(leader, cast, emit, Aux,
                                             Log, MacState),
    [X] = ets:lookup(ra_fifo_usage, aux_test),
    ?assert(X > 0.0),
    ok.


-endif.

