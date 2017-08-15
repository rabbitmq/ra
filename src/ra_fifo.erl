-module(ra_fifo).

-compile({no_auto_import, [apply/3]}).

-export([
         empty_state/0,
         apply/3
        ]).

-type msg() :: term().
-type idx() :: non_neg_integer().
-type customer_id() :: pid(). % the entity that receives messages

-type checkout_spec() :: {once | auto, Num :: non_neg_integer()}.

-type protocol() ::
    {enqueue, Msg :: msg()} |
    {checkout, Spec :: checkout_spec(), Customer :: customer_id()} |
    {settle, Idx :: idx(), Customer :: customer_id()} |
    {return, Idx :: idx(), Customer :: customer_id()}.

-type raq_msg() :: {msg, Idx :: idx(), Msg :: msg()}.

-record(customer, {checked_out = #{} :: #{MsgIndex :: idx() => {RaftIndex :: idx(), msg()}},
                   num = 0 :: non_neg_integer(),
                   seen = 0 :: non_neg_integer(), % number of allocated messages
                   lifetime = once :: once | auto}).

-record(state, {queue = #{} :: map(),
                next_index = 0 :: idx(),
                low_index :: idx() | undefined,
                customers = #{} :: #{customer_id() =>  #customer{}},
                service_queue = queue:new() :: queue:queue()
               }).

-type state() :: #state{}.

-export_type([protocol/0,
              raq_msg/0]).

-spec empty_state() -> state().
empty_state() ->
    #state{}.

apply(RaftIdx, {enqueue, Msg}, #state{queue = Queue, low_index = Low,
                                      next_index = Next} = State0) ->
    State1 = State0#state{queue = Queue#{Next => {RaftIdx, Msg}},
                          low_index = min(Next, Low), next_index = Next+1},
    {State, Effects} = checkout(State1),
    {effects, State, Effects};
apply(_RaftId, {checkout, Spec, Customer}, State0) ->
    State1 = update_customer(Customer, Spec, State0),
    {State, Effects} = checkout(State1),
    {effects, State, [{monitor, process, Customer} | Effects]};
apply(_RaftId, {settle, Idx, CustomerId},
      #state{customers = Custs0, service_queue = SQ0} = State0) ->
    case Custs0 of
        #{CustomerId := Cust0 = #customer{checked_out = Checked}} ->
            Cust = Cust0#customer{checked_out = maps:remove(Idx, Checked)},
            {Custs, SQ, Efxs} =
                update_or_remove_sub(CustomerId, Cust, Custs0, SQ0),
            {State, Effects} = checkout(State0#state{customers = Custs,
                                                     service_queue = SQ}),
            {effects, State, Efxs ++ Effects};
        _ ->
            {effects, State0, []}
    end;
apply(_RaftId, {down, CustomerId}, #state{customers = Custs0} = State0) ->
    % return checked out messages to main queue
    case maps:take(CustomerId, Custs0) of
        {#customer{checked_out = Checked}, Custs} ->
            State = maps:fold(fun (MsgId, Msg, S) ->
                                      return(MsgId, Msg, S)
                              end, State0, Checked),
            {effects, State#state{customers = Custs}, []};
        error ->
            % already removed - do nothing
            {effects, State0, []}
    end.


%%% internal

return(MsgId, Msg, #state{queue = Queue, low_index = Low0} = State0) ->
    State0#state{queue = maps:put(MsgId, Msg, Queue),
                 low_index = min(MsgId, Low0)}.


checkout(State) ->
    checkout0(checkout_one(State), []).

checkout0({State, []}, Effects) ->
    {State, lists:reverse(Effects)};
checkout0({State, Efxs}, Effects) ->
    checkout0(checkout_one(State), Efxs ++ Effects).

checkout_one(#state{queue = Queue0, low_index = LowIdx, next_index = High,
                    service_queue = ServiceQueue0,
                    customers = Custs0} = State0) ->
    % messages are available
    case maps:take(LowIdx, Queue0) of
        {{_, Msg} = Item, Queue} ->
            % there are customers waiting to be serviced
            case queue:out(ServiceQueue0) of
                {{value, CustomerId}, ServiceQueue} ->
                    % process customer checkout
                    case maps:get(CustomerId, Custs0, undefined) of
                        #customer{checked_out = Checked, seen = Seen} = Cust0 ->
                            Cust = Cust0#customer{checked_out = Checked#{LowIdx => Item},
                                                  seen = Seen+1},
                            {Custs, ServiceQueue1, []} = % we expect no effects
                                update_or_remove_sub(CustomerId, Cust, Custs0, ServiceQueue),
                            State = State0#state{service_queue = ServiceQueue1,
                                                 low_index = find_next_larger(LowIdx+1, High, Queue),
                                                 queue = Queue, customers = Custs},
                            {State, [{send_msg, CustomerId, {msg, LowIdx, Msg}}]};
                        undefined ->
                            % customerect did not exist but was queued, recurse
                            checkout_one(State0#state{service_queue = ServiceQueue})
                    end;
                _ ->
                    {State0, []}
            end;
        error ->
            {State0, []}
    end.

update_or_remove_sub(CustomerId, #customer{lifetime = once,
                                           checked_out = Checked,
                                           num = N, seen = N} = Cust,
                     Custs, ServiceQueue) ->
    % we have seen enough, no more servicing
    case map_size(Checked)  of
        0 ->
            % we're done
            {maps:remove(CustomerId, Custs), ServiceQueue,
             [{demonitor, CustomerId}]};
        _ ->
            % there are unsettled items
            {maps:update(CustomerId, Cust, Custs), ServiceQueue, []}
    end;
update_or_remove_sub(CustomerId, #customer{lifetime = once} = Cust,
                     Custs, ServiceQueue) ->
    {maps:update(CustomerId, Cust, Custs),
     uniq_queue_in(CustomerId, ServiceQueue), []};
update_or_remove_sub(CustomerId, #customer{lifetime = auto,
                                           checked_out = Checked,
                                           num = Num} = Cust,
                     Custs, ServiceQueue)
  when map_size(Checked) < Num ->
    {maps:update(CustomerId, Cust, Custs),
     uniq_queue_in(CustomerId, ServiceQueue), []};
update_or_remove_sub(CustomerId, Cust, Custs, ServiceQueue) ->
    {maps:update(CustomerId, Cust, Custs), ServiceQueue, []}.

uniq_queue_in(Key, Queue) ->
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
                     ServiceQueue0)
  when map_size(Checked) < Num ->
    % customerect needs service - check if already on service queue
    case queue:member(CustomerId, ServiceQueue0) of
        true ->
            ServiceQueue0;
        false ->
            queue:in(CustomerId, ServiceQueue0)
    end;
maybe_queue_customer(_, _, ServiceQueue) ->
    ServiceQueue.


find_next_larger(_Idx, _High, Q) when map_size(Q) == 0 ->
    undefined;
find_next_larger(Idx, High, Queue) when Idx < High ->
    case maps:is_key(Idx, Queue) of
        true ->
            Idx;
        false ->
            find_next_larger(Idx+1, High, Queue)
    end.


-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").


enq_enq_checkout_test() ->
    {effects, State1, _} = enq(1, first, #state{}),
    {effects, State2, _} = enq(2, second, State1),
    {effects, State3, [{monitor, _, _}, _, _]} =
        apply(3, {checkout, {once, 2}, self()}, State2),
    ra_lib:dump(State3),
    ok.

checkout_enq_settle_test() ->
    {effects, State1, [{monitor, _, _}]} = check(1, #state{}),
    {effects, State2, [{send_msg, _, {msg, 0, first}}]} =
        enq(1, first, State1),
    {effects, State3, []} = enq(2, second, State2),
    {effects, _, _} = settle(3, 0, State3),
    ok.

down_customer_returns_unsettled_test() ->
    {effects, State0, []} = enq(1, second, empty_state()),
    {effects, State1, [{monitor, process, Pid}, Del]} = check(2, State0),
    {effects, State2, []} = apply(3, {down, Pid}, State1),
    {effects, _State, [{monitor, process, Pid}, Del]} = check(4, State2),
    ok.


completed_customer_yields_demonitor_effect_test() ->
    {effects, State0, []} = enq(1, second, empty_state()),
    {effects, State1, [{monitor, process, Pid}, _Msg]} = check(2, State0),
    {effects, _, [{demonitor, Pid}]} = settle(3, 0, State1),
    ok.

enq(Idx, Msg, State) ->
    apply(Idx, {enqueue, Msg}, State).

check(Idx, State) ->
    apply(Idx, {checkout, {once, 1}, self()}, State).

settle(Idx, MsgId,  State) ->
    apply(Idx, {settle, MsgId, self()}, State).

-endif.

