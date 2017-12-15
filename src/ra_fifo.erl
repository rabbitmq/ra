-module(ra_fifo).

-compile({no_auto_import, [apply/3]}).

-include("ra.hrl").

-export([
         init/1,
         apply/3
        ]).

-type msg() :: term().
-type msg_id() :: non_neg_integer().
-type customer_id() :: pid(). % the entity that receives messages

-type checkout_spec() :: {once | auto, Num :: non_neg_integer()}.

-type protocol() ::
    {enqueue, Msg :: msg()} |
    {checkout, Spec :: checkout_spec(), Customer :: customer_id()} |
    {settle, MsgId :: msg_id(), Customer :: customer_id()} |
    {return, MsgId :: msg_id(), Customer :: customer_id()}.

-define(METRICS_TABLE, ra_fifo_metrics).
% metrics tuple format:
% {Key, Enqueues, Checkouts, Settlements, Returns}

-record(customer,
        {checked_out = #{} :: #{MsgIndex :: msg_id() => {RaftIndex :: ra_index(), msg()}},
         next_msg_id = 0 :: msg_id(),
         num = 0 :: non_neg_integer(),
         seen = 0 :: non_neg_integer(), % number of allocated messages
         lifetime = once :: once | auto}).

-record(state, {name :: atom(),
                % unassigned messages
                messages = #{} :: #{ra_index() => msg()},
                % master index of all enqueue raft indexes
                % gb_set so that can take the smallest
                ra_indexes = gb_sets:new() :: gb_sets:set(ra_index()),
                % defines the lowest index available in the messages map
                low_index :: ra_index() | undefined,
                % the current release cursor index
                release_cursor :: ra_index() | undefined,
                customers = #{} :: #{customer_id() => #customer{}},
                % customers that require further service are queued here
                service_queue = queue:new() :: queue:queue(customer_id())
               }).

-type state() :: #state{}.

-export_type([protocol/0]).

-spec init(atom()) -> state().
init(Name) ->
    #state{name = Name}.

% msg_ids are scoped per customer
% ra_indexes holds all raft indexes for enqueues currently on queue
apply(RaftIdx, {enqueue, Msg}, #state{ra_indexes = Indexes,
                                      messages = Messages,
                                      release_cursor = ReleaseCursor,
                                      low_index = Low} = State0) ->
    State1 = State0#state{ra_indexes = gb_sets:add(RaftIdx, Indexes),
                          messages = Messages#{RaftIdx => Msg},
                          release_cursor = min(RaftIdx, ReleaseCursor),
                          low_index = min(RaftIdx, Low)},
    {State, Effects, Num} = checkout(State1, []),
    Metric = {incr_metrics, ?METRICS_TABLE, [{2, 1}, {3, Num}]},
    {effects, State, [Metric | Effects]};
apply(IncomingRaftIdx, {settle, MsgId, CustomerId},
      #state{customers = Custs0, service_queue = SQ0,
             release_cursor = ReleaseCursor0,
             ra_indexes = Indexes0} = State0) ->
    case Custs0 of
        #{CustomerId := Cust0 = #customer{checked_out = Checked0}} ->
            {{RaftIdx, _}, Checked} = maps:take(MsgId, Checked0),

            Cust = Cust0#customer{checked_out = Checked},
            {Custs, SQ, Effects0} =
                update_or_remove_sub(CustomerId, Cust, Custs0, SQ0),
            Indexes = gb_sets:delete(RaftIdx, Indexes0),
            {State, Effects1, NumChecked} = checkout(State0#state{customers = Custs,
                                                     ra_indexes = Indexes,
                                                     service_queue = SQ},
                                       Effects0),
            Effects = [{incr_metrics, ?METRICS_TABLE,
                        [{3, NumChecked}, {4, 1}]} | Effects1],
            {ReleaseCursor, AllEffects} =
                case gb_sets:size(Indexes) of
                    0 ->
                        % there are no messages on queue anymore
                        % we can forward release_cursor all the way until
                        % the last received command
                        {undefined,
                         [{release_cursor, IncomingRaftIdx} | Effects]};
                    _ when ReleaseCursor0 =:= RaftIdx ->
                        % the release cursor can be fowarded to next available message
                        Smallest = gb_sets:smallest(Indexes),
                        {Smallest,
                         [{release_cursor, Smallest} | Effects]};
                    _ ->
                        % release cursor cannot be forwarded
                        {ReleaseCursor0,  Effects}
                    end,
            {effects, State#state{release_cursor = ReleaseCursor}, AllEffects};
        _ ->
            {effects, State0, []}
    end;
apply(_RaftId, {checkout, Spec, Customer}, State0) ->
    State1 = update_customer(Customer, Spec, State0),
    {State, Effects, Num} = checkout(State1, []),
    Metric = {incr_metrics, ?METRICS_TABLE, [{3, Num}]},
    {effects, State, [{monitor, process, Customer}, Metric | Effects]};
apply(_RaftId, {down, CustomerId}, #state{customers = Custs0} = State0) ->
    % return checked out messages to main queue
    case maps:take(CustomerId, Custs0) of
        {#customer{checked_out = Checked0}, Custs} ->
            State = maps:fold(fun (_MsgId, {RaftId, Msg}, S) ->
                                         return(RaftId, Msg, S)
                                 end, State0, Checked0),
            Metric = {incr_metrics, ?METRICS_TABLE,
                      [{5, maps:size(Checked0)}]},
            {effects, State#state{customers = Custs}, [Metric]};
        error ->
            % already removed - do nothing
            {effects, State0, []}
    end.

%%% Internal

return(RaftId, Msg, #state{messages = Messages,
                           ra_indexes = Indexes,
                           low_index = Low0} = State0) ->
    % this should not affect the release cursor in any way
    State0#state{messages = maps:put(RaftId, Msg, Messages),
                 ra_indexes = gb_sets:add(RaftId, Indexes),
                 low_index = min(RaftId, Low0)}.


checkout(State, Effects) ->
    checkout0(checkout_one(State), Effects, 0).

checkout0({State, []}, Effects, Num) ->
    {State, lists:reverse(Effects), Num};
checkout0({State, Efxs}, Effects, Num) ->
    checkout0(checkout_one(State), Efxs ++ Effects, Num + 1).

checkout_one(#state{messages = Messages0,
                    low_index = LowIdx,
                    ra_indexes = Indexes,
                    service_queue = SQ0,
                    customers = Custs0} = State0) ->
    % messages are available
    case maps:take(LowIdx, Messages0) of
        {Msg, Messages} ->
            % there are customers waiting to be serviced
            case queue:out(SQ0) of
                {{value, CustomerId}, SQ1} ->
                    % process customer checkout
                    case maps:get(CustomerId, Custs0, undefined) of
                        #customer{checked_out = Checked0,
                                  next_msg_id = Next,
                                  seen = Seen} = Cust0 ->
                            Checked = maps:put(Next, {LowIdx, Msg}, Checked0),
                            Cust = Cust0#customer{checked_out = Checked,
                                                  next_msg_id = Next+1,
                                                  seen = Seen+1},
                            {Custs, SQ, []} = % we expect no effects
                                update_or_remove_sub(CustomerId, Cust, Custs0, SQ1),
                            State = State0#state{service_queue = SQ,
                                                 low_index = find_next_after(LowIdx, Indexes),
                                                 messages = Messages,
                                                 customers = Custs},
                            {State, [{send_msg, CustomerId, {msg, Next, Msg}}]};
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

find_next_after(Idx, Indexes) ->
    % Idx + 1 to get the next greatest element
    Iter = gb_sets:iterator_from(Idx + 1, Indexes),
    case gb_sets:next(Iter) of
        none ->
            undefined;
        {Elem, _Iter} ->
            Elem
    end.


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



-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

-define(assertEffect(EfxPat, Effects),
    ?assert(lists:any(fun (EfxPat) -> true;
                          (_) -> false
                      end, Effects))).

enq_enq_checkout_test() ->
    {effects, State1, _} = enq(1, first, #state{}),
    {effects, State2, _} = enq(2, second, State1),
    {effects, _State3, Effects} =
        apply(3, {checkout, {once, 2}, self()}, State2),
    ?assertEffect({monitor, _, _}, Effects),
    ok.

release_cursor_test() ->
    {effects, State1, _} = enq(1, first, #state{}),
    {effects, State2, _} = enq(2, second, State1),
    {effects, State3, _} = check(3, 10, State2),
    {effects, State4, Effects} = settle(4, 1, State3),
    % no release cursor update at this point
    ?assert(lists:any(fun ({release_cursor, _}) -> false;
                          (_) -> true
                      end, Effects)),
    {effects, _Final, Effects1} = settle(5, 0, State4),
    % empty queue forwards release cursor all the way
    ?assertEffect({release_cursor, 5}, Effects1),
    ok.

checkout_enq_settle_test() ->
    {effects, State1, [{monitor, _, _}, _]} = check(1, #state{}),
    {effects, State2, Effects0} = enq(1, first, State1),
    ?assertEffect({send_msg, _, {msg, 0, first}}, Effects0),
    {effects, State3, [_]} = enq(2, second, State2),
    {effects, _, Effects} = settle(3, 0, State3),
    % the release cursor is the smallest raft index that still
    % contribute to the state of the application
    ?assertEffect({release_cursor, 2}, Effects),
    ok.

down_customer_returns_unsettled_test() ->
    {effects, State0, [_]} = enq(1, second, init(test)),
    {effects, State1, [{monitor, process, Pid}, _, _Del]} = check(2, State0),
    {effects, State2, [_]} = apply(3, {down, Pid}, State1),
    {effects, _State, Effects} = check(4, State2),
    ?assertEffect({monitor, process, _}, Effects),
    ok.

completed_customer_yields_demonitor_effect_test() ->
    {effects, State0, [{incr_metrics, _, _}]} = enq(1, second, init(test)),
    {effects, State1, [{monitor, process, _}, _, _Msg]} = check(2, State0),
    {effects, _, Effects} = settle(3, 0, State1),
    ?assertEffect({demonitor, _}, Effects),
    % release cursor for empty queue
    ?assertEffect({release_cursor, 3}, Effects),
    ok.

enq(Idx, Msg, State) ->
    apply(Idx, {enqueue, Msg}, State).

check(Idx, State) ->
    apply(Idx, {checkout, {once, 1}, self()}, State).

check(Idx, Num, State) ->
    apply(Idx, {checkout, {once, Num}, self()}, State).

settle(Idx, MsgId, State) ->
    apply(Idx, {settle, MsgId, self()}, State).

-endif.

