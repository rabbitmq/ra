-module(ra_fifo).

-compile({no_auto_import, [apply/3]}).

-include("ra.hrl").

-export([
         init/1,
         apply/3,
         shadow_copy/1,
         size_test/2,
         perf_test/2,
         profile/1
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
         next_msg_id = 0 :: msg_id(), % part of snapshot data
         num = 0 :: non_neg_integer(), % part of snapshot data
         % number of allocated messages
         % part of snapshot data
         seen = 0 :: non_neg_integer(),
         lifetime = once :: once | auto}).

-record(state, {name :: atom(),
                % unassigned messages
                messages = #{} :: #{ra_index() => msg()},
                % master index of all enqueue raft indexes
                % ra_fifo_index so that can take the smallest
                % TODO: ra_fifo_index are too slow for insert heavy workloads
                % replace with some kidn of map based abstraction
                ra_indexes = ra_fifo_index:empty() :: ra_fifo_index:state(),
                % defines the lowest index available in the messages map
                low_index :: ra_index() | undefined,
                % the raft index of the first enqueue operation that
                % contribute to the current state
                first_enqueue_raft_index :: ra_index() | undefined,
                % customers need to reflect customer state at time of snapshot
                % needs to be part of snapshot
                customers = #{} :: #{customer_id() => #customer{}},
                % customers that require further service are queued here
                % needs to be part of snapshot
                service_queue = queue:new() :: queue:queue(customer_id())
               }).

-type state() :: #state{}.

-export_type([protocol/0]).

-spec init(atom()) -> state().
init(Name) ->
    #state{name = Name}.

shadow_copy(#state{customers = Customers} = State) ->
    % creates a copy of the current state suitable for snapshotting
    State#state{messages = #{},
                ra_indexes = ra_fifo_index:empty(),
                low_index = undefined,
                first_enqueue_raft_index = undefined,
                % TODO: optimise
                % this is inefficient (from a memory use point of view)
                % as it creates a new tuple for every customer
                % even if they haven't changed instead we could just update a copy
                % of the last dehydrated state with the difference
                customers = maps:map(fun (_, V) -> V#customer{checked_out = #{}} end,
                                     Customers)
               }.


% msg_ids are scoped per customer
% ra_indexes holds all raft indexes for enqueues currently on queue
apply(RaftIdx, {enqueue, Msg}, #state{ra_indexes = Indexes,
                                      messages = Messages,
                                      first_enqueue_raft_index = FirstEnqueueIdx,
                                      low_index = Low} = State0) ->
    State1 = State0#state{ra_indexes = ra_fifo_index:append(RaftIdx, shadow_copy(State0), Indexes),
                          messages = Messages#{RaftIdx => Msg},
                          first_enqueue_raft_index = min(RaftIdx, FirstEnqueueIdx),
                          low_index = min(RaftIdx, Low)},
    {State, Effects, Num} = checkout(State1, []),
    Metric = {incr_metrics, ?METRICS_TABLE, [{2, 1}, {3, Num}]},
    {effects, State, [Metric | Effects]};
apply(RaftIdx, {settle, MsgId, CustomerId},
      #state{customers = Custs0} = State) ->
    case Custs0 of
        #{CustomerId := Cust0 = #customer{checked_out = Checked0}} ->
            case maps:take(MsgId, Checked0) of
                error ->
                    % null operation
                    % we must be recovering after a snapshot
                    % in this case it should not have any effect on the final
                    % state
                    {effects, State, []};
                {{MsgRaftIdx, _}, Checked} ->
                    settle(RaftIdx, CustomerId, MsgRaftIdx,
                           Cust0, Checked, State)
            end;
        _ ->
            {effects, State, []}
    end;
apply(_RaftIdx, {checkout, Spec, Customer}, State0) ->
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

settle(IncomingRaftIdx, CustomerId, MsgRaftIdx, Cust0, Checked,
       #state{customers = Custs0, service_queue = SQ0,
              ra_indexes = Indexes0} = State0) ->
    Cust = Cust0#customer{checked_out = Checked},
    {Custs, SQ, Effects0} = update_or_remove_sub(CustomerId, Cust, Custs0, SQ0),
    Indexes = ra_fifo_index:delete(MsgRaftIdx, Indexes0),
    {State1, Effects1, NumChecked} =
        checkout(State0#state{customers = Custs,
                              ra_indexes = Indexes,
                              service_queue = SQ},
                 Effects0),
    Effects2 = [{incr_metrics, ?METRICS_TABLE,
                [{3, NumChecked}, {4, 1}]} | Effects1],
    {State, Effects} = update_first_enqueue_raft_index(IncomingRaftIdx,
                                                       MsgRaftIdx, Effects2,
                                                       State1),
    {effects, State, Effects}.

update_first_enqueue_raft_index(IncomingRaftIdx, MsgRaftIdx, Effects,
                                #state{first_enqueue_raft_index = First,
                                       ra_indexes = Indexes} = State) ->
    case ra_fifo_index:size(Indexes) of
        0 ->
            % there are no messages on queue anymore
            % we can forward release_cursor all the way until
            % the last received command
            {State#state{first_enqueue_raft_index = undefined},
             [{release_cursor, IncomingRaftIdx, shadow_copy(State)} | Effects]};
        _ when First =:= MsgRaftIdx ->
            % the first_enqueue_raft_index can be fowarded to next
            % available message
            {Smallest, Shadow} = ra_fifo_index:smallest(Indexes),
            % we emit the last index _not_ to contribute to the
            % current state - hence the -1
            {State#state{first_enqueue_raft_index = Smallest},
             [{release_cursor, Smallest - 1, Shadow} | Effects]};
        _ ->
            % first_enqueue_raft_index  cannot be forwarded
            {State, Effects}
    end.

return(RaftId, Msg, #state{messages = Messages,
                           % ra_indexes = Indexes,
                           low_index = Low0} = State0) ->
    % this should not affect the release cursor in any way
    State0#state{messages = maps:put(RaftId, Msg, Messages),
                 % ra_indexes = ra_fifo_index:enter(RaftId,  Indexes),
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
                                                 low_index = ra_fifo_index:next_key_after(LowIdx, Indexes),
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
    S0 = run_log(1, NumMsg, EnqGen, init(size_test)),
    S = run_log(NumMsg, NumMsg + NumCust, CustGen, S0),
    S2 = S#state{ra_indexes = ra_fifo_index:map(fun(_, _) -> undefined end, S#state.ra_indexes)},
    {erts_debug:size(S), erts_debug:size(S2)}.

perf_test(NumMsg, NumCust) ->
    timer:tc(fun () ->
                     EnqGen = fun(N) -> {N, {enqueue, N}} end,
                     Pid = spawn(fun() -> ok end),
                     CustGen = fun(N) -> {N, {checkout, {auto, NumMsg}, Pid}} end,
                     SetlGen = fun(N) -> {N, {settle, N - NumMsg - NumCust - 1, Pid}} end,
                     S0 = run_log(1, NumMsg, EnqGen, init(size_test)),
                     S1 = run_log(NumMsg, NumMsg + NumCust, CustGen, S0),
                     _ = run_log(NumMsg, NumMsg + NumCust + NumMsg, SetlGen, S1),
                     ok
             end).

profile(File) ->
    GzFile = atom_to_list(File) ++ ".gz",
    lg:trace([ra_fifo, maps, queue, ra_fifo_index], lg_file_tracer,
             GzFile, #{running => false, mode => profile}),
    NumMsg = 10000,
    NumCust = 500,
    EnqGen = fun(N) -> {N, {enqueue, N}} end,
    Pid = spawn(fun() -> ok end),
    CustGen = fun(N) -> {N, {checkout, {auto, NumMsg}, Pid}} end,
    SetlGen = fun(N) -> {N, {settle, N - NumMsg - NumCust - 1, Pid}} end,
    S0 = run_log(1, NumMsg, EnqGen, init(size_test)),
    S1 = run_log(NumMsg, NumMsg + NumCust, CustGen, S0),
    _ = run_log(NumMsg, NumMsg + NumCust + NumMsg, SetlGen, S1),
    lg:stop().


run_log(Num, Num, _Gen, State) ->
    State;
run_log(Num, Max, Gen, State0) ->
    {_, E} = Gen(Num),
    run_log(Num+1, Max, Gen, element(2, apply(Num, E, State0))).


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
    ?assertEffect({release_cursor, 5, _}, Effects1),
    ok.

checkout_enq_settle_test() ->
    {effects, State1, [{monitor, _, _}, _]} = check(1, #state{}),
    {effects, State2, Effects0} = enq(2, first, State1),
    ?assertEffect({send_msg, _, {msg, 0, first}}, Effects0),
    {effects, State3, [_]} = enq(3, second, State2),
    {effects, _, Effects} = settle(4, 0, State3),
    % the release cursor is the smallest raft index that does not
    % contribute to the state of the application
    ?assertEffect({release_cursor, 2, _}, Effects),
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
    ?assertEffect({release_cursor, 3, _}, Effects),
    ok.


release_cursor_snapshot_state_test() ->
    OthPid = spawn(fun () -> ok end),
    Commands = [
                {checkout, {auto, 5}, self()},
                {enqueue, 0},
                {enqueue, 1},
                {settle, 0, self()},
                {enqueue, 2},
                {settle, 1, self()},
                {checkout, {auto, 4}, OthPid},
                {enqueue, 3},
                {enqueue, 4},
                {settle, 2, self()},
                {settle, 3, self()},
                {enqueue, 5},
                {settle, 0, OthPid},
                {enqueue, 6},
                {settle, 4, self()},
                {checkout, {once, 0}, OthPid}
              ],
    Indexes = lists:seq(1, length(Commands)),
    Entries = lists:zip(Indexes, Commands),
    {State, Effects} = run_log(init(help), Entries),

    [begin
         Filtered = lists:dropwhile(fun({X, _}) when X =< SnapIdx -> true;
                                       (_) -> false
                                    end, Entries),
         {S, _} = run_log(SnapState, Filtered),
         % assert log can be restored from any release cursor index
         ?assert(S =:= State)
     end || {release_cursor, SnapIdx, SnapState} <- Effects],
    ok.

performance_test() ->
    % just under ~500ms on my machine [Karl]
    NumMsgs = 100000,
    {Taken, _} = perf_test(NumMsgs, 0),
    ?debugFmt("performance_test took ~p ms for ~p messages",
              [Taken / 1000, NumMsgs]),
    ok.

enq(Idx, Msg, State) ->
    apply(Idx, {enqueue, Msg}, State).

check(Idx, State) ->
    apply(Idx, {checkout, {once, 1}, self()}, State).

check(Idx, Num, State) ->
    apply(Idx, {checkout, {once, Num}, self()}, State).

settle(Idx, MsgId, State) ->
    apply(Idx, {settle, MsgId, self()}, State).

run_log(InitState, Entries) ->
    lists:foldl(fun ({Idx, E}, {Acc0, Efx0}) ->
                        {effects, Acc, Efx} = apply(Idx, E, Acc0),
                        {Acc, Efx0 ++ Efx}
                end, {InitState, []}, Entries).
-endif.

