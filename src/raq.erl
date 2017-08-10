-module(raq).

-export([
         s/1,
         sj/2,
         pub/2,
         pub_wait/2,
         pop/1,
         check/1,
         auto/1,
         recv/1
        ]).

-type msg() :: term().
-type idx() :: non_neg_integer().
-type subject_id() :: pid(). % represents the entity that receives messages
% -type settlement_status() :: unsettled | settled.

-type checkout_spec() :: {once | cont, Num :: non_neg_integer()}.

-type protocol() ::
    {enqueue, Msg :: msg()} |
    {checkout, Spec :: checkout_spec(), Subject :: subject_id()} |
    {settle, Idx :: idx(), Subject :: subject_id()}.

-type raq_msg() :: {msg, Idx :: idx(), Msg :: msg()}.

-record(subj, {checked_out = #{} :: #{MsgIndex :: idx() => {RaftIndex :: idx(), msg()}},
               num = 0 :: non_neg_integer(),
               seen = 0 :: non_neg_integer(), % total number of allocated messages
               % settlement = settled :: settlement_status(),
               lifetime = once :: once | auto}).

-record(raq_state, {queue = #{} :: map(),
                    next_index = 0 :: idx(),
                    low_index :: idx() | undefined,
                    subjects = #{} :: #{subject_id() =>  #subj{}},
                    service_queue = queue:new() :: queue:queue()
                   }).

-export_type([protocol/0,
              raq_msg/0]).


raq_apply(RaftIdx, {enqueue, Msg},
          State0 = #raq_state{queue = Queue, low_index = Low,
                             next_index = Next}) ->
    State1 = State0#raq_state{queue = Queue#{Next => {RaftIdx, Msg}},
                              low_index = min(Next, Low),
                              next_index = Next+1},
    {State, Effects} = checkout(State1),
    {effects, State, Effects};
raq_apply(_RaftIdx, {dequeue, _Subj},
          State = #raq_state{low_index = undefined}) ->
    {effects, State, []};
raq_apply(_RaftIdx, {dequeue, Subj},
          State = #raq_state{queue = Queue0,
                             low_index = LowIdx0,
                             next_index = High}) ->
    {_RIdx, Msg} = maps:get(LowIdx0, Queue0),
    {LowIdx, Queue} = settle(LowIdx0, LowIdx0, High, Queue0),
    {effects, State#raq_state{queue = Queue, low_index = LowIdx},
     [{send_msg, Subj, {msg, LowIdx0, Msg}}]};
raq_apply(_RaftId, {checkout, Spec, Subject}, State0) ->
    State1 = update_subj(Subject, Spec, State0),
    {State, Effects} = checkout(State1),
    {effects, State, [{monitor, process, Subject} | Effects]};
raq_apply(_RaftId, {settle, Idx, SubjectId},
          State0 = #raq_state{subjects = Subjs0,
                              service_queue = SQ0}) ->
    case Subjs0 of
        #{SubjectId := Subj0 = #subj{checked_out = Checked}} ->
            Subj = Subj0#subj{checked_out = maps:remove(Idx, Checked)},
            % TODO: issue demonitor effect if Subj was completed
            {Subjs, SQ} = update_or_remove_sub(SubjectId, Subj, Subjs0, SQ0),
            {State, Effects} = checkout(State0#raq_state{subjects = Subjs,
                                                         service_queue = SQ}),
            {effects, State, Effects};
        _ ->
            {effects, State0, []}
    end;
raq_apply(_RaftId, {down, SubjectId},
          State = #raq_state{subjects = Subjs}) ->
    % TODO: return messages to main queue
    {effects, State#raq_state{subjects = maps:remove(SubjectId, Subjs)}, []}.




%%% internal

checkout(State) ->
    checkout0(checkout_one(State), []).

checkout0({State, undefined}, Effects) ->
    {State, lists:reverse(Effects)};
checkout0({State, Efx}, Effects) ->
    checkout0(checkout_one(State), [Efx | Effects]).

checkout_one(State0 = #raq_state{queue = Queue0, low_index = LowIdx,
                                 next_index = High,
                                 service_queue = ServiceQueue0,
                                 subjects = Subs0}) ->
    % messages are available
    case maps:take(LowIdx, Queue0) of
        {{_, Msg} = Item, Queue} ->
            % there are subjects waiting to be serviced
            case queue:out(ServiceQueue0) of
                {{value, SubjectId}, ServiceQueue} ->
                    % process subject checkout
                    case maps:get(SubjectId, Subs0, undefined) of
                        #subj{checked_out = Checked, seen = Seen} = Subj0 ->
                            Subj = Subj0#subj{checked_out = Checked#{LowIdx => Item},
                                              seen = Seen+1},
                            {Subs, ServiceQueue1} =
                                update_or_remove_sub(SubjectId, Subj, Subs0, ServiceQueue),
                            State = State0#raq_state{service_queue = ServiceQueue1,
                                                     low_index = find_next_larger(LowIdx+1, High, Queue),
                                                     queue = Queue, subjects = Subs},
                            {State, {send_msg, SubjectId, {msg, LowIdx, Msg}}};
                        undefined ->
                            % subject did not exist but was queued, recurse
                            checkout_one(State0#raq_state{service_queue = ServiceQueue})
                    end;
                _ ->
                    {State0, undefined}
            end;
        error ->
            {State0, undefined}
    end.

update_or_remove_sub(SubjectId,
                     Subj = #subj{lifetime = once, checked_out = Checked,
                                  num = N, seen = N},
                     Subs, ServiceQueue) ->
    % we have seen enough, no more servicing
    case map_size(Checked)  of
        0 ->
            % we're done
            {maps:remove(SubjectId, Subs), ServiceQueue};
        _ ->
            % there are unsettled items
            {maps:update(SubjectId, Subj, Subs), ServiceQueue}
    end;
update_or_remove_sub(SubjectId, Subj = #subj{lifetime = once}, Subs, ServiceQueue) ->
    {maps:update(SubjectId, Subj, Subs), uniq_queue_in(SubjectId, ServiceQueue)};
update_or_remove_sub(SubjectId, #subj{lifetime = auto, checked_out = Checked,
                                      num = Num} = Subj,
                     Subs, ServiceQueue)
  when map_size(Checked) < Num ->
    {maps:update(SubjectId, Subj, Subs), uniq_queue_in(SubjectId, ServiceQueue)};
update_or_remove_sub(SubjectId, Subj, Subs, ServiceQueue) ->
    {maps:update(SubjectId, Subj, Subs), ServiceQueue}.

uniq_queue_in(Key, Queue) ->
    case queue:member(Key, Queue) of
        true ->
            Queue;
        false ->
            queue:in(Key, Queue)
    end.


update_subj(SubjectId, {Life, Num},
            State0 = #raq_state{subjects = Subs0,
                                service_queue = ServiceQueue0}) ->
    Init = #subj{lifetime = Life, num = Num},
    Subs = maps:update_with(SubjectId,
                            fun(S) ->
                                    S#subj{lifetime = Life, num = Num}
                            end, Init, Subs0),
    ServiceQueue = maybe_queue_subj(SubjectId, maps:get(SubjectId, Subs),
                                    ServiceQueue0),

    State0#raq_state{subjects = Subs,
                     service_queue = ServiceQueue}.

maybe_queue_subj(SubjectId, #subj{checked_out = Checked, num = Num},
                 ServiceQueue0)
  when map_size(Checked) < Num ->
    % subject needs service - check if already on service queue
    case queue:member(SubjectId, ServiceQueue0) of
        true ->
            ServiceQueue0;
        false ->
            queue:in(SubjectId, ServiceQueue0)
    end;
maybe_queue_subj(_, _, ServiceQueue) ->
    ServiceQueue.


settle(Idx, Idx, High, Queue0) ->
    % we can forward lowest index
    Queue = maps:remove(Idx, Queue0),
    LowIdx = find_next_larger(Idx+1, High, Queue),
    {LowIdx, Queue};
settle(Idx, LowIdx, _High, Queue) ->
    {LowIdx, maps:remove(Idx, Queue)}.

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

dec_test() ->
    {effects, #raq_state{queue = #{}}, []} = deq(2, #raq_state{}),
    ok.

enq_dec_test() ->
    Msg = first,
    {effects, State1, []} = enq(1, Msg, #raq_state{}),

    {effects, #raq_state{low_index = undefined, next_index = 1, queue = #{}},
     [{send_msg, _, {msg, 0, first}}]} = deq(2, State1),
    ok.

enq_enq_dec_dec_test() ->
    {effects, State1, _} = enq(1, first, #raq_state{}),
    {effects, State2, _} = enq(2, second, State1),

    {effects, State3 = #raq_state{low_index = 1},
     [{send_msg, _, {msg, 0, first}}]} = deq(3, State2),
    {effects, _State4, [{send_msg, _, {msg, 1, second}}]} = deq(4, State3),
    ok.

enq_enq_checkout_test() ->
    {effects, State1, _} = enq(1, first, #raq_state{}),
    {effects, State2, _} = enq(2, second, State1),
    {effects, State3, [_, _]} =
        raq_apply(3, {checkout, {once, 2}, self()}, State2),
    ra_lib:dump(State3),
    ok.

checkout_enq_settle_test() ->
    {effects, State1, []} = check(1, #raq_state{}),
    {effects, State2, [{send_msg, _, {msg, 0, first}}]} =
        enq(1, first, State1),
    {effects, State3, []} = enq(2, second, State2),
    {effects, _, []} = raq_apply(3, {settle, 0, self()}, State3),
    ok.

enq(Idx, Msg, State) ->
    raq_apply(Idx, {enqueue, Msg}, State).

deq(Idx, State) ->
        raq_apply(Idx, {dequeue, self()}, State).

check(Idx, State) ->
    raq_apply(Idx, {checkout, {once, 1}, self()}, State).

-endif.

%%%
%%% ra api
%%%

s(Vol) ->
    Dir = filename:join(["/Volumes", Vol]),
    start_node(raq, [], fun raq_apply/3, #raq_state{}, Dir).

sj(PeerNode, Vol) ->
    {ok, _, _Leader} = ra:add_node({raq, PeerNode}, {raq, node()}),
    s(Vol).

check(Node) ->
    ra:send({raq, Node}, {checkout, {once, 5}, self()}).

auto(Node) ->
    ra:send({raq, Node}, {checkout, {auto, 5}, self()}).

pub(Node, Msg) ->
    ra:send({raq, Node}, {enqueue, Msg}).

pub_wait(Node, Msg) ->
    ra:send_and_await_consensus({raq, Node}, {enqueue, Msg}).

pop(Node) ->
    ra:send({raq, Node}, {dequeue, self()}),
    receive
        {msg, _, _} = Msg ->
            Msg
    after 5000 ->
              timeout
    end.

recv(Node) ->
    receive
        {msg, Idx, _} = Msg ->
            io:format("got ~p~n", [Msg]),
            ra:send({raq, Node}, {settle, Idx, self()}),
            recv(Node)
    end.


start_node(Name, Nodes, ApplyFun, InitialState, Dir) ->
    Conf = #{log_module => ra_log_file,
             log_init_args => #{directory => Dir},
             initial_nodes => Nodes,
             apply_fun => ApplyFun,
             initial_state => InitialState,
             cluster_id => Name},
    ra:start_node(Name, Conf).

