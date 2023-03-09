-module(ra_ets_queue).

-export([
         new/0,
         in/2,
         reset/1,
         take/2,
         len/1
         ]).


-record(?MODULE, {tbl :: ets:tid(),
                  next_key = 0 :: non_neg_integer(),
                  len = 0 :: non_neg_integer(),
                  queue = queue:new() :: queue:queue()}).


-opaque state() :: #?MODULE{}.
-type item() :: term().

-export_type([
              state/0
             ]).

-spec new() -> state().
new() ->
    Tid = ets:new(?MODULE, [set, private]),
    #?MODULE{tbl = Tid}.

-spec in(item(), state()) -> state().
in(Item, #?MODULE{tbl = Tbl,
                  queue = Q,
                  len = Len,
                  next_key = NextKey} = State) ->
    true = ets:insert(Tbl, {NextKey, Item}),
    State#?MODULE{next_key = NextKey + 1,
                  len = Len + 1,
                  queue = queue:in(NextKey, Q)}.

-spec len(state()) -> non_neg_integer().
len(#?MODULE{len = Len}) ->
    Len.

-spec reset(state()) -> state().
reset(#?MODULE{tbl = Tbl}) ->
    true = ets:delete_all_objects(Tbl),
    #?MODULE{tbl = Tbl,
             len = 0,
             queue = queue:new()}.

-spec take(non_neg_integer(), state()) ->
    {[item()], state()}.
take(Num, #?MODULE{tbl = Tbl,
                   len = Len,
                   queue = Q0} = State) ->
    {Q, Taken, Out} = queue_take(Num, Q0, Tbl),
    {Out, State#?MODULE{len = Len - Taken,
                        queue = Q}}.

%% Internal

queue_take(N, Q, Tbl) ->
    queue_take(N, Q, Tbl, 0, []).

queue_take(0, Q, _Tbl, Taken, Acc) ->
    {Q, Taken, lists:reverse(Acc)};
queue_take(N, Q0, Tbl, Taken, Acc) ->
    case queue:out(Q0) of
        {{value, Key}, Q} ->
            [{Key, I}] = ets:take(Tbl, Key),
            queue_take(N - 1, Q, Tbl, Taken + 1,
                       [I | Acc]);
        {empty, _} ->
            {Q0, Taken, lists:reverse(Acc)}
    end.
