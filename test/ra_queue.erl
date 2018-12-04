-module(ra_queue).

-behaviour(ra_machine).

-export([
         init/1,
         apply/4,
         state_enter/2,
         tick/2,
         overview/1
        ]).
init(_) -> [].

apply(#{index := Idx}, {enq, Msg}, Effects, State) ->
    {State ++ [{Idx, Msg}], Effects, ok};
apply(_Meta, {deq, ToPid}, Effects, [{EncIdx, Msg} | State]) ->
    {State, [{send_msg, ToPid, Msg},
             {release_cursor, EncIdx, []}
             | Effects], ok};
apply(_Meta, deq, Effects, [{EncIdx, Msg} | State]) ->
    {State, [{release_cursor, EncIdx, State} | Effects], Msg};
apply(_Meta, _, Effects, [] = State) ->
    {State, Effects, ok}.


state_enter(_, _) -> [].

tick(_, _) -> [].


overview(_) -> #{}.
