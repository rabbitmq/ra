-module(ra_queue).

-behaviour(ra_machine).

-export([
         init/1,
         apply/3,
         state_enter/2,
         tick/2,
         overview/1
        ]).
init(_) -> [].

apply(#{index := Idx}, {enq, Msg}, State) ->
    {State ++ [{Idx, Msg}], ok};
apply(_Meta, {deq, ToPid}, [{EncIdx, Msg} | State]) ->
    {State, ok, [{send_msg, ToPid, Msg},
                 {release_cursor, EncIdx, []}]};
apply(_Meta, deq, [{EncIdx, Msg} | State]) ->
    {State, Msg, {release_cursor, EncIdx, State}};
apply(_Meta, _, [] = State) ->
    {State, ok}.


state_enter(_, _) -> [].

tick(_, _) -> [].


overview(_) -> #{}.
