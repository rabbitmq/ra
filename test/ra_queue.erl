-module(ra_queue).

-behaviour(ra_machine).

-export([
         init/1,
         apply/4,
         leader_effects/1,
         tick/2,
         overview/1
        ]).
init(_) -> {[], []}.

apply(#{index := Idx}, {enq, Msg}, Effects, State) ->
    {State ++ [{Idx, Msg}], Effects, ok};
apply(_Meta, {deq, ToPid}, Effects, [{EncIdx, Msg} | State]) ->
    {State, [{send_msg, ToPid, Msg},
             {release_cursor, EncIdx, []}
             | Effects], ok};
apply(_Meta, deq, Effects, [{EncIdx, _Msg} | State]) ->
    {State, [{release_cursor, EncIdx, []} | Effects], ok};
% due to compaction there may not be a dequeue op to do
apply(_Meta, _, Effects, [] = State) ->
    {State, Effects, ok}.


leader_effects(_) -> [].

tick(_, _) -> [].


overview(_) -> #{}.
