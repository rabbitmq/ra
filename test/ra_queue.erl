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

apply(Idx, {enq, Msg}, Effects, State) ->
    {State ++ [{Idx, Msg}], Effects};
apply(_Idx, {deq, ToPid}, Effects, [{EncIdx, Msg} | State]) ->
    {State, [{send_msg, ToPid, Msg},
             {release_cursor, EncIdx, []}
             | Effects]};
apply(_Idx, deq, Effects, [{EncIdx, _Msg} | State]) ->
    {State, [{release_cursor, EncIdx, []}
             | Effects]};
% due to compaction there may not be a dequeue op to do
apply(_Idx, _, Effects, [] = State) ->
    {State, Effects}.


leader_effects(_) -> [].

tick(_, _) -> [].


overview(_) -> #{}.
