-module(ra_queue).

-behaviour(ra_machine).

-export([
         init/1,
         apply/3
        ]).
init(_) -> {[], []}.

apply(Idx, {enq, Msg}, State) ->
    {State ++ [{Idx, Msg}], []};
apply(_Idx, {deq, ToPid}, [{EncIdx, Msg} | State]) ->
    {State, [{send_msg, ToPid, Msg}, {release_cursor, EncIdx, []}]};
apply(_Idx, deq, [{EncIdx, _Msg} | State]) ->
    {State, [{release_cursor, EncIdx, []}]};
% due to compaction there may not be a dequeue op to do
apply(_Idx, _, [] = State) ->
    {State, []}.


