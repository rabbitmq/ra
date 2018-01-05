-module(ra_queue).

-export([
         simple_apply/3
        ]).

simple_apply(Idx, {enq, Msg}, State) ->
    {effects, State ++ [{Idx, Msg}], []};
simple_apply(_Idx, {deq, ToPid}, [{EncIdx, Msg} | State]) ->
    {effects, State, [{send_msg, ToPid, Msg}, {release_cursor, EncIdx, []}]};
simple_apply(_Idx, deq, [{EncIdx, _Msg} | State]) ->
    {effects, State, [{release_cursor, EncIdx, []}]};
% due to compaction there may not be a dequeue op to do
simple_apply(_Idx, _, [] = State) ->
    {effects, State, []}.


