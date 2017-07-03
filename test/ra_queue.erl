-module(ra_queue).

-export([
         simple_apply/3
        ]).

simple_apply(Idx, {enq, Msg}, State) ->
    {effects, State ++ [{Idx, Msg}], [{snapshot_point, Idx}]};
simple_apply(_Idx, {deq, ToPid}, [{EncIdx, Msg} | State]) ->
    {effects, State, [{send_msg, ToPid, Msg}, {release_up_to, EncIdx}]};
simple_apply(_Idx, deq, [{EncIdx, _Msg} | State]) ->
    {effects, State, [{release_up_to, EncIdx}]};
% due to compaction there may not be a dequeue op to do
simple_apply(_Idx, _, [] = State) ->
    {effects, State, []}.


