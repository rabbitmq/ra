-module(ra_log_snapshot_state).

-export([
         insert/5,
         delete/2,
         smallest/2,
         live_indexes/2,
         snapshot/2
        ]).

-spec insert(ets:table(), ra:uid(), -1 | ra:index(), ra:index(), ra_seq:state()) ->
    ok.
insert(Table, UId, SnapIdx, SmallestIdx, LiveIndexes)
  when is_binary(UId) andalso
       is_integer(SnapIdx) andalso
       is_integer(SmallestIdx) andalso
       is_list(LiveIndexes) ->
    true = ets:insert(Table, {UId, SnapIdx, SmallestIdx, LiveIndexes}),
    ok.

delete(Table, UId) ->
    true = ets:delete(Table, UId),
    ok.

-spec smallest(ets:table(), ra:uid()) ->
    ra:index().
smallest(Table, UId) when is_binary(UId) ->
    ets:lookup_element(Table, UId, 3, 0).

-spec live_indexes(ets:table(), ra:uid()) ->
    ra:index().
live_indexes(Table, UId) when is_binary(UId) ->
    ets:lookup_element(Table, UId, 4, []).

-spec snapshot(ets:table(), ra:uid()) ->
    ra:index() | -1.
snapshot(Table, UId) when is_binary(UId) ->
    ets:lookup_element(Table, UId, 2, -1).

%%% ===================
%%% Internal unit tests
%%% ===================

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

basics_test() ->

    UId = atom_to_binary(?FUNCTION_NAME, utf8),
    T = ets:new(?FUNCTION_NAME, [set]),
    ok = insert(T, UId, 50, 51, []),
    ?assertEqual(51, smallest(T, UId)),
    ?assertEqual(50, snapshot(T, UId)),
    ok.

-endif.
