-module(ra_mt_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-export([
         ]).

-include_lib("eunit/include/eunit.hrl").

%%%===================================================================
%%% Common Test callbacks
%%%===================================================================

all() ->
    [
     {group, tests}
    ].


all_tests() ->
    [
     basics,
     fold,
     record_flushed,
     record_flushed_missed,
     record_flushed_missed_prev,
     record_flushed_after_set_first,
     record_flushed_prev,
     set_first,
     set_first_with_multi_prev,
     set_first_with_middle_small_range,
     set_first_with_old_larger_range,
     set_first_with_old_smaller_range,
     successor,
     successor_below,
     stage_commit,
     range_overlap,
     stage_commit_2,
     perf,
     sparse,
     sparse_after_non_sparse
    ].

groups() ->
    [{tests, [], all_tests()}].

%%%===================================================================
%%% Test cases
%%%===================================================================

basics(_Config) ->
    Tid = ets:new(t1, [set, public]),
    Mt0 = ra_mt:init(Tid),
    Mt1 = lists:foldl(
            fun (I, Acc) ->
                    element(2, ra_mt:insert({I, 1, <<"banana">>}, Acc))
            end, Mt0, lists:seq(1, 1000)),
    {[Spec], Mt2} = ra_mt:set_first(500, Mt1),
    499 = ra_mt:delete(Spec),
    ?assertEqual({500, 1000}, ra_mt:range(Mt2)),
    ?assertEqual(501, ets:info(Tid, size)),
    ?assertEqual(lists:seq(510, 505, -1),
                 ra_mt:fold(505, 510, fun ({I, _, _}, Acc) ->
                                              [I | Acc]
                                      end, [], Mt2)),
    {Spec2, Mt3} = ra_mt:record_flushed(Tid, [{1, 999}], Mt2),
    500 = ra_mt:delete(Spec2),
    ?assertEqual(1, ra_mt:lookup_term(1000, Mt3)),
    ok.

fold(_Config) ->
    Tid = ets:new(t1, [set, public]),
    Mt0 = ra_mt:init(Tid),
    Mt1 = lists:foldl(
            fun (I, Acc) ->
                    element(2, ra_mt:insert({I, 1, <<"banana">>}, Acc))
            end, Mt0, lists:seq(1, 1000)),
    {[Spec], Mt2} = ra_mt:set_first(500, Mt1),
    499 = ra_mt:delete(Spec),
    ?assertEqual({500, 1000}, ra_mt:range(Mt2)),
    ?assertEqual(501, ets:info(Tid, size)),
    ?assertEqual(lists:seq(510, 505, -1),
                 ra_mt:fold(505, 510, fun ({I, _, _}, Acc) ->
                                              [I | Acc]
                                      end, [], Mt2)),
    ?assertError({missing_key, 1001, _},
                 ra_mt:fold(999, 1010,
                            fun ({I, _, _}, Acc) ->
                                    [I | Acc]
                            end, [], Mt2)),
    ?assertEqual([1000, 999],
                 ra_mt:fold(999, 1010,
                            fun ({I, _, _}, Acc) ->
                                    [I | Acc]
                            end, [], Mt2, return)),
    ok.

record_flushed(_Config) ->
    %%TODO: test that deletes the same spec twice
    Tid = ets:new(t1, [set, public]),
    Mt0 = ra_mt:init(Tid),
    Mt1 = lists:foldl(
            fun (I, Acc) ->
                    element(2, ra_mt:insert({I, 1, <<"banana">>}, Acc))
            end, Mt0, lists:seq(1, 100)),
    {Spec, Mt2} = ra_mt:record_flushed(Tid, [{1, 49}], Mt1),
    ?assertMatch({indexes, _, [{1, 49}]}, Spec),
    ?assertMatch({50, 100}, ra_mt:range(Mt2)),
    _ = ra_mt:delete(Spec),
    {Spec2, Mt3} = ra_mt:record_flushed(Tid, [{1, 49}], Mt2),
    ?assertMatch(undefined, Spec2),
    _ = ra_mt:delete(Spec2),
    {Spec3, Mt4} = ra_mt:record_flushed(Tid, [{50, 100}], Mt3),
    ?assertMatch({indexes, _, [{50, 100}]}, Spec3),
    ?assertEqual(undefined, ra_mt:range(Mt4)),
    _ = ra_mt:delete(Spec3),
    ?assertMatch(#{size := 0}, ra_mt:info(Mt4)),
    ok.

record_flushed_missed(_Config) ->
    Tid = ets:new(t1, [set, public]),
    Mt0 = ra_mt:init(Tid),
    Mt1 = lists:foldl(
            fun (I, Acc) ->
                    element(2, ra_mt:insert({I, 1, <<"banana">>}, Acc))
            end, Mt0, lists:seq(1, 105)),
    {Spec3, Mt4} = ra_mt:record_flushed(Tid, [{50, 100}], Mt1),
    ?assertMatch({indexes, _, [{1, 100}]}, Spec3),
    ?assertEqual({101, 105}, ra_mt:range(Mt4)),
    _ = ra_mt:delete(Spec3),
    ?assertMatch(#{size := 5}, ra_mt:info(Mt4)),
    ok.

record_flushed_missed_prev(_Config) ->
    %% test that a prior mem table is cleared up when the current one is
    %% recorded flushed
    Tid = ets:new(t1, [set, public]),
    Mt0 = ra_mt:init(Tid),
    Mt1 = lists:foldl(
            fun (I, Acc) ->
                    element(2, ra_mt:insert({I, 1, <<"banana">>}, Acc))
            end, Mt0, lists:seq(1, 49)),

    Tid2 = ets:new(t2, [set, public]),
    Mt2 = ra_mt:init_successor(Tid2, read_write, Mt1),
    Mt3 = lists:foldl(
            fun (I, Acc) ->
                    element(2, ra_mt:insert({I, 2, <<"apple">>}, Acc))
            end, Mt2, lists:seq(25, 105)),

    {Spec3, Mt4} = ra_mt:record_flushed(Tid2, [{25, 100}], Mt3),
    ?assertMatch({multi, [{indexes, Tid2, [{25, 100}]},
                          {delete, Tid}]}, Spec3),
    ct:pal("Mt4 ~p", [Mt4]),
    ?assertEqual({101, 105}, ra_mt:range(Mt4)),
    _ = ra_mt:delete(Spec3),
    ?assertMatch(#{size := 5}, ra_mt:info(Mt4)),
    ok.

record_flushed_after_set_first(_Config) ->
    Tid = ets:new(t1, [set, public]),
    Mt0 = ra_mt:init(Tid),
    Mt1 = lists:foldl(
            fun (I, Acc) ->
                    element(2, ra_mt:insert({I, 1, <<"banana">>}, Acc))
            end, Mt0, lists:seq(1, 100)),
    {Spec, Mt2} = ra_mt:record_flushed(Tid, [{1, 49}], Mt1),
    ?assertMatch({indexes, _, [{1, 49}]}, Spec),
    ?assertMatch({50, 100}, ra_mt:range(Mt2)),
    _ = ra_mt:delete(Spec),
    {[Spec2], Mt3} = ra_mt:set_first(150, Mt2),
    ?assertMatch({indexes, Tid, [{50, 100}]}, Spec2),
    ?assertMatch(undefined, ra_mt:range(Mt3)),
    {undefined, Mt4} = ra_mt:record_flushed(Tid, [{1, 49}], Mt3),
    ?assertMatch(undefined, ra_mt:range(Mt4)),
    ok.

record_flushed_prev(_Config) ->
    Tid = ets:new(t1, [set, public]),
    Mt0 = ra_mt:init(Tid),
    Mt1 = lists:foldl(
            fun (I, Acc) ->
                    element(2, ra_mt:insert({I, 1, <<"banana">>}, Acc))
            end, Mt0, lists:seq(1, 100)),

    Tid2 = ets:new(t2, [set, public]),
    Mt2 = ra_mt:init_successor(Tid2, read_write, Mt1),
    ?assertMatch({1, 100}, ra_mt:range(Mt2)),
    Mt3 = lists:foldl(
            fun (I, Acc) ->
                    element(2, ra_mt:insert({I, 2, <<"banana">>}, Acc))
            end, Mt2, lists:seq(50, 80)),
    ?assertMatch({1, 100}, ra_mt:range(ra_mt:prev(Mt3))),
    ?assertMatch({1, 80}, ra_mt:range(Mt3)),
    %%
    {Spec, Mt4} = ra_mt:record_flushed(Tid, [{1, 49}], Mt3),
    ?assertMatch({indexes, Tid, [{1, 49}]}, Spec),
    ?assertMatch({50, 80}, ra_mt:range(Mt4)),
    ?assertMatch({50, 100}, ra_mt:range(ra_mt:prev(Mt4))),
    _ = ra_mt:delete(Spec),

    %% delete the remainder of the old mt
    {Spec2, Mt5} = ra_mt:record_flushed(Tid, [{50, 100}], Mt4),
    ?assertMatch({delete, Tid}, Spec2),
    ?assertEqual(undefined, ra_mt:prev(Mt5)),
    ?assertMatch({50, 80}, ra_mt:range(Mt5)),
    _ = ra_mt:delete(Spec2),
    ok.

set_first(_Config) ->
    %% test with prev
    Tid = ets:new(t1, [set, public]),
    Mt0 = ra_mt:init(Tid),
    Mt1 = lists:foldl(
            fun (I, Acc) ->
                    element(2, ra_mt:insert({I, 1, <<"banana">>}, Acc))
            end, Mt0, lists:seq(1, 100)),
    Tid2 = ets:new(t2, [set, public]),
    Mt2 = ra_mt:init_successor(Tid2, read_write, Mt1),
    Mt3 = lists:foldl(
            fun (I, Acc) ->
                    element(2, ra_mt:insert({I, 2, <<"banana">>}, Acc))
            end, Mt2, lists:seq(50, 120)),
    {[Spec1, Spec2], Mt4} = ra_mt:set_first(75, Mt3),
    ?assertMatch({indexes, Tid2, [{50, 74}]}, Spec1),
    ?assertMatch({delete, Tid}, Spec2),
    ?assertMatch({75, 120}, ra_mt:range(Mt4)),
    ?assertMatch(undefined, ra_mt:prev(Mt4)),

    {[Spec3], Mt5} = ra_mt:set_first(105, Mt4),
    ?assertMatch({indexes, Tid2, [{75, 104}]}, Spec3),
    % ?assertMatch({delete, Tid}, Spec4),
    ?assertMatch({105, 120}, ra_mt:range(Mt5)),
    ?assertMatch(undefined, ra_mt:prev(Mt5)),
    ok.

set_first_with_multi_prev(_Config) ->
    Tid1 = ets:new(t1, []),
    Mt0 = ra_mt:init(Tid1),
    Mt1 = lists:foldl(
            fun (I, Acc) ->
                    element(2, ra_mt:insert({I, 1, <<"banana">>}, Acc))
            end, Mt0, lists:seq(1, 100)),

    Tid2 = ets:new(t2, []),
    Mt2 = lists:foldl(
            fun (I, Acc) ->
                    element(2, ra_mt:insert({I, 2, <<"banana">>}, Acc))
            end, ra_mt:init_successor(Tid2, read_write, Mt1),
            lists:seq(50, 150)),

    Tid3 = ets:new(t2, []),
    Mt3 = lists:foldl(
            fun (I, Acc) ->
                    element(2, ra_mt:insert({I, 3, <<"banana">>}, Acc))
            end, ra_mt:init_successor(Tid3, read_write, Mt2),
            lists:seq(75, 200)),

    ?assertEqual({1, 200}, ra_mt:range(Mt3)),

    {[{indexes, Tid3, [{75, 79}]},
      {delete, Tid2},
      {delete, Tid1}], Mt4} = ra_mt:set_first(80, Mt3),

    {[{indexes, Tid3, [{80, 159}]}], _Mt5} = ra_mt:set_first(160, Mt4),
    ok.

set_first_with_middle_small_range(_Config) ->
    %% {1, 200}, {50, 120}, set_first(105) should delete prev completely as it
    %% will never be needed?? (what about wal recovery?)
    Tid1 = ets:new(t1, []),
    Mt0 = ra_mt:init(Tid1),
    Mt1 = lists:foldl(
            fun (I, Acc) ->
                    element(2, ra_mt:insert({I, 1, <<"banana">>}, Acc))
            end, Mt0, lists:seq(1, 100)),

    Tid2 = ets:new(t2, []),
    Mt2 = lists:foldl(
            fun (I, Acc) ->
                    element(2, ra_mt:insert({I, 2, <<"banana">>}, Acc))
            end, ra_mt:init_successor(Tid2, read_write, Mt1),
            lists:seq(50, 75)),

    Tid3 = ets:new(t2, []),
    Mt3 = lists:foldl(
            fun (I, Acc) ->
                    element(2, ra_mt:insert({I, 3, <<"banana">>}, Acc))
            end, ra_mt:init_successor(Tid3, read_write, Mt2),
            lists:seq(75, 200)),

    ?assertEqual({1, 200}, ra_mt:range(Mt3)),

    {[{indexes, Tid3, [{75, 84}]},
      {delete, Tid2},
      {delete, Tid1}], Mt4} = ra_mt:set_first(85, Mt3),
    ?assertEqual({85, 200}, ra_mt:range(Mt4)),

    {[{indexes, Tid3, [{85, 100}]}
     ], Mt5} = ra_mt:set_first(101, Mt4),
    ?assertEqual({101, 200}, ra_mt:range(Mt5)),
    ?assertEqual(undefined, ra_mt:prev(Mt5)),

    ok.

set_first_with_old_larger_range(_Config) ->
    %% {1, 200}, {50, 120}, set_first(105) should delete prev completely as it
    %% will never be needed?? (what about wal recovery?)
    Tid1 = ets:new(t1, []),
    Mt0 = ra_mt:init(Tid1),
    Mt1 = lists:foldl(
            fun (I, Acc) ->
                    element(2, ra_mt:insert({I, 1, <<"banana">>}, Acc))
            end, Mt0, lists:seq(1, 100)),

    Tid2 = ets:new(t2, []),
    Mt2 = lists:foldl(
            fun (I, Acc) ->
                    element(2, ra_mt:insert({I, 2, <<"banana">>}, Acc))
            end, ra_mt:init_successor(Tid2, read_write, Mt1),
            lists:seq(50, 75)),
    {[{indexes, Tid2, [{50, 75}]},
      {delete, Tid1}], Mt3} = ra_mt:set_first(85, Mt2),
    ct:pal("Mt3 ~p", [Mt3]),
    ?assertEqual(undefined, ra_mt:range(Mt3)),
    ?assertEqual(undefined, ra_mt:prev(Mt3)),
    ok.

set_first_with_old_smaller_range(_Config) ->
    Tid1 = ets:new(t1, []),
    Mt0 = ra_mt:init(Tid1),
    Mt1 = lists:foldl(
            fun (I, Acc) ->
                    element(2, ra_mt:insert({I, 1, <<"banana">>}, Acc))
            end, Mt0, lists:seq(50, 75)),

    Tid2 = ets:new(t2, []),
    Mt2 = lists:foldl(
            fun (I, Acc) ->
                    element(2, ra_mt:insert({I, 2, <<"banana">>}, Acc))
            end, ra_mt:init_successor(Tid2, read_write, Mt1),
            lists:seq(1, 100)),

    ?assertEqual({1, 100}, ra_mt:range(Mt2)),
    {[{indexes, Tid2, [{1, 84}]},
      {delete, Tid1}], Mt3} = ra_mt:set_first(85, Mt2),
    ?assertEqual({85, 100}, ra_mt:range(Mt3)),
    ok.

successor(_Config) ->
    Tid = ets:new(t1, [set, public]),
    Mt0 = ra_mt:init(Tid),
    Mt1 = lists:foldl(
            fun (I, Acc) ->
                    element(2, ra_mt:insert({I, 1, <<"banana">>}, Acc))
            end, Mt0, lists:seq(1, 100)),
    ?assertMatch({1, 100}, ra_mt:range(Mt1)),
    Tid2 = ets:new(t2, [set, public]),
    Mt2 = ra_mt:init_successor(Tid2, read_write, Mt1),
    ?assertMatch({1, 100}, ra_mt:range(Mt2)),
    Mt3 = lists:foldl(
            fun (I, Acc) ->
                    element(2, ra_mt:insert({I, 2, <<"banana">>}, Acc))
            end, Mt2, lists:seq(50, 120)),
    ?assertMatch({1, 120}, ra_mt:range(Mt3)),
    %% assert all entries are readable
    lists:foreach(fun (I) ->
                          T = ra_mt:lookup_term(I, Mt3),
                          ?assertMatch({I, T, _}, ra_mt:lookup(I, Mt3))
                  end, lists:seq(1, 100)),

    {{indexes, Tid, [{1, 20}]}, Mt4a} = ra_mt:record_flushed(Tid, [{1, 20}], Mt3),
    ?assertMatch({21, 120}, ra_mt:range(Mt4a)),

    {{indexes, Tid, [{1, 60}]}, Mt4b} = ra_mt:record_flushed(Tid, [{1, 60}], Mt3),
    ?assertMatch({50, 120}, ra_mt:range(Mt4b)),

    ok.

successor_below(_Config) ->
    Tid = ets:new(t1, [set, public]),
    Mt0 = ra_mt:init(Tid),
    Mt1 = lists:foldl(
            fun (I, Acc) ->
                    element(2, ra_mt:insert({I, 1, <<"banana">>}, Acc))
            end, Mt0, lists:seq(100, 200)),
    ?assertMatch({100, 200}, ra_mt:range(Mt1)),
    Tid2 = ets:new(t2, [set, public]),
    Mt2 = ra_mt:init_successor(Tid2, read_write, Mt1),
    Mt3 = lists:foldl(
            fun (I, Acc) ->
                    element(2, ra_mt:insert({I, 2, <<"banana">>}, Acc))
            end, Mt2, lists:seq(50, 75)),
    ?assertMatch({50, 75}, ra_mt:range(Mt3)),

    {{indexes, Tid, [{100, 150}]}, Mt4a} =
        ra_mt:record_flushed(Tid, [{100, 150}], Mt3),
    ?assertMatch({50, 75}, ra_mt:range(Mt4a)),

    % {{indexes, Tid2, [{50, 60}]}, Mt4b} =
    %     ra_mt:record_flushed(Tid2, [{50, 60}], Mt3),
    % ?assertMatch({61, 75}, ra_mt:range(Mt4b)),
    {{multi, [{indexes, Tid2, [{50, 60}]},
              {delete, Tid}]}, Mt4b} =
        ra_mt:record_flushed(Tid2, [{50, 60}], Mt3),
    ?assertMatch({61, 75}, ra_mt:range(Mt4b)),

    {{delete, Tid}, Mt4c} =
        ra_mt:record_flushed(Tid, [{100, 200}], Mt3),
    ?assertMatch({50, 75}, ra_mt:range(Mt4c)),
    ?assertMatch(#{has_previous := false}, ra_mt:info(Mt4c)),
    ok.

stage_commit(_Config) ->
    Tid = ets:new(t1, [set, public]),
    Mt0 = ra_mt:init(Tid),
    Mt1 = lists:foldl(
            fun (I, Acc) ->
                    element(2, ra_mt:stage({I, 1, <<"banana">>}, Acc))
            end, Mt0, lists:seq(1, 10)),
    ?assertMatch({1, 10}, ra_mt:range(Mt1)),
    [{I, _, _} = ra_mt:lookup(I, Mt1)
    || I <- lists:seq(1, 10)],
    {Entries, Mt2}= ra_mt:commit(Mt1),
    ?assertMatch({1, 10}, ra_mt:range(Mt2)),
    ?assertEqual(10, length(Entries)),
    ?assertMatch([{1, 1, _} | _], Entries),
    [{I, _, _} = ra_mt:lookup(I, Mt2)
    || I <- lists:seq(1, 10)],
    ok.

range_overlap(_Config) ->
    Tid = ets:new(t1, [set, public]),
    Mt0 = ra_mt:init(Tid),
    Mt1 = lists:foldl(
            fun (I, Acc) ->
                    element(2, ra_mt:insert({I, 2, <<"banana">>}, Acc))
            end, Mt0, lists:seq(20, 30)),
    {undefined, {1, 10}} = ra_mt:range_overlap({1, 10}, Mt1),
    {undefined, {31, 40}} = ra_mt:range_overlap({31, 40}, Mt1),
    {{30, 30}, {31, 50}} = ra_mt:range_overlap({30, 50}, Mt1),
    {{20, 30}, {10, 19}} = ra_mt:range_overlap({10, 30}, Mt1),
    {{20, 30}, undefined} = ra_mt:range_overlap({20, 30}, Mt1),
    {{20, 30}, {31, 40}} = ra_mt:range_overlap({20, 40}, Mt1),
    %% TODO: mt: realistically this test will never happen as we will never
    %% request to read entries larger then the last written
    % {{20, 30}, {31, 40}} = ra_mt:range_overlap({10, 40}, Mt1),
    ok.

stage_commit_2(_Config) ->
    Tid = ets:new(t1, [set, public]),
    Mt0 = ra_mt:init(Tid),
    Mt1 = lists:foldl(
            fun (I, Acc) ->
                    element(2, ra_mt:stage({I, 2, <<"banana">>}, Acc))
            end, Mt0, lists:seq(20, 30)),
    ?assertMatch(#{size := 0,
                   range := {20, 30}}, ra_mt:info(Mt1)),

    {[{20, _, _} | _] = Entries, Mt} = ra_mt:commit(Mt1),
    ?assertEqual(11, length(Entries)),

    ?assertMatch(#{size := 11,
                   range := {20, 30}}, ra_mt:info(Mt)),

    ok.

perf(_Config) ->
    Num = 1_000_000,
    Tables = [ra_mt:init(ets:new(t1, [set, public])),
              ra_mt:init(ets:new(t2, [ordered_set, public])),
              ra_mt:init(ets:new(t3, [ordered_set, public,
                                              {write_concurrency, true}]))
             ],

    InsertedMts =
    [begin
         {Taken, MtOut} = timer:tc(?MODULE, insert_n, [0, Num, <<"banana">>, Mt]),
         #{name := Name, size := Size} = ra_mt:info(MtOut),
         ct:pal("~s insert ~b entries took ~bms",
                [Name, Size, Taken div 1000]),
         MtOut
     end || Mt <- Tables
    ],

    [begin
         {Taken, MtOut} = timer:tc(?MODULE, read_keys_n, [0, Num, [0, Num-1], Mt]),
         #{name := Name} = ra_mt:info(MtOut),
         ct:pal("~s read_keys took ~bms",
                [Name, Taken div 1000]),
         ok
     end || Mt <- InsertedMts
    ],

    From = trunc(Num * 0.9),
    To = Num - 2,
    [begin
         {Taken, Read} = timer:tc(?MODULE, read_n, [From, To, [], Mt]),
         #{name := Name} = ra_mt:info(Mt),
         ct:pal("~s read_n ~b took ~bms",
                [Name, length(Read), Taken div 1000]),
         ok
     end || Mt <- InsertedMts
    ],

    Indexes = lists:seq(1, 1000, 2),
    [begin
         Fun = fun () -> _ = ra_mt:get_items(Indexes, Mt) end,
         {Taken, _Read} = timer:tc(?MODULE, do_n, [0, 100, Fun]),
         #{name := Name} = ra_mt:info(Mt),
         ct:pal("~s read_sparse ~b took ~bms",
                [Name, 0, Taken div 1000]),
         ok
     end || Mt <- InsertedMts
    ],

    DelTo = (trunc(Num * 0.9)),
    [begin
         {[Spec], _} = ra_mt:set_first(DelTo-1, Mt),
         {Taken, Deleted} = timer:tc(ra_mt, delete, [Spec]),
          #{name := Name, size := Size} = ra_mt:info(Mt),
         ct:pal("~s size ~b select_delete ~b entries took ~bms Spec ~p",
                [Name, Size, Deleted, Taken div 1000, Spec]),
         ok
     end || Mt <- InsertedMts
    ],

    ok.

%% TODO: expand sparse tests
sparse(_Config) ->
    Tid = ets:new(t1, [set, public]),
    Mt0 = ra_mt:init(Tid),
    {ok, Mt1} = ra_mt:insert_sparse({2, 1, <<"banana">>}, 1, Mt0),
    {ok, Mt2} = ra_mt:insert_sparse({5, 1, <<"banana">>}, 2, Mt1),
    ?assertEqual({2, 5}, ra_mt:range(Mt2)),
    {Spec, Mt3} = ra_mt:record_flushed(Tid, ra_seq:from_list([2, 5]), Mt2),
    2 = ra_mt:delete(Spec),
    ?assertMatch(#{size := 0,
                   range := undefined}, ra_mt:info(Mt3)),
    ?assertEqual(0, ets:info(Tid, size)),
    ok.

sparse_after_non_sparse(_Config) ->
    Tid = ets:new(t1, [set, public]),
    Mt0 = ra_mt:init(Tid),
    Mt1 = lists:foldl(
            fun (I, M0) ->
                    {ok, M} = ra_mt:insert({I, 1, <<"banana">>}, M0),
                    M
            end, Mt0, lists:seq(1, 10)),
    Tid2 = ets:new(t2, [set, public]),
    Mt2 = ra_mt:init_successor(Tid2, read_write, Mt1),

    {ok, Mt3} = ra_mt:insert_sparse({12, 1, <<"banana">>}, undefined, Mt2),
    {ok, Mt4} = ra_mt:insert_sparse({15, 1, <<"banana">>}, 12, Mt3),
    ?assertEqual({1, 15}, ra_mt:range(Mt4)),
    ?assertMatch(#{size := 2,
                   range := {1, 15}}, ra_mt:info(Mt4)),

    {Spec, Mt5} = ra_mt:record_flushed(Tid, ra_seq:from_list(lists:seq(1, 10)), Mt4),
    %% full table delete
    10 = ra_mt:delete(Spec),
    {Spec2, Mt6} = ra_mt:record_flushed(Tid2, ra_seq:from_list([12, 15]), Mt5),
    2 = ra_mt:delete(Spec2),
    ?assertMatch(#{size := 0,
                   range := undefined}, ra_mt:info(Mt6)),
    ?assertEqual(0, ets:info(Tid2, size)),
    ok.

%%% Util

read_n(N, N, Acc, _Mt) ->
    Acc;
read_n(K, N, Acc, Mt) ->
    {K, _, _} = X = ra_mt:lookup(K, Mt),
    read_n(K+2, N, [X | Acc], Mt).

read_keys_n(N, N, _Keys, Mt) ->
    Mt;
read_keys_n(K, N, Keys, Mt) ->
    [{_, _, _} = ra_mt:lookup(Key, Mt) || Key <- Keys],
    read_keys_n(K+1, N, Keys, Mt).

insert_n(N, N, _Data, Mt) ->
    Mt;
insert_n(K, N, Data, Mt) ->
    insert_n(K+1, N, Data,
             element(2, ra_mt:insert({K, 42, Data}, Mt))).

do_n(N, N, _Fun) ->
    ok;
do_n(N, To, Fun) ->
    Fun(),
    do_n(N+1, To, Fun).
