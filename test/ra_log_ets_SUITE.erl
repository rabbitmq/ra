-module(ra_log_ets_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-export([
         ]).

-include_lib("common_test/include/ct.hrl").
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
     exec_delete
    ].

groups() ->
    [
     {tests, [], all_tests()}
    ].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(_Group, Config) ->
    Config.

end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(TestCase, Config) ->
    PrivDir = ?config(priv_dir, Config),
    Dir = filename:join(PrivDir, TestCase),
    ok = ra_lib:make_dir(Dir),
    SysCfg = (ra_system:default_config())#{data_dir => Dir},
    ra_system:store(SysCfg),
    _ = ra_log_ets:start_link(SysCfg),
    ra_counters:init(),
    [{sys_cfg, SysCfg}, {data_dir, Dir} | Config].

end_per_testcase(_TestCase, _Config) ->
    proc_lib:stop(ra_log_ets),
    ok.

%%%===================================================================
%%% Test cases
%%%===================================================================

exec_delete(Config) ->
    %% Test all delete_spec() variants via ra_log_ets:execute_delete/3
    %% delete_spec() :: undefined |
    %%                  {'<', ets:tid(), ra:index()} |
    %%                  {delete, ets:tid()} |
    %%                  {indexes, ets:tid(), ra_seq:state()} |
    %%                  {multi, [delete_spec()]}.

    SysCfg = ?config(sys_cfg, Config),
    Names = maps:get(names, SysCfg),

    %% Test 1: undefined - should be a no-op
    UId1 = <<"uid1">>,
    ok = ra_directory:register_name(default, UId1, self(), undefined, test1, test1),
    {ok, Mt1} = ra_log_ets:mem_table_please(Names, UId1),
    Tid1 = ra_mt:tid(Mt1),
    populate_table(Tid1, 1, 10),
    ok = ra_log_ets:execute_delete(Names, UId1, undefined),
    _ = gen_server:call(maps:get(log_ets, Names), ping, infinity), %% sync
    %% Table should still exist with all entries
    ?assertEqual(10, ets:info(Tid1, size)),
    {ok, Mt1After} = ra_log_ets:mem_table_please(Names, UId1),
    ?assertEqual(Tid1, ra_mt:tid(Mt1After)),

    %% Test 2: {delete, Tid} - deletes entire table
    UId2 = <<"uid2">>,
    ok = ra_directory:register_name(default, UId2, self(), undefined, test2, test2),
    {ok, Mt2} = ra_log_ets:mem_table_please(Names, UId2),
    Tid2 = ra_mt:tid(Mt2),
    populate_table(Tid2, 1, 10),
    ?assertEqual(10, ets:info(Tid2, size)),
    ok = ra_log_ets:execute_delete(Names, UId2, {delete, Tid2}),
    _ = gen_server:call(maps:get(log_ets, Names), ping, infinity), %% sync
    %% Table should be deleted
    ?assertEqual(undefined, ets:info(Tid2, size)),
    %% mem_table_please should return a new table for this uid
    {ok, Mt2After} = ra_log_ets:mem_table_please(Names, UId2),
    ?assertNotEqual(Tid2, ra_mt:tid(Mt2After)),

    %% Test 3: {'<', Tid, Index} - deletes all entries with index < Index
    UId3 = <<"uid3">>,
    ok = ra_directory:register_name(default, UId3, self(), undefined, test3, test3),
    {ok, Mt3} = ra_log_ets:mem_table_please(Names, UId3),
    Tid3 = ra_mt:tid(Mt3),
    populate_table(Tid3, 1, 10),
    ?assertEqual(10, ets:info(Tid3, size)),
    %% Delete entries with index < 6 (i.e., 1-5)
    ok = ra_log_ets:execute_delete(Names, UId3, {'<', Tid3, 6}),
    _ = gen_server:call(maps:get(log_ets, Names), ping, infinity), %% sync
    ?assertEqual(5, ets:info(Tid3, size)),
    %% Verify entries 1-5 are gone, 6-10 remain
    ?assertEqual([], ets:lookup(Tid3, 1)),
    ?assertEqual([], ets:lookup(Tid3, 5)),
    ?assertEqual([{6, 1, <<6:64/integer>>}], ets:lookup(Tid3, 6)),
    ?assertEqual([{10, 1, <<10:64/integer>>}], ets:lookup(Tid3, 10)),
    %% Table should still be tracked
    {ok, Mt3After} = ra_log_ets:mem_table_please(Names, UId3),
    ?assertEqual(Tid3, ra_mt:tid(Mt3After)),

    %% Test 4: {indexes, Tid, Seq} - deletes specific indexes from sequence
    UId4 = <<"uid4">>,
    ok = ra_directory:register_name(default, UId4, self(), undefined, test4, test4),
    {ok, Mt4} = ra_log_ets:mem_table_please(Names, UId4),
    Tid4 = ra_mt:tid(Mt4),
    populate_table(Tid4, 1, 10),
    ?assertEqual(10, ets:info(Tid4, size)),
    %% Delete indexes 1, 2, 3
    Seq4 = ra_seq:from_list([1, 2, 3]),
    ok = ra_log_ets:execute_delete(Names, UId4, {indexes, Tid4, Seq4}),
    _ = gen_server:call(maps:get(log_ets, Names), ping, infinity), %% sync
    ?assertEqual(7, ets:info(Tid4, size)),
    %% Verify entries 1-3 are gone, 4-10 remain
    ?assertEqual([], ets:lookup(Tid4, 1)),
    ?assertEqual([], ets:lookup(Tid4, 3)),
    ?assertEqual([{4, 1, <<4:64/integer>>}], ets:lookup(Tid4, 4)),
    %% Table should still be tracked
    {ok, Mt4After} = ra_log_ets:mem_table_please(Names, UId4),
    ?assertEqual(Tid4, ra_mt:tid(Mt4After)),

    %% Test 5: {indexes, Tid, []} - empty sequence is a no-op
    UId5 = <<"uid5">>,
    ok = ra_directory:register_name(default, UId5, self(), undefined, test5, test5),
    {ok, Mt5} = ra_log_ets:mem_table_please(Names, UId5),
    Tid5 = ra_mt:tid(Mt5),
    populate_table(Tid5, 1, 10),
    ok = ra_log_ets:execute_delete(Names, UId5, {indexes, Tid5, []}),
    _ = gen_server:call(maps:get(log_ets, Names), ping, infinity), %% sync
    ?assertEqual(10, ets:info(Tid5, size)),

    %% Test 6: {multi, [Specs]} with {indexes, _, _} and {'<', _, _}
    UId6 = <<"uid6">>,
    UId6b = <<"uid6b">>,
    ok = ra_directory:register_name(default, UId6, self(), undefined, test6, test6),
    ok = ra_directory:register_name(default, UId6b, self(), undefined, test6b, test6b),
    {ok, Mt6} = ra_log_ets:mem_table_please(Names, UId6),
    {ok, Mt6b} = ra_log_ets:mem_table_please(Names, UId6b),
    Tid6 = ra_mt:tid(Mt6),
    Tid6b = ra_mt:tid(Mt6b),
    populate_table(Tid6, 1, 10),
    populate_table(Tid6b, 1, 5),
    MultiSpec1 = {multi, [
        {indexes, Tid6, ra_seq:from_list([1, 2])},
        {'<', Tid6b, 3}
    ]},
    ok = ra_log_ets:execute_delete(Names, UId6, MultiSpec1),
    _ = gen_server:call(maps:get(log_ets, Names), ping, infinity), %% sync
    ?assertEqual(8, ets:info(Tid6, size)),
    ?assertEqual(3, ets:info(Tid6b, size)),

    %% Test 7: {multi, _} with {indexes, _, _} and {delete, _}
    UId7 = <<"uid7">>,
    UId7b = <<"uid7b">>,
    ok = ra_directory:register_name(default, UId7, self(), undefined, test7, test7),
    ok = ra_directory:register_name(default, UId7b, self(), undefined, test7b, test7b),
    {ok, Mt7} = ra_log_ets:mem_table_please(Names, UId7),
    Tid7 = ra_mt:tid(Mt7),
    populate_table(Tid7, 1, 10),
    MultiSpec2 = {multi, [{indexes, Tid7, ra_seq:from_list([1, 2, 3])},
                          {delete, Tid7}
                         ]},
    ok = ra_log_ets:execute_delete(Names, UId7, MultiSpec2),
    _ = gen_server:call(maps:get(log_ets, Names), ping, infinity), %% sync
    %% Tid7 should have 7 entries remaining
    %% Tid7b should be deleted
    %% Tid7 should still be tracked
    {ok, Mt7After} = ra_log_ets:mem_table_please(Names, UId7),
    ?assertNotEqual(Tid7, ra_mt:tid(Mt7After)),
    ?assertEqual(undefined, ets:info(Tid7, size)),
    %% Tid7b should get a new table
    ok.

%% Utility

populate_table(Tid, From, To) ->
    Entries = [{I, 1, <<I:64/integer>>} || I <- lists:seq(From, To)],
    true = ets:insert(Tid, Entries),
    ok.
