-module(ra_kv_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-export([
         ]).

-include_lib("src/ra.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(SYS, default).
%%%===================================================================
%%% Common Test callbacks
%%%===================================================================

all() ->
    [
     {group, tests}
    ].


all_tests() ->
    [
     basics
    ].

groups() ->
    [
     {tests, [], all_tests()}
    ].

init_per_suite(Config) ->
    %% as we're not starting the ra application and we want the logs
    ra_env:configure_logger(logger),
    {ok, _} = ra:start_in(?config(priv_dir, Config)),
    Config.

end_per_suite(_Config) ->
    application:stop(ra),
    ok.

init_per_group(_Group, Config) ->
    Config.

end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(TestCase, Config) ->
    DataDir = filename:join(?config(priv_dir, Config), TestCase),
    [{data_dir, DataDir}, {cluster_name, TestCase} | Config].

end_per_testcase(_TestCase, _Config) ->
    ok.

%%%===================================================================
%%% Test cases
%%%===================================================================


basics(_Config) ->
    Members = [{kv1, node()}],
    KvId = hd(Members),
    {ok, Members, _} = ra_kv:start_cluster(?SYS, ?FUNCTION_NAME,
                                          #{members => Members}),
    {ok, #{}} = ra_kv:put(KvId, <<"k1">>, <<"k1-value01">>, 5000),
    K2 = <<"k2">>,
    %% write 10k entries of the same key
    [{ok, #{}} = ra_kv:put(KvId, K2, I, 5000)
     || I <- lists:seq(1, 10000)],

    ?assertMatch({ok, #{machine := #{num_keys := 2}}, KvId},
                 ra:member_overview(KvId)),
    ra_log_wal:force_roll_over(ra_log_wal),
    timer:sleep(1000),
    ok = ra:aux_command(KvId, take_snapshot),
    timer:sleep(1000),
    ok = ra:stop_server(default, KvId),
    ok = ra:restart_server(default, KvId),
    {ok, #{index := LastIdx}} = ra_kv:put(KvId, <<"k3">>, <<"k3">>, 5000),
    ct:pal("overview after ~p", [ra:member_overview(KvId)]),
    {ok, #{machine := #{live_indexes := Live}}, _} = ra:member_overview(KvId),
    {ok, {Reads, _}} = ra_server_proc:read_entries(KvId, [LastIdx | Live],
                                                   undefined, 1000),
    ct:pal("ReadRes ~p", [Reads]),

    % debugger:start(),
    % int:i(ra_log),
    % int:i(ra_snapshot),
    % int:i(ra_server_proc),
    % int:break(ra_server_proc, 1922),
    % int:break(ra_log, 873),
    % int:break(ra_log, 1002),
    % int:break(ra_log, 1328),
    KvId2 = {kv2, node()},
    ok = ra_kv:add_member(?SYS, KvId2, KvId),
    timer:sleep(1000),
    {ok, {Reads2, _}} = ra_server_proc:read_entries(KvId2, [LastIdx | Live],
                                                    undefined, 1000),
    ct:pal("ReadRes2 ~p", [Reads2]),
    ct:pal("overview ~p", [ra:member_overview(KvId2)]),
    ?assertEqual(3, map_size(Reads2)),
    ra_log_wal:force_roll_over(ra_log_wal),
    timer:sleep(1000),
    {ok, {Reads3, _}} = ra_server_proc:read_entries(KvId2, [LastIdx | Live],
                                                    undefined, 1000),
    ct:pal("ReadRes3 ~p", [Reads3]),
    ct:pal("overview3 ~p", [ra:member_overview(KvId2)]),
    ?assertEqual(3, map_size(Reads3)),

    ok.
