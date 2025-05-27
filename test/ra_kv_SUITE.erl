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

    ct:pal("kv get ~p", [ra_kv:get(KvId, <<"k1">>, 5000)]),
    ct:pal("leaderboard ~p", [ets:tab2list(ra_leaderboard)]),

    ?assertMatch({ok, #{machine := #{num_keys := 2}}, KvId},
                 ra:member_overview(KvId)),
    ra_log_wal:force_roll_over(ra_log_wal),
    %% wait for rollover processing
    ra_log_wal:last_writer_seq(ra_log_wal, <<>>),
    %% wait for segment writer to process
    ra_log_segment_writer:await(ra_log_segment_writer),
    %% promt ra_kv to take a snapshot
    ok = ra:aux_command(KvId, take_snapshot),
    %% wait for snapshot to complete
    ok = ra_lib:retry(
           fun () ->
                   {ok, #{log := #{snapshot_index := SnapIdx,
                                   num_segments := NumSegments,
                                   last_index := LastIdx}}, _} =
                       ra:member_overview(KvId),
                   SnapIdx == LastIdx andalso NumSegments == 2
           end, 100, 100),
    %% restart server to test recovery
    ok = ra:stop_server(default, KvId),
    ok = ra:restart_server(default, KvId),
    {ok, #{index := LastIdx}} = ra_kv:put(KvId, <<"k3">>, <<"k3">>, 5000),
    {ok, #{machine := #{live_indexes := Live},
           log := #{last_index := KvIdLastIdx}}, _} = ra:member_overview(KvId),
    {ok, {Reads, _}} = ra_server_proc:read_entries(KvId, [LastIdx | Live],
                                                   undefined, 1000),
    ?assertEqual(3, map_size(Reads)),
    % ct:pal("ReadRes ~p", [Reads]),
    KvId2 = {kv2, node()},
    ok = ra_kv:add_member(?SYS, KvId2, KvId),
    ok = ra_lib:retry(
           fun () ->
                   {ok, #{log := #{last_index := Last}}, _} =
                       ra:member_overview(KvId2),
                   Last >= KvIdLastIdx
           end, 100, 100),
    {ok, {Reads2, _}} = ra_server_proc:read_entries(KvId2, [LastIdx | Live],
                                                    undefined, 1000),
    ?assertEqual(3, map_size(Reads2)),
    ra_log_wal:force_roll_over(ra_log_wal),
    ra_log_wal:last_writer_seq(ra_log_wal, <<>>),
    ra_log_segment_writer:await(ra_log_segment_writer),
    {ok, {Reads3, _}} = ra_server_proc:read_entries(KvId2, [LastIdx | Live],
                                                    undefined, 1000),
    ct:pal("ReadRes3 ~p", [Reads3]),
    % ct:pal("overview3 ~p", [ra:member_overview(KvId2)]),
    ?assertEqual(3, map_size(Reads3)),

    %% TODO: test recovery of kv
    ok = ra:stop_server(default, KvId2),
    ok = ra:restart_server(default, KvId2),
    {ok, {Reads4, _}} = ra_server_proc:read_entries(KvId2, [LastIdx | Live],
                                                    undefined, 1000),

    ?assertEqual(3, map_size(Reads4)),
    ok.
