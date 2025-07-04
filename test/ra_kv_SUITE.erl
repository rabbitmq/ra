-module(ra_kv_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

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
     basics,
     snapshot_replication,
     snapshot_replication_interrupted
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

-define(KV(N),
        binary_to_atom(<<(atom_to_binary(?FUNCTION_NAME))/binary,
                         (integer_to_binary(N))/binary>>)).

snapshot_replication_interrupted(_Config) ->
    Kv1 = ?KV(1), Kv2 = ?KV(2), Kv3 = ?KV(3),
    Members = [{Kv1, node()}, {Kv2, node()}],
    KvId = hd(Members),
    {ok, _, _} = ra_kv:start_cluster(?SYS, ?FUNCTION_NAME,
                                     #{members => Members}),
    ra:transfer_leadership(KvId, KvId),
    Data = crypto:strong_rand_bytes(100_000),
    %% write 10k entries of the same key
    [{ok, #{}} = ra_kv:put(KvId, term_to_binary(I), Data, 5000)
     || I <- lists:seq(1, 10_000)],
    ?assertMatch({ok, #{machine := #{num_keys := 10_000}}, KvId},
                 ra:member_overview(KvId)),

    ra_log_wal:force_roll_over(ra_log_wal),
    ra_log_wal:last_writer_seq(ra_log_wal, <<>>),
    ra_log_segment_writer:await(ra_log_segment_writer),
    ok = ra:aux_command(KvId, take_snapshot),
    ok = ra_lib:retry(
           fun () ->
                   {ok, #{log := #{snapshot_index := SnapIdx,
                                   last_index := LastIdx}}, _} =
                       ra:member_overview(KvId),
                   SnapIdx == LastIdx
           end, 100, 100),
    KvId3 = {Kv3, node()},
    ok = ra_kv:add_member(?SYS, KvId3, KvId),
    KvId3Pid = whereis(Kv3),
    ?assert(is_pid(KvId3Pid)),
    %% wait for the follower to enter snapshot state
    ok = ra_lib:retry(
           fun () ->
                   receive_snapshot == element(2, hd(ets:lookup(ra_state, Kv3)))
           end, 100, 100),

    ct:pal("ra_state ~p", [ets:tab2list(ra_state)]),
    ok = ra:stop_server(?SYS, KvId3),
    [{ok, #{}} = ra_kv:put(KvId, term_to_binary(I), Data, 5000)
     || I <- lists:seq(10_001, 10_010)],
    ok = ra:restart_server(?SYS, KvId3),
    {ok, #{log := #{last_index := Kv1LastIndex  }}, _} = ra:member_overview(KvId),
    ok = ra_lib:retry(
           fun () ->
                   {ok, #{log := #{last_index := LastIdx}}, _} =
                       ra:member_overview(KvId3),
                   Kv1LastIndex == LastIdx
           end, 100, 256),
    ra:delete_cluster([KvId, {Kv2, node()}, KvId3]),
    ok.

snapshot_replication(_Config) ->
    Kv1 = ?KV(1), Kv2 = ?KV(2), Kv3 = ?KV(3),
    Members = [{Kv1, node()}, {Kv2, node()}],
    KvId = hd(Members),
    {ok, _, _} = ra_kv:start_cluster(?SYS, ?FUNCTION_NAME,
                                     #{members => Members}),
    ra:transfer_leadership(KvId, KvId),
    {ok, #{}} = ra_kv:put(KvId, <<"k1">>, <<"k1-value01">>, 5000),
    %% write 10k entries of the same key
    [{ok, #{}} = ra_kv:put(KvId, integer_to_binary(I), I, 5000)
     || I <- lists:seq(1, 5000)],

    ?assertMatch({ok, #{machine := #{num_keys := _}}, KvId},
                 ra:member_overview(KvId)),
    ra_log_wal:force_roll_over(ra_log_wal),
    %% wait for rollover processing
    ra_log_wal:last_writer_seq(ra_log_wal, <<>>),
    %% wait for segment writer to process
    ra_log_segment_writer:await(ra_log_segment_writer),
    %% promt ra_kv to take a snapshot
    ok = ra:aux_command(KvId, take_snapshot),
    ok = ra_lib:retry(
           fun () ->
                   {ok, #{log := #{snapshot_index := SnapIdx,
                                   last_index := LastIdx}}, _} =
                       ra:member_overview(KvId),
                   SnapIdx == LastIdx
           end, 100, 100),

    KvId3 = {Kv3, node()},
    ok = ra_kv:add_member(?SYS, KvId3, KvId),
    KvId3Pid = whereis(Kv3),
    ?assert(is_pid(KvId3Pid)),
    {ok, #{}} = ra_kv:put(KvId, <<"k3">>, <<"k3-value">>, 5000),
    {ok, #{}} = ra_kv:put(KvId, <<"k4">>, <<"k4-value">>, 5000),
    ok = ra:aux_command(KvId, take_snapshot),
    % timer:sleep(1000),
    {ok, #{log := #{last_index := Kv1LastIndex  }}, _} = ra:member_overview(KvId),
    ok = ra_lib:retry(
           fun () ->
                   {ok, #{log := #{last_index := LastIdx}}, _} =
                       ra:member_overview(KvId3),
                   Kv1LastIndex == LastIdx
           end, 100, 100),
    ct:pal("counters ~p", [ra_counters:counters(KvId3, [last_applied])]),
    %% ensure Kv3 did not crash during snapshot replication
    ?assertEqual(KvId3Pid, whereis(Kv3)),

    ok = ra:stop_server(default, KvId3),

    {ok, #{}} = ra_kv:put(KvId, <<"k5">>, <<"k5-value">>, 5000),
    {ok, #{}} = ra_kv:put(KvId, <<"k6">>, <<"k6-value">>, 5000),
    ok = ra:aux_command(KvId, take_snapshot),

    ok = ra:restart_server(default, KvId3),
    {ok, #{log := #{last_index := Kv1LastIndex2}}, _} = ra:member_overview(KvId),
    ok = ra_lib:retry(
           fun () ->
                   {ok, #{log := #{last_index := LastIdx}}, _} =
                       ra:member_overview(KvId3),
                   Kv1LastIndex2 == LastIdx
           end, 100, 100),

    ra:delete_cluster([KvId, {kv2, node()}, KvId3]),
    ok.

basics(_Config) ->
    Kv1 = ?KV(1), Kv2 = ?KV(2), _Kv3 = ?KV(3),
    Members = [{Kv1, node()}],
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
           log := #{range := {_, KvIdLastIdx}}}, _} = ra:member_overview(KvId),
    {ok, {Reads, _}} = ra_server_proc:read_entries(KvId, [LastIdx | Live],
                                                   undefined, 1000),
    ?assertEqual(3, map_size(Reads)),
    % ct:pal("ReadRes ~p", [Reads]),
    KvId2 = {Kv2, node()},
    ok = ra_kv:add_member(?SYS, KvId2, KvId),
    ok = ra_lib:retry(
           fun () ->
                   {ok, #{log := #{range := {_, Last}}}, _} =
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
    ?assertEqual(3, map_size(Reads3)),

    %% TODO: test recovery of kv
    ok = ra:stop_server(default, KvId2),
    ok = ra:restart_server(default, KvId2),
    {ok, {Reads4, _}} = ra_server_proc:read_entries(KvId2, [LastIdx | Live],
                                                    undefined, 1000),

    ?assertEqual(3, map_size(Reads4)),
    ra:trigger_compaction(KvId),
    %% wait for compaction by querying counters
    ok = ra_lib:retry(
           fun () ->
                   #{major_compactions := Maj} =
                       ra_counters:counters(KvId, [major_compactions]),
                   Maj == 1
           end, 10, 100),
    {ok, {Reads5, _}} = ra_server_proc:read_entries(KvId, [LastIdx | Live],
                                                    undefined, 1000),
    ?assertEqual(Reads4, Reads5),
    ct:pal("counters ~p", [ra_counters:overview(KvId)]),
    ra:delete_cluster([KvId, KvId2]),
    ok.
