%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2017-2025 Broadcom. All Rights Reserved. The term Broadcom refers to Broadcom Inc. and/or its subsidiaries.
%%
-module(ra_log_segment_writer_SUITE).
-compile(nowarn_export_all).
-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-define(SEGWR, ra_log_segment_writer).

%%
%%

all() ->
    [
     {group, tests}
    ].


all_tests() ->
    [
     accept_mem_tables,
     accept_mem_tables_append,
     accept_mem_tables_overwrite,
     accept_mem_tables_overwrite_same_wal,
     accept_mem_tables_multi_segment,
     accept_mem_tables_multi_segment_max_size,
     accept_mem_tables_multi_segment_overwrite,
     accept_mem_tables_for_down_server,
     accept_mem_tables_with_deleted_server,
     accept_mem_tables_with_corrupt_segment,
     accept_mem_tables_multiple_ranges,
     accept_mem_tables_multiple_ranges_snapshot,
     truncate_segments,
     truncate_segments_with_pending_update,
     truncate_segments_with_pending_overwrite,
     my_segments,
     upgrade_segment_name_format,
     skip_entries_lower_than_snapshot_index,
     skip_all_entries_lower_than_snapshot_index,
     live_indexes_1,
     live_indexes_2
    ].

groups() ->
    [
     {tests, [], all_tests()}
    ].

init_per_group(tests, Config) ->
    ra_env:configure_logger(logger),
    Config.

end_per_group(tests, Config) ->
    Config.

init_per_testcase(TestCase, Config) ->
    logger:set_primary_config(level, all),
    PrivDir = ?config(priv_dir, Config),
    Dir = filename:join(PrivDir, TestCase),
    #{name := System} = SysCfg = ra_system:default_config(),
    ra_system:store(SysCfg),
    _ = ra_log_ets:start_link(SysCfg),
    ra_counters:init(System),
    UId = atom_to_binary(TestCase, utf8),
    ok = ra_directory:register_name(default, UId, self(), undefined,
                                    TestCase, TestCase),
    ok = ra_lib:make_dir(Dir),
    ServerDir = filename:join(Dir, UId),
    ok = ra_lib:make_dir(ServerDir),
    register(TestCase, self()),
    ets:new(ra_log_snapshot_state, [named_table, public]),
    [{uid, UId},
     {server_dir, ServerDir},
     {test_case, TestCase},
     {wal_dir, Dir} | Config].

end_per_testcase(_, Config) ->
    proc_lib:stop(ra_log_ets),
    Config.

accept_mem_tables(Config) ->
    Dir = ?config(wal_dir, Config),
    UId = ?config(uid, Config),
    {ok, TblWriterPid} = ra_log_segment_writer:start_link(#{name => ?SEGWR,
                                                            system => default,
                                                            data_dir => Dir}),
    % fake up a mem segment for Self
    Entries = [{1, 42, a}, {2, 42, b}, {3, 43, c}],
    Mt = make_mem_table(UId, Entries),
    Tid = ra_mt:tid(Mt),
    TidSeqs = [{Tid, [ra_mt:range(Mt)]}],
    Ranges = #{UId => TidSeqs},
    make_wal(Config, "w1.wal"),
    ok = ra_log_segment_writer:accept_mem_tables(?SEGWR, Ranges,
                                                 make_wal(Config, "w1.wal")),
    receive
        {ra_log_event, {segments, TidSeqs, [{SegFile, {1, 3}}]}} ->
            SegmentFile = filename:join(?config(server_dir, Config), SegFile),
            {ok, Seg} = ra_log_segment:open(SegmentFile, #{mode => read}),
            % assert Entries have been fully transferred
            Entries = [{I, T, binary_to_term(B)}
                       || {I, T, B} <- read_sparse(Seg, [1, 2, 3])]
    after 3000 ->
              flush(),
              throw(ra_log_event_timeout)
    end,

    timer:sleep(250),

    % assert wal file has been deleted.
    false = is_wal_file(Config, "w1.wal"),
    ok = gen_server:stop(TblWriterPid),
    ok.

accept_mem_tables_append(Config) ->
    % append to a previously written segment
    Dir = ?config(wal_dir, Config),
    UId = ?config(uid, Config),
    {ok, TblWriterPid} = ra_log_segment_writer:start_link(#{system => default,
                                                            name => ?SEGWR,
                                                            data_dir => Dir}),
    % first batch
    Entries = [{1, 42, a}, {2, 42, b}, {3, 43, c}],
    Tid = ets:new(?FUNCTION_NAME, []),
    _ = make_mem_table(UId, Tid, Entries),
    FlushSpec = #{UId => [{Tid, [{1, 3}]}]},
    ok = ra_log_segment_writer:accept_mem_tables(?SEGWR, FlushSpec,
                                                 make_wal(Config, "w1.wal")),
    % second batch
    Entries2 = [{4, 43, d}, {5, 43, e}],
    _ = make_mem_table(UId, Tid, Entries2),
    FlushSpec2 = #{UId => [{Tid, [{4, 5}]}]},
    ok = ra_log_segment_writer:accept_mem_tables(?SEGWR, FlushSpec2,
                                                 make_wal(Config,  "w2.wal")),
    AllEntries = Entries ++ Entries2,
    receive
        {ra_log_event, {segments, [{Tid, [{4, 5}]}], [{Fn, {1, 5}}]}} ->
            SegmentFile = filename:join(?config(server_dir, Config), Fn),
            {ok, Seg} = ra_log_segment:open(SegmentFile, #{mode => read}),
            % assert Entries have been fully transferred
            AllEntries = [{I, T, binary_to_term(B)}
                          || {I, T, B} <- read_sparse(Seg, lists:seq(1, 5))]
    after 3000 ->
              flush(),
              throw(ra_log_event_timeout)
    end,
    flush(),
    ok = gen_server:stop(TblWriterPid),
    ok.

accept_mem_tables_overwrite(Config) ->
    Dir = ?config(wal_dir, Config),
    {ok, TblWriterPid} = ra_log_segment_writer:start_link(#{system => default,
                                                            name => ?SEGWR,
                                                            data_dir => Dir}),
    UId = ?config(uid, Config),
    Entries = [{3, 42, c}, {4, 42, d}, {5, 42, e}],
    Tid = ra_mt:tid(make_mem_table(UId, Entries)),
    Ranges = #{UId => [{Tid, [{3, 5}]}]},
    ok = ra_log_segment_writer:accept_mem_tables(?SEGWR, Ranges,
                                                 make_wal(Config, "w1.wal")),
    receive
        {ra_log_event, {segments, [{Tid, [{3, 5}]}], [{Fn, {3, 5}}]}} ->
            SegmentFile = filename:join(?config(server_dir, Config), Fn),
            {ok, Seg} = ra_log_segment:open(SegmentFile, #{mode => read}),
            ?assertMatch({_, {3, 5}}, ra_log_segment:segref(Seg)),
            ra_log_segment:close(Seg),
            ok
    after 3000 ->
              flush(),
              throw(ra_log_event_timeout)
    end,
    % second batch
    Entries2 = [{1, 43, a}, {2, 43, b}, {3, 43, c2}],
    Tid2 = ra_mt:tid(make_mem_table(UId, Entries2)),
    Ranges2 = #{UId => [{Tid2, [{1, 3}]}]},
    ok = ra_log_segment_writer:accept_mem_tables(?SEGWR, Ranges2,
                                                 make_wal(Config, "w2.wal")),
    receive
        {ra_log_event, {segments, [{Tid2, [{1, 3}]}], [{Fn2, {1, 3}}]}} ->
            SegmentFile2 = filename:join(?config(server_dir, Config), Fn2),
            {ok, Seg2} = ra_log_segment:open(SegmentFile2, #{mode => read}),
            ?assertMatch({_, {1, 3}}, ra_log_segment:segref(Seg2)),
            C2 = term_to_binary(c2),
            [{1, 43, _}, {2, 43, _}] = read_sparse(Seg2, [1, 2]),
            [{3, 43, C2}] = read_sparse(Seg2, [3]),
            ?assertExit({missing_key, 4}, read_sparse(Seg2, [4]))
    after 3000 ->
              flush(),
              throw(ra_log_event_timeout)
    end,
    flush(),
    ok = gen_server:stop(TblWriterPid),
    ok.

accept_mem_tables_overwrite_same_wal(Config) ->
    Dir = ?config(wal_dir, Config),
    {ok, TblWriterPid} = ra_log_segment_writer:start_link(#{system => default,
                                                            name => ?SEGWR,
                                                            data_dir => Dir}),
    UId = ?config(uid, Config),

    Entries = [{2, 42, b}, {3, 42, c}, {4, 42, d}, {5, 42, e}],
    Tid = ra_mt:tid(make_mem_table(UId, Entries)),
    % second batch
    Entries2 = [{4, 43, d2}, {5, 43, e2}, {6, 43, f}],
    Tid2 = ra_mt:tid(make_mem_table(UId, Entries2)),
    Ranges2 = #{UId => [{Tid2, [{4, 6}]}, {Tid, [{2, 5}]}]},
    ok = ra_log_segment_writer:accept_mem_tables(?SEGWR, Ranges2,
                                                 make_wal(Config, "w2.wal")),
    receive
        {ra_log_event,
         {segments, [{Tid2, [{4, 6}]}, {Tid, [{2, 5}]}], [{Fn, {2, 6}}]}} ->
            SegmentFile = filename:join(?config(server_dir, Config), Fn),
            {ok, Seg} = ra_log_segment:open(SegmentFile, #{mode => read}),
            ?assertMatch({_, {2, 6}}, ra_log_segment:segref(Seg)),
            [{2, 42, _},
             {3, 42, _},
             {4, 43, _},
             {5, 43, _},
             {6, 43, _}] = read_sparse(Seg, [2, 3, 4, 5, 6]),
            ok
    after 3000 ->
              flush(),
              throw(ra_log_event_timeout)
    end,
    flush(),
    ok = gen_server:stop(TblWriterPid),
    ok.

accept_mem_tables_multi_segment(Config) ->
    Dir = ?config(wal_dir, Config),
    UId = ?config(uid, Config),
    % configure max segment size
    Conf = #{data_dir => Dir,
             system => default,
             name => ?SEGWR,
             segment_conf => #{max_count => 8}},
    {ok, Pid} = ra_log_segment_writer:start_link(Conf),
    % more entries than fit a single segment
    Entries = [{I, 2, x} || I <- lists:seq(1, 10)],
    Mt = make_mem_table(UId, Entries),
    Tid = ra_mt:tid(Mt),
    TidSeq = {Tid, [ra_mt:range(Mt)]},
    TidRanges = [TidSeq],
    Ranges = #{UId => TidRanges},
    ok = ra_log_segment_writer:accept_mem_tables(?SEGWR, Ranges,
                                                 make_wal(Config, "w.wal")),
    receive
        {ra_log_event, {segments, TidRanges, [{_, {9, 10}}, {_, {1, 8}}]}} ->
            ok
    after 3000 ->
              flush(),
              throw(ra_log_event_timeout)
    end,
    ok = gen_server:stop(Pid),
    ok.

accept_mem_tables_multi_segment_max_size(Config) ->
    Dir = ?config(wal_dir, Config),
    UId = ?config(uid, Config),
    % configure max segment size
    Conf = #{data_dir => Dir,
             system => default,
             name => ?SEGWR,
             segment_conf => #{max_size => 1000}},
    {ok, Pid} = ra_log_segment_writer:start_link(Conf),
    % more entries than fit a single segment
    Entries = [{I, 2, crypto:strong_rand_bytes(120)} || I <- lists:seq(1, 10)],
    Mt = make_mem_table(UId, Entries),
    Tid = ra_mt:tid(Mt),
    TidRanges = [{Tid, [ra_mt:range(Mt)]}],
    Ranges = #{UId => TidRanges},
    ok = ra_log_segment_writer:accept_mem_tables(?SEGWR, Ranges,
                                                 make_wal(Config, "w.wal")),
    receive
        {ra_log_event, {segments, TidRanges, [{_, {9, 10}}, {_, {1, 8}}]}} ->
            ok
    after 3000 ->
              flush(),
              throw(ra_log_event_timeout)
    end,
    ok = gen_server:stop(Pid),
    ok.

accept_mem_tables_multi_segment_overwrite(Config) ->
    Dir = ?config(wal_dir, Config),
    UId = ?config(uid, Config),
    % configure max segment size
    Conf = #{data_dir => Dir,
             system => default,
             name => ?SEGWR,
             segment_conf => #{max_count => 8}},
    {ok, Pid} = ra_log_segment_writer:start_link(Conf),
    % more entries than fit a single segment
    Entries = [{I, 2, x} || I <- lists:seq(1, 10)],
    Mt = make_mem_table(UId, Entries),
    Tid = ra_mt:tid(Mt),
    TidRanges = [{Tid, [ra_mt:range(Mt)]}],
    Ranges = #{UId => TidRanges},
    ok = ra_log_segment_writer:accept_mem_tables(?SEGWR, Ranges,
                                                 make_wal(Config, "w.wal")),
    LastFile =
    receive
        {ra_log_event, {segments, TidRanges, [{Seg2, {9, 10}}, {_Seg1, {1, 8}}]}} ->
            Seg2
            % ok
    after 3000 ->
              flush(),
              throw(ra_log_event_timeout)
    end,

    Entries2 = [{I, 3, x} || I <- lists:seq(7, 15)],
    Mt2 = make_mem_table(UId, Entries2),
    Tid2 = ra_mt:tid(Mt2),
    TidRanges2 = [{Tid2, [ra_mt:range(Mt2)]}],
    Ranges2 = #{UId => TidRanges2},
    ok = ra_log_segment_writer:accept_mem_tables(?SEGWR, Ranges2,
                                                 make_wal(Config, "w2.wal")),
    receive
        {ra_log_event, {segments, TidRanges2,
                        [{_, {13, 15}}, {LastFile, {7, 12}}]}} ->
            ok
    after 3000 ->
              flush(),
              throw(ra_log_event_timeout)
    end,
    MySegments = ra_log_segment_writer:my_segments(?SEGWR, UId),
    ct:pal("segrefs ~p", [[ra_log_segment:segref(F) || F <- MySegments]]),
    ok = gen_server:stop(Pid),
    ok.

accept_mem_tables_for_down_server(Config) ->
    %% fake a closed mem table
    ets:new(ra_log_closed_mem_tables, [named_table, bag, public]),
    Dir = ?config(wal_dir, Config),
    UId = ?config(uid, Config),
    DownUId = <<"down-uid">>,
    %% only insert into dets so that the server is shown as registered
    %% but not running
    ok = dets:insert(maps:get(directory_rev, get_names(default)),
                     {down_uid, DownUId}),
    true = ets:insert(maps:get(directory, get_names(default)),
                      {DownUId, undefined, undefined, down_uid, undefined}),
    ok = ra_lib:make_dir(filename:join(Dir, DownUId)),
    application:start(sasl),
    {ok, TblWriterPid} = ra_log_segment_writer:start_link(#{system => default,
                                                            name => ?SEGWR,
                                                            data_dir => Dir}),
    % fake up a mem segment for Self
    Entries = [{1, 42, a}, {2, 42, b}, {3, 43, c}],
    Mt = make_mem_table(DownUId, Entries),
    Mt2 = make_mem_table(UId, Entries),
    Tid = ra_mt:tid(Mt),
    Tid2 = ra_mt:tid(Mt2),
    Ranges = #{DownUId => [{Tid, [{1, 3}]}],
               UId => [{Tid2, [{1, 3}]}]},
    WalFile = filename:join(Dir, "00001.wal"),
    ok = file:write_file(WalFile, <<"waldata">>),
    ok = ra_log_segment_writer:accept_mem_tables(?SEGWR, Ranges, WalFile),
    receive
        {ra_log_event, {segments, [{Tid2, [{1, 3}]}], [{Fn, {1, 3}}]}} ->
            SegmentFile = filename:join(?config(server_dir, Config), Fn),
            {ok, Seg} = ra_log_segment:open(SegmentFile, #{mode => read}),
            % assert Entries have been fully transferred
            Entries = [{I, T, binary_to_term(B)}
                       || {I, T, B} <- read_sparse(Seg, [1, 2, 3])]
    after 3000 ->
              flush(),
              throw(ra_log_event_timeout)
    end,
    flush(),
    %% validate fake uid entries were written
    ra_log_segment_writer:await(?SEGWR),
    DownFn = ra_lib:zpad_filename("", "segment", 1),
    ct:pal("DownFn ~s", [DownFn]),
    DownSegmentFile = filename:join([?config(wal_dir, Config),
                                     DownUId, DownFn]),
    {ok, FakeSeg} = ra_log_segment:open(DownSegmentFile, #{mode => read}),
    % assert Entries have been fully transferred
    Entries = [{I, T, binary_to_term(B)}
               || {I, T, B} <- read_sparse(FakeSeg, [1, 2, 3])],

    %% if the server is down at the time the segment writer send the segments
    %% the segment writer should clear up the ETS mem tables
    timer:sleep(500),
    FakeMt = ra_mt:init(Tid),
    ?assertMatch(#{size := 0}, ra_mt:info(FakeMt)),

    % assert wal file has been deleted.
    % the delete happens after the segment notification so we need to retry
    ok = ra_lib:retry(fun() ->
                              false = filelib:is_file(WalFile),
                              ok
                      end, 5, 100),
    ok = gen_server:stop(TblWriterPid),
    ok.

accept_mem_tables_with_deleted_server(Config) ->
    Dir = ?config(wal_dir, Config),
    UId = ?config(uid, Config),
    DeletedUId = <<"not_self">>,
    ok = ra_lib:make_dir(filename:join(Dir, DeletedUId)),
    application:start(sasl),
    {ok, TblWriterPid} = ra_log_segment_writer:start_link(#{system => default,
                                                            name => ?SEGWR,
                                                            data_dir => Dir}),
    % fake up a mem segment for Self
    Entries = [{1, 42, a}, {2, 42, b}, {3, 43, c}],
    {ok, Mt0} = ra_log_ets:mem_table_please(get_names(default), DeletedUId),
    Mt = lists:foldl(fun(E, Acc0) ->
                             {ok, Acc} = ra_mt:insert(E, Acc0),
                             Acc
                     end, Mt0, Entries),

    Mt2 = make_mem_table(UId, Entries),
    Tid = ra_mt:tid(Mt),
    Tid2 = ra_mt:tid(Mt2),
    Ranges = #{DeletedUId => [{Tid, [{1, 3}]}],
               UId => [{Tid2, [{1, 3}]}]},
    WalFile = make_wal(Config, "00001.wal"),
    ok = ra_log_segment_writer:accept_mem_tables(?SEGWR, Ranges, WalFile),
    receive
        {ra_log_event, {segments, [{Tid2, [{1, 3}]}], [{Fn, {1, 3}}]}} ->
            SegmentFile = filename:join(?config(server_dir, Config), Fn),
            {ok, Seg} = ra_log_segment:open(SegmentFile, #{mode => read}),
            % assert Entries have been fully transferred
            Entries = [{I, T, binary_to_term(B)}
                       || {I, T, B} <- read_sparse(Seg, [1, 2, 3])]
    after 3000 ->
              flush(),
              throw(ra_log_event_timeout)
    end,
    %% validate fake uid entries were written
    ra_log_segment_writer:await(?SEGWR),
    FakeSegmentFile = filename:join([?config(wal_dir, Config),
                                     DeletedUId,
                                     "00000001.segment"]),
    ?assertNot(filelib:is_file(FakeSegmentFile)),
    gen_server:call(ra_log_ets, dummy),

    %% if the server is down at the time the segment writer send the segments
    %% the segment writer should clear up the ETS mem tables
    ?assertNot(ra_directory:is_registered_uid(default, DeletedUId)),
    ?assertEqual(undefined, ets:info(Tid)),

    % assert wal file has been deleted.
    % the delete happens after the segment notification so we need to retry
    ok = ra_lib:retry(fun() ->
                              false = filelib:is_file(WalFile),
                              ok
                      end, 5, 100),
    ok = gen_server:stop(TblWriterPid),
    ok.

accept_mem_tables_with_corrupt_segment(Config) ->
    Dir = ?config(wal_dir, Config),
    UId = ?config(uid, Config),
    {ok, TblWriterPid} = ra_log_segment_writer:start_link(#{name => ?SEGWR,
                                                            system => default,
                                                            data_dir => Dir}),
    % fake up a mem segment for Self
    Entries = [{1, 42, a}, {2, 42, b}, {3, 43, c}],
    Mt = make_mem_table(UId, Entries),
    Tid = ra_mt:tid(Mt),
    TidRanges = [{Tid, [ra_mt:range(Mt)]}],
    Ranges = #{UId => TidRanges},
    WalFile = make_wal(Config, "0000001.wal"),
    %% write an empty file to simulate corrupt segment
    %% this can happen if a segment is opened but is interrupted before syncing
    %% the header
    file:write_file(filename:join(?config(server_dir, Config), "0000001.segment"), <<>>),
    ok = ra_log_segment_writer:accept_mem_tables(?SEGWR, Ranges, WalFile),
    receive
        {ra_log_event, {segments, TidRanges, [{SegFile, {1, 3}}]}} ->
            SegmentFile = filename:join(?config(server_dir, Config), SegFile),
            {ok, Seg} = ra_log_segment:open(SegmentFile, #{mode => read}),
            % assert Entries have been fully transferred
            Entries = [{I, T, binary_to_term(B)}
                       || {I, T, B} <- read_sparse(Seg, [1, 2, 3])]
    after 3000 ->
              flush(),
              throw(ra_log_event_timeout)
    end,

    % the delete happens after the segment notification so we need to retry
    ok = ra_lib:retry(fun() ->
                              false = filelib:is_file(WalFile),
                              ok
                      end, 5, 100),
    ok = gen_server:stop(TblWriterPid),
    ok.

accept_mem_tables_multiple_ranges(Config)->
    Dir = ?config(wal_dir, Config),
    SegConf = #{max_count => 16},
    {ok, TblWriterPid} = ra_log_segment_writer:start_link(#{system => default,
                                                            name => ?SEGWR,
                                                            data_dir => Dir,
                                                            segment_conf => SegConf}),
    UId = ?config(uid, Config),
    Entries = [{N, 42, N} || N <- lists:seq(1, 32)],
    Mt = make_mem_table(UId, Entries),
    Entries2 = [{N, 42, N} || N <- lists:seq(33, 64)],
    Mt2 = make_mem_table(UId, Entries2),
    TidRanges = [
                 {ra_mt:tid(Mt2), [ra_mt:range(Mt2)]},
                 {ra_mt:tid(Mt), [ra_mt:range(Mt)]}
                ],
    Ranges = #{UId => TidRanges},
    ok = ra_log_segment_writer:accept_mem_tables(?SEGWR, Ranges,
                                                 make_wal(Config, "w1.wal")),
    receive
        {ra_log_event, {segments, TidRanges, SegRefs}} ->
            ?assertMatch([
                          {_, {49, 64}},
                          {_, {33, 48}},
                          {_, {17, 32}},
                          {_, {1, 16}}
                         ], SegRefs),
            ok
    after 3000 ->
              flush(),
              throw(ra_log_event_timeout)
    end,
    ok = gen_server:stop(TblWriterPid),
    ok.

accept_mem_tables_multiple_ranges_snapshot(Config)->
    Dir = ?config(wal_dir, Config),
    SegConf = #{max_count => 16},
    {ok, TblWriterPid} = ra_log_segment_writer:start_link(#{system => default,
                                                            name => ?SEGWR,
                                                            data_dir => Dir,
                                                            segment_conf => SegConf}),
    UId = ?config(uid, Config),
    Entries = [{N, 42, N} || N <- lists:seq(1, 32)],
    Mt = make_mem_table(UId, Entries),
    Entries2 = [{N, 42, N} || N <- lists:seq(33, 64)],
    Mt2 = make_mem_table(UId, Entries2),
    TidRanges = [
                 {ra_mt:tid(Mt2), [ra_mt:range(Mt2)]},
                 {ra_mt:tid(Mt), [ra_mt:range(Mt)]}
                ],
    Ranges = #{UId => TidRanges},
    ra_log_snapshot_state:insert(ra_log_snapshot_state, UId, 64, 65, []),
    ok = ra_log_segment_writer:accept_mem_tables(?SEGWR, Ranges,
                                                 make_wal(Config, "w1.wal")),

    receive
        {ra_log_event, {segments, TidRanges, SegRefs}} ->
            ?assertMatch([], SegRefs),
            ok
    after 3000 ->
              flush(),
              throw(ra_log_event_timeout)
    end,
    ok = gen_server:stop(TblWriterPid),
    ok.

truncate_segments(Config) ->
    Dir = ?config(wal_dir, Config),
    SegConf = #{max_count => 12},
    {ok, TblWriterPid} = ra_log_segment_writer:start_link(
                           #{name => ?SEGWR, data_dir => Dir, system => default,
                             segment_conf => SegConf}),
    UId = ?config(uid, Config),
    % fake up a mem segment for Self
    Entries = [{N, 42, N} || N <- lists:seq(1, 32)],
    Mt = make_mem_table(UId, Entries),
    Tid = ra_mt:tid(Mt),
    TidRanges = [{Tid, [ra_mt:range(Mt)]}],
    Ranges = #{UId => TidRanges},
    WalFile = make_wal(Config, "0000001.wal"),
    ok = ra_log_segment_writer:accept_mem_tables(?SEGWR, Ranges, WalFile),
    receive
        {ra_log_event, {segments, TidRanges, [{S, {25, 32}} = Cur | Rem]}} ->
            % test a lower index _does not_ delete the file
            SegmentFile = filename:join(?config(server_dir, Config), S),
            ?assert(filelib:is_file(SegmentFile)),
            ok = ra_log_segment_writer:truncate_segments(TblWriterPid,
                                                         UId, Cur),
            ra_log_segment_writer:await(?SEGWR),
            [{S1, _}, {S2, _}] = Rem,
            SegmentFile1 = filename:join(?config(server_dir, Config), S1),
            ?assertNot(filelib:is_file(SegmentFile1)),
            SegmentFile2 = filename:join(?config(server_dir, Config), S2),
            ?assertNot(filelib:is_file(SegmentFile2)),
            ?assertMatch([_], segments_for(UId, Dir)),
            ok
    after 3000 ->
              throw(ra_log_event_timeout)
    end,
    ok = gen_server:stop(TblWriterPid),
    ok.

truncate_segments_with_pending_update(Config) ->
    Dir = ?config(wal_dir, Config),
    SegConf = #{max_count => 12},
    {ok, TblWriterPid} = ra_log_segment_writer:start_link(#{system => default,
                                                            name => ?SEGWR,
                                                            data_dir => Dir,
                                                            segment_conf => SegConf}),
    UId = ?config(uid, Config),
    Entries = [{N, 42, N} || N <- lists:seq(1, 32)],
    Mt = make_mem_table(UId, Entries),
    Ranges = #{UId => [{ra_mt:tid(Mt), [ra_mt:range(Mt)]}]},
    ok = ra_log_segment_writer:accept_mem_tables(?SEGWR, Ranges,
                                                 make_wal(Config, "w1.wal")),
    ra_log_segment_writer:await(?SEGWR),
    %% write another range
    Entries2 = [{N, 42, N} || N <- lists:seq(33, 40)],
    Mt2 = make_mem_table(UId, Entries2),
    Ranges2 = #{UId => [{ra_mt:tid(Mt2), [ra_mt:range(Mt2)]}]},
    ok = ra_log_segment_writer:accept_mem_tables(?SEGWR, Ranges2,
                                                 make_wal(Config, "w2.erl")),
    receive
        {ra_log_event, {segments, _TidRanges, [{S, {25, 32}} = Cur | Rem]}} ->
            % this is the event from the first call to accept_mem_tables,
            % the Cur segments has been appended to since so should _not_
            % be deleted when it is provided as the cutoff segref for
            % truncation
            SegmentFile = filename:join(?config(server_dir, Config), S),
            ?assert(filelib:is_file(SegmentFile)),
            ok = ra_log_segment_writer:truncate_segments(TblWriterPid,
                                                         UId, Cur),
            ra_log_segment_writer:await(?SEGWR),
            ?assert(filelib:is_file(SegmentFile)),
            [{S1, _}, {S2, _}] = Rem,
            SegmentFile1 = filename:join(?config(server_dir, Config), S1),
            ?assertNot(filelib:is_file(SegmentFile1)),
            SegmentFile2 = filename:join(?config(server_dir, Config), S2),
            ?assertNot(filelib:is_file(SegmentFile2)),
            ok
    after 3000 ->
              flush(),
              throw(ra_log_event_timeout)
    end,
    flush(),
    ok = gen_server:stop(TblWriterPid),
    ok.

truncate_segments_with_pending_overwrite(Config) ->
    Dir = ?config(wal_dir, Config),
    SegConf = #{max_count => 12},
    {ok, TblWriterPid} = ra_log_segment_writer:start_link(#{system => default,
                                                            name => ?SEGWR,
                                                            data_dir => Dir,
                                                            segment_conf => SegConf}),
    UId = ?config(uid, Config),
    % fake up a mem segment for Self
    Entries = [{N, 42, N} || N <- lists:seq(1, 32)],
    Mt = make_mem_table(UId, Entries),
    Ranges = #{UId => [{ra_mt:tid(Mt), [ra_mt:range(Mt)]}]},
    ok = ra_log_segment_writer:accept_mem_tables(?SEGWR, Ranges,
                                                 make_wal(Config, "w1.wal")),
    %% write one more entry separately
    Entries2 = [{N, 43, N} || N <- lists:seq(12, 25)],
    Mt2 = make_mem_table(UId, Entries2),
    Ranges2 = #{UId => [{ra_mt:tid(Mt2), [ra_mt:range(Mt2)]}]},
    ok = ra_log_segment_writer:accept_mem_tables(?SEGWR, Ranges2,
                                                 make_wal(Config, "w2.wal")),
    receive
        {ra_log_event, {segments, _Tid, [{S, {25, 32}} = Cur | Rem]}} ->
            % test a lower index _does not_ delete the file
            SegmentFile = filename:join(?config(server_dir, Config), S),
            ?assert(filelib:is_file(SegmentFile)),
            ok = ra_log_segment_writer:truncate_segments(TblWriterPid,
                                                         UId, Cur),
            _ = ra_log_segment_writer:await(?SEGWR),
            SegmentFile = filename:join(?config(server_dir, Config), S),
            ?assert(filelib:is_file(SegmentFile)),
            [{S1, _}, {S2, _}] = Rem,
            SegmentFile1 = filename:join(?config(server_dir, Config), S1),
            ?assertNot(filelib:is_file(SegmentFile1)),
            SegmentFile2 = filename:join(?config(server_dir, Config), S2),
            ?assertNot(filelib:is_file(SegmentFile2)),
            ct:pal("segments for ~p",  [segments_for(UId, Dir)]),
            ok
    after 3000 ->
              flush(),
              throw(ra_log_event_timeout)
    end,
    receive
        {ra_log_event, {segments, _, [{F, {16, 25}} = Cur2, {F2, {12, 15}}]}} ->
            ?assertMatch([_, _], segments_for(UId, Dir)),
            ok = ra_log_segment_writer:truncate_segments(TblWriterPid,
                                                         UId, Cur2),
            _ = ra_log_segment_writer:await(?SEGWR),
            SegFile = filename:join(?config(server_dir, Config), F),
            ?assertNot(filelib:is_file(SegFile)),
            SegFile2 = filename:join(?config(server_dir, Config), F2),
            ?assertNot(filelib:is_file(SegFile2)),
            %% validate there is a new empty segment
            [NewSegFile] = segments_for(UId, Dir),
            {ok, NewSeg} = ra_log_segment:open(NewSegFile, #{mode => read}),
            ?assertEqual(undefined, ra_log_segment:segref(NewSeg)),
            ok
    after 3000 ->
              flush(),
              throw(ra_log_event_timeout2)
    end,
    ok = gen_server:stop(TblWriterPid),
    ok.

my_segments(Config) ->
    Dir = ?config(wal_dir, Config),
    {ok, TblWriterPid} = ra_log_segment_writer:start_link(#{name => ?SEGWR,
                                                            system => default,
                                                            data_dir => Dir}),
    UId = ?config(uid, Config),
    % fake up a mem segment for Self
    Entries = [{1, 42, a}, {2, 42, b}, {3, 43, c}],
    Mt = make_mem_table(UId, Entries),
    Ranges = #{UId => [{ra_mt:tid(Mt), [ra_mt:range(Mt)]}]},
    TidRanges = maps:get(UId, Ranges),
    WalFile = make_wal(Config, "00001.wal"),
    ok = ra_log_segment_writer:accept_mem_tables(?SEGWR, Ranges, WalFile),
    receive
        {ra_log_event, {segments, TidRanges, [{Fn, {1, 3}}]}} ->
            SegmentFile = filename:join(?config(server_dir, Config), Fn),
            [MyFile] = ra_log_segment_writer:my_segments(?SEGWR,UId),
            ?assertEqual(SegmentFile, unicode:characters_to_binary(MyFile)),
            ?assert(filelib:is_file(SegmentFile))
    after 2000 ->
              flush(),
              exit(ra_log_event_timeout)
    end,
    proc_lib:stop(TblWriterPid),
    ok.

upgrade_segment_name_format(Config) ->
    Dir = ?config(wal_dir, Config),
    {ok, TblWriterPid} = ra_log_segment_writer:start_link(#{name => ?SEGWR,
                                                            system => default,
                                                            data_dir => Dir}),
    UId = ?config(uid, Config),
    % fake up a mem segment for Self
    Entries = [{1, 42, a}, {2, 42, b}, {3, 43, c}],
    Mt = make_mem_table(UId, Entries),
    Ranges = #{UId => [{ra_mt:tid(Mt), [ra_mt:range(Mt)]}]},
    TidRanges = maps:get(UId, Ranges),
    WalFile = make_wal(Config, "00001.wal"),
    ok = ra_log_segment_writer:accept_mem_tables(?SEGWR, Ranges, WalFile),
    File =
    receive
        {ra_log_event, {segments, TidRanges, [{_, {1, 3}}]}} ->
            [MyFile] = ra_log_segment_writer:my_segments(?SEGWR,UId),
            MyFile
    after 2000 ->
              flush(),
              exit(ra_log_event_timeout)
    end,

    %% stop segment writer and rename existing segment to old format
    proc_lib:stop(TblWriterPid),
    Root = filename:dirname(File),
    Base = filename:basename(File),
    {_, FileOld} = lists:split(8, Base),
    ok = file:rename(File, filename:join(Root, FileOld)),
    %% also remove upgrade marker file
    ok = file:delete(filename:join(Dir, "segment_name_upgrade_marker")),
    %% restart segment writer which should trigger upgrade process
    {ok, Pid2} = ra_log_segment_writer:start_link(#{name => ?SEGWR,
                                                    system => default,
                                                    data_dir => Dir}),
    %% validate the renamed segment has been renamed back to the new
    %% 16 character format
    [File] = ra_log_segment_writer:my_segments(?SEGWR, UId),

    proc_lib:stop(Pid2),
    ok.

skip_entries_lower_than_snapshot_index(Config) ->
    Dir = ?config(wal_dir, Config),
    UId = ?config(uid, Config),
    {ok, TblWriterPid} = ra_log_segment_writer:start_link(#{system => default,
                                                            name => ?SEGWR,
                                                            data_dir => Dir}),
    % first batch
    Entries = [{1, 42, a},
               {2, 42, b},
               {3, 43, c},
               {4, 43, d},
               {5, 43, e}
              ],
    Mt = make_mem_table(UId, Entries),
    Ranges = #{UId => [{ra_mt:tid(Mt), [ra_mt:range(Mt)]}]},
    %% update snapshot state table
    ra_log_snapshot_state:insert(ra_log_snapshot_state, UId, 3, 4, []),
    ok = ra_log_segment_writer:accept_mem_tables(?SEGWR, Ranges,
                                                 make_wal(Config, "w1.wal")),
    receive
        {ra_log_event, {segments, _Tid, [{Fn, {4, 5}}]}} ->
            SegmentFile = filename:join(?config(server_dir, Config), Fn),
            {ok, Seg} = ra_log_segment:open(SegmentFile, #{mode => read}),
            % assert only entries with a higher index than the snapshot
            % have been written
            ok = gen_server:stop(TblWriterPid),
            ?assertExit({missing_key, 1}, read_sparse(Seg, [1,2, 3])),
            [{4, _, _}, {5, _, _}] = read_sparse(Seg, [4, 5])
    after 3000 ->
              flush(),
              ok = gen_server:stop(TblWriterPid),
              throw(ra_log_event_timeout)
    end,
    ok.

skip_all_entries_lower_than_snapshot_index(Config) ->
    Dir = ?config(wal_dir, Config),
    UId = ?config(uid, Config),
    {ok, TblWriterPid} = ra_log_segment_writer:start_link(#{system => default,
                                                            name => ?SEGWR,
                                                            data_dir => Dir}),
    % first batch
    Entries = [{1, 43, c},
               {2, 43, d},
               {3, 43, e}
              ],
    Mt = make_mem_table(UId, Entries),
    Ranges = #{UId => [{ra_mt:tid(Mt), [ra_mt:range(Mt)]}]},
    %% update snapshot state table
    ra_log_snapshot_state:insert(ra_log_snapshot_state, UId, 3, 4, []),
    ok = ra_log_segment_writer:accept_mem_tables(?SEGWR, Ranges,
                                                 make_wal(Config, "w1.wal")),
    TIDRANGES = maps:get(UId, Ranges),
    receive
        {ra_log_event, {segments, TIDRANGES, []}} ->
            %% no segments were generated for this mem table
            ok
    after 3000 ->
              flush(),
              ok = gen_server:stop(TblWriterPid),
              throw(ra_log_event_timeout)
    end,
    ok = gen_server:stop(TblWriterPid),
    ok.

live_indexes_1(Config) ->
    Dir = ?config(wal_dir, Config),
    UId = ?config(uid, Config),
    {ok, TblWriterPid} = ra_log_segment_writer:start_link(#{system => default,
                                                            name => ?SEGWR,
                                                            data_dir => Dir}),
    % first batch
    Entries = [{1, 42, a},
               {2, 42, b},
               {3, 43, c},
               {4, 43, d},
               {5, 43, e},
               {6, 43, f}
              ],
    Mt = make_mem_table(UId, Entries),
    Ranges = #{UId => [{ra_mt:tid(Mt), [ra_mt:range(Mt)]}]},
    %% update snapshot state table
    ra_log_snapshot_state:insert(ra_log_snapshot_state, UId, 4, 2, [4, 2]),
    ok = ra_log_segment_writer:accept_mem_tables(?SEGWR, Ranges,
                                                 make_wal(Config, "w1.wal")),
    receive
        {ra_log_event, {segments, _Tid, [{Fn, {2, 6}}]}} ->
            SegmentFile = filename:join(?config(server_dir, Config), Fn),
            {ok, Seg} = ra_log_segment:open(SegmentFile, #{mode => read}),
            % assert only entries with a higher index than the snapshot
            % have been written
            ok = gen_server:stop(TblWriterPid),
            ?assertExit({missing_key, 3}, read_sparse(Seg, [2, 3, 4])),
            [
             {2, _, _},
             {4, _, _},
             {5, _, _},
             {6, _, _}
            ] = read_sparse(Seg, [2, 4, 5, 6])
    after 3000 ->
              flush(),
              ok = gen_server:stop(TblWriterPid),
              throw(ra_log_event_timeout)
    end,
    ok.

live_indexes_2(Config) ->
    Dir = ?config(wal_dir, Config),
    UId = ?config(uid, Config),
    {ok, TblWriterPid} = ra_log_segment_writer:start_link(#{system => default,
                                                            name => ?SEGWR,
                                                            data_dir => Dir}),
    Mt1 = make_mem_table(UId, [{5, 1, a}]),
    Mt2 = make_mem_table(UId, [
                               {7, 1, b},
                               {8, 1, b},
                               {9, 1, b}
                              ]),
    Ranges = #{UId => [{ra_mt:tid(Mt1), [ra_mt:range(Mt1)]},
                       {ra_mt:tid(Mt2), [ra_mt:range(Mt2)]}]},
    ra_log_snapshot_state:insert(ra_log_snapshot_state, UId, 40, 1, [{7, 9}, 1]),
    ok = ra_log_segment_writer:accept_mem_tables(?SEGWR, Ranges,
                                                 make_wal(Config, "w1.wal")),
    receive
        {ra_log_event, {segments, _Tid, [{_, {7, 9}}]}} ->
            ok
    after 3000 ->
              flush(),
              ok = gen_server:stop(TblWriterPid),
              throw(ra_log_event_timeout)
    end,
    ok = gen_server:stop(TblWriterPid),
    ok.

%%% Internal

fake_mem_table(UId, Dir, Entries) ->
    Tid = make_mem_table(UId, Entries),
    {FirstIdx, _, _} = hd(Entries),
    {LastIdx, _, _} = lists:last(Entries),
    MemTables = [{UId, FirstIdx, LastIdx, Tid}],
    {MemTables, filename:join(Dir, "blah.wal")}.

make_mem_table(UId, Entries) ->
    N = ra_directory:name_of(default, UId),
    Tid = ets:new(N, [set, public]),
    make_mem_table(UId, Tid, Entries).

make_mem_table(_UId, Tid, Entries) ->
    Mt = ra_mt:init(Tid),
    lists:foldl(fun(E, Acc0) ->
                        {ok, Acc} = ra_mt:insert(E, Acc0),
                        Acc
                end, Mt, Entries).

flush() ->
    receive Msg ->
                ct:pal("flush: ~p", [Msg]),
                flush()
    after 0 -> ok
    end.

segments_for(UId, DataDir) ->
    Dir = filename:join(DataDir, ra_lib:to_list(UId)),
    SegFiles = lists:sort(filelib:wildcard(filename:join(Dir, "*.segment"))),
    SegFiles.

read_sparse(R, Idxs) ->
    {ok, _, Entries} = ra_log_segment:read_sparse(R, Idxs,
                                                  fun(I, T, B, Acc) ->
                                                          [{I, T, B} | Acc]
                                                  end, []),
    lists:reverse(Entries).

get_names(System) when is_atom(System) ->
    #{names := Names} = ra_system:fetch(System),
    Names.

make_wal(Config, Name) ->
    Dir = ?config(wal_dir, Config),
    FullWalFile = filename:join(Dir, Name),
    ok = file:write_file(FullWalFile, <<"waldata">>),
    FullWalFile.

is_wal_file(Config, Name) ->
    Dir = ?config(wal_dir, Config),
    WalFile = filename:join(Dir, Name),
    filelib:is_file(WalFile).
