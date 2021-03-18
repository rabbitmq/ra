%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2017-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%
-module(ra_log_segment_writer_SUITE).
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
     accept_mem_tables_rollover,
     accept_mem_tables_for_down_server,
     accept_mem_tables_with_corrupt_segment,
     accept_mem_tables_with_delete_server,
     truncate_segments,
     truncate_segments_with_pending_update,
     truncate_segments_with_pending_overwrite,
     my_segments,
     skip_entries_lower_than_snapshot_index,
     skip_all_entries_lower_than_snapshot_index
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
    ra_system:store(ra_system:default_config()),
    ra_directory:init(default),
    ra_counters:init(),
    UId = atom_to_binary(TestCase, utf8),
    ok = ra_directory:register_name(default, UId, self(), undefined,
                                    TestCase, TestCase),
    ok = ra_lib:make_dir(Dir),
    ServerDir = filename:join(Dir, UId),
    ok = ra_lib:make_dir(ServerDir),
    register(TestCase, self()),
    _ = ets:new(ra_open_file_metrics, [named_table, public,
                                       {write_concurrency, true}]),
    _ = ets:new(ra_io_metrics, [named_table, public,
                                {write_concurrency, true}]),
    ets:new(ra_log_snapshot_state, [named_table, public]),
    ra_file_handle:start_link(),
    [{uid, UId},
     {server_dir, ServerDir},
     {test_case, TestCase},
     {wal_dir, Dir} | Config].

end_per_testcase(_, Config) ->
    ok = gen_server:stop(ra_file_handle),
    Config.

accept_mem_tables(Config) ->
    Dir = ?config(wal_dir, Config),
    UId = ?config(uid, Config),
    {ok, TblWriterPid} = ra_log_segment_writer:start_link(#{name => ?SEGWR,
                                                            system => default,
                                                            data_dir => Dir}),
    % fake up a mem segment for Self
    Entries = [{1, 42, a}, {2, 42, b}, {3, 43, c}],
    Tid = make_mem_table(UId, Entries),
    MemTables = [{UId, 1, 3, Tid}],
    WalFile = filename:join(Dir, "00001.wal"),
    ok = file:write_file(WalFile, <<"waldata">>),
    ok = ra_log_segment_writer:accept_mem_tables(?SEGWR, MemTables, WalFile),
    receive
        {ra_log_event, {segments, Tid, [{1, 3, Fn}]}} ->
            SegmentFile = filename:join(?config(server_dir, Config), Fn),
            {ok, Seg} = ra_log_segment:open(SegmentFile, #{mode => read}),
            % assert Entries have been fully transferred
            Entries = [{I, T, binary_to_term(B)}
                       || {I, T, B} <- ra_log_segment:read(Seg, 1, 3)]
    after 3000 ->
              throw(ra_log_event_timeout)
    end,

    timer:sleep(250),

    % assert wal file has been deleted.
    false = filelib:is_file(WalFile),
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
    {MemTables, WalFile} = fake_mem_table(UId, Dir, Entries),
    ok = ra_log_segment_writer:accept_mem_tables(?SEGWR, MemTables, WalFile),
    receive
        {ra_log_event, {segments, _Tid, [{25, 32, S} = Cur | Rem]}} ->
            % test a lower index _does not_ delete the file
            SegmentFile = filename:join(?config(server_dir, Config), S),
            ?assert(filelib:is_file(SegmentFile)),
            ok = ra_log_segment_writer:truncate_segments(TblWriterPid,
                                                         UId, Cur),
            validate_segment_deleted(100, SegmentFile),
            % test a fully inclusive snapshot index _does_ delete the current
            % segment file
            [{_, _, S1}, {_, _, S2}] = Rem,
            SegmentFile1 = filename:join(?config(server_dir, Config), S1),
            validate_segment_deleted(100, SegmentFile1),
            SegmentFile2 = filename:join(?config(server_dir, Config), S2),
            validate_segment_deleted(10, SegmentFile2),
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
    {ok, TblWriterPid} = ra_log_segment_writer:start_link(
                           #{system => default,
                             name => ?SEGWR,
                             data_dir => Dir,
                             segment_conf => SegConf}),
    UId = ?config(uid, Config),
    % fake up a mem segment for Self
    Entries = [{N, 42, N} || N <- lists:seq(1, 32)],
    {MemTables, WalFile} = fake_mem_table(UId, Dir, Entries),
    ok = ra_log_segment_writer:accept_mem_tables(?SEGWR, MemTables, WalFile),
    %% write one more entry separately
    Entries2 = [{N, 42, N} || N <- lists:seq(33, 40)],
    {MemTables2, _} = fake_mem_table(UId, Dir, Entries2),
    ok = ra_log_segment_writer:accept_mem_tables(?SEGWR, MemTables2, WalFile),
    receive
        {ra_log_event, {segments, _Tid, [{25, 32, S} = Cur | Rem]}} ->
            % test a lower index _does not_ delete the file
            SegmentFile = filename:join(?config(server_dir, Config), S),
            ?assert(filelib:is_file(SegmentFile)),
            ok = ra_log_segment_writer:truncate_segments(TblWriterPid,
                                                         UId, Cur),
            timer:sleep(1000),
            SegmentFile = filename:join(?config(server_dir, Config), S),
            % test a fully inclusive snapshot index _does_ delete the current
            % segment file
            [{_, _, S1}, {_, _, S2}] = Rem,
            SegmentFile1 = filename:join(?config(server_dir, Config), S1),
            validate_segment_deleted(100, SegmentFile1),
            SegmentFile2 = filename:join(?config(server_dir, Config), S2),
            validate_segment_deleted(10, SegmentFile2),
            ok
    after 3000 ->
              throw(ra_log_event_timeout)
    end,
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
    {MemTables, WalFile} = fake_mem_table(UId, Dir, Entries),
    ok = ra_log_segment_writer:accept_mem_tables(?SEGWR, MemTables, WalFile),
    %% write one more entry separately
    Entries2 = [{N, 43, N} || N <- lists:seq(12, 25)],
    {MemTables2, WalFile2} = fake_mem_table(UId, Dir, Entries2),
    ok = ra_log_segment_writer:accept_mem_tables(?SEGWR, MemTables2, WalFile2),
    receive
        {ra_log_event, {segments, _Tid, [{25, 32, S} = Cur | Rem]}} ->
            % test a lower index _does not_ delete the file
            SegmentFile = filename:join(?config(server_dir, Config), S),
            ?assert(filelib:is_file(SegmentFile)),
            ok = ra_log_segment_writer:truncate_segments(TblWriterPid,
                                                         UId, Cur),
            _ = ra_log_segment_writer:my_segments(?SEGWR, UId),
            SegmentFile = filename:join(?config(server_dir, Config), S),
            ?assert(filelib:is_file(SegmentFile)),
            % test a fully inclusive snapshot index _does_ delete the current
            % segment file
            [{_, _, S1}, {_, _, S2}] = Rem,
            SegmentFile1 = filename:join(?config(server_dir, Config), S1),
            validate_segment_deleted(100, SegmentFile1),
            SegmentFile2 = filename:join(?config(server_dir, Config), S2),
            validate_segment_deleted(10, SegmentFile2),
            ok
    after 3000 ->
              throw(ra_log_event_timeout)
    end,
    receive
        {ra_log_event, {segments, _, [{16, 25, F} = Cur2, {12, 15, F2}]}} ->
            ok = ra_log_segment_writer:truncate_segments(TblWriterPid,
                                                         UId, Cur2),
            SegFile = filename:join(?config(server_dir, Config), F),
            validate_segment_deleted(100, SegFile),
            SegFile2 = filename:join(?config(server_dir, Config), F2),
            validate_segment_deleted(10, SegFile2),
            %% validate there is a new empty segment
            ?assertMatch([_], segments_for(UId, Dir)),
            ok
    after 3000 ->
              flush(),
              throw(ra_log_event_timeout2)
    end,
    ok = gen_server:stop(TblWriterPid),
    ok.

validate_segment_deleted(0, _) ->
    exit(file_was_not_deleted);
validate_segment_deleted(N, SegFile) ->
    timer:sleep(20),
    case filelib:is_file(SegFile) of
        true ->
            validate_segment_deleted(N-1, SegFile),
            ok;
        false ->
            ok
    end.


my_segments(Config) ->
    Dir = ?config(wal_dir, Config),
    {ok, TblWriterPid} = ra_log_segment_writer:start_link(#{name => ?SEGWR,
                                                            system => default,
                                                            data_dir => Dir}),
    UId = ?config(uid, Config),
    % fake up a mem segment for Self
    Entries = [{1, 42, a}, {2, 42, b}, {3, 43, c}],
    Tid = make_mem_table(UId, Entries),
    MemTables = [{UId, 1, 3, Tid}],
    WalFile = filename:join(Dir, "00001.wal"),
    ok = file:write_file(WalFile, <<"waldata">>),
    ok = ra_log_segment_writer:accept_mem_tables(?SEGWR,MemTables, WalFile),
    receive
        {ra_log_event, {segments, Tid, [{1, 3, Fn}]}} ->
            SegmentFile = filename:join(?config(server_dir, Config), Fn),
            [MyFile] = ra_log_segment_writer:my_segments(?SEGWR,UId),
            ?assertEqual(SegmentFile, list_to_binary(MyFile)),
            ?assert(filelib:is_file(SegmentFile))
    after 2000 ->
              exit(ra_log_event_timeout)
    end,
    proc_lib:stop(TblWriterPid),
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
    {MemTables, WalFile} = fake_mem_table(UId, Dir, Entries),
    %% update snapshot state table
    ets:insert(ra_log_snapshot_state, {UId, 3}),
    ok = ra_log_segment_writer:accept_mem_tables(?SEGWR,MemTables, WalFile),
    receive
        {ra_log_event, {segments, _Tid, [{4, 5, Fn}]}} ->
            SegmentFile = filename:join(?config(server_dir, Config), Fn),
            {ok, Seg} = ra_log_segment:open(SegmentFile, #{mode => read}),
            % assert only entries with a higher index than the snapshot
            % have been written
            ok = gen_server:stop(TblWriterPid),
            [{4, _, _}, {5, _, _}] = ra_log_segment:read(Seg, 1, 5)
    after 3000 ->
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
    {MemTables, WalFile} = fake_mem_table(UId, Dir, Entries),
    %% update snapshot state table
    ets:insert(ra_log_snapshot_state, {UId, 3}),
    ok = ra_log_segment_writer:accept_mem_tables(?SEGWR,MemTables, WalFile),
    receive
        {ra_log_event, {segments, _Tid, []}} ->
            %% no segments were generated for this mem table
            ok
    after 3000 ->
              ok = gen_server:stop(TblWriterPid),
              throw(ra_log_event_timeout)
    end,
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
    {MemTables, WalFile} = fake_mem_table(UId, Dir, Entries),
    ok = ra_log_segment_writer:accept_mem_tables(?SEGWR, MemTables, WalFile),
    % second batch
    Entries2 = [{4, 43, d}, {5, 43, e}],
    {MemTables2, WalFile2} = fake_mem_table(UId, Dir, Entries2),
    ok = ra_log_segment_writer:accept_mem_tables(?SEGWR, MemTables2, WalFile2),
    AllEntries = Entries ++ Entries2,
    receive
        {ra_log_event, {segments, _Tid, [{1, 5, Fn}]}} ->
            SegmentFile = filename:join(?config(server_dir, Config), Fn),
            {ok, Seg} = ra_log_segment:open(SegmentFile, #{mode => read}),
            % assert Entries have been fully transferred
            AllEntries = [{I, T, binary_to_term(B)}
                          || {I, T, B} <- ra_log_segment:read(Seg, 1, 5)]
    after 3000 ->
              throw(ra_log_event_timeout)
    end,
    ok = gen_server:stop(TblWriterPid),
    ok.

accept_mem_tables_overwrite(Config) ->
    Dir = ?config(wal_dir, Config),
    {ok, TblWriterPid} = ra_log_segment_writer:start_link(#{system => default,
                                                            name => ?SEGWR,
                                                            data_dir => Dir}),
    UId = ?config(uid, Config),
    % first batch
    Entries = [{3, 42, c}, {4, 42, d}, {5, 42, e}],
    {MemTables, WalFile} = fake_mem_table(UId, Dir, Entries),
    ok = ra_log_segment_writer:accept_mem_tables(?SEGWR, MemTables, WalFile),
    % second batch overwrites the first
    Entries2 = [{1, 43, a}, {2, 43, b}, {3, 43, c2}],
    {MemTables2, WalFile2} = fake_mem_table(UId, Dir, Entries2),
    ok = ra_log_segment_writer:accept_mem_tables(?SEGWR, MemTables2, WalFile2),

    receive
        {ra_log_event, {segments, _Tid, [{1, 3, Fn}]}} ->
            SegmentFile = filename:join(?config(server_dir, Config), Fn),
            {ok, Seg} = ra_log_segment:open(SegmentFile, #{mode => read}),
            C2 = term_to_binary(c2),
            [{1, 43, _}, {2, 43, _}] = ra_log_segment:read(Seg, 1, 2),
            [{3, 43, C2}] = ra_log_segment:read(Seg, 3, 1),
            [] = ra_log_segment:read(Seg, 4, 2)
    after 3000 ->
              throw(ra_log_event_timeout)
    end,
    ok = gen_server:stop(TblWriterPid),
    ok.

accept_mem_tables_rollover(Config) ->
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
    {MemTables, WalFile} = fake_mem_table(UId, Dir, Entries),
    ok = ra_log_segment_writer:accept_mem_tables(?SEGWR, MemTables, WalFile),
    receive
        {ra_log_event, {segments, _Tid, [{9, 10, _Seg2}, {1, 8, _Seg1}]}} ->
            ok
    after 3000 ->
              throw(ra_log_event_timeout)
    end,
    % receive then receive again to breach segment size limit
    ok = gen_server:stop(Pid),
    ok.

accept_mem_tables_for_down_server(Config) ->
    %% fake a closed mem table
    ets:new(ra_log_closed_mem_tables, [named_table, bag, public]),
    Dir = ?config(wal_dir, Config),
    UId = ?config(uid, Config),
    FakeUId = <<"not_self">>,
    ok = ra_lib:make_dir(filename:join(Dir, FakeUId)),
    application:start(sasl),
    {ok, TblWriterPid} = ra_log_segment_writer:start_link(#{system => default,
                                                            name => ?SEGWR,
                                                            data_dir => Dir}),
    % fake up a mem segment for Self
    Entries = [{1, 42, a}, {2, 42, b}, {3, 43, c}],
    Tid = make_mem_table(FakeUId, Entries),
    Tid2 = make_mem_table(UId, Entries),
    MemTables = [{FakeUId, 1, 3, Tid},
                 {UId, 1, 3, Tid2}],
    ets:insert(ra_log_closed_mem_tables, {FakeUId, 1, 1, 3, Tid}),
    WalFile = filename:join(Dir, "00001.wal"),
    ok = file:write_file(WalFile, <<"waldata">>),
    ok = ra_log_segment_writer:accept_mem_tables(?SEGWR, MemTables, WalFile),
    receive
        {ra_log_event, {segments, Tid2, [{1, 3, Fn}]}} ->
            SegmentFile = filename:join(?config(server_dir, Config), Fn),
            {ok, Seg} = ra_log_segment:open(SegmentFile, #{mode => read}),
            % assert Entries have been fully transferred
            Entries = [{I, T, binary_to_term(B)}
                       || {I, T, B} <- ra_log_segment:read(Seg, 1, 3)]
    after 3000 ->
              throw(ra_log_event_timeout)
    end,
    %% as segments are written in parallel we need await the segment writer
    %% before asserting
    ra_log_segment_writer:await(?SEGWR),

    %% if the server is down at the time the segment writer send the segments
    %% the segment writer should clear up the ETS mem tables
    %% This is safe as the server synchronises through the segment writer
    %% on start.
    %% check the fake tid is removed
    ?assertEqual(undefined, ets:info(Tid)),
    [] = ets:tab2list(ra_log_closed_mem_tables),

    % assert wal file has been deleted.
    % the delete happens after the segment notification so we need to retry
    ok = ra_lib:retry(fun() ->
                              false = filelib:is_file(WalFile),
                              ok
                      end, 5, 100),
    ok = gen_server:stop(TblWriterPid),
    ok.

accept_mem_tables_with_corrupt_segment(Config) ->
    %% fake a closed mem table
    ets:new(ra_log_closed_mem_tables, [named_table, bag, public]),
    Dir = ?config(wal_dir, Config),
    UId = ?config(uid, Config),
    % FakeUId = <<"not_self">>,
    % ok = ra_lib:make_dir(filename:join(Dir, FakeUId)),
    application:start(sasl),
    {ok, TblWriterPid} = ra_log_segment_writer:start_link(#{system => default,
                                                            name => ?SEGWR,
                                                            data_dir => Dir}),
    % fake up a mem segment for Self
    Entries = [{1, 42, a}, {2, 42, b}, {3, 43, c}],
    % Tid = make_mem_table(FakeUId, Entries),
    Tid2 = make_mem_table(UId, Entries),
    MemTables = [{UId, 1, 3, Tid2}],
    % ets:insert(ra_log_closed_mem_tables, {FakeUId, 1, 1, 3, Tid}),
    WalFile = filename:join(Dir, "00001.wal"),
    ok = file:write_file(WalFile, <<"waldata">>),
    %% write an empty file to simulate corrupt segment
    %% this can happen if a segment is opened but is interrupted before syncing
    %% the header
    file:write_file(filename:join(?config(server_dir, Config), "0000001.segment"), <<>>),
    ok = ra_log_segment_writer:accept_mem_tables(?SEGWR, MemTables, WalFile),
    receive
        {ra_log_event, {segments, Tid2, [{1, 3, Fn}]}} ->
            SegmentFile = filename:join(?config(server_dir, Config), Fn),
            {ok, Seg} = ra_log_segment:open(SegmentFile, #{mode => read}),
            % assert Entries have been fully transferred
            Entries = [{I, T, binary_to_term(B)}
                       || {I, T, B} <- ra_log_segment:read(Seg, 1, 3)]
    after 3000 ->
              flush(),
              throw(ra_log_event_timeout)
    end,

    %% if the server is down at the time the segment writer send the segments
    %% the segment writer should clear up the ETS mem tables
    %% This is safe as the server synchronises through the segment writer
    %% on start.
    %% check the fake tid is removed
    % undefined = ets:info(Tid),
    [] = ets:tab2list(ra_log_closed_mem_tables),

    % assert wal file has been deleted.
    % the delete happens after the segment notification so we need to retry
    ok = ra_lib:retry(fun() ->
                              false = filelib:is_file(WalFile),
                              ok
                      end, 5, 100),
    ok = gen_server:stop(TblWriterPid),
    ok.

accept_mem_tables_with_delete_server(Config) ->
    ets:new(ra_log_closed_mem_tables, [named_table, bag, public]),
    Dir = ?config(wal_dir, Config),
    UId = ?config(uid, Config),
    application:start(sasl),
    {ok, TblWriterPid} = ra_log_segment_writer:start_link(#{system => default,
                                                            name => ?SEGWR,
                                                            data_dir => Dir}),
    % fake up a mem segment for Self
    Tid = make_mem_table(UId, [{1, 42, a}, {2, 42, b}, {3, 42, c}]),
    Tid2 = make_mem_table(UId, [{4, 42, d}]),
    MemTables = [{UId, 1, 3, Tid}],
    MemTables2 = [{UId, 4, 4, Tid2}],
    % ets:insert(ra_log_closed_mem_tables, {FakeUId, 1, 1, 3, Tid}),
    WalFile = filename:join(Dir, "00001.wal"),
    ok = file:write_file(WalFile, <<"waldata">>),
    ok = ra_log_segment_writer:accept_mem_tables(?SEGWR, MemTables, WalFile),
    receive
        {ra_log_event, {segments, Tid, [{1, 3, _Fn}]}} ->
            ok
    after 3000 ->
              flush(),
              throw(ra_log_event_timeout)
    end,

    %% delete server directory to simulate server deletion
    ServerDir = ?config(server_dir, Config),
    ra_lib:recursive_delete(ServerDir),
    ?assert(filelib:is_dir(ServerDir) == false),
    ok = ra_log_segment_writer:accept_mem_tables(?SEGWR, MemTables2, WalFile),
    receive
        {ra_log_event, {segments, Tid2, _}} ->
            exit(unexpected_log_event)
    after 1000 ->
              ok
    end,
    %% if the server is down at the time the segment writer send the segments
    %% the segment writer should clear up the ETS mem tables
    %% This is safe as the server synchronises through the segment writer
    %% on start.
    %% check the fake tid is removed
    % undefined = ets:info(Tid),
    [] = ets:tab2list(ra_log_closed_mem_tables),

    % assert wal file has been deleted.
    % the delete happens after the segment notification so we need to retry
    ok = ra_lib:retry(fun() ->
                              false = filelib:is_file(WalFile),
                              ok
                      end, 5, 100),
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
    Tid = ets:new(N, [public]),
    [ets:insert(Tid, E) || E <- Entries],
    Tid.

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
