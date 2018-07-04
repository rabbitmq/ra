-module(ra_log_segment_writer_SUITE).
-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

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
     delete_segments,
     my_segments,
     skip_entries_lower_than_snapshot_index,
     skip_all_entries_lower_than_snapshot_index
    ].

groups() ->
    [
     {tests, [], all_tests()}
    ].

init_per_group(tests, Config) ->
    Config.

end_per_group(tests, Config) ->
    Config.

init_per_testcase(TestCase, Config) ->
    PrivDir = ?config(priv_dir, Config),
    Dir = filename:join(PrivDir, TestCase),
    ra_directory:init(PrivDir),
    UId = atom_to_binary(TestCase, utf8),
    yes = ra_directory:register_name(UId, self(), TestCase),
    file:make_dir(Dir),
    NodeDir = filename:join(Dir, UId),
    file:make_dir(NodeDir),
    register(TestCase, self()),
    _ = ets:new(ra_open_file_metrics, [named_table, public,
                                       {write_concurrency, true}]),
    _ = ets:new(ra_io_metrics, [named_table, public,
                                {write_concurrency, true}]),
    ets:new(ra_log_snapshot_state, [named_table, public]),
    ra_file_handle:start_link(),
    [{uid, UId},
     {node_dir, NodeDir},
     {test_case, TestCase},
     {wal_dir, Dir} | Config].

end_per_testcase(_, Config) ->
    ok = gen_server:stop(ra_file_handle),
    Config.

accept_mem_tables(Config) ->
    Dir = ?config(wal_dir, Config),
    UId = ?config(uid, Config),
    {ok, TblWriterPid} = ra_log_segment_writer:start_link(#{data_dir => Dir}),
    % fake up a mem segment for Self
    Entries = [{1, 42, a}, {2, 42, b}, {3, 43, c}],
    Tid = make_mem_table(UId, Entries),
    MemTables = [{UId, 1, 3, Tid}],
    WalFile = filename:join(Dir, "00001.wal"),
    ok = file:write_file(WalFile, <<"waldata">>),
    ok = ra_log_segment_writer:accept_mem_tables(MemTables, WalFile),
    receive
        {ra_log_event, {segments, Tid, [{1, 3, Fn}]}} ->
            SegmentFile = filename:join(?config(node_dir, Config), Fn),
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

delete_segments(Config) ->
    Dir = ?config(wal_dir, Config),
    {ok, TblWriterPid} = ra_log_segment_writer:start_link(#{data_dir => Dir}),
    UId = ?config(uid, Config),
    % fake up a mem segment for Self
    Entries = [{1, 42, a}, {2, 42, b}, {3, 43, c}],
    Tid = make_mem_table(UId, Entries),
    MemTables = [{UId, 1, 3, Tid}],
    WalFile = filename:join(Dir, "00001.wal"),
    ok = file:write_file(WalFile, <<"waldata">>),
    ok = ra_log_segment_writer:accept_mem_tables(MemTables, WalFile),
    receive
        {ra_log_event, {segments, Tid, [{1, 3, Fn}]}} ->
            SegmentFile = filename:join(?config(node_dir, Config), Fn),
            % test a lower index _does not_ delete the file
            ok = ra_log_segment_writer:delete_segments(TblWriterPid, UId, 2,
                                                       [SegmentFile]),
            timer:sleep(500),
            ?assert(filelib:is_file(SegmentFile)),
            % test a fully inclusive snapshot index _does_ delete the current
            % segment file
            ok = ra_log_segment_writer:delete_segments(TblWriterPid, UId, 3,
                                                       [SegmentFile]),
            timer:sleep(1000),
            % validate file is gone
            ?assert(false =:= filelib:is_file(SegmentFile)),
            ok
    after 3000 ->
              throw(ra_log_event_timeout)
    end,
    ok = gen_server:stop(TblWriterPid),
    ok.

my_segments(Config) ->
    Dir = ?config(wal_dir, Config),
    {ok, TblWriterPid} = ra_log_segment_writer:start_link(#{data_dir => Dir}),
    UId = ?config(uid, Config),
    % fake up a mem segment for Self
    Entries = [{1, 42, a}, {2, 42, b}, {3, 43, c}],
    Tid = make_mem_table(UId, Entries),
    MemTables = [{UId, 1, 3, Tid}],
    WalFile = filename:join(Dir, "00001.wal"),
    ok = file:write_file(WalFile, <<"waldata">>),
    ok = ra_log_segment_writer:accept_mem_tables(MemTables, WalFile),
    receive
        {ra_log_event, {segments, Tid, [{1, 3, Fn}]}} ->
            SegmentFile = filename:join(?config(node_dir, Config), Fn),
            [MyFile] = ra_log_segment_writer:my_segments(UId),
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
    {ok, TblWriterPid} = ra_log_segment_writer:start_link(#{data_dir => Dir}),
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
    ok = ra_log_segment_writer:accept_mem_tables(MemTables, WalFile),
    receive
        {ra_log_event, {segments, _Tid, [{4, 5, Fn}]}} ->
            SegmentFile = filename:join(?config(node_dir, Config), Fn),
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
    {ok, TblWriterPid} = ra_log_segment_writer:start_link(#{data_dir => Dir}),
    % first batch
    Entries = [{1, 43, c},
               {2, 43, d},
               {3, 43, e}
              ],
    {MemTables, WalFile} = fake_mem_table(UId, Dir, Entries),
    %% update snapshot state table
    ets:insert(ra_log_snapshot_state, {UId, 3}),
    ok = ra_log_segment_writer:accept_mem_tables(MemTables, WalFile),
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
    {ok, TblWriterPid} = ra_log_segment_writer:start_link(#{data_dir => Dir}),
    % first batch
    Entries = [{1, 42, a}, {2, 42, b}, {3, 43, c}],
    {MemTables, WalFile} = fake_mem_table(UId, Dir, Entries),
    ok = ra_log_segment_writer:accept_mem_tables(MemTables, WalFile),
    % second batch
    Entries2 = [{4, 43, d}, {5, 43, e}],
    {MemTables2, WalFile2} = fake_mem_table(UId, Dir, Entries2),
    ok = ra_log_segment_writer:accept_mem_tables(MemTables2, WalFile2),
    AllEntries = Entries ++ Entries2,
    receive
        {ra_log_event, {segments, _Tid, [{1, 5, Fn}]}} ->
            SegmentFile = filename:join(?config(node_dir, Config), Fn),
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
    {ok, TblWriterPid} = ra_log_segment_writer:start_link(#{data_dir => Dir}),
    UId = ?config(uid, Config),
    % first batch
    Entries = [{3, 42, c}, {4, 42, d}, {5, 42, e}],
    {MemTables, WalFile} = fake_mem_table(UId, Dir, Entries),
    ok = ra_log_segment_writer:accept_mem_tables(MemTables, WalFile),
    % second batch overwrites the first
    Entries2 = [{1, 43, a}, {2, 43, b}, {3, 43, c2}],
    {MemTables2, WalFile2} = fake_mem_table(UId, Dir, Entries2),
    ok = ra_log_segment_writer:accept_mem_tables(MemTables2, WalFile2),

    receive
        {ra_log_event, {segments, _Tid, [{1, 3, Fn}]}} ->
            SegmentFile = filename:join(?config(node_dir, Config), Fn),
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
             segment_conf => #{max_count => 8}},
    {ok, Pid} = ra_log_segment_writer:start_link(Conf),
    % more entries than fit a single segment
    Entries = [{I, 2, x} || I <- lists:seq(1, 10)],
    {MemTables, WalFile} = fake_mem_table(UId, Dir, Entries),
    ok = ra_log_segment_writer:accept_mem_tables(MemTables, WalFile),
    receive
        {ra_log_event, {segments, _Tid, [{9, 10, _Seg2}, {1, 8, _Seg1}]}} ->
            ok
    after 3000 ->
              throw(ra_log_event_timeout)
    end,
    % receive then receive again to breach segment size limit
    ok = gen_server:stop(Pid),
    ok.

accept_mem_tables_for_down_node(Config) ->
    Dir = ?config(wal_dir, Config),
    UId = ?config(uid, Config),
    application:start(sasl),
    {ok, TblWriterPid} = ra_log_segment_writer:start_link(#{data_dir => Dir}),
    % fake up a mem segment for Self
    Entries = [{1, 42, a}, {2, 42, b}, {3, 43, c}],
    Tid = make_mem_table(<<"not_self">>, Entries),
    Tid2 = make_mem_table(UId, Entries),
    MemTables = [{<<"not_self">>, 1, 3, Tid},
                 {UId, 1, 3, Tid2}],
    % delete the ETS table to simulate down node
    ets:delete(Tid),
    WalFile = filename:join(Dir, "00001.wal"),
    ok = file:write_file(WalFile, <<"waldata">>),
    ok = ra_log_segment_writer:accept_mem_tables(MemTables, WalFile),
    receive
        {ra_log_event, {segments, Tid2, [{1, 3, Fn}]}} ->
            SegmentFile = filename:join(?config(node_dir, Config), Fn),
            {ok, Seg} = ra_log_segment:open(SegmentFile, #{mode => read}),
            % assert Entries have been fully transferred
            Entries = [{I, T, binary_to_term(B)}
                       || {I, T, B} <- ra_log_segment:read(Seg, 1, 3)]
    after 3000 ->
              throw(ra_log_event_timeout)
    end,

    % assert wal file has been deleted.
    false = filelib:is_file(WalFile),
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
    N = ra_directory:what_node(UId),
    Tid = ets:new(N, []),
    [ets:insert(Tid, E) || E <- Entries],
    Tid.
