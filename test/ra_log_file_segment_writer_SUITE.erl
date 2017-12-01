-module(ra_log_file_segment_writer_SUITE).
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
     % TODO: decide on error handling strategy for segments
     % accept_mem_tables_for_down_node,
     delete_segments
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
    file:make_dir(Dir),
    register(TestCase, self()),
    [{test_case, TestCase}, {wal_dir, Dir} | Config].

accept_mem_tables(Config) ->
    Dir = ?config(wal_dir, Config),
    {ok, TblWriterPid} = ra_log_file_segment_writer:start_link(#{data_dir => Dir}),
    {registered_name, Self} = erlang:process_info(self(), registered_name),
    % fake up a mem segment for Self
    Entries = [{1, 42, a}, {2, 42, b}, {3, 43, c}],
    Tid = make_mem_table(Self, Entries),
    MemTables = [{Self, 1, 3, Tid}],
    WalFile = filename:join(Dir, "00001.wal"),
    ok = file:write_file(WalFile, <<"waldata">>),
    ok = ra_log_file_segment_writer:accept_mem_tables(MemTables, WalFile),
    receive
        {ra_log_event, {segments, Tid, [{1, 3, SegmentFile}]}} ->
            {ok, Seg} = ra_log_file_segment:open(SegmentFile, #{mode => read}),
            % assert Entries have been fully transferred
            Entries = [{I, T, binary_to_term(B)}
                       || {I, T, B} <- ra_log_file_segment:read(Seg, 1, 3)]
    after 3000 ->
              throw(ra_log_event_timeout)
    end,

    % assert wal file has been deleted.
    false = filelib:is_file(WalFile),
    ok = gen_server:stop(TblWriterPid),
    ok.

delete_segments(Config) ->
    Dir = ?config(wal_dir, Config),
    {ok, TblWriterPid} = ra_log_file_segment_writer:start_link(#{data_dir => Dir}),
    {registered_name, Self} = erlang:process_info(self(), registered_name),
    % fake up a mem segment for Self
    Entries = [{1, 42, a}, {2, 42, b}, {3, 43, c}],
    Tid = make_mem_table(Self, Entries),
    MemTables = [{Self, 1, 3, Tid}],
    WalFile = filename:join(Dir, "00001.wal"),
    ok = file:write_file(WalFile, <<"waldata">>),
    ok = ra_log_file_segment_writer:accept_mem_tables(MemTables, WalFile),
    receive
        {ra_log_event, {segments, Tid, [{1, 3, SegmentFile}]}} ->
            % test a lower index _does not_ delete the file
            ok = ra_log_file_segment_writer:delete_segments(TblWriterPid,
                                                            Self, 2,
                                                            [SegmentFile]),
            timer:sleep(500),
            ?assert(filelib:is_file(SegmentFile)),
            % test a fully inclusive snapshot index _does_ delete the current
            % segment file
            ok = ra_log_file_segment_writer:delete_segments(TblWriterPid,
                                                            Self, 3,
                                                            [SegmentFile]),
            timer:sleep(500),
            % validate file is gone
            ?assert(false =:= filelib:is_file(SegmentFile)),
            ok
    after 3000 ->
              throw(ra_log_event_timeout)
    end,
    ok.

accept_mem_tables_append(Config) ->
    % append to a previously written segment
    Dir = ?config(wal_dir, Config),
    {ok, TblWriterPid} = ra_log_file_segment_writer:start_link(#{data_dir => Dir}),
    % first batch
    Entries = [{1, 42, a}, {2, 42, b}, {3, 43, c}],
    {MemTables, WalFile} = fake_mem_table(Dir, Entries),
    ok = ra_log_file_segment_writer:accept_mem_tables(MemTables, WalFile),
    % second batch
    Entries2 = [{4, 43, d}, {5, 43, e}],
    {MemTables2, WalFile2} = fake_mem_table(Dir, Entries2),
    ok = ra_log_file_segment_writer:accept_mem_tables(MemTables2, WalFile2),
    AllEntries = Entries ++ Entries2,
    receive
        {ra_log_event, {segments, _Tid, [{1, 5, SegmentFile}]}} ->
            {ok, Seg} = ra_log_file_segment:open(SegmentFile, #{mode => read}),
            % assert Entries have been fully transferred
            AllEntries = [{I, T, binary_to_term(B)}
                          || {I, T, B} <- ra_log_file_segment:read(Seg, 1, 5)]
    after 3000 ->
              throw(ra_log_event_timeout)
    end,
    ok = gen_server:stop(TblWriterPid),
    ok.

accept_mem_tables_overwrite(Config) ->
    Dir = ?config(wal_dir, Config),
    {ok, TblWriterPid} = ra_log_file_segment_writer:start_link(#{data_dir => Dir}),
    % first batch
    Entries = [{3, 42, c}, {4, 42, d}, {5, 42, e}],
    {MemTables, WalFile} = fake_mem_table(Dir, Entries),
    ok = ra_log_file_segment_writer:accept_mem_tables(MemTables, WalFile),
    % second batch overwrites the first
    Entries2 = [{1, 43, a}, {2, 43, b}, {3, 43, c2}],
    {MemTables2, WalFile2} = fake_mem_table(Dir, Entries2),
    ok = ra_log_file_segment_writer:accept_mem_tables(MemTables2, WalFile2),

    receive
        {ra_log_event, {segments, _Tid, [{1, 3, SegmentFile}]}} ->
            {ok, Seg} = ra_log_file_segment:open(SegmentFile, #{mode => read}),
            C2 = term_to_binary(c2),
            [{1, 43, _}, {2, 43, _}] = ra_log_file_segment:read(Seg, 1, 2),
            [{3, 43, C2}] = ra_log_file_segment:read(Seg, 3, 1),
            [] = ra_log_file_segment:read(Seg, 4, 2)
    after 3000 ->
              throw(ra_log_event_timeout)
    end,
    ok = gen_server:stop(TblWriterPid),
    ok.

accept_mem_tables_rollover(Config) ->
    Dir = ?config(wal_dir, Config),
    % configure max dets segment size
    Conf = #{data_dir => Dir,
             segment_conf => #{max_count => 8}},
    {ok, Pid} = ra_log_file_segment_writer:start_link(Conf),
    % more entries than fit a single segment
    Entries = [{I, 2, x} || I <- lists:seq(1, 10)],
    {MemTables, WalFile} = fake_mem_table(Dir, Entries),
    ok = ra_log_file_segment_writer:accept_mem_tables(MemTables, WalFile),
    receive
        {ra_log_event, {segments, _Tid, [{9, 10, _Seg2}, {1, 8, _Seg1}]}} ->
            ok
    after 3000 ->
              throw(ra_log_event_timeout)
    end,
    % receive then receive again to breach segment size limit
    % ensure dets segment is closed and writer id notified: {log_event, {segment_closed, Path}}
    ok = gen_server:stop(Pid),
    ok.

accept_mem_tables_for_down_node(Config) ->
    Dir = ?config(wal_dir, Config),
    application:start(sasl),
    {ok, TblWriterPid} = ra_log_file_segment_writer:start_link(#{data_dir => Dir}),
    {registered_name, Self} = erlang:process_info(self(), registered_name),
    % fake up a mem segment for Self
    Entries = [{1, 42, a}, {2, 42, b}, {3, 43, c}],
    Tid = make_mem_table(not_self, Entries),
    Tid2 = make_mem_table(Self, Entries),
    MemTables = [{not_self, 1, 3, Tid},
                 {Self, 1, 3, Tid2}],
    % delete the ETS table to simulate down node
    ets:delete(Tid),
    WalFile = filename:join(Dir, "00001.wal"),
    ok = file:write_file(WalFile, <<"waldata">>),
    ok = ra_log_file_segment_writer:accept_mem_tables(MemTables, WalFile),
    receive
        {ra_log_event, {segments, Tid2, [{1, 3, SegmentFile}]}} ->
            {ok, Seg} = ra_log_file_segment:open(SegmentFile, #{mode => read}),
            % assert Entries have been fully transferred
            Entries = [{I, T, binary_to_term(B)}
                       || {I, T, B} <- ra_log_file_segment:read(Seg, 1, 3)]
    after 3000 ->
              throw(ra_log_event_timeout)
    end,

    % assert wal file has been deleted.
    false = filelib:is_file(WalFile),
    ok = gen_server:stop(TblWriterPid),
    ok.

accept_mem_tables_rollover_overwrite(_Config) ->
    % create rolled over segment for indexes 10 - 20
    % then write 1 - 30
    % TODO: what should happen here
    % who shouold clean up the first segment
    not_impl.

%%% Internal

fake_mem_table(Dir, Entries) ->
    {registered_name, Self} = erlang:process_info(self(), registered_name),
    Tid = make_mem_table(Self, Entries),
    {FirstIdx, _, _} = hd(Entries),
    {LastIdx, _, _} = lists:last(Entries),
    MemTables = [{Self, FirstIdx, LastIdx, Tid}],
    {MemTables, filename:join(Dir, "blah.wal")}.

make_mem_table(Name, Entries) ->
    Tid = ets:new(Name, []),
    [ets:insert(Tid, E) || E <- Entries],
    Tid.
