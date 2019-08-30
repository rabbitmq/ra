-module(ra_log_wal_SUITE).
-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

all() ->
    [
     {group, default},
     {group, o_sync}
    ].


all_tests() ->
    [
     basic_log_writes,
     same_uid_different_process,
     write_to_unavailable_wal_returns_error,
     write_many,
     overwrite,
     truncate_write,
     out_of_seq_writes,
     roll_over,
     recover,
     recover_after_roll_over,
     recover_truncated_write,
     sys_get_status
    ].

groups() ->
    [
     {default, [], all_tests()},
     {o_sync, [], all_tests()}
    ].

init_per_group(Group, Config) ->
    application:ensure_all_started(sasl),
    application:load(ra),
    ok = application:set_env(ra, data_dir, ?config(priv_dir, Config)),
    ra_directory:init(?config(priv_dir, Config)),
    % application:ensure_all_started(lg),
    [{write_strategy, Group} | Config].

end_per_group(_, Config) ->
    Config.

init_per_testcase(TestCase, Config) ->
    PrivDir = ?config(priv_dir, Config),
    G = ?config(write_strategy, Config),
    Dir = filename:join([PrivDir, G, TestCase]),
    {ok, Ets} = ra_log_ets:start_link(PrivDir),
    UId = atom_to_binary(TestCase, utf8),
    yes = ra_directory:register_name(UId, self(), TestCase),
    WalConf = #{dir => Dir, write_strategy => G},
    _ = ets:new(ra_open_file_metrics, [named_table, public, {write_concurrency, true}]),
    _ = ets:new(ra_io_metrics, [named_table, public, {write_concurrency, true}]),
    ra_file_handle:start_link(),
    [{ra_log_ets, Ets},
     {writer_id, {UId, self()}},
     {test_case, TestCase},
     {wal_conf, WalConf},
     {wal_dir, Dir} | Config].

end_per_testcase(_TestCase, Config) ->
    proc_lib:stop(?config(ra_log_ets, Config)),
    proc_lib:stop(ra_file_handle),
    Config.

basic_log_writes(Config) ->
    Conf = ?config(wal_conf, Config),
    {UId, _} = WriterId = ?config(writer_id, Config),
    {ok, Pid} = ra_log_wal:start_link(Conf, []),
    ok = ra_log_wal:write(WriterId, ra_log_wal, 12, 1, "value"),
    {12, 1, "value"} = await_written(WriterId, {12, 12, 1}),
    ok = ra_log_wal:write(WriterId, ra_log_wal, 13, 1, "value2"),
    {13, 1, "value2"} = await_written(WriterId, {13, 13, 1}),
    % previous log value is still there
    {12, 1, "value"} = mem_tbl_read(UId, 12),
    undefined = mem_tbl_read(UId, 14),
    ra_lib:dump(ets:tab2list(ra_log_open_mem_tables)),
    proc_lib:stop(Pid),
    ok.

same_uid_different_process(Config) ->
    Conf = ?config(wal_conf, Config),
    {UId, _} = WriterId = ?config(writer_id, Config),
    {ok, Pid} = ra_log_wal:start_link(Conf, []),
    ok = ra_log_wal:write(WriterId, ra_log_wal, 12, 1, "value"),
    {12, 1, "value"} = await_written(WriterId, {12, 12, 1}),
    Self = self(),
    _ = spawn(fun() ->
                      Wid = {UId, self()},
                      ok = ra_log_wal:write(Wid, ra_log_wal, 13, 1, "value2"),
                      {13, 1, "value2"} = await_written(Wid, {13, 13, 1}),
                      Self ! go
              end),
    receive
        go -> ok
    after 250 ->
              exit(go_timeout)
    end,
    {12, 1, "value"} = mem_tbl_read(UId, 12),
    {13, 1, "value2"} = mem_tbl_read(UId, 13),
    proc_lib:stop(Pid),
    ok.



write_to_unavailable_wal_returns_error(Config) ->
    WriterId = ?config(writer_id, Config),
    {error, wal_down} = ra_log_wal:write(WriterId, ra_log_wal, 12, 1, "value"),
    {error, wal_down} = ra_log_wal:truncate_write(WriterId, ra_log_wal, 12, 1, "value"),
    ok.

write_many(Config) ->
    NumWrites = 10000,
    Conf = ?config(wal_conf, Config),
    WriterId = ?config(writer_id, Config),
    {ok, WalPid} = ra_log_wal:start_link(Conf#{compute_checksums => false},
                                         []),
    Data = crypto:strong_rand_bytes(1024),
    ok = ra_log_wal:write(WriterId, ra_log_wal, 0, 1, Data),
    timer:sleep(5),
    % start_profile(Config, [ra_log_wal, ra_file_handle, ets, file, lists, os]),
    Writes = lists:seq(1, NumWrites),
    {_, GarbBefore} = erlang:process_info(WalPid, garbage_collection),
    {_, MemBefore} = erlang:process_info(WalPid, memory),
    {_, BinBefore} = erlang:process_info(WalPid, binary),
    {reductions, RedsBefore} = erlang:process_info(WalPid, reductions),

    {Taken, _} =
        timer:tc(
          fun () ->
                  [ok = ra_log_wal:write(WriterId, ra_log_wal, Idx, 1,
                                         {data, Data}) || Idx <- Writes],
                  receive
                      {ra_log_event, {written, {_, NumWrites, 1}}} ->
                          ok
                  after 100000 ->
                            throw(written_timeout)
                  end
          end),
    timer:sleep(5), % give the gc some time
    {reductions, RedsAfter} = erlang:process_info(WalPid, reductions),
    {_, BinAfter} = erlang:process_info(WalPid, binary),
    {_, GarbAfter} = erlang:process_info(WalPid, garbage_collection),
    {_, MemAfter} = erlang:process_info(WalPid, memory),

    ct:pal("Binary:~n~w~n~w~n", [length(BinBefore), length(BinAfter)]),
    ct:pal("Garbage:~n~w~n~w~n", [GarbBefore, GarbAfter]),
    ct:pal("Memory:~n~w~n~w~n", [MemBefore, MemAfter]),

    Reds = RedsAfter - RedsBefore,
    ct:pal("~b 1024 byte writes took ~p milliseconds~n~n"
           "Reductions: ~b",
           [NumWrites, Taken / 1000, Reds]),

    % assert memory use after isn't absurdly larger than before
    ?assert(MemAfter < (MemBefore * 2)),

    % assert we aren't regressing on reductions used
    ?assert(Reds < 52023339 * 1.1),
    % stop_profile(Config),
    Metrics = [M || {_, V} = M <- lists:sort(ets:tab2list(ra_log_wal_metrics)),
                    V =/= undefined],
    ct:pal("Metrics: ~p~n", [Metrics]),
    proc_lib:stop(WalPid),
    ok.

overwrite(Config) ->
    Conf = ?config(wal_conf, Config),
    WriterId = ?config(writer_id, Config),
    {ok, Pid} = ra_log_wal:start_link(Conf, []),
    Data = data,
    [ok = ra_log_wal:write(WriterId, ra_log_wal, I, 1, Data)
     || I <- lists:seq(1, 3)],
    await_written(WriterId, {1, 3, 1}),
    % write next index then immediately overwrite
    ok = ra_log_wal:write(WriterId, ra_log_wal, 4, 1, Data),
    ok = ra_log_wal:write(WriterId, ra_log_wal, 2, 2, Data),
    % ensure we await the correct range that should not have a wonky start
    await_written(WriterId, {2, 2, 2}),
    proc_lib:stop(Pid),
    ok.

truncate_write(Config) ->
    % a truncate write should update the range to not include previous indexes
    % a trucated write does not need to follow the sequence
    Conf = ?config(wal_conf, Config),
    {UId, _} = WriterId = ?config(writer_id, Config),
    {ok, Pid} = ra_log_wal:start_link(Conf, []),
    Data = crypto:strong_rand_bytes(1024),
    % write 1-3
    [ok = ra_log_wal:write(WriterId, ra_log_wal, I, 1, Data)
     || I <- lists:seq(1, 3)],
    await_written(WriterId, {1, 3, 1}),
    % then write 7 as may happen after snapshot installation
    ok = ra_log_wal:truncate_write(WriterId, ra_log_wal, 7, 1, Data),
    ok = ra_log_wal:write(WriterId, ra_log_wal, 8, 1, Data),
    await_written(WriterId, {7, 8, 1}),
    [{UId, 7, 8, Tid}] = ets:lookup(ra_log_open_mem_tables, UId),
    [_] = ets:lookup(Tid, 7),
    [_] = ets:lookup(Tid, 8),
    proc_lib:stop(Pid),
    ok.

out_of_seq_writes(Config) ->
    % INVARIANT: the WAL expects writes for a particular ra server to be done
    % using a contiguous range of integer keys (indexes). If a gap is detected
    % it will notify the write of the missing index and the writer can resend
    % writes from that point
    % the wal will discard all subsequent writes until it receives the missing one
    Conf = ?config(wal_conf, Config),
    {_UId, _} = WriterId = ?config(writer_id, Config),
    {ok, Pid} = ra_log_wal:start_link(Conf, []),
    Data = crypto:strong_rand_bytes(1024),
    % write 1-3
    [ok = ra_log_wal:write(WriterId, ra_log_wal, I, 1, Data)
     || I <- lists:seq(1, 3)],
    await_written(WriterId, {1, 3, 1}),
    % then write 5
    ok = ra_log_wal:write(WriterId, ra_log_wal, 5, 1, Data),
    % ensure an out of sync notification is received
    receive
        {ra_log_event, {resend_write, 4}} -> ok
    after 500 ->
              throw(reset_write_timeout)
    end,
    % try writing 6
    ok = ra_log_wal:write(WriterId, ra_log_wal, 6, 1, Data),

    % then write 4 and 5
    ok = ra_log_wal:write(WriterId, ra_log_wal, 4, 1, Data),
    await_written(WriterId, {4, 4, 1}),
    ok = ra_log_wal:write(WriterId, ra_log_wal, 5, 1, Data),
    await_written(WriterId, {5, 5, 1}),

    % perform another out of sync write
    ok = ra_log_wal:write(WriterId, ra_log_wal, 7, 1, Data),
    receive
        {ra_log_event, {resend_write, 6}} -> ok
    after 500 ->
              throw(written_timeout)
    end,
    % force a roll over
    ok = ra_log_wal:force_roll_over(ra_log_wal),
    % try writing another
    ok = ra_log_wal:write(WriterId, ra_log_wal, 8, 1, Data),
    % ensure a written event is _NOT_ received
    % when a roll-over happens after out of sync write
    receive
        {ra_log_event, {written, {8, 8, 1}}} ->
            throw(unexpected_written_event)
    after 500 -> ok
    end,
    % write the missing one
    ok = ra_log_wal:write(WriterId, ra_log_wal, 6, 1, Data),
    await_written(WriterId, {6, 6, 1}),
    proc_lib:stop(Pid),
    ok.

roll_over(Config) ->
    Conf = ?config(wal_conf, Config),
    {UId, _} = WriterId = ?config(writer_id, Config),
    NumWrites = 5,
    meck:new(ra_log_segment_writer, [passthrough]),
    meck:expect(ra_log_segment_writer, await,
                fun(_) -> ok end),
    % configure max_wal_size_bytes
    {ok, Pid} = ra_log_wal:start_link(Conf#{max_size_bytes => 1024 * NumWrites,
                                            segment_writer => self()}, []),
    % write enough entries to trigger roll over
    Data = crypto:strong_rand_bytes(1024),
    [begin
         ok = ra_log_wal:write(WriterId, ra_log_wal, Idx, 1, Data)
     end || Idx <- lists:seq(1, NumWrites)],
    % wait for writes
    receive {ra_log_event, {written, {_, NumWrites, 1}}} -> ok
    after 5000 -> throw(written_timeout)
    end,

    % validate we receive the new mem tables notifications as if we were
    % the writer process
    receive
        {'$gen_cast', {mem_tables, [{UId, _Fst, _Lst, Tid}], _}} ->
            [{UId, 5, 5, CurrentTid}] = ets:lookup(ra_log_open_mem_tables, UId),
            % the current tid is not the same as the rolled over one
            ?assert(Tid =/= CurrentTid),
            % ensure closed mem tables contain the previous mem_table
            [{UId, _, 1, 4, Tid}] = ets:lookup(ra_log_closed_mem_tables, UId)
    after 2000 ->
              throw(new_mem_tables_timeout)
    end,

    % TODO: validate we can read first and last written
    ?assert(undefined =/= mem_tbl_read(UId, 1)),
    ?assert(undefined =/= mem_tbl_read(UId, 5)),
    meck:unload(),
    proc_lib:stop(Pid),
    ok.

recover_truncated_write(Config) ->
    % open wal and write a few entreis
    % close wal + delete mem_tables
    % re-open wal and validate mem_tables are re-created
    Conf0 = ?config(wal_conf, Config),
    {UId, _} = WriterId = ?config(writer_id, Config),
    Conf = Conf0#{segment_writer => self()},
    Data = <<42:256/unit:8>>,
    meck:new(ra_log_segment_writer, [passthrough]),
    meck:expect(ra_log_segment_writer, await,
                fun(_) -> ok end),
    {ok, _Pid} = ra_log_wal:start_link(Conf, []),
    [ok = ra_log_wal:write(WriterId, ra_log_wal, Idx, 1, Data)
     || Idx <- lists:seq(1, 3)],
    ok = ra_log_wal:truncate_write(WriterId, ra_log_wal, 9, 1, Data),
    empty_mailbox(),
    proc_lib:stop(ra_log_wal),
    {ok, Pid} = ra_log_wal:start_link(Conf, []),
    % how can we better wait for recovery to finish?
    timer:sleep(1000),
    [{UId, _, 9, 9, _}] =
        lists:sort(ets:lookup(ra_log_closed_mem_tables, UId)),
    meck:unload(),
    proc_lib:stop(Pid),
    ok.

sys_get_status(Config) ->
    Conf = ?config(wal_conf, Config),
    {_UId, _} = ?config(writer_id, Config),
    {ok, Pid} = ra_log_wal:start_link(Conf, []),
    {_, _, _, [_, _, _, _, [_, _ ,S]]} = sys:get_status(ra_log_wal),
    #{write_strategy := _} = S,
    proc_lib:stop(Pid),
    ok.

recover_after_roll_over(Config) ->
    Conf0 = ?config(wal_conf, Config),
    WriterId = ?config(writer_id, Config),
    Data = <<42:256/unit:8>>,
    Conf = Conf0#{segment_writer => self(),
                  max_size_bytes => byte_size(Data) * 75},
    meck:new(ra_log_segment_writer, [passthrough]),
    meck:expect(ra_log_segment_writer, await, fun(_) -> ok end),
    {ok, _} = ra_log_wal:start_link(Conf, []),
    [ok = ra_log_wal:write(WriterId, ra_log_wal, Idx, 1, Data)
     || Idx <- lists:seq(1, 100)],
    empty_mailbox(),
    proc_lib:stop(ra_log_wal),
    {ok, Wal} = ra_log_wal:start_link(Conf, []),
    % how can we better wait for recovery to finish?
    timer:sleep(1000),
    ?assert(erlang:is_process_alive(Wal)),
    meck:unload(),
    proc_lib:stop(Wal),
    ok.

recover(Config) ->
    % open wal and write a few entreis
    % close wal + delete mem_tables
    % re-open wal and validate mem_tables are re-created
    Conf0 = ?config(wal_conf, Config),
    {UId, _} = WriterId = ?config(writer_id, Config),
    Conf = Conf0#{segment_writer => self()},
    Data = <<42:256/unit:8>>,
    meck:new(ra_log_segment_writer, [passthrough]),
    meck:expect(ra_log_segment_writer, await, fun(_) -> ok end),
    {ok, _Wal} = ra_log_wal:start_link(Conf, []),
    [ok = ra_log_wal:write(WriterId, ra_log_wal, Idx, 1, Data)
     || Idx <- lists:seq(1, 100)],
    ra_log_wal:force_roll_over(ra_log_wal),
    [ok = ra_log_wal:write(WriterId, ra_log_wal, Idx, 2, Data)
     || Idx <- lists:seq(101, 200)],
    empty_mailbox(),
    proc_lib:stop(ra_log_wal),
    {ok, Pid} = ra_log_wal:start_link(Conf, []),
    % how can we better wait for recovery to finish?
    timer:sleep(1000),

    % there should be no open mem tables after recovery as we treat any found
    % wal files as complete
    [] = ets:lookup(ra_log_open_mem_tables, UId),
    [ {UId, _, 1, 100, MTid1}, % this is the "old" table
      % these are the recovered tables
      {UId, _, 1, 100, MTid2}, {UId, _, 101, 200, MTid4} ] =
        lists:sort(ets:lookup(ra_log_closed_mem_tables, UId)),
    100 = ets:info(MTid1, size),
    100 = ets:info(MTid2, size),
    100 = ets:info(MTid4, size),
    % check that both mem_tables notifications are received by the segment writer
    receive
        {'$gen_cast', {mem_tables, [{UId, 1, 100, _}], _}} -> ok
    after 2000 ->
              throw(new_mem_tables_timeout)
    end,
    receive
        {'$gen_cast', {mem_tables, [{UId, 101, 200, _}], _}} -> ok
    after 2000 ->
              throw(new_mem_tables_timeout)
    end,

    meck:unload(),
    proc_lib:stop(Pid),
    ok.

empty_mailbox() ->
    receive
        _ ->
            empty_mailbox()
    after 100 ->
              ok
    end.

await_written({UId, _} = Id, {From, To, Term} = Written) ->
    receive
        {ra_log_event, {written, Written}} ->
            mem_tbl_read(UId, To);
        {ra_log_event, {written, {From, T, _}}} ->
            ct:pal("received partial written event ~w~n", [Written]),
            await_written(Id, {T+1, To, Term})
    after 5000 ->
              throw({written_timeout, To})
    end.

start_profile(Config, Modules) ->
    Dir = ?config(priv_dir, Config),
    Case = ?config(test_case, Config),
    GzFile = filename:join([Dir, "lg_" ++ atom_to_list(Case) ++ ".gz"]),
    ct:pal("Profiling to ~p~n", [GzFile]),

    lg:trace(Modules, lg_file_tracer,
             GzFile, #{running => false, mode => profile}).

stop_profile(Config) ->
    Case = ?config(test_case, Config),
    ct:pal("Stopping profiling for ~p~n", [Case]),
    lg:stop(),
    % this segfaults
    % timer:sleep(2000),
    Dir = ?config(priv_dir, Config),
    Name = filename:join([Dir, "lg_" ++ atom_to_list(Case)]),
    lg_callgrind:profile_many(Name ++ ".gz.*", Name ++ ".out",#{}),
    ok.


% mem table read functions
% the actual logic is implemented in ra_log
mem_tbl_read(Id, Idx) ->
    case ets:lookup(ra_log_open_mem_tables, Id) of
        [{_, Fst, _, _}] = Tids when Idx >= Fst ->
            tbl_lookup(Tids, Idx);
        _ ->
            closed_mem_tbl_read(Id, Idx)
    end.

closed_mem_tbl_read(Id, Idx) ->
    case ets:lookup(ra_log_closed_mem_tables, Id) of
        [] ->
            undefined;
        Tids0 ->
            Tids = lists:sort(fun(A, B) -> B > A end, Tids0),
            closed_tbl_lookup(Tids, Idx)
    end.

closed_tbl_lookup([], _Idx) ->
    undefined;
closed_tbl_lookup([{_, _, _First, Last, Tid} | Tail], Idx) when Last >= Idx ->
    % TODO: it is possible the ETS table has been deleted at this
    % point so should catch the error
    case ets:lookup(Tid, Idx) of
        [] ->
            closed_tbl_lookup(Tail, Idx);
        [Entry] -> Entry
    end;
closed_tbl_lookup([_ | Tail], Idx) ->
    closed_tbl_lookup(Tail, Idx).

tbl_lookup([], _Idx) ->
    undefined;
tbl_lookup([{_, _First, Last, Tid} | Tail], Idx) when Last >= Idx ->
    % TODO: it is possible the ETS table has been deleted at this
    % point so should catch the error
    case ets:lookup(Tid, Idx) of
        [] ->
            tbl_lookup(Tail, Idx);
        [Entry] -> Entry
    end;
tbl_lookup([_ | Tail], Idx) ->
    tbl_lookup(Tail, Idx).
