%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2017-2025 Broadcom. All Rights Reserved. The term Broadcom refers to Broadcom Inc. and/or its subsidiaries.
%%
-module(ra_log_wal_SUITE).
-compile(nowarn_export_all).
-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(MAX_SIZE_BYTES, 128 * 1000 * 1000).

all() ->
    [
     {group, default},
     {group, fsync},
     {group, no_sync}
    ].


all_tests() ->
    [
     basic_log_writes,
     sparse_writes,
     sparse_write_same_batch,
     sparse_write_overwrite,
     sparse_write_recover,
     wal_filename_upgrade,
     same_uid_different_process,
     consecutive_terms_in_batch_should_result_in_two_written_events,
     overwrite_in_same_batch,
     writes_implicit_truncate_write,
     writes_snapshot_idx_overtakes,
     writes_snapshot_idx_overtakes_same_batch,
     overwrite_completely,
     overwrite_inside,
     recover,
     recover_overwrite,
     recover_with_snapshot_index,
     recover_overwrite_rollover,
     recover_existing_mem_table,
     recover_existing_mem_table_with_deletes,
     recover_existing_mem_table_overwrite,
     recover_implicit_truncate,
     recover_delete_uid,
     write_to_unavailable_wal_returns_error,
     write_many,
     write_many_by_many,
     out_of_seq_writes,
     roll_over_max_size,
     roll_over_with_data_larger_than_max_size,
     roll_over_entry_limit,
     recover_empty,
     recover_with_last_entry_corruption,
     recover_with_last_entry_corruption_pre_allocate,
     checksum_failure_in_middle_of_file_should_fail,
     recover_with_partial_last_entry,
     sys_get_status
    ].

groups() ->
    [
     {default, [], all_tests()},
     %% uses fsync instead of the default fdatasync
     %% just testing that the configuration and dispatch works
     {fsync, [], [basic_log_writes]},
     {no_sync, [], all_tests()}
    ].

-define(SYS, default).

init_per_group(Group, Config) ->
    meck:unload(),
    application:ensure_all_started(sasl),
    application:load(ra),
    ok = application:set_env(ra, data_dir, ?config(priv_dir, Config)),
    ra_env:configure_logger(logger),
    Dir = ?config(priv_dir, Config),
    SysCfg = (ra_system:default_config())#{data_dir => Dir},
    ra_system:store(SysCfg),
    ra_directory:init(?SYS),
    ra_counters:init(),
    % application:ensure_all_started(lg),
    SyncMethod =
        case Group of
            fsync ->
                sync;
            no_sync ->
                none;
            _ ->
                datasync
        end,
    [
     {sys_cfg, SysCfg},
     {sync_method,  SyncMethod} | Config].

end_per_group(_, Config) ->
    Config.

init_per_testcase(TestCase, Config) ->
    PrivDir = ?config(priv_dir, Config),
    M = ?config(sync_method, Config),
    Sys = ?config(sys_cfg, Config),
    Dir = filename:join([PrivDir, M, TestCase]),
    {ok, Ets} = ra_log_ets:start_link(Sys),
    ra_counters:init(),
    UId = atom_to_binary(TestCase, utf8),
    ok = ra_directory:register_name(default, UId, self(), undefined,
                                    TestCase, TestCase),
    Names = maps:get(names, Sys),
    WalConf = #{dir => Dir,
                system => ?SYS,
                system => default,
                names => Names#{segment_writer => self()},
                max_size_bytes => ?MAX_SIZE_BYTES},
    _ = ets:new(ra_log_snapshot_state,
                [named_table, public, {write_concurrency, true}]),
    [{ra_log_ets, Ets},
     {writer_id, {UId, self()}},
     {test_case, TestCase},
     {wal_conf, WalConf},
     {names, Names},
     {wal_dir, Dir} | Config].

end_per_testcase(_TestCase, Config) ->
    meck:unload(),
    proc_lib:stop(?config(ra_log_ets, Config)),
    Config.

basic_log_writes(Config) ->
    meck:new(ra_log_segment_writer, [passthrough]),
    meck:expect(ra_log_segment_writer, await, fun(_) -> ok end),
    Conf = ?config(wal_conf, Config),
    {UId, _} = WriterId = ?config(writer_id, Config),
    Tid = ets:new(?FUNCTION_NAME, []),
    {ok, Pid} = ra_log_wal:start_link(Conf),
    {ok, _} = ra_log_wal:write(Pid, WriterId, Tid, 12, 1, "value"),
    ok = await_written(WriterId, 1, [12]),
    {ok, _} = ra_log_wal:write(Pid, WriterId, Tid, 13, 1, "value2"),
    ok = await_written(WriterId, 1, [13]),
    ra_log_wal:force_roll_over(Pid),
    receive
        {'$gen_cast',
         {mem_tables, #{UId := [{Tid, [13, 12]}]}, "0000000000000001.wal"}} ->
            ok
    after 5000 ->
              flush(),
              ct:fail("receiving mem table ranges timed out")
    end,
    proc_lib:stop(Pid),
    meck:unload(),
    ok.

sparse_writes(Config) ->
    meck:new(ra_log_segment_writer, [passthrough]),
    meck:expect(ra_log_segment_writer, await, fun(_) -> ok end),
    Conf = ?config(wal_conf, Config),
    {UId, _} = WriterId = ?config(writer_id, Config),
    Tid = ets:new(?FUNCTION_NAME, []),
    {ok, Pid} = ra_log_wal:start_link(Conf),
    {ok, _} = ra_log_wal:write(Pid, WriterId, Tid, 11, 12, 1, "value"),
    ok = await_written(WriterId, 1, [12]),
    %% write  a "sparse write" at index 15 but reference 12 as the last
    %% one
    {ok, _} = ra_log_wal:write(Pid, WriterId, Tid, 12, 15, 1, "value2"),
    ok = await_written(WriterId, 1, [15]),
    ra_log_wal:force_roll_over(Pid),
    receive
        {'$gen_cast',
         {mem_tables, #{UId := [{Tid, [15, 12]}]}, _}} ->
            ok
    after 5000 ->
              flush(),
              ct:fail("receiving mem table ranges timed out")
    end,
    proc_lib:stop(Pid),
    meck:unload(),
    ok.

sparse_write_same_batch(Config) ->
    meck:new(ra_log_segment_writer, [passthrough]),
    meck:expect(ra_log_segment_writer, await, fun(_) -> ok end),
    Conf = ?config(wal_conf, Config),
    {UId, _} = WriterId = ?config(writer_id, Config),
    Tid = ets:new(?FUNCTION_NAME, []),
    {ok, Pid} = ra_log_wal:start_link(Conf),

    suspend_process(Pid),
    {ok, _} = ra_log_wal:write(Pid, WriterId, Tid, 11, 12, 1, "value"),
    {ok, _} = ra_log_wal:write(Pid, WriterId, Tid, 12, 15, 1, "value2"),
    erlang:resume_process(Pid),

    ok = await_written(WriterId, 1, [15, 12]),
    ra_log_wal:force_roll_over(Pid),
    receive
        {'$gen_cast',
         {mem_tables, #{UId := [{Tid, [15, 12]}]}, _}} ->
            ok
    after 5000 ->
              flush(),
              ct:fail("receiving mem table ranges timed out")
    end,
    proc_lib:stop(Pid),
    meck:unload(),
    ok.

sparse_write_recover(Config) ->
    meck:new(ra_log_segment_writer, [passthrough]),
    meck:expect(ra_log_segment_writer, await, fun(_) -> ok end),
    Conf = ?config(wal_conf, Config),
    {UId, _} = WriterId = ?config(writer_id, Config),
    Tid = ets:new(?FUNCTION_NAME, []),
    {ok, Pid} = ra_log_wal:start_link(Conf),
    {ok, _} = ra_log_wal:write(Pid, WriterId, Tid, 11, 12, 1, "value"),
    ok = await_written(WriterId, 1, [12]),
    {ok, _} = ra_log_wal:write(Pid, WriterId, Tid, 12, 15, 1, "value2"),
    ok = await_written(WriterId, 1, [15]),

    ok = proc_lib:stop(ra_log_wal),
    {ok, _Pid2} = ra_log_wal:start_link(Conf),
    % {ok, Mt} = ra_log_ets:mem_table_please(?config(names, Config), UId),
    receive
        {'$gen_cast',
         {mem_tables, #{UId := [{Tid, [15, 12]}]}, _}} ->
            ok
    after 5000 ->
              flush(),
              ct:fail("receiving mem table ranges timed out")
    end,
    proc_lib:stop(Pid),
    meck:unload(),
    ok.

%% TODO: as sparse writes are pre committed I dont
%% think we'll ever overwrite anything.
sparse_write_overwrite(_Config) ->
    ok.

wal_filename_upgrade(Config) ->
    meck:new(ra_log_segment_writer, [passthrough]),
    meck:expect(ra_log_segment_writer, await, fun(_) -> ok end),
    Conf = ?config(wal_conf, Config),
    #{dir := Dir} = Conf,
    {UId, _} = WriterId = ?config(writer_id, Config),
    Tid = ets:new(?FUNCTION_NAME, []),
    {ok, Pid} = ra_log_wal:start_link(Conf),
    {ok, _} = ra_log_wal:write(Pid, WriterId, Tid, 12, 1, "value"),
    ok = await_written(WriterId, 1, [12]),
    {ok, _} = ra_log_wal:write(Pid, WriterId, Tid, 13, 1, "value2"),
    ok = await_written(WriterId, 1, [13]),
    proc_lib:stop(Pid),
    %% rename file to old 8 character format
    Fn = filename:join(Dir, "0000000000000001.wal"),
    FnOld = filename:join(Dir, "00000001.wal"),
    ok = file:rename(Fn, FnOld),
    {ok, Pid2} = ra_log_wal:start_link(Conf),
    receive
        {'$gen_cast',
         {mem_tables, #{UId := [{_Tid, [13, 12]}]}, "0000000000000001.wal"}} ->
            ok
    after 5000 ->
              flush(),
              ct:fail("receiving mem tables timed out")
    end,
    proc_lib:stop(Pid2),
    meck:unload(),
    ok.

same_uid_different_process(Config) ->
    meck:new(ra_log_segment_writer, [passthrough]),
    meck:expect(ra_log_segment_writer, await, fun(_) -> ok end),
    Conf = ?config(wal_conf, Config),
    {UId, _} = WriterId = ?config(writer_id, Config),
    Tid = ets:new(?FUNCTION_NAME, []),
    {ok, Pid} = ra_log_wal:start_link(Conf),
    {ok, _} = ra_log_wal:write(Pid, WriterId, Tid, 12, 1, "value"),
    ok = await_written(WriterId, 1, [12]),
    Self = self(),
    _ = spawn(fun() ->
                      Wid = {UId, self()},
                      {ok, _} = ra_log_wal:write(Pid, Wid, Tid, 13, 1, "value2"),
                      ok = await_written(Wid, 1, [13]),
                      Self ! go
              end),
    receive
        go -> ok
    after 250 ->
              flush(),
              exit(go_timeout)
    end,
    ra_log_wal:force_roll_over(Pid),
    receive
        {'$gen_cast',
         {mem_tables, #{UId := [{Tid, [13, 12]}]}, _WalFile}} ->
            ok
    after 5000 ->
              flush(),
              ct:fail("receiving mem tables timed out")
    end,
    proc_lib:stop(Pid),
    meck:unload(ra_log_segment_writer),
    ok.

consecutive_terms_in_batch_should_result_in_two_written_events(Config) ->
    meck:new(ra_log_segment_writer, [passthrough]),
    meck:expect(ra_log_segment_writer, await, fun(_) -> ok end),
    Conf = ?config(wal_conf, Config),
    {UId, _} = WriterId = ?config(writer_id, Config),
    {ok, Pid} = ra_log_wal:start_link(Conf),
    Data = <<"data">>,
    Tid = ets:new(?FUNCTION_NAME, []),
    [{ok, _} = ra_log_wal:write(Pid, WriterId, Tid, I, 1, Data)
     || I <- lists:seq(1, 3)],
    await_written(WriterId, 1, [{1, 3}]),
    flush(),
    suspend_process(Pid),
    {ok, _} = ra_log_wal:write(Pid, WriterId, Tid, 4, 1, Data),
    {ok, _} = ra_log_wal:write(Pid, WriterId, Tid, 5, 2, Data),
    erlang:resume_process(Pid),
    await_written(WriterId, 1, [4]),
    await_written(WriterId, 2, [5]),
    ra_log_wal:force_roll_over(Pid),
    receive
        {'$gen_cast',
         {mem_tables, #{UId := [{Tid, [{1, 5}]}]}, _WalFile}} ->
            ok
    after 5000 ->
              flush(),
              ct:fail("receiving mem tables timed out")
    end,
    proc_lib:stop(Pid),
    meck:unload(ra_log_segment_writer),
    ok.

writes_snapshot_idx_overtakes(Config) ->
    meck:new(ra_log_segment_writer, [passthrough]),
    meck:expect(ra_log_segment_writer, await, fun(_) -> ok end),
    Conf = ?config(wal_conf, Config),
    {UId, _} = WriterId = ?config(writer_id, Config),
    {ok, Pid} = ra_log_wal:start_link(Conf),
    Data = <<"data">>,
    Tid = ets:new(?FUNCTION_NAME, []),
    [{ok, _} = ra_log_wal:write(Pid, WriterId, Tid, I, 1, Data)
     || I <- lists:seq(1, 3)],
    await_written(WriterId, 1, [{1, 3}]),
    % snapshot idx overtakes
    ok = ra_log_snapshot_state:insert(ra_log_snapshot_state, UId, 5, 6, []),
    [{ok, _} = ra_log_wal:write(Pid, WriterId, Tid, I, 1, Data)
     || I <- lists:seq(4, 7)],
    await_written(WriterId, 1, [{6, 7}]),
    ra_log_wal:force_roll_over(Pid),
    receive
        {'$gen_cast',
         {mem_tables, #{UId := [{Tid, [7, 6]}]}, _WalFile}} ->
            ok
    after 5000 ->
              flush(),
              ct:fail("receiving mem tables timed out")
    end,
    proc_lib:stop(Pid),
    meck:unload(ra_log_segment_writer),
    flush(),
    ok.

writes_implicit_truncate_write(Config) ->
    meck:new(ra_log_segment_writer, [passthrough]),
    meck:expect(ra_log_segment_writer, await, fun(_) -> ok end),
    Conf = ?config(wal_conf, Config),
    {UId, _} = WriterId = ?config(writer_id, Config),
    {ok, Pid} = ra_log_wal:start_link(Conf),
    Data = <<"data">>,
    Tid = ets:new(?FUNCTION_NAME, []),
    [{ok, _} = ra_log_wal:write(Pid, WriterId, Tid, I, 1, Data)
     || I <- lists:seq(1, 3)],
    await_written(WriterId, 1, [{1, 3}]),
    % snapshot idx updated and we follow that with the next index after the
    % snapshot.
    % before we had to detect this and send a special {truncate, append request
    % but this is not necessary anymore
    ok = ra_log_snapshot_state:insert(ra_log_snapshot_state, UId, 5, 6, []),
    [{ok, _} = ra_log_wal:write(Pid, WriterId, Tid, I, 1, Data)
     || I <- lists:seq(6, 7)],
    await_written(WriterId, 1, [7, 6]),
    ra_log_wal:force_roll_over(Pid),
    receive
        {'$gen_cast',
         {mem_tables, #{UId := [{Tid, [7, 6]}]}, _WalFile}} ->
            ok
    after 5000 ->
              flush(),
              ct:fail("receiving mem tables timed out")
    end,
    proc_lib:stop(Pid),
    meck:unload(ra_log_segment_writer),
    flush(),
    ok.

writes_snapshot_idx_overtakes_same_batch(Config) ->
    meck:new(ra_log_segment_writer, [passthrough]),
    meck:expect(ra_log_segment_writer, await, fun(_) -> ok end),
    Conf = ?config(wal_conf, Config),
    {UId, _} = WriterId = ?config(writer_id, Config),
    {ok, Pid} = ra_log_wal:start_link(Conf),
    Data = <<"data">>,
    Tid = ets:new(?FUNCTION_NAME, []),
    erlang:suspend_process(Pid),
    {ok, _} = ra_log_wal:write(Pid, WriterId, Tid, 1, 1, Data),
    {ok, _} = ra_log_wal:write(Pid, WriterId, Tid, 2, 1, Data),
    {ok, _} = ra_log_wal:write(Pid, WriterId, Tid, 3, 1, Data),
    %% this ensures the snapshot state is updated within the processing of a
    %% single batch
    gen_batch_server:cast(
      Pid, {query,
            fun (_) ->

                    ok = ra_log_snapshot_state:insert(ra_log_snapshot_state, UId,
                                                      5, 6, [])
            end}),
    {ok, _} = ra_log_wal:write(Pid, WriterId, Tid, 4, 1, Data),
    {ok, _} = ra_log_wal:write(Pid, WriterId, Tid, 5, 1, Data),
    {ok, _} = ra_log_wal:write(Pid, WriterId, Tid, 6, 1, Data),
    {ok, _} = ra_log_wal:write(Pid, WriterId, Tid, 7, 1, Data),
    erlang:resume_process(Pid),
    % await_written(WriterId, {1, 3, 1}),
    await_written(WriterId, 1, [{6, 7}]),
    ra_log_wal:force_roll_over(Pid),
    receive
        {'$gen_cast',
         {mem_tables, #{UId := [{Tid, [7, 6]}]}, _WalFile}} ->
            ok
    after 5000 ->
              flush(),
              ct:fail("receiving mem tables timed out")
    end,
    proc_lib:stop(Pid),
    meck:unload(ra_log_segment_writer),
    ok.

overwrite_in_same_batch(Config) ->
    meck:new(ra_log_segment_writer, [passthrough]),
    meck:expect(ra_log_segment_writer, await, fun(_) -> ok end),
    Conf = ?config(wal_conf, Config),
    {UId, _} = WriterId = ?config(writer_id, Config),
    {ok, Pid} = ra_log_wal:start_link(Conf),
    Data = <<"data">>,
    Tid = ets:new(?FUNCTION_NAME, []),
    Tid2 = ets:new(?FUNCTION_NAME, []),
    [{ok, _} = ra_log_wal:write(Pid, WriterId, Tid, I, 1, Data)
     || I <- lists:seq(1, 3)],
    await_written(WriterId, 1, [{1, 3}]),
    % write next index then immediately overwrite
    suspend_process(Pid),
    {ok, _} = ra_log_wal:write(Pid, WriterId, Tid, 4, 1, Data),
    {ok, _} = ra_log_wal:write(Pid, WriterId, Tid, 5, 1, Data),
    %% overwrites can _only_ be done with a new mem table,
    %% this should be the only time a new mem table should need to
    %% be opened
    {ok, _} = ra_log_wal:write(Pid, WriterId, Tid2, 5, 2, Data),
    erlang:resume_process(Pid),
    % currently this event woudl cause ra_log to enter resend flow.
    % TODO: mt: find a way to avoid this, ideally we'd like to know the ranges
    % for each term such that we can walk back until the first index that matches
    % the term and set that as the last_written_index
    await_written(WriterId, 1, [{4, 5}]),
    await_written(WriterId, 2, [{5, 5}]),
    ra_log_wal:force_roll_over(Pid),
    receive
        {'$gen_cast',
         {mem_tables, #{UId := [{Tid2, [5]},%% the range to flush from the new table
                                {Tid, [{1, 5}]}] %% this is the old table
                       }, _WalFile}} ->
            ok
    after 5000 ->
              flush(),
              ct:fail("receiving mem tables timed out")
    end,
    proc_lib:stop(Pid),
    meck:unload(ra_log_segment_writer),
    ok.

overwrite_completely(Config) ->
    meck:new(ra_log_segment_writer, [passthrough]),
    meck:expect(ra_log_segment_writer, await, fun(_) -> ok end),
    Conf = ?config(wal_conf, Config),
    {UId, _} = WriterId = ?config(writer_id, Config),
    {ok, Pid} = ra_log_wal:start_link(Conf),
    Data = <<"data">>,
    Tid = ets:new(?FUNCTION_NAME, []),
    Tid2 = ets:new(?FUNCTION_NAME, []),
    [{ok, _} = ra_log_wal:write(Pid, WriterId, Tid, I, 1, Data)
     || I <- lists:seq(3, 5)],
    await_written(WriterId, 1, [{3, 5}]),
    % overwrite it all
    [{ok, _} = ra_log_wal:write(Pid, WriterId, Tid2, I, 2, Data)
     || I <- lists:seq(3, 5)],
    await_written(WriterId, 2, [{3, 5}]),
    ra_log_wal:force_roll_over(Pid),
    receive
        {'$gen_cast',
         {mem_tables, #{UId := [{Tid2, [{3, 5}]},
                                {Tid, [{3, 5}]}]}, _WalFile}} ->
            ok
    after 5000 ->
              flush(),
              ct:fail("receiving mem tables timed out")
    end,
    proc_lib:stop(Pid),
    meck:unload(ra_log_segment_writer),
    ok.

overwrite_inside(Config) ->
    meck:new(ra_log_segment_writer, [passthrough]),
    meck:expect(ra_log_segment_writer, await, fun(_) -> ok end),
    Conf = ?config(wal_conf, Config),
    {UId, _} = WriterId = ?config(writer_id, Config),
    {ok, Pid} = ra_log_wal:start_link(Conf),
    Data = <<"data">>,
    Tid = ets:new(?FUNCTION_NAME, []),
    Tid2 = ets:new(?FUNCTION_NAME, []),
    [{ok, _} = ra_log_wal:write(Pid, WriterId, Tid, I, 1, Data)
     || I <- lists:seq(1, 5)],
    await_written(WriterId, 1, [{1, 5}]),
    % overwrite it all
    [{ok, _} = ra_log_wal:write(Pid, WriterId, Tid2, I, 2, Data)
     || I <- lists:seq(3, 4)],
    await_written(WriterId, 2, [{3, 4}]),
    ra_log_wal:force_roll_over(Pid),
    receive
        {'$gen_cast',
         {mem_tables, #{UId := [{Tid2, [4, 3]},
                                {Tid, [{1, 5}]}]}, _WalFile}} ->
            ok
    after 5000 ->
              flush(),
              ct:fail("receiving mem tables timed out")
    end,
    proc_lib:stop(Pid),
    meck:unload(ra_log_segment_writer),
    ok.

write_to_unavailable_wal_returns_error(Config) ->
    WriterId = ?config(writer_id, Config),
    Tid = ets:new(?FUNCTION_NAME, []),
    {error, wal_down} = ra_log_wal:write(ra_log_wal, WriterId, Tid, 12, 1, "value"),
    ok.

write_many(Config) ->
    Tests = [
             {"10k/8k",   10000,  false, 8000,  1024},
             {"100k/8k",  100000, false, 8000,  1024},
             {"200k/4k",  200000, false, 4000,  1024},
             {"200k/8k",  200000, false, 8000,  1024},
             {"200k/16k", 200000, false, 16000, 1024}
             % {"200k/32k", 200000, false, 32000, 1024},
             % {"200k/64k", 200000, false, 64000, 1024}
            ],
    Results = [begin
                   {Time, Reductions} = test_write_many(Name, Num, Check,
                                                        Batch, Data, Config),
                   io_lib:format("Scenario ~s took ~bms using ~b "
                                 "reductions for ~b writes @ ~b bytes, "
                                 "batch size ~b~n",
                                 [Name, Time, Reductions, Num, Data, Batch])
               end || {Name, Num, Check, Batch, Data} <- Tests],
    ct:pal("~s", [Results]),
    #{dir := Dir0} = ?config(wal_conf, Config),
    ra_lib:recursive_delete(Dir0),
    ok.

test_write_many(Name, NumWrites, ComputeChecksums, BatchSize, DataSize, Config) ->
    Conf0 = #{dir := Dir0} = set_segment_writer(?config(wal_conf, Config),
                                                spawn(fun () -> ok end)),
    Dir = filename:join(Dir0, Name),
    Conf = Conf0#{dir => Dir},
    WriterId = ?config(writer_id, Config),
    {ok, WalPid} = ra_log_wal:start_link(Conf#{compute_checksums => ComputeChecksums,
                                               garbage_collect => true,
                                               max_batch_size => BatchSize}),
    Data = crypto:strong_rand_bytes(DataSize),
    Tid = ets:new(?FUNCTION_NAME, []),
    {ok, _} = ra_log_wal:write(ra_log_wal, WriterId, Tid, 0, 1, Data),
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
                  [{ok, _} = ra_log_wal:write(ra_log_wal, WriterId, Tid, Idx, 1,
                                              {data, Data}) || Idx <- Writes],

                  await_written(WriterId, 1, [{1, NumWrites}], fun ra_lib:ignore/2)
          end),
    timer:sleep(100),
    {_, BinAfter} = erlang:process_info(WalPid, binary),
    {_, GarbAfter} = erlang:process_info(WalPid, garbage_collection),
    {_, MemAfter} = erlang:process_info(WalPid, memory),
    erlang:garbage_collect(WalPid),
    {reductions, RedsAfter} = erlang:process_info(WalPid, reductions),

    ct:pal("Binary:~n~w~n~w", [length(BinBefore), length(BinAfter)]),
    ct:pal("Garbage:~n~w~n~w", [GarbBefore, GarbAfter]),
    ct:pal("Memory:~n~w~n~w", [MemBefore, MemAfter]),

    Reds = RedsAfter - RedsBefore,
    % ct:pal("~b 1024 byte writes took ~p milliseconds~n~n"
    %        "Reductions: ~b",
    %        [NumWrites, Taken / 1000, Reds]),

    % assert memory use after isn't absurdly larger than before
    ?assert(MemAfter < (MemBefore * 3)),

    % assert we aren't regressing on reductions used
    ?assert(Reds < 52023339 * 1.1),
    % stop_profile(Config),
    % ct:pal("Metrics: ~p", [Metrics]),
    proc_lib:stop(WalPid),
    {Taken div 1000, Reds}.

write_many_by_many(Config) ->
    NumWrites = 100,
    NumWriters = 100,
    Conf = set_segment_writer(?config(wal_conf, Config),
                              spawn(fun() -> ok end)),
    % {_UId, _} = WriterId = ?config(writer_id, Config),
    {ok, WalPid} = ra_log_wal:start_link(Conf#{compute_checksums => false}),
    Data = crypto:strong_rand_bytes(1024),
    timer:sleep(5),
    Writes = lists:seq(1, NumWrites),
    {_, GarbBefore} = erlang:process_info(WalPid, garbage_collection),
    {_, MemBefore} = erlang:process_info(WalPid, memory),
    {_, BinBefore} = erlang:process_info(WalPid, binary),
    {reductions, RedsBefore} = erlang:process_info(WalPid, reductions),

    Before = os:system_time(millisecond),
    Self = self(),
    [spawn_link(
       fun () ->
               WId = {term_to_binary(I), self()},
               Tid = ets:new(?FUNCTION_NAME, []),
               put(wid, WId),
               [{ok, _} = ra_log_wal:write(ra_log_wal, WId, Tid, Idx, 1,
                                           {data, Data}) || Idx <- Writes],
               await_written(WId, 1, [{1, NumWrites}], fun ra_lib:ignore/2),
               Self ! wal_write_done
       end) || I <- lists:seq(1, NumWriters)],
    [begin
         receive
             wal_write_done ->
                 ok
         after 200000 ->
                   exit(wal_write_timeout)
         end
     end || _ <- lists:seq(1, NumWriters)],

    After = os:system_time(millisecond),
    timer:sleep(5), % give the gc some time
    {reductions, RedsAfter} = erlang:process_info(WalPid, reductions),
    {_, BinAfter} = erlang:process_info(WalPid, binary),
    {_, GarbAfter} = erlang:process_info(WalPid, garbage_collection),
    {_, MemAfter} = erlang:process_info(WalPid, memory),

    ct:pal("Binary:~n~w~n~w", [length(BinBefore), length(BinAfter)]),
    ct:pal("Garbage:~n~w~n~w", [GarbBefore, GarbAfter]),
    ct:pal("Memory:~n~w~n~w", [MemBefore, MemAfter]),

    Reds = RedsAfter - RedsBefore,
    ct:pal("~b 1024 byte writes took ~p milliseconds~n~n"
           "Reductions: ~b",
           [NumWrites * NumWriters, After - Before, Reds]),

    % assert memory use after isn't absurdly larger than before
    % ?assert(MemAfter < (MemBefore * 2)),

    % % assert we aren't regressing on reductions used
    % ?assert(Reds < 52023339 * 1.1),
    % stop_profile(Config),
    proc_lib:stop(WalPid),
    ok.

out_of_seq_writes(Config) ->
    % INVARIANT: the WAL expects writes for a particular ra server to be done
    % using a contiguous range of integer keys (indexes). If a gap is detected
    % it will notify the write of the missing index and the writer can resend
    % writes from that point
    % the wal will discard all subsequent writes until it receives the missing one
    Conf = set_segment_writer(?config(wal_conf, Config),
                              spawn(fun() -> ok end)),
    {_UId, _} = WriterId = ?config(writer_id, Config),
    {ok, Pid} = ra_log_wal:start_link(Conf),
    Data = crypto:strong_rand_bytes(1024),
    Tid = ets:new(?FUNCTION_NAME, []),
    % write 1-3
    [{ok, _} = ra_log_wal:write(Pid, WriterId, Tid, I, 1, Data)
     || I <- lists:seq(1, 3)],
    await_written(WriterId, 1, [{1, 3}]),
    % then write 5
    {ok, _} = ra_log_wal:write(Pid, WriterId, Tid, 5, 1, Data),
    % ensure an out of sync notification is received
    receive
        {ra_log_event, {resend_write, 4}} -> ok
    after 500 ->
              flush(),
              throw(reset_write_timeout)
    end,
    % try writing 6
    {ok, _} = ra_log_wal:write(Pid, WriterId, Tid, 6, 1, Data),

    % then write 4 and 5
    {ok, _} = ra_log_wal:write(Pid, WriterId, Tid, 4, 1, Data),
    await_written(WriterId, 1, [4]),
    {ok, _} = ra_log_wal:write(Pid, WriterId, Tid, 5, 1, Data),
    await_written(WriterId, 1, [5]),

    % perform another out of sync write
    {ok, _} = ra_log_wal:write(Pid, WriterId, Tid, 7, 1, Data),
    receive
        {ra_log_event, {resend_write, 6}} -> ok
    after 500 ->
              flush(),
              throw(written_timeout)
    end,
    % force a roll over
    ok= ra_log_wal:force_roll_over(ra_log_wal),
    % try writing another
    {ok, _} = ra_log_wal:write(Pid, WriterId, Tid, 8, 1, Data),
    % ensure a written event is _NOT_ received
    % when a roll-over happens after out of sync write
    receive
        {ra_log_event, {written, 1, [8]}} ->
            throw(unexpected_written_event)
    after 500 -> ok
    end,
    % write the missing one
    {ok, _} = ra_log_wal:write(Pid, WriterId, Tid, 6, 1, Data),
    await_written(WriterId, 1, [6]),
    proc_lib:stop(Pid),
    ok.

roll_over_max_size(Config) ->
    Conf = ?config(wal_conf, Config),
    {UId, _} = WriterId = ?config(writer_id, Config),
    NumWrites = 100,
    meck:new(ra_log_segment_writer, [passthrough]),
    meck:expect(ra_log_segment_writer, await,
                fun(_) -> ok end),
    % configure max_wal_size_bytes
    {ok, Pid} = ra_log_wal:start_link(Conf#{max_size_bytes => 1024 * NumWrites}),
    %% DO this to ensure the actual max size bytes config is in place and not
    %% the randomised value
    ra_log_wal:force_roll_over(Pid),
    % write enough entries to trigger roll over
    Data = crypto:strong_rand_bytes(1024),
    Tid  = ets:new(?FUNCTION_NAME, []),
    [begin
         {ok, _} = ra_log_wal:write(ra_log_wal, WriterId, Tid, Idx, 1, Data)
     end || Idx <- lists:seq(1, NumWrites)],
    await_written(UId, 1, [{1, NumWrites}]),

    receive
        {'$gen_cast',
         {mem_tables, #{UId := [{Tid, [{1, 97}]}]}, _Wal}} ->
            %% TODO: do we realy need the hard coded 97 or just assert that
            %% the wal was rolled, not exactly at which point?
            ok
    after 2000 ->
              flush(),
              ct:fail("mem_tables timeout")
    end,
    meck:unload(),
    proc_lib:stop(Pid),
    ok.

roll_over_with_data_larger_than_max_size(Config) ->
    Conf = ?config(wal_conf, Config),
    {UId, _} = WriterId = ?config(writer_id, Config),
    NumWrites = 2,
    meck:new(ra_log_segment_writer, [passthrough]),
    meck:expect(ra_log_segment_writer, await,
                fun(_) -> ok end),
    % configure max_wal_size_bytes
    {ok, Pid} = ra_log_wal:start_link(Conf#{max_size_bytes => 1024 * NumWrites * 10}),
    % write entries each larger than the WAL max size to trigger roll over
    Data = crypto:strong_rand_bytes(64 * 1024),
    Tid = ets:new(?FUNCTION_NAME, []),
    [begin
         {ok, _} = ra_log_wal:write(ra_log_wal, WriterId, Tid, Idx, 1, Data)
     end || Idx <- lists:seq(1, NumWrites)],
    await_written(UId, 1, [{1, NumWrites}]),

    receive
        {'$gen_cast',
         {mem_tables, #{UId := [{Tid, [1]}]}, _Wal}} ->
            ok
    after 2000 ->
              flush(),
              ct:fail("mem_tables timeout")
    end,
    %% NOTE: the second entry will only cause the wal to roll next time an
    %% entry is received
    meck:unload(),
    proc_lib:stop(Pid),
    ok.

roll_over_entry_limit(Config) ->
    Conf = ?config(wal_conf, Config),
    {UId, _} = WriterId = ?config(writer_id, Config),
    NumWrites = 1001,
    meck:new(ra_log_segment_writer, [passthrough]),
    meck:expect(ra_log_segment_writer, await,
                fun(_) -> ok end),
    % configure max_wal_entries
    {ok, Pid} = ra_log_wal:start_link(Conf#{max_entries => 1000}),
    % write enough entries to trigger roll over
    Data = crypto:strong_rand_bytes(1024),
    Tid = ets:new(?FUNCTION_NAME, []),
    [begin
         {ok, _} = ra_log_wal:write(ra_log_wal, WriterId, Tid, Idx, 1, Data)
     end || Idx <- lists:seq(1, NumWrites)],
    await_written(UId, 1, [{1, NumWrites}]),

    receive
        {'$gen_cast',
         {mem_tables, #{UId := [{Tid, [{1, 1000}]}]}, _Wal}} ->
            %% 1000 is the last entry before the limit was reached
            ok
    after 2000 ->
              flush(),
              ct:fail("mem_tables timeout")
    end,

    meck:unload(),
    proc_lib:stop(Pid),
    ok.


sys_get_status(Config) ->
    Conf = set_segment_writer(?config(wal_conf, Config),
                              spawn(fun () -> ok end)),
    {_UId, _} = ?config(writer_id, Config),
    {ok, Pid} = ra_log_wal:start_link(Conf),
    {_, _, _, [_, _, _, _, [_, _ , S]]} = sys:get_status(ra_log_wal),
    ?assert(is_map(S)),

    ?assertMatch(#{sync_method := _,
                   compute_checksums := _,
                   writers := _,
                   filename := _,
                   current_size := _,
                   max_size_bytes := _,
                   counters := _ }, S),
    proc_lib:stop(Pid),
    ok.

recover(Config) ->
    ok = logger:set_primary_config(level, all),
    Conf = ?config(wal_conf, Config),
    {UId, _} = WriterId = ?config(writer_id, Config),
    Data = <<42:256/unit:8>>,
    meck:new(ra_log_segment_writer, [passthrough]),
    meck:expect(ra_log_segment_writer, await, fun(_) -> ok end),
    Tid = ets:new(?FUNCTION_NAME, []),
    {ok, Pid} = ra_log_wal:start_link(Conf),
    ?assertMatch({ok, undefined}, ra_log_wal:last_writer_seq(Pid, UId)),
    %% write some in one term
    [{ok, _} = ra_log_wal:write(Pid, WriterId, Tid, Idx, 1, Data)
     || Idx <- lists:seq(1, 100)],
    _ = await_written(WriterId, 1, [{1, 100}]),

    ra_log_wal:force_roll_over(ra_log_wal),

    %% then some more in another
    [{ok, _} = ra_log_wal:write(Pid, WriterId, Tid, Idx, 2, Data)
     || Idx <- lists:seq(101, 200)],
    _ = await_written(WriterId, 2, [{101, 200}]),

    flush(),
    ok = proc_lib:stop(ra_log_wal),
    {ok, Pid2} = ra_log_wal:start_link(Conf),
    {ok, Mt} = ra_log_ets:mem_table_please(?config(names, Config), UId),
    %% check the last writer seq is recovered ok
    ?assertMatch({ok, 200}, ra_log_wal:last_writer_seq(Pid2, UId)),

    ?assertMatch(#{size := 200}, ra_mt:info(Mt)),
    MtTid = ra_mt:tid(Mt),
    receive
        {'$gen_cast',
         {mem_tables, #{UId := [{MtTid, [{1, 100}]}]}, _Wal}} ->
            ok
    after 2000 ->
              flush(),
              ct:fail("new_mem_tables_timeout")
    end,
    receive
        {'$gen_cast',
         {mem_tables, #{UId := [{MtTid, [{101, 200}]}]}, _}} ->
            ok
    after 2000 ->
              flush(),
              ct:fail("new_mem_tables_timeout")
    end,

    %% try an out of sequence write to check the tracking state was recovered
    % ensure an out of sync notification is received
    {ok, _} = ra_log_wal:write(ra_log_wal, WriterId, Tid, 220, 2, Data),
    receive
        {ra_log_event, {resend_write, 201}} -> ok
    after 500 ->
              flush(),
              throw(reset_write_timeout)
    end,

    meck:unload(),
    proc_lib:stop(Pid2),
    ok.

recover_with_snapshot_index(Config) ->
    ok = logger:set_primary_config(level, all),
    Conf = ?config(wal_conf, Config),
    {UId, _} = WriterId = ?config(writer_id, Config),
    Data = <<42:256/unit:8>>,
    meck:new(ra_log_segment_writer, [passthrough]),
    meck:expect(ra_log_segment_writer, await, fun(_) -> ok end),
    Tid = ets:new(?FUNCTION_NAME, []),
    {ok, Pid} = ra_log_wal:start_link(Conf),
    %% write some in one term
    [{ok, _} = ra_log_wal:write(Pid, WriterId, Tid, Idx, 1, Data)
     || Idx <- lists:seq(1, 100)],
    _ = await_written(WriterId, 1, [{1, 100}]),
    flush(),
    ok = proc_lib:stop(ra_log_wal),


    ok = ra_log_snapshot_state:insert(ra_log_snapshot_state, UId, 50, 51, []),
    {ok, Pid2} = ra_log_wal:start_link(Conf),
    {ok, Mt} = ra_log_ets:mem_table_please(?config(names, Config), UId),

    ?assertMatch(#{size := 50}, ra_mt:info(Mt)),
    MtTid = ra_mt:tid(Mt),
    receive
        {'$gen_cast',
         {mem_tables, #{UId := [{MtTid, [{51, 100}]}]}, _Wal}} ->
            ok
    after 2000 ->
              flush(),
              ct:fail("new_mem_tables_timeout")
    end,

    meck:unload(),
    proc_lib:stop(Pid2),
    ok.

recover_overwrite(Config) ->
    ok = logger:set_primary_config(level, all),
    Conf = ?config(wal_conf, Config),
    {UId, _} = WriterId = ?config(writer_id, Config),
    Data = <<42:256/unit:8>>,
    meck:new(ra_log_segment_writer, [passthrough]),
    meck:expect(ra_log_segment_writer, await, fun(_) -> ok end),
    Tid = ets:new(?FUNCTION_NAME, []),
    Tid2 = ets:new(?FUNCTION_NAME, []),
    {ok, Pid} = ra_log_wal:start_link(Conf),
    %% write some in one term
    [{ok, _} = ra_log_wal:write(Pid, WriterId, Tid, Idx, 1, Data)
     || Idx <- lists:seq(1, 10)],
    _ = await_written(WriterId, 1, [{1, 10}]),

    %% then some more in another
    [{ok, _} = ra_log_wal:write(Pid, WriterId, Tid2, Idx, 2, Data)
     || Idx <- lists:seq(5, 20)],
    _ = await_written(WriterId, 2, [{5, 20}]),

    flush(),
    ok = proc_lib:stop(ra_log_wal),
    {ok, Pid2} = ra_log_wal:start_link(Conf),
    {ok, Mt} = ra_log_ets:mem_table_please(?config(names, Config), UId),

    ?assertMatch({1, 20}, ra_mt:range(Mt)),
    MtTid = ra_mt:tid(Mt),
    PrevMtTid = ra_mt:tid(ra_mt:prev(Mt)),
    receive
        {'$gen_cast',
         {mem_tables, #{UId := [{MtTid, [{5, 20}]},
                                {PrevMtTid, [{1, 10}]}
                                ]}, _Wal}} ->
            ok
    after 2000 ->
              flush(),
              ct:fail("new_mem_tables_timeout")
    end,
    meck:unload(),
    proc_lib:stop(Pid2),
    ok.

recover_overwrite_rollover(Config) ->
    ok = logger:set_primary_config(level, all),
    Conf = ?config(wal_conf, Config),
    {UId, _} = WriterId = ?config(writer_id, Config),
    Data = <<42:256/unit:8>>,
    meck:new(ra_log_segment_writer, [passthrough]),
    meck:expect(ra_log_segment_writer, await, fun(_) -> ok end),
    Tid = ets:new(?FUNCTION_NAME, []),
    Tid2 = ets:new(?FUNCTION_NAME, []),
    {ok, Pid} = ra_log_wal:start_link(Conf),
    %% write some in one term
    [{ok, _} = ra_log_wal:write(Pid, WriterId, Tid, Idx, 1, Data)
     || Idx <- lists:seq(1, 10)],
    _ = await_written(WriterId, 1, [{1, 10}]),

    ra_log_wal:force_roll_over(ra_log_wal),

    %% then some more in another
    [{ok, _} = ra_log_wal:write(Pid, WriterId, Tid2, Idx, 2, Data)
     || Idx <- lists:seq(5, 20)],
    _ = await_written(WriterId, 2, [{5, 20}]),

    flush(),
    ok = proc_lib:stop(ra_log_wal),
    {ok, Pid2} = ra_log_wal:start_link(Conf),
    {ok, Mt} = ra_log_ets:mem_table_please(?config(names, Config), UId),
    ?assertMatch({1, 20}, ra_mt:range(Mt)),
    MtTid = ra_mt:tid(Mt),
    PrevMtTid = ra_mt:tid(ra_mt:prev(Mt)),
    receive
        {'$gen_cast',
         {mem_tables, #{UId := [{PrevMtTid, [{1, 10}]}]}, _Wal}} ->
            ok
    after 2000 ->
              flush(),
              ct:fail("new_mem_tables_timeout")
    end,
    receive
        {'$gen_cast',
         {mem_tables, #{UId := [{MtTid, [{5, 20}]}]}, _}} ->
            ok
    after 2000 ->
              flush(),
              ct:fail("new_mem_tables_timeout")
    end,

    meck:unload(),
    proc_lib:stop(Pid2),
    ok.

recover_existing_mem_table(Config) ->
    ok = logger:set_primary_config(level, all),
    Conf = ?config(wal_conf, Config),
    {UId, _} = WriterId = ?config(writer_id, Config),
    Data = <<42:256/unit:8>>,
    meck:new(ra_log_segment_writer, [passthrough]),
    meck:expect(ra_log_segment_writer, await, fun(_) -> ok end),
    {ok, Pid} = ra_log_wal:start_link(Conf),
    {ok, Mt0} = ra_log_ets:mem_table_please(?config(names, Config), UId),
    Tid = ra_mt:tid(Mt0),
    %% write some in one term
    Mt1 = lists:foldl(
            fun (Idx, Acc0) ->
                    {ok, Acc} = ra_mt:insert({Idx, 1, Data}, Acc0),
                    {ok, _} = ra_log_wal:write(Pid, WriterId, Tid, Idx, 1, Data),
                    Acc
            end, Mt0, lists:seq(1, 100)),
    _ = await_written(WriterId, 1, [{1, 100}]),
    flush(),
    ok = proc_lib:stop(ra_log_wal),
    {ok, Pid2} = ra_log_wal:start_link(Conf),
    {ok, Mt} = ra_log_ets:mem_table_please(?config(names, Config), UId),
    ?assertEqual(Mt1, Mt),
    ?assertMatch({1, 100}, ra_mt:range(Mt)),
    receive
        {'$gen_cast',
         {mem_tables, #{UId := [{Tid, [{1, 100}]}]}, _}} ->
            ok
    after 2000 ->
              flush(),
              ct:fail("new_mem_tables_timeout")
    end,

    %% try an out of sequence write to check the tracking state was recovered
    % ensure an out of sync notification is received
    {ok, _} = ra_log_wal:write(ra_log_wal, WriterId, Tid, 220, 2, Data),
    receive
        {ra_log_event, {resend_write, 101}} -> ok
    after 500 ->
              flush(),
              throw(reset_write_timeout)
    end,
    meck:unload(),
    proc_lib:stop(Pid2),
    ok.

recover_existing_mem_table_with_deletes(Config) ->
    %% tests dirty recovery with partial mem table
    ok = logger:set_primary_config(level, all),
    Conf = ?config(wal_conf, Config),
    {UId, _} = WriterId = ?config(writer_id, Config),
    Data = <<42:256/unit:8>>,
    meck:new(ra_log_segment_writer, [passthrough]),
    meck:expect(ra_log_segment_writer, await, fun(_) -> ok end),
    {ok, Pid} = ra_log_wal:start_link(Conf),
    {ok, Mt0} = ra_log_ets:mem_table_please(?config(names, Config), UId),
    Tid = ra_mt:tid(Mt0),
    %% write some in one term
    Mt1 = lists:foldl(
            fun (Idx, Acc0) ->
                    {ok, Acc} = ra_mt:insert({Idx, 1, Data}, Acc0),
                    {ok, _} = ra_log_wal:write(Pid, WriterId, Tid, Idx, 1, Data),
                    Acc
            end, Mt0, lists:seq(1, 100)),
    _ = await_written(WriterId, 1, [{1, 100}]),
    %% the delete comes in before recovery
    {[Spec], _Mt2} = ra_mt:set_first(50, Mt1),
    ?assert(0 < ra_mt:delete(Spec)),
    flush(),
    ok = proc_lib:stop(ra_log_wal),
    {ok, _Pid2} = ra_log_wal:start_link(Conf),
    {ok, Mt} = ra_log_ets:mem_table_please(?config(names, Config), UId),
    ?assertMatch({50, 100}, ra_mt:range(Mt)),
    receive
        {'$gen_cast',
         {mem_tables, #{UId := [{Tid, [{50, 100}]}]}, _}} ->
            ok
    after 2000 ->
              flush(),
              ct:fail("new_mem_tables_timeout")
    end,
    meck:unload(ra_log_segment_writer),
    ok = proc_lib:stop(ra_log_wal),
    ok.

recover_existing_mem_table_overwrite(Config) ->
    ok = logger:set_primary_config(level, all),
    Conf = ?config(wal_conf, Config),
    {UId, _} = WriterId = ?config(writer_id, Config),
    Data = <<42:256/unit:8>>,
    meck:new(ra_log_segment_writer, [passthrough]),
    meck:expect(ra_log_segment_writer, await, fun(_) -> ok end),
    {ok, Pid} = ra_log_wal:start_link(Conf),
    {ok, Mt0} = ra_log_ets:mem_table_please(?config(names, Config), UId),
    %% write some in one term
    Mt1 = lists:foldl(
            fun (Idx, Acc0) ->
                    {ok, Acc} = ra_mt:insert({Idx, 1, Data}, Acc0),
                    {ok, _} = ra_log_wal:write(Pid, WriterId,
                                               ra_mt:tid(Acc0), Idx, 1, Data),
                    Acc
            end, Mt0, lists:seq(1, 100)),
    _ = await_written(WriterId, 1, [{1, 100}]),
    Mt2 = lists:foldl(
           fun (Idx, Acc0) ->
                   {ok, Acc} = ra_mt:insert({Idx, 2, Data}, Acc0),
                   {ok, _} = ra_log_wal:write(Pid, WriterId,
                                              ra_mt:tid(Acc0), Idx, 2, Data),
                   Acc
           end, element(2,
                        ra_log_ets:new_mem_table_please(?config(names, Config),
                                                        UId, Mt1)),
           lists:seq(50, 200)),
    _ = await_written(WriterId, 2, [{50, 200}]),
    flush(),
    ok = proc_lib:stop(ra_log_wal),
    {ok, Pid2} = ra_log_wal:start_link(Conf),
    {ok, Mt} = ra_log_ets:mem_table_please(?config(names, Config), UId),
    ?assertEqual(Mt2, Mt),
    ?assertMatch({1, 200}, ra_mt:range(Mt)),
    ct:pal("Mt ~p", [Mt]),
    Tid = ra_mt:tid(Mt1),
    Tid2 = ra_mt:tid(Mt2),
    receive
        {'$gen_cast',
         {mem_tables, #{UId := [{Tid2, [{50, 200}]},
                                {Tid, [{1, 100}]}]}, _}} ->
            ok
    after 2000 ->
              flush(),
              ct:fail("mem_tables_timeout")
    end,

    meck:unload(),
    proc_lib:stop(Pid2),
    ok.

recover_implicit_truncate(Config) ->
    meck:new(ra_log_segment_writer, [passthrough]),
    meck:expect(ra_log_segment_writer, await, fun(_) -> ok end),
    Conf = ?config(wal_conf, Config),
    {UId, _} = WriterId = ?config(writer_id, Config),
    {ok, Pid} = ra_log_wal:start_link(Conf),
    Data = <<"data">>,
    Tid = ets:new(?FUNCTION_NAME, []),
    [{ok, _} = ra_log_wal:write(Pid, WriterId, Tid, I, 1, Data)
     || I <- lists:seq(1, 3)],
    await_written(WriterId, 1, [{1, 3}]),
    % snapshot idx updated and we follow that with the next index after the
    % snapshot.
    % before we had to detect this and send a special {truncate, append request
    % but this is not necessary anymore
    ok = ra_log_snapshot_state:insert(ra_log_snapshot_state, UId, 5, 6, []),
    [{ok, _} = ra_log_wal:write(Pid, WriterId, Tid, I, 1, Data)
     || I <- lists:seq(6, 7)],
    await_written(WriterId, 1, [{6, 7}]),
    flush(),
    ok = proc_lib:stop(Pid),

    %% this could happen potentially in some edge cases??
    ra_log_snapshot_state:delete(ra_log_snapshot_state, UId),
    {ok, Pid2} = ra_log_wal:start_link(Conf),
    {ok, Mt} = ra_log_ets:mem_table_please(?config(names, Config), UId),

    ?assertMatch(#{size := 2}, ra_mt:info(Mt)),
    MtTid = ra_mt:tid(Mt),
    receive
        {'$gen_cast',
         {mem_tables, #{UId := [{MtTid, [7, 6]}]}, _Wal}} ->
            ok
    after 2000 ->
              flush(),
              ct:fail("new_mem_tables_timeout")
    end,

    meck:unload(),
    proc_lib:stop(Pid2),
    ok.

recover_delete_uid(Config) ->
    meck:new(ra_log_segment_writer, [passthrough]),
    meck:expect(ra_log_segment_writer, await, fun(_) -> ok end),
    Conf = ?config(wal_conf, Config),
    {UId, _} = WriterId = ?config(writer_id, Config),
    {ok, Pid} = ra_log_wal:start_link(Conf),
    {UId2, _} = WriterId2 = {<<"DELETEDUID">>, self()},
    Data = <<"data">>,
    Tid = ets:new(?FUNCTION_NAME, []),
    [{ok, _} = ra_log_wal:write(Pid, WriterId, Tid, I, 1, Data)
     || I <- lists:seq(1, 3)],
    await_written(WriterId, 1, [{1, 3}]),

    Tid2 = ets:new(?FUNCTION_NAME, []),
    [{ok, _} = ra_log_wal:write(Pid, WriterId2, Tid2, I, 9, Data)
     || I <- lists:seq(5, 9)],
    await_written(WriterId, 9, [{5, 9}]),
    _ = ra_directory:unregister_name(default, UId2),
    flush(),
    ok = proc_lib:stop(Pid),

    {ok, Pid2} = ra_log_wal:start_link(Conf),

    {ok, Mt} = ra_log_ets:mem_table_please(?config(names, Config), UId),
    ?assertMatch(#{size := 3}, ra_mt:info(Mt)),
    ?assertMatch({1, 3}, ra_mt:range(Mt)),
    MtTid = ra_mt:tid(Mt),
    receive
        {'$gen_cast',
         {mem_tables, #{UId := [{MtTid, [{1, 3}]}]} = Tables, _Wal}}
          when not is_map_key(UId2, Tables) ->
            ok
    after 2000 ->
              flush(),
              ct:fail("new_mem_tables_timeout")
    end,
    flush(),

    meck:unload(),
    proc_lib:stop(Pid2),
    ok.

recover_empty(Config) ->
    ok = logger:set_primary_config(level, all),
    Conf = ?config(wal_conf, Config),
    meck:new(ra_log_segment_writer, [passthrough]),
    meck:expect(ra_log_segment_writer, await,
                fun(_) -> ok end),
    {ok, _Pid} = ra_log_wal:start_link(Conf),
    proc_lib:stop(ra_log_wal),
    {ok, Pid} = ra_log_wal:start_link(Conf),
    proc_lib:stop(Pid),
    meck:unload(),
    ok.

recover_with_partial_last_entry(Config) ->
    ok = logger:set_primary_config(level, all),
    #{dir := Dir} = Conf = ?config(wal_conf, Config),
    {UId, _} = WriterId = ?config(writer_id, Config),
    Data = crypto:strong_rand_bytes(1000),
    meck:new(ra_log_segment_writer, [passthrough]),
    meck:expect(ra_log_segment_writer, await, fun(_) -> ok end),
    {ok, _Wal} = ra_log_wal:start_link(Conf),
    Tid = ets:new(?FUNCTION_NAME, []),
    [{ok, _} = ra_log_wal:write(ra_log_wal, WriterId, Tid, Idx, 1, Data)
     || Idx <- lists:seq(1, 100)],
    _ = await_written(WriterId, 1, [{1, 100}]),
    empty_mailbox(),
    ok = proc_lib:stop(ra_log_wal),

    [WalFile] = filelib:wildcard(filename:join(Dir, "*.wal")),

    %% overwrite last few bytes of the file with 0s
    {ok, Fd} = file:open(WalFile, [raw, binary, read, write]),
    {ok, _Pos} = file:position(Fd, {eof, -10}),
    file:truncate(Fd),
    file:close(Fd),

    {ok, Pid} = ra_log_wal:start_link(Conf),
    ?assert(erlang:is_process_alive(Pid)),
    {ok, Mt} = ra_log_ets:mem_table_please(?config(names, Config), UId),
    ?assertMatch({1, 99}, ra_mt:range(Mt)),
    MtTid = ra_mt:tid(Mt),
    receive
        {'$gen_cast',
         {mem_tables, #{UId := [{MtTid, [{1, 99}]}]}, _File}} ->
            ok
    after 5000 ->
              flush(),
              ct:fail("receiving mem tables timed out")
    end,
    ok = proc_lib:stop(ra_log_wal),
    meck:unload(),
    flush(),
    ok.

recover_with_last_entry_corruption(Config) ->
    ok = logger:set_primary_config(level, all),
    #{dir := Dir} = Conf0 = ?config(wal_conf, Config),
    WriterId = ?config(writer_id, Config),
    Conf = set_segment_writer(Conf0, spawn(fun () -> ok end)),
    Data = crypto:strong_rand_bytes(1000),
    meck:new(ra_log_segment_writer, [passthrough]),
    meck:expect(ra_log_segment_writer, await, fun(_) -> ok end),
    Tid = ets:new(?FUNCTION_NAME, []),
    {ok, Pid} = ra_log_wal:start_link(Conf),
    [{ok, _} = ra_log_wal:write(Pid, WriterId, Tid, Idx, 1, Data)
     || Idx <- lists:seq(1, 100)],
    _ = await_written(WriterId, 1, [{1, 100}]),
    flush(),
    ok = proc_lib:stop(ra_log_wal),

    [WalFile] = filelib:wildcard(filename:join(Dir, "*.wal")),

    %% overwrite last few bytes of the file with 0s
    {ok, Fd} = file:open(WalFile, [raw, binary, read, write]),
    {ok, _Pos} = file:position(Fd, {eof, -10}),
    ok = file:write(Fd, <<0,0,0,0,0,0,0,0,0,0>>),
    file:close(Fd),

    {ok, Pid2} = ra_log_wal:start_link(Conf),
    ?assert(erlang:is_process_alive(Pid2)),
    ok = proc_lib:stop(ra_log_wal),
    meck:unload(),
    ok.

recover_with_last_entry_corruption_pre_allocate(Config) ->
    ok = logger:set_primary_config(level, all),
    #{dir := Dir} = Conf0 = ?config(wal_conf, Config),
    WriterId = ?config(writer_id, Config),
    Conf = set_segment_writer(Conf0, spawn(fun () -> ok end)),
    Data = crypto:strong_rand_bytes(1000),
    meck:new(ra_log_segment_writer, [passthrough]),
    meck:expect(ra_log_segment_writer, await, fun(_) -> ok end),
    {ok, Pid} = ra_log_wal:start_link(Conf),
    Tid = ets:new(?FUNCTION_NAME, []),
    [{ok, _} = ra_log_wal:write(Pid, WriterId, Tid, Idx, 1, Data)
     || Idx <- lists:seq(1, 100)],
    _ = await_written(WriterId, 1, [{1, 100}]),
    empty_mailbox(),
    ok = proc_lib:stop(ra_log_wal),

    [WalFile] = filelib:wildcard(filename:join(Dir, "*.wal")),

    %% overwrite last few bytes of the file with 0s
    {ok, Fd} = file:open(WalFile, [raw, binary, read, write]),
    %% TODO: if the internal WAL format changes this will be wrong
    _ = file:position(Fd, 103331),
    ok = file:write(Fd, <<0,0,0,0,0,0,0,0,0,0>>),
    file:close(Fd),

    {ok, Pid2} = ra_log_wal:start_link(Conf),
    ?assert(erlang:is_process_alive(Pid2)),
    ok = proc_lib:stop(ra_log_wal),
    meck:unload(),
    ok.

checksum_failure_in_middle_of_file_should_fail(Config) ->
    process_flag(trap_exit, true),
    ok = logger:set_primary_config(level, all),
    #{dir := Dir} = Conf0 = ?config(wal_conf, Config),
    WriterId = ?config(writer_id, Config),
    Conf = set_segment_writer(Conf0, spawn(fun () -> ok end)),
    Data = crypto:strong_rand_bytes(1000),
    meck:new(ra_log_segment_writer, [passthrough]),
    meck:expect(ra_log_segment_writer, await, fun(_) -> ok end),
    {ok, Pid} = ra_log_wal:start_link(Conf),
    Tid = ets:new(?FUNCTION_NAME, []),
    [{ok, _} = ra_log_wal:write(Pid, WriterId, Tid, Idx, 1, Data)
     || Idx <- lists:seq(1, 100)],
    _ = await_written(WriterId, 1, [{1, 100}]),
    empty_mailbox(),
    ok = proc_lib:stop(ra_log_wal),

    [WalFile] = filelib:wildcard(filename:join(Dir, "*.wal")),

    %% overwrite last few bytes of the file with 0s
    {ok, Fd} = file:open(WalFile, [raw, binary, read, write]),
    {ok, _Pos} = file:position(Fd, 1000),
    ok = file:write(Fd, <<0,0,0,0,0,0,0,0,0,0>>),
    file:close(Fd),

    {error, wal_checksum_validation_failure} = ra_log_wal:start_link(Conf),
    empty_mailbox(),
    meck:unload(),
    ok.

empty_mailbox() ->
    receive
        _ ->
            empty_mailbox()
    after 100 ->
              ok
    end.

await_written(Id, Term, Written) when is_list(Written) ->
    await_written(Id, Term, Written, fun ct:pal/2).

await_written(Id, Term, Expected, LogFun) ->
    receive
        {ra_log_event, {written, Term, Written}}
          when Written == Expected ->
            %% consumed all of expected
            LogFun("~s, got all ~b ~w", [?FUNCTION_NAME, Term, Written]),
            ok;
        {ra_log_event, {written, Term, Written}} ->
            LogFun("~s, got ~b ~w", [?FUNCTION_NAME, Term, Written]),
            case ra_seq:subtract(Expected, Written) of
                [] ->
                    %% we're done
                    ok;
                Rem ->
                    await_written(Id, Term, Rem, LogFun)
            end;
        {ra_log_event, {written, OthTerm, Written}}
          when OthTerm =/= Term ->
            %% different term
            LogFun("~s, got oth term ~b ~w", [?FUNCTION_NAME, Term, Written]),
            await_written(Id, Term, Expected, LogFun)
    after 5000 ->
              flush(),
              throw({written_timeout, Expected})
    end.

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

flush() ->
    receive Msg ->
                ct:pal("flush: ~p", [Msg]),
                flush()
    after 0 -> ok
    end.

%% erlang:suspend_process/1 can be a bit racy it seems. Some tests that use it
%% occasionally failed due to an error when the function thinks the pid is not
%% alive (although it isn't). This helper works around that
suspend_process(Pid) ->
    try erlang:suspend_process(Pid) of
        true ->
            true
    catch error:internal_error:Stack ->
              erlang:resume_process(Pid),
              case erlang:is_process_alive(Pid) of
                  true ->
                      ct:pal("~s retry", [?FUNCTION_NAME]),
                      timer:sleep(32),
                      erlang:suspend_process(Pid);
                  false ->
                      erlang:raise(error, internal_error, Stack)
              end
    end.

set_segment_writer(#{names := Names} = Conf, Writer) ->
    Conf#{names => maps:put(segment_writer, Writer, Names)}.
