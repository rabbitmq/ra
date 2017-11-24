-module(ra_log_wal_SUITE).
-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

all() ->
    [
     {group, tests}
    ].


all_tests() ->
    [
     basic_log_writes,
     write_many,
     roll_over,
     recover,
     out_of_seq_writes
    ].

groups() ->
    [
     {tests, [], all_tests()}
    ].

init_per_group(tests, Config) ->
    application:ensure_all_started(sasl),
    application:ensure_all_started(lg),
    Config.

end_per_group(tests, Config) ->
    Config.

init_per_testcase(TestCase, Config) ->
    PrivDir = ?config(priv_dir, Config),
    Dir = filename:join(PrivDir, TestCase),
    register(TestCase, self()),
    [{test_case, TestCase}, {wal_dir, Dir} | Config].


basic_log_writes(Config) ->
    Dir = ?config(wal_dir, Config),
    {ok, _Pid} = ra_log_wal:start_link(#{dir => Dir}, []),
    {registered_name, Self} = erlang:process_info(self(), registered_name),
    ok = ra_log_wal:write(Self, ra_log_wal, 12, 1, "value"),
    {12, 1, "value"} = await_written(Self, {12, 1}),
    ok = ra_log_wal:write(Self, ra_log_wal, 13, 1, "value2"),
    {13, 1, "value2"} = await_written(Self, {13, 1}),
    % previous log value is still there
    {12, 1, "value"} = ra_log_wal:mem_tbl_read(Self, 12),
    undefined = ra_log_wal:mem_tbl_read(Self, 14),
    ra_lib:dump(ets:tab2list(ra_log_open_mem_tables)),
    ok.

write_many(Config) ->
    NumWrites = 10000,
    Dir = ?config(wal_dir, Config),
    Modes = [{delayed_write, 1024 * 1024 * 4, 60 * 1000}],
    % Modes = [],
    {ok, _Pid} = ra_log_wal:start_link(#{dir => Dir,
                                         additional_wal_file_modes => Modes}, []),
    {registered_name, Self} = erlang:process_info(self(), registered_name),
    Data = crypto:strong_rand_bytes(1024),
    ok = ra_log_wal:write(Self, ra_log_wal, 0, 1, Data),
    timer:sleep(5),
    % start_profile(Config, [ra_log_wal, ets, file, os]),
    {Taken, _} =
        timer:tc(
          fun () ->
                  [begin
                       ok = ra_log_wal:write(Self, ra_log_wal, Idx, 1, Data)
                   end || Idx <- lists:seq(1, NumWrites)],
                  await_written(Self, {NumWrites, 1})
          end),

    ct:pal("~b 1024 byte writes took ~p milliseconds~nFile modes: ~p~n",
           [NumWrites, Taken / 1000, Modes]),
    % stop_profile(Config),
    Metrics = [M || {_, V} = M <- lists:sort(ets:tab2list(ra_log_wal_metrics)),
                    V =/= undefined],
    ct:pal("Metrics: ~p~n", [Metrics]),
    ok.


out_of_seq_writes(Config) ->
    % INVARIANT: the WAL expects writes for a particular ra node to be done
    % using a contiguous range of integer keys (indexes). If a gap is detected
    % it will notify the write of the missing index and the writer can resend
    % writes from that point
    % the wal will discard all subsequent writes until it receives the missing one
    Dir = ?config(wal_dir, Config),
    Modes = [{delayed_write, 1024 * 1024 * 4, 60 * 1000}],
    % Modes = [],
    {ok, _Pid} = ra_log_wal:start_link(#{dir => Dir,
                                         additional_wal_file_modes => Modes}, []),
    {registered_name, Self} = erlang:process_info(self(), registered_name),
    Data = crypto:strong_rand_bytes(1024),
    % write 1-3
    [ok = ra_log_wal:write(Self, ra_log_wal, I, 1, Data)
     || I <- lists:seq(1, 3)],
    await_written(Self, {3, 1}),
    % then write 5
    ok = ra_log_wal:write(Self, ra_log_wal, 5, 1, Data),
    % ensure an out of sync notification is received
    receive
        {ra_log_event, {resend_write, 4}} -> ok
    after 500 ->
              throw(reset_write_timeout)
    end,
    % try writing 6
    ok = ra_log_wal:write(Self, ra_log_wal, 6, 1, Data),

    % then write 4 and 5
    ok = ra_log_wal:write(Self, ra_log_wal, 4, 1, Data),
    ok = ra_log_wal:write(Self, ra_log_wal, 5, 1, Data),
    await_written(Self, {5, 1}),

    % perform another out of sync write
    ok = ra_log_wal:write(Self, ra_log_wal, 7, 1, Data),
    receive
        {ra_log_event, {resend_write, 6}} -> ok
    after 500 ->
              throw(written_timeout)
    end,
    % force a roll over
    ok = ra_log_wal:force_roll_over(ra_log_wal),
    % try writing another
    ok = ra_log_wal:write(Self, ra_log_wal, 8, 1, Data),
    % ensure a written event is _NOT_ received
    % when a roll-over happens after out of sync write
    receive
        {ra_log_event, {written, {8, 1}}} ->
            throw(unexpected_written_event)
    after 500 -> ok
    end,
    % write the missing one
    ok = ra_log_wal:write(Self, ra_log_wal, 6, 1, Data),
    await_written(Self, {6, 1}),

    ok.

roll_over(Config) ->
    Dir = ?config(wal_dir, Config),
    {registered_name, Self} = erlang:process_info(self(), registered_name),
    NumWrites = 5,
    % configure max_wal_size_bytes
    {ok, _Pid} = ra_log_wal:start_link(#{dir => Dir,
                                         max_wal_size_bytes => 1024 * NumWrites,
                                         segment_writer => Self}, []),
    % write enough entries to trigger roll over
    Data = crypto:strong_rand_bytes(1024),
    [begin
         ok = ra_log_wal:write(Self, ra_log_wal, Idx, 1, Data)
     end || Idx <- lists:seq(1, NumWrites)],
    % wait for writes
    receive {ra_log_event, {written, {NumWrites, 1}}} -> ok
    after 5000 -> throw(written_timeout)
    end,

    % validate we receive the new mem tables notifications as if we were
    % the writer process
    receive
        {'$gen_cast', {mem_tables, [{Self, _Fst, _Lst, Tid}], _}} ->
            [{Self, 5, 5, CurrentTid}] = ets:lookup(ra_log_open_mem_tables, Self),
            % the current tid is not the same as the rolled over one
            ?assert(Tid =/= CurrentTid),
            % ensure closed mem tables contain the previous mem_table
            [{Self, _, 1, 4, Tid}] = ets:lookup(ra_log_closed_mem_tables, Self)
    after 2000 ->
              throw(new_mem_tables_timeout)
    end,

    % TODO: validate we can read first and last written
    ?assert(undefined =/= ra_log_wal:mem_tbl_read(Self, 1)),
    ?assert(undefined =/= ra_log_wal:mem_tbl_read(Self, 5)),
    ok.

recover(Config) ->
    % open wal and write a few entreis
    % close wal + delete mem_tables
    % re-open wal and validate mem_tables are re-created
    Dir = ?config(wal_dir, Config),
    {registered_name, Self} = erlang:process_info(self(), registered_name),
    Data = <<42:256/unit:8>>,
    {ok, _} = ra_log_wal:start_link(#{dir => Dir, segment_writer => Self}, []),
    [ok = ra_log_wal:write(Self, ra_log_wal, Idx, 1, Data)
     || Idx <- lists:seq(1, 100)],
    ra_log_wal:force_roll_over(ra_log_wal),
    [ok = ra_log_wal:write(Self, ra_log_wal, Idx, 2, Data)
     || Idx <- lists:seq(101, 200)],
    empty_mailbox(),
    proc_lib:stop(ra_log_wal),
    {ok, _} = ra_log_wal:start_link(#{dir => Dir, segment_writer => Self}, []),
    % how can we better wait for recovery to finish?
    timer:sleep(1000),

    % there should be no open mem tables after recovery as we treat any found
    % wal files as complete
    [] = ets:lookup(ra_log_open_mem_tables, Self),
    [{Self, _, 1, 100, OpnMTTid1}, {Self, _, 101, 200, OpnMTTid2}] =
        lists:sort(ets:lookup(ra_log_closed_mem_tables, Self)),
    100 = ets:info(OpnMTTid1, size),
    100 = ets:info(OpnMTTid2, size),
    % check that both mem_tables notifications are received by the segment writer
    receive
        {'$gen_cast', {mem_tables, [{Self, 1, 100, _}], _}} -> ok
    after 2000 ->
              throw(new_mem_tables_timeout)
    end,
    receive
        {'$gen_cast', {mem_tables, [{Self, 101, 200, _}], _}} -> ok
    after 2000 ->
              throw(new_mem_tables_timeout)
    end,

    ok.

empty_mailbox() ->
    receive
        _ ->
            empty_mailbox()
    after 100 ->
              ok
    end.

await_written(Id, IdxTerm) ->
    receive
        {ra_log_event, {written, {Idx, _} = IdxTerm}} ->
            ra_log_wal:mem_tbl_read(Id, Idx)
    after 5000 ->
              throw(written_timeout)
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
    Dir = ?config(priv_dir, Config),
    Name = filename:join([Dir, "lg_" ++ atom_to_list(Case)]),
    timer:sleep(2000),
    lg_callgrind:profile_many(Name ++ ".gz.*", Name ++ ".out",#{running => false}),
    lg_callgrind:profile_many("lg_write_many.gz.*", "lg_write_many.out",#{running => false}),
    ok.
