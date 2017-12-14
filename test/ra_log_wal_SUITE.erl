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
     write_to_unavailable_wal_returns_error,
     write_many,
     overwrite,
     truncate_write,
     out_of_seq_writes,
     roll_over,
     recover,
     recover_truncated_write
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
    _ = ra_log_file_ets:start_link(),
    register(TestCase, self()),
    [{test_case, TestCase}, {wal_dir, Dir} | Config].


basic_log_writes(Config) ->
    Dir = ?config(wal_dir, Config),
    {ok, _Pid} = ra_log_wal:start_link(#{dir => Dir}, []),
    {registered_name, Self} = erlang:process_info(self(), registered_name),
    ok = ra_log_wal:write(Self, ra_log_wal, 12, 1, "value"),
    {12, 1, "value"} = await_written(Self, {12, 12, 1}),
    ok = ra_log_wal:write(Self, ra_log_wal, 13, 1, "value2"),
    {13, 1, "value2"} = await_written(Self, {13, 13, 1}),
    % previous log value is still there
    {12, 1, "value"} = mem_tbl_read(Self, 12),
    undefined = mem_tbl_read(Self, 14),
    ra_lib:dump(ets:tab2list(ra_log_open_mem_tables)),
    ok.

write_to_unavailable_wal_returns_error(_Config) ->
    {registered_name, Self} = erlang:process_info(self(), registered_name),
    {error, wal_down} = ra_log_wal:write(Self, ra_log_wal, 12, 1, "value"),
    {error, wal_down} = ra_log_wal:truncate_write(Self, ra_log_wal, 12, 1, "value"),
    ok.

write_many(Config) ->
    NumWrites = 10000,
    Dir = ?config(wal_dir, Config),
    Modes = [{delayed_write, 1024 * 1024 * 4, 1}],
    % Modes = [],
    {ok, WalPid} = ra_log_wal:start_link(#{dir => Dir,
                                           additional_wal_file_modes => Modes,
                                           compute_checksums => false}, []),
    {registered_name, Self} = erlang:process_info(self(), registered_name),
    Data = crypto:strong_rand_bytes(1024),
    ok = ra_log_wal:write(Self, ra_log_wal, 0, 1, Data),
    timer:sleep(5),
    % start_profile(Config, [ra_log_wal, ets, file, os]),
    {reductions, RedsBefore} = erlang:process_info(WalPid, reductions),
    {Taken, _} =
        timer:tc(
          fun () ->
                  [begin
                       ok = ra_log_wal:write(Self, ra_log_wal, Idx, 1, Data)
                   end || Idx <- lists:seq(1, NumWrites)],
                  receive
                      {ra_log_event, {written, {_, NumWrites, 1}}} ->
                          ok
                  after 5000 ->
                            throw(written_timeout)
                  end
          end),
    {reductions, RedsAfter} = erlang:process_info(WalPid, reductions),

    Reds = RedsAfter - RedsBefore,
    ct:pal("~b 1024 byte writes took ~p milliseconds~nFile modes: ~p~n"
           "Reductions: ~b",
           [NumWrites, Taken / 1000, Modes, Reds]),

    % assert we aren't regressing on reductions used
    ?assert(Reds < 52023339 * 1.1),
    % stop_profile(Config),
    Metrics = [M || {_, V} = M <- lists:sort(ets:tab2list(ra_log_wal_metrics)),
                    V =/= undefined],
    ct:pal("Metrics: ~p~n", [Metrics]),
    ok.

overwrite(Config) ->
    Dir = ?config(wal_dir, Config),
    Modes = [{delayed_write, 1024 * 1024 * 4, 60 * 1000}],
    % Modes = [],
    {ok, _Pid} = ra_log_wal:start_link(#{dir => Dir,
                                         additional_wal_file_modes => Modes}, []),
    {registered_name, Self} = erlang:process_info(self(), registered_name),
    Data = data,
    [ok = ra_log_wal:write(Self, ra_log_wal, I, 1, Data)
     || I <- lists:seq(1, 3)],
    await_written(Self, {1, 3, 1}),
    % write next index then immediately overwrite
    ok = ra_log_wal:write(Self, ra_log_wal, 4, 1, Data),
    ok = ra_log_wal:write(Self, ra_log_wal, 2, 2, Data),
    % ensure we await the correct range that should not have a wonky start
    await_written(Self, {2, 2, 2}),
    ok.

truncate_write(Config) ->
    % a truncate write should update the range to not include previous indexes
    % a trucated write does not need to follow the sequence
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
    await_written(Self, {1, 3, 1}),
    % then write 7 as may happen after snapshot installation
    ok = ra_log_wal:truncate_write(Self, ra_log_wal, 7, 1, Data),
    ok = ra_log_wal:write(Self, ra_log_wal, 8, 1, Data),
    await_written(Self, {7, 8, 1}),
    [{Self, 7, 8, Tid}] = ets:lookup(ra_log_open_mem_tables, Self),
    [_] = ets:lookup(Tid, 7),
    [_] = ets:lookup(Tid, 8),
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
    await_written(Self, {1, 3, 1}),
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
    await_written(Self, {4, 5, 1}),

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
        {ra_log_event, {written, {8, 8, 1}}} ->
            throw(unexpected_written_event)
    after 500 -> ok
    end,
    % write the missing one
    ok = ra_log_wal:write(Self, ra_log_wal, 6, 1, Data),
    await_written(Self, {6, 6, 1}),

    ok.

roll_over(Config) ->
    Dir = ?config(wal_dir, Config),
    {registered_name, Self} = erlang:process_info(self(), registered_name),
    NumWrites = 5,
    % configure max_wal_size_bytes
    {ok, _Pid} = ra_log_wal:start_link(#{dir => Dir,
                                         max_wal_size_bytes => 1024 * NumWrites,
                                         segment_writer => Self}, []),
    handle_seg_writer_await(),
    % write enough entries to trigger roll over
    Data = crypto:strong_rand_bytes(1024),
    [begin
         ok = ra_log_wal:write(Self, ra_log_wal, Idx, 1, Data)
     end || Idx <- lists:seq(1, NumWrites)],
    % wait for writes
    receive {ra_log_event, {written, {1, NumWrites, 1}}} -> ok
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
    ?assert(undefined =/= mem_tbl_read(Self, 1)),
    ?assert(undefined =/= mem_tbl_read(Self, 5)),
    ok.

recover_truncated_write(Config) ->
    % open wal and write a few entreis
    % close wal + delete mem_tables
    % re-open wal and validate mem_tables are re-created
    Dir = ?config(wal_dir, Config),
    {registered_name, Self} = erlang:process_info(self(), registered_name),
    Data = <<42:256/unit:8>>,
    {ok, _} = ra_log_wal:start_link(#{dir => Dir, segment_writer => Self}, []),
    handle_seg_writer_await(),
    [ok = ra_log_wal:write(Self, ra_log_wal, Idx, 1, Data)
     || Idx <- lists:seq(1, 3)],
    ok = ra_log_wal:truncate_write(Self, ra_log_wal, 9, 1, Data),
    empty_mailbox(),
    proc_lib:stop(ra_log_wal),
    {ok, _} = ra_log_wal:start_link(#{dir => Dir, segment_writer => Self}, []),
    handle_seg_writer_await(),
    % how can we better wait for recovery to finish?
    timer:sleep(1000),
    [{Self, _, 9, 9, _}] =
        lists:sort(ets:lookup(ra_log_closed_mem_tables, Self)),
    ok.

recover(Config) ->
    % open wal and write a few entreis
    % close wal + delete mem_tables
    % re-open wal and validate mem_tables are re-created
    Dir = ?config(wal_dir, Config),
    {registered_name, Self} = erlang:process_info(self(), registered_name),
    Data = <<42:256/unit:8>>,
    {ok, _} = ra_log_wal:start_link(#{dir => Dir, segment_writer => Self,
                                      compute_checksums => true}, []),
    handle_seg_writer_await(),
    [ok = ra_log_wal:write(Self, ra_log_wal, Idx, 1, Data)
     || Idx <- lists:seq(1, 100)],
    ra_log_wal:force_roll_over(ra_log_wal),
    [ok = ra_log_wal:write(Self, ra_log_wal, Idx, 2, Data)
     || Idx <- lists:seq(101, 200)],
    empty_mailbox(),
    proc_lib:stop(ra_log_wal),
    {ok, _} = ra_log_wal:start_link(#{dir => Dir, segment_writer => Self}, []),
    handle_seg_writer_await(),
    % how can we better wait for recovery to finish?
    timer:sleep(1000),

    % there should be no open mem tables after recovery as we treat any found
    % wal files as complete
    [] = ets:lookup(ra_log_open_mem_tables, Self),
    [ {Self, _, 1, 100, MTid1}, % this is the "old" table
      % these are the recovered tables
      {Self, _, 1, 100, MTid2}, {Self, _, 101, 200, MTid4} ] =
        lists:sort(ets:lookup(ra_log_closed_mem_tables, Self)),
    100 = ets:info(MTid1, size),
    100 = ets:info(MTid2, size),
    100 = ets:info(MTid4, size),
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

handle_seg_writer_await() ->
    receive
        {'$gen_call', From, await} ->
            gen:reply(From, ok);
        Msg ->
            ct:pal("seg writer wait got ~p", [Msg]),
            handle_seg_writer_await()
    after 2000 ->
              throw(seq_writer_await_timeout)
    end.

empty_mailbox() ->
    receive
        _ ->
            empty_mailbox()
    after 100 ->
              ok
    end.

await_written(Id, {_From, To, _Term} = Written) ->
    receive
        {ra_log_event, {written, Written}} ->
            mem_tbl_read(Id, To)
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
    % this segfaults
    % timer:sleep(2000),
    % Dir = ?config(priv_dir, Config),
    % Name = filename:join([Dir, "lg_" ++ atom_to_list(Case)]),
    % lg_callgrind:profile_many(Name ++ ".gz.*", Name ++ ".out",#{}),
    ok.


% mem table read functions
% the actual logic is implemented in ra_log_file
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
