-module(ra_log_2_SUITE).
-compile(nowarn_export_all).
-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("kernel/include/file.hrl").

-include("src/ra.hrl").
%%
%%

all() ->
    [
     {group, random},
     {group, sequential}
    ].


all_tests() ->
    [
     resend_write_lost_in_wal_crash,
     resend_after_written_event_lost_in_wal_crash,
     resend_write_after_tick,
     handle_overwrite,
     handle_overwrite_append,
     receive_segment,
     delete_during_segment_flush,
     read_one,
     take_after_overwrite_and_init,
     validate_sequential_fold,
     validate_reads_for_overlapped_writes,
     cache_overwrite_then_take,
     last_written_overwrite,
     last_written_overwrite_2,
     last_index_reset,
     fold_after_sparse_mem_table,
     fold_after_sparse_segments,
     write_sparse_re_init,
     write_sparse_after_index_reset,
     write_sparse_after_index_reset_segments,
     last_index_reset_before_written,
     recovery,
     recover_many,
     recovery_with_missing_directory,
     recovery_with_missing_checkpoints_directory,
     recovery_with_missing_config_file,
     wal_crash_recover,
     wal_crash_with_lost_message_and_log_init,
     missed_written_then_write,
     missed_written_then_segments_then_write,
     wal_down_read_availability,
     wal_down_append_throws,
     wal_down_write_returns_error_wal_down,

     detect_lost_written_range,
     snapshot_installation,
     snapshot_written_after_installation,
     oldcheckpoints_deleted_after_snapshot_install,
     append_after_snapshot_installation_with_live_indexes,
     append_after_snapshot_installation,
     release_cursor_after_snapshot_installation,
     written_event_after_snapshot_installation,
     update_release_cursor,
     update_release_cursor_with_machine_version,
     missed_mem_table_entries_are_deleted_at_next_opportunity,
     transient_writer_is_handled,
     read_opt,
     sparse_read,
     read_plan_modified,
     read_plan,
     sparse_read_out_of_range,
     sparse_read_out_of_range_2,
     written_event_after_snapshot,
     writes_lower_than_snapshot_index_are_dropped,
     recover_after_snapshot,
     updated_segment_can_be_read,
     open_segments_limit,
     write_config,
     sparse_write,
     overwritten_segment_is_cleared,
     overwritten_segment_is_cleared_on_init,
     concurrent_snapshot_install_and_compaction,
     snapshot_installation_with_live_indexes,
     init_with_dangling_symlink,
     init_after_missing_segments_event
   ].

groups() ->
    [
     {random, [], all_tests()},
     {sequential, [], all_tests()}
    ].

init_per_suite(Config) ->
    Config.

end_per_suite(Config) ->
    Config.

init_per_group(G, Config0) ->
    DataDir = filename:join(?config(priv_dir, Config0), G),
    ra_env:configure_logger(logger),
    LogFile = filename:join(DataDir, "ra.log"),
    logger:set_primary_config(level, debug),
    logger:add_handler(ra_handler, logger_std_h,
                       #{config => #{file => LogFile}}),
    Config = [{access_pattern, G},
              {work_dir, DataDir}
              | Config0],

    ok = start_ra(Config),
    Config.

end_per_group(_, Config) ->
    application:stop(ra),
    Config.

init_per_testcase(TestCase, Config) ->
    DataDir = ?config(work_dir, Config),
    UId = <<(atom_to_binary(TestCase, utf8))/binary,
            (atom_to_binary(?config(access_pattern, Config)))/binary>>,
    ok = ra_directory:register_name(default, UId, self(), undefined,
                                    TestCase, TestCase),
    ServerConf = #{log_init_args => #{uid => UId}},

    ok = ra_lib:make_dir(filename:join([DataDir, node(), UId])),
    ok = ra_lib:write_file(filename:join([DataDir, node(), UId, "config"]),
                           list_to_binary(io_lib:format("~p.", [ServerConf]))),

    [SupPid] = [P || {ra_log_wal_sup, P, _, _}
                     <- supervisor:which_children(ra_log_sup)],
    _ = supervisor:restart_child(SupPid, ra_log_wal),
    [{uid, UId}, {test_case, TestCase}, {wal_dir, DataDir} | Config].

end_per_testcase(_, Config) ->
    ra_directory:unregister_name(default, ?config(uid, Config)),
    ok.

-define(N1, {n1, node()}).
-define(N2, {n2, node()}).
% -define(N3, {n3, node()}).

handle_overwrite(Config) ->
    Log0 = ra_log_init(Config),
    {ok, Log1} = ra_log:write([{1, 1, "value"},
                               {2, 1, "value"}], Log0),
    receive
        {ra_log_event, {written, 1, [2, 1]}} -> ok
    after 2000 ->
              exit(written_timeout)
    end,
    {ok, Log3} = ra_log:write([{1, 2, "value"}], Log1),
    % ensure immediate truncation
    {1, 2} = ra_log:last_index_term(Log3),
    {ok, Log4} = ra_log:write([{2, 2, "value"}], Log3),
    % simulate the first written event coming after index 20 has already
    % been written in a new term
    {Log, _} = ra_log:handle_event({written, 1, [2, 1]}, Log4),
    % ensure last written has not been incremented
    {0, 0} = ra_log:last_written(Log),
    {2, 2} = ra_log:last_written(
               element(1, ra_log:handle_event({written, 2, [2, 1]}, Log))),
    ok = ra_log_wal:force_roll_over(ra_log_wal),
    _ = deliver_all_log_events(Log, 100),
    ra_log:close(Log),
    flush(),
    ok.

handle_overwrite_append(Config) ->
    %% this is a theoretical case where a follower has written some entries
    %% then another leader advised to reset last index backwards, _then_
    %% somehow the current follower become leader
    Log0 = ra_log_init(Config),
    {ok, Log1} = ra_log:write([{1, 1, "value"},
                               {2, 1, "value"}], Log0),
    receive
        {ra_log_event, {written, 1, [2, 1]}} -> ok
    after 2000 ->
              flush(),
              exit(written_timeout)
    end,
    {ok, Log2} = ra_log:set_last_index(1, Log1),
    {0, 0} = ra_log:last_written(Log2),
    {1, 1} = ra_log:last_index_term(Log2),
    Log3 = ra_log:append({2, 3, "value"}, Log2),
    {2, 3} = ra_log:last_index_term(Log3),
    % ensure immediate truncation
    Log4 = ra_log:append({3, 3, "value"}, Log3),
    {3, 3} = ra_log:last_index_term(Log4),
    % simulate the first written event coming after index has already
    % been written in a new term
    {Log, _} = ra_log:handle_event({written, 1, [2, 1]}, Log4),
    % ensure last written has not been incremented
    {1, 1} = ra_log:last_written(Log),
    {3, 3} = ra_log:last_written(
               element(1, ra_log:handle_event({written, 3, [3, 2]}, Log))),
    ok = ra_log_wal:force_roll_over(ra_log_wal),
    _ = deliver_all_log_events(Log, 100),
    ra_log:close(Log),
    flush(),
    ok.

receive_segment(Config) ->
    Log0 = ra_log_init(Config),
    % write a few entries
    Entries = [{I, 1, <<"value_", I:32/integer>>} || I <- lists:seq(1, 3)],

    {PreWritten, _} = ra_log:last_written(Log0),
    Log1 = lists:foldl(fun(E, Acc0) ->
                               ra_log:append(E, Acc0)
                       end, Log0, Entries),
    Log2 = deliver_log_events_cond(
             Log1, fun (L) ->
                           {PostWritten, _} = ra_log:last_written(L),
                           PostWritten >= (PreWritten + 3)
                   end, 100),
    {3, 1} = ra_log:last_written(Log2),
    % force wal roll over
    ok = ra_log_wal:force_roll_over(ra_log_wal),

    Log3 = deliver_log_events_cond(
             Log2, fun (L) ->
                           #{mem_table_range := MtRange} = ra_log:overview(L),
                           MtRange == undefined
                   end, 100),
    % validate reads
    {Entries, FinalLog} = ra_log_take(1, 3, Log3),
    ?assertEqual(length(Entries), 3),
    ra_log:close(FinalLog),
    ok.

delete_during_segment_flush(Config) ->
    %% this test doesn't necessarily trigger the potential issue but is
    %% worth keeping around
    Log0 = ra_log_init(Config),
    Data = crypto:strong_rand_bytes(4000),
    % write a few entries
    Entries = [{I, 1, Data} || I <- lists:seq(1, 100000)],

    {PreWritten, _} = ra_log:last_written(Log0),
    Log1 = lists:foldl(fun(E, Acc0) ->
                               ra_log:append(E, Acc0)
                       end, Log0, Entries),
    Log2 = deliver_log_events_cond(
             Log1, fun (L) ->
                           {PostWritten, _} = ra_log:last_written(L),
                           PostWritten >= (PreWritten + 10000)
                   end, 100),
    Ref = monitor(process, ra_log_segment_writer),
    % force wal roll over
    ok = ra_log_wal:force_roll_over(ra_log_wal),

    timer:sleep(0),
    ra_log:delete_everything(Log2),


    receive
        {'DOWN', Ref, _, _, _} ->
            flush(),
            ct:fail("segment writer unexpectedly exited")
    after 100 ->
              ok
    end,
    flush(),

    ok.

read_one(Config) ->
    ra_counters:new(?FUNCTION_NAME, ?RA_COUNTER_FIELDS),
    Log0 = ra_log_init(Config, #{counter => ra_counters:fetch(?FUNCTION_NAME)}),
    Log1 = append_n(1, 2, 1, Log0),
    % ensure the written event is delivered
    Log2 = deliver_all_log_events(Log1, 200),
    {[_], Log} = ra_log_take(1, 1, Log2),
    % read out of range
    #{?FUNCTION_NAME := #{read_mem_table := M1,
                          read_segment := M2}} = ra_counters:overview(),
    % read two entries
    ?assertEqual(1, M1 + M2),
    ra_log:close(Log),
    ok.

take_after_overwrite_and_init(Config) ->
    Log0 = ra_log_init(Config),
    Log1 = write_and_roll_no_deliver(1, 5, 1, Log0),
    Log2 = deliver_written_log_events(Log1, 200),
    {[_, _, _, _], Log3} = ra_log_take(1, 4, Log2),
    Log4 = write_and_roll_no_deliver(1, 2, 2, Log3),
    Log5 = deliver_log_events_cond(Log4,
                                   fun (L) ->
                                           {1, 2} =:= ra_log:last_written(L)
                                   end, 100),

    % ensure we cannot take stale entries
    {[{1, 2, _}], Log6} = ra_log_take(1, 4, Log5),
    _ = ra_log:close(Log6),
    Log = ra_log_init(Config),
    {[{1, 2, _}], _} = ra_log_take(1, 4, Log),
    ok.


validate_sequential_fold(Config) ->
    ra_counters:new(?FUNCTION_NAME, ?RA_COUNTER_FIELDS),
    Log0 = ra_log_init(Config, #{counter => ra_counters:fetch(?FUNCTION_NAME),
                                 max_open_segments => 2}),
    % write 1000 entries
    Log1 = append_and_roll(1, 500, 1, Log0),
    Log2 = append_n(500, 999, 1, Log1),
    %% need to ensure the segments are delivered
    Log3 = deliver_all_log_events(Log2, 200),
    %% write two to be held in cache
    Log = append_n(999, 1001, 1, Log3),
    _ = erlang:statistics(exact_reductions),
    {ColdTaken, {ColdReds, FinLog}} =
        timer:tc(fun () ->
                         {_, Reds0} = erlang:statistics(exact_reductions),
                         L = validate_fold(1, 1000, 1, Log),
                         {_, Reds} = erlang:statistics(exact_reductions),
                         {Reds - Reds0, L}
                 end),
    ct:pal("validate_sequential_fold COLD took ~pms Reductions: ~p~nMetrics: ",
           [ColdTaken/1000, ColdReds]),

    ct:pal("ra_log:overview/1 ~p", [ra_log:overview(FinLog)]),

    #{read_mem_table := M1,
      open_segments := 2, %% as this is the max
      read_segment := M4} = O = ra_counters:overview(?FUNCTION_NAME),
    ct:pal("counters ~p", [O]),
    ?assertEqual(1000, M1 + M4),

    ra_log:close(FinLog),
    ok.

validate_reads_for_overlapped_writes(Config) ->
    ra_counters:new(?FUNCTION_NAME, ?RA_COUNTER_FIELDS),
    Log0 = ra_log_init(Config, #{counter => ra_counters:fetch(?FUNCTION_NAME)
                        }),
    % write a segment and roll 1 - 299 - term 1
    Log1 = write_and_roll(1, 300, 1, Log0),
    % write 300 - 399 in term 1 - no roll
    Log2 = write_n(300, 400, 1, Log1),
    % write 200 - 350 in term 2 and roll
    Log3 = write_and_roll(200, 350, 2, Log2),
    % write 350 - 500 in term 2
    Log4 = write_and_roll(350, 500, 2, Log3),
    Log5 = write_n(500, 551, 2, Log4),
    % Log6 = deliver_all_log_events(Log5, 200),
    Log6 = deliver_log_events_cond(
             Log5, fun (L) ->
                           {W, _} = ra_log:last_written(L),
                           W >= 550
                   end, 100),

    Log7 = validate_fold(1, 199, 1, Log6),
    Log8 = validate_fold(200, 550, 2, Log7),

    #{?FUNCTION_NAME := #{read_mem_table := M1,
                          read_segment := M2}} = ra_counters:overview(),
    ?assertEqual(550, M1 + M2),
    ra_log:close(Log8),
    %% re open to test init with overlapping segments
    Log = ra_log_init(Config, #{counter => ra_counters:fetch(?FUNCTION_NAME)}),
    ra_log:close(Log),
    ok.

read_opt(Config) ->
    Log0 = ra_log_init(Config),
    % Log0 = ra_log:release_resources(2, undefined, Log00),
    % write a segment and roll 1 - 299 - term 1
    Num = 4096 * 2,
    Log1 = write_and_roll(1, Num, 1, Log0, 50),
    Log2 = wait_for_segments(Log1, 5000),
    %% read small batch of the latest entries
    {_, Log} = ra_log_take(Num - 5, Num, Log2),
    %% measure the time it takes to read the first index
    {Time, _} = timer:tc(fun () ->
                                 _ = erlang:statistics(exact_reductions),
                                 ra_log_take(1, 1, Log)
                         end),
    {_, Reds} = erlang:statistics(exact_reductions),
    ct:pal("read took ~wms Reduction ~w", [Time / 1000, Reds]),
    {Time2, _} = timer:tc(fun () ->
                                 _ = erlang:statistics(exact_reductions),
                                 ra_log_init(Config, #{mode => read})
                         end),
    {_, Reds2} = erlang:statistics(exact_reductions),

    ct:pal("read init took ~wms Reduction ~w", [Time2 / 1000, Reds2]),

    {Time3, _} = timer:tc(fun () ->
                                 _ = erlang:statistics(exact_reductions),
                                 ra_log_take(1, Num, Log)
                         end),
    {_, Reds3} = erlang:statistics(exact_reductions),
    ct:pal("read all took ~wms Reduction ~w", [Time3 / 1000, Reds3]),
    ok.

sparse_read_out_of_range(Config) ->
    Log0 = ra_log_init(Config),
    Log1 = write_and_roll(1, 2, 1, Log0, 50),
    Log = deliver_all_log_events(Log1, 100),
    ?assertMatch({[], _}, ra_log:sparse_read([2, 100], Log)),
    ra_log:close(Log),
    ok.

sparse_read_out_of_range_2(Config) ->
    Log0 = ra_log_init(Config),
    {0, 0} = ra_log:last_index_term(Log0),
    %% write 10 entries
    %% but only process events for 9
    Log1 = deliver_all_log_events(write_n(10, 20, 2,
                                          write_and_roll(1, 10, 2, Log0)), 50),
    SnapIdx = 10,
    %% do snapshot in
    {Log2, Effs} = ra_log:update_release_cursor(SnapIdx, #{}, macctx(),
                                                <<"snap@10">>, Log1),
    run_effs(Effs),
    {Log3, Effs3} = receive
                        {ra_log_event, {snapshot_written, {10, 2}, _,
                                        snapshot, _} = Evt} ->
                            ra_log:handle_event(Evt, Log2)
                    after 5000 ->
                              flush(),
                              exit(snapshot_written_timeout)
                    end,
    run_effs(Effs3),
    Log4 = deliver_all_log_events(Log3, 100),

    {SnapIdx, 2} = ra_log:snapshot_index_term(Log4),

    ?assertMatch({[{11, _, _}], _},
                 ra_log:sparse_read([1,2, 11, 100], Log4)),
    ra_log:close(Log4),
    ok.

sparse_read(Config) ->
    Num = 4096 * 2,
    Div = 2,
    Log0 = write_and_roll(1, Num div Div, 1, ra_log_init(Config), 50),
    Log1 = wait_for_segments(Log0, 5000),
    Log2 = write_no_roll(Num div Div, Num, 1, Log1, 50),
    %% read small batch of the latest entries
    {_, Log3} = ra_log_take(Num - 5, Num, Log2),
    ct:pal("log overview ~p", [ra_log:overview(Log3)]),
    %% ensure cache is empty as this indicates all enties have at least
    %% been written to the WAL and thus will be available in mem tables.
    Log4 = deliver_log_events_cond(Log3,
                                   fun (L) ->
                                           LIT = ra_log:last_index_term(L),
                                           case ra_log:last_written(L) of
                                               LIT ->
                                                   true;
                                               _ ->
                                                   false
                                           end
                                   end, 100),
    ra_log:close(Log4),
    NumDiv2 = Num div 2,
    %% create a list of indexes with some consecutive and some gaps
    Indexes = lists:usort(lists:seq(1, Num, 2) ++ lists:seq(1, Num, 5)),
    %% make sure that the ETS deletes have been finished before we re-init
    gen_server:call(ra_log_ets, ok),
    LogTake = ra_log_init(Config),
    {TimeTake, {_, LogTake1}} =
        timer:tc(fun () ->
                         _ = erlang:statistics(exact_reductions),
                         ra_log_take(1, NumDiv2, LogTake)
                 end),
    {_, Reds} = erlang:statistics(exact_reductions),
    ra_log:close(LogTake1),
    ct:pal("read ~b Indexes with take/3 took ~wms Reduction ~w",
           [NumDiv2, TimeTake / 1000, Reds]),

    LogSparse = ra_log_init(Config),
    {TimeSparse, {SparseEntries, _}} =
        timer:tc(fun () ->
                         _ = erlang:statistics(exact_reductions),
                         ra_log:sparse_read(Indexes, LogSparse)
                 end),
    {_, Reds2} = erlang:statistics(exact_reductions),
    ReadIndexes = [I || {I, _, _} <- SparseEntries],
    ?assertEqual(Indexes, ReadIndexes),
    ct:pal("read ~b indexes with sparse_read/2 took ~wms Reduction ~w",
           [length(SparseEntries), TimeSparse / 1000, Reds2]),

    LogO = ra_log_init(Config),
    {[{1, _, _},
      {2, _, _},
      {3, _, _}], LogO1} = ra_log:sparse_read([1,2,3], LogO),

    {[{6, _, _},
      {5, _, _},
      {3, _, _}], LogO2} = ra_log:sparse_read([6,5,3], LogO1),

    {[{1000, _, _},
      {5, _, _},
      {99, _, _}], _LogO3} = ra_log:sparse_read([1000,5,99], LogO2),
    ok.

read_plan_modified(Config) ->
    Log0 = ra_log_init(Config),
    Log1 = write_and_roll(1, 2, 1, Log0, 50),
    Log2 = deliver_all_log_events(Log1, 100),
    Plan = ra_log:partial_read([1], Log2, fun (_, _, Cmd) -> Cmd end),
    {#{1 := _}, Flru} = ra_log_read_plan:execute(Plan, undefined),

    Log3 = deliver_all_log_events(write_and_roll(2, 3, 1, Log2, 50), 100),
    Plan2 = ra_log:partial_read([1,2], Log3, fun (_, _, Cmd) -> Cmd end),
    %% assert we can read the newly appended item with the cached
    %% segment
    {#{1 := _, 2 := _}, Flru2} = ra_log_read_plan:execute(Plan2, Flru),
    Log = deliver_all_log_events(write_and_roll(3, 4, 1, Log3, 50), 100),
    {#{1 := _, 2 := _}, _} = ra_log_read_plan:execute(Plan2, Flru2),
    ra_log:close(Log),
    ok.

read_plan(Config) ->
    Num = 256 * 2,
    Div = 2,
    Log0 = write_and_roll(1, Num div Div, 1, ra_log_init(Config), 50),
    Log1 = wait_for_segments(Log0, 5000),
    Log2 = write_no_roll(Num div Div, Num, 1, Log1, 50),
    %% read small batch of the latest entries
    {_, Log3} = ra_log_take(Num - 5, Num, Log2),
    %% ensure cache is empty as this indicates all enties have at least
    %% been written to the WAL and thus will be available in mem tables.
    Log4 = deliver_log_events_cond(Log3,
                                   fun (L) ->
                                           ra_log:last_written(L) ==
                                           ra_log:last_index_term(L)
                                   end, 100),
    %% create a list of indexes with some consecutive and some gaps
    Indexes = lists:usort(lists:seq(1, Num, 2) ++ lists:seq(1, Num, 5)),
    %% make sure that the ETS deletes have been finished before we re-init
    gen_server:call(ra_log_ets, ok),
    ReadPlan = ra_log:partial_read(Indexes, Log4, fun (_, _, Cmd) -> Cmd end),
    ?assert(is_map(ra_log_read_plan:info(ReadPlan))),
    {EntriesOut, _} = ra_log_read_plan:execute(ReadPlan, undefined),
    %% try again with different read plan options
    {EntriesOut, _} = ra_log_read_plan:execute(ReadPlan, undefined,
                                               #{access_pattern => sequential,
                                                 file_advise => random}),
    ?assertEqual(length(Indexes), maps:size(EntriesOut)),
    %% assert the indexes requestd were all returned in order
    [] = Indexes -- [I || I <- maps:keys(EntriesOut)],
    ok.

written_event_after_snapshot(Config) ->
    Log0 = ra_log_init(Config, #{min_snapshot_interval => 1}),
    Log1 = ra_log:append({1, 1, <<"one">>}, Log0),
    Log1b = ra_log:append({2, 1, <<"two">>}, Log1),
    {Log2, Effs} = ra_log:update_release_cursor(2, #{}, macctx(),
                                                <<"one+two">>, Log1b),
    run_effs(Effs),
    {Log3, _} = receive
                    {ra_log_event, {snapshot_written, {2, 1}, _,
                                    snapshot, _} = Evt} ->
                        ra_log:handle_event(Evt, Log2)
                after 500 ->
                          exit(snapshot_written_timeout)
                end,

    %% the written events for indexes [1,2] are delivered after
    Log4 = deliver_all_log_events(Log3, 100),
    ct:pal("Log4 ~p", [ra_log:overview(Log4)]),
    % true = filelib:is_file(Snap1),
    Log5  = ra_log:append({3, 1, <<"three">>}, Log4),
    Log6  = ra_log:append({4, 1, <<"four">>}, Log5),
    Log6b = deliver_all_log_events(Log6, 100),
    {Log7, Effs2} = ra_log:update_release_cursor(4, #{}, macctx(),
                                                 <<"one+two+three+four">>,
                                                 Log6b),
    run_effs(Effs2),
    _ = receive
            {ra_log_event, {snapshot_written, {4, 1}, _, snapshot, _} = E} ->
                ra_log:handle_event(E, Log7)
        after 500 ->
                  exit(snapshot_written_timeout)
        end,

    %% this will no longer be false as the snapshot deletion is an effect
    %% and not done by the log itself
    % false = filelib:is_file(Snap1),
    ok.


recover_after_snapshot(Config) ->
    Log0 = ra_log_init(Config, #{min_snapshot_interval => 1}),
    Log1 = ra_log:append({1, 1, <<"one">>}, Log0),
    Log2 = ra_log:append({2, 1, <<"two">>}, Log1),
    {Log3, Effs} = ra_log:update_release_cursor(2, #{}, macctx(),
                                                <<"one+two">>, Log2),
    run_effs(Effs),
    Log4 = deliver_all_log_events(Log3, 100),
    ra_log:close(Log4),
    restart_wal(),
    timer:sleep(1000),
    Log = ra_log_init(Config, #{min_snapshot_interval => 1}),
    Overview = ra_log:overview(Log),
    ra_log:close(Log),
    ?assertMatch(#{range := undefined,
                   last_term := 1,
                   snapshot_index := 2,
                   last_written_index_term := {2, 1}}, Overview),
    ok.

writes_lower_than_snapshot_index_are_dropped(Config) ->
    logger:set_primary_config(level, debug),
    Log0 = ra_log_init(Config, #{min_snapshot_interval => 1}),
    Log1 = ra_log:append({1, 1, <<"one">>}, Log0),
    Log1b = deliver_all_log_events(ra_log:append({2, 1, <<"two">>}, Log1), 500),
    true = erlang:suspend_process(whereis(ra_log_wal)),
    Log2 = write_n(3, 500, 1, Log1b),
    {Log3, Effs0} = ra_log:update_release_cursor(100, #{}, macctx(),
                                                 <<"100">>, Log2),
    run_effs(Effs0),
    Log4 = deliver_all_log_events(Log3, 500),

    Overview = ra_log:overview(Log4),
    ?assertMatch(#{range := {101, 499},
                   mem_table_range := {101, 499},
                   last_written_index_term := {100, 1}}, Overview),

    true = erlang:resume_process(whereis(ra_log_wal)),

    %% no written notifications for anything lower than the snapshot should
    %% be received
    Log5 = receive
               {ra_log_event, {written, _Term, [{From, _To}]} = E}
                 when From == 101 ->
                   {Log4b, Effs} = ra_log:handle_event(E, Log4),
                   Log4c = lists:foldl(
                             fun ({next_event, {ra_log_event, Evt}}, Acc0) ->
                                     {Acc, _} = ra_log:handle_event(Evt, Acc0),
                                     Acc;
                                 (_, Acc) ->
                                     Acc
                             end, Log4b, Effs),
                   deliver_all_log_events(Log4c, 200);
               {ra_log_event, E} ->
                   ct:fail("unexpected log event ~p", [E])
           after 500 ->
                     flush(),
                     ct:fail("expected log event not received")
           end,
    OverviewAfter = ra_log:overview(Log5),
    ?assertMatch(#{range := {101, 499},
                   snapshot_index := 100,
                   mem_table_range := {101, 499},
                   last_written_index_term := {499, 1}}, OverviewAfter),
    %% restart the app to test recovery with a "gappy" wal
    application:stop(ra),
    start_ra(Config),
    erlang:monitor(process, whereis(ra_log_segment_writer)),
    receive
        {'DOWN', _, _, _, _} = D ->
            ct:fail("DOWN received ~p", [D])
    after 500 ->
              ok
    end,
    flush(),
    ok.

updated_segment_can_be_read(Config) ->
    ra_counters:new(?FUNCTION_NAME, ?RA_COUNTER_FIELDS),
    Log0 = ra_log_init(Config,
                       #{counter => ra_counters:fetch(?FUNCTION_NAME),
                         min_snapshot_interval => 1}),
    %% append a few entries
    Log2 = append_and_roll(1, 5, 1, Log0),
    % Log2 = deliver_all_log_events(Log1, 200),
    %% read some, this will open the segment with the an index of entries
    %% 1 - 4
    {Entries, Log3} = ra_log_take(1, 25, Log2),
    ?assertEqual(4, length(Entries)),
    %% append a few more itmes and process the segments
    Log4 = append_and_roll(5, 16, 1, Log3),
    % this should return all entries
    {Entries1, _} = ra_log_take(1, 15, Log4),
    ?assertEqual(15, length(Entries1)),
    ?assertEqual(15, length(Entries1)),
    ok.

cache_overwrite_then_take(Config) ->
    Log0 = ra_log_init(Config),
    Log1 = write_n(1, 5, 1, Log0),
    Log2 = write_n(3, 4, 2, Log1),
    % validate only 3 entries can be read even if requested range is greater
    {[_, _, _], _} = ra_log_take(1, 5, Log2),
    ok.

last_written_overwrite(Config) ->
    Log0 = ra_log_init(Config),
    Log1 = write_n(1, 5, 1, Log0),
    Log2 = assert_log_events(Log1, fun (L) ->
                                           {4, 1} == ra_log:last_written(L)
                                   end),
    % write an event for a prior index
    {ok, Log3} = ra_log:write([{3, 2, <<3:64/integer>>}], Log2),
    Log4 = assert_log_events(Log3, fun (L) ->
                                           {3, 2} == ra_log:last_written(L)
                                   end),
    ra_log:close(Log4),
    ok.

last_written_overwrite_2(Config) ->
    Log0 = ra_log_init(Config),

    WalPid = whereis(ra_log_wal),
    erlang:suspend_process(WalPid),
    %% ensure full batch
    Log1 = write_n(1, 5, 1, Log0),
    erlang:resume_process(WalPid),
    %% how else to wait for wal processing but not process the written event
    timer:sleep(500),

    erlang:suspend_process(WalPid),
    %% partially overwrite prior batch
    Log2 = write_n(4, 6, 2, Log1),

    %% ensure last written is applied up to the last valid index term
    Log3 = assert_log_events(Log2, fun (L) ->
                                           {3, 1} == ra_log:last_written(L)
                                   end),
    erlang:resume_process(WalPid),
    %
    Log4 = assert_log_events(Log3, fun (L) ->
                                           {5, 2} == ra_log:last_written(L)
                                   end),
    ra_log:close(Log4),
    ok.

last_index_reset(Config) ->
    Log0 = ra_log_init(Config),
    Log1 = write_n(1, 6, 1, Log0),
    Pred = fun (L) ->
                   {5, 1} == ra_log:last_written(L)
           end,
    Log2 = assert_log_events(Log1, Pred, 2000),
    6 = ra_log:next_index(Log2),
    {5, 1} = ra_log:last_index_term(Log2),
    % reverts last index to a previous index
    % needs to be done if a new leader sends an empty AER
    {ok, Log3} = ra_log:set_last_index(3, Log2),
    {3, 1} = ra_log:last_written(Log3),
    4 = ra_log:next_index(Log3),
    {3, 1} = ra_log:last_index_term(Log3),
    O = ra_log:overview(Log3),
    ct:pal("o ~p", [O]),
    ?assertMatch(#{range := {0, 3},
                   %% we have a new mem table but the mem table does not know
                   %% whatever the first index should be so reports the
                   %% full previous range, this will be corrected after the
                   %% next write at index 4
                   mem_table_range := {0, 5}},
                 O),
    {ok, Log} = ra_log:write([{4, 2, hi}], Log3),
    O2 = ra_log:overview(Log),
    ct:pal("o ~p", [O2]),
    ?assertMatch(#{range := {0, 4},
                   mem_table_range := {0, 4}},
                 O2),
    ok.

fold_after_sparse_mem_table(Config) ->
    Log0 = ra_log_init(Config),
    Log1 = write_n(1, 6, 1, Log0),
    Pred = fun (L) ->
                   {5, 1} == ra_log:last_written(L)
           end,
    Log2 = assert_log_events(Log1, Pred, 500),
    6 = ra_log:next_index(Log2),
    {5, 1} = ra_log:last_index_term(Log2),
    % reverts last index to a previous index
    % needs to be done if a new leader sends an empty AER
    {ok, Log2b} = ra_log:set_last_index(5, Log2),
    {ok, Log3} = ra_log:write_sparse({7, 1, hi}, undefined, Log2b),
    {ok, Log4} = ra_log:write_sparse({17, 1, hi}, 7, Log3),
    Log = deliver_all_log_events(Log4, 500),
    {Res, _Log} = ra_log:fold(1, 17,
                              fun ({I, _, _}, Is) ->
                                      [I | Is]
                              end, [], Log, return),
    ct:pal("Res ~p", [Res]),
    ?assertMatch([5,4,3,2,1], Res),
    ok.

fold_after_sparse_segments(Config) ->
    Log0 = ra_log_init(Config),
    Log1 = write_n(1, 6, 1, Log0),
    Pred = fun (L) ->
                   {5, 1} == ra_log:last_written(L)
           end,
    Log2 = assert_log_events(Log1, Pred, 500),
    6 = ra_log:next_index(Log2),
    {5, 1} = ra_log:last_index_term(Log2),
    % reverts last index to a previous index
    % needs to be done if a new leader sends an empty AER
    {ok, Log2b} = ra_log:set_last_index(5, Log2),
    {ok, Log3} = ra_log:write_sparse({7, 1, hi}, undefined, Log2b),
    {ok, Log4} = ra_log:write_sparse({17, 1, hi}, 7, Log3),
    ok = ra_log_wal:force_roll_over(ra_log_wal),
    Log = deliver_all_log_events(Log4, 500),
    ra_log:fold(1, 17, fun (_, _) -> ok end, undefined, Log, return),
    ok.

write_sparse_re_init(Config) ->
    Log0 = ra_log_init(Config),
    Log1 = write_and_roll(1, 6, 1, Log0),
    Pred = fun (L) ->
                   {5, 1} == ra_log:last_written(L)
           end,
    Log2 = assert_log_events(Log1, Pred, 500),
    6 = ra_log:next_index(Log2),
    {5, 1} = ra_log:last_index_term(Log2),
    % reverts last index to a previous index
    % needs to be done if a new leader sends an empty AER
    {ok, Log2b} = ra_log:set_last_index(5, Log2),
    {ok, Log3} = ra_log:write_sparse({7, 1, hi}, undefined, Log2b),
    {ok, Log4} = ra_log:write_sparse({17, 1, hi}, 7, Log3),
    Log = deliver_all_log_events(Log4, 500),

    O = ra_log:overview(Log),
    ra_log:close(Log),

    ReInitLog = ra_log_init(Config),
    O2 = ra_log:overview(ReInitLog ),
    OO = maps:without([last_wal_write, open_segments], O),
    OO2 = maps:without([last_wal_write, open_segments], O2),
    ?assertEqual(OO, OO2),
    ok.

write_sparse_after_index_reset(Config) ->
    Log0 = ra_log_init(Config),
    Log1 = write_n(1, 6, 1, Log0),
    Pred = fun (L) ->
                   {5, 1} == ra_log:last_written(L)
           end,
    Log2 = assert_log_events(Log1, Pred, 500),
    6 = ra_log:next_index(Log2),
    {5, 1} = ra_log:last_index_term(Log2),
    % reverts last index to a previous index
    % needs to be done if a new leader sends an empty AER
    {ok, Log3} = ra_log:set_last_index(3, Log2),
    {3, 1} = ra_log:last_written(Log3),
    4 = ra_log:next_index(Log3),
    {3, 1} = ra_log:last_index_term(Log3),
    O = ra_log:overview(Log3),
    ct:pal("o ~p", [O]),
    {ok, Log4} = ra_log:write_sparse({7, 1, hi}, undefined, Log3),
    {ok, Log} = ra_log:write_sparse({17, 1, hi}, 7, Log4),
    O2 = ra_log:overview(Log),
    ct:pal("o ~p", [O2]),
    ?assertMatch(#{range := {0, 17},
                   mem_table_range := {0, 17}},
                 O2),
    ok.

write_sparse_after_index_reset_segments(Config) ->
    Log0 = ra_log_init(Config),
    Log1 = write_n(1, 6, 1, Log0),
    Pred = fun (L) ->
                   {5, 1} == ra_log:last_written(L)
           end,
    Log2 = assert_log_events(Log1, Pred, 2000),
    6 = ra_log:next_index(Log2),
    {5, 1} = ra_log:last_index_term(Log2),
    ra_log_wal:force_roll_over(ra_log_wal),
    Log2b = deliver_all_log_events(Log2, 500),
    % reverts last index to a previous index
    % needs to be done if a new leader sends an empty AER
    {ok, Log3} = ra_log:set_last_index(3, Log2b),
    {3, 1} = ra_log:last_written(Log3),
    4 = ra_log:next_index(Log3),
    {3, 1} = ra_log:last_index_term(Log3),
    O = ra_log:overview(Log3),
    ct:pal("o ~p", [O]),
    {ok, Log4} = ra_log:write_sparse({7, 1, hi}, undefined, Log3),
    {ok, Log5} = ra_log:write_sparse({17, 1, hi}, 7, Log4),
    Log5b = deliver_all_log_events(Log5, 500),
    O2 = ra_log:overview(Log5b),
    ?assertMatch(#{range := {0, 17},
                   mem_table_range := {7, 17}},
                 O2),

    %% try overwrite again
    {ok, Log6} = ra_log:set_last_index(3, Log5b),
    {3, 1} = ra_log:last_index_term(Log6),
    {ok, Log7} = ra_log:write_sparse({7, 1, hi}, undefined, Log6),
    {ok, Log8} = ra_log:write_sparse({17, 1, hi}, 7, Log7),
    Log = deliver_all_log_events(Log8, 500),
    O5 = ra_log:overview(Log),
    ct:pal("o ~p", [O5]),
    ok.

last_index_reset_before_written(Config) ->
    Log0 = ra_log_init(Config),
    Log1 = write_n(1, 5, 1, Log0),
    #{mem_table_range := {0, 4}} = ra_log:overview(Log1),
    {0, 0} = ra_log:last_written(Log1),
    5 = ra_log:next_index(Log1),
    {4, 1} = ra_log:last_index_term(Log1),
    % reverts last index to a previous index
    % needs to be done if a new leader sends an empty AER
    {ok, Log2} = ra_log:set_last_index(3, Log1),
    % #{cache_size := 3} = ra_log:overview(Log2),
    {0, 0} = ra_log:last_written(Log2),
    4 = ra_log:next_index(Log2),
    {3, 1} = ra_log:last_index_term(Log2),
    %% deliver written events should not allow the last_written to go higher
    %% than the reset
    Log3 = assert_log_events(Log2, fun (L) ->
                                           {3, 1} == ra_log:last_written(L)
                                   end),
    4 = ra_log:next_index(Log3),
    {3, 1} = ra_log:last_index_term(Log3),
    ok.

recovery(Config) ->
    Log0 = ra_log_init(Config),
    {0, 0} = ra_log:last_index_term(Log0),
    Log1 = write_and_roll(1, 10, 1, Log0),
    {9, 1} = ra_log:last_index_term(Log1),
    Log2 = write_and_roll(5, 15, 2, Log1),
    {14, 2} = ra_log:last_index_term(Log2),
    Log3 = write_n(15, 21, 3, Log2),
    {20, 3} = ra_log:last_index_term(Log3),
    % Log4 = deliver_all_log_events(Log3, 200),
    Pred = fun (L) ->
                   {20, 3} =:= ra_log:last_index_term(L)
           end,
    Log4 = assert_log_events(Log3, Pred, 2000),
    ra_log:close(Log4),
    application:stop(ra),
    start_ra(Config),

    Log5 = ra_log_init(Config),
    {20, 3} = ra_log:last_index_term(Log5),
    Log6 = validate_fold(1, 4, 1, Log5),
    Log7 = validate_fold(5, 14, 2, Log6),
    Log8 = validate_fold(15, 20, 3, Log7),
    ra_log:close(Log8),

    ok.

recover_many(Config) ->
    Log0 = ra_log_init(Config),
    Log1 = write_n(1, 10000, 1, Log0),
    Pred = fun (L) ->
                   {9999, 1} =:= ra_log:last_index_term(L) andalso
                   {9999, 1} =:= ra_log:last_written(L)
           end,
    Log2 = assert_log_events(Log1, Pred, 2000),
    ra_log:close(Log2),
    application:stop(ra),
    start_ra(Config),
    Log = ra_log_init(Config),
    {9999, 1} = ra_log:last_written(Log),
    {9999, 1} = ra_log:last_index_term(Log),
    ra_log:close(Log),
    ok.

recovery_with_missing_directory(Config) ->
    %% checking that the ra system can be restarted even if a directory
    %% has been deleted with a ra_directory entry still in place.
    logger:set_primary_config(level, debug),
    UId = ?config(uid, Config),
    Log0 = ra_log_init(Config),
    ra_log:close(Log0),

    ServerDataDir = ra_env:server_data_dir(default, UId),
    ok = ra_lib:recursive_delete(ServerDataDir),
    ?assertNot(filelib:is_dir(ServerDataDir)),

    ?assert(ra_directory:is_registered_uid(default, UId)),
    application:stop(ra),
    start_ra(Config),
    ?assertNot(ra_directory:is_registered_uid(default, UId)),

    Log5 = ra_log_init(Config),
    ra_log:close(Log5),
    ok = ra_lib:recursive_delete(ServerDataDir),
    ?assertNot(filelib:is_dir(ServerDataDir)),

    ok.

recovery_with_missing_checkpoints_directory(Config) ->
    %% checking that the ra system can be restarted even if the checkpoints
    %% directory is missing, it will be created the next time the
    %% log is initialised
    logger:set_primary_config(level, debug),
    UId = ?config(uid, Config),
    Log0 = ra_log_init(Config),
    ra_log:close(Log0),

    ServerDataDir = ra_env:server_data_dir(default, UId),
    CheckpointsDir = filename:join(ServerDataDir, "checkpoints"),
    ok = ra_lib:recursive_delete(CheckpointsDir),
    ?assertNot(filelib:is_dir(CheckpointsDir)),

    application:stop(ra),
    start_ra(Config),

    Log5 = ra_log_init(Config),
    ra_log:close(Log5),
    ok = ra_lib:recursive_delete(ServerDataDir),
    ?assertNot(filelib:is_dir(ServerDataDir)),

    ok.

recovery_with_missing_config_file(Config) ->
    %% checking that the ra system can be restarted even when the config
    %% file is missing
    logger:set_primary_config(level, debug),
    UId = ?config(uid, Config),
    Log0 = ra_log_init(Config),
    ra_log:close(Log0),

    ServerDataDir = ra_env:server_data_dir(default, UId),
    ConfigFile = filename:join(ServerDataDir, "config"),
    file:delete(ConfigFile),
    ?assertNot(filelib:is_file(ConfigFile)),

    application:stop(ra),
    start_ra(Config),

    Log5 = ra_log_init(Config),
    ra_log:close(Log5),
    ok = ra_lib:recursive_delete(ServerDataDir),
    ?assertNot(filelib:is_dir(ServerDataDir)),

    ok.

resend_write_lost_in_wal_crash(Config) ->
    Log0 = ra_log_init(Config),
    {0, 0} = ra_log:last_index_term(Log0),
    %% write 1..9
    Log1 = append_n(1, 10, 2, Log0),
    Log2 = assert_log_events(Log1, fun (L) ->
                                           {9, 2} == ra_log:last_written(L)
                                   end),
    WalPid = whereis(ra_log_wal),
    %% suspend wal, write an entry then kill it
    erlang:suspend_process(WalPid),
    Log2b = append_n(10, 11, 2, Log2),
    exit(WalPid, kill),
    wait_for_wal(WalPid),
    %% write 11..12 which should trigger resend
    Log3 = append_n(11, 13, 2, Log2b),
    Log4 = receive
               {ra_log_event, {resend_write, 10} = Evt} ->
                   ct:pal("resend"),
                   element(1, ra_log:handle_event(Evt, Log3));
               {ra_log_event, {written, 2, {11, 12}}} ->
                   ct:fail("unexpected gappy write!!")
           after 500 ->
                     flush(),
                     ct:fail(resend_write_timeout)
           end,
    Log5 = ra_log:append({13, 2, banana}, Log4),
    Log6 = assert_log_events(Log5, fun (L) ->
                                           {13, 2} == ra_log:last_written(L)
                                   end),
    {[_, _, _, _, _], _} = ra_log_take(9, 14, Log6),
    ra_log:close(Log6),

    ok.

resend_after_written_event_lost_in_wal_crash(Config) ->
    Log0 = ra_log_init(Config),
    {0, 0} = ra_log:last_index_term(Log0),
    %% write 1..9
    Log1 = append_n(1, 10, 2, Log0),
    Log2 = assert_log_events(Log1, fun (L) ->
                                           {9, 2} == ra_log:last_written(L)
                                   end),
    WalPid = whereis(ra_log_wal),
    %% suspend wal, write an entry then kill it
    Log2b = append_n(10, 11, 2, Log2),
    receive
        {ra_log_event, {written, 2, [10]}} ->
            %% drop written event to simulate being lost in wal crash
            ok
    after 500 ->
              flush(),
              ct:fail(resend_write_timeout)
    end,
    %% restart wal to get a new pid, shouldn't matter
    exit(WalPid, kill),
    wait_for_wal(WalPid),
    %% write 11..12 which should trigger resend
    Log3 = append_n(11, 12, 2, Log2b),
    Log6 = assert_log_events(Log3, fun (L) ->
                                           {11, 2} == ra_log:last_written(L)
                                   end),
    {[_, _, _], _} = ra_log_take(9, 11, Log6),
    ra_log:close(Log6),
    ok.

resend_write_after_tick(Config) ->
    meck:new(ra_log_wal, [passthrough]),
    WalPid = whereis(ra_log_wal),
    Log0 = ra_log_init(Config),
    {0, 0} = ra_log:last_index_term(Log0),
    meck:expect(ra_log_wal, write, fun (_, _, _, _, _, _) ->
                                           {ok, WalPid}
                                   end),
    Log1 = ra_log:append({1, 2, banana}, Log0),
    %% this append should be lost
    meck:unload(ra_log_wal),
    %% restart wal to get a new wal pid so that the ra_log detects on tick
    %% that the wal process has changed
    restart_wal(),

    Ms = erlang:system_time(millisecond) + 5001,
    Log2 = ra_log:tick(Ms, Log1),
    Log = assert_log_events(Log2, fun (L) ->
                                          {1, 2} == ra_log:last_written(L)
                                  end),
    % ct:pal("overvew ~p", [ra_log:overview(Log)]),
    ra_log:close(Log),
    ok.

wal_crash_recover(Config) ->
    Log0 = ra_log_init(Config, #{resend_window => 1}),
    Log1 = write_n(1, 50, 2, Log0),
    % crash the wal
    ok = proc_lib:stop(ra_log_segment_writer),
    % write something
    timer:sleep(100),
    Log2 = deliver_one_log_events(write_n(50, 75, 2, Log1), 100),
    spawn(fun () -> proc_lib:stop(ra_log_segment_writer) end),
    Log3 = write_n(75, 100, 2, Log2),
    % wait long enough for the resend window to pass
    timer:sleep(1000),
    Log4 = write_n(100, 101, 2, Log3),
    {true, _} = ra_log:exists({100, 2}, Log4),
    Log = assert_log_events(Log4,
                            fun (L) ->
                                    {Exists, _} = ra_log:exists({100, 2}, L),
                                    ct:pal("Exists ~w", [Exists]),
                                    {100, 2} == ra_log:last_written(L)
                            end, 2000000),
    {100, 2} = ra_log:last_written(Log),
    validate_fold(1, 99, 2, Log),
    ok.

wal_crash_with_lost_message_and_log_init(Config) ->
    Log0 = ra_log_init(Config, #{wal => ra_log_wal}),
    {0, 0} = ra_log:last_index_term(Log0),
    % write some entries
    Log1 = append_n(1, 10, 2, Log0),
    Log2 = assert_log_events(Log1, fun (L) ->
                                           {9, 2} == ra_log:last_written(L)
                                   end),
    % simulate wal outage
    WalPid = whereis(ra_log_wal),
    true = ra_log_wal_SUITE:suspend_process(WalPid),

    % append some messages that will be lost
    Log3 = append_n(10, 15, 2, Log2),
    ra_log:close(Log3),
    % kill WAL to ensure lose the transient state keeping track of
    % each writer's last written index
    exit(WalPid, kill),

    wait_for_wal(WalPid),

    Log = ra_log_init(Config, #{wal => ra_log_wal}),
    ?assertEqual({9, 2}, ra_log:last_written(Log)),

    ok.

missed_written_then_write(Config) ->
    Log0 = ra_log_init(Config, #{wal => ra_log_wal}),
    {0, 0} = ra_log:last_index_term(Log0),
    % write some entries
    Log1 = append_n(1, 10, 2, Log0),

    %% lose the written event
    receive
        {ra_log_event,{written, _, _}} -> ok
    after 5000 ->
              ct:fail("written event timeout")
    end,

    Log3 = append_n(10, 15, 2, Log1),
    _Log4 = assert_log_events(Log3, fun (L) ->
                                           {14, 2} == ra_log:last_written(L)
                                   end),
    ok.

missed_written_then_segments_then_write(Config) ->
    Log0 = ra_log_init(Config, #{wal => ra_log_wal}),
    {0, 0} = ra_log:last_index_term(Log0),
    % write some entries
    Log1 = append_n(1, 10, 2, Log0),

    %% lose the written event
    receive
        {ra_log_event,{written, _, _}} -> ok
    after 5000 ->
              ct:fail("written event timeout")
    end,

    ra_log_wal:force_roll_over(ra_log_wal),
    Log2 = assert_log_events(Log1,
                             fun (L) ->
                                     #{segments_range := SR,
                                       num_pending := Pnd,
                                       last_written_index_term := LWIT} =
                                         ra_log:overview(L),
                                     SR == {0,9} andalso
                                     Pnd == 0 andalso
                                     LWIT == {9, 2}
                             end),
    Log3 = append_n(10, 15, 2, Log2),
    _Log4 = assert_log_events(Log3, fun (L) ->
                                           {14, 2} == ra_log:last_written(L)
                                   end),
    ok.

wal_down_read_availability(Config) ->
    Log0 = ra_log_init(Config),
    Log1 = append_n(1, 10, 2, Log0),
    Log2 = assert_log_events(Log1, fun (L) ->
                                           {9, 2} == ra_log:last_written(L)
                                   end),
    [SupPid] = [P || {ra_log_wal_sup, P, _, _}
                     <- supervisor:which_children(ra_log_sup)],
    ok = supervisor:terminate_child(SupPid, ra_log_wal),
    {Entries, _} = ra_log_take(0, 10, Log2),
    ?assert(length(Entries) =:= 10),
    ok.

wal_down_append_throws(Config) ->
    Log0 = ra_log_init(Config),
    ?assert(ra_log:can_write(Log0)),
    [SupPid] = [P || {ra_log_wal_sup, P, _, _}
                     <- supervisor:which_children(ra_log_sup)],
    ok = supervisor:terminate_child(SupPid, ra_log_wal),
    ?assert(not ra_log:can_write(Log0)),
    ?assertError(wal_down, ra_log:append({1, 1, hi}, Log0)),
    ok.

wal_down_write_returns_error_wal_down(Config) ->
    Log0 = ra_log_init(Config),
    [SupPid] = [P || {ra_log_wal_sup, P, _, _}
                     <- supervisor:which_children(ra_log_sup)],
    ok = supervisor:terminate_child(SupPid, ra_log_wal),
    {error, wal_down} = ra_log:write([{1, 1, hi}], Log0),
    ok.

detect_lost_written_range(Config) ->
    Log0 = ra_log_init(Config, #{wal => ra_log_wal}),
    {0, 0} = ra_log:last_index_term(Log0),
    % write some entries
    Log1 = append_n(1, 10, 2, Log0),
    Log2 = assert_log_events(Log1, fun (L) ->
                                           {9, 2} == ra_log:last_written(L)
                                   end),
    % simulate wal outage
    WalPid = whereis(ra_log_wal),
    true = ra_log_wal_SUITE:suspend_process(WalPid),

    % append some messages that will be lost
    Log3 = append_n(10, 15, 2, Log2),

    % kill WAL to ensure lose the transient state keeping track of
    % each writer's last written index
    exit(WalPid, kill),

    wait_for_wal(WalPid),

    % append some more stuff
    Log4 = append_n(15, 20, 2, Log3),
    Log5 = assert_log_events(Log4, fun (L) ->
                                           {19, 2} == ra_log:last_written(L)
                                   end),
    % validate no writes were lost and can be recovered
    {Entries, _} = ra_log_take(0, 20, Log5),
    ?assertEqual(20, length(Entries)),
    ra_log:close(Log5),
    ra_log_wal:force_roll_over(ra_log_wal),
    timer:sleep(1000),
    Log = ra_log_init(Config),
    {19, 2} = ra_log:last_written(Log5),
    {RecoveredEntries, _} = ra_log_take(0, 20, Log),
    ?assert(length(Entries) =:= 20),
    ?assert(length(RecoveredEntries) =:= 20),
    Entries = RecoveredEntries,
    ok.



snapshot_written_after_installation(Config) ->
    Log0 = ra_log_init(Config, #{min_snapshot_interval => 2}),
    %% log 1 .. 9, should create a single segment
    Log1 = write_and_roll(1, 10, 1, Log0),
    {Log2, Effs} = ra_log:update_release_cursor(5, #{}, macctx(),
                                                <<"one-five">>, Log1),
    run_effs(Effs),
    DelayedSnapWritten = receive
                             {ra_log_event, {snapshot_written, {5, 1}, _,
                                             snapshot, _} = Evt} ->
                                 Evt
                         after 1000 ->
                                   flush(),
                                   exit(snapshot_written_timeout)
                         end,

    Meta = meta(15, 2, [?N1]),
    Context = #{},
    Chunk = create_snapshot_chunk(Config, Meta, Context),
    SnapState0 = ra_log:snapshot_state(Log2),
    {ok, SnapState1} = ra_snapshot:begin_accept(Meta, SnapState0),
    Machine = {machine, ?MODULE, #{}},
    {SnapState, _, LiveIndexes, AEffs} = ra_snapshot:complete_accept(Chunk, 1, Machine,
                                                                     SnapState1),
    run_effs(AEffs),
    {ok, Log3, _} = ra_log:install_snapshot({15, 2}, ?MODULE, LiveIndexes,
                                            ra_log:set_snapshot_state(SnapState, Log2)),
    %% write some more to create another segment
    Log4 = write_and_roll(16, 20, 2, Log3),
    {Log5, Efx4} = ra_log:handle_event(DelayedSnapWritten, Log4),
    {19, _} = ra_log:last_index_term(Log5),
    {19, _} = ra_log:last_written(Log5),

    [begin
         case E of
             {delete_snapshot, Dir, S} ->
                 ra_snapshot:delete(Dir, S);
             _ ->
                 ok
         end
     end || E <- Efx4],

    %% assert there is no pending snapshot
    ?assertEqual(undefined, ra_snapshot:pending(ra_log:snapshot_state(Log5))),

    _ = ra_log:close(ra_log_init(Config, #{min_snapshot_interval => 2})),

    ok.

oldcheckpoints_deleted_after_snapshot_install(Config) ->
    Log0 = ra_log_init(Config, #{min_snapshot_interval => 2,
                                 min_checkpoint_interval => 2}),
    %% log 1 .. 9, should create a single segment
    Log1 = write_and_roll(1, 10, 1, Log0),
    {Log2, Effs} = ra_log:checkpoint(5, #{}, macctx(), <<"one-five">>, Log1),
    run_effs(Effs),
    DelayedSnapWritten = receive
                             {ra_log_event, {snapshot_written, {5, 1}, _,
                                             checkpoint, _} = Evt} ->
                                 Evt
                         after 1000 ->
                                   flush(),
                                   exit(snapshot_written_timeout)
                         end,
    {Log3, Efx4} = ra_log:handle_event(DelayedSnapWritten, Log2),

    Meta = meta(15, 2, [?N1]),
    Context = #{},
    Chunk = create_snapshot_chunk(Config, Meta, Context),
    SnapState0 = ra_log:snapshot_state(Log3),
    {ok, SnapState1} = ra_snapshot:begin_accept(Meta, SnapState0),
    % {ok, SnapState, AcceptEffs} =
    %     ra_snapshot:accept_chunk(Chunk, 1, last, SnapState1),
    Machine = {machine, ?MODULE, #{}},
    {SnapState, _, LiveIndexes, AEffs} = ra_snapshot:complete_accept(Chunk, 1, Machine,
                                                                     SnapState1),
    run_effs(AEffs),
    {ok, Log4, Effs4} = ra_log:install_snapshot({15, 2}, ?MODULE, LiveIndexes,
                                                ra_log:set_snapshot_state(SnapState, Log3)),
    ?assert(lists:any(fun (E) -> element(1, E) == delete_snapshot end, Effs4)),
    %% write some more to create another segment
    Log5 = write_and_roll(16, 20, 2, Log4),
    {19, _} = ra_log:last_index_term(Log5),
    {19, _} = ra_log:last_written(Log5),

    [begin
         case E of
             {delete_snapshot, Dir, S} ->
                 ra_snapshot:delete(Dir, S);
             _ ->
                 ok
         end
     end || E <- Efx4],

    SnapStateAfter1 = ra_log:snapshot_state(Log5),
    {false, SnapsStateAfter, _} =
        ra_snapshot:promote_checkpoint(19, SnapStateAfter1),
    %% assert there is no pending snapshot as checkpoint promotion should
    %% not have promoted anything
    ?assertEqual(undefined, ra_snapshot:pending(SnapsStateAfter)),

    _ = ra_log:close(ra_log_init(Config, #{min_snapshot_interval => 2})),

    ok.

snapshot_installation_with_live_indexes(Config) ->
    Log0 = ra_log_init(Config),
    {0, 0} = ra_log:last_index_term(Log0),
    Log1 = assert_log_events(write_n(1, 10, 2, Log0),
                             fun (L) ->
                                     LW = ra_log:last_written(L),
                                     {9, 2} == LW
                             end),
    Log2 = Log1,

    %% create snapshot chunk
    Meta = meta(15, 2, [?N1]),
    Chunk = create_snapshot_chunk(Config, Meta, [2, 9, 14], [2, 9, 14], #{}),
    SnapState0 = ra_log:snapshot_state(Log2),
    {ok, SnapState1} = ra_snapshot:begin_accept(Meta, SnapState0),
    Machine = {machine, ?MODULE, #{}},

    %% write  a sparse one
    {ok, Log2b} = ra_log:write_sparse({14, 2, <<>>}, 9, Log2),
    {SnapState, _, LiveIndexes, AEffs} = ra_snapshot:complete_accept(Chunk, 1, Machine,
                                                                     SnapState1),
    run_effs(AEffs),
    {ok, Log3, Effs4} = ra_log:install_snapshot({15, 2}, ?MODULE, LiveIndexes,
                                                ra_log:set_snapshot_state(SnapState, Log2b)),


    run_effs(Effs4),
    {15, _} = ra_log:last_index_term(Log3),
    {15, _} = ra_log:last_written(Log3),
    %% write the next index, bearning in mind the last index the WAL saw
    %% was 14
    {ok, Log4} = ra_log:write([{16, 2, <<>>}], Log3),
    Log5 = assert_log_events(Log4,
                            fun (L) ->
                                    LW = ra_log:last_written(L),
                                    {16, 2} == LW
                            end),
    ra_log_wal:force_roll_over(ra_log_wal),
    Log = assert_log_events(Log5,
                            fun (L) ->
                                    #{mem_table_range := R} = ra_log:overview(L),
                                    R == undefined
                            end),
    ct:pal("o ~p", [ra_log:overview(Log)]),
    UId = ?config(uid, Config),
    ?assertEqual(LiveIndexes, ra_log_snapshot_state:live_indexes(
                                ra_log_snapshot_state, UId)),
    ra_log:close(Log),
    flush(),
    _LogAfter = ra_log_init(Config),
    %% validate recovery recovers the live indexes correctly
    ?assertEqual(LiveIndexes, ra_log_snapshot_state:live_indexes(
                                ra_log_snapshot_state, UId)),
    ok.

snapshot_installation(Config) ->
    Log0 = ra_log_init(Config),
    {0, 0} = ra_log:last_index_term(Log0),
    Log1 = assert_log_events(write_n(1, 10, 2, Log0),
                             fun (L) ->
                                     LW = ra_log:last_written(L),
                                     {9, 2} == LW
                             end),

    Log2 = Log1,

    %% create snapshot chunk
    Meta = meta(15, 2, [?N1]),
    Chunk = create_snapshot_chunk(Config, Meta, #{}),
    SnapState0 = ra_log:snapshot_state(Log2),
    {ok, SnapState1} = ra_snapshot:begin_accept(Meta, SnapState0),
    Machine = {machine, ?MODULE, #{}},
    {SnapState, _, LiveIndexes, AEffs} = ra_snapshot:complete_accept(Chunk, 1, Machine,
                                                                     SnapState1),
    run_effs(AEffs),
    {ok, Log3, Effs4} = ra_log:install_snapshot({15, 2}, ?MODULE, LiveIndexes,
                                                ra_log:set_snapshot_state(SnapState, Log2)),

    run_effs(Effs4),
    {15, _} = ra_log:last_index_term(Log3),
    {15, _} = ra_log:last_written(Log3),
    #{mem_table_range := undefined} = ra_log:overview(Log3),
    ra_log_wal:force_roll_over(ra_log_wal),
    Log4 = deliver_all_log_events(Log3, 100),
    {15, _} = ra_log:last_index_term(Log4),
    {15, _} = ra_log:last_written(Log4),
    #{mem_table_range := undefined} = ra_log:overview(Log4),

    % after a snapshot we need a "truncating write" that ignores missing
    % indexes
    Log5 = write_n(16, 20, 2, Log4),
    %% check that the log can be reset to a pending write index
    {ok, Log5b} = ra_log:set_last_index(19, Log5),

    {[], _} = ra_log_take(1, 9, Log5b),
    {[_, _], _} = ra_log_take(16, 17, Log5b),
    Log6 = assert_log_events(Log5b, fun (L) ->
                                           {19, 2} == ra_log:last_written(L)
                                   end),
    {[], _} = ra_log_take(1, 9, Log6),
    {[_, _], _} = ra_log_take(16, 17, Log6),
    ra_log_wal:force_roll_over(ra_log_wal),
    {[], _} = ra_log_take(1, 9, Log6),
    {[_, _], _} = ra_log_take(16, 17, Log6),
    Log = deliver_all_log_events(Log6, 100),
    {[], _} = ra_log_take(1, 9, Log),
    {[_, _], _} = ra_log_take(16, 17, Log),
    ok.

append_after_snapshot_installation_with_live_indexes(Config) ->
    logger:set_primary_config(level, all),
    %% simulates scenario where a node becomes leader after receiving a
    %% snapshot with live indexes
    % write a few entries
    % simulate outage/ message loss
    % write snapshot for entry not seen
    % then write entries
    Log0 = ra_log_init(Config), {0, 0} = ra_log:last_index_term(Log0),
    % Log1 = write_n(1, 10, 2, Log0),
    Log1 = assert_log_events(write_n(1, 10, 2, Log0),
                             fun (L) ->
                                     {9, 2} == ra_log:last_written(L)
                             end),
    %% do snapshot
    Meta = meta(15, 2, [?N1]),
    LiveIndexes = [5,1],
    Chunk = create_snapshot_chunk(Config, Meta, LiveIndexes, LiveIndexes, #{}),
    SnapState0 = ra_log:snapshot_state(Log1),
    {ok, SnapState1} = ra_snapshot:begin_accept(Meta, SnapState0),
    Machine = {machine, ?MODULE, #{}},
    {SnapState, _, LiveIndexes, AEffs} = ra_snapshot:complete_accept(Chunk, 1, Machine,
                                                                     SnapState1),
    run_effs(AEffs),
    {ok, Log2, Effs4} = ra_log:install_snapshot({15, 2}, ?MODULE, LiveIndexes,
                                                ra_log:set_snapshot_state(SnapState, Log1)),
    run_effs(Effs4),
    {15, _} = ra_log:last_index_term(Log2),
    {15, _} = ra_log:last_written(Log2),

    % after a snapshot we need a "truncating write" that ignores missing
    % indexes
    Log3 = append_n(16, 20, 3, Log2),
    Log = assert_log_events(Log3, fun (L) ->
                                          {19, 3} == ra_log:last_written(L)
                                  end),

    ?assertMatch({[_, _], _}, ra_log:sparse_read([1, 5], Log)),
    % {[], _} = ra_log_take(1, 9, Log),
    {[_, _], _} = ra_log_take(16, 17, Log),
    ok.

append_after_snapshot_installation(Config) ->
    logger:set_primary_config(level, all),
    %% simulates scenario where a node becomes leader after receiving a
    %% snapshot
    % write a few entries
    % simulate outage/ message loss
    % write snapshot for entry not seen
    % then write entries
    Log0 = ra_log_init(Config), {0, 0} = ra_log:last_index_term(Log0),
    % Log1 = write_n(1, 10, 2, Log0),
    Log1 = assert_log_events(write_n(1, 10, 2, Log0),
                             fun (L) ->
                                     {9, 2} == ra_log:last_written(L)
                             end),
    %% do snapshot
    Meta = meta(15, 2, [?N1]),
    Chunk = create_snapshot_chunk(Config, Meta, #{}),
    SnapState0 = ra_log:snapshot_state(Log1),
    {ok, SnapState1} = ra_snapshot:begin_accept(Meta, SnapState0),
    Machine = {machine, ?MODULE, #{}},
    {SnapState, _, LiveIndexes, AEffs} = ra_snapshot:complete_accept(Chunk, 1, Machine,
                                                                     SnapState1),
    run_effs(AEffs),
    {ok, Log2, Effs4} = ra_log:install_snapshot({15, 2}, ?MODULE, LiveIndexes,
                                                ra_log:set_snapshot_state(SnapState, Log1)),
    run_effs(Effs4),
    {15, _} = ra_log:last_index_term(Log2),
    {15, _} = ra_log:last_written(Log2),

    % after a snapshot we need a "truncating write" that ignores missing
    % indexes
    Log3 = append_n(16, 20, 2, Log2),
    Log = assert_log_events(Log3, fun (L) ->
                                          {19, 2} == ra_log:last_written(L)
                                  end),
    {[], _} = ra_log_take(1, 9, Log),
    {[_, _], _} = ra_log_take(16, 17, Log),
    ok.

release_cursor_after_snapshot_installation(Config) ->
    Log0 = ra_log_init(Config, #{min_snapshot_interval => 0}),
    {0, 0} = ra_log:last_index_term(Log0),
    Log1 = assert_log_events(write_n(1, 16, 2, Log0),
                             fun (L) ->
                                     LW = ra_log:last_written(L),
                                     {15, 2} == LW
                             end),

    Log2 = Log1,

    %% create snapshot chunk
    Meta = meta(15, 2, [?N1]),
    Chunk = create_snapshot_chunk(Config, Meta, [1, 5, 10], [1, 5, 10], #{}),
    SnapState0 = ra_log:snapshot_state(Log2),
    {ok, SnapState1} = ra_snapshot:begin_accept(Meta, SnapState0),
    Machine = {machine, ?MODULE, #{}},
    {SnapState, _, LiveIndexes, AEffs} = ra_snapshot:complete_accept(Chunk, 1, Machine,
                                                                     SnapState1),
    run_effs(AEffs),
    {ok, Log3, Effs4} = ra_log:install_snapshot({15, 2}, ?MODULE, LiveIndexes,
                                                ra_log:set_snapshot_state(SnapState, Log2)),

    run_effs(Effs4),
    {15, 2} = ra_snapshot:current(ra_log:snapshot_state(Log3)),

    %% Write some entries
    Log4 = assert_log_events(write_n(16, 20, 2, Log3),
                             fun (L) ->
                                     LW = ra_log:last_written(L),
                                     {19, 2} == LW
                             end),

    %% then take a snapshot
    {Log5, Effs5} = ra_log:update_release_cursor(19, #{?N1 => new_peer(),
                                                       ?N2 => new_peer()},
                                                 macctx(), [1, 5, 10, 17], Log4),

    run_effs(Effs5),
    %% ensure snapshot index has been updated and 1 segment deleted
    Log = assert_log_events(Log5,
                             fun (L) ->
                                     {19, 2} == ra_log:snapshot_index_term(L)
                                     % andalso
                                     % length(find_segments(Config)) == 1
                             end),

    ct:pal("Log ~p", [Log]),

    ok.

written_event_after_snapshot_installation(Config) ->
    logger:set_primary_config(level, all),
    %% simulates scenario where a server receives a written event from the wal
    %% immediately after a snapshot has been installed and the written event
    %% is for a past index.
    Log0 = ra_log_init(Config),
    {0, 0} = ra_log:last_index_term(Log0),
    %% write 10 entries
    %% but only process events for 9
    Log1 = write_n(1, 10, 2, Log0),
    SnapIdx = 10,
    %% do snapshot in
    Meta = meta(SnapIdx, 2, [?N1]),
    Chunk = create_snapshot_chunk(Config, Meta, #{}),
    SnapState0 = ra_log:snapshot_state(Log1),
    {ok, SnapState1} = ra_snapshot:begin_accept(Meta, SnapState0),
    Machine = {machine, ?MODULE, #{}},
    {SnapState, _, LiveIndexes, AEffs} = ra_snapshot:complete_accept(Chunk, 1, Machine,
                                                                     SnapState1),
    run_effs(AEffs),
    {ok, Log2, Effs4} = ra_log:install_snapshot({SnapIdx, 2}, ?MODULE, LiveIndexes,
                                                ra_log:set_snapshot_state(SnapState, Log1)),
    run_effs(Effs4),
    {SnapIdx, _} = ra_log:last_index_term(Log2),
    {SnapIdx, _} = ra_log:last_written(Log2),
    NextIdx = SnapIdx + 1,
    NextIdx = ra_log:next_index(Log2),
    {undefined, _} = ra_log:fetch_term(SnapIdx, Log2),
    {SnapIdx, 2} = ra_log:snapshot_index_term(Log2),

    %% process "old" written events
    Log3 = assert_log_events(Log2,
                             fun (L) ->
                                     {SnapIdx, 2} == ra_log:last_written(L)
                             end),
    {SnapIdx, _} = ra_log:last_index_term(Log3),
    {SnapIdx, _} = ra_log:last_written(Log3),
    NextIdx = ra_log:next_index(Log3),
    {undefined, _} = ra_log:fetch_term(SnapIdx, Log3),
    {SnapIdx, 2} = ra_log:snapshot_index_term(Log3),
    ok.

update_release_cursor(Config) ->
    % ra_log should initiate shapshot if segments can be released
    Log0 = ra_log_init(Config),
    % beyond 128 limit - should create two segments
    Log1 = assert_log_events(append_and_roll_no_deliver(1, 150, 2, Log0),
                             fun (L) ->
                                     case ra_log:overview(L) of
                                         #{num_segments := 2} ->
                                             true;
                                         _ ->
                                             false
                                     end
                             end),
    % assert there are two segments at this point
    [_, _] = find_segments(Config),
    % update release cursor to the last entry of the first segment
    {Log2, Effs} = ra_log:update_release_cursor(127, #{?N1 => new_peer(),
                                                       ?N2 => new_peer()},
                                                macctx(), initial_state, Log1),

    run_effs(Effs),
    %% ensure snapshot index has been updated and 1 segment deleted
    Log3 = assert_log_events(Log2,
                             fun (L) ->
                                     {127, 2} == ra_log:snapshot_index_term(L) andalso
                                     length(find_segments(Config)) == 1
                             end),
    %% now the snapshot_written should have been delivered and the
    %% snapshot state table updated
    UId = ?config(uid, Config),
    127 = ra_log_snapshot_state:snapshot(ra_log_snapshot_state, UId),
    % this should delete a single segment
    ct:pal("Log3 ~p", [Log3]),
    Log3b = validate_fold(128, 149, 2, Log3),
    % update the release cursor all the way
    {Log4, Effs2} = ra_log:update_release_cursor(149, #{?N1 => new_peer(),
                                                        ?N2 => new_peer()},
                                                 macctx(), initial_state, Log3b),
    run_effs(Effs2),
    Log5 = assert_log_events(Log4,
                             fun (L) ->
                                     {149, 2} == ra_log:snapshot_index_term(L)
                             end),

    149 = ra_log_snapshot_state:snapshot(ra_log_snapshot_state, UId),

    % only one segment should remain as the segment writer always keeps
    % at least one segment for each
    ra_lib:retry(fun () ->
                         1 == length(find_segments(Config))
                 end, 10, 100),

    % append a few more items
    Log = assert_log_events(append_and_roll_no_deliver(150, 155, 2, Log5),
                            fun (L) ->
                                    {154, 2} == ra_log:last_written(L)
                            end),
    validate_fold(150, 154, 2, Log),
    % assert there is only one segment - the current
    % snapshot has been confirmed.
    ra_log_segment_writer:await(ra_log_segment_writer),
    [_] = find_segments(Config),

    ok.

update_release_cursor_with_machine_version(Config) ->
    % ra_log should initiate shapshot if segments can be released
    Log0 = ra_log_init(Config, #{min_snapshot_interval => 64}),
    % beyond 128 limit - should create two segments
    Log1 = append_and_roll(1, 150, 2, Log0),
    timer:sleep(300),
    % assert there are two segments at this point
    [_, _] = find_segments(Config),
    % update release cursor to the last entry of the first segment
    {Log2, Effs} = ra_log:update_release_cursor(127, #{?N1 => new_peer(),
                                                       ?N2 => new_peer()},
                                                {0, ?MODULE},
                                                initial_state, Log1),
    run_effs(Effs),
    Log = assert_log_events(Log2,
                            fun (L) ->
                                    {127, 2} == ra_log:snapshot_index_term(L)
                            end),
    SnapState = ra_log:snapshot_state(Log),
    %% assert the version is in the snapshot state meta data
    CurrentDir = ra_snapshot:current_snapshot_dir(SnapState),
    {ok, Meta} = ra_snapshot:read_meta(ra_log_snapshot, CurrentDir),
    ?assertMatch(#{index := 127, machine_version := 0}, Meta),
    ok.

missed_mem_table_entries_are_deleted_at_next_opportunity(Config) ->
    % ra_log should initiate shapshot if segments can be released
    Log00 = ra_log_init(Config),
    % assert there are no segments at this point
    [] = find_segments(Config),

    % create a segment
    Log0 = append_and_roll(1, 130, 2, Log00),
    Log1 = assert_log_events(Log0,
                             fun (L) ->
                                     #{num_segments := Segs} = ra_log:overview(L),
                                     Segs > 0
                             end),
    % and another but don't notify ra_server
    Log2 = append_and_roll_no_deliver(130, 150, 2, Log1),
    % deliver only written events
    Log3 = deliver_written_log_events(Log2, 500),
    % simulate the segments events getting lost due to crash
    % TODO: mt: should we not re-init the log here?
    timer:sleep(1500),
    flush(),
    % empty_mailbox(500),
    % although this has been flushed to disk the ra_server wasn't available
    % to clean it up.
    #{mem_table_range := {130, 149}} = ra_log:overview(Log3),
    ra_log:close(Log3),

    % append and roll some more entries
    Log4 = deliver_all_log_events(append_and_roll(150, 155, 2,
                                                  ra_log_init(Config)), 200),

    % TODO: validate reads
    Log5 = validate_fold(1, 154, 2, Log4),

    % then update the release cursor
    {Log6, Effs2} = ra_log:update_release_cursor(154, #{?N1 => new_peer(),
                                                        ?N2 => new_peer()},
                                                 macctx(), initial_state, Log5),
    run_effs(Effs2),
    ct:pal("Effs2 ~p", [Effs2]),
    ct:pal("find segments ~p", [find_segments(Config)]),
    Log7 = deliver_log_events_cond(Log6,
                                   fun (_) ->
                                           case find_segments(Config) of
                                               [_] -> true;
                                               _ -> false
                                           end
                                   end, 100),
    %% dummy call to ensure deletes have completed
    gen_server:call(ra_log_ets, dummy),
    #{mem_table_range := undefined,
      mem_table_info := #{size := 0}} = ra_log:overview(Log7),
    ok.

await_cond(_Fun, 0) ->
    false;
await_cond(Fun, N) ->
    case Fun() of
        true -> true;
        false ->
            timer:sleep(250),
            await_cond(Fun, N -1)
    end.

transient_writer_is_handled(Config) ->
    Self = self(),
    UId2 = <<(?config(uid, Config))/binary, "sub_proc">>,
    _Pid = spawn(fun () ->
                         ra_directory:register_name(default, UId2,
                                                    self(), undefined,
                                                    sub_proc, sub_proc),
                         Log0 = ra_log_init(Config, #{uid => UId2}),
                         Log1 = append_n(1, 10, 2, Log0),
                         % ignore events
                         Log2 = deliver_all_log_events(Log1, 500),
                         ra_log:close(Log2),
                         Self ! done,
                         ok
                 end),
    receive done -> ok
    after 2000 -> exit(timeout)
    end,
    UId2 = ra_directory:unregister_name(default, UId2),
    _ = ra_log_init(Config),
    ct:pal("~p", [ra_directory:list_registered(default)]),
    ok.

open_segments_limit(Config) ->
    Max = 3,
    Log0 = ra_log_init(Config, #{max_open_segments => Max}),
    % write a few entries
    Log1 = append_and_roll(1, 2000, 1, Log0),
    %% this should result in a few segments
    %% validate as this read all of them
    Log1b = wait_for_segments(Log1, 5000),
    Log2 = validate_fold(1, 1999, 1, Log1b),
    Segs = find_segments(Config),
    #{open_segments := Open}  = ra_log:overview(Log2),
    ?assert(length(Segs) > Max),
    ?assert(Open =< Max),
    ok.

write_config(Config) ->
    C = #{cluster_name => ?MODULE,
          id => {?MODULE, node()},
          uid => <<"blah">>,
          log_init_args => #{uid => <<"blah">>},
          initial_members => [],
          machine => {module, ?MODULE, #{}}},
    Log0 = ra_log_init(Config),
    ok = ra_log:write_config(C, Log0),

    ?assertMatch({ok, C}, ra_log:read_config(Log0)),

    ok.

sparse_write(Config) ->
    Log00 = ra_log_init(Config),
    % assert there are no segments at this point
    [] = find_segments(Config),

    % create a segment

    Indexes = lists:seq(1, 10, 2),
    Log0 = write_sparse(Indexes, 0, Log00),
    Log0b = assert_log_events(Log0,
                             fun (L) ->
                                     #{num_pending := Num} = ra_log:overview(L),
                                     Num == 0
                             end),
    {Res0, _Log} = ra_log:sparse_read(Indexes, Log0b),
    ?assertMatch([{1, _, _},
                  {3, _, _},
                  {5, _, _},
                  {7, _, _},
                  {9, _, _}], Res0),

    %% roll wal and assert we can read sparsely from segments
    ra_log_wal:force_roll_over(ra_log_wal),
    Log1 = assert_log_events(Log0b,
                             fun (L) ->
                                     #{num_segments := Segs} = ra_log:overview(L),
                                     Segs > 0
                             end),

    {Res, Log2} = ra_log:sparse_read(Indexes, Log1),
    ?assertMatch([{1, _, _},
                  {3, _, _},
                  {5, _, _},
                  {7, _, _},
                  {9, _, _}], Res),

    ct:pal("ov: ~p", [ra_log:overview(Log2)]),

    %% the snapshot is written after live index replication
    Meta = meta(15, 2, [?N1]),
    Context = #{},
    %% passing all Indexes but first one as snapshot state
    LiveIndexes = tl(Indexes),
    Chunk = create_snapshot_chunk(Config, Meta, LiveIndexes, LiveIndexes, Context),
    SnapState0 = ra_log:snapshot_state(Log2),
    {ok, SnapState1} = ra_snapshot:begin_accept(Meta, SnapState0),
    Machine = {machine, ?MODULE, #{}},
    {SnapState, _, LiveIndexesSeq, AEffs} = ra_snapshot:complete_accept(Chunk, 1,
                                                                        Machine,
                                                                        SnapState1),
    ?assertEqual(LiveIndexes, lists:reverse(ra_seq:expand(LiveIndexesSeq))),
    run_effs(AEffs),
    Log3 = ra_log:set_snapshot_state(SnapState, Log2),
    {ok, Log4, _} = ra_log:install_snapshot({15, 2}, ?MODULE,
                                            LiveIndexes, Log3),

    ct:pal("overview Log4 ~p", [ra_log:overview(Log4)]),
    ?assertEqual(16, ra_log:next_index(Log4)),

    {ok, Log} = ra_log:write([{16, 1, <<>>}], Log4),
    {ResFinal, _} = ra_log:sparse_read(LiveIndexes, Log),
    ?assertMatch([{3, _, _},
                  {5, _, _},
                  {7, _, _},
                  {9, _, _}], ResFinal),

    ReInitLog= ra_log_init(Config),
    {ResReInit, _} = ra_log:sparse_read(LiveIndexes, ReInitLog),
    ?assertMatch([{3, _, _},
                  {5, _, _},
                  {7, _, _},
                  {9, _, _}], ResReInit),
    ok.

overwritten_segment_is_cleared(Config) ->
    Log0 = ra_log_init(Config, #{}),
    % write a few entries
    Log1 = write_and_roll(1, 256, 1, Log0),
    Log2 = assert_log_events(Log1,
                             fun(L) ->
                                     #{num_segments := N} = ra_log:overview(L),
                                     N == 2
                             end),
    Log3 = write_and_roll(128, 256 + 128, 2, Log2),
    UId = ?config(uid, Config),
    Log = assert_log_events(Log3,
                            fun(L) ->
                                    #{num_segments := N} = ra_log:overview(L),
                                    N == 3 andalso
                                    3 == length(ra_log_segment_writer:my_segments(ra_log_segment_writer, UId))
                            end),

    ct:pal("Log overview ~p", [ra_log:overview(Log)]),
    ok.

overwritten_segment_is_cleared_on_init(Config) ->
    Log0 = ra_log_init(Config, #{}),
    % write a few entries
    Log1 = write_and_roll(1, 256, 1, Log0),
    Log2 = assert_log_events(Log1,
                             fun(L) ->
                                     #{num_segments := N} = ra_log:overview(L),
                                     N == 2
                             end),
    Log3 = write_n(128, 256 + 128, 2, Log2),
    ok = ra_log_wal:force_roll_over(ra_log_wal),
    ra_log:close(Log3),
    % _Log3 = write_and_roll(128, 256 + 128, 2, Log2),
    UId = ?config(uid, Config),
    timer:sleep(1000),
    flush(),
    Log = ra_log_init(Config, #{}),

    ct:pal("my segments ~p",
           [ra_log_segment_writer:my_segments(ra_log_segment_writer, UId)]),
    ct:pal("Log overview ~p", [ra_log:overview(Log)]),
    ?assertEqual(3, length(
                      ra_log_segment_writer:my_segments(ra_log_segment_writer, UId))),

    ok.

%% @doc Test that concurrent snapshot installation and compaction don't cause
%% segment leaks or corruption. This tests the race condition where:
%% 1. Follower installs snapshot A (triggers minor compaction)
%% 2. then a major compaction is triggered
%% 3. before major compaction is completed another snapshot is installed
%% 4. Verify that a subsequent minor compaction is performed after the major
%% compaction completes.
concurrent_snapshot_install_and_compaction(Config) ->
    Log00 = ra_log_init(Config, #{}),
    Log0 = assert_log_events(write_and_roll(1, 1000, 1, Log00),
                             fun(L) ->
                                     #{num_segments := N} = ra_log:overview(L),
                                     N == 8
                             end),

    %% Setup: Create a log with multiple segments

    %% Create first snapshot with live indexes
    LiveIndexes1 = ra_seq:from_list([100, 200, 300]),
    SnapIdx1 = 500,
    SnapTerm1 = 1,

    Machine = {machine, ?MODULE, #{}},
    Context = #{},
    CreateSnapshot =
        fun (Meta, LiveIndexes, L0) ->
                Chunk = create_snapshot_chunk(Config, Meta, LiveIndexes,
                                              LiveIndexes1, Context),
                SnapState0 = ra_log:snapshot_state(L0),
                {ok, SnapState1} = ra_snapshot:begin_accept(Meta, SnapState0),
                {SnapState, _, LiveIndexes, AEffs} =
                ra_snapshot:complete_accept(Chunk, 1, Machine, SnapState1),
                {ra_log:set_snapshot_state(SnapState, L0), AEffs}
        end,
    {Log1, AEffs} = CreateSnapshot(meta(500, 1, [?N1]), LiveIndexes1, Log0),
    run_effs(AEffs),
    %% Install first snapshot - this triggers minor compaction
    {ok, Log2, Effects1} = ra_log:install_snapshot({SnapIdx1, SnapTerm1},
                                                   test_machine,
                                                   LiveIndexes1, Log1),

    %% Verify compaction was scheduled
    ?assert(lists:any(fun({next_event, {ra_log_event, {compaction_result, _}}}) -> true;
                         (_) -> false
                      end, Effects1)),
    [{next_event, {ra_log_event, Evt}}] = Effects1,
    {Log3, []} = ra_log:handle_event(Evt, Log2),

    {Log4, Effects3} = ra_log:handle_event(major_compaction, Log3),
    run_effs(Effects3),
    %% Before processing major compaction result, install second snapshot
    LiveIndexes2 = ra_seq:from_list([600, 700]),
    SnapIdx2 = 800,
    SnapTerm2 = 1,
    {Log5, AEffs2} = CreateSnapshot(meta(800, 1, [?N1]), LiveIndexes2, Log4),
    run_effs(AEffs2),
    %% no minor compaction here, i.e. no compaction result as part of effects
    {ok, Log6, []} = ra_log:install_snapshot({SnapIdx2, SnapTerm2},
                                             test_machine,
                                             LiveIndexes2,
                                             Log5),

    %% Now process both compaction results until we have only 4 left
    Log7 = assert_log_events(Log6,
                             fun(L) ->
                                     #{num_segments := N} = ra_log:overview(L),
                                     N == 4
                             end),

    %% Verify log state is consistent
    {SnapIdx2, SnapTerm2} = ra_log:snapshot_index_term(Log7),

    %% Verify no segment leaks by checking all segments are accounted for
    %% Cleanup
    ra_log:close(Log6),
    ok.

init_with_dangling_symlink(Config) ->
    %% This test verifies that ra_log:init handles dangling symlinks correctly.
    %% During compaction, segment files can be replaced with symlinks pointing
    %% to a compacted segment. If the target segment is deleted (e.g., due to
    %% incomplete compaction recovery), the symlink becomes dangling.
    %%
    %% purge_dangling_symlinks should clean these up, but there's a bug where
    %% it uses term_to_binary(File) instead of just File, causing it to check
    %% the wrong filenames and never actually purge dangling symlinks.
    %%
    %% This causes ra_log:init to crash when my_segrefs tries to read info
    %% from a dangling symlink.
    UId = ?config(uid, Config),
    ServerDataDir = ra_env:server_data_dir(default, UId),

    %% First, create a valid log with some segments
    Log0 = ra_log_init(Config),
    Log1 = write_and_roll(1, 256, 1, Log0),
    Log2 = assert_log_events(Log1,
                             fun(L) ->
                                     #{num_segments := N} = ra_log:overview(L),
                                     N >= 2
                             end),
    ra_log:close(Log2),

    %% Find the segment files
    Segments = filelib:wildcard(filename:join(ServerDataDir, "*.segment")),
    ?assert(length(Segments) >= 2),

    %% Pick the first segment as the "target" and create a dangling symlink
    %% that looks like a compacted segment pointing to a non-existent target
    [_FirstSeg | _] = lists:sort(Segments),
    DanglingSymlinkName = filename:join(ServerDataDir, "99999999.segment"),

    %% Create a symlink pointing to a non-existent file (simulating a
    %% compaction where the target was deleted but the symlink remains)
    NonExistentTarget = "deleted_segment.segment",
    ok = file:make_symlink(NonExistentTarget, DanglingSymlinkName),

    %% Verify the symlink exists and is dangling
    {ok, #file_info{type = symlink}} = file:read_link_info(DanglingSymlinkName),
    {error, enoent} = file:read_file_info(DanglingSymlinkName),

    %% Now try to re-init the log - this should NOT crash
    %% Currently it crashes because:
    %% 1. purge_dangling_symlinks has a bug (uses term_to_binary)
    %% 2. my_segrefs lists all .segment files including the dangling symlink
    %% 3. ra_log_segment:info tries to open the dangling symlink and fails
    Log3 = ra_log_init(Config),

    %% If we get here, the dangling symlink was handled correctly
    %% Verify the dangling symlink was cleaned up
    ?assertEqual({error, enoent}, file:read_link_info(DanglingSymlinkName)),

    ra_log:close(Log3),
    ok.

init_after_missing_segments_event(Config) ->
    %% This test verifies that ra_log:init handles the case where a segments
    %% event was missed (e.g., due to a crash or message loss). The log should
    %% recover correctly and be able to read all entries.

    %% Step 1: Write entries 1-127 and roll, but discard the segments event
    Log0 = ra_log_init(Config),
    Log1 = write_n(1, 111, 1, Log0),
    ok = ra_log_wal:force_roll_over(ra_log_wal),
    %% Only deliver written events, discard segments events
    Log2 = deliver_all_log_events_except_segments(Log1, 500),
    {110, 1} = ra_log:last_written(Log2),


    %% set_last_index 100 - this opens a new mt table
    {ok, Log3} = ra_log:set_last_index(100, Log2),
    % ct:pal("O3 ~p", [ra_log:overview(Log3)]),
    %%
    %% sparse writes, [105, 110, 115]
    {ok, Log4}  = ra_log:write_sparse({105, 1, 105}, 100, Log3),
    {ok, Log5}  = ra_log:write_sparse({110, 1, 110}, 105, Log4),
    {ok, Log6}  = ra_log:write_sparse({115, 1, 110}, 110, Log5),


    %% the install snapshot, but all I think we need to do is force a new mt,
    %% set_last_index 115
    {ok, Log7} = ra_log:set_last_index(115, Log6),
    % ct:pal("O7 ~p", [ra_log:overview(Log7)]),
    %% write {116, 130} and roll
    %% this _should_ clear all previous mem tables

    Log8 = write_and_roll(116, 131, 1, Log7),
    Log9 = write_n(131, 140, 1, Log8),

    ct:pal("O9 ~p", [ra_log:overview(Log9)]),
    ra_log:close(Log9),
    %%
    %% re-init
    Log = ra_log_init(Config),
    ct:pal("O ~p", [ra_log:overview(Log)]),

    %% validate all indexes since "snapshot" (115) can be read during recovery
    validate_fold(116, 139, 1, Log),

    ok.



%% INTERNAL
validate_fold(From, To, Term, Log0) ->
    {Entries0, Log} = ra_log:fold(From, To, fun ra_lib:cons/2, [], Log0),
    ?assertEqual(To - From + 1, length(Entries0)),
    % validate entries are correctly read
    Expected = [{I, Term, <<I:64/integer>>} ||
                I <- lists:seq(To, From, -1)],
    ?assertEqual(Expected, Entries0),
    Log.

append_and_roll(From, To, Term, Log0) ->
    Log1 = append_n(From, To, Term, Log0),
    ok = ra_log_wal:force_roll_over(ra_log_wal),
    assert_log_events(Log1, fun(L) ->
                                    ra_log:last_written(L) == {To-1, Term}
                            end).

append_and_roll_no_deliver(From, To, Term, Log0) ->
    Log1 = append_n(From, To, Term, Log0),
    ok = ra_log_wal:force_roll_over(ra_log_wal),
    Log1.

write_and_roll(From, To, Term, Log0) ->
    write_and_roll(From, To, Term, Log0, 200).

write_and_roll(From, To, Term, Log0, Timeout) ->
    Log1 = write_n(From, To, Term, Log0),
    ok = ra_log_wal:force_roll_over(ra_log_wal),
    deliver_all_log_events(Log1, Timeout).

write_no_roll(From, To, Term, Log0, Timeout) ->
    Log1 = write_n(From, To, Term, Log0),
    deliver_all_log_events(Log1, Timeout).

write_and_roll_no_deliver(From, To, Term, Log0) ->
    Log1 = write_n(From, To, Term, Log0),
    ok = ra_log_wal:force_roll_over(ra_log_wal),
    Log1.

% not inclusive
append_n(To, To, _Term, Log) ->
    Log;
append_n(From, To, Term, Log0) ->
    Log = ra_log:append({From, Term, <<From:64/integer>>}, Log0),
    append_n(From+1, To, Term, Log).

write_n(From, To, Term, Log0) ->
    Entries = [{X, Term, <<X:64/integer>>} ||
               X <- lists:seq(From, To - 1)],
    {ok, Log} = ra_log:write(Entries, Log0),
    Log.

write_sparse([], _, Log0) ->
    Log0;
write_sparse([I | Rem], LastIdx, Log0) ->
    ct:pal("write_sparse index ~b last ~w", [I, LastIdx]),
    {ok, Log} = ra_log:write_sparse({I, 1, <<I:64/integer>>}, LastIdx, Log0),
    write_sparse(Rem, I, Log).

%% Utility functions

deliver_log_events_cond(Log0, _CondFun, 0) ->
    flush(),
    ct:pal("Log ~p", [ra_log:overview(Log0)]),
    ct:fail("condition did not manifest");
deliver_log_events_cond(Log0, CondFun, N) ->
    receive
        {ra_log_event, Evt} ->
            ct:pal("log evt: ~p", [Evt]),
            {Log1, Effs} = ra_log:handle_event(Evt, Log0),
            Log2 = lists:foldl(
                    fun({send_msg, P, E}, Acc) ->
                            P ! E,
                            Acc;
                       ({next_event, {ra_log_event, E}}, Acc0) ->
                            {Acc, Effs1} = ra_log:handle_event(E, Acc0),
                            run_effs(Effs1),
                            Acc;
                       ({bg_work, Fun, _}, Acc) ->
                            Fun(),
                            Acc;
                       (_, Acc) ->
                            Acc
                    end, Log1, Effs),
            case CondFun(Log2) of
                {false, Log} ->
                    deliver_log_events_cond(Log, CondFun, N-1);
                false ->
                    deliver_log_events_cond(Log1, CondFun, N-1);
                {true, Log} ->
                    ct:pal("condition was true!!"),
                    Log;
                true ->
                    ct:pal("condition was true!"),
                    Log2
            end
    after 100 ->
            case CondFun(Log0) of
                {false, Log} ->
                    deliver_log_events_cond(Log, CondFun, N-1);
                false ->
                    deliver_log_events_cond(Log0, CondFun, N-1);
                {true, Log} ->
                    ct:pal("condition was true!"),
                    Log;
                true ->
                    ct:pal("condition was true!"),
                    Log0
            end
    end.

deliver_all_log_events(Log0, Timeout) ->
    receive
        {ra_log_event, Evt} ->
            ct:pal("log evt: ~p", [Evt]),
            {Log1, Effs} = ra_log:handle_event(Evt, Log0),
            Log = lists:foldl(
                    fun({send_msg, P, E}, Acc) ->
                            P ! E,
                            Acc;
                       ({next_event, {ra_log_event, E}}, Acc0) ->
                            {Acc, Effs} = ra_log:handle_event(E, Acc0),
                            run_effs(Effs),
                            Acc;
                       ({bg_work, Fun, _}, Acc) ->
                            Fun(),
                            Acc;
                       (_, Acc) ->
                            Acc
                    end, Log1, Effs),
            % ct:pal("log evt effs: ~p", [Effs]),
            deliver_all_log_events(Log, Timeout)
    after Timeout ->
              Log0
    end.

assert_log_events(Log0, AssertPred) ->
    assert_log_events(Log0, AssertPred, 2000).

assert_log_events(Log0, AssertPred, Timeout) ->
    case AssertPred(Log0) of
        true ->
            Log0;
        false ->
            receive
                {ra_log_event, Evt} ->
                    ct:pal("log evt: ~p", [Evt]),
                    {Log1, Effs} = ra_log:handle_event(Evt, Log0),
                    run_effs(Effs),
                    %% handle any next events
                    Log = lists:foldl(
                            fun ({next_event, {ra_log_event, E}}, Acc0) ->
                                    ct:pal("log evt: ~p", [E]),
                                    {Acc, Effs1} = ra_log:handle_event(E, Acc0),
                                    run_effs(Effs1),
                                    Acc;
                                (_, Acc) ->
                                    Acc
                            end, Log1, Effs),
                    assert_log_events(Log, AssertPred, Timeout)

            after Timeout ->
                      flush(),
                      exit({assert_log_events_timeout, Log0})
            end
    end.

wait_for_segments(Log0, Timeout) ->
    receive
        {ra_log_event, {segments, _, _} = Evt} ->
            ct:pal("log evt: ~p", [Evt]),
            {Log, _} = ra_log:handle_event(Evt, Log0),
            deliver_all_log_events(Log, 100)
    after Timeout ->
              Log0
    end.

deliver_all_log_events_except_segments(Log0, Timeout) ->
    receive
        {ra_log_event, {segments, _, _} = Evt} ->
            ct:pal("log evt dropping: ~p", [Evt]),
            deliver_all_log_events_except_segments(Log0, Timeout);
        {ra_log_event, Evt} ->
            ct:pal("log evt: ~p", [Evt]),
            {Log, _} = ra_log:handle_event(Evt, Log0),
            deliver_all_log_events_except_segments(Log, Timeout)
    after Timeout ->
              Log0
    end.

deliver_one_log_events(Log0, Timeout) ->
    receive
        {ra_log_event, Evt} ->
            ct:pal("log evt: ~p", [Evt]),
            element(1, ra_log:handle_event(Evt, Log0))
    after Timeout ->
              Log0
    end.

deliver_written_log_events(Log0, Timeout) ->
    receive
        {ra_log_event, {written, _, _} = Evt} ->
            ct:pal("log evt: ~p", [Evt]),
            {Log, _} = ra_log:handle_event(Evt, Log0),
            deliver_written_log_events(Log, 100)
    after Timeout ->
              Log0
    end.

validate_rolled_reads(_Config) ->
    % 1. configure WAL to low roll over limit
    % 2. append enough entries to ensure it has rolled over
    % 3. pass all log events received to ra_log
    % 4. validate all entries can be read
    % 5. check there is only one .wal file
    exit(not_implemented).

find_segments(Config) ->
    UId = ?config(uid, Config),
    ServerDataDir = ra_env:server_data_dir(default, UId),
    filelib:wildcard(filename:join(ServerDataDir, "*.segment")).

empty_mailbox() ->
    empty_mailbox(100).

empty_mailbox(T) ->
    receive
        _ ->
            empty_mailbox()
    after T ->
              ok
    end.

new_peer() ->
    #{next_index => 1,
      match_index => 0,
      commit_index_sent => 0,
      query_index => 0,
      status => normal}.

flush() ->
    receive
        Any ->
            ct:pal("flush ~p", [Any]),
            flush()
    after 0 ->
              ok
    end.

meta(Idx, Term, Cluster) ->
    #{index => Idx,
      term => Term,
      cluster => Cluster,
      machine_version => 1}.

create_snapshot_chunk(Config, Meta, Context) ->
    create_snapshot_chunk(Config, Meta, <<"9">>, [], Context).

create_snapshot_chunk(Config, #{index := Idx,
                                term := Term} = Meta, MacState, LiveIndexes, Context) ->
    OthDir = filename:join(?config(work_dir, Config), "snapshot_installation"),
    CPDir = filename:join(?config(work_dir, Config), "checkpoints"),
    ok = ra_lib:make_dir(OthDir),
    ok = ra_lib:make_dir(CPDir),
    Sn0 = ra_snapshot:init(<<"someotheruid_adsfasdf">>, ra_log_snapshot,
                           OthDir, CPDir, undefined, ?DEFAULT_MAX_CHECKPOINTS),
    % LiveIndexes = [],
    {Sn1, [{bg_work, Fun, _ErrFun}]} =
        ra_snapshot:begin_snapshot(Meta, ?MODULE, MacState, snapshot, Sn0),
    Fun(),
    Sn2 =
        receive
            {ra_log_event, {snapshot_written, {Idx, Term} = IdxTerm, _, snapshot, _}} ->
                ra_snapshot:complete_snapshot(IdxTerm, snapshot,
                                              LiveIndexes, Sn1)
        after 1000 ->
                  flush(),
                  exit(snapshot_timeout)
        end,
    {ok, Meta, ChunkSt} = ra_snapshot:begin_read(Sn2, Context),
    {ok, Chunk, _} = ra_snapshot:read_chunk(ChunkSt, 1000000000, Sn2),
    Chunk.

ra_log_init(Config) ->
    ra_log_init(Config, #{}).

ra_log_init(Config, Cfg0) ->
    Cfg = maps:merge(#{uid => ?config(uid, Config),
                       initial_access_pattern => ?config(access_pattern, Config)},
                     Cfg0),
    %% augment with default system config
    ra_log:init(Cfg#{system_config => ra_system:default_config()}).

ra_log_take(From, To, Log0) ->
    {Acc, Log} = ra_log:fold(From, To, fun (E, Acc) -> [E | Acc] end, [], Log0),
    {lists:reverse(Acc), Log}.

restart_wal() ->
    [SupPid] = [P || {ra_log_wal_sup, P, _, _}
                     <- supervisor:which_children(ra_log_sup)],
    ok = supervisor:terminate_child(SupPid, ra_log_wal),
    {ok, _} = supervisor:restart_child(SupPid, ra_log_wal),
    ok.

start_ra(Config) ->
    {ok, _} = ra:start([{data_dir, ?config(work_dir, Config)},
                        {segment_max_entries, 128}]),
    ok.

wait_for_wal(OldPid) ->
    ok = ra_lib:retry(fun () ->
                              P = whereis(ra_log_wal),
                              is_pid(P) andalso P =/= OldPid
                      end, 100, 100).
run_effs(Effs) ->
    [Fun() || {bg_work, Fun, _} <- Effs].


%% ra_machine fakes
version() -> 1.
which_module(_) -> ?MODULE.
live_indexes(MacState) when is_list(MacState) ->
    %% fake returning live indexes
    MacState;
live_indexes(_) ->
    [].

macctx() ->
    {version(), ?MODULE}.
