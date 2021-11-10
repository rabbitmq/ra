-module(ra_log_2_SUITE).
-compile(nowarn_export_all).
-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

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
     resend_write,
     handle_overwrite,
     receive_segment,
     read_one,
     take_after_overwrite_and_init,
     validate_sequential_reads,
     validate_reads_for_overlapped_writes,
     cache_overwrite_then_take,
     last_written_overwrite,
     last_index_reset,
     last_index_reset_before_written,
     recovery,
     recover_bigly,
     wal_crash_recover,
     wal_down_read_availability,
     wal_down_append_throws,
     wal_down_write_returns_error_wal_down,

     detect_lost_written_range,
     % snapshot_recovery,
     snapshot_installation,
     append_after_snapshot_installation,
     written_event_after_snapshot_installation,
     update_release_cursor,
     update_release_cursor_with_machine_version,
     missed_closed_tables_are_deleted_at_next_opportunity,
     transient_writer_is_handled,
     read_opt,
     written_event_after_snapshot,
     updated_segment_can_be_read,
     open_segments_limit,
     external_reader,
     write_config
    ].

groups() ->
    [
     {random, [], all_tests()},
     {sequential, [], all_tests()}
    ].

init_per_suite(Config) ->
    {ok, _} = ra:start([{data_dir, ?config(priv_dir, Config)},
                        {segment_max_entries, 128}]),
    Config.

end_per_suite(Config) ->
    application:stop(ra),
    Config.

init_per_group(G, Config) ->
    [{access_pattern, G} | Config].

end_per_group(_, Config) ->
    Config.

init_per_testcase(TestCase, Config) ->
    PrivDir = ?config(priv_dir, Config),
    UId = <<(atom_to_binary(TestCase, utf8))/binary,
            (atom_to_binary(?config(access_pattern, Config)))/binary>>,
    ra:start(),
    ok = ra_directory:register_name(default, UId, self(), undefined,
                                    TestCase, TestCase),
    [{uid, UId}, {test_case, TestCase}, {wal_dir, PrivDir} | Config].

end_per_testcase(_, _Config) ->
    ok.

-define(N1, {n1, node()}).
-define(N2, {n2, node()}).
% -define(N3, {n3, node()}).

handle_overwrite(Config) ->
    Log0 = ra_log_init(Config),
    {ok, Log1} = ra_log:write([{1, 1, "value"},
                                        {2, 1, "value"}], Log0),
    receive
        {ra_log_event, {written, {1, 2, 1}}} -> ok
    after 2000 ->
              exit(written_timeout)
    end,
    {ok, Log3} = ra_log:write([{1, 2, "value"}], Log1),
    % ensure immediate truncation
    {1, 2} = ra_log:last_index_term(Log3),
    {ok, Log4} = ra_log:write([{2, 2, "value"}], Log3),
    % simulate the first written event coming after index 20 has already
    % been written in a new term
    {Log, _} = ra_log:handle_event({written, {1, 2, 1}}, Log4),
    % ensure last written has not been incremented
    {0, 0} = ra_log:last_written(Log),
    {2, 2} = ra_log:last_written(
               element(1, ra_log:handle_event({written, {1, 2, 2}}, Log))),
    ok = ra_log_wal:force_roll_over(ra_log_wal),
    _ = deliver_all_log_events(Log, 1000),
    ra_log:close(Log),
    ok.

receive_segment(Config) ->
    Log0 = ra_log_init(Config),
    % write a few entries
    Entries = [{I, 1, <<"value_", I:32/integer>>} || I <- lists:seq(1, 3)],

    Log1 = lists:foldl(fun(E, Acc0) ->
                               ra_log:append(E, Acc0)
                       end, Log0, Entries),
    Log2 = deliver_all_log_events(Log1, 500),
    {3, 1} = ra_log:last_written(Log2),
    UId = ?config(uid, Config),
    [MemTblTid] = [Tid || {Key, _, _, Tid}
                          <- ets:tab2list(ra_log_open_mem_tables), Key == UId],
    % force wal roll over
    ok = ra_log_wal:force_roll_over(ra_log_wal),
    Log3 = deliver_all_log_events(Log2, 1500),
    % validate ets table has been recovered
    ?assert(lists:member(MemTblTid, ets:all()) =:= false),
    [] = ets:tab2list(ra_log_open_mem_tables),
    [] = ets:tab2list(ra_log_closed_mem_tables),
    % validate reads
    {Entries, C, FinalLog} = ra_log:take(1, 3, Log3),
    ?assertEqual(length(Entries), C),
    ra_log:close(FinalLog),
    ok.

read_one(Config) ->
    ra_counters:new(?FUNCTION_NAME, ?RA_COUNTER_FIELDS),
    Log0 = ra_log_init(Config, #{counter => ra_counters:fetch(?FUNCTION_NAME)}),
    Log1 = append_n(1, 2, 1, Log0),
    % Log1 = ra_log:append({1, 1, <<1:64/integer>>}, Log0),
    % ensure the written event is delivered
    Log2 = deliver_all_log_events(Log1, 200),
    {[_], 1, Log} = ra_log:take(1, 5, Log2),
    % read out of range
    {[], 0, Log} = ra_log:take(5, 5, Log2),
    #{?FUNCTION_NAME := #{read_cache := M1,
                          read_open_mem_tbl := M2,
                          read_closed_mem_tbl := M3,
                          read_segment := M4}} = ra_counters:overview(),
    % read two entries
    ?assertEqual(1, M1 + M2 + M3 + M4),
    ra_log:close(Log),
    ok.

take_after_overwrite_and_init(Config) ->
    Log0 = ra_log_init(Config),
    Log1 = write_and_roll_no_deliver(1, 5, 1, Log0),
    Log2 = deliver_written_log_events(Log1, 200),
    {[_, _, _, _], 4, Log3} = ra_log:take(1, 5, Log2),
    Log4 = write_and_roll_no_deliver(1, 2, 2, Log3),
    % fake lost segments event
    Log5 = deliver_written_log_events(Log4, 200),
    % ensure we cannot take stale entries
    {[{1, 2, _}], 1, Log6} = ra_log:take(1, 5, Log5),
    _ = ra_log:close(Log6),
    Log = ra_log_init(Config),
    {[{1, 2, _}], _, _} = ra_log:take(1, 5, Log),
    ok.


validate_sequential_reads(Config) ->
    ra_counters:new(?FUNCTION_NAME, ?RA_COUNTER_FIELDS),
    Log0 = ra_log_init(Config, #{counter => ra_counters:fetch(?FUNCTION_NAME),
                                 max_open_segments => 100}),
    % write a few entries
    Log1 = append_and_roll(1, 500, 1, Log0),
    Log2 = append_and_roll(500, 1001, 1, Log1),
    %% need to ensure the segments are delivered
    Log = deliver_all_log_events(Log2, 200),
    _ = erlang:statistics(exact_reductions),
    {ColdTaken, {ColdReds, FinLog}} =
        timer:tc(fun () ->
                         {_, Reds0} = erlang:statistics(exact_reductions),
                         L = validate_read(1, 1001, 1, Log),
                         {_, Reds} = erlang:statistics(exact_reductions),
                         {Reds - Reds0, L}
                 end),

    #{?FUNCTION_NAME := #{read_cache := M1,
                          read_open_mem_tbl := M2,
                          read_closed_mem_tbl := M3,
                          read_segment := M4} = O} = ra_counters:overview(),
    ?assertEqual(1000, M1 + M2 + M3 + M4),

    ct:pal("validate_sequential_reads COLD took ~pms Reductions: ~p~nMetrics: ~p",
           [ColdTaken/1000, ColdReds, O]),
    % we'd like to know if we regress beyond this
    % some of the reductions are spent validating the reads
    % NB: in OTP 21.1 reduction counts shot up mostly probably due to lists:reverse
    % not previously using up enough reductions
    ?assert(ColdReds < 200000),
    _ = erlang:statistics(exact_reductions),
    {WarmTaken, {WarmReds, FinLog2}} =
        timer:tc(fun () ->
                         {_, R0} = erlang:statistics(exact_reductions),
                         % start_profile(Config, [lists, ra_log, ra_flru,
                         %                        file, ra_file_handle,
                         %                        ra_log_segment]),
                         L = validate_read(1, 1001, 1, FinLog),
                         % stop_profile(Config),
                         {_, R} = erlang:statistics(exact_reductions),
                         {R - R0, L}
                 end),
    ct:pal("validate_sequential_reads WARM took ~pms Reductions: ~p",
           [WarmTaken/1000, WarmReds]),
    % warm reductions should always be less than cold
    % NB: this isn't necessarily always the case with OTP 24 so commenting out assertion
    % ?assert(WarmReds < ColdReds),
    ra_log:close(FinLog2),
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
    Log6 = deliver_all_log_events(Log5, 200),

    Log7 = validate_read(1, 200, 1, Log6),
    Log8 = validate_read(200, 551, 2, Log7),

    #{?FUNCTION_NAME := #{read_cache := M1,
                          read_open_mem_tbl := M2,
                          read_closed_mem_tbl := M3,
                          read_segment := M4}} = ra_counters:overview(),
    ?assertEqual(550, M1 + M2 + M3 + M4),
    ra_log:close(Log8),
    ok.

read_opt(Config) ->
    Log0 = ra_log_init(Config),
    % Log0 = ra_log:release_resources(2, undefined, Log00),
    % write a segment and roll 1 - 299 - term 1
    Num = 4096 * 2,
    Log1 = write_and_roll(1, Num, 1, Log0, 50),
    Log2 = wait_for_segments(Log1, 5000),
    %% read small batch of the latest entries
    {_, _, Log} = ra_log:take(Num - 5, 5, Log2),
    %% measure the time it takes to read the first index
    {Time, _} = timer:tc(fun () ->
                                 _ = erlang:statistics(exact_reductions),
                                 ra_log:take(1, 1, Log)
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
                                 ra_log:take(1, Num, Log)
                         end),
    {_, Reds3} = erlang:statistics(exact_reductions),
    ct:pal("read all took ~wms Reduction ~w", [Time3 / 1000, Reds3]),
    ok.


written_event_after_snapshot(Config) ->
    Log0 = ra_log_init(Config, #{snapshot_interval => 1}),
    Log1 = ra_log:append({1, 1, <<"one">>}, Log0),
    Log1b = ra_log:append({2, 1, <<"two">>}, Log1),
    {Log2, _} = ra_log:update_release_cursor(2, #{}, 1,
                                             <<"one+two">>, Log1b),
    {Log3, _} = receive
                    {ra_log_event, {snapshot_written, {2, 1}} = Evt} ->
                        ra_log:handle_event(Evt, Log2)
                after 500 ->
                          exit(snapshot_written_timeout)
                end,
    Log4 = deliver_all_log_events(Log3, 100),
    % true = filelib:is_file(Snap1),
    Log5  = ra_log:append({3, 1, <<"three">>}, Log4),
    Log6  = ra_log:append({4, 1, <<"four">>}, Log5),
    Log6b = deliver_all_log_events(Log6, 100),
    {Log7, _} = ra_log:update_release_cursor(4, #{}, 1,
                                             <<"one+two+three+four">>,
                                             Log6b),
    _ = receive
            {ra_log_event, {snapshot_written, {4, 1}} = E} ->
                ra_log:handle_event(E, Log7)
        after 500 ->
                  exit(snapshot_written_timeout)
        end,

    %% this will no longer be false as the snapshot deletion is an effect
    %% and not done by the log itself
    % false = filelib:is_file(Snap1),
    ok.

updated_segment_can_be_read(Config) ->
    ra_counters:new(?FUNCTION_NAME, ?RA_COUNTER_FIELDS),
    Log0 = ra_log_init(Config,
                       #{counter => ra_counters:fetch(?FUNCTION_NAME),
                         snapshot_interval => 1}),
    %% append a few entrie
    Log2 = append_and_roll(1, 5, 1, Log0),
    % Log2 = deliver_all_log_events(Log1, 200),
    %% read some, this will open the segment with the an index of entries
    %% 1 - 4
    {Entries, C0, Log3} = ra_log:take(1, 25, Log2),
    ?assertEqual(length(Entries), C0),
    %% append a few more itmes and process the segments
    Log4 = append_and_roll(5, 16, 1, Log3),
    % this should return all entries
    {Entries1, C1, _} = ra_log:take(1, 15, Log4),
    ?assertEqual(length(Entries1), C1),
    ct:pal("Entries: ~p", [Entries]),
    ct:pal("Entries1: ~p", [Entries1]),
    ct:pal("Counters ~p", [ra_counters:overview(?FUNCTION_NAME)]),
    ct:pal("closed ~p", [ets:tab2list(ra_log_closed_mem_tables)]),
    ?assertEqual(15, length(Entries1)),
    % l18 = length(Entries1),
    ok.

cache_overwrite_then_take(Config) ->
    Log0 = ra_log_init(Config),
    Log1 = write_n(1, 5, 1, Log0),
    Log2 = write_n(3, 4, 2, Log1),
    % validate only 3 entries can be read even if requested range is greater
    {[_, _, _], 3, _} = ra_log:take(1, 5, Log2),
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

last_index_reset(Config) ->
    Log0 = ra_log_init(Config),
    Log1 = write_n(1, 5, 1, Log0),
    Pred = fun (L) ->
                   {4, 1} == ra_log:last_written(L)
           end,
    Log2 = assert_log_events(Log1, Pred, 2000),
    5 = ra_log:next_index(Log2),
    {4, 1} = ra_log:last_index_term(Log2),
    % reverts last index to a previous index
    % needs to be done if a new leader sends an empty AER
    {ok, Log3} = ra_log:set_last_index(3, Log2),
    {3, 1} = ra_log:last_written(Log3),
    4 = ra_log:next_index(Log3),
    {3, 1} = ra_log:last_index_term(Log3),
    ok.

last_index_reset_before_written(Config) ->
    Log0 = ra_log_init(Config),
    Log1 = write_n(1, 5, 1, Log0),
    {0, 0} = ra_log:last_written(Log1),
    5 = ra_log:next_index(Log1),
    {4, 1} = ra_log:last_index_term(Log1),
    % reverts last index to a previous index
    % needs to be done if a new leader sends an empty AER
    {ok, Log2} = ra_log:set_last_index(3, Log1),
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
    #{cache_size := 0} = ra_log:overview(Log3),
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
    ra:start(),

    Log5 = ra_log_init(Config),
    {20, 3} = ra_log:last_index_term(Log5),
    Log6 = validate_read(1, 5, 1, Log5),
    Log7 = validate_read(5, 15, 2, Log6),
    Log8 = validate_read(15, 21, 3, Log7),
    ra_log:close(Log8),

    ok.

recover_bigly(Config) ->
    Log0 = ra_log_init(Config),
    Log1 = write_n(1, 10000, 1, Log0),
    Pred = fun (L) ->
                   {9999, 1} =:= ra_log:last_index_term(L) andalso
                   {9999, 1} =:= ra_log:last_written(L)
           end,
    Log2 = assert_log_events(Log1, Pred, 2000),
    ra_log:close(Log2),
    application:stop(ra),
    ra:start(),
    Log = ra_log_init(Config),
    {9999, 1} = ra_log:last_written(Log),
    {9999, 1} = ra_log:last_index_term(Log),
    ra_log:close(Log),
    ok.


resend_write(Config) ->
    % logger:set_primary_config(level, debug),
    % simulate lost messages requiring the ra server to resend in flight
    % writes
    meck:new(ra_log_wal, [passthrough]),
    meck:expect(ra_log_wal, write, fun (_, _, 10, _, _) -> ok;
                                       (A, B, C, D, E) ->
                                           meck:passthrough([A, B, C, D, E])
                                   end),
    timer:sleep(100),
    Log0 = ra_log_init(Config),
    {0, 0} = ra_log:last_index_term(Log0),
    %% write 1..9
    Log1 = append_n(1, 10, 2, Log0),
    Log2 = assert_log_events(Log1, fun (L) ->
                                           {9, 2} == ra_log:last_written(L)
                                   end),
    % fake missing entry
    %% write 10 which will be dropped by meck interception
    Log2b = append_n(10, 11, 2, Log2),
    %% write 11..12 which should trigger resend
    Log3 = append_n(11, 13, 2, Log2b),
    Log4 = receive
               {ra_log_event, {resend_write, 10} = Evt} ->
                   %% unload mock so that write of index 10 can go through
                   meck:unload(ra_log_wal),
                   element(1, ra_log:handle_event(Evt, Log3))
           after 500 ->
                     throw(resend_write_timeout)
           end,
    Log5 = ra_log:append({13, 2, banana}, Log4),
    Log6 = assert_log_events(Log5, fun (L) ->
                                           {13, 2} == ra_log:last_written(L)
                                   end),
    {[_, _, _, _, _], 5, _} = ra_log:take(9, 5, Log6),
    ra_log:close(Log6),

    ok.

wal_crash_recover(Config) ->
    Log0 = ra_log_init(Config, #{resend_window => 1}),
    Log1 = write_n(1, 50, 2, Log0),
    % crash the wal
    ok = proc_lib:stop(ra_log_segment_writer),
    % write someting
    timer:sleep(100),
    Log2 = deliver_one_log_events(write_n(50, 75, 2, Log1), 100),
    ok = proc_lib:stop(ra_log_segment_writer),
    Log3 = write_n(75, 100, 2, Log2),
    % Log4 = assert_log_events(Log3, fun (L) ->
    %                                        {99, 2} == ra_log:last_written(L)
    %                                end),
    % wait long enough for the resend window to pass
    timer:sleep(2000),
    Log = assert_log_events(write_n(100, 101, 2,  Log3),
                            fun (L) ->
                                    {100, 2} == ra_log:last_written(L)
                            end),
    {100, 2} = ra_log:last_written(Log),
    validate_read(1, 100, 2, Log),
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
    {Entries, _, _} = ra_log:take(0, 10, Log2),
    ?assert(length(Entries) =:= 10),
    ok.

wal_down_append_throws(Config) ->
    Log0 = ra_log_init(Config),
    ?assert(ra_log:can_write(Log0)),
    [SupPid] = [P || {ra_log_wal_sup, P, _, _}
                     <- supervisor:which_children(ra_log_sup)],
    ok = supervisor:terminate_child(SupPid, ra_log_wal),
    ?assert(not ra_log:can_write(Log0)),
    ?assertExit(wal_down, ra_log:append({1,1,hi}, Log0)),
    ok.

wal_down_write_returns_error_wal_down(Config) ->
    Log0 = ra_log_init(Config),
    [SupPid] = [P || {ra_log_wal_sup, P, _, _}
                     <- supervisor:which_children(ra_log_sup)],
    ok = supervisor:terminate_child(SupPid, ra_log_wal),
    {error, wal_down} = ra_log:write([{1,1,hi}], Log0),
    ok.

detect_lost_written_range(Config) ->
    Log0 = ra_log_init(Config, #{wal => ra_log_wal}),
    meck:new(ra_log_wal, [passthrough]),
    {0, 0} = ra_log:last_index_term(Log0),
    % write some entries
    Log1 = append_and_roll_no_deliver(1, 10, 2, Log0),
    Log2 = assert_log_events(Log1, fun (L) ->
                                           {9, 2} == ra_log:last_written(L)
                                   end),
    % WAL rolls over and WAL file is deleted
    % simulate wal outage
    meck:expect(ra_log_wal, write, fun (_, _, _, _, _) -> ok end),

    % append some messages that will be lost
    Log3 = append_n(10, 15, 2, Log2),

    % restart WAL to ensure lose the transient state keeping track of
    % each writer's last written index
    [SupPid] = [P || {ra_log_wal_sup, P, _, _}
                     <- supervisor:which_children(ra_log_sup)],
    ok = supervisor:terminate_child(SupPid, ra_log_wal),
    {ok, _} = supervisor:restart_child(SupPid, ra_log_wal),

    % WAL recovers
    meck:unload(ra_log_wal),

    % append some more stuff
    Log4 = append_n(15, 20, 2, Log3),
    Log5 = assert_log_events(Log4, fun (L) ->
                                           {19, 2} == ra_log:last_written(L)
                                   end),
    % validate no writes were lost and can be recovered
    {Entries, C0, _} = ra_log:take(0, 20, Log5),
    ?assertEqual(length(Entries), C0),
    ra_log:close(Log5),
    Log = ra_log_init(Config),
    {19, 2} = ra_log:last_written(Log5),
    {RecoveredEntries, C1, _} = ra_log:take(0, 20, Log),
    ?assertEqual(length(RecoveredEntries), C1),
    ?assert(length(Entries) =:= 20),
    ?assert(length(RecoveredEntries) =:= 20),
    Entries = RecoveredEntries,
    ok.

snapshot_installation(Config) ->
    logger:set_primary_config(level, all),
    % write a few entries
    % simulate outage/ message loss
    % write snapshot for entry not seen
    % then write entries
    Log0 = ra_log_init(Config),
    {0, 0} = ra_log:last_index_term(Log0),
    % Log1 = write_n(1, 10, 2, Log0),
    Log1 = assert_log_events(write_n(1, 10, 2, Log0),
                             fun (L) ->
                                     {9, 2} == ra_log:last_written(L)
                             end),
    Log2 = write_n(10, 20, 2, Log1),
    %% cache all log events
    DelayedEvt = receive
                     {ra_log_event, E} ->
                         ct:pal("cached log evt: ~p", [E]),
                         E
                 after 1000 ->
                           exit(log_event_timeout)
                 end,

    %% create snapshot chunk
    Meta = meta(15, 2, [?N1]),
    Chunk = create_snapshot_chunk(Config, Meta),

    SnapState0 = ra_log:snapshot_state(Log2),
    {ok, SnapState1} = ra_snapshot:begin_accept(Meta, SnapState0),
    {ok, SnapState} = ra_snapshot:accept_chunk(Chunk, 1, last, SnapState1),

    {Log3, _} = ra_log:install_snapshot({15, 2}, SnapState, Log2),
    {15, _} = ra_log:last_index_term(Log3),
    {15, _} = ra_log:last_written(Log3),
    #{cache_size := 0} = ra_log:overview(Log3),
    {Log4, _} = ra_log:handle_event(DelayedEvt, Log3),
    {15, _} = ra_log:last_index_term(Log4),
    {15, _} = ra_log:last_written(Log4),
    #{cache_size := 0} = ra_log:overview(Log4),

    flush(),

    % after a snapshot we need a "truncating write" that ignores missing
    % indexes
    Log5 = write_n(16, 20, 2, Log4),
    Log = assert_log_events(Log5, fun (L) ->
                                          {19, 2} == ra_log:last_written(L)
                                  end),
    {[], 0, _} = ra_log:take(1, 9, Log),
    {[_, _], 2, _} = ra_log:take(16, 2, Log),
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
    Chunk = create_snapshot_chunk(Config, Meta),
    SnapState0 = ra_log:snapshot_state(Log1),
    {ok, SnapState1} = ra_snapshot:begin_accept(Meta, SnapState0),
    {ok, SnapState} = ra_snapshot:accept_chunk(Chunk, 1, last, SnapState1),
    {Log2, _} = ra_log:install_snapshot({15, 2}, SnapState, Log1),
    {15, _} = ra_log:last_index_term(Log2),
    {15, _} = ra_log:last_written(Log2),

    % after a snapshot we need a "truncating write" that ignores missing
    % indexes
    Log3 = append_n(16, 20, 2, Log2),
    Log = assert_log_events(Log3, fun (L) ->
                                          {19, 2} == ra_log:last_written(L)
                                  end),
    {[], 0, _} = ra_log:take(1, 9, Log),
    {[_, _], 2, _} = ra_log:take(16, 2, Log),
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
    Chunk = create_snapshot_chunk(Config, Meta),
    SnapState0 = ra_log:snapshot_state(Log1),
    {ok, SnapState1} = ra_snapshot:begin_accept(Meta, SnapState0),
    {ok, SnapState} = ra_snapshot:accept_chunk(Chunk, 1, last, SnapState1),
    {Log2, _} = ra_log:install_snapshot({SnapIdx, 2}, SnapState, Log1),
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
    {Log2, _} = ra_log:update_release_cursor(127, #{?N1 => new_peer(),
                                                    ?N2 => new_peer()},
                                             1, initial_state, Log1),

    Log3 = assert_log_events(Log2,
                             fun (L) ->
                                     {127, 2} == ra_log:snapshot_index_term(L)
                             end),
    % Log3 = deliver_all_log_events(Log2, 500),
    %% now the snapshot_written should have been delivered and the
    %% snapshot state table updated
    [{UId, 127}] = ets:lookup(ra_log_snapshot_state, ?config(uid, Config)),
    % this should delete a single segment
    ra_lib:retry(fun () ->
                         1 == length(find_segments(Config))
                 end, 10, 100),
    Log3b = validate_read(128, 150, 2, Log3),
    % update the release cursor all the way
    {Log4, _} = ra_log:update_release_cursor(149, #{?N1 => new_peer(),
                                                    ?N2 => new_peer()},
                                             1, initial_state, Log3b),
    Log5 = assert_log_events(Log4,
                             fun (L) ->
                                     {149, 2} == ra_log:snapshot_index_term(L)
                             end),
    % Log5 = deliver_all_log_events(Log4, 500),

    [{UId, 149}] = ets:lookup(ra_log_snapshot_state, UId),

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
    % Log6 = append_and_roll(150, 155, 2, Log5),
    % Log = deliver_all_log_events(Log6, 500),
    validate_read(150, 155, 2, Log),
    % assert there is only one segment - the current
    % snapshot has been confirmed.
    [_] = find_segments(Config),

    ok.

update_release_cursor_with_machine_version(Config) ->
    % ra_log should initiate shapshot if segments can be released
    Log0 = ra_log_init(Config, #{snapshot_interval => 64}),
    % beyond 128 limit - should create two segments
    Log1 = append_and_roll(1, 150, 2, Log0),
    timer:sleep(300),
    % assert there are two segments at this point
    [_, _] = find_segments(Config),
    % update release cursor to the last entry of the first segment
    MacVer = 2,
    {Log2, _} = ra_log:update_release_cursor(127, #{?N1 => new_peer(),
                                                    ?N2 => new_peer()},
                                             MacVer,
                                             initial_state, Log1),
    Log = assert_log_events(Log2,
                            fun (L) ->
                                    {127, 2} == ra_log:snapshot_index_term(L)
                            end),
    SnapState = ra_log:snapshot_state(Log),
    %% assert the version is in the snapshot state meta data
    CurrentDir = ra_snapshot:current_snapshot_dir(SnapState),
    {ok, Meta} = ra_snapshot:read_meta(ra_log_snapshot, CurrentDir),
    ?assertMatch(#{index := 127, machine_version := MacVer}, Meta),
    ok.

missed_closed_tables_are_deleted_at_next_opportunity(Config) ->
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
    empty_mailbox(500),
    % although this has been flushed to disk the ra_server wasn't available
    % to clean it up.
    [_] = ets:tab2list(ra_log_closed_mem_tables),
    % then deliver all log events

    % append and roll some more entries
    Log4 = deliver_all_log_events(append_and_roll(150, 155, 2, Log3), 200),

    % the missed closed mem table should have been cleaned up at the same
    % time as the next one.
    [] = ets:tab2list(ra_log_closed_mem_tables),
    [] = ets:tab2list(ra_log_open_mem_tables),

    % TODO: validate reads
    Log5 = validate_read(1, 155, 2, Log4),

    % then update the release cursor
    {Log6, _} = ra_log:update_release_cursor(154, #{?N1 => new_peer(),
                                                    ?N2 => new_peer()},
                                             1, initial_state, Log5),
    _Log = deliver_all_log_events(Log6, 500),

    [_] = find_segments(Config),
    ok.

transient_writer_is_handled(Config) ->
    Self = self(),
    UId2 = <<(?config(uid, Config))/binary, "sub_proc">>,
    _Pid = spawn(fun () ->
                         ra_directory:register_name(default, <<"sub_proc">>,
                                                    self(), undefined,
                                                    sub_proc, sub_proc),
                         Log0 = ra_log_init(Config, #{uid => UId2}),
                         Log1 = append_n(1, 10, 2, Log0),
                         % ignore events
                         Log2 = deliver_all_log_events(Log1, 500),
                         ra_log:close(Log2),
                         Self ! done
                 end),
    receive done -> ok
    after 2000 -> exit(timeout)
    end,
    ra:start(),
    _ = ra_log_init(Config),
    ok.

open_segments_limit(Config) ->
    Max = 3,
    Log0 = ra_log_init(Config, #{max_open_segments => Max}),
    % write a few entries
    Log1 = append_and_roll(1, 2000, 1, Log0),
    %% this should result in a few segments
    %% validate as this read all of them
    Log1b = wait_for_segments(Log1, 5000),
    Log2 = validate_read(1, 2000, 1, Log1b),
    Segs = find_segments(Config),
    #{open_segments := Open}  = ra_log:overview(Log2),
    ?assert(length(Segs) > Max),
    ?assert(Open =< Max),
    ok.

external_reader(Config) ->
    %% external readers should be notified of all new segments
    %% and the lower bound of the log
    %% The upper bound needs to be discovered by querying
    logger:set_primary_config(level, all),
    Log0 = ra_log_init(Config),
    {0, 0} = ra_log:last_index_term(Log0),

    Log1 = write_n(200, 220, 2,
                   write_and_roll(1, 200, 2, Log0)),

    timer:sleep(1000),

    Self = self(),
    Pid = spawn(
            fun () ->
                    receive
                        {ra_log_reader_state, R1} = Evt ->
                            {Es, _, R2} = ra_log_reader:read(0, 220, R1),
                            Len1 = length(Es),
                            ct:pal("Es ~w", [Len1]),
                            Self ! {got, Evt, Es},
                            receive
                                {ra_log_update, _, F, _} = Evt2 ->
                                    %% reader before update has been processed
                                    %% should work
                                    {Stale, _, _} = ra_log_reader:read(F, 220, R2),
                                    ?assertEqual(Len1, length(Stale)),
                                    R3 = ra_log_reader:handle_log_update(Evt2, R2),
                                    {Es2, _, _R4} = ra_log_reader:read(F, 220, R3),
                                    ct:pal("Es2 ~w", [length(Es2)]),
                                    ?assertEqual(Len1, length(Es2)),
                                    Self ! {got, Evt2, Es2}
                            end
                    end
            end),
    {Log2, [{reply, {ok, Reader}} | _]} =
        ra_log:register_reader(Pid, Log1),
    Pid ! {ra_log_reader_state, Reader},
    %% TODO: validate there is monitor effect
    receive
        {got, Evt, Entries} ->
            ct:pal("got segs: ~w ~w", [Evt, length(Entries)]),
            ok
    after 2000 ->
              flush(),
              exit(got_timeout)
    end,
    ra_log_wal:force_roll_over(ra_log_wal),

    _Log3 = deliver_all_log_events(Log2, 500),

    %% this should result in a segment update
    receive
        {got, Evt2, Entries1} ->
            ct:pal("got segs: ~w ~w", [Evt2, length(Entries1)]),
            ok
    after 2000 ->
              flush(),
              exit(got_timeout_2)
    end,
    timer:sleep(2000),
    flush(),
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

validate_read(To, To, _Term, Log0) ->
    Log0;
validate_read(From, To, Term, Log0) ->
    End = min(From + 25, To),
    {Entries, C0, Log} = ra_log:take(From, End - From, Log0),
    ?assertEqual(length(Entries), C0),
    % validate entries are correctly read
    Expected = [ {I, Term, <<I:64/integer>>} ||
                 I <- lists:seq(From, End - 1) ],
    ?assertEqual(Expected, Entries),
    validate_read(End, To, Term, Log).


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

%% Utility functions

deliver_all_log_events(Log0, Timeout) ->
    receive
        {ra_log_event, Evt} ->
            ct:pal("log evt: ~p", [Evt]),
            {Log, Effs} = ra_log:handle_event(Evt, Log0),
            [P ! E || {send_msg, P, E, _} <- Effs],
            % ct:pal("log evt effs: ~p", [Effs]),
            deliver_all_log_events(Log, Timeout)
    after Timeout ->
              Log0
    end.

assert_log_events(Log0, AssertPred) ->
    assert_log_events(Log0, AssertPred, 2000).

assert_log_events(Log0, AssertPred, Timeout) ->
    receive
        {ra_log_event, Evt} ->
            ct:pal("log evt: ~p", [Evt]),
            {Log, _} = ra_log:handle_event(Evt, Log0),
            case AssertPred(Log) of
                true ->
                    Log;
                false ->
                    assert_log_events(Log, AssertPred, Timeout)
            end
    after Timeout ->
              exit(assert_log_events_timeout)
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
        {ra_log_event, {written, _} = Evt} ->
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
start_profile(Config, Modules) ->
    Dir = ?config(priv_dir, Config),
    Case = ?config(test_case, Config),
    GzFile = filename:join([Dir, "lg_" ++ atom_to_list(Case) ++ ".gz"]),
    ct:pal("Profiling to ~p", [GzFile]),

    lg:trace(Modules, lg_file_tracer,
             GzFile, #{running => false, mode => profile}).

stop_profile(Config) ->
    Case = ?config(test_case, Config),
    ct:pal("Stopping profiling for ~p", [Case]),
    lg:stop(),
    % this segfaults
    Dir = ?config(priv_dir, Config),
    Name = filename:join([Dir, "lg_" ++ atom_to_list(Case)]),
    lg_callgrind:profile_many(Name ++ ".gz.*", Name ++ ".out",#{}),
    ok.

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

create_snapshot_chunk(Config, #{index := Idx} =  Meta) ->
    OthDir = filename:join(?config(priv_dir, Config), "snapshot_installation"),
    ok = ra_lib:make_dir(OthDir),
    Sn0 = ra_snapshot:init(<<"someotheruid_adsfasdf">>, ra_log_snapshot,
                           OthDir),
    MacRef = <<"9">>,
    {Sn1, _} = ra_snapshot:begin_snapshot(Meta, MacRef, Sn0),
    Sn2 =
        receive
            {ra_log_event, {snapshot_written, {Idx, 2} = IdxTerm}} ->
                ra_snapshot:complete_snapshot(IdxTerm, Sn1)
        after 1000 ->
                  exit(snapshot_timeout)
        end,
    {ok, Meta, ChunkSt} = ra_snapshot:begin_read(Sn2),
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
