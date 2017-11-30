-module(ra_log_file_SUITE).
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
     handle_overwrite,
     receive_segment,
     read_one,
     validate_sequential_reads,
     validate_reads_for_overlapped_writes,
     recovery,
     resend_write,
     wal_down_append_throws,
     wal_down_write_returns_error_wal_down,

     % detect_lost_written_range,
     snapshot_recovery,
     snapshot_installation,
     update_release_cursor
    ].

groups() ->
    [
     {tests, [], all_tests()}
    ].

init_per_suite(Config) ->
    _ = application:load(ra),
    ok = application:set_env(ra, data_dir, ?config(priv_dir, Config)),
    ok = application:set_env(ra, segment_max_entries, 128),
    application:ensure_all_started(ra),
    Config.

end_per_suite(Config) ->
    application:stop(ra),
    Config.

init_per_group(tests, Config) ->
    Config.

end_per_group(tests, Config) ->
    Config.

init_per_testcase(TestCase, Config) ->
    PrivDir = ?config(priv_dir, Config),
    Dir = filename:join(PrivDir, TestCase),
    register(TestCase, self()),
    [{test_case, TestCase}, {wal_dir, Dir} | Config].

end_per_testcase(_, _Config) ->
    _ = supervisor:restart_child(ra_sup, ra_log_wal),
    ok.

handle_overwrite(Config) ->
    Dir = ?config(wal_dir, Config),
    {registered_name, Self} = erlang:process_info(self(), registered_name),
    Log0 = ra_log_file:init(#{directory => Dir, id => Self}),
    {queued, Log1} = ra_log_file:write([{1, 1, "value"}, {2, 1, "value"}], Log0),
    receive
        {ra_log_event, {written, {2, 1}}} -> ok
    after 2000 ->
              exit(written_timeout)
    end,
    {queued, Log3} = ra_log_file:write([{1, 2, "value"}], Log1),
    % ensure immediate truncation
    {1, 2} = ra_log_file:last_index_term(Log3),
    {queued, Log4} = ra_log_file:write([{2, 2, "value"}], Log3),
    % simulate the first written event coming after index 20 has already
    % been written in a new term
    Log = ra_log_file:handle_event({written, {2, 1}}, Log4),
    % ensure last written has not been incremented
    {0, 0} = ra_log_file:last_written(Log),
    {2, 2} = ra_log_file:last_written(
                ra_log_file:handle_event({written, {2, 2}}, Log)),
    ok = ra_log_wal:force_roll_over(ra_log_wal),
    _ = deliver_all_log_events(Log, 1000),
    ra_log_file:close(Log),
    ok.

receive_segment(Config) ->
    Dir = ?config(wal_dir, Config),
    {registered_name, Self} = erlang:process_info(self(), registered_name),
    Log0 = ra_log_file:init(#{directory => Dir, id => Self}),
    % write a few entries
    Entries = [{I, 1, <<"value_", I:32/integer>>} || I <- lists:seq(1, 3)],

    Log1 = lists:foldl(fun(E, Acc0) ->
                               {queued, Acc} =
                                   ra_log_file:append(E, Acc0),
                               Acc
                       end, Log0, Entries),
    Log2 = deliver_all_log_events(Log1, 500),
    {3, 1} = ra_log_file:last_written(Log2),
    [MemTblTid] = [Tid || {receive_segment, _, _, Tid}
                          <- ets:tab2list(ra_log_open_mem_tables)],
    % force wal roll over
    ok = ra_log_wal:force_roll_over(ra_log_wal),
    Log3 = deliver_all_log_events(Log2, 1500),
    % validate ets table has been recovered
    ?assert(lists:member(MemTblTid, ets:all()) =:= false),
    [] = ets:tab2list(ra_log_open_mem_tables),
    [] = ets:tab2list(ra_log_closed_mem_tables),
    % validate reads
    {Entries, FinalLog} = ra_log_file:take(1, 3, Log3),
    ra_log_file:close(FinalLog),
    ok.

read_one(Config) ->
    Dir = ?config(wal_dir, Config),
    {registered_name, Self} = erlang:process_info(self(), registered_name),
    Log0 = ra_log_file:init(#{directory => Dir, id => Self}),
    Log1 = append_n(1, 2, 1, Log0),
    % ensure the written event is delivered
    Log2 = deliver_all_log_events(Log1, 200),
    {[_], Log} = ra_log_file:take(1, 5, Log2),
    % read out of range
    {[], Log} = ra_log_file:take(5, 5, Log2),
    [{_, M1, M2, M3, M4} = M] = ets:lookup(ra_log_file_metrics, Self),
    ct:pal("M ~p", [M]),
    % read two entries
    ?assert(M1 + M2 + M3 + M4 =:= 1),
    ra_log_file:close(Log),
    ok.


validate_sequential_reads(Config) ->
    Dir = ?config(wal_dir, Config),
    {registered_name, Self} = erlang:process_info(self(), registered_name),
    Log0 = ra_log_file:init(#{directory => Dir, id => Self}),
    % write a few entries
    Log1 = append_and_roll(1, 100, 1, Log0),
    Log2 = append_and_roll(100, 200, 1, Log1),
    Log3 = append_and_roll(200, 400, 1, Log2),
    Log4 = append_and_roll(400, 500, 1, Log3),
    Log = append_and_roll(500, 1001, 1, Log4),
    {ColdTaken, {ColdReds, FinLog}} =
        timer:tc(fun () ->
                         {_, Reds0} = process_info(self(), reductions),
                         L = validate_read(1, 1001, 1, Log),
                         {_, Reds} = process_info(self(), reductions),
                         {Reds - Reds0, L}
                 end),
    [{_, M1, M2, M3, M4}] = Metrics = ets:lookup(ra_log_file_metrics, Self),
    ?assert(M1 + M2 + M3 + M4 =:= 1000),

    ct:pal("validate_sequential_reads COLD took ~pms Reductions: ~p~nMetrics: ~p",
           [ColdTaken/1000, ColdReds, Metrics]),
    % we'd like to know if we regress beyond this
    % some of the reductions are spent validating the reads
    ?assert(ColdReds < 100000),
    {WarmTaken, {WarmReds, FinLog2}} =
        timer:tc(fun () ->
                         {_, R0} = process_info(self(), reductions),
                         L = validate_read(1, 1001, 1, FinLog),
                         {_, R} = process_info(self(), reductions),
                         {R - R0, L}
                 end),
    ct:pal("validate_sequential_reads WARM took ~pms Reductions: ~p~n",
           [WarmTaken/1000, WarmReds]),
    % we'd like to know if we regress beyond this
    ?assert(WarmReds < 75000),
    ra_log_file:close(FinLog2),
    ok.

validate_reads_for_overlapped_writes(Config) ->
    Dir = ?config(wal_dir, Config),
    {registered_name, Self} = erlang:process_info(self(), registered_name),
    Log0 = ra_log_file:init(#{directory => Dir, id => Self}),
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

    [{_, M1, M2, M3, M4}] = Metrics = ets:lookup(ra_log_file_metrics, Self),
    ct:pal("Metrics ~p", [Metrics]),
    ?assert(M1 + M2 + M3 + M4 =:= 550),
    ra_log_file:close(Log8),
    ok.


recovery(Config) ->
    Dir = ?config(wal_dir, Config),
    {registered_name, Self} = erlang:process_info(self(), registered_name),
    Log0 = ra_log_file:init(#{directory => Dir, id => Self}),
    {0, 0} = ra_log_file:last_index_term(Log0),
    Log1 = write_and_roll(1, 10, 1, Log0),
    {9, 1} = ra_log_file:last_index_term(Log1),
    Log2 = write_and_roll(5, 15, 2, Log1),
    {14, 2} = ra_log_file:last_index_term(Log2),
    Log3 = write_n(15, 21, 3, Log2),
    {20, 3} = ra_log_file:last_index_term(Log3),
    Log4 = deliver_all_log_events(Log3, 200),
    {20, 3} = ra_log_file:last_index_term(Log4),
    ra_log_file:close(Log4),
    application:stop(ra),
    application:ensure_all_started(ra),
    % % TODO how to avoid sleep
    timer:sleep(2000),
    Log5 = ra_log_file:init(#{directory => Dir, id => Self}),
    {20, 3} = ra_log_file:last_index_term(Log5),
    Log6 = validate_read(1, 5, 1, Log5),
    Log7 = validate_read(5, 15, 2, Log6),
    Log8 = validate_read(15, 21, 3, Log7),
    ra_log_file:close(Log8),

    ok.

resend_write(Config) ->
    % simulate lost messages requiring the ra node to resend in flight
    % writes
    meck:new(ra_log_wal, [passthrough]),
    meck:expect(ra_log_wal, write, fun (_, _, 10, _, _) -> ok;
                                       (A, B, C, D, E) ->
                                           meck:passthrough([A, B, C, D, E])
                                   end),
    Dir = ?config(wal_dir, Config),
    {registered_name, Self} = erlang:process_info(self(), registered_name),
    Log0 = ra_log_file:init(#{directory => Dir, id => Self}),
    {0, 0} = ra_log_file:last_index_term(Log0),
    Log1 = append_n(1, 10, 2, Log0),
    Log2 = deliver_all_log_events(Log1, 500),
    % fake missing entry
    Log2b = append_n(10, 11, 2, Log2),
    Log3 = append_n(11, 13, 2, Log2b),
    Log4 = receive
               {ra_log_event, {resend_write, 10} = Evt} ->
                   ra_log_file:handle_event(Evt, Log3)
           after 500 ->
                     throw(resend_write_timeout)
           end,
    {queued, Log5} = ra_log_file:append({13, 2, banana}, Log4),
    Log6 = deliver_all_log_events(Log5, 500),
    {[_, _, _, _, _], _} = ra_log_file:take(9, 5, Log6),

    meck:unload(ra_log_wal),
    ok.

wal_down_append_throws(Config) ->
    Dir = ?config(wal_dir, Config),
    {registered_name, Self} = erlang:process_info(self(), registered_name),

    Log0 = ra_log_file:init(#{directory => Dir, id => Self}),
    ok = supervisor:terminate_child(ra_sup, ra_log_wal),
    ?assertExit(wal_down, ra_log_file:append({1,1,hi}, Log0)),
    ok.

wal_down_write_returns_error_wal_down(Config) ->
    Dir = ?config(wal_dir, Config),
    {registered_name, Self} = erlang:process_info(self(), registered_name),
    Log0 = ra_log_file:init(#{directory => Dir, id => Self}),
    ok = supervisor:terminate_child(ra_sup, ra_log_wal),
    {error, wal_down} = ra_log_file:write([{1,1,hi}], Log0),
    ok.

detect_lost_written_range(Config) ->
    % ra_log_file writes some messages
    % WAL rolls over and WAL file is deleted
    % WAL crashes
    % ra_log_file writes some more message (that are lost)
    % WAL recovers
    % ra_log_file continues writing
    % ra_log_file receives a {written, Start, End}  log event and detects that
    % messages with a lower id than 'Start' were enver confirmed.
    % ra_log_file resends all lost and new writes to create a contiguous range
    % of writes
    Dir = ?config(wal_dir, Config),
    {registered_name, Self} = erlang:process_info(self(), registered_name),
    Log0 = ra_log_file:init(#{directory => Dir, id => Self,
                              wal => ra_log_wal}),
    {0, 0} = ra_log_file:last_index_term(Log0),
    Log1 = append_and_roll(1, 10, 2, Log0),
    Log2 = deliver_all_log_events(Log1, 500),
    % simulate wal outage
    proc_lib:stop(ra_log_wal),

    % write some messages
    Log3 = append_n(10, 15, 2, Log2),
    % ok = application:stop(ra),
    % ok = application:start(ra),
    timer:sleep(1000),
    Log4 = append_n(15, 20, 2, Log3),
    Log5 = deliver_all_log_events(Log4, 1000),
    % validate no writes were lost and can be recovered
    {Entries, _} = ra_log_file:take(0, 20, Log5),
    ra_log_file:close(Log5),
    Log = ra_log_file:init(#{directory => Dir, id => Self}),
    {RecoveredEntries, _} = ra_log_file:take(0, 20, Log),
    ct:pal("entries ~p ~n ~p", [Entries, Log5]),
    ?assert(length(Entries) =:= 20),
    ?assert(length(RecoveredEntries) =:= 20),
    Entries = RecoveredEntries,
    ok.

snapshot_recovery(Config) ->
    Dir = ?config(wal_dir, Config),
    {registered_name, Self} = erlang:process_info(self(), registered_name),
    Log0 = ra_log_file:init(#{directory => Dir, id => Self}),
    {0, 0} = ra_log_file:last_index_term(Log0),
    Log1 = append_and_roll(1, 10, 2, Log0),
    Snapshot = {9, 2, #{n1 => #{}}, <<"9">>},
    Log2 = ra_log_file:write_snapshot(Snapshot, Log1),
    Log3 = deliver_all_log_events(Log2, 500),
    ra_log_file:close(Log3),
    Log = ra_log_file:init(#{directory => Dir, id => Self}),
    Snapshot = ra_log_file:read_snapshot(Log),
    {9, 2} = ra_log_file:last_index_term(Log),
    {[], _} = ra_log_file:take(1, 9, Log),
    ok.

snapshot_installation(Config) ->
    % write a few entries
    % simulate outage/ message loss
    % write snapshot for entry not seen
    % then write entries
    Dir = ?config(wal_dir, Config),
    {registered_name, Self} = erlang:process_info(self(), registered_name),
    Log0 = ra_log_file:init(#{directory => Dir, id => Self}),
    {0, 0} = ra_log_file:last_index_term(Log0),
    Log1 = write_n(1, 10, 2, Log0),
    Snapshot = {15, 2, #{n1 => #{}}, <<"9">>},
    Log2 = ra_log_file:write_snapshot(Snapshot, Log1),

    % after a snapshot we need a "truncating write" that ignores missing
    % indexes
    Log3 = write_n(16, 20, 2, Log2),
    Log = deliver_all_log_events(Log3, 500),
    {19, 2} = ra_log_file:last_index_term(Log),
    {[], _} = ra_log_file:take(1, 9, Log),
    {[_, _], _} = ra_log_file:take(16, 2, Log),
    ok.

update_release_cursor(Config) ->
    % ra_log_file should initiate shapshot if segments can be released
    Dir = ?config(wal_dir, Config),
    {registered_name, Self} = erlang:process_info(self(), registered_name),
    Log0 = ra_log_file:init(#{directory => Dir, id => Self}),
    % beyond 128 limit - should create two segments
    Log1 = append_and_roll(1, 150, 2, Log0),
    Log2 = deliver_all_log_events(Log1, 500),
    % assert there are two segments at this point
    [_, _] = filelib:wildcard(filename:join(Dir, "*.segment")),
    % leave one entry in the current segment
    Log3 = ra_log_file:update_release_cursor(150, #{n1 => #{}, n2 => #{}},
                                             initial_state, Log2),
    Log4 = deliver_all_log_events(Log3, 500),
    % no segments
    [] = filelib:wildcard(filename:join(Dir, "*.segment")),
    % append a few more items
    Log5 = append_and_roll(150, 155, 2, Log4),
    Log6 = deliver_all_log_events(Log5, 500),
    ra_lib:dump('Log6', Log6),
    % assert there is only one segment - the current
    % snapshot has been confirmed.
    [_] = filelib:wildcard(filename:join(Dir, "*.segment")),

    ok.

validate_read(To, To, _Term, Log0) ->
    Log0;
validate_read(From, To, Term, Log0) ->
    End = min(From + 5, To),
    {Entries, Log} = ra_log_file:take(From, End - From, Log0),
    % validate entries are correctly read
    Expected = [ {I, Term, <<I:64/integer>>} ||
                 I <- lists:seq(From, End - 1) ],
    Expected = Entries,
    validate_read(End, To, Term, Log).


append_and_roll(From, To, Term, Log0) ->
    Log1 = append_n(From, To, Term, Log0),
    ok = ra_log_wal:force_roll_over(ra_log_wal),
    deliver_all_log_events(Log1, 200).

write_and_roll(From, To, Term, Log0) ->
    Log1 = write_n(From, To, Term, Log0),
    ok = ra_log_wal:force_roll_over(ra_log_wal),
    deliver_all_log_events(Log1, 200).

% not inclusivw
append_n(To, To, _Term, Log) ->
    Log;
append_n(From, To, Term, Log0) ->
    {queued, Log} = ra_log_file:append({From, Term,
                                        <<From:64/integer>>}, Log0),
    append_n(From+1, To, Term, Log).

write_n(From, To, Term, Log0) ->
    Entries = [{X, Term, <<X:64/integer>>} ||
               X <- lists:seq(From, To - 1)],
    {queued, Log} = ra_log_file:write(Entries, Log0),
    Log.

%% Utility functions

deliver_all_log_events(Log0, Timeout) ->
    receive
        {ra_log_event, Evt} ->
            Log = ra_log_file:handle_event(Evt, Log0),
            deliver_all_log_events(Log, 100)
    after Timeout ->
              Log0
    end.

validate_rolled_reads(_Config) ->
    % 1. configure WAL to low roll over limit
    % 2. append enough entries to ensure it has rolled over
    % 3. pass all log events received to ra_log_file
    % 4. validate all entries can be read
    % 5. check there is only one .wal file
    exit(not_implemented).
