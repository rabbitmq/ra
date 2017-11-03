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
     recovery
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

handle_overwrite(Config) ->
    Dir = ?config(wal_dir, Config),
    {registered_name, Self} = erlang:process_info(self(), registered_name),
    Log0 = ra_log_file:init(#{directory => Dir, id => Self}),
    {queued, Log1} = ra_log_file:append({1, 1, "value"}, overwrite, Log0),
    {queued, Log2} = ra_log_file:append({2, 1, "value"}, overwrite, Log1),
    receive
        {ra_log_event, {written, {2, 1}}} -> ok
    after 2000 ->
              exit(written_timeout)
    end,
    {queued, Log3} = ra_log_file:append({1, 2, "value"}, overwrite, Log2),
    % ensure immediate truncation
    {1, 2} = ra_log_file:last_index_term(Log3),
    {queued, Log4} = ra_log_file:append({2, 2, "value"}, overwrite, Log3),
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
                                   ra_log_file:append(E, no_overwrite, Acc0),
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
    Log1 = append_n(0, 1, 1, Log0),
    % ensure the written event is delivered
    Log2 = deliver_all_log_events(Log1, 200),
    {[_], Log} = ra_log_file:take(0, 5, Log2),
    % read out of range
    {[], Log} = ra_log_file:take(5, 5, Log2),
    [{_, M1, M2, M3, M4}] = ets:lookup(ra_log_file_metrics, Self),
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
    % write a segment and roll 1 - 300 - term 1
    ct:pal("Before ~p", [ets:lookup(ra_log_file_metrics, Self)]),
    Log1 = append_and_roll(1, 300, 1, Log0),
    % write 300 - 400 in term 1 - no roll
    Log2 = append_n(300, 400, 1, Log1),
    % write 200 - 350 in term 2 and roll
    Log3 = append_and_roll(200, 350, 2, Log2),
    % write 350 - 500 in term 2
    Log4 = append_and_roll(350, 500, 2, Log3),
    Log5 = append_n(500, 551, 2, Log4),
    Log6 = deliver_all_log_events(Log5, 200),

    Log7 = validate_read(1, 200, 1, Log6),
    ct:pal("After ~p", [ets:lookup(ra_log_file_metrics, Self)]),
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
    Log1 = append_and_roll(1, 10, 1, Log0),
    {9, 1} = ra_log_file:last_index_term(Log1),
    Log2 = append_and_roll(5, 15, 2, Log1),
    {14, 2} = ra_log_file:last_index_term(Log2),
    Log3 = append_n(15, 21, 3, Log2),
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

% not inclusivw
append_n(To, To, _Term, Log) ->
    Log;
append_n(From, To, Term, Log0) ->
    _Bin = crypto:strong_rand_bytes(1024),
    {queued, Log} = ra_log_file:append({From, Term,
                                        <<From:64/integer>>},
                                       overwrite, Log0),
    append_n(From+1, To, Term, Log).



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
