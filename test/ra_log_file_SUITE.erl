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
     receive_segment
    ].

groups() ->
    [
     {tests, [], all_tests()}
    ].

init_per_suite(Config) ->
    _ = application:load(ra),
    ok = application:set_env(ra, data_dir, ?config(priv_dir, Config)),
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
    Entries = [ {I, 1, <<"value_", I:32/integer>>} || I <- lists:seq(1, 3)],

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
    {ReadEntries, FinalLog} = ra_log_file:take(1, 3, Log3),
    Entries = [{I, T, binary_to_term(D)} || {I, T, D} <- ReadEntries],
    ra_log_file:close(FinalLog),
    ok.


%% Utility functions

deliver_all_log_events(Log0, Timeout) ->
    receive
        {ra_log_event, Evt} ->
            % ct:pal("ra_log_event ~p", [Evt]),
            Log = ra_log_file:handle_event(Evt, Log0),
            deliver_all_log_events(Log, Timeout)
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
