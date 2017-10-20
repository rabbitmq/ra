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
     handle_overwrite
     % validate_rolled_reads
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
    register(TestCase, self()),
    [{test_case, TestCase}, {wal_dir, Dir} | Config].

handle_overwrite(Config) ->
    Dir = ?config(wal_dir, Config),
    {ok, _Pid} = ra_log_wal:start_link(#{dir => Dir}, []),
    {registered_name, Self} = erlang:process_info(self(), registered_name),
    Log0 = ra_log_file:init(#{directory => Dir, id => Self}),
    {queued, Log1} = ra_log_file:append({20, 1, "value"}, overwrite, Log0),
    receive
        {written, {20, 1}} -> ok
    after 2000 ->
              exit(written_timeout)
    end,
    {queued, Log2} = ra_log_file:append({19, 2, "value"}, overwrite, Log1),
    % ensure immediate truncation
    {19, 2} = ra_log_file:last_index_term(Log2),
    {queued, Log3} = ra_log_file:append({20, 2, "value"}, overwrite, Log2),
    % simulate the first written event coming after index 20 has already
    % been written in a new term
    Log = ra_log_file:handle_written({20, 1}, Log3),
    ra_lib:dump("log", Log3),
    % ensure last written has not been incremented
    {0, 0} = ra_log_file:last_written(Log),
    {20, 2} = ra_log_file:last_written(ra_log_file:handle_written({20, 2}, Log)),
    ok.




validate_rolled_reads(_Config) ->
    % 1. configure WAL to low roll over limit
    % 2. append enough entries to ensure it has rolled over
    % 3. pass all log events received to ra_log_file
    % 4. validate all entries can be read
    % 5. check there is only one .wal file

    exit(not_implemented).
