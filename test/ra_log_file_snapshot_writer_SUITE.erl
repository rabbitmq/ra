-module(ra_log_file_snapshot_writer_SUITE).
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
     write_snapshot
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
    file:make_dir(Dir),
    register(TestCase, self()),
    [{test_case, TestCase}, {data_dir, Dir} | Config].


write_snapshot(Config) ->
    Dir = ?config(data_dir, Config),
    _ = ra_log_file_snapshot_writer:start_link(),
    Snapshot = {10, 5, [node1], some_data},
    Self = self(),
    ok = ra_log_file_snapshot_writer:write_snapshot(Self, Dir, Snapshot),
    receive
        {ra_log_event, {snapshot_written, {10, 5}, _}} ->
            % TODO: validate snapshot
            {ok, Data} = file:read_file(filename:join(Dir, "00000001.snapshot")),
            Snapshot = binary_to_term(Data),
            ok
    after 2000 ->
              throw(ra_log_event_timeout)
    end,
    % Write a second snapshot
    Snapshot2 = {20, 6, [node1, node2], some_data2},
    ok = ra_log_file_snapshot_writer:write_snapshot(Self, Dir, Snapshot2),
    receive
        {ra_log_event, {snapshot_written, {20, 6}, _}} ->
            % TODO: validate snapshot
            false = filelib:is_file(filename:join(Dir, "00000001.snapshot")),
            {ok, Data2} = file:read_file(filename:join(Dir, "00000002.snapshot")),
            Snapshot2 = binary_to_term(Data2),
            ok
    after 2000 ->
              throw(ra_log_event_timeout)
    end,
    ok.


