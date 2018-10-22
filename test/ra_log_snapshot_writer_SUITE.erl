-module(ra_log_snapshot_writer_SUITE).
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
     write_snapshot,
     write_snapshot_call
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
    _ = ra_log_snapshot_writer:start_link(),
    SnapshotMeta = {10, 5, [server1]},
    SnapshotRef = some_data,
    Self = self(),
    ok = ra_log_snapshot_writer:write_snapshot(Self, Dir, SnapshotMeta, SnapshotRef, ra_log_snapshot),
    receive
        {ra_log_event, {snapshot_written, {10, 5}, File, []}} ->
            % TODO: validate snapshot data for ref
            {ok, SnapshotMeta, _SnapshotData} = ra_log_snapshot:read(File),
            ok
    after 2000 ->
              throw(ra_log_event_timeout)
    end,
    % Write a second snapshot
    SnapshotMeta2 = {20, 6, [server1, server2]},
    SnapshotRef2 = some_data2,
    ok = ra_log_snapshot_writer:write_snapshot(Self, Dir, SnapshotMeta2, SnapshotRef2, ra_log_snapshot),
    receive
        {ra_log_event, {snapshot_written, {20, 6}, File2, [Old]}} ->
            % TODO: validate snapshot data for ref
            true = filelib:is_file(Old),
            {ok, SnapshotMeta2, _SnapshotData2} = ra_log_snapshot:read(File2),
            ok
    after 2000 ->
              throw(ra_log_event_timeout)
    end,
    ok.


write_snapshot_call(Config) ->
    Dir = ?config(data_dir, Config),
    _ = ra_log_snapshot_writer:start_link(),
    SnapshotMeta = {10, 5, [server1]},
    SnapshotData = some_data,
    {ok, File, _} = ra_log_snapshot_writer:save_snapshot_call(Dir, SnapshotMeta, SnapshotData, ra_log_snapshot),
    ?assert(filelib:is_file(File)),
    {ok, SnapshotMeta, SnapshotData} = ra_log_snapshot:read(File),
    ok.
