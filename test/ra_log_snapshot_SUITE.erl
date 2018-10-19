-module(ra_log_snapshot_SUITE).

-compile(export_all).

-export([
         ]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

%%%===================================================================
%%% Common Test callbacks
%%%===================================================================

all() ->
    [
     {group, tests}
    ].


all_tests() ->
    [
     roundtrip,
     read_missing,
     read_other_file,
     read_invalid_version,
     read_invalid_checksum,
     read_index_term
    ].

groups() ->
    [
     {tests, [], all_tests()}
    ].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(_Group, Config) ->
    Config.

end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(TestCase, Config) ->
    F = filename:join(?config(priv_dir, Config), TestCase),
    [{file, F} | Config].

end_per_testcase(_TestCase, _Config) ->
    ok.

%%%===================================================================
%%% Test cases
%%%===================================================================

roundtrip(Config) ->
    File = ?config(file, Config),
    SnapshotMeta = {33, 94, [{banana, node@jungle}, {banana, node@savanna}]},
    SnapshotRef = my_state,
    ok = ra_log_snapshot:write(File, SnapshotMeta, SnapshotRef),
    {ok, SnapshotMeta, SnapshotRef} = ra_log_snapshot:read(File),
    ok.

read_missing(Config) ->
    File = ?config(file, Config),
    {error, enoent} = ra_log_snapshot:read(File),
    ok.

read_other_file(Config) ->
    File = ?config(file, Config),
    file:write_file(File, <<"NSAR", 1:8/unsigned>>),
    {error, invalid_format} = ra_log_snapshot:read(File),
    ok.

read_invalid_version(Config) ->
    File = ?config(file, Config),
    Data = term_to_binary(snapshot),
    Crc = erlang:crc32(Data),
    file:write_file(File, [<<"RASN", 99:8/unsigned, Crc:32/integer>>, Data]),
    {error, {invalid_version, 99}} = ra_log_snapshot:read(File),
    ok.

read_invalid_checksum(Config) ->
    File = ?config(file, Config),
    file:write_file(File, [<<"RASN">>, <<1:8/unsigned, 0:32/unsigned>>,
                           term_to_binary(<<"hi">>)]),
    {error, checksum_error} = ra_log_snapshot:read(File),
    ok.

read_index_term(Config) ->
    File = ?config(file, Config),
    SnapshotMeta = {33, 94, [{banana, node@jungle}, {banana, node@savanna}]},
    SnapshotRef = my_state,
    ok = ra_log_snapshot:write(File, SnapshotMeta, SnapshotRef),
    {ok, {33, 94}} = ra_log_snapshot:read_indexterm(File),
    ok.

save_same_as_write(Config) ->
    File = ?config(file, Config),
    SnapshotMeta = {33, 94, [{banana, node@jungle}, {banana, node@savanna}]},
    SnapshotData = my_state,
    ok = ra_log_snapshot:save(File, SnapshotMeta, SnapshotData),
    {ok, SnapshotMeta, SnapshotData} = ra_log_snapshot:read(File),
    ok.

recover_same_as_read(Config) ->
    File = ?config(file, Config),
    SnapshotMeta = {33, 94, [{banana, node@jungle}, {banana, node@savanna}]},
    SnapshotData = my_state,
    ok = ra_log_snapshot:save(File, SnapshotMeta, SnapshotData),
    {ok, SnapshotMeta, SnapshotData} = ra_log_snapshot:recover(File),
    ok.

install_does_nothing(Config) ->
    SnapshotData = my_state,
    {ok, SnapshotData} = ra_log_snapshot:install(SnapshotData, "some.file").


%% Utility
