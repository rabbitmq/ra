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
     recover_invalid_checksum,
     read_index_term,
     recover_same_as_read
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
    Dir = filename:join(?config(priv_dir, Config), TestCase),
    file:make_dir(Dir),
    [{dir, Dir} | Config].

end_per_testcase(_TestCase, _Config) ->
    ok.

%%%===================================================================
%%% Test cases
%%%===================================================================

roundtrip(Config) ->
    Dir = ?config(dir, Config),
    SnapshotMeta = {33, 94, [{banana, node@jungle}, {banana, node@savanna}]},
    SnapshotRef = my_state,
    ok = ra_log_snapshot:write(Dir, SnapshotMeta, SnapshotRef),
    ?assertEqual({SnapshotMeta, SnapshotRef}, read(Dir)),
    ok.

read(Dir) ->
    case ra_log_snapshot:begin_read(128, Dir) of
        {ok, _Crc, Meta, St} ->
            Snap = read_all_snapshot(St, Dir, 128, <<>>),
            {Meta, binary_to_term(Snap)};
        Err -> Err
    end.

read_all_snapshot(St, Dir, Size, Acc) ->
    case ra_log_snapshot:read_chunk(St, Size, Dir) of
        {ok, Data, {next, St1}} ->
            read_all_snapshot(St1, Dir, <<Acc/binary, Data/binary>>);
        {ok, Data, last} ->
            <<Acc/binary, Data/binary>>;
        Err -> Err
    end.

read_missing(Config) ->
    File = ?config(file, Config),
    {error, enoent} = read(File),
    ok.

read_other_file(Config) ->
    Dir = ?config(dir, Config),
    File = filename:join(Dir, "snapshot.dat"),
    file:write_file(File, <<"NSAR", 1:8/unsigned>>),
    {error, invalid_format} = read(Dir),
    ok.

read_invalid_version(Config) ->
    Dir = ?config(dir, Config),
    File = filename:join(Dir, "snapshot.dat"),
    Data = term_to_binary(snapshot),
    Crc = erlang:crc32(Data),
    file:write_file(File, [<<"RASN", 99:8/unsigned, Crc:32/integer>>, Data]),
    {error, {invalid_version, 99}} = read(Dir),
    ok.

recover_invalid_checksum(Config) ->
    Dir = ?config(dir, Config),
    File = filename:join(Dir, "snapshot.dat"),
    file:write_file(File, [<<"RASN">>, <<1:8/unsigned, 0:32/unsigned>>,
                           term_to_binary(<<"hi">>)]),
    {error, checksum_error} = ra_log_snapshot:recover(Dir),
    ok.

read_index_term(Config) ->
    Dir = ?config(dir, Config),
    SnapshotMeta = {33, 94, [{banana, node@jungle}, {banana, node@savanna}]},
    SnapshotRef = my_state,
    ok = ra_log_snapshot:write(Dir, SnapshotMeta, SnapshotRef),
    {ok, {33, 94, _}} = ra_log_snapshot:read_meta(Dir),
    ok.

recover_same_as_read(Config) ->
    Dir = ?config(dir, Config),
    SnapshotMeta = {33, 94, [{banana, node@jungle}, {banana, node@savanna}]},
    SnapshotData = my_state,
    ok = ra_log_snapshot:write(Dir, SnapshotMeta, SnapshotData),
    {ok, SnapshotMeta, SnapshotData} = ra_log_snapshot:recover(Dir),
    ok.

%% Utility
