%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2017-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%
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
     read_meta_data,
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
    ok = ra_lib:make_dir(Dir),
    [{dir, Dir} | Config].

end_per_testcase(_TestCase, _Config) ->
    ok.

%%%===================================================================
%%% Test cases
%%%===================================================================

roundtrip(Config) ->
    Dir = ?config(dir, Config),
    SnapshotMeta = meta(33, 94, [{banana, node@jungle}, {banana, node@savanna}]),
    SnapshotRef = my_state,
    ok = ra_log_snapshot:write(Dir, SnapshotMeta, SnapshotRef),
    ?assertEqual({SnapshotMeta, SnapshotRef}, read(Dir)),
    ok.

read(Dir) ->
    case ra_log_snapshot:begin_read(Dir) of
        {ok, Meta, St} ->
            <<_Crc:32/integer, Snap/binary>> = read_all_snapshot(St, Dir, 128, <<>>),
            {Meta, binary_to_term(Snap)};
        Err -> Err
    end.

read_all_snapshot(St, Dir, Size, Acc) ->
    case ra_log_snapshot:read_chunk(St, Size, Dir) of
        {ok, Data, {next, St1}} ->
            read_all_snapshot(St1, Dir, Size, <<Acc/binary, Data/binary>>);
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

read_meta_data(Config) ->
    Dir = ?config(dir, Config),
    SnapshotMeta = meta(33, 94, [{banana, node@jungle}, {banana, node@savanna}]),
    SnapshotRef = my_state,
    ok = ra_log_snapshot:write(Dir, SnapshotMeta, SnapshotRef),
    {ok, SnapshotMeta} = ra_log_snapshot:read_meta(Dir),
    ok.

recover_same_as_read(Config) ->
    Dir = ?config(dir, Config),
    SnapshotMeta = meta(33, 94, [{banana, node@jungle}, {banana, node@savanna}]),
    SnapshotData = my_state,
    ok = ra_log_snapshot:write(Dir, SnapshotMeta, SnapshotData),
    {ok, SnapshotMeta, SnapshotData} = ra_log_snapshot:recover(Dir),
    ok.

%% Utility

meta(Idx, Term, Cluster) ->
    #{index => Idx,
      term => Term,
      cluster => Cluster,
      machine_version => 1}.
