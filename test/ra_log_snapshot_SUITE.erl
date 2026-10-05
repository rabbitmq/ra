%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2017-2025 Broadcom. All Rights Reserved. The term Broadcom refers to Broadcom Inc. and/or its subsidiaries.
%%
-module(ra_log_snapshot_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-export([
         ]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("kernel/include/file.hrl").

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
     roundtrip_compat,
     accept,
     read_missing,
     read_other_file,
     read_invalid_version,
     recover_invalid_checksum,
     read_meta_data,
     recover_same_as_read,
     padded_aligned,
     padded_boundary,
     recover_unpadded_file,
     padding_checksum_error,
     encode_decode_image
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
    Dir = filename:join([?config(priv_dir, Config), TestCase, write]),
    ok = ra_lib:make_dir(Dir),
    AcceptDir = filename:join([?config(priv_dir, Config), TestCase, accept]),
    ok = ra_lib:make_dir(AcceptDir),
    [{accept_dir, AcceptDir}, {dir, Dir} | Config].

end_per_testcase(_TestCase, _Config) ->
    ok.

%%%===================================================================
%%% Test cases
%%%===================================================================

roundtrip(Config) ->
    Dir = ?config(dir, Config),
    SnapshotMeta = meta(33, 94, [{banana, node@jungle}, {banana, node@savanna}]),
    SnapshotRef = my_state,
    {ok, _} = ra_log_snapshot:write(Dir, SnapshotMeta, SnapshotRef, true),
    Context = #{can_accept_full_file => true},
    ?assertEqual({SnapshotMeta, SnapshotRef}, read(Dir, Context)),
    ok.

roundtrip_compat(Config) ->
    Dir = ?config(dir, Config),
    SnapshotMeta = meta(33, 94, [{banana, node@jungle}, {banana, node@savanna}]),
    SnapshotRef = my_state,
    {ok, _} = ra_log_snapshot:write(Dir, SnapshotMeta, SnapshotRef, true),
    ?assertEqual({SnapshotMeta, SnapshotRef}, read(Dir)),
    ok.

dir(Base, Dir0) ->
    Dir = filename:join(Base, Dir0),
    ok = ra_lib:make_dir(Dir),
    Dir.


accept(Config) ->
    test_accept(Config, one, 1024, true, 128),
    test_accept(Config, two, 1024, false, 128),
    test_accept(Config, three, 8, true, 64),
    test_accept(Config, four, 8, false, 64),
    ok.



test_accept(Config, Name, DataSize, FullFile, ChunkSize) ->
    Dir = dir(?config(dir, Config), Name),
    AcceptDir = dir(?config(accept_dir, Config), Name),
    ct:pal("test_accept ~w ~b ~w ~b", [Name, DataSize, FullFile, ChunkSize]),
    SnapshotMeta = meta(33, 94, [{banana, node@jungle}, {banana, node@savanna}]),
    SnapshotRef = crypto:strong_rand_bytes(DataSize),
    {ok, _} = ra_log_snapshot:write(Dir, SnapshotMeta, SnapshotRef, true),
    Context = #{can_accept_full_file => FullFile},
    {ok, Meta, St} =  ra_log_snapshot:begin_read(Dir, Context),
    %% how to ensure
    [LastChunk | Chunks] = lists:reverse(read_all_chunks(St, Dir, ChunkSize, [])),
    {ok, A0} = ra_log_snapshot:begin_accept(AcceptDir, Meta),
    A1 = lists:foldl(fun (Ch, Acc0) ->
                             {ok, Acc} = ra_log_snapshot:accept_chunk(Ch, Acc0),
                             Acc
                     end, A0, lists:reverse(Chunks)),
    {ok, _Bytes} = ra_log_snapshot:complete_accept(LastChunk, A1),
    ok.

read(Dir) ->
    read(Dir, #{}).

read(Dir, Context) ->
    case ra_log_snapshot:begin_read(Dir, Context) of
        {ok, _Meta, St} ->
            case iolist_to_binary(
                   read_all_chunks(St, Dir, 128, [])) of
                <<"RASN", 1:8, _Crc:32/integer,
                  MetaSz:32/integer, Meta:MetaSz/binary,
                  Snap/binary>> ->
                    {binary_to_term(Meta), binary_to_term(Snap)};
                <<_Crc:32/integer, Snap/binary>> ->
                    {_Meta, binary_to_term(Snap)}
            end;
        Err -> Err
    end.

read_all_chunks(St, Dir, Size, Acc) ->
    case ra_log_snapshot:read_chunk(St, Size, Dir) of
        {ok, Data, {next, St1}} ->
            read_all_chunks(St1, Dir, Size, [Data | Acc]);
        {ok, Data, last} ->
            lists:reverse([Data | Acc]);
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
    {ok, _} = ra_log_snapshot:write(Dir, SnapshotMeta, SnapshotRef, true),
    {ok, SnapshotMeta} = ra_log_snapshot:read_meta(Dir),
    ok.

recover_same_as_read(Config) ->
    Dir = ?config(dir, Config),
    SnapshotMeta = meta(33, 94, [{banana, node@jungle}, {banana, node@savanna}]),
    SnapshotData = my_state,
    {ok, _} = ra_log_snapshot:write(Dir, SnapshotMeta, SnapshotData, true),
    {ok, SnapshotMeta, SnapshotData} = ra_log_snapshot:recover(Dir),
    ok.

padded_aligned(Config) ->
    Dir = ?config(dir, Config),
    File = filename:join(Dir, "snapshot.dat"),
    SnapshotMeta = meta(33, 94, [{banana, node@jungle}]),
    [begin
         State = crypto:strong_rand_bytes(Size),
         {ok, Bytes} = ra_log_snapshot:write(Dir, SnapshotMeta, State, true),
         {ok, #file_info{size = FileSize}} = file:read_file_info(File),
         ?assertEqual(FileSize, Bytes),
         ?assertEqual({ok, FileSize}, ra_log_snapshot:get_size(Dir)),
         ?assertEqual(0, FileSize rem 4096),
         ?assertEqual({ok, SnapshotMeta, State},
                      ra_log_snapshot:recover(Dir)),
         ?assertEqual(ok, ra_log_snapshot:validate(Dir)),
         ?assertEqual({SnapshotMeta, State}, read(Dir))
     end || Size <- [0, 1, 100, 4000, 4096, 5000, 20000]],
    ok.

padded_boundary(Config) ->
    %% find a state size that produces an exactly aligned file with no
    %% padding and check neighbours either side
    Dir = ?config(dir, Config),
    SnapshotMeta = meta(33, 94, [{banana, node@jungle}]),
    Sz = fun(N) ->
                 {ok, B} = ra_log_snapshot:write(Dir, SnapshotMeta,
                                                 binary:copy(<<0>>, N), false),
                 B
         end,
    %% unpadded size = 9 + 4 + meta + term overhead + N
    Base = byte_size(term_to_binary(SnapshotMeta)) + 13 +
        (byte_size(term_to_binary(<<>>))),
    Exact = 4096 - Base,
    ?assertEqual(4096, Sz(Exact)),
    ?assertEqual(8192, Sz(Exact + 1)),
    ?assertEqual(4096, Sz(Exact - 1)),
    ok.

recover_unpadded_file(Config) ->
    %% files written before padding was introduced must still be readable
    Dir = ?config(dir, Config),
    File = filename:join(Dir, "snapshot.dat"),
    SnapshotMeta = meta(33, 94, [{banana, node@jungle}]),
    MetaBin = term_to_binary(SnapshotMeta),
    Data = [<<(byte_size(MetaBin)):32/unsigned>>, MetaBin,
            term_to_binary(my_state)],
    Crc = erlang:crc32(Data),
    ok = file:write_file(File, [<<"RASN", 1:8/unsigned, Crc:32/integer>>,
                                Data]),
    ?assertEqual({ok, SnapshotMeta, my_state}, ra_log_snapshot:recover(Dir)),
    ?assertEqual(ok, ra_log_snapshot:validate(Dir)),
    ?assertEqual({ok, SnapshotMeta}, ra_log_snapshot:read_meta(Dir)),
    ok.

padding_checksum_error(Config) ->
    Dir = ?config(dir, Config),
    File = filename:join(Dir, "snapshot.dat"),
    SnapshotMeta = meta(33, 94, [{banana, node@jungle}]),
    {ok, 4096} = ra_log_snapshot:write(Dir, SnapshotMeta, my_state, true),
    {ok, Fd} = file:open(File, [read, write, raw, binary]),
    ok = file:pwrite(Fd, 4095, <<1>>),
    ok = file:close(Fd),
    ?assertEqual({error, checksum_error}, ra_log_snapshot:recover(Dir)),
    ok.

encode_decode_image(_Config) ->
    Meta = meta(33, 94, [{banana, node@jungle}]),
    State = crypto:strong_rand_bytes(5000),
    %% padded and unpadded images both round trip in memory
    [begin
         {Image, Size} = ra_log_snapshot:encode(Meta, State, Pad),
         Bin = iolist_to_binary(Image),
         ?assertEqual(Size, byte_size(Bin)),
         ?assertEqual(Pad, Size rem 4096 == 0),
         ?assertEqual({ok, Meta, State}, ra_log_snapshot:decode_image(Bin)),
         ?assertEqual({ok, Meta}, ra_log_snapshot:meta_from_image(Bin)),
         %% corrupting the body is detected
         Bad = <<Bin:(byte_size(Bin) - 1)/binary, 255>>,
         ?assertEqual({error, checksum_error},
                      ra_log_snapshot:decode_image(Bad))
     end || Pad <- [true, false]],
    ?assertEqual({error, invalid_format},
                 ra_log_snapshot:decode_image(<<"nope">>)),
    ?assertEqual({error, invalid_format},
                 ra_log_snapshot:meta_from_image(<<"nope">>)),
    ?assertEqual({error, {invalid_version, 9}},
                 ra_log_snapshot:decode_image(<<"RASN", 9, 0:32, 1>>)),
    ?assertEqual({error, {invalid_version, 9}},
                 ra_log_snapshot:meta_from_image(<<"RASN", 9, 0:32, 1>>)),
    %% an image written by encode is byte-identical to the file write/4 makes
    Dir = ?config(dir, _Config),
    {ok, Bytes} = ra_log_snapshot:write(Dir, Meta, State, false),
    {ok, OnDisk} = file:read_file(filename:join(Dir, "snapshot.dat")),
    ?assertEqual(Bytes, byte_size(OnDisk)),
    {PaddedImage, _} = ra_log_snapshot:encode(Meta, State, true),
    ?assertEqual(iolist_to_binary(PaddedImage), OnDisk),
    ok.

%% Utility

meta(Idx, Term, Cluster) ->
    #{index => Idx,
      term => Term,
      cluster => Cluster,
      machine_version => 1}.
