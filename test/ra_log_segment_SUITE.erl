%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term Broadcom refers to Broadcom Inc. and/or its subsidiaries.
%%
-module(ra_log_segment_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

%% common ra_log tests to ensure behaviour is equivalent across
%% ra_log backends

all() ->
    [
     {group, tests}
    ].

all_tests() ->
    [
     open_close_persists_max_count,
     write_then_read,
     write_then_read_file_advise,
     write_then_read_no_checksums,
     read_cons,
     write_close_open_write,
     full_file,
     try_read_missing,
     overwrite,
     term_query,
     write_many,
     read_sparse_append_read,
     open_invalid,
     corrupted_segment,
     large_segment,
     segref,
     versions_v1
    ].

groups() ->
    [
     {tests, [], all_tests()}
    ].

init_per_testcase(TestCase, Config) ->
    PrivDir = ?config(priv_dir, Config),
    Dir = filename:join(PrivDir, TestCase),
    ok = ra_lib:make_dir(Dir),
    _ = ets:new(ra_open_file_metrics,
                [named_table, public, {write_concurrency, true}]),
    _ = ets:new(ra_io_metrics,
                [named_table, public, {write_concurrency, true}]),
    [{test_case, TestCase}, {data_dir, Dir} | Config].

end_per_testcase(_, Config) ->
    Config.


open_close_persists_max_count(Config) ->
    Dir = ?config(data_dir, Config),
    Fn = filename:join(Dir, "seg1.seg"),
    {ok, Seg0} = ra_log_segment:open(Fn, #{max_count => 128}),
    128 = ra_log_segment:max_count(Seg0),
    ok = ra_log_segment:close(Seg0),
    {ok, Seg} = ra_log_segment:open(Fn),
    128 = ra_log_segment:max_count(Seg),
    undefined = ra_log_segment:range(Seg),
    ok = ra_log_segment:close(Seg),
    ok.

corrupted_segment(Config) ->
    Dir = ?config(data_dir, Config),
    Fn = filename:join(Dir, "seg1.seg"),
    % Data = make_data(1024),
    Data = <<"banana">>,
    ok = open_write_close(1, 2, Data, Fn),
    %% truncate file a bit to simulate lost bytes
    truncate(Fn, {eof, -2}),
    %% ct:pal("DUMP PRE ~p", [ra_log_segment:dump_index(Fn)]),
    %% check that the current state throws a missing key
    {ok, SegR0} = ra_log_segment:open(Fn, #{mode => read}),
    ?assertNot(ra_log_segment:is_modified(SegR0)),
    ?assertExit({missing_key, 2},
                read_sparse(SegR0, [1, 2])),

    %% rewrite items as would happen if error was encountered
    ok = open_write_close(1, 2, Data, Fn),

    % ct:pal("DUMP ~p", [ra_log_segment:dump(Fn)]),
    % {ok, SegR} = ra_log_segment:open(Fn, #{mode => read}),
    % write_trunc_until_full(Fn),

    {ok, SegR} = ra_log_segment:open(Fn, #{mode => read}),
    [{1, 2, Data}] =
          ra_log_segment:fold(SegR, 1, 1,
                              fun ra_lib:id/1,
                              fun (E, A) -> [E | A] end,
                              []),
    %% for now we are just going to exit when reaching this point
    %% in the future we can find a strategy for handling this case
    ok.

truncate(Fn, Pos) ->
    {ok, Fd} = file:open(Fn, [read, write, raw, binary]),
    {ok, _Pos} = file:position(Fd, Pos),
    ok = file:truncate(Fd),
    ok = file:close(Fd),
    ok.

write_trunc_until_full(Fn) ->
    {ok, Seg0} = ra_log_segment:open(Fn),
    case ra_log_segment:append(Seg0, 1, 2, <<"banana">>) of
        {ok, Seg1} ->
            ok = ra_log_segment:close(Seg1),
            truncate(Fn, {eof, -2}),
            write_trunc_until_full(Fn);
        {error, full} ->
            ok = ra_log_segment:close(Seg0),
            ok
    end.


large_segment(Config) ->
    % tests items are bing persisted and index can be recovered
    Dir = ?config(data_dir, Config),
    Fn = filename:join(Dir, "seg1.seg"),
    {ok, Seg0} = ra_log_segment:open(Fn),
    Seg = lists:foldl(
      fun (Idx, S0) ->
              Data = term_to_iovec(crypto:strong_rand_bytes(1100 * 1100)),
              {ok, S} = ra_log_segment:append(S0, Idx, 1, Data),
              S
      end, Seg0, lists:seq(1, 4096)),
    ok = ra_log_segment:close(Seg),
    %% validate all entries can be read
    {ok, Seg1} = ra_log_segment:open(Fn, #{mode => read}),
    [begin
         [{Idx, 1, _B}] = read_sparse(Seg1, [Idx])
     end
     || Idx <- lists:seq(1, 4096)],
    ct:pal("Index ~p", [lists:last(ra_log_segment:dump_index(Fn))]),
    %% it's a large file, let's cleanup when test is successful
    file:delete(Fn),
    ok.

versions_v1(Config) ->
    Dir = ?config(data_dir, Config),
    Fn = filename:join(Dir, "seg1.seg"),
    Data = make_data(1024),
    Crc =  erlang:crc32(Data),
    NumEntries = 4,
    Idx =  1,
    Term = 2,
    Version = 1,
    %% v1 index record size was 28
    %% header size is 8
    DataOffset = 8 + (NumEntries * 28),
    %% in v1 the offset was 32 bit
    IndexData = <<Idx:64/unsigned, Term:64/unsigned,
                  DataOffset:32/unsigned,
                  (byte_size(Data)):32/unsigned,
                  Crc:32/unsigned>>,
    %% fake version 1
    Header = <<"RASG", Version:16/unsigned, NumEntries:16/unsigned>>,
    {ok, Fd} = file:open(Fn, [write, raw, binary]),
    ok = file:pwrite(Fd, [{0, Header},
                     {8, IndexData},
                     {DataOffset, Data}]),
    ok = file:sync(Fd),
    ok = file:close(Fd),
    {ok, R0} = ra_log_segment:open(Fn, #{mode => read}),
    [{Idx, Term, Data}] = read_sparse(R0, [Idx]),
    ok = ra_log_segment:close(R0),

    %% append as v1
    {ok, W0} = ra_log_segment:open(Fn),
    {ok, W} = ra_log_segment:append(W0, Idx+1, Term, Data),
    ok = ra_log_segment:close(W),
    %% read again
    {ok, R1} = ra_log_segment:open(Fn, #{mode => read}),
    [{Idx, Term, Data}] = read_sparse(R1, [Idx]),
    ok = ra_log_segment:close(R1),
    ok.

open_write_close(Idx, Term, Data, Fn) ->
    {ok, Seg0} = ra_log_segment:open(Fn),
    {ok, Seg1} = ra_log_segment:append(Seg0, Idx, Term, Data),
    {ok, Seg} = ra_log_segment:sync(Seg1),
    ok = ra_log_segment:close(Seg).

open_invalid(Config) ->
    Dir = ?config(data_dir, Config),
    Fn = filename:join(Dir, "seg1.seg"),
    {ok, Fd} = file:open(Fn, [write, raw, binary]),
    {error, missing_segment_header} = ra_log_segment:open(Fn),
    file:close(Fd),
    ok.

segref(Config) ->
    Dir = ?config(data_dir, Config),
    Fn = filename:join(Dir, "seg1.seg"),
    {ok, Seg0} = ra_log_segment:open(Fn, #{max_count => 128}),
    undefined = ra_log_segment:segref(Seg0),
    {ok, Seg1} = ra_log_segment:append(Seg0, 1, 2, <<"Adsf">>),
    {{1, 1}, "seg1.seg"} = ra_log_segment:segref(Seg1),
    ok.


full_file(Config) ->
    Dir = ?config(data_dir, Config),
    Fn = filename:join(Dir, "seg1.seg"),
    Data = make_data(10),
    {ok, Seg0} = ra_log_segment:open(Fn, #{max_count => 2,
                                           max_pending => 1}),
    {ok, Seg1} = ra_log_segment:append(Seg0, 1, 2, Data),
    {ok, Seg} = ra_log_segment:append(Seg1, 2, 2, Data),
    {error, full} = ra_log_segment:append(Seg, 3, 2, Data),
    ?assertNot(ra_log_segment:is_modified(Seg)),
    {1,2} = ra_log_segment:range(Seg),
    ok = ra_log_segment:close(Seg),
    ok.

write_close_open_write(Config) ->
    Dir = ?config(data_dir, Config),
    Fn = filename:join(Dir, "seg1.seg"),
    % create unique data so that the CRC check is triggered in case we
    % write to the wrong offset
    Data = fun (Num) ->
                   I = integer_to_binary(Num),
                   <<"data", I/binary>>
           end,
    {ok, Seg0} = ra_log_segment:open(Fn),
    {ok, Seg1} = ra_log_segment:append(Seg0, 1, 2, Data(1)),
    {ok, Seg} = ra_log_segment:append(Seg1, 2, 2, Data(2)),
    ok = ra_log_segment:close(Seg),

    % reopen file and append again
    {ok, SegA0} = ra_log_segment:open(Fn),
    % also open a reader
    {ok, SegA1} = ra_log_segment:append(SegA0, 3, 2, Data(3)),
    {ok, SegA} = ra_log_segment:sync(SegA1),
    % need to re-read index
    {ok, SegR} = ra_log_segment:open(Fn, #{mode => read}),
    {1, 3} = ra_log_segment:range(SegR),
    [{1, 2, <<"data1">>}, {2, 2, <<"data2">>}, {3, 2, <<"data3">>}] =
        read_sparse(SegR, [1, 2, 3]),
    ok = ra_log_segment:close(SegA),
    ok = ra_log_segment:close(SegR),
    ok.

write_then_read(Config) ->
    % tests items are bing persisted and index can be recovered
    Dir = ?config(data_dir, Config),
    Fn = filename:join(Dir, "seg1.seg"),
    Data = make_data(1024),
    {ok, Seg0} = ra_log_segment:open(Fn, #{compute_checksums => true}),
    {ok, Seg1} = ra_log_segment:append(Seg0, 1, 2, Data),
    {ok, Seg2} = ra_log_segment:append(Seg1, 2, 2, Data),
    {ok, Seg} = ra_log_segment:sync(Seg2),
    ok = ra_log_segment:close(Seg),

    % read two consecutive entries from index 1
    {ok, SegR} = ra_log_segment:open(Fn, #{mode => read}),
    [{1, 2, Data}, {2, 2, Data}] = read_sparse(SegR, [1, 2]),
    [{2, 2, Data}] = read_sparse(SegR, [2]),
    {1, 2} = ra_log_segment:range(SegR),
    ok = ra_log_segment:close(SegR),
    ok.

write_then_read_file_advise(Config) ->
    %% there is no real way to assert on the file_advise configuration
    %% this test is mostly just a means of getting some coverage
    % tests items are bing persisted and index can be recovered
    Dir = ?config(data_dir, Config),
    Fn = filename:join(Dir, "seg1.seg"),
    Data = make_data(1024),
    {ok, Seg0} = ra_log_segment:open(Fn, #{compute_checksums => true,
                                           file_advise => normal}),
    {ok, Seg1} = ra_log_segment:append(Seg0, 1, 2, Data),
    {ok, Seg2} = ra_log_segment:append(Seg1, 2, 2, Data),
    {ok, Seg} = ra_log_segment:sync(Seg2),
    ok = ra_log_segment:close(Seg),

    % read two consecutive entries from index 1
    {ok, SegR} = ra_log_segment:open(Fn, #{mode => read,
                                           file_advise => random}),
    [{1, 2, Data}, {2, 2, Data}] = read_sparse(SegR, [1, 2]),
    [{2, 2, Data}] = read_sparse(SegR, [2]),
    {1, 2} = ra_log_segment:range(SegR),
    ok = ra_log_segment:close(SegR),
    ok.

write_then_read_no_checksums(Config) ->
    % tests items are bing persisted and index can be recovered
    Dir = ?config(data_dir, Config),
    Fn = filename:join(Dir, "seg1.seg"),
    Data = make_data(1024),
    {ok, Seg0} = ra_log_segment:open(Fn, #{compute_checksums => false}),
    {ok, Seg1} = ra_log_segment:append(Seg0, 1, 2, Data),
    {ok, Seg2} = ra_log_segment:append(Seg1, 2, 2, Data),
    {ok, Seg} = ra_log_segment:sync(Seg2),
    ok = ra_log_segment:close(Seg),

    % read two consecutive entries from index 1
    {ok, SegR} = ra_log_segment:open(Fn, #{mode => read}),
    [{1, 2, Data}, {2, 2, Data}] = read_sparse(SegR, [1, 2]),
    [{2, 2, Data}] = read_sparse(SegR, [2]),
    {1, 2} = ra_log_segment:range(SegR),
    ok = ra_log_segment:close(SegR),
    ok.

read_cons(Config) ->
    Dir = ?config(data_dir, Config),
    Fn = filename:join(Dir, "seg1.seg"),
    Data = make_data(1024),
    {ok, Seg0} = ra_log_segment:open(Fn),
    {ok, Seg1} = ra_log_segment:append(Seg0, 1, 2, Data),
    {ok, Seg2} = ra_log_segment:append(Seg1, 2, 2, Data),
    {ok, Seg3} = ra_log_segment:append(Seg2, 3, 2, Data),
    {ok, Seg} = ra_log_segment:sync(Seg3),
    ok = ra_log_segment:close(Seg),

    %% end of setup
    {ok, SegR} = ra_log_segment:open(Fn, #{mode => read}),
    [{1, 2, Data}] = Read = read_sparse(SegR, [1]),
    %% validate a larger range still returns results
    [{1, 2, Data}, {2, 2, Data}, {3, 2, Data}] =
        lists:reverse(
          ra_log_segment:fold(SegR, 2, 3, fun ra_lib:id/1,
                            fun (E, A) -> [E | A] end,
                            Read)),
    ok = ra_log_segment:close(SegR),

    ok.

try_read_missing(Config) ->
    % tests items are bing persisted and index can be recovered
    Dir = ?config(data_dir, Config),
    Fn = filename:join(Dir, "seg1.seg"),
    Data = make_data(1024),
    {ok, Seg0} = ra_log_segment:open(Fn),
    {ok, Seg1} = ra_log_segment:append(Seg0, 1, 2, Data),
    {ok, Seg} = ra_log_segment:sync(Seg1),
    ok = ra_log_segment:close(Seg),

    {ok, SegR} = ra_log_segment:open(Fn, #{mode => read}),
    [_] = read_sparse(SegR, [1]),
    ok.

overwrite(Config) ->
    Dir = ?config(data_dir, Config),
    Fn = filename:join(Dir, "seg1.seg"),
    Data = make_data(1024),
    {ok, Seg0} = ra_log_segment:open(Fn),
    {ok, Seg1} = ra_log_segment:append(Seg0, 5, 1, Data),
    % overwrite - simulates follower receiving entries from new leader
    {ok, Seg2} = ra_log_segment:append(Seg1, 2, 2, Data),
    {2, 2} = ra_log_segment:range(Seg2),
    {ok, Seg} = ra_log_segment:sync(Seg2),
    {ok, SegR} = ra_log_segment:open(Fn, #{mode => read}),
    {2, 2} = ra_log_segment:range(Seg),
    ?assertExit({missing_key, 5}, read_sparse(SegR, [5])),
    [{2, 2, Data}] = read_sparse(SegR, [2]),
    ok = ra_log_segment:close(Seg),
    ok.

term_query(Config) ->
    Dir = ?config(data_dir, Config),
    Fn = filename:join(Dir, "term_query.seg"),
    {ok, Seg0} = ra_log_segment:open(Fn),
    {ok, Seg1} = ra_log_segment:append(Seg0, 5, 2, <<"a">>),
    {ok, Seg2} = ra_log_segment:append(Seg1, 6, 3, <<"b">>),
    _ = ra_log_segment:close(Seg2),
    {ok, Seg} = ra_log_segment:open(Fn, #{mode => read}),
    2 = ra_log_segment:term_query(Seg, 5),
    3 = ra_log_segment:term_query(Seg, 6),
    undefined = ra_log_segment:term_query(Seg, 7),
    _ = ra_log_segment:close(Seg),
    ok.

write_many(Config) ->
    Dir = ?config(data_dir, Config),
    Sizes = [10,
             100,
             1000
             %% commented out to not create insane data on ci
             % 10000,
             % 100000,
             % 256000
            ],
    Result =
    [begin
         {Max,
          [begin
               Data = make_data(Size),
               Name = integer_to_list(Max) ++ "_" ++  integer_to_list(Size) ++ ".seg",
               Fn = filename:join(Dir, Name),
               {ok, Seg0} = ra_log_segment:open(Fn, #{max_count => 4096 * 2,
                                                      max_pending => Max}),
               % start_profile(Config, [ra_log_segment,
               %                        file,
               %                        ra_file_handle,
               %                        prim_file]),

               {Taken, Seg} = timer:tc(
                                fun() ->
                                        S0 = write_until_full(1, 2, Data, Seg0),
                                        {ok, S} = ra_log_segment:flush(S0),
                                        S
                                end),
               % stop_profile(Config),
               % ct:pal("write_many ~b size ~b took ~bms",
               %        [Max, Size, Taken div 1000]),

               ok = ra_log_segment:close(Seg),
               {Size, Taken div 1000}
           end || Size <- Sizes]}
     end || Max <- [64,128,256,512,1024,2048,4096]],

    ct:pal("~p", [Result]),
    ok.


read_sparse_append_read(Config) ->
    Dir = ?config(data_dir, Config),
    Fn = filename:join(Dir, <<"0000000.segment">>),
    {ok, W0} = ra_log_segment:open(Fn, #{}),
    Data = <<"banana">>,
    Term = 1,
    %% write two entries in term 1
    {ok, W1} = ra_log_segment:append(W0, 1, Term, Data),
    {ok, W2} = ra_log_segment:append(W1, 2, Term, Data),
    {ok, W3} = ra_log_segment:flush(W2),


    {ok, R0} = ra_log_segment:open(Fn, #{mode => read}),
    {ok, 2, [_, _]}  = ra_log_segment:read_sparse(R0, [1, 2],
                                                  fun (I, _, _, Acc) ->
                                                          [I | Acc]
                                                  end, []),

    ?assertNot(ra_log_segment:is_modified(R0)),
    %% overwrite in term 2
    {ok, W4} = ra_log_segment:append(W3, 2, 2, <<"apple">>),
    {ok, W5} = ra_log_segment:append(W4, 3, 2, <<"apple">>),
    {ok, W} = ra_log_segment:flush(W5),
    ?assert(ra_log_segment:is_modified(R0)),
    {error, modified} = ra_log_segment:read_sparse(R0, [2],
                                                   fun (_I, _, B, Acc) ->
                                                           [B | Acc]
                                                   end, []),
    ra_log_segment:close(W),
    ra_log_segment:close(R0),
    ok.

write_until_full(Idx, Term, Data, Seg0) ->
    case ra_log_segment:append(Seg0, Idx, Term, Data) of
        {ok, Seg} ->
            write_until_full(Idx+1, Term, Data, Seg);
        {error, full} ->
            Seg0
    end.


%%% Internal
make_data(Size) ->
    term_to_binary(crypto:strong_rand_bytes(Size)).

read_sparse(R, Idxs) ->
    {ok, _, Entries} = ra_log_segment:read_sparse(R, Idxs,
                                                  fun (I, T, B, Acc) ->
                                                          [{I, T, B} | Acc]
                                                  end, []),
    lists:reverse(Entries).
