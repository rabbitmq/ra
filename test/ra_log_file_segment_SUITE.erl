-module(ra_log_file_segment_SUITE).

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
     write_close_open_write,
     full_file,
     try_read_missing,
     overwrite
    ].

groups() ->
    [
     {tests, [], all_tests()}
    ].

init_per_testcase(TestCase, Config) ->
    PrivDir = ?config(priv_dir, Config),
    Dir = filename:join(PrivDir, TestCase),
    _ = file:make_dir(Dir),
    [{test_case, TestCase}, {data_dir, Dir} | Config].

open_close_persists_max_count(Config) ->
    Dir = ?config(data_dir, Config),
    Fn = filename:join(Dir, "seg1.seg"),
    {ok, Seg0} = ra_log_file_segment:open(Fn, #{max_count => 128}),
    128 = ra_log_file_segment:max_count(Seg0),
    ok = ra_log_file_segment:close(Seg0),
    {ok, Seg} = ra_log_file_segment:open(Fn),
    128 = ra_log_file_segment:max_count(Seg),
    ok = ra_log_file_segment:close(Seg),
    ok.

full_file(Config) ->
    Dir = ?config(data_dir, Config),
    Fn = filename:join(Dir, "seg1.seg"),
    Data = make_data(1024),
    {ok, Seg0} = ra_log_file_segment:open(Fn, #{max_count => 2}),
    {ok, Seg1} = ra_log_file_segment:append(Seg0, 1, 2, Data),
    {ok, Seg} = ra_log_file_segment:append(Seg1, 2, 2, Data),
    {error, full} = ra_log_file_segment:append(Seg, 3, 2, Data),
    ok = ra_log_file_segment:close(Seg),
    ok.

write_close_open_write(Config) ->
    Dir = ?config(data_dir, Config),
    Fn = filename:join(Dir, "seg1.seg"),
    % create unique data so that the CRC check is trigged in case we
    % write to the wrong offset
    Data = fun (Num) ->
                   I = integer_to_binary(Num),
                   <<"data", I/binary>>
           end,
    {ok, Seg0} = ra_log_file_segment:open(Fn),
    {ok, Seg1} = ra_log_file_segment:append(Seg0, 1, 2, Data(1)),
    {ok, Seg} = ra_log_file_segment:append(Seg1, 2, 2, Data(2)),
    ok = ra_log_file_segment:sync(Seg),
    ok = ra_log_file_segment:close(Seg),

    % reopen file and append again
    {ok, SegA0} = ra_log_file_segment:open(Fn),
    % also open a reader
    {ok, SegA} = ra_log_file_segment:append(SegA0, 3, 2, Data(3)),
    ok = ra_log_file_segment:sync(SegA),
    % need to re-read index
    {ok, SegR} = ra_log_file_segment:open(Fn, #{mode => read}),
    [{1, 2, <<"data1">>}, {2, 2, <<"data2">>}, {3, 2, <<"data3">>}] =
        ra_log_file_segment:read(SegR, 1, 3),
    ok = ra_log_file_segment:close(SegA),
    ok = ra_log_file_segment:close(SegR),
    ok.

write_then_read(Config) ->
    % tests items are bing persisted and index can be recovered
    Dir = ?config(data_dir, Config),
    Fn = filename:join(Dir, "seg1.seg"),
    Data = make_data(1024),
    {ok, Seg0} = ra_log_file_segment:open(Fn),
    {ok, Seg1} = ra_log_file_segment:append(Seg0, 1, 2, Data),
    {ok, Seg} = ra_log_file_segment:append(Seg1, 2, 2, Data),
    ok = ra_log_file_segment:sync(Seg),
    ok = ra_log_file_segment:close(Seg),

    % read two consequtive entries from index 1
    {ok, SegR} = ra_log_file_segment:open(Fn, #{mode => read}),
    [{1, 2, Data}, {2, 2, Data}] = ra_log_file_segment:read(SegR, 1, 2),
    ok = ra_log_file_segment:close(SegR),
    ok.

try_read_missing(Config) ->
    % tests items are bing persisted and index can be recovered
    Dir = ?config(data_dir, Config),
    Fn = filename:join(Dir, "seg1.seg"),
    Data = make_data(1024),
    {ok, Seg0} = ra_log_file_segment:open(Fn),
    {ok, Seg} = ra_log_file_segment:append(Seg0, 1, 2, Data),
    ok = ra_log_file_segment:sync(Seg),
    ok = ra_log_file_segment:close(Seg),

    {ok, SegR} = ra_log_file_segment:open(Fn, #{mode => read}),
    [] = ra_log_file_segment:read(SegR, 2, 2),
    ok.

overwrite(Config) ->
    Dir = ?config(data_dir, Config),
    Fn = filename:join(Dir, "seg1.seg"),
    Data = make_data(1024),
    {ok, Seg0} = ra_log_file_segment:open(Fn),
    {ok, Seg1} = ra_log_file_segment:append(Seg0, 5, 2, Data),
    % overwrite - simulates follower receiving entries from new leader
    {ok, Seg} = ra_log_file_segment:append(Seg1, 2, 2, Data),
    ok = ra_log_file_segment:sync(Seg),
    ok = ra_log_file_segment:close(Seg),
    {ok, SegR} = ra_log_file_segment:open(Fn, #{mode => read}),
    [] = ra_log_file_segment:read(SegR, 5, 1),
    [{2, 2, Data}] = ra_log_file_segment:read(SegR, 2, 1),
    ok.



%%% Internal
%%% p
make_data(Size) ->
    term_to_binary(crypto:strong_rand_bytes(Size)).
