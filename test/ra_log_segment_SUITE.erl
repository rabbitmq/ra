-module(ra_log_segment_SUITE).

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
     overwrite,
     term_query,
     write_many,
     open_invalid,
     segref
    ].

groups() ->
    [
     {tests, [], all_tests()}
    ].

init_per_testcase(TestCase, Config) ->
    PrivDir = ?config(priv_dir, Config),
    Dir = filename:join(PrivDir, TestCase),
    _ = file:make_dir(Dir),
    _ = ets:new(ra_open_file_metrics,
                [named_table, public, {write_concurrency, true}]),
    _ = ets:new(ra_io_metrics,
                [named_table, public, {write_concurrency, true}]),
    ra_file_handle:start_link(),
    [{test_case, TestCase}, {data_dir, Dir} | Config].

end_per_testcase(_, Config) ->
    exit(whereis(ra_file_handle), normal),
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
    {1, 1, "seg1.seg"} = ra_log_segment:segref(Seg1),
    ok.


full_file(Config) ->
    Dir = ?config(data_dir, Config),
    Fn = filename:join(Dir, "seg1.seg"),
    Data = make_data(1024),
    {ok, Seg0} = ra_log_segment:open(Fn, #{max_count => 2}),
    {ok, Seg1} = ra_log_segment:append(Seg0, 1, 2, Data),
    {ok, Seg} = ra_log_segment:append(Seg1, 2, 2, Data),
    {error, full} = ra_log_segment:append(Seg, 3, 2, Data),
    {1,2} = ra_log_segment:range(Seg),
    ok = ra_log_segment:close(Seg),
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
        ra_log_segment:read(SegR, 1, 3),
    ok = ra_log_segment:close(SegA),
    ok = ra_log_segment:close(SegR),
    ok.

write_then_read(Config) ->
    % tests items are bing persisted and index can be recovered
    Dir = ?config(data_dir, Config),
    Fn = filename:join(Dir, "seg1.seg"),
    Data = make_data(1024),
    {ok, Seg0} = ra_log_segment:open(Fn),
    {ok, Seg1} = ra_log_segment:append(Seg0, 1, 2, Data),
    {ok, Seg2} = ra_log_segment:append(Seg1, 2, 2, Data),
    {ok, Seg} = ra_log_segment:sync(Seg2),
    ok = ra_log_segment:close(Seg),

    % read two consequtive entries from index 1
    {ok, SegR} = ra_log_segment:open(Fn, #{mode => read}),
    [{1, 2, Data}, {2, 2, Data}] = ra_log_segment:read(SegR, 1, 2),
    %% validate a larger range still returns results
    [{1, 2, Data}, {2, 2, Data}] = ra_log_segment:read(SegR, 1, 5),
    %% out of range returns nothing
    [{2, 2, Data}] = ra_log_segment:read(SegR, 2, 2),
    {1, 2} = ra_log_segment:range(SegR),
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
    [] = ra_log_segment:read(SegR, 2, 2),
    ok.

overwrite(Config) ->
    Dir = ?config(data_dir, Config),
    Fn = filename:join(Dir, "seg1.seg"),
    Data = make_data(1024),
    {ok, Seg0} = ra_log_segment:open(Fn),
    {ok, Seg1} = ra_log_segment:append(Seg0, 5, 2, Data),
    % overwrite - simulates follower receiving entries from new leader
    {ok, Seg2} = ra_log_segment:append(Seg1, 2, 2, Data),
    {2, 2} = ra_log_segment:range(Seg2),
    {ok, Seg} = ra_log_segment:sync(Seg2),
    {ok, SegR} = ra_log_segment:open(Fn, #{mode => read}),
    {2, 2} = ra_log_segment:range(Seg),
    [] = ra_log_segment:read(SegR, 5, 1),
    [{2, 2, Data}] = ra_log_segment:read(SegR, 2, 1),
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
    Fn = filename:join(Dir, "seg1.seg"),
    Data = make_data(1024),
    {ok, Seg0} = ra_log_segment:open(Fn),
    {Taken, {ok, Seg}} = timer:tc(fun() ->
                                    S = write_until_full(1, 2, Data, Seg0),
                                    ra_log_segment:sync(S)
                            end),
    ct:pal("write_many took ~pms~n", [Taken/1000]),

    ok = ra_log_segment:close(Seg),
    ok.

write_until_full(Idx, Term, Data, Seg0) ->
    case ra_log_segment:append(Seg0, Idx, Term, Data) of
        {ok, Seg} ->
            write_until_full(Idx+1, Term, Data, Seg);
        {error, full} ->
            Seg0
    end.


%%% Internal
%%% p
make_data(Size) ->
    term_to_binary(crypto:strong_rand_bytes(Size)).
