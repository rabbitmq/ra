%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term Broadcom refers to Broadcom Inc. and/or its subsidiaries.
%%
-module(ra_log_segments_SUITE).

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
     basics,
     major
    ].

groups() ->
    [
     {tests, [], all_tests()}
    ].

init_per_testcase(TestCase, Config) ->
    PrivDir = ?config(priv_dir, Config),
    Dir = filename:join(PrivDir, TestCase),
    ok = ra_lib:make_dir(Dir),
    [{uid, atom_to_binary(TestCase, utf8)},
     {test_case, TestCase},
     {data_dir, Dir} | Config].

end_per_testcase(_, Config) ->
    Config.

major(Config) ->
    SegConf = #{max_count => 128},
    Dir = ?config(data_dir, Config),
    Seg0 = open_first_segment(Dir, SegConf),
    Entries = [{I, 1, term_to_binary(<<"data1">>)} || I <- lists:seq(1, 641)],
    Seg = append_to_segment(Seg0, Entries, SegConf),
    _ = ra_log_segment:close(Seg),
    SegRefs = seg_refs(Dir),
    UId = ?config(uid, Config),
    Segs0 = ra_log_segments:init(UId, Dir, 1, lists:reverse(SegRefs)),
    Live = ra_seq:from_list(lists:seq(1, 383, 3) ++
                            lists:seq(384, 511) ++ %% the 4th segment is still full
                            lists:seq(512, 640, 3)
                           ),
    [{bg_work, Fun3, _}] = ra_log_segments:schedule_compaction(major, 640,
                                                               Live, Segs0),
    Fun3(),
    CompRes3 = receive
                   {ra_log_event, {compaction_result, Res3}} ->
                       ct:pal("ra_log_event ~p", [Res3]),
                       Res3
               after 5000 ->
                         flush(),
                         exit(ra_log_event_timeout_2)
               end,

    {Segs1, [{bg_work, Fun4, _}]} =
        ra_log_segments:handle_compaction_result(CompRes3, Segs0),
    Fun4(),

    {Read, _} = ra_log_segments:sparse_read(Segs1, ra_seq:expand(Live), []),
    %% assert we can read
    ?assertEqual(ra_seq:length(Live), length(Read)),
    ct:pal("seg refs state after ~p", [ra_log_segments:segment_refs(Segs1)]),
    % ct:pal("SegRefs After ~p", [seg_refs(Dir)]),
    ct:pal("Infos After ~p", [infos(Dir)]),
    [{bg_work, Fun5, _}] = ra_log_segments:schedule_compaction(major, 640,
                                                               Live, Segs1),
    Fun5(),
    %% ensure idempotency
    receive
        {ra_log_event, {compaction_result,
                        {compaction_result, [], [], []}}} ->
            ok
    after 5000 ->
              flush(),
              exit(compaction_result_timeout)
    end,
    flush(),


    ok.

basics(Config) ->
    SegConf = #{max_count => 128},
    Dir = ?config(data_dir, Config),
    Seg0 = open_first_segment(Dir, SegConf),
    Entries = [{I, 1, term_to_binary(<<"data1">>)} || I <- lists:seq(1, 128 * 3)],
    Seg = append_to_segment(Seg0, Entries, SegConf),
    _ = ra_log_segment:close(Seg),
    SegRefs = seg_refs(Dir),
    UId = ?config(uid, Config),
    Segs0 = ra_log_segments:init(UId, Dir, 1, lists:reverse(SegRefs)),
    ct:pal("seg refs ~p", [ra_log_segments:segment_refs(Segs0)]),
    Live = ra_seq:from_list(lists:seq(1, 128, 5)),
    [{bg_work, Fun1, _}] = ra_log_segments:schedule_compaction(minor, 128 * 2,
                                                               Live, Segs0),
    Fun1(),
    CompRes1 = receive
                   {ra_log_event, {compaction_result, Res1}} ->
                       ct:pal("ra_log_event ~p", [Res1]),
                       Res1
               after 5000 ->
                         flush(),
                         exit(ra_log_event_timeout)
               end,

    {Segs1, [{bg_work, Fun2, _}]} =
        ra_log_segments:handle_compaction_result(CompRes1, Segs0),
    Fun2(),
    [{bg_work, Fun3, _}] = ra_log_segments:schedule_compaction(major, 128 * 2,
                                                               Live, Segs1),
    Fun3(),
    CompRes3 = receive
                   {ra_log_event, {compaction_result, Res3}} ->
                       ct:pal("ra_log_event ~p", [Res3]),
                       Res3
               after 5000 ->
                         flush(),
                         exit(ra_log_event_timeout_2)
               end,

    {Segs2, [{bg_work, Fun4, _}]} =
        ra_log_segments:handle_compaction_result(CompRes3, Segs1),
    Fun4(),
    ct:pal("SegRefs After ~p", [seg_refs(Dir)]),
    ct:pal("Infos After ~p", [infos(Dir)]),
    ok.


%% Helpers

open_first_segment(Dir, SegConf) ->
    Fn = ra_lib:zpad_filename("", "segment", 1),
    SegFn = filename:join(Dir, Fn),
    {ok, Seg} = ra_log_segment:open(SegFn, SegConf),
    Seg.

append_to_segment(Seg0, [], _Conf) ->
    Seg0;
append_to_segment(Seg0, [{Idx, Term, Data} | Rem] = All, Conf) ->
    DataSize = iolist_size(Data),
    case ra_log_segment:append(Seg0, Idx, Term, {DataSize, Data}) of
        {ok, Seg} ->
            append_to_segment(Seg, Rem, Conf);
        {error, full} ->
            % close and open a new segment
            append_to_segment(open_successor_segment(Seg0, Conf), All, Conf)
    end.

open_successor_segment(CurSeg, SegConf) ->
    Fn0 = ra_log_segment:filename(CurSeg),
    Fn = ra_lib:zpad_filename_incr(Fn0),
    ok = ra_log_segment:close(CurSeg),
    {ok, Seg} =  ra_log_segment:open(Fn, SegConf),
    Seg.


seg_refs(Dir) ->
    [ra_log_segment:segref(F) || F <- segment_files(Dir)].

infos(Dir) ->
    [ra_log_segment:info(F) || F <- segment_files(Dir)].

segment_files(Dir) ->
    case prim_file:list_dir(Dir) of
        {ok, Files0} ->
            Files = [filename:join(Dir, F)
                     || F <- Files0, filename:extension(F) =:= ".segment"],
            lists:sort(Files);
        {error, enoent} ->
            []
    end.

flush() ->
    receive
        Any ->
            ct:pal("flush ~p", [Any]),
            flush()
    after 0 ->
              ok
    end.
