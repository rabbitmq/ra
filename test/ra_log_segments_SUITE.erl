%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2017-2025 Broadcom. All Rights Reserved. The term Broadcom
%% refers to Broadcom Inc. and/or its subsidiaries.
-module(ra_log_segments_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("kernel/include/file.hrl").

%% common ra_log tests to ensure behaviour is equivalent across
%% ra_log backends

all() ->
    [
     {group, tests}
    ].

all_tests() ->
    [
     recover1,
     recover2,
     basics,
     major,
     major_max_size,
     minor,
     overwrite,
     result_after_segments,
     result_after_segments_overwrite
    ].

groups() ->
    [
     {tests, [], all_tests()}
    ].

init_per_testcase(TestCase, Config) ->
    PrivDir = ?config(priv_dir, Config),
    Dir = filename:join(PrivDir, TestCase),
    ok = ra_lib:make_dir(Dir),
    CompConf = #{max_count => 128,
                 max_size => 128_000},
    [{uid, atom_to_binary(TestCase, utf8)},
     {comp_conf, CompConf},
     {test_case, TestCase},
     {dir, Dir} | Config].

end_per_testcase(_, Config) ->
    Config.

%% TESTS

result_after_segments(Config) ->
    Dir = ?config(dir, Config),
    LiveList = lists:seq(1, 128 * 3, 8),
    Live = ra_seq:from_list(LiveList),
    Scen =
    [
     {entries, 1, lists:seq(1, 128 * 3)},
     {major, 128 * 3, Live},
     {entries, 1, lists:seq(128 * 3, 128 * 4)},
     handle_compaction_result,
     {assert, 1, LiveList},
     {assert, 1, lists:seq(128 * 3, 128 * 4)},
     {assert, fun (S) ->
                      ct:pal("seg refs ~p", [ra_log_segments:segment_refs(S)]),
                      true
              end}
    ],
    SegConf = #{max_count => 128},
    Segs0 = ra_log_segments_init(Config, Dir, seg_refs(Dir)),
    run_scenario([{seg_conf, SegConf} | Config], Segs0, Scen),
    ct:pal("infos ~p", [infos(Dir)]),
    ok.

result_after_segments_overwrite(Config) ->
    Dir = ?config(dir, Config),
    LiveList = lists:seq(1, 128 * 2, 8),
    Live = ra_seq:from_list(LiveList),
    Scen =
    [
     {entries, 1, lists:seq(1, 128 * 3)},
     {major, 128 * 3, Live},
     {entries, 2, lists:seq(128 * 2, 128 * 4)},
     handle_compaction_result,
     {print, "1"},
     {assert, 1, ra_seq:expand(ra_seq:limit(128 * 2, Live))},
     {assert, 2, lists:seq(128 * 2, 128 * 3)},
     {print, "2"},
     {assert, fun (S) ->
                      ct:pal("seg refs ~p", [ra_log_segments:segment_refs(S)]),
                      true
              end},
     {assert, 2, lists:seq(128 * 3, 128 * 4)}
    ],
    SegConf = #{max_count => 128},
    Segs0 = ra_log_segments_init(Config, Dir, seg_refs(Dir)),
    run_scenario([{seg_conf, SegConf} | Config], Segs0, Scen),
    ct:pal("infos ~p", [infos(Dir)]),
    ok.

recover1(Config) ->
    %% major compactions can be interrupted at a variety of points and each
    %% needs to be handled carefully to ensure the log isn't incorrectly
    %% recovered

    %% There is a .compacting file in the segments directory
    %% 1. Compaction stopped before the compacting segment got renamed to
    %% the lowest numbered segments.
    %% 2. Compaction stopped before or during additional segments in the group
    %% where linked (.compacting link count > 1)

    Dir = ?config(dir, Config),
    CompactingFn = <<"0000000000000001-0000000000000002-0000000000000003.compacting">>,
    Scen =
    [
     {entries, 1, lists:seq(1, 128 * 4)},
     {assert, fun (_) ->
                      ok = ra_lib:write_file(filename:join(Dir, CompactingFn), <<>>),
                      true
              end},
     reinit,
     {assert, fun (_) ->
                      %% a compacting file with 1 link only should just be deleted
                      %% during init
                      not filelib:is_file(filename:join(Dir, CompactingFn))
              end}
    ],
    ct:pal("infos ~p", [infos(Dir)]),
    SegConf = #{max_count => 128},
    Segs0 = ra_log_segments_init(Config, Dir, seg_refs(Dir)),
    run_scenario([{seg_conf, SegConf} | Config], Segs0, Scen),
    ok.

recover2(Config) ->
    Dir = ?config(dir, Config),
    LiveList = lists:seq(1, 128 * 3, 8),
    Live = ra_seq:from_list(LiveList),
    CompactingShortFn = <<"0000000000000001-0000000000000002-0000000000000003.compacting">>,
    CompactingFn = filename:join(Dir, CompactingShortFn),
    Scen =
    [
     {entries, 1, lists:seq(1, 128 * 4)},
     {assert, fun (_) ->
                      %% fake a .compacting file, and perform a copy
                      %% (this code is copied from ra_log_segments
                      All = lists:reverse(tl(seg_refs(Dir))),
                      {ok, CompSeg0} = ra_log_segment:open(CompactingFn,
                                                           #{max_count => 128}),
                      CompSeg = lists:foldl(
                                  fun ({F, R}, S0) ->
                                          L = ra_seq:in_range(R, Live),
                                          {ok, S} = ra_log_segment:copy(
                                                      S0, filename:join(Dir, F),
                                                      ra_seq:expand(L)),
                                          S
                                  end, CompSeg0, All),
                      ok = ra_log_segment:close(CompSeg),
                      [{FstFn, _}, {SndFn, _}, {_ThrFn, _}] = All,

                      CompactedFn = filename:join(Dir, with_ext(FstFn, ".compacted")),
                      ok = prim_file:make_link(CompactingFn, CompactedFn),
                      FirstSegmentFn = filename:join(Dir, FstFn),
                      ok = prim_file:rename(CompactedFn, FirstSegmentFn),
                      % SecondFn = filename:join(Dir, SndFn),
                      SndLinkFn = filename:join(Dir, with_ext(SndFn, ".link")),
                      ok = prim_file:make_symlink(FirstSegmentFn, SndLinkFn),
                      %% the first segment has now been replaced with a link
                      %% to the compacting segment but not all symlinks may have been created
                      true
            end},
     reinit,
     {assert, fun (_) ->
                      Infos = infos(Dir),
                      NumLinks = length([a || #{file_type := symlink} <- Infos]),
                      %% a compacting file with 1 link only should just be deleted
                      %% during init
                      not filelib:is_file(CompactingFn) andalso
                      NumLinks == 2
              end},
     {assert, fun(S) ->
                      SegRefs = ra_log_segments:segment_refs(S),
                      ct:pal("SegRefs ~p, ~p", [SegRefs, seg_refs(Dir)]),
                      SegRefs == seg_refs(Dir)
              end}



    ],
    SegConf = #{max_count => 128},
    Segs0 = ra_log_segments_init(Config, Dir, seg_refs(Dir)),
    run_scenario([{seg_conf, SegConf} | Config], Segs0, Scen),
    ct:pal("infos ~p", [infos(Dir)]),
    ok.

basics(Config) ->
    %% creates 3 segments then a snapshot at the first index of the last segment
    %% with live indexes only in the first segment.
    Dir = ?config(dir, Config),
    LiveList = lists:seq(1, 128, 5),
    Live = ra_seq:from_list(LiveList),
    ct:pal("Live ~p", [Live]),
    Scen =
    [
     {entries, 1, lists:seq(1, 128 * 3)},
     {assert, 1, lists:seq(1, 128 * 3)},
     {assert, fun (S) ->
                      SegRefs = ra_log_segments:segment_refs(S),
                      length(SegRefs) == 3
              end},
     %% this compaction will delete 1 segment (segment 2)
     {minor, 128 * 2, Live},
     handle_compaction_result,
     {assert, 1, LiveList},
     {assert, fun (S) ->
                      SegRefs = ra_log_segments:segment_refs(S),
                      length(SegRefs) == 2
              end},
     %% this compaction will compact segment 1
     {major, 128 * 2, Live},
     handle_compaction_result,
     reinit,
     {assert, 1, LiveList},
     {assert, fun (S) ->
                      [#{num_entries := NumEntries} |_ ] = infos(Dir),
                      SegRefs = ra_log_segments:segment_refs(S),
                      length(SegRefs) == 2 andalso
                      ra_seq:length(Live) == NumEntries

              end}
    ],


    SegConf = #{max_count => 128},
    Segs0 = ra_log_segments_init(Config, Dir, seg_refs(Dir)),
    run_scenario([{seg_conf, SegConf} | Config], Segs0, Scen),

    ct:pal("infos ~p", [infos(Dir)]),
    ok.

minor(Config) ->
    LiveList = [1 | lists:seq(257, 500, 10)],
    Live = ra_seq:from_list(LiveList),
    Scen =
    [
     {entries, 1, lists:seq(1, 500)},
     {assert, 1, lists:seq(1, 500)},
     {assert, fun (S) ->
                      SegRefs = ra_log_segments:segment_refs(S),
                      length(SegRefs) == 4
              end},
     {minor, 500, Live},
     handle_compaction_result,
     {assert, 1, LiveList},
     {assert, fun (S) ->
                      SegRefs = ra_log_segments:segment_refs(S),
                      length(SegRefs) == 3
              end},
     reinit,
     {assert, 1, LiveList},
     {assert, fun (S) ->
                      SegRefs = ra_log_segments:segment_refs(S),
                      length(SegRefs) == 3
              end},

     %% simulate a purge command
     {entries, 1, [501]},
     {minor, 501, []},
     handle_compaction_result,
     {assert, fun (S) ->
                      SegRefs = ra_log_segments:segment_refs(S),
                      length(SegRefs) == 1
              end}
    ],

    SegConf = #{max_count => 128},
    Dir = ?config(dir, Config),
    Segs0 = ra_log_segments_init(Config, Dir, seg_refs(Dir)),
    run_scenario([{seg_conf, SegConf} | Config], Segs0, Scen),

    ok.

overwrite(Config) ->
    Live = ra_seq:from_list(lists:seq(1, 600, 10)),
    Scen =
    [
     {entries, 1, lists:seq(1, 500)},
     {entries, 2, lists:seq(200, 700)},
     reinit,
     {assert, 1, lists:seq(1, 200, 10)},
     {assert, 2, lists:seq(201, 700, 10)},
     {minor, 600, Live},
     handle_compaction_result,
     {major, 600, Live},
     {assert, 1, lists:seq(1, 200, 10)},
     {assert, 2, lists:seq(201, 700, 10)},
     handle_compaction_result,
     {assert, 1, lists:seq(1, 200, 10)},
     {assert, 2, lists:seq(201, 700, 10)}
    ],

    SegConf = #{max_count => 128},
    Dir = ?config(dir, Config),
    Segs0 = ra_log_segments_init(Config, Dir, seg_refs(Dir)),
    run_scenario([{seg_conf, SegConf} | Config], Segs0, Scen),
    ok.

major_max_size(Config) ->
    %% this test could compact 3 segemtns into one just based on entry counts
    %% however the max_size configuration needs to be taken into account
    %% with the compaction grouping and not create an oversized taget segment
    Dir = ?config(dir, Config),
    Data = crypto:strong_rand_bytes(2000),
    Entries = [{I, 1, term_to_binary(Data)}
               || I <- lists:seq(1, 128 * 4)],
    LiveList = lists:seq(1, 30) ++
               lists:seq(128, 128 + 30) ++
               lists:seq(256, 256 + 30),
    Live = ra_seq:from_list(LiveList),
    Scen =
    [
     {entries, 1, Entries},
     {assert, 1, lists:seq(1, 128 * 4)},
     {assert, fun (S) ->
                      SegRefs = ra_log_segments:segment_refs(S),
                      length(SegRefs) == 4
              end},
     {major, 128 * 4, Live},
     handle_compaction_result,
     {assert, 1, LiveList},
     {assert, fun (S) ->
                      %% infos contain one symlink
                      Infos = infos(Dir),
                      ct:pal("Infos ~p", [Infos]),
                      Symlinks = [I || #{file_type := symlink} = I <- Infos],
                      SegRefs = ra_log_segments:segment_refs(S),
                      length(SegRefs) == 3 andalso
                      length(Infos) == 4 andalso
                      length(Symlinks) == 1
              end}
    ],
    SegConf = #{max_count => 128},
    Segs0 = ra_log_segments_init(Config, Dir, seg_refs(Dir)),
    run_scenario([{seg_conf, SegConf} | Config], Segs0, Scen),
    ok.

major(Config) ->
    Dir = ?config(dir, Config),
    Entries1 = lists:seq(1, 641),
    LiveList = lists:seq(1, 383, 3) ++
                lists:seq(384, 511) ++ %% the 4th segment is still full
                lists:seq(512, 640, 3),
    Live = ra_seq:from_list(LiveList),
    Scen =
    [
     {entries, 1, Entries1},
     {assert, 1, lists:seq(1, 641)},
     {assert, fun (S) ->
                      SegRefs = ra_log_segments:segment_refs(S),
                      length(SegRefs) == 6
              end},
     {major, 640, Live},
     handle_compaction_result,
     {assert, 1, LiveList},
     {assert, fun (S) ->
                      %% infos contain one symlink
                      Infos = infos(Dir),
                      Symlinks = [I || #{file_type := symlink} = I <- Infos],
                      SegRefs = ra_log_segments:segment_refs(S),
                      length(SegRefs) == 5 andalso
                      length(Infos) == 6 andalso
                      length(Symlinks) == 1
              end},
     reinit,
     {assert, 1, LiveList},
     {assert, fun (S) ->
                      SegRefs = ra_log_segments:segment_refs(S),
                      length(SegRefs) == 5
              end},
     {assert,
      fun (_S) ->
              ok = ra_log_segments:purge_symlinks(Dir, 1),
              [_] = [I || #{file_type := symlink} = I <- infos(Dir)],
              timer:sleep(2000),
              ok = ra_log_segments:purge_symlinks(Dir, 1),
              Infos = infos(Dir),
              Symlinks = [I || #{file_type := symlink} = I <- Infos],
              Files = [I || #{file_type := regular} = I <- Infos],
              length(Symlinks) == 0 andalso
              length(Files) == 5
      end}
    ],

    SegConf = #{max_count => 128},
    Segs0 = ra_log_segments_init(Config, Dir, seg_refs(Dir)),
    run_scenario([{seg_conf, SegConf} | Config], Segs0, Scen),

    ok.

%% Helpers

open_last_segment(Config, SegConf) ->
    Dir = ?config(dir, Config),
    case seg_refs(Dir) of
        [] ->
            Fn = ra_lib:zpad_filename("", "segment", 1),
            SegFn = filename:join(Dir, Fn),
            {ok, Seg} = ra_log_segment:open(SegFn, SegConf),
            Seg;
        [{Fn, _} | _] ->
            SegFn = filename:join(Dir, Fn),
            {ok, Seg} = ra_log_segment:open(SegFn, SegConf),
            Seg
    end.



append_to_segment(Seg0, [], Refs, _Conf) ->
    {Seg0, [ra_log_segment:segref(Seg0) | Refs]};
append_to_segment(Seg0, [{Idx, Term, Data} | Rem] = All, Refs, Conf) ->
    DataSize = iolist_size(Data),
    case ra_log_segment:append(Seg0, Idx, Term, {DataSize, Data}) of
        {ok, Seg} ->
            append_to_segment(Seg, Rem, Refs, Conf);
        {error, full} ->
            Ref = ra_log_segment:segref(Seg0),
            % close and open a new segment
            append_to_segment(open_successor_segment(Seg0, Conf), All,
                              [Ref | Refs], Conf)
    end.

open_successor_segment(CurSeg, SegConf) ->
    Fn0 = ra_log_segment:filename(CurSeg),
    Fn = ra_lib:zpad_filename_incr(Fn0),
    ok = ra_log_segment:close(CurSeg),
    {ok, Seg} =  ra_log_segment:open(Fn, SegConf),
    Seg.


seg_refs(Dir) ->
    lists:reverse(
      [ra_log_segment:segref(F) || F <- segment_files(Dir),
                                   is_regular(F) andalso
      lists:member(filename:extension(F), [".segment", <<".segment">>])]).

infos(Dir) ->
    [ra_log_segment:info(F) || F <- segment_files(Dir)].

segment_files(Dir) ->
    case prim_file:list_dir(Dir) of
        {ok, Files0} ->
            Files = [filename:join(Dir, F)
                     || F <- Files0,
                        begin
                            Ext = filename:extension(F),
                            lists:member(Ext, [".segment",
                                               ".compacting",
                                               ".compacted",
                                               ".link"])
                        end],
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

is_regular(Filename) ->
    {ok, #file_info{type = T}} = file:read_link_info(Filename, [raw, {time, posix}]),
    T == regular.

run_scenario(_, Segs, []) ->
    Segs;
run_scenario(Config, Segs0, [reinit | Rem]) ->
    Dir = ?config(dir, Config),
    CompConf = ?config(comp_conf, Config),
    ra_log_segments:close(Segs0),
    Segs = ra_log_segments:init(?config(uid, Config), Dir, 1, random,
                                seg_refs(Dir), undefined, CompConf, ""),
    ?FUNCTION_NAME(Config, Segs, Rem);
run_scenario(Config, Segs0, [{entries, Term, IndexesOrEntries} | Rem]) ->
    SegConf = ?config(seg_conf, Config),
    Seg0 = open_last_segment(Config, SegConf),
    Entries = case is_tuple(hd(IndexesOrEntries)) of
                  true ->
                      IndexesOrEntries;
                  false ->
                      [{I, Term, term_to_binary(<<"data1">>)}
                       || I <- IndexesOrEntries]
              end,
    {Seg, Refs} = append_to_segment(Seg0, Entries, [], SegConf),
    _ = ra_log_segment:close(Seg),
    {Segs, _Overwritten} = ra_log_segments:update_segments(Refs, Segs0),
    %% TODO: what to do about overwritten
    ?FUNCTION_NAME(Config, Segs, Rem);
run_scenario(Config, Segs0, [{Type, SnapIdx, Live} | Rem])
  when Type == major orelse Type == minor ->
    Effs = ra_log_segments:schedule_compaction(Type, SnapIdx, Live, Segs0),
    Segs = lists:foldl(fun ({bg_work, Fun, _}, S0) ->
                               Fun(),
                               S0;
                           ({next_event, {ra_log_event,
                                          {compaction_result, _Res}} = E}, S0) ->
                               self() ! E,
                               S0
                       end, Segs0, Effs),

    ?FUNCTION_NAME(Config, Segs, Rem);
run_scenario(Config, Segs0, [handle_compaction_result = Step | Rem]) ->
    CompRes3 = receive
                   {ra_log_event, {compaction_result, Res3}} ->
                       ct:pal("compaction result ~p", [Res3]),
                       Res3
               after 5000 ->
                         flush(),
                         exit({ra_log_event_timeout, Step})
               end,

    {Segs1, Effs} =
        ra_log_segments:handle_compaction_result(CompRes3, Segs0),
    [Fun1() || {bg_work, Fun1, _} <- Effs],
    ?FUNCTION_NAME(Config, Segs1, Rem);
run_scenario(Config, Segs0, [{assert, Term, Indexes} | Rem]) ->
    {Read, Segs2} = ra_log_segments:sparse_read(Segs0,
                                                lists:reverse(Indexes), []),
    %% assert we can read
    ?assertEqual(length(Indexes), length(Read)),
    ?assert(lists:all(fun ({_, T, _}) -> T == Term end, Read)),
    ?FUNCTION_NAME(Config, Segs2, Rem);
run_scenario(Config, Segs0, [{assert, Fun} | Rem])
  when is_function(Fun) ->
    ?assert(Fun(Segs0)),
    ?FUNCTION_NAME(Config, Segs0, Rem);
run_scenario(Config, Segs0, [{print, What} | Rem]) ->
    ct:pal(What),
    ?FUNCTION_NAME(Config, Segs0, Rem).


with_ext(Fn, Ext) when is_binary(Fn) andalso is_list(Ext) ->
    <<(filename:rootname(Fn))/binary, (ra_lib:to_binary(Ext))/binary>>.

ra_log_segments_init(Config, Dir, SegRefs) ->
    UId = ?config(uid, Config),
    CompConf = ?config(comp_conf, Config),
    ra_log_segments:init(UId, Dir, 1, random,
                         SegRefs, undefined,
                         CompConf, "").
