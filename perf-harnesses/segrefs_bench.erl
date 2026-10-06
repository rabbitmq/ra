-module(segrefs_bench).

-export([run/0]).

t(Name, Fun, N) ->
    _ = Fun(),
    erlang:garbage_collect(),
    {Time, _} = timer:tc(fun () -> loop(Fun, N) end),
    io:format("~-58s ~12.2f us/op~n", [Name, Time / N]),
    Time / N.

loop(_Fun, 0) -> ok;
loop(Fun, N) -> _ = Fun(), loop(Fun, N - 1).

run() ->
    io:format("~n=== ra_log_segments segref bookkeeping vs segment count ===~n"),
    Dir = "/tmp/ra_segrefs_bench",
    _ = os:cmd("rm -rf " ++ Dir),
    ok = filelib:ensure_dir(filename:join(Dir, "x")),
    [bench(Dir, N) || N <- [10, 100, 1000, 5000]],
    _ = os:cmd("rm -rf " ++ Dir),
    ok.

segref(I) ->
    {ra_lib:to_binary(ra_lib:zpad_filename("", "segment", I)),
     {(I - 1) * 4096, I * 4096 - 1}}.

bench(Dir, NumSegs) ->
    io:format("-- ~b segments (~b entries)~n", [NumSegs, NumSegs * 4096]),
    SegRefs = [segref(I) || I <- lists:seq(NumSegs, 1, -1)],
    St = ra_log_segments:init(<<"uid">>, Dir, 5, random, SegRefs, undefined,
                              #{max_size => 64000000,
                                max_count => 4096,
                                major_strategy => {num_minors, 8}}, "bench"),
    New = [segref(NumSegs + 1)],
    t("update_segments/2 (once per WAL flush event)",
      fun () -> {_, _} = ra_log_segments:update_segments(New, St) end,
      iters(NumSegs)),
    t("segment_ref_count/1",
      fun () -> ra_log_segments:segment_ref_count(St) end, iters(NumSegs)),
    SnapIdx = (NumSegs div 2) * 4096,
    t("schedule_compaction(minor, live=[]) (per snapshot)",
      fun () -> {_, _} = ra_log_segments:schedule_compaction(minor, SnapIdx, [],
                                                             St)
      end, iters(NumSegs)),
    Live = ra_seq:from_list([I * 4096 + 7 || I <- lists:seq(0, NumSegs - 1)]),
    t("schedule_compaction(minor, live=1 per segment)",
      fun () -> {_, _} = ra_log_segments:schedule_compaction(minor, SnapIdx,
                                                             Live, St)
      end, iters(NumSegs)),
    t("read_plan/2 for 64 indexes in newest segment",
      fun () ->
              ra_log_segments:read_plan(
                St, lists:seq(NumSegs * 4096 - 1, NumSegs * 4096 - 64, -1))
      end, iters(NumSegs)),
    t("read_plan/2 for 64 indexes in oldest segment",
      fun () ->
              ra_log_segments:read_plan(St, lists:seq(64, 1, -1))
      end, iters(NumSegs)),
    ok.

iters(N) when N >= 5000 -> 200;
iters(N) when N >= 1000 -> 1000;
iters(_) -> 20000.
