%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Micro-benchmark for measuring the overhead of persisting the WAL
%% writers snapshot file at roll-over time.
%%
%% Usage from an Erlang shell (after compiling):
%%   ra_log_wal_writers_bench:run().
-module(ra_log_wal_writers_bench).

-export([run/0]).

run() ->
    Dir = filename:join(["/tmp", "ra_wal_writers_bench_" ++
                         integer_to_list(erlang:system_time(microsecond))]),
    ok = filelib:ensure_dir(filename:join(Dir, "dummy")),
    try
        io:format("~n=== WAL Writers Snapshot Micro-Benchmark ===~n~n"),
        io:format("~-15s ~-15s ~-15s ~-15s~n",
                  ["Writers", "Serialize(us)", "Write(us)", "Total(us)"]),
        io:format("~s~n", [lists:duplicate(60, $-)]),
        lists:foreach(
          fun (N) ->
                  bench_writers(Dir, N)
          end, [10, 100, 1000, 10000]),
        io:format("~n=== Baseline: roll-over file operations ===~n~n"),
        bench_rollover_baseline(Dir),
        io:format("~nDone.~n")
    after
        os:cmd("rm -rf " ++ Dir)
    end.

bench_writers(Dir, NumWriters) ->
    Writers = maps:from_list(
                [{list_to_binary(io_lib:format("uid_~6..0b", [I])),
                  {in_seq, rand:uniform(1000000)}}
                 || I <- lists:seq(1, NumWriters)]),
    Iters = 100,
    WritersFile = filename:join(Dir, "writers.snapshot"),
    TmpFile = WritersFile ++ ".tmp",
    {SerUs, WriteUs} =
        lists:foldl(
          fun (_I, {SAcc, WAcc}) ->
                  {SerTime, Bin} = timer:tc(fun () ->
                                                    term_to_binary(Writers)
                                            end),
                  {WriteTime, ok} = timer:tc(fun () ->
                                                     ok = ra_lib:write_file(TmpFile, Bin),
                                                     prim_file:rename(TmpFile, WritersFile)
                                             end),
                  {SAcc + SerTime, WAcc + WriteTime}
          end, {0, 0}, lists:seq(1, Iters)),
    AvgSer = SerUs div Iters,
    AvgWrite = WriteUs div Iters,
    io:format("~-15b ~-15b ~-15b ~-15b~n",
              [NumWriters, AvgSer, AvgWrite, AvgSer + AvgWrite]).

bench_rollover_baseline(Dir) ->
    Iters = 100,
    io:format("Measuring file open + close + sync_dir (~b iterations):~n", [Iters]),
    TotalUs =
        lists:foldl(
          fun (I, Acc) ->
                  File = filename:join(Dir, io_lib:format("baseline_~b.wal", [I])),
                  Tmp = File ++ ".tmp",
                  {ok, TmpFd} = file:open(Tmp, [write, binary, raw]),
                  ok = file:write(TmpFd, <<"RAWA", 1:8/unsigned>>),
                  ok = file:sync(TmpFd),
                  ok = file:close(TmpFd),
                  {Time, _} = timer:tc(fun () ->
                                               ok = prim_file:rename(Tmp, File),
                                               {ok, Fd} = file:open(File, [raw, write, read, binary]),
                                               {ok, 5} = file:position(Fd, 5),
                                               ok = file:close(Fd)
                                       end),
                  Acc + Time
          end, 0, lists:seq(1, Iters)),
    Avg = TotalUs div Iters,
    io:format("  Average roll-over file ops: ~b us~n", [Avg]).
