%% The raw cost of the file system operations snapshots are made of on the
%% machine it runs on, so that the results of the other snapshot benchmarks can
%% be read against it (consumer NVMe, enterprise NVMe with power loss
%% protection, a network volume and so on differ by orders of magnitude here).
%%
%%   erlc -o /tmp snap_env_probe.erl
%%   erl -noshell -pa /tmp -eval 'snap_env_probe:run("/mnt/ext4/probe"), halt().'
%%
%% Reports, as microseconds:
%%   append+fdatasync  what the log (and the WAL) pays per batch
%%   dir snapshot      what one snapshot costs as a directory: mkdir, create two
%%                     files, fsync one, fsync the directory and its parent, then
%%                     delete the previous one. Alone and with 8 and 32 at once.
-module(snap_env_probe).

-export([run/1, run/2]).

run(Dir) ->
    run(Dir, #{}).

run(Dir0, Opts) ->
    Dir = filename:absname(Dir0),
    Secs = maps:get(seconds, Opts, 5),
    _ = file:del_dir_r(Dir),
    ok = filelib:ensure_dir(filename:join(Dir, "x")),
    io:format("~nprobing ~ts for ~b s each~n", [Dir, Secs]),
    io:format("~n~-34s ~9s ~9s ~9s ~9s ~10s~n",
              ["", "p50 us", "p99 us", "max us", "ops", "ops/s"]),
    append_sync(Dir, Secs),
    [dir_snapshots(Dir, Secs, Par) || Par <- [1, 8, 32]],
    _ = file:del_dir_r(Dir),
    ok.

append_sync(Dir, Secs) ->
    {ok, Fd} = file:open(filename:join(Dir, "append.dat"),
                         [write, raw, binary]),
    Block = crypto:strong_rand_bytes(4096),
    Lats = loop(erlang:monotonic_time(millisecond) + Secs * 1000,
                fun () ->
                        ok = file:write(Fd, Block),
                        ok = file:datasync(Fd)
                end, []),
    ok = file:close(Fd),
    report("append 4KB + fdatasync", Lats, Secs).

dir_snapshots(Dir, Secs, Par) ->
    Parent = self(),
    Deadline = erlang:monotonic_time(millisecond) + Secs * 1000,
    Pids = [spawn_link(
              fun () ->
                      Base = filename:join(Dir, "w" ++ integer_to_list(W)),
                      ok = filelib:ensure_dir(filename:join([Base, "snapshots", "x"])),
                      Snaps = filename:join(Base, "snapshots"),
                      Lats = dir_loop(Snaps, Deadline, 0, undefined, []),
                      Parent ! {done, self(), Lats}
              end) || W <- lists:seq(1, Par)],
    Lats = lists:append([receive {done, P, L} -> L end || P <- Pids]),
    report(io_lib:format("dir snapshot, ~b at once", [Par]), Lats, Secs).

dir_loop(Snaps, Deadline, N, Prev, Acc) ->
    case erlang:monotonic_time(millisecond) >= Deadline of
        true ->
            Acc;
        false ->
            T0 = erlang:monotonic_time(microsecond),
            D = filename:join(Snaps, integer_to_list(N)),
            ok = file:make_dir(D),
            F = filename:join(D, "snapshot.dat"),
            {ok, Fd} = file:open(F, [write, raw, binary]),
            ok = file:write(Fd, binary:copy(<<"x">>, 1024)),
            ok = file:write_file(filename:join(D, "indexes"), <<"idx">>),
            ok = file:sync(Fd),
            ok = file:close(Fd),
            sync_dir(D),
            sync_dir(Snaps),
            Prev == undefined orelse delete(Prev),
            T1 = erlang:monotonic_time(microsecond),
            dir_loop(Snaps, Deadline, N + 1, D, [T1 - T0 | Acc])
    end.

delete(D) ->
    _ = file:delete(filename:join(D, "snapshot.dat")),
    _ = file:delete(filename:join(D, "indexes")),
    _ = file:del_dir(D),
    ok.

sync_dir(D) ->
    {ok, Fd} = file:open(D, [read, directory, raw]),
    ok = file:datasync(Fd),
    ok = file:close(Fd).

loop(Deadline, Fun, Acc) ->
    case erlang:monotonic_time(millisecond) >= Deadline of
        true ->
            Acc;
        false ->
            T0 = erlang:monotonic_time(microsecond),
            Fun(),
            T1 = erlang:monotonic_time(microsecond),
            loop(Deadline, Fun, [T1 - T0 | Acc])
    end.

report(Name, Lats, Secs) ->
    Sorted = lists:sort(Lats),
    N = length(Sorted),
    P = fun (Q) -> case N of
                       0 -> 0;
                       _ -> lists:nth(max(1, min(N, round(N * Q))), Sorted)
                   end
        end,
    io:format("~-34s ~9b ~9b ~9b ~9b ~10.1f~n",
              [lists:flatten(Name), P(0.5), P(0.99), P(1.0), N, N / Secs]).
