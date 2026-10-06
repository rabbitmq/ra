%% Drives the real ra_log_snap_store directly (no Ra servers) with an open loop
%% load: Clients members each put a snapshot of Size bytes every
%% Clients * 1000 / Rate milliseconds, for Duration seconds. A WAL-like writer
%% (4KB append + fdatasync) runs on the same file system at the same time and
%% its fsync latency is reported against a run with no snapshots at all.
%%
%%   erlc -o /tmp snap_store_bench.erl
%%   erl -noshell -pa /tmp -pa <ra>/_build/default/lib/*/ebin -eval '
%%     snap_store_bench:run("/mnt/ext4/store",
%%                          #{rates => [1000, 5000, 10000, 20000],
%%                            clients => 10000, size => 1024, duration => 30,
%%                            device => "nvme0n1"}),
%%     halt().'
%%
%% Options (defaults): rates [10000] puts/s offered in total, clients 10000,
%% size 1024 (bytes of snapshot), duration 30 (s), wal true, min_file_bytes
%% 64MB, device undefined (adds /proc/diskstats deltas).
-module(snap_store_bench).

-export([run/1, run/2]).

-define(NAME, snap_store_bench_store).

run(Dir) ->
    run(Dir, #{}).

run(Dir0, Opts) ->
    Dir = filename:absname(Dir0),
    Rates = maps:get(rates, Opts, [10000]),
    %% the baseline: the WAL writer alone
    Base = bench(Dir, none, Opts),
    Rows = [Base | [bench(Dir, Rate, Opts) || Rate <- Rates]],
    io:format("~n~8s ~9s ~9s ~8s ~8s ~9s ~8s ~9s ~8s ~7s ~8s ~9s ~8s ~7s ~8s~n",
              ["offered", "done/s", "puts", "p50 ms", "p99 ms", "max ms",
               "put/bat", "fsync ms", "busy %", "B/put", "files", "disk wr",
               "flushes", "MB wr", "wal p99"]),
    [row(R) || R <- Rows],
    io:format("~nThe first row has no snapshots: the WAL writer alone.~n"
              "busy % is the part of the run the store spent writing and "
              "syncing, near 100 means it is saturated.~n"),
    ok.

bench(Dir, Rate, Opts) ->
    Clients = maps:get(clients, Opts, 10000),
    Size = maps:get(size, Opts, 1024),
    Duration = maps:get(duration, Opts, 30),
    Base = filename:join(Dir, "bench"),
    _ = file:del_dir_r(Base),
    ok = filelib:ensure_dir(filename:join(Base, "x")),
    io:format("~nrate ~p: ~b clients, ~b bytes, ~b s~n",
              [Rate, Clients, Size, Duration]),
    _ = os:cmd("sync"),
    timer:sleep(1000),
    Wal = case maps:get(wal, Opts, true) of
              true -> start_wal(Base);
              false -> undefined
          end,
    {ok, _} = case Rate of
                  none ->
                      {ok, none};
                  _ ->
                      ra_log_snap_store:start_link(
                        #{name => ?NAME,
                          dir => filename:join(Base, "store"),
                          min_file_bytes => maps:get(min_file_bytes, Opts,
                                                     64 * 1024 * 1024)})
              end,
    Image = crypto:strong_rand_bytes(Size),
    Dev = maps:get(device, Opts, undefined),
    D0 = disk_stats(Dev),
    Parent = self(),
    T0 = erlang:monotonic_time(microsecond),
    Stop = T0 + Duration * 1000000,
    Lats = case Rate of
               none ->
                   timer:sleep(Duration * 1000),
                   [];
               _ ->
                   PeriodUs = round(Clients * 1000000 / Rate),
                   Pids = [spawn_link(
                             fun () ->
                                     %% spread the starts over a period
                                     timer:sleep(rand:uniform(max(1, PeriodUs div 1000))),
                                     UId = <<"member", (integer_to_binary(C))/binary>>,
                                     Parent ! {done, self(),
                                               client(UId, Image, 1, PeriodUs,
                                                      Stop, [])}
                             end) || C <- lists:seq(1, Clients)],
                   lists:append([receive {done, P, L} -> L end || P <- Pids])
           end,
    T1 = erlang:monotonic_time(microsecond),
    Info = case Rate of
               none -> #{};
               _ -> ra_log_snap_store:info(?NAME)
           end,
    D1 = disk_stats(Dev),
    WalLats = stop_wal(Wal),
    case Rate of
        none -> ok;
        _ -> ok = ra_log_snap_store:stop(?NAME)
    end,
    _ = file:del_dir_r(Base),
    Wall = (T1 - T0) / 1.0e6,
    Sorted = lists:sort(Lats),
    InWindow = [L || {St, L} <- WalLats, St >= T0, St =< T1],
    #{rate => Rate, wall => Wall, puts => length(Lats),
      lats => Sorted, info => Info, disk => diff(D0, D1),
      wal_p99 => pct(lists:sort(InWindow), 0.99) / 1000}.

%% one member: a snapshot every period, or back to back if the store is slower
client(UId, Image, Idx, PeriodUs, Stop, Acc) ->
    Start = erlang:monotonic_time(microsecond),
    case Start >= Stop of
        true ->
            Acc;
        false ->
            ok = ra_log_snap_store:put(?NAME, UId, <<"e">>, {Idx, 1}, Image, []),
            End = erlang:monotonic_time(microsecond),
            Left = PeriodUs - (End - Start),
            Left >= 1000 andalso timer:sleep(Left div 1000),
            client(UId, Image, Idx + 1, PeriodUs, Stop, [End - Start | Acc])
    end.

row(#{rate := Rate, wall := Wall, puts := Puts, lats := Lats, info := Info,
      disk := Disk, wal_p99 := WalP99}) ->
    Batches = maps:get(batches, Info, 0),
    Fsync = maps:get(fsync_time_us, Info, 0),
    Bytes = maps:get(bytes_written, Info, 0),
    io:format("~8s ~9.1f ~9b ~8.1f ~8.1f ~9.1f ~8.1f ~9.2f ~8.1f ~7.0f ~8b ~9b ~8b ~7.1f ~8.1f~n",
              [case Rate of none -> "-"; _ -> integer_to_list(Rate) end,
               Puts / Wall, Puts,
               pct(Lats, 0.5) / 1000, pct(Lats, 0.99) / 1000,
               case Lats of [] -> 0.0; _ -> lists:last(Lats) / 1000 end,
               case Batches of 0 -> 0.0; _ -> maps:get(puts, Info) / Batches end,
               case Batches of 0 -> 0.0; _ -> Fsync / Batches / 1000 end,
               100 * Fsync / 1.0e6 / Wall,
               case Puts of 0 -> 0.0; _ -> Bytes / (Puts * 1.0) end,
               maps:get(files, Info, 0),
               maps:get(writes, Disk, 0), maps:get(flushes, Disk, 0),
               maps:get(sectors, Disk, 0) * 512 / 1048576, WalP99]).

pct([], _) -> 0;
pct(Sorted, P) ->
    lists:nth(max(1, min(length(Sorted), round(length(Sorted) * P))), Sorted).

%%% WAL-like writer: 4KB append + fdatasync
start_wal(Base) ->
    Block = crypto:strong_rand_bytes(4096),
    File = filename:join(Base, "wal.dat"),
    spawn_link(fun () ->
                       {ok, Fd} = file:open(File, [write, raw, binary]),
                       wal_loop(Fd, Block, [])
               end).

wal_loop(Fd, Block, Lats) ->
    receive
        {stop, From, Ref} ->
            ok = file:close(Fd),
            From ! {Ref, Lats}
    after 1 ->
              T0 = erlang:monotonic_time(microsecond),
              ok = file:write(Fd, Block),
              ok = file:datasync(Fd),
              T1 = erlang:monotonic_time(microsecond),
              wal_loop(Fd, Block, [{T0, T1 - T0} | Lats])
    end.

stop_wal(undefined) ->
    [];
stop_wal(Pid) ->
    Ref = make_ref(),
    Pid ! {stop, self(), Ref},
    receive {Ref, Lats} -> Lats end.

%%% /proc/diskstats
disk_stats(undefined) ->
    undefined;
disk_stats(Dev) ->
    {ok, Bin} = file:read_file("/proc/diskstats"),
    Want = list_to_binary(Dev),
    case [T || L <- binary:split(Bin, <<"\n">>, [global]),
               [_, _, D | T] <- [binary:split(L, [<<" ">>],
                                              [global, trim_all])],
               D =:= Want] of
        [Nums | _] ->
            Ints = [binary_to_integer(X) || X <- Nums],
            #{writes => lists:nth(5, Ints), sectors => lists:nth(7, Ints),
              flushes => case length(Ints) >= 16 of
                             true -> lists:nth(16, Ints);
                             false -> 0
                         end};
        [] ->
            undefined
    end.

diff(#{} = A, #{} = B) -> maps:map(fun (K, V) -> V - maps:get(K, A) end, B);
diff(_, _) -> #{}.
