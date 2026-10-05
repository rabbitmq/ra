%% Phase 0 benchmark for the "many clusters snapshotting in parallel" problem.
%% Self-contained: no Ra modules are used, only plain file operations that
%% mimic what Ra does. Run it on the filesystem under test (ext4, data=ordered).
%%
%% Scenarios (see run/2 options):
%%   a   dir layout, serial syncs in a small worker pool  (what Ra does today)
%%         per snapshot: mkdir, snapshot.dat (4KB padded), indexes file,
%%         fsync file, sync snapshot dir, sync parent dir, then delete the
%%         previous snapshot dir
%%   b   as a, but every sync fun in a worker batch runs concurrently
%%   b2  as a, but phased: all file fsyncs of a batch concurrently, then all
%%         directory syncs of the batch concurrently
%%   c   flat layout: one file per snapshot directly in snapshots/, indexes
%%         embedded, fsync file + sync dir, serial worker pool
%%   c2  as c, phased concurrent syncs
%%   none no snapshot I/O (baseline: background WAL writer only)
%%   d   shared snapshot log: single writer, batched append + one fsync per
%%         batch, ETS pointer table, roll at max(MinBytes, 2*live) with a
%%         background copy-forward compactor
%%
%% Every "cluster" is an Erlang process doing `rounds' snapshots back to back
%% (waiting for the sync to complete each time, then removing the previous
%% snapshot, as Ra does). All clusters start together.
-module(snap_fs_bench).

-export([run/1, run/2]).

-define(ALIGN, 4096).
-define(DEF_CAP, 256).
-define(MAX_BATCH_BYTES, 8 * 1024 * 1024).
-define(PUT, 1).
-define(PAD, 0).
-define(HDR, 9).

-record(st, {dir, no, fd, off = 0, tid, live = 0, min, align, counter, comp,
             appended = 0, submitted = 0, rolls = 0, batches = 0, puts = 0, io_us = 0, max_batch = 0}).

%% @doc run(Dir) with defaults. Dir must be on the filesystem under test.
run(Dir) ->
    run(Dir, #{}).

%% Opts:
%%   clusters  [pos_integer()]  default [100, 1000]
%%   sizes     [bytes]          default [1024, 8192, 65536] (machine state size)
%%   rounds    pos_integer()    default 3 (snapshots per cluster)
%%   scenarios [a|b|b2|c|c2|d]  default all
%%   pool      pos_integer()    sync worker pool size, default schedulers div 4
%%   cap       pos_integer()    max concurrent syncs per worker batch for the
%%                              concurrent scenarios (b, b2, c2), default 256
%%   device    string()         e.g. "nvme0n1": adds /proc/diskstats and jbd2
%%                              deltas to the output (Linux only)
%%   wal       boolean()        default true: background process doing a
%%                              4KB append + fdatasync loop, reports its
%%                              fsync latency (shares the jbd2 journal)
%%   duration  seconds          run each scenario for a fixed time instead of
%%                              `rounds' snapshots per cluster (recommended
%%                              for WAL-latency comparisons so every scenario
%%                              is sampled over a comparable window)
%%   interval_ms                with `duration': each cluster takes a snapshot
%%                              every interval_ms (open loop: offered load is
%%                              clusters*1000/interval_ms per second; a
%%                              saturated scenario falls behind and runs
%%                              back-to-back). 0 (default) = closed loop.
%%   skew      none | zipf      with interval_ms: zipf makes a few clusters
%%                              snapshot very often and many rarely (same
%%                              total offered rate), to exercise cold members
%%   store_min_bytes            default 64MB, minimum size before d rolls
%%   csv       filename()       also write results as csv
run(Dir0, Opts) ->
    Dir = filename:absname(Dir0),
    ok = filelib:ensure_dir(filename:join(Dir, "x")),
    Clusters = maps:get(clusters, Opts, [100, 1000]),
    Sizes = maps:get(sizes, Opts, [1024, 8192, 65536]),
    Scenarios = maps:get(scenarios, Opts, [a, b, b2, c, c2, d]),
    Rows = [bench(Dir, S, N, Sz, Opts)
            || N <- Clusters, Sz <- Sizes, S <- Scenarios],
    print_rows(Rows),
    maybe_csv(Opts, Rows),
    ok.

%%% ------------------------------------------------------------------
%%% one benchmark run
%%% ------------------------------------------------------------------

bench(Dir, Scen, N, Size, Opts) ->
    io:format("~n== scenario ~s: ~b clusters, ~b byte state ==~n",
              [Scen, N, Size]),
    Base = filename:join(Dir, "bench"),
    _ = file:del_dir_r(Base),
    ok = filelib:ensure_dir(filename:join(Base, "x")),
    Rounds = maps:get(rounds, Opts, 3),
    DurUs = case maps:get(duration, Opts, undefined) of
                undefined -> undefined;
                Secs -> round(Secs * 1.0e6)
            end,
    IntervalUs = maps:get(interval_ms, Opts, 0) * 1000,
    Skew = maps:get(skew, Opts, none),
    Uids = [iolist_to_binary(io_lib:format("uid~7..0b", [I]))
            || I <- lists:seq(1, N)],
    ok = setup(Scen, Base, Uids),
    _ = os:cmd("sync"),
    timer:sleep(1000),
    Dev = maps:get(device, Opts, undefined),
    Wal = start_wal(Base, Opts),
    Infra = start_infra(Scen, Base, Opts),
    State = crypto:strong_rand_bytes(Size),
    Parent = self(),
    Harm = lists:sum([1 / K || K <- lists:seq(1, N)]),
    Pids = [spawn_link(
              fun () ->
                      Deadline = receive {go, DL} -> DL end,
                      Period = period(Skew, IntervalUs, R, N, Harm),
                      Plan = #{stop => case DurUs of
                                           undefined -> {rounds, Rounds};
                                           _ -> {until, Deadline}
                                       end,
                               period => Period,
                               first_delay => first_delay(Period, DurUs)},
                      CT0 = erlang:monotonic_time(microsecond),
                      L = cluster(Scen, Infra, Base, Uid, State, Plan),
                      CT1 = erlang:monotonic_time(microsecond),
                      Parent ! {done, L, CT0, CT1}
              end) || {R, Uid} <- lists:zip(lists:seq(1, N), Uids)],
    S0 = disk_stats(Dev),
    J0 = jbd2(Dev),
    T0 = erlang:monotonic_time(microsecond),
    Deadline0 = case DurUs of
                    undefined -> 0;
                    _ -> T0 + DurUs
                end,
    [P ! {go, Deadline0} || P <- Pids],
    %% collect in arrival order (a selective receive per pid is O(n^2) with
    %% a 10k deep mailbox)
    Done = [receive {done, L, CT0, CT1} -> {L, CT0, CT1} end || _ <- Pids],
    T1 = erlang:monotonic_time(microsecond),
    Lats = lists:append([L || {L, _, _} <- Done]),
    FirstStart = lists:min([A || {_, A, _} <- Done]),
    LastEnd = lists:max([B || {_, _, B} <- Done]),
    %% wall time as seen by the clusters themselves; compare with Wall
    ClusterWall = (LastEnd - FirstStart) / 1.0e6,
    StartLag = (FirstStart - T0) / 1.0e6,
    %% for d this also waits for outstanding compaction
    StoreStats = stop_infra(Infra),
    S1 = disk_stats(Dev),
    J1 = jbd2(Dev),
    %% only WAL samples taken while the snapshot load was running
    WalLats = [L || {St, L} <- stop_wal(Wal), St >= T0, St =< T1],
    Space = dir_size(Base),
    _ = file:del_dir_r(Base),
    Wall = (T1 - T0) / 1.0e6,
    Total = length(Lats),
    Sorted = lists:sort(Lats),
    #{scen => Scen, n => N, size => Size,
      snaps_s => Total / max(ClusterWall, 1.0e-6),
      snaps => Total,
      wal_samples => length(WalLats),
      wall_s => Wall,
      cluster_wall_s => ClusterWall,
      start_lag_s => StartLag,
      p50 => pct(Sorted, 0.50) / 1000,
      p99 => pct(Sorted, 0.99) / 1000,
      disk => diff_stats(S0, S1),
      jbd2 => diff_jbd2(J0, J1),
      wal_p50 => pct(lists:sort(WalLats), 0.50) / 1000,
      wal_p99 => pct(lists:sort(WalLats), 0.99) / 1000,
      space => Space,
      store => StoreStats,
      cap => case sync_mode_or_none(Scen) of
                 none -> 0;
                 serial -> 0;
                 _ -> maps:get(cap, Opts, ?DEF_CAP)
             end,
      pool => case Scen of d -> 0; _ -> pool_size(Opts) end,
      wal_on => maps:get(wal, Opts, true),
      submitted => Total * Size}.

sync_mode_or_none(d) -> none;
sync_mode_or_none(none) -> none;
sync_mode_or_none(S) -> sync_mode(S).

setup(none, _Base, _Uids) ->
    ok;
setup(d, Base, _Uids) ->
    ok = file:make_dir(filename:join(Base, "store"));
setup(_, Base, Uids) ->
    [ok = filelib:ensure_dir(filename:join([Base, binary_to_list(U),
                                            "snapshots", "x"]))
     || U <- Uids],
    ok.

layout(a) -> dir;
layout(b) -> dir;
layout(b2) -> dir;
layout(c) -> flat;
layout(c2) -> flat;
layout(d) -> store;
layout(none) -> none.

sync_mode(a) -> serial;
sync_mode(b) -> conc;
sync_mode(b2) -> phased;
sync_mode(c) -> serial;
sync_mode(c2) -> phased.

%%% ------------------------------------------------------------------
%%% cluster workload
%%% ------------------------------------------------------------------

%% microseconds between snapshots for cluster of rank R (1 = hottest)
period(_, 0, _, _, _) -> 0;
period(none, IntervalUs, _, _, _) -> IntervalUs;
period(zipf, IntervalUs, R, N, Harm) ->
    %% rate_r ~ 1/r with the same total rate as N clusters at IntervalUs
    max(1, round(IntervalUs * R * Harm / N)).

first_delay(0, _) -> 0;
first_delay(Period, undefined) -> rand:uniform(Period);
first_delay(Period, DurUs) -> rand:uniform(max(1, min(Period, DurUs))).

cluster(Scen, Infra, Base, Uid, State, Plan) ->
    SnapDir = filename:join([Base, binary_to_list(Uid), "snapshots"]),
    sleep_us(maps:get(first_delay, Plan)),
    cluster_loop(1, Plan, layout(Scen), Infra, SnapDir, Uid, State,
                 fun () -> ok end, []).

sleep_us(Us) when Us >= 1000 -> timer:sleep(Us div 1000);
sleep_us(_) -> ok.

done(I, #{stop := {rounds, R}}) ->
    I > R;
done(_, #{stop := {until, Deadline}}) ->
    erlang:monotonic_time(microsecond) >= Deadline.

cluster_loop(I, Plan, Layout, Infra, SnapDir, Uid, State, PrevCleanup, Lats) ->
    case done(I, Plan) of
        true ->
            PrevCleanup(),
            Lats;
        false ->
            T0 = erlang:monotonic_time(microsecond),
            Cleanup = snapshot(Layout, Infra, SnapDir, Uid, I, State),
            T1 = erlang:monotonic_time(microsecond),
            PrevCleanup(),
            %% open loop: wait out the rest of the period
            sleep_us(maps:get(period, Plan) -
                     (erlang:monotonic_time(microsecond) - T0)),
            cluster_loop(I + 1, Plan, Layout, Infra, SnapDir, Uid, State,
                         Cleanup, [T1 - T0 | Lats])
    end.

%% returns a fun that removes this snapshot (called after the next one is
%% durable)
snapshot(dir, Infra, SnapDir, _Uid, Idx, State) ->
    Dir = filename:join(SnapDir, name(Idx)),
    ok = file:make_dir(Dir),
    SnapFile = filename:join(Dir, "snapshot.dat"),
    IdxFile = filename:join(Dir, "indexes"),
    ok = write_file(SnapFile, image(Idx, State, <<>>, true)),
    ok = write_file(IdxFile, indexes_bin()),
    ok = pool_sync(Infra, {fun () -> sync_file(SnapFile) end,
                           fun () ->
                                   sync_dir(Dir),
                                   sync_dir(SnapDir)
                           end}),
    fun () ->
            _ = file:delete(SnapFile),
            _ = file:delete(IdxFile),
            _ = file:del_dir(Dir),
            ok
    end;
snapshot(flat, Infra, SnapDir, _Uid, Idx, State) ->
    File = filename:join(SnapDir, name(Idx) ++ ".snap"),
    ok = write_file(File, image(Idx, State, indexes_bin(), true)),
    ok = pool_sync(Infra, {fun () -> sync_file(File) end,
                           fun () -> sync_dir(SnapDir) end}),
    fun () -> _ = file:delete(File), ok end;
%% `none': no snapshot I/O at all. Run it with the same duration/wal/device
%% options to get the baseline (WAL writer only) for the disk counters.
snapshot(none, _Infra, _SnapDir, _Uid, _Idx, _State) ->
    timer:sleep(100),
    fun () -> ok end;
snapshot(store, Infra, _SnapDir, Uid, Idx, State) ->
    ok = store_put(Infra, Uid, Idx, image(Idx, State, indexes_bin(), false)),
    fun () -> ok end.

name(Idx) ->
    lists:flatten(io_lib:format("~16.16.0b_~16.16.0b", [1, Idx])).

indexes_bin() ->
    Bin = term_to_binary({seq, [{1, 1000}]}),
    <<"RASI", 1:8, (erlang:crc32(Bin)):32, Bin/binary>>.

%% same shape as ra_log_snapshot's file image
image(Idx, State, Ind, Pad) ->
    Meta = term_to_binary(#{index => Idx, term => 1, cluster => #{},
                            machine_version => 1}),
    Data = [<<(byte_size(Meta)):32>>, Meta, State, Ind],
    Crc = erlang:crc32(Data),
    Bytes0 = 9 + iolist_size(Data),
    PadBytes = case Pad of
                   true -> (?ALIGN - (Bytes0 rem ?ALIGN)) rem ?ALIGN;
                   false -> 0
               end,
    [<<"RASN", 1:8, Crc:32>>, Data, <<0:(PadBytes * 8)>>].

%% fd limits (ulimit -n) are easily hit with thousands of clusters writing at
%% once; back off briefly instead of failing
open_retry(File, Modes) ->
    case file:open(File, Modes) of
        {error, emfile} ->
            timer:sleep(2),
            open_retry(File, Modes);
        Res ->
            Res
    end.

write_file(File, IO) ->
    {ok, Fd} = open_retry(File, [write, raw, binary]),
    ok = file:write(Fd, IO),
    ok = file:close(Fd).

sync_file(File) ->
    {ok, Fd} = open_retry(File, [read, write, raw, binary]),
    ok = file:sync(Fd),
    ok = file:close(Fd).

sync_dir(Dir) ->
    {ok, Fd} = open_retry(Dir, [read, directory, raw]),
    ok = file:datasync(Fd),
    ok = file:close(Fd).

%%% ------------------------------------------------------------------
%%% infra: sync worker pool (a, b, b2, c, c2) or snapshot store (d)
%%% ------------------------------------------------------------------

start_infra(none, _Base, _Opts) ->
    none;
start_infra(d, Base, Opts) ->
    Min = maps:get(store_min_bytes, Opts, 64 * 1024 * 1024),
    Dir = filename:join(Base, "store"),
    {store, spawn_link(fun () -> store_init(Dir, Min) end)};
start_infra(Scen, _Base, Opts) ->
    PoolSize = pool_size(Opts),
    Mode = sync_mode(Scen),
    Cap = maps:get(cap, Opts, ?DEF_CAP),
    {pool, list_to_tuple([spawn_link(fun () -> worker(Mode, Cap) end)
                          || _ <- lists:seq(1, PoolSize)])}.

pool_size(Opts) ->
    maps:get(pool, Opts, max(1, erlang:system_info(schedulers) div 4)).

stop_infra(none) ->
    undefined;
stop_infra({pool, Workers}) ->
    [begin unlink(W), exit(W, kill) end || W <- tuple_to_list(Workers)],
    undefined;
stop_infra({store, Pid}) ->
    Ref = make_ref(),
    Pid ! {stop, self(), Ref},
    receive {Ref, Stats} -> Stats end.

pool_sync({pool, Workers}, Item) ->
    W = element(rand:uniform(tuple_size(Workers)), Workers),
    Ref = make_ref(),
    W ! {sync, self(), Ref, Item},
    receive {Ref, ok} -> ok end.

worker(Mode, Cap) ->
    receive
        {sync, _, _, _} = M ->
            %% most recent first, like ra_log_sync (reversed_batch)
            run_batch(Mode, Cap, drain([M])),
            worker(Mode, Cap)
    end.

drain(Acc) ->
    receive
        {sync, _, _, _} = M -> drain([M | Acc])
    after 0 ->
              Acc
    end.

run_batch(serial, _Cap, Batch) ->
    [begin
         P1(),
         P2(),
         From ! {Ref, ok}
     end || {sync, From, Ref, {P1, P2}} <- Batch],
    ok;
run_batch(conc, Cap, Batch) ->
    [begin
         par([fun () -> P1(), P2() end || {sync, _, _, {P1, P2}} <- Chunk]),
         reply(Chunk)
     end || Chunk <- chunks(Batch, Cap)],
    ok;
run_batch(phased, Cap, Batch) ->
    [begin
         par([P1 || {sync, _, _, {P1, _}} <- Chunk]),
         par([P2 || {sync, _, _, {_, P2}} <- Chunk]),
         reply(Chunk)
     end || Chunk <- chunks(Batch, Cap)],
    ok.

reply(Chunk) ->
    [From ! {Ref, ok} || {sync, From, Ref, _} <- Chunk],
    ok.

par(Funs) ->
    Self = self(),
    Refs = [begin
                R = make_ref(),
                spawn_link(fun () -> F(), Self ! {R, ok} end),
                R
            end || F <- Funs],
    [receive {R, ok} -> ok end || R <- Refs],
    ok.

chunks([], _) -> [];
chunks(L, N) when length(L) =< N -> [L];
chunks(L, N) ->
    {A, B} = lists:split(N, L),
    [A | chunks(B, N)].

%%% ------------------------------------------------------------------
%%% scenario d: shared snapshot log
%%% ------------------------------------------------------------------

store_put({store, Pid}, Uid, Idx, Img) ->
    Ref = make_ref(),
    Bin = iolist_to_binary(Img),
    Pid ! {put, self(), Ref, Uid, Idx, Bin},
    receive {Ref, ok} -> ok end.

store_init(Dir, Min) ->
    Tid = ets:new(snap_store, [set, public, {write_concurrency, true}]),
    Counter = atomics:new(1, []),
    Comp = spawn_link(fun () -> compactor(Dir, Tid, Counter, 0, #{}) end),
    No = atomics:add_get(Counter, 1, 1),
    Fd = open_new(Dir, No),
    store_loop(#st{dir = Dir, no = No, fd = Fd, tid = Tid, min = Min,
                   align = true, counter = Counter, comp = Comp}).

store_loop(#st{} = S) ->
    receive
        {stop, From, Ref} ->
            CRef = make_ref(),
            S#st.comp ! {stop, self(), CRef},
            Compacted = receive {CRef, W} -> W end,
            ok = file:close(S#st.fd),
            From ! {Ref, #{appended => S#st.appended,
                           compacted => Compacted,
                           submitted => S#st.submitted,
                           rolls => S#st.rolls,
                           batches => S#st.batches,
                           puts => S#st.puts,
                           io_s => S#st.io_us / 1.0e6,
                           max_batch => S#st.max_batch,
                           live => S#st.live}};
        {put, _, _, _, _, Img} = M ->
            {Puts, _} = collect([M], byte_size(Img)),
            store_loop(store_batch(lists:reverse(Puts), S))
    end.

collect(Acc, Bytes) when Bytes >= ?MAX_BATCH_BYTES ->
    {Acc, Bytes};
collect(Acc, Bytes) ->
    receive
        {put, _, _, _, _, Img} = M -> collect([M | Acc], Bytes + byte_size(Img))
    after 0 ->
              {Acc, Bytes}
    end.

store_batch(Puts, #st{no = No, off = Off0, fd = Fd, tid = Tid, align = Align,
                      live = Live0} = S) ->
    {IO, Entries, Off1, Submitted} = encode_batch(Puts, No, Off0, Align),
    IoT0 = erlang:monotonic_time(microsecond),
    ok = file:write(Fd, IO),
    ok = file:sync(Fd),
    IoT1 = erlang:monotonic_time(microsecond),
    %% pointers are published only once the batch is durable
    Live1 = lists:foldl(
              fun ({From, Ref, {Uid, _, _, TL, _} = E}, L) ->
                      L1 = case ets:lookup(Tid, Uid) of
                               [{_, _, _, OldTL, _}] -> L - OldTL;
                               [] -> L
                           end,
                      true = ets:insert(Tid, E),
                      From ! {Ref, ok},
                      L1 + TL
              end, Live0, Entries),
    S1 = S#st{off = Off1, live = Live1,
              appended = S#st.appended + (Off1 - Off0),
              submitted = S#st.submitted + Submitted,
              batches = S#st.batches + 1,
              puts = S#st.puts + length(Puts),
              io_us = S#st.io_us + (IoT1 - IoT0),
              max_batch = max(S#st.max_batch, length(Puts))},
    maybe_roll(S1).

encode_batch(Puts, No, Off0, Align) ->
    {RevIO, RevEntries, Off1, Sub} =
        lists:foldl(
          fun ({put, From, Ref, Uid, Idx, Img}, {IOAcc, EAcc, Off, SubAcc}) ->
                  Body = [<<(byte_size(Uid)):16>>, Uid, <<Idx:64>>, Img],
                  Len = iolist_size(Body),
                  Rec = [<<?PUT:8, Len:32, (erlang:crc32(Body)):32>>, Body],
                  TL = ?HDR + Len,
                  E = {Uid, No, Off, TL, Idx},
                  {[Rec | IOAcc], [{From, Ref, E} | EAcc], Off + TL,
                   SubAcc + byte_size(Img)}
          end, {[], [], Off0, 0}, Puts),
    {PadIO, PadSize} = pad(Off1, Align),
    {lists:reverse([PadIO | RevIO]), lists:reverse(RevEntries),
     Off1 + PadSize, Sub}.

%% a pad record keeps the file parseable: <<0:8, ZeroLen:32, 0:32, Zeros>>
pad(_End, false) ->
    {[], 0};
pad(End, true) ->
    case End rem ?ALIGN of
        0 ->
            {[], 0};
        Rem ->
            P0 = ?ALIGN - Rem,
            P = case P0 < ?HDR of true -> P0 + ?ALIGN; false -> P0 end,
            {<<?PAD:8, (P - ?HDR):32, 0:32, 0:((P - ?HDR) * 8)>>, P}
    end.

maybe_roll(#st{off = Off, live = Live, min = Min} = S)
  when Off >= max(Min, 2 * Live) ->
    ok = file:close(S#st.fd),
    S#st.comp ! {compact, S#st.no, Off},
    No = atomics:add_get(S#st.counter, 1, 1),
    Fd = open_new(S#st.dir, No),
    S#st{no = No, fd = Fd, off = 0, rolls = S#st.rolls + 1};
maybe_roll(S) ->
    S.

open_new(Dir, No) ->
    {ok, Fd} = open_retry(fname(Dir, No), [write, raw, binary]),
    sync_dir(Dir),
    Fd.

fname(Dir, No) ->
    filename:join(Dir, io_lib:format("~8..0b.snap", [No])).

%% Copy-forward compactor. Rolled files and previous compaction outputs are
%% all candidates: after each compaction, any file whose live bytes have
%% dropped to half its size or less is compacted too (otherwise garbage in
%% outputs leaks forever). NB: output files take a fresh file number (higher
%% than the active file); a real implementation needs sequence numbers for
%% recovery ordering, which does not matter for this benchmark.
compactor(Dir, Tid, Counter, Written, Files) ->
    receive
        {compact, No, Size} ->
            {W, Files1} = compact_all(Dir, Tid, Counter, [No],
                                      Files#{No => Size}),
            compactor(Dir, Tid, Counter, Written + W, Files1);
        {stop, From, Ref} ->
            From ! {Ref, Written}
    end.

compact_all(_Dir, _Tid, _Counter, [], Files) ->
    {0, Files};
compact_all(Dir, Tid, Counter, [No | Rest], Files0) ->
    {W, Out} = compact(Dir, Tid, Counter, No),
    Files1 = maps:remove(No, Files0),
    Files2 = case Out of
                 none -> Files1;
                 {NewNo, NewSize} -> Files1#{NewNo => NewSize}
             end,
    %% anything (incl. outputs) that is now mostly garbage
    LiveBy = ets:foldl(fun ({_, F, _, TL, _}, Acc) ->
                               maps:update_with(F, fun (V) -> V + TL end, TL, Acc)
                       end, #{}, Tid),
    More = [F || {F, Size} <- maps:to_list(Files2),
                 F =/= No,
                 %% at least half garbage AND at least one block to reclaim
                 %% (sizes exclude pad records, otherwise a small, fully live
                 %% output looks half dead and is recompacted forever)
                 maps:get(F, LiveBy, 0) * 2 =< Size,
                 Size - maps:get(F, LiveBy, 0) >= ?ALIGN,
                 not lists:member(F, Rest)],
    {W2, Files3} = compact_all(Dir, Tid, Counter, Rest ++ More, Files2),
    {W + W2, Files3}.

compact(Dir, Tid, Counter, No) ->
    Old = fname(Dir, No),
    {ok, Bin} = file:read_file(Old),
    case scan(Bin, 0, No, Tid, []) of
        [] ->
            ok = file:delete(Old),
            sync_dir(Dir),
            {0, none};
        Live ->
            NewNo = atomics:add_get(Counter, 1, 1),
            {RevIO, Moves, End} =
                lists:foldl(
                  fun ({Uid, Off, TL, Idx, Rec}, {IOAcc, MAcc, NewOff}) ->
                          {[Rec | IOAcc],
                           [{Uid, Off, TL, Idx, NewOff} | MAcc], NewOff + TL}
                  end, {[], [], 0}, Live),
            {PadIO, PadSize} = pad(End, true),
            {ok, Fd} = open_retry(fname(Dir, NewNo), [write, raw, binary]),
            ok = file:write(Fd, lists:reverse([PadIO | RevIO])),
            ok = file:sync(Fd),
            ok = file:close(Fd),
            sync_dir(Dir),
            %% repoint only records that are still live (CAS)
            [ets:select_replace(
               Tid, [{{Uid, No, Off, TL, Idx}, [],
                      [{{Uid, NewNo, NewOff, TL, Idx}}]}])
             || {Uid, Off, TL, Idx, NewOff} <- Moves],
            ok = file:delete(Old),
            sync_dir(Dir),
            {End + PadSize, {NewNo, End}}
    end.

scan(Bin, Off, _No, _Tid, Acc) when Off >= byte_size(Bin) ->
    lists:reverse(Acc);
scan(Bin, Off, No, Tid, Acc) ->
    <<_:Off/binary, Type:8, Len:32, _Crc:32, Rest/binary>> = Bin,
    TL = ?HDR + Len,
    case Type of
        ?PAD ->
            scan(Bin, Off + TL, No, Tid, Acc);
        ?PUT ->
            <<Body:Len/binary, _/binary>> = Rest,
            <<ULen:16, Uid:ULen/binary, Idx:64, _/binary>> = Body,
            case ets:lookup(Tid, Uid) of
                [{Uid, No, Off, TL, Idx}] ->
                    scan(Bin, Off + TL, No, Tid,
                         [{Uid, Off, TL, Idx, binary:part(Bin, Off, TL)} | Acc]);
                _ ->
                    scan(Bin, Off + TL, No, Tid, Acc)
            end
    end.

%%% ------------------------------------------------------------------
%%% background "WAL": 4KB append + fdatasync, measures fsync latency while
%%% snapshots are being written (shares the jbd2 journal)
%%% ------------------------------------------------------------------

start_wal(Base, Opts) ->
    case maps:get(wal, Opts, true) of
        true ->
            Block = crypto:strong_rand_bytes(?ALIGN),
            File = filename:join(Base, "wal.dat"),
            %% raw fds are only usable by the process that opened them
            spawn_link(fun () ->
                               {ok, Fd} = open_retry(File, [write, raw, binary]),
                               wal_loop(Fd, Block, [])
                       end);
        false ->
            undefined
    end.

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

%%% ------------------------------------------------------------------
%%% OS statistics (Linux)
%%% ------------------------------------------------------------------

disk_stats(undefined) ->
    undefined;
disk_stats(Dev) ->
    case file:read_file("/proc/diskstats") of
        {ok, Bin} ->
            Lines = binary:split(Bin, <<"\n">>, [global]),
            Want = list_to_binary(Dev),
            case [T || L <- Lines,
                       [_, _, D | T] <- [binary:split(L, [<<" ">>], [global, trim_all])],
                       D =:= Want] of
                [Nums | _] ->
                    Ints = [binary_to_integer(N) || N <- Nums],
                    #{writes => nth(5, Ints), merged => nth(6, Ints),
                      sectors => nth(7, Ints), flushes => nth(16, Ints)};
                [] ->
                    undefined
            end;
        _ ->
            undefined
    end.

nth(N, L) when length(L) >= N -> lists:nth(N, L);
nth(_, _) -> 0.

diff_stats(#{} = A, #{} = B) ->
    maps:map(fun (K, V) -> V - maps:get(K, A) end, B);
diff_stats(_, _) ->
    undefined.

jbd2(undefined) ->
    undefined;
jbd2(Dev) ->
    Files = filelib:wildcard("/proc/fs/jbd2/" ++ Dev ++ "*/info"),
    lists:sum([case file:read_file(F) of
                   {ok, B} ->
                       case re:run(B, "(\\d+) transactions",
                                   [{capture, [1], list}]) of
                           {match, [S]} -> list_to_integer(S);
                           _ -> 0
                       end;
                   _ ->
                       0
               end || F <- Files]).

diff_jbd2(A, B) when is_integer(A), is_integer(B) -> B - A;
diff_jbd2(_, _) -> undefined.

dir_size(Dir) ->
    filelib:fold_files(Dir, ".*", true,
                       fun (F, Acc) -> Acc + filelib:file_size(F) end, 0).

%%% ------------------------------------------------------------------
%%% reporting
%%% ------------------------------------------------------------------

pct([], _) -> 0;
pct(Sorted, P) ->
    lists:nth(max(1, min(length(Sorted), round(length(Sorted) * P))), Sorted).

print_rows(Rows) ->
    io:format("~n~-5s ~6s ~7s ~4s ~9s ~8s ~8s ~9s ~9s ~7s ~9s ~7s ~8s "
              "~8s ~6s ~8s ~7s ~7s ~7s ~6s ~8s ~7s~n",
              ["scen", "N", "size", "cap", "snaps/s", "p50 ms", "p99 ms",
               "disk wr", "merged", "merge%", "MB wr", "flush", "jbd2tx",
               "wal p99", "WA", "put/bat", "space", "wall s", "clus s",
               "io s", "snaps", "wal n"]),
    [print_row(R) || R <- Rows],
    io:format("~nNB: disk/flush/jbd2 columns include the background WAL "
              "writer's own I/O when wal=true; use wal=>false runs for "
              "clean snapshot I/O counts.~n"),
    ok.

derived(#{disk := Disk, jbd2 := J, store := Store, submitted := Sub,
          space := Space}) ->
    {W, M, MB, F} = case Disk of
                        #{writes := W0, merged := M0, sectors := S0,
                          flushes := F0} ->
                            {W0, M0, S0 * 512 / 1048576, F0};
                        _ ->
                            {0, 0, 0.0, 0}
                    end,
    MergePct = case W + M of 0 -> 0.0; T -> 100 * M / T end,
    {WA, PutsPerBatch} =
        case Store of
            #{appended := A, compacted := C, batches := B, puts := P}
              when Sub > 0, B > 0 ->
                {(A + C) / Sub, P / B};
            _ ->
                {0.0, 0.0}
        end,
    #{writes => W, merged => M, merge_pct => MergePct, mb => MB, flushes => F,
      jbd2 => J, wa => WA, put_per_batch => PutsPerBatch,
      space_mb => Space / 1048576}.

print_row(#{scen := S, n := N, size := Sz, snaps_s := Sps, p50 := P50,
            p99 := P99, wal_p99 := WP99, cap := Cap, wall_s := Wall,
            cluster_wall_s := CWall, store := Store} = R) ->
    #{writes := W, merged := M, merge_pct := MP, mb := MB, flushes := F,
      jbd2 := J, wa := WA, put_per_batch := PPB, space_mb := Space} =
        derived(R),
    io:format("~-5s ~6b ~7b ~4b ~9.1f ~8.1f ~8.1f ~9b ~9b ~7.1f ~9.1f ~7b "
              "~8s ~8.1f ~6.2f ~8.1f ~7.1f ~7.2f ~7.2f ~6.2f ~8b ~7b~n",
              [S, N, Sz, Cap, Sps, P50, P99, W, M, MP, MB, F,
               case J of undefined -> "-"; _ -> integer_to_list(J) end,
               WP99, WA, PPB, Space, Wall, CWall,
               case Store of #{io_s := IoS} -> IoS; _ -> 0.0 end,
               maps:get(snaps, R), maps:get(wal_samples, R)]).

maybe_csv(#{csv := File}, Rows) ->
    Lines = [begin
                 #{writes := W, merged := M, mb := MB, flushes := F, jbd2 := J,
                   wa := WA, put_per_batch := PPB, space_mb := Space} =
                     derived(R),
                 #{scen := S, n := N, size := Sz, snaps_s := Sps, p50 := P50,
                   p99 := P99, wal_p50 := WP50, wal_p99 := WP99, cap := Cap,
                   pool := Pool, wal_on := WalOn, wall_s := Wall,
                   cluster_wall_s := CWall, start_lag_s := Lag,
                   store := Store} = R,
                 io_lib:format("~s,~b,~b,~b,~b,~p,~.1f,~.2f,~.2f,~b,~b,~.1f,~b,"
                               "~s,~.2f,~.2f,~.2f,~.2f,~.2f,~.3f,~.3f,~.3f,~.3f,~b,~b~n",
                               [S, N, Sz, Cap, Pool, WalOn, Sps, P50, P99, W, M,
                                MB, F,
                                case J of undefined -> "-"; _ -> integer_to_list(J) end,
                                WP50, WP99, WA, PPB, Space, Wall, CWall, Lag,
                                case Store of #{io_s := IoS} -> IoS; _ -> 0.0 end,
                                maps:get(snaps, R), maps:get(wal_samples, R)])
             end || R <- Rows],
    ok = file:write_file(File,
                         ["scen,n,size,cap,pool,wal_on,snaps_s,p50_ms,p99_ms,"
                          "disk_writes,merged,mb_written,flushes,jbd2_tx,"
                          "wal_p50_ms,wal_p99_ms,write_amp,puts_per_batch,"
                          "space_mb,wall_s,cluster_wall_s,start_lag_s,store_io_s,snaps,"
                          "wal_samples\n" | Lines]);
maybe_csv(_, _) ->
    ok.
