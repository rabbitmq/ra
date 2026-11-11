-module(wal_bench).

%% End-to-end WAL throughput benchmark. Drives ra_log_wal directly with
%% N concurrent writer processes, bypassing ra_server.

-export([run/0, run/1]).

-define(DATA_SIZE, 256).

run() ->
    run(#{}).

run(Opts) ->
    _ = application:load(ra),
    Dir0 = maps:get(dir, Opts, "/tmp/ra_wal_bench"),
    _ = os:cmd("rm -rf " ++ Dir0),
    ok = application:set_env(ra, data_dir, Dir0),
    ok = filelib:ensure_dir(filename:join(Dir0, "x")),
    ra_env:configure_logger(logger),
    logger:set_primary_config(level, error),
    SysCfg = (ra_system:default_config())#{data_dir => Dir0},
    ra_system:store(SysCfg),
    %% ra_log_ets:init creates the directory table
    ra_counters:init(),
    ra_snapshot:init_ets(),
    {ok, Ets} = ra_log_ets:start_link(SysCfg),

    io:format("~n=== WAL end-to-end throughput ===~n"),
    io:format("data size ~b bytes, batch cap ~b~n",
              [maps:get(data_size, Opts, ?DATA_SIZE), 8192]),
    io:format("~-8s ~-10s ~-8s ~12s ~12s ~12s ~10s~n",
              ["writers", "sync", "cksum", "entries/s", "MB/s", "us/entry",
               "batches"]),
    io:format("~s~n", [lists:duplicate(84, $-)]),
    [begin
         bench(Dir0, SysCfg, NumWriters, Sync, Cksum, Opts)
     end || Sync <- maps:get(syncs, Opts, [none, datasync]),
            Cksum <- maps:get(cksums, Opts, [true, false]),
            NumWriters <- maps:get(writers, Opts, [1, 8, 64])],
    proc_lib:stop(Ets),
    _ = os:cmd("rm -rf " ++ Dir0),
    ok.

bench(BaseDir, SysCfg, NumWriters, Sync, Cksum, Opts) ->
    NumEntries = maps:get(entries, Opts, 200000),
    DataSize = maps:get(data_size, Opts, ?DATA_SIZE),
    Dir = filename:join([BaseDir, io_lib:format("~s_~s_~b", [Sync, Cksum,
                                                             NumWriters])]),
    ok = filelib:ensure_dir(filename:join(Dir, "x")),
    Names = maps:get(names, SysCfg),
    %% a sink process to stand in for the segment writer
    Sink = spawn_link(fun sink/0),
    WalConf = #{dir => Dir,
                system => default,
                names => Names#{segment_writer => Sink},
                compute_checksums => Cksum,
                sync_method => Sync,
                max_size_bytes => 512 * 1000 * 1000},
    {ok, Wal} = ra_log_wal:start_link(WalConf),

    Each = NumEntries div NumWriters,
    Parent = self(),
    Gen = erlang:unique_integer([positive, monotonic]),
    Pids = [spawn_writer(Parent, Wal, Names, {Gen, I}, Each, DataSize)
            || I <- lists:seq(1, NumWriters)],
    [receive {ready, P} -> ok end || P <- Pids],
    T0 = erlang:monotonic_time(),
    [P ! go || P <- Pids],
    [receive {done, P} -> ok after 600000 -> exit({timeout, P}) end || P <- Pids],
    T1 = erlang:monotonic_time(),
    Us = erlang:convert_time_unit(T1 - T0, native, microsecond),
    Total = Each * NumWriters,
    Batches = counters:get(ra_counters:fetch(maps:get(wal, Names)), 2),
    io:format("~-8w ~-10w ~-8w ~12.1f ~12.1f ~12.3f ~10w~n",
              [NumWriters, Sync, Cksum,
               Total / (Us / 1000000),
               (Total * DataSize) / (Us / 1000000) / 1048576,
               Us / Total,
               Batches]),
    proc_lib:stop(Wal),
    unlink(Sink),
    exit(Sink, kill),
    [begin
         ok = ra_log_ets:delete_mem_tables(Names, uid({Gen, I})),
         ra_directory:unregister_name(default, uid({Gen, I}))
     end || I <- lists:seq(1, NumWriters)],
    ok.

uid({G, I}) ->
    list_to_binary(io_lib:format("bench_uid_~6..0b_~6..0b", [G, I])).

spawn_writer(Parent, Wal, Names, I, Num, DataSize) ->
    spawn_link(
      fun () ->
              UId = uid(I),
              Nm = list_to_atom(binary_to_list(UId)),
              ok = ra_directory:register_name(default, UId, self(), undefined,
                                              Nm, Nm),
              ok = ra_log_snapshot_state:insert(ra_log_snapshot_state, UId,
                                                -1, 0, []),
              {ok, Mt} = ra_log_ets:mem_table_please(Names, UId),
              Tid = ra_mt:tid(Mt),
              Data = crypto:strong_rand_bytes(DataSize),
              Cmd = {enqueue, self(), 1, Data},
              Bin = {ttb, term_to_iovec(Cmd)},
              _ = Cmd,
              Parent ! {ready, self()},
              receive go -> ok end,
              ok = write_loop({UId, self()}, Wal, Tid, Bin, 0, Num, 0),
              Parent ! {done, self()}
      end).

%% keep at most Pipe writes outstanding, consuming written events
-define(PIPE, 2000).

write_loop(_Id, _Wal, _Tid, _Bin, Idx, Num, Confirmed)
  when Confirmed >= Num ->
    _ = Idx,
    ok;
write_loop(Id, Wal, Tid, Bin, Idx, Num, Confirmed)
  when Idx - Confirmed >= ?PIPE orelse Idx >= Num ->
    %% wait for confirmations
    receive
        {ra_log_event, {written, _Term, Seq}} ->
            Last = ra_seq:last(Seq),
            write_loop(Id, Wal, Tid, Bin, Idx, Num, Last + 1)
    after 60000 ->
              exit({timeout_waiting_for_written, Idx, Confirmed})
    end;
write_loop(Id, Wal, Tid, Bin, Idx, Num, Confirmed) ->
    %% ETS insert as ra_log does
    true = ets:insert(Tid, {Idx, 1, Bin}),
    {ok, _} = ra_log_wal:write(Wal, Id, Tid, Idx - 1, Idx, 1, Bin),
    write_loop(Id, Wal, Tid, Bin, Idx + 1, Num, Confirmed).

sink() ->
    receive
        {'$gen_call', From, _} ->
            gen:reply(From, ok),
            sink();
        _ ->
            sink()
    end.
