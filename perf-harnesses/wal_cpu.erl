-module(wal_cpu).

%% Isolates the CPU cost of the WAL hot path by calling
%% ra_log_wal:handle_batch/2 directly with a synthetic op list.
%% No producer/consumer scheduling noise.

-export([run/0, run/1]).

run() -> run(#{}).

run(Opts) ->
    _ = application:load(ra),
    Dir = "/tmp/ra_wal_cpu",
    _ = os:cmd("rm -rf " ++ Dir),
    ok = application:set_env(ra, data_dir, Dir),
    ok = filelib:ensure_dir(filename:join(Dir, "x")),
    ra_env:configure_logger(logger),
    logger:set_primary_config(level, error),
    SysCfg = (ra_system:default_config())#{data_dir => Dir},
    ra_system:store(SysCfg),
    ra_counters:init(),
    ra_snapshot:init_ets(),
    {ok, Ets} = ra_log_ets:start_link(SysCfg),
    Names = maps:get(names, SysCfg),
    io:format("~nmodule: ~s~n", [code:which(ra_log_wal)]),
    io:format("~-10s ~-8s ~-12s ~14s ~14s ~12s~n",
              ["writers", "cksum", "batchsize", "us/entry", "entries/s",
               "reds/entry"]),
    io:format("~s~n", [lists:duplicate(76, $-)]),
    [bench(Dir, Names, NumWriters, Cksum, BatchSize, Opts)
     || Cksum <- maps:get(cksums, Opts, [true, false]),
        NumWriters <- maps:get(writers, Opts, [1, 8, 64]),
        BatchSize <- maps:get(batch_sizes, Opts, [64, 1024, 8192])],
    proc_lib:stop(Ets),
    _ = os:cmd("rm -rf " ++ Dir),
    ok.

bench(BaseDir, Names, NumWriters, Cksum, BatchSize, Opts) ->
    NumEntries = maps:get(entries, Opts, 400000),
    DataSize = maps:get(data_size, Opts, 256),
    Parent = self(),
    Runner =
        spawn_link(
          fun () ->
                  Gen = erlang:unique_integer([positive, monotonic]),
                  Dir = filename:join([BaseDir,
                                       io_lib:format("~w_~w_~w_~w",
                                                     [Gen, NumWriters, Cksum,
                                                      BatchSize])]),
                  ok = filelib:ensure_dir(filename:join(Dir, "x")),
                  Sink = spawn_link(fun sink/0),
                  Conf = #{dir => Dir,
                           system => default,
                           names => Names#{segment_writer => Sink,
                                           wal => wal_name(Gen)},
                           compute_checksums => Cksum,
                           sync_method => none,
                           max_size_bytes => 4096 * 1000 * 1000},
                  {ok, St0} = ra_log_wal:init(Conf),
                  %% register writers + mem tables
                  Writers =
                      [begin
                           UId = uid(Gen, I),
                           ok = ra_directory:register_name(
                                  default, UId, Sink, undefined,
                                  binary_to_atom(UId, utf8),
                                  binary_to_atom(UId, utf8)),
                           ok = ra_log_snapshot_state:insert(
                                  ra_log_snapshot_state, UId, -1, 0, []),
                           {ok, Mt} = ra_log_ets:mem_table_please(Names, UId),
                           {UId, ra_mt:tid(Mt)}
                       end || I <- lists:seq(1, NumWriters)],
                  Data = crypto:strong_rand_bytes(DataSize),
                  Bin = {ttb, term_to_iovec({enqueue, Sink, 1, Data})},
                  NumBatches = NumEntries div BatchSize,
                  %% pre-build the op lists so building them is not measured
                  Batches = build_batches(Writers, Sink, Bin, BatchSize,
                                          NumBatches),
                  R0 = element(2, process_info(self(), reductions)),
                  T0 = erlang:monotonic_time(),
                  _ = lists:foldl(fun (Ops, S) ->
                                          {ok, _, S1} =
                                              ra_log_wal:handle_batch(Ops, S),
                                          S1
                                  end, St0, Batches),
                  T1 = erlang:monotonic_time(),
                  R1 = element(2, process_info(self(), reductions)),
                  Us = erlang:convert_time_unit(T1 - T0, native, microsecond),
                  Total = NumBatches * BatchSize,
                  Parent ! {result, Us, Total, R1 - R0},
                  [ra_directory:unregister_name(default, U)
                   || {U, _} <- Writers],
                  [ra_log_ets:delete_mem_tables(Names, U)
                   || {U, _} <- Writers],
                  exit(Sink, kill),
                  ok
          end),
    receive
        {result, Us, Total, Reds} ->
            io:format("~-10w ~-8w ~-12w ~14.3f ~14.1f ~12.1f~n",
                      [NumWriters, Cksum, BatchSize, Us / Total,
                       Total / (Us / 1000000), Reds / Total])
    after 600000 ->
              exit(timeout)
    end,
    unlink(Runner),
    ok.

wal_name(Gen) ->
    binary_to_atom(iolist_to_binary(io_lib:format("wal_cpu_~b", [Gen])), utf8).

uid(Gen, I) ->
    iolist_to_binary(io_lib:format("cpu_uid_~6..0b_~6..0b", [Gen, I])).

%% Round-robin ops across writers. Each writer's indexes are sequential.
build_batches(Writers, Pid, Bin, BatchSize, NumBatches) ->
    W = list_to_tuple(Writers),
    N = tuple_size(W),
    {Batches, _} =
        lists:foldl(
          fun (_B, {Acc, Counts0}) ->
                  {Ops, Counts} =
                      lists:foldl(
                        fun (K, {Os, Cs}) ->
                                WI = (K rem N) + 1,
                                {UId, Tid} = element(WI, W),
                                Idx = maps:get(WI, Cs, 0),
                                Op = {cast, {append, {UId, Pid}, Tid,
                                             Idx - 1, Idx, 1, Bin}},
                                {[Op | Os], Cs#{WI => Idx + 1}}
                        end, {[], Counts0}, lists:seq(1, BatchSize)),
                  %% gen_batch_server delivers reversed batches
                  {[Ops | Acc], Counts}
          end, {[], #{}}, lists:seq(1, NumBatches)),
    %% batches were accumulated newest first, restore ascending index order
    lists:reverse(Batches).

sink() ->
    receive
        {'$gen_call', From, _} ->
            gen:reply(From, ok),
            sink();
        _ ->
            sink()
    end.
