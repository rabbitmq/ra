-module(wal_scale).

%% Separates the two quantities that could drive the WAL's per-entry cost:
%%
%%   Total  - writers known to the WAL process: the #state.writers map, the
%%            #wal.writer_name_cache and #wal.ranges maps. These persist for
%%            the lifetime of the WAL process / WAL file.
%%   InBatch- writers appearing in the batch being handled: the #batch.waiting
%%            map (keyed by pid) and the complete_batch/1 fold over it.
%%
%% All Total writers are warmed into the WAL's maps first, then the measured
%% batches only touch InBatch of them.

-export([run/0, run/1]).

run() -> run(#{}).

run(Opts) ->
    _ = application:load(ra),
    Dir = "/tmp/ra_wal_scale",
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
    io:format("~-10s ~-10s ~14s ~14s ~10s ~8s~n",
              ["total", "in_batch", "reds/entry", "us/entry", "wal_writes",
               "batches"]),
    io:format("~s~n", [lists:duplicate(72, $-)]),
    Combos = maps:get(combos, Opts,
                      [{1, 1},
                       {8, 1}, {8, 8},
                       {64, 1}, {64, 8}, {64, 64},
                       {512, 1}, {512, 8}, {512, 64}, {512, 512},
                       {4096, 1}, {4096, 8}, {4096, 64}]),
    [bench(Dir, Names, T, W, Opts) || {T, W} <- Combos],
    proc_lib:stop(Ets),
    _ = os:cmd("rm -rf " ++ Dir),
    ok.

bench(BaseDir, Names, Total, InBatch, Opts) ->
    NumEntries = maps:get(entries, Opts, 200000),
    BatchSize = maps:get(batch_size, Opts, 1024),
    DataSize = maps:get(data_size, Opts, 256),
    Parent = self(),
    _ = spawn_link(
          fun () ->
                  Gen = erlang:unique_integer([positive, monotonic]),
                  Dir = filename:join([BaseDir,
                                       io_lib:format("~w_~w_~w",
                                                     [Gen, Total, InBatch])]),
                  ok = filelib:ensure_dir(filename:join(Dir, "x")),
                  Sink = spawn_link(fun sink/0),
                  Conf = #{dir => Dir,
                           system => default,
                           names => Names#{segment_writer => Sink,
                                           wal => wal_name(Gen)},
                           compute_checksums =>
                               maps:get(cksum, Opts, true),
                           sync_method => none,
                           max_size_bytes => 16#7fffffff},
                  {ok, St0} = ra_log_wal:init(Conf),
                  %% each writer must have its own pid: the WAL keys its
                  %% per-batch #batch_writer state by pid, so sharing one pid
                  %% across writers with different tids would force a new
                  %% chained batch_writer for every single entry
                  Writers = [begin
                                 UId = uid(Gen, I),
                                 ok = ra_log_snapshot_state:insert(
                                        ra_log_snapshot_state, UId, -1, 0, []),
                                 {ok, Mt} = ra_log_ets:mem_table_please(Names,
                                                                        UId),
                                 {UId, ra_mt:tid(Mt), spawn_link(fun sink/0)}
                             end || I <- lists:seq(1, Total)],
                  W = list_to_tuple(Writers),
                  Data = crypto:strong_rand_bytes(DataSize),
                  Bin = {ttb, term_to_iovec({enqueue, Sink, 1, Data})},

                  %% warm up: one op per writer so that every Total writer is
                  %% present in #state.writers, writer_name_cache and ranges
                  WarmOps = [op(element(K, W), Bin, 0)
                             || K <- lists:seq(1, Total)],
                  {ok, _, St1} = ra_log_wal:handle_batch(WarmOps, St0),

                  %% measured batches touch only the first InBatch writers,
                  %% continuing each writer's index sequence from 1
                  NumBatches = NumEntries div BatchSize,
                  Batches = build(W, InBatch, Bin, BatchSize, NumBatches),

                  R0 = element(2, process_info(self(), reductions)),
                  T0 = erlang:monotonic_time(),
                  _ = lists:foldl(fun (Ops, S) ->
                                          {ok, _, S1} =
                                              ra_log_wal:handle_batch(Ops, S),
                                          S1
                                  end, St1, Batches),
                  T1 = erlang:monotonic_time(),
                  R1 = element(2, process_info(self(), reductions)),
                  Us = erlang:convert_time_unit(T1 - T0, native, microsecond),
                  CRef = ra_counters:fetch(wal_name(Gen)),
                  Parent ! {result, Us, NumBatches * BatchSize, R1 - R0,
                            counters:get(CRef, 3),
                            counters:get(CRef, 2)},
                  [ra_log_ets:delete_mem_tables(Names, U)
                   || {U, _, _} <- Writers],
                  [exit(P, kill) || {_, _, P} <- Writers],
                  exit(Sink, kill)
          end),
    receive
        {result, Us, N, Reds, Writes, Batches} ->
            io:format("~-10w ~-10w ~14.1f ~14.3f ~10w ~8w~n",
                      [Total, InBatch, Reds / N, Us / N, Writes, Batches])
    after 600000 ->
              exit(timeout)
    end.

op({UId, Tid, Pid}, Bin, Idx) ->
    {cast, {append, {UId, Pid}, Tid, Idx - 1, Idx, 1, Bin}}.

build(W, InBatch, Bin, BatchSize, NumBatches) ->
    {Batches, _} =
        lists:foldl(
          fun (_B, {Acc, Counts0}) ->
                  {Ops, Counts} =
                      lists:foldl(
                        fun (K, {Os, Cs}) ->
                                WI = (K rem InBatch) + 1,
                                Idx = maps:get(WI, Cs, 1),
                                {[op(element(WI, W), Bin, Idx) | Os],
                                 Cs#{WI => Idx + 1}}
                        end, {[], Counts0}, lists:seq(1, BatchSize)),
                  {[Ops | Acc], Counts}
          end, {[], #{}}, lists:seq(1, NumBatches)),
    %% batches were accumulated newest first, restore ascending index order
    %% so that each batch continues the previous one's sequence
    lists:reverse(Batches).

wal_name(Gen) ->
    binary_to_atom(iolist_to_binary(io_lib:format("wal_scale_~b", [Gen])),
                   utf8).

uid(Gen, I) ->
    iolist_to_binary(io_lib:format("scale_~6..0b_~6..0b", [Gen, I])).

sink() ->
    receive
        {'$gen_call', From, _} ->
            gen:reply(From, ok),
            sink();
        _ ->
            sink()
    end.
