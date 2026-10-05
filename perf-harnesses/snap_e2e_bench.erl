%% End to end benchmark of snapshots through real Ra servers: many single
%% member clusters, each applying commands that take a snapshot every few
%% commands, with the snapshot log (ra_log_snap_store) on or off.
%%
%%   erlc -o /tmp snap_e2e_bench.erl
%%   erl -noshell -pa /tmp -pa <ra>/_build/default/lib/*/ebin -eval '
%%     snap_e2e_bench:run("/mnt/ext4/e2e",
%%                        #{members => 1000, state_size => 1024,
%%                          commands => 100, snapshot_every => 5,
%%                          modes => [directories, log],
%%                          device => "nvme0n1"}),
%%     halt().'
%%
%% Reports commands/s, snapshots/s (sum of the snapshots_written counters),
%% how many of the snapshots the workload asked for were taken (a snapshot is
%% skipped if the previous one is still being written), and with `device' the
%% block layer writes/flushes/MB written over the run.
%%
%% Modes: directories (a directory per snapshot), log (the snapshot log) and
%% none (no snapshots at all, the baseline of WAL and segment writes to
%% subtract from the others).
-module(snap_e2e_bench).

-export([run/1, run/2]).
%% ra_machine
-export([init/1, apply/3]).

run(Dir) ->
    run(Dir, #{}).

run(Dir0, Opts) ->
    Dir = filename:absname(Dir0),
    {ok, _} = application:ensure_all_started(ra),
    Modes = maps:get(modes, Opts, [directories, log]),
    Rows = [bench(Dir, Mode, Opts) || Mode <- Modes],
    io:format("~n~-12s ~8s ~10s ~10s ~9s ~7s ~9s ~9s ~9s ~9s~n",
              ["mode", "members", "cmds/s", "snaps/s", "snaps", "done %",
               "wall s", "disk wr", "flushes", "MB wr"]),
    [io:format("~-12s ~8b ~10.1f ~10.1f ~9b ~7.1f ~9.2f ~9b ~9b ~9.1f~n",
               [Mode, N, CmdsS, SnapsS, Snaps, Done, Wall, W, F, MB])
     || #{mode := Mode, members := N, cmds_s := CmdsS, snaps_s := SnapsS,
          snaps := Snaps, done := Done, wall := Wall, writes := W,
          flushes := F, mb := MB} <- Rows],
    ok.

bench(Dir, Mode, Opts) ->
    N = maps:get(members, Opts, 1000),
    Size = maps:get(state_size, Opts, 1024),
    Cmds = maps:get(commands, Opts, 100),
    Every0 = maps:get(snapshot_every, Opts, 5),
    %% no snapshots for the baseline
    Every = case Mode of
                none -> 1000000000;
                _ -> Every0
            end,
    Sys = list_to_atom("e2e_" ++ atom_to_list(Mode)),
    DataDir = filename:join(Dir, atom_to_list(Mode)),
    _ = file:del_dir_r(DataDir),
    ok = filelib:ensure_dir(filename:join(DataDir, "x")),
    SysCfg0 = #{name => Sys,
                names => ra_system:derive_names(Sys),
                data_dir => DataDir},
    SysCfg = case Mode of
                 log ->
                     SysCfg0#{snapshot_store =>
                                  #{max_size => 16384,
                                    min_file_bytes => maps:get(min_file_bytes,
                                                               Opts, 64 * 1024 * 1024)}};
                 _ ->
                     SysCfg0
             end,
    {ok, _} = ra_system:start(SysCfg),
    Names = [list_to_atom("m" ++ integer_to_list(I)) || I <- lists:seq(1, N)],
    io:format("~s: starting ~b members~n", [Mode, N]),
    Ids = [{Name, node()} || Name <- Names],
    [begin
         UId = atom_to_binary(Name, utf8),
         {ok, [_], []} = ra:start_cluster(
                           Sys, [#{cluster_name => Name,
                                   id => Id,
                                   uid => UId,
                                   initial_members => [Id],
                                   machine => {module, ?MODULE,
                                               #{size => Size}},
                                   log_init_args =>
                                       #{uid => UId,
                                         min_snapshot_interval => Every}}])
     end || {Name, _} = Id <- Ids],
    _ = os:cmd("sync"),
    timer:sleep(1000),
    Dev = maps:get(device, Opts, undefined),
    D0 = disk_stats(Dev),
    Parent = self(),
    T0 = erlang:monotonic_time(millisecond),
    Pids = [spawn_link(fun () ->
                               [{ok, _, _} = ra:process_command(Id, inc, 60000)
                                || _ <- lists:seq(1, Cmds)],
                               Parent ! {done, self()}
                       end) || Id <- Ids],
    [receive {done, P} -> ok after 600000 -> exit(timeout) end || P <- Pids],
    T1 = erlang:monotonic_time(millisecond),
    %% let outstanding snapshots finish
    timer:sleep(2000),
    D1 = disk_stats(Dev),
    Snaps = lists:sum([maps:get(snapshots_written,
                                ra_counters:counters(Id, [snapshots_written]),
                                0) || Id <- Ids]),
    Wall = (T1 - T0) / 1000,
    ok = ra_system:stop(Sys),
    #{mode := _} = Row = #{mode => Mode, members => N,
                           cmds_s => N * Cmds / Wall,
                           snaps_s => Snaps / Wall,
                           snaps => Snaps, wall => Wall,
                           done => 100 * Snaps / max(1, N * (Cmds div Every0)),
                           writes => delta(writes, D0, D1),
                           flushes => delta(flushes, D0, D1),
                           mb => delta(sectors, D0, D1) * 512 / 1048576},
    Row.

delta(K, #{} = A, #{} = B) -> maps:get(K, B) - maps:get(K, A);
delta(_, _, _) -> 0.

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
            #{writes => lists:nth(5, Ints),
              sectors => lists:nth(7, Ints),
              flushes => case length(Ints) >= 16 of
                             true -> lists:nth(16, Ints);
                             false -> 0
                         end};
        [] ->
            undefined
    end.

%% ra_machine: a counter plus a blob that makes the snapshot the wanted size
init(#{size := Size}) ->
    #{count => 0, blob => crypto:strong_rand_bytes(Size)}.

apply(#{index := Idx}, inc, #{count := Count} = State0) ->
    State = State0#{count := Count + 1},
    {State, Count + 1, [{release_cursor, Idx, State}]}.
