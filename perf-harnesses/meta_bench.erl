-module(meta_bench).

%% ra_log_meta is a per-system shared gen_batch_server backed by dets.
%% ra_server_proc calls ra_server:persist_last_applied/1 on every leader
%% event, which casts a `last_applied' store whenever last_applied advanced.
%% This measures how many such stores per second the meta store can absorb.

-export([run/0]).

run() ->
    _ = application:load(ra),
    Dir = "/tmp/ra_meta_bench",
    _ = os:cmd("rm -rf " ++ Dir),
    ok = application:set_env(ra, data_dir, Dir),
    ok = filelib:ensure_dir(filename:join(Dir, "x")),
    ra_env:configure_logger(logger),
    logger:set_primary_config(level, error),
    SysCfg = (ra_system:default_config())#{data_dir => Dir},
    {ok, Pid} = ra_log_meta:start_link(SysCfg),
    Name = ra_log_meta,
    io:format("~n=== ra_log_meta:store throughput (dets backed) ===~n"),
    [begin
         UIds = [iolist_to_binary(io_lib:format("meta_uid_~6..0b", [I]))
                 || I <- lists:seq(1, NumServers)],
         N = 100000,
         T0 = erlang:monotonic_time(),
         store_loop(Name, list_to_tuple(UIds), NumServers, N, 1),
         %% force a flush by doing a sync call
         ok = ra_log_meta:store_sync(Name, hd(UIds), last_applied, N),
         T1 = erlang:monotonic_time(),
         Us = erlang:convert_time_unit(T1 - T0, native, microsecond),
         io:format("~6b servers: ~14.1f stores/sec (~8.3f us/store)~n",
                   [NumServers, N / (Us / 1000000), Us / N])
     end || NumServers <- [1, 100, 1000]],
    io:format("~n=== raw dets vs ets insert cost ===~n"),
    T = ra_log_meta,
    Objs = [{iolist_to_binary(io_lib:format("x_~6..0b", [I])), 1, undefined, I}
            || I <- lists:seq(1, 1000)],
    time_it("dets:insert 1000 objects (one batch)",
            fun () -> ok = dets:insert(T, Objs) end, 200),
    time_it("ets:insert 1000 objects (one batch)",
            fun () -> true = ets:insert(T, Objs) end, 200),
    time_it("dets:sync", fun () -> ok = dets:sync(T) end, 100),
    proc_lib:stop(Pid),
    _ = os:cmd("rm -rf " ++ Dir),
    ok.

time_it(Name, Fun, N) ->
    _ = Fun(),
    {Time, _} = timer:tc(fun () -> loop(Fun, N) end),
    io:format("~-46s ~10.1f us/op~n", [Name, Time / N]),
    ok.

loop(_Fun, 0) -> ok;
loop(Fun, N) -> _ = Fun(), loop(Fun, N - 1).

store_loop(_Name, _UIds, _NumServers, 0, _V) ->
    ok;
store_loop(Name, UIds, NumServers, N, V) ->
    UId = element((N rem NumServers) + 1, UIds),
    ok = ra_log_meta:store(Name, UId, last_applied, V),
    store_loop(Name, UIds, NumServers, N - 1, V + 1).
