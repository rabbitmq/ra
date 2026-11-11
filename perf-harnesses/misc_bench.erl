-module(misc_bench).

-export([run/0]).

t(Name, Fun, N) ->
    _ = Fun(),
    erlang:garbage_collect(),
    {Time, _} = timer:tc(fun () -> loop(Fun, N) end),
    io:format("~-60s ~12.3f us/op~n", [Name, Time / N]),
    Time / N.

loop(_Fun, 0) -> ok;
loop(Fun, N) -> _ = Fun(), loop(Fun, N - 1).

run() ->
    io:format("~n=== ra_seq:in/2 - no early exit below Idx ===~n"),
    bench_in(),
    io:format("~n=== WAL batch write shape (iovec nesting vs flattened) ===~n"),
    bench_write(),
    io:format("~n=== segment writer chunk barrier vs work queue ===~n"),
    bench_sched(),
    ok.

bench_in() ->
    %% 10k singleton live indexes, ordered high -> low
    Seq = ra_seq:from_list([I * 3 || I <- lists:seq(1, 10000)]),
    Hi = 29997,
    Lo = 3,
    t("ra_seq:in/2 highest index (should be O(1))",
      fun () -> true = ra_seq:in(Hi, Seq) end, 50000),
    t("ra_seq:in/2 lowest index (worst case)",
      fun () -> true = ra_seq:in(Lo, Seq) end, 20000),
    t("ra_seq:in/2 absent, above range",
      fun () -> false = ra_seq:in(40000, Seq) end, 50000),
    t("ra_seq:in/2 absent, below range (full scan today)",
      fun () -> false = ra_seq:in(1, Seq) end, 20000),
    t("early-exit in/2 highest",
      fun () -> true = in_ee(Hi, Seq) end, 50000),
    t("early-exit in/2 lowest",
      fun () -> true = in_ee(Lo, Seq) end, 20000),
    t("early-exit in/2 absent below range",
      fun () -> false = in_ee(1, Seq) end, 200000),
    ok.

%% early exit variant: the sequence is ordered high -> low so we can stop
%% as soon as we are below Idx
in_ee(_Idx, []) ->
    false;
in_ee(Idx, [Idx | _]) ->
    true;
in_ee(Idx, [Next | Rem]) when is_integer(Next) ->
    if Next < Idx -> false;
       true -> in_ee(Idx, Rem)
    end;
in_ee(Idx, [{S, E} | Rem]) ->
    if Idx > E -> false;
       Idx >= S -> true;
       true -> in_ee(Idx, Rem)
    end.

bench_write() ->
    Dir = "/tmp/ra_misc_bench",
    _ = os:cmd("rm -rf " ++ Dir),
    ok = filelib:ensure_dir(filename:join(Dir, "x")),
    Data = crypto:strong_rand_bytes(256),
    Rec = fun (I) ->
                  Bin = term_to_iovec({enqueue, self(), I, Data}),
                  [<<1:1, 1:1, 1:22>>, <<0:32, 300:32>>,
                   <<I:64, 1:64>> | Bin]
          end,
    [begin
         Nested = lists:foldl(fun (I, Acc) -> [Acc | Rec(I)] end, [],
                              lists:seq(1, BatchSize)),
         Flat = iolist_to_binary(Nested),
         PerRec = [iolist_to_binary(Rec(I)) || I <- lists:seq(1, BatchSize)],
         io:format("-- batch of ~b records (~b bytes)~n",
                   [BatchSize, byte_size(Flat)]),
         t("iolist_to_iovec of nested batch",
           fun () -> _ = erlang:iolist_to_iovec(Nested) end, 2000),
         io:format("   iovec elements: ~b~n",
                   [length(erlang:iolist_to_iovec(Nested))]),
         {ok, Fd} = file:open(filename:join(Dir, "a"), [raw, write, binary]),
         t("file:write nested iolist",
           fun () -> ok = file:write(Fd, Nested) end, 500),
         _ = file:close(Fd),
         {ok, Fd2} = file:open(filename:join(Dir, "b"), [raw, write, binary]),
         t("file:write single flattened binary",
           fun () -> ok = file:write(Fd2, Flat) end, 500),
         _ = file:close(Fd2),
         {ok, Fd3} = file:open(filename:join(Dir, "c"), [raw, write, binary]),
         t("file:write list of per-record binaries",
           fun () -> ok = file:write(Fd3, PerRec) end, 500),
         _ = file:close(Fd3),
         t("iolist_to_binary of nested batch (the flatten cost)",
           fun () -> _ = iolist_to_binary(Nested) end, 500)
     end || BatchSize <- [1024, 8192]],
    _ = os:cmd("rm -rf " ++ Dir),
    ok.

%% Simulate the segment writer's chunk-barrier scheduling vs a work queue,
%% with skewed per-writer work (as in a real broker: few busy queues).
bench_sched() ->
    Degree = max(1, erlang:system_info(schedulers) - 2),
    rand:seed(exsss, {7, 8, 9}),
    NumWriters = 200,
    %% heavy tailed: most writers tiny, a few large
    Work = [case rand:uniform(20) of
                1 -> 40 + rand:uniform(40);   %% ~5% big
                _ -> 1 + rand:uniform(3)
            end || _ <- lists:seq(1, NumWriters)],
    io:format("~b writers, degree ~b, total work ~b units~n",
              [NumWriters, Degree, lists:sum(Work)]),
    io:format("chunk-barrier model: ~b units of critical path~n",
              [chunked_cost(Work, Degree)]),
    io:format("work-queue model:    ~b units of critical path~n",
              [queue_cost(Work, Degree)]),
    ok.

chunked_cost(Work, Degree) ->
    Chunks = chunk(Work, Degree, []),
    lists:sum([lists:max(C) || C <- Chunks]).

chunk([], _N, Acc) -> lists:reverse(Acc);
chunk(L, N, Acc) when length(L) =< N -> lists:reverse([L | Acc]);
chunk(L, N, Acc) ->
    {H, T} = lists:split(N, L),
    chunk(T, N, [H | Acc]).

%% greedy list scheduling
queue_cost(Work, Degree) ->
    Slots = lists:duplicate(Degree, 0),
    lists:max(lists:foldl(fun (W, Ss) ->
                                  M = lists:min(Ss),
                                  replace_first(M, M + W, Ss)
                          end, Slots, Work)).

replace_first(X, Y, [X | R]) -> [Y | R];
replace_first(X, Y, [H | R]) -> [H | replace_first(X, Y, R)].
