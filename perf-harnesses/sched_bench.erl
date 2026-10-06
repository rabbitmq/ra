-module(sched_bench).

%% Finding 10: the segment writer chunks writers into groups of
%% schedulers-2 and barriers on each chunk. How much that costs depends
%% entirely on how skewed the per-writer work is, which I have no field
%% data for - so model a range of distributions and see where the barrier
%% actually hurts.
%%
%% Per writer cost = Fixed + Entries * PerEntry.
%% Fixed is now ~150us after the segment file cache (finding 8); before it
%% was 150-1100us depending on directory size. PerEntry is ~1.0us from the
%% flush measurement in finding 9.

-export([run/0]).

-define(FIXED_US, 150).
-define(PER_ENTRY_US, 1.0).

run() ->
    rand:seed(exsss, {42, 42, 42}),
    Degree = max(1, erlang:system_info(schedulers) - 2),
    io:format("~ndegree ~b (schedulers - 2), fixed ~bus/writer, "
              "~.1fus/entry~n", [Degree, ?FIXED_US, ?PER_ENTRY_US]),
    io:format("~n~-22s ~7s ~10s ~10s ~10s ~10s ~8s ~8s~n",
              ["distribution", "writers", "total ms", "chunked", "sorted",
               "queue", "c/q", "s/q"]),
    io:format("~s~n", [lists:duplicate(96, $-)]),
    [begin
         Work = [cost(E) || E <- entries(Dist, N)],
         Total = lists:sum(Work),
         C = chunked(Work, Degree),
         S = chunked(lists:reverse(lists:sort(Work)), Degree),
         Q = queued(Work, Degree),
         io:format("~-22s ~7b ~10.1f ~10.1f ~10.1f ~10.1f ~8.2f ~8.2f~n",
                   [Dist, N, Total / 1000, C / 1000, S / 1000, Q / 1000,
                    C / Q, S / Q])
     end || {Dist, N} <- [{uniform, 100}, {uniform, 1000},
                          {mild_skew, 100}, {mild_skew, 1000},
                          {heavy_tail, 100}, {heavy_tail, 1000},
                          {one_dominant, 100}, {one_dominant, 1000},
                          {mostly_idle, 1000}, {mostly_idle, 10000}]],
    io:format("~nchunked = current, sorted = current but longest first,~n"
              "queue   = greedy work queue. c/q and s/q are speedups over"
              " the queue.~n"),
    ok.

cost(Entries) ->
    ?FIXED_US + Entries * ?PER_ENTRY_US.

%% entry counts per writer for this wal file
entries(uniform, N) ->
    [1000 || _ <- lists:seq(1, N)];
entries(mild_skew, N) ->
    %% exponential-ish spread, ratio of about 20x across writers
    [round(100 * math:exp(rand:uniform() * 3)) || _ <- lists:seq(1, N)];
entries(heavy_tail, N) ->
    %% 5% of writers carry 100x the entries of the rest
    [case rand:uniform(20) of
         1 -> 20000 + rand:uniform(20000);
         _ -> 100 + rand:uniform(200)
     end || _ <- lists:seq(1, N)];
entries(one_dominant, N) ->
    %% a single very hot writer, the rest light
    [100 + rand:uniform(100) || _ <- lists:seq(1, N - 1)] ++ [500000];
entries(mostly_idle, N) ->
    %% the shape a broker with many idle queues has: nearly all writers
    %% contribute a handful of entries, a few are busy
    [case rand:uniform(50) of
         1 -> 5000 + rand:uniform(5000);
         _ -> 1 + rand:uniform(5)
     end || _ <- lists:seq(1, N)].

%% current: chunk into groups of Degree, barrier on each chunk
chunked(Work, Degree) ->
    lists:sum([lists:max(C) || C <- chunk(Work, Degree, [])]).

chunk([], _N, Acc) -> lists:reverse(Acc);
chunk(L, N, Acc) when length(L) =< N -> lists:reverse([L | Acc]);
chunk(L, N, Acc) ->
    {H, T} = lists:split(N, L),
    chunk(T, N, [H | Acc]).

%% greedy list scheduling onto Degree workers
queued(Work, Degree) ->
    Slots = lists:duplicate(Degree, 0),
    lists:max(lists:foldl(fun (W, Ss) ->
                                  [M | Rest] = lists:sort(Ss),
                                  [M + W | Rest]
                          end, Slots, Work)).
