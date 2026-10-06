-module(cksum3).

%% Reductions, not just wall clock. Checksum BIFs are charged reductions
%% proportional to bytes hashed, so batching may save wall clock without
%% saving much reduction cost. This measures both.

-export([run/0]).

t(Name, Fun, N, PerEntry) ->
    _ = Fun(),
    erlang:garbage_collect(),
    R0 = element(2, process_info(self(), reductions)),
    {Time, _} = timer:tc(fun () -> loop(Fun, N) end),
    R1 = element(2, process_info(self(), reductions)),
    Reds = (R1 - R0) / N,
    io:format("  ~-46s ~9.1f us ~9.1f reds | per entry ~7.3f us ~7.2f reds~n",
              [Name, Time / N, Reds, (Time / N) / PerEntry, Reds / PerEntry]),
    {Time / N, Reds}.

loop(_Fun, 0) -> ok;
loop(Fun, N) -> _ = Fun(), loop(Fun, N - 1).

run() ->
    NumRecs = 1024,
    [bench(NumRecs, Sz) || Sz <- [64, 256, 4096]],
    ok.

bench(NumRecs, PayloadSz) ->
    io:format("~n-- ~b records of ~b byte payloads --~n", [NumRecs, PayloadSz]),
    Payload = crypto:strong_rand_bytes(PayloadSz),
    Bin = term_to_iovec({enqueue, self(), 1, Payload}),
    Entries = [[<<I:64, 1:64>> | Bin] || I <- lists:seq(1, NumRecs)],
    Flat = iolist_to_binary(Entries),
    Iters = 2000,
    io:format("  (batch is ~.1f KB)~n", [byte_size(Flat) / 1024]),

    {_, RA} = t("N x adler32 of each record iolist (current)",
                fun () -> [erlang:adler32(E) || E <- Entries] end,
                Iters, NumRecs),
    {_, RB} = t("1 x crc32 of the pre-flattened batch",
                fun () -> erlang:crc32(Flat) end, Iters, NumRecs),
    {_, RC} = t("iolist_to_binary of the batch (the flatten)",
                fun () -> iolist_to_binary(Entries) end, Iters, NumRecs),
    {_, RD} = t("flatten + crc32 (what framing actually costs)",
                fun () -> erlang:crc32(iolist_to_binary(Entries)) end,
                Iters, NumRecs),
    io:format("  reductions: current ~.1f -> framed ~.1f  (~.1f pct of current)~n",
              [RA, RD, 100 * RD / RA]),
    _ = {RB, RC},
    ok.
