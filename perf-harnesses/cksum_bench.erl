-module(cksum_bench).

%% Per-record checksum vs one checksum over the whole batch.
%% Both hash the same number of bytes, so the question is how much of the
%% per-record cost is fixed BIF overhead rather than hashing throughput.

-export([run/0]).

t(Name, Fun, N) ->
    _ = Fun(),
    erlang:garbage_collect(),
    {Time, _} = timer:tc(fun () -> loop(Fun, N) end),
    io:format("~-52s ~10.3f us~n", [Name, Time / N]),
    Time / N.

loop(_Fun, 0) -> ok;
loop(Fun, N) -> _ = Fun(), loop(Fun, N - 1).

run() ->
    io:format("~n=== fixed per-call overhead vs throughput ===~n"),
    [begin
         B = crypto:strong_rand_bytes(Sz),
         A = t(io_lib:format("adler32 of ~b bytes", [Sz]),
               fun () -> erlang:adler32(B) end, 200000),
         io:format("     -> ~.2f GB/s implied~n", [Sz / A / 1000]),
         C = t(io_lib:format("crc32   of ~b bytes", [Sz]),
               fun () -> erlang:crc32(B) end, 200000),
         io:format("     -> ~.2f GB/s implied~n", [Sz / C / 1000])
     end || Sz <- [64, 256, 4096, 65536, 1048576]],

    io:format("~n=== per-record vs per-batch over a whole batch ===~n"),
    [begin
         io:format("~n-- ~b records of ~b byte payloads --~n",
                   [NumRecs, PayloadSz]),
         Payload = crypto:strong_rand_bytes(PayloadSz),
         Bin = term_to_iovec({enqueue, self(), 1, Payload}),
         Hdr = <<1:1, 1:1, 1:22>>,
         %% build the batch exactly as the wal does: a left nested iolist
         PerRecord =
             fun () ->
                     lists:foldl(
                       fun (I, Acc) ->
                               Entry = [<<I:64, 1:64>> | Bin],
                               C = erlang:adler32(Entry),
                               L = iolist_size(Bin),
                               [Acc, Hdr, <<C:32, L:32>> | Entry]
                       end, [], lists:seq(1, NumRecs))
             end,
         PerBatch =
             fun () ->
                     Pend = lists:foldl(
                              fun (I, Acc) ->
                                      Entry = [<<I:64, 1:64>> | Bin],
                                      L = iolist_size(Bin),
                                      [Acc, Hdr, <<L:32>> | Entry]
                              end, [], lists:seq(1, NumRecs)),
                     Crc = erlang:crc32(Pend),
                     [<<"RABT", NumRecs:32, Crc:32>> | Pend]
             end,
         Iters = max(100, 200000 div NumRecs),
         A = t("build batch, checksum per record (current)", PerRecord, Iters),
         B = t("build batch, one crc32 over the batch", PerBatch, Iters),
         NoCk = t("build batch, no checksum at all (floor)",
                  fun () ->
                          lists:foldl(
                            fun (I, Acc) ->
                                    Entry = [<<I:64, 1:64>> | Bin],
                                    L = iolist_size(Bin),
                                    [Acc, Hdr, <<0:32, L:32>> | Entry]
                            end, [], lists:seq(1, NumRecs))
                  end, Iters),
         io:format("  per entry: current ~.3f us, batch crc ~.3f us, "
                   "none ~.3f us~n",
                   [A / NumRecs, B / NumRecs, NoCk / NumRecs]),
         io:format("  checksum cost per entry: current ~.3f us -> "
                   "batch ~.3f us  (~.0f%% less)~n",
                   [(A - NoCk) / NumRecs, (B - NoCk) / NumRecs,
                    100 * (1 - (B - NoCk) / max(A - NoCk, 0.000001))])
     end || {NumRecs, PayloadSz} <- [{1024, 64}, {1024, 256}, {1024, 4096},
                                     {64, 256}, {8192, 256}]],
    ok.
