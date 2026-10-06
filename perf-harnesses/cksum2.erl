-module(cksum2).

%% Full pipeline comparison: build the batch, checksum it, write it.
%%   A: per-record adler32, write the nested iolist          (current)
%%   B: no per-record checksum, flatten once, crc32 the flat
%%      binary, write [BatchHeader, FlatBin]                 (batch framing)
%%   C: no checksum at all, write the nested iolist          (floor)

-export([run/0]).

t(Name, Fun, N) ->
    _ = Fun(),
    erlang:garbage_collect(),
    {Time, _} = timer:tc(fun () -> loop(Fun, N) end),
    io:format("  ~-50s ~9.1f us  ~7.3f us/entry~n",
              [Name, Time / N, (Time / N) / get(nr)]),
    Time / N.

loop(_Fun, 0) -> ok;
loop(Fun, N) -> _ = Fun(), loop(Fun, N - 1).

run() ->
    Dir = "/tmp/ra_cksum2",
    _ = os:cmd("rm -rf " ++ Dir),
    ok = filelib:ensure_dir(filename:join(Dir, "x")),
    {ok, Fd} = file:open(filename:join(Dir, "w"), [raw, write, binary]),
    [bench(Fd, NumRecs, Sz) || {NumRecs, Sz} <- [{1024, 64},
                                                 {1024, 256},
                                                 {1024, 4096},
                                                 {8192, 256}]],
    _ = file:close(Fd),
    _ = os:cmd("rm -rf " ++ Dir),
    ok.

bench(Fd, NumRecs, PayloadSz) ->
    put(nr, NumRecs),
    io:format("~n-- ~b records of ~b byte payloads (~.1f KB batch) --~n",
              [NumRecs, PayloadSz, NumRecs * (PayloadSz + 47) / 1024]),
    Payload = crypto:strong_rand_bytes(PayloadSz),
    Bin = term_to_iovec({enqueue, self(), 1, Payload}),
    Hdr = <<1:1, 1:1, 1:22>>,
    Iters = max(100, 100000 div NumRecs),
    Rewind = fun () -> {ok, 0} = file:position(Fd, 0) end,

    A = t("A current: per record adler32 + write nested",
          fun () ->
                  Pend = lists:foldl(
                           fun (I, Acc) ->
                                   E = [<<I:64, 1:64>> | Bin],
                                   C = erlang:adler32(E),
                                   L = iolist_size(Bin),
                                   [Acc, Hdr, <<C:32, L:32>> | E]
                           end, [], lists:seq(1, NumRecs)),
                  ok = file:write(Fd, Pend),
                  Rewind()
          end, Iters),

    B = t("B batch: flatten once, crc32 flat, write [hdr|flat]",
          fun () ->
                  Pend = lists:foldl(
                           fun (I, Acc) ->
                                   E = [<<I:64, 1:64>> | Bin],
                                   L = iolist_size(Bin),
                                   [Acc, Hdr, <<L:32>> | E]
                           end, [], lists:seq(1, NumRecs)),
                  Flat = iolist_to_binary(Pend),
                  Crc = erlang:crc32(Flat),
                  ok = file:write(Fd, [<<"RABT", NumRecs:32, Crc:32,
                                         (byte_size(Flat)):32>> | Flat]),
                  Rewind()
          end, Iters),

    C = t("C floor: no checksum + write nested",
          fun () ->
                  Pend = lists:foldl(
                           fun (I, Acc) ->
                                   E = [<<I:64, 1:64>> | Bin],
                                   L = iolist_size(Bin),
                                   [Acc, Hdr, <<0:32, L:32>> | E]
                           end, [], lists:seq(1, NumRecs)),
                  ok = file:write(Fd, Pend),
                  Rewind()
          end, Iters),
    D = t("D per record adler32, flatten once, write flat",
          fun () ->
                  Pend = lists:foldl(
                           fun (I, Acc) ->
                                   E = [<<I:64, 1:64>> | Bin],
                                   Ck = erlang:adler32(E),
                                   L = iolist_size(Bin),
                                   [Acc, Hdr, <<Ck:32, L:32>> | E]
                           end, [], lists:seq(1, NumRecs)),
                  ok = file:write(Fd, iolist_to_binary(Pend)),
                  Rewind()
          end, Iters),
    io:format("  vs A:  B ~.1f%%   C ~.1f%%   D ~.1f%%~n",
              [100 * (B - A) / A, 100 * (C - A) / A, 100 * (D - A) / A]),
    io:format("  so: flatten alone (D) buys ~.1f%%, the format change "
              "buys a further ~.1f%%~n",
              [100 * (A - D) / A, 100 * (D - B) / A]),
    ok.
