-module(rec_bench).

%% Compares the current WAL record shape (nested iolist + adler32 over an
%% iolist) with a per-record flattened binary shape.

-export([run/0]).

t(Name, Fun, N) ->
    _ = Fun(),
    erlang:garbage_collect(),
    {Time, _} = timer:tc(fun () -> loop(Fun, N) end),
    io:format("~-58s ~10.1f us/batch ~8.3f us/entry~n",
              [Name, Time / N, (Time / N) / get(bs)]),
    Time / N.

loop(_Fun, 0) -> ok;
loop(Fun, N) -> _ = Fun(), loop(Fun, N - 1).

run() ->
    Dir = "/tmp/ra_rec_bench",
    _ = os:cmd("rm -rf " ++ Dir),
    ok = filelib:ensure_dir(filename:join(Dir, "x")),
    [bench(Dir, DS, BS) || DS <- [64, 256, 4096], BS <- [1024]],
    _ = os:cmd("rm -rf " ++ Dir),
    ok.

bench(Dir, DataSize, BatchSize) ->
    put(bs, BatchSize),
    io:format("~n-- payload ~b bytes, batch ~b records~n", [DataSize, BatchSize]),
    Data = crypto:strong_rand_bytes(DataSize),
    Cmd = {enqueue, self(), 1, Data},
    Bin = term_to_iovec(Cmd),
    Hdr = <<1:1, 1:1, 1:22>>,
    File = filename:join(Dir, "w"),
    {ok, Fd} = file:open(File, [raw, write, binary]),

    t("current: nested iolist + adler32(iolist) + file:write",
      fun () ->
              Pend = lists:foldl(
                       fun (I, Acc) ->
                               Entry = [<<I:64, 1:64>> | Bin],
                               C = erlang:adler32(Entry),
                               L = iolist_size(Bin),
                               [Acc, Hdr, <<C:32, L:32>> | Entry]
                       end, [], lists:seq(1, BatchSize)),
              ok = file:write(Fd, Pend),
              {ok, 0} = file:position(Fd, 0)
      end, 300),

    t("per-record binary + adler32(binary) + file:write(list)",
      fun () ->
              Pend = lists:foldl(
                       fun (I, Acc) ->
                               EB = iolist_to_binary([<<I:64, 1:64>> | Bin]),
                               C = erlang:adler32(EB),
                               L = byte_size(EB) - 16,
                               [<<Hdr/bitstring, C:32, L:32, EB/binary>> | Acc]
                       end, [], lists:seq(1, BatchSize)),
              ok = file:write(Fd, lists:reverse(Pend)),
              {ok, 0} = file:position(Fd, 0)
      end, 300),

    t("per-record binary, reversed_batch aware (no lists:reverse)",
      fun () ->
              Pend = lists:foldl(
                       fun (I, Acc) ->
                               EB = iolist_to_binary([<<I:64, 1:64>> | Bin]),
                               C = erlang:adler32(EB),
                               L = byte_size(EB) - 16,
                               [Acc | <<Hdr/bitstring, C:32, L:32, EB/binary>>]
                       end, [], lists:seq(1, BatchSize)),
              ok = file:write(Fd, Pend),
              {ok, 0} = file:position(Fd, 0)
      end, 300),

    t("no checksum: nested iolist + file:write",
      fun () ->
              Pend = lists:foldl(
                       fun (I, Acc) ->
                               Entry = [<<I:64, 1:64>> | Bin],
                               L = iolist_size(Bin),
                               [Acc, Hdr, <<0:32, L:32>> | Entry]
                       end, [], lists:seq(1, BatchSize)),
              ok = file:write(Fd, Pend),
              {ok, 0} = file:position(Fd, 0)
      end, 300),
    _ = file:close(Fd),
    ok.
