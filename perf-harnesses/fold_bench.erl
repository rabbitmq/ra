-module(fold_bench).

-export([run/0]).

t(Name, Fun, N) ->
    _ = Fun(),
    erlang:garbage_collect(),
    {Time, _} = timer:tc(fun () -> loop(Fun, N) end),
    io:format("~-58s ~12.1f us/op~n", [Name, Time / N]),
    Time / N.

loop(_Fun, 0) -> ok;
loop(Fun, N) -> _ = Fun(), loop(Fun, N - 1).

run() ->
    Dir = "/tmp/ra_fold_bench",
    _ = os:cmd("rm -rf " ++ Dir),
    ok = filelib:ensure_dir(filename:join(Dir, "x")),
    Fn = filename:join(Dir, "0001.segment"),
    {ok, S0} = ra_log_segment:open(Fn, #{max_count => 4096}),
    Data = term_to_iovec({enqueue, self(), 1, crypto:strong_rand_bytes(256)}),
    Sz = iolist_size(Data),
    S = lists:foldl(fun (I, Acc) ->
                            {ok, A} = ra_log_segment:append(Acc, I, 1, {Sz, Data}),
                            A
                    end, S0, lists:seq(1, 4096)),
    ok = ra_log_segment:close(S),

    io:format("~n=== ra_log_segment:fold over 4096 entries ===~n"),
    {ok, RSeq} = ra_log_segment:open(Fn, #{mode => read,
                                           access_pattern => sequential}),
    {ok, RRnd} = ra_log_segment:open(Fn, #{mode => read,
                                           access_pattern => random}),
    F = fun (_) -> ok end,
    A = fun (_, Acc) -> Acc end,
    t("fold 4096 entries, access_pattern = sequential",
      fun () -> ok = ra_log_segment:fold(RSeq, 1, 4096, F, A, ok) end, 200),
    t("fold 4096 entries, access_pattern = random (1 pread/entry)",
      fun () -> ok = ra_log_segment:fold(RRnd, 1, 4096, F, A, ok) end, 200),
    t("fold 256 entries, sequential",
      fun () -> ok = ra_log_segment:fold(RSeq, 1, 256, F, A, ok) end, 2000),
    t("fold 256 entries, random",
      fun () -> ok = ra_log_segment:fold(RRnd, 1, 256, F, A, ok) end, 2000),
    ok = ra_log_segment:close(RSeq),
    ok = ra_log_segment:close(RRnd),

    io:format("~n=== memory of an open read segment ===~n"),
    mem(Fn, map),
    mem(Fn, binary),
    _ = os:cmd("rm -rf " ++ Dir),
    ok.

mem(Fn, Mode) ->
    Parent = self(),
    P = spawn(fun () ->
                      {ok, R} = ra_log_segment:open(Fn, #{mode => read,
                                                          index_mode => Mode}),
                      erlang:garbage_collect(),
                      {memory, M} = process_info(self(), memory),
                      Parent ! {mem, M},
                      receive stop -> ok end,
                      _ = ra_log_segment:close(R)
              end),
    Base = spawn(fun () ->
                         erlang:garbage_collect(),
                         {memory, M} = process_info(self(), memory),
                         Parent ! {base, M},
                         receive stop -> ok end
                 end),
    receive {mem, M1} -> ok end,
    receive {base, M0} -> ok end,
    io:format("index_mode = ~-8w  process memory ~b bytes (baseline ~b)~n",
              [Mode, M1, M0]),
    P ! stop, Base ! stop,
    ok.
