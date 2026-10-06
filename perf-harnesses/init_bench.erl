-module(init_bench).

-export([run/0]).

-include_lib("kernel/include/file.hrl").

-define(HEADER_SIZE, 8).
-define(REC, 32).

t(Name, Fun, N) ->
    _ = Fun(),
    erlang:garbage_collect(),
    {Time, _} = timer:tc(fun () -> loop(Fun, N) end),
    io:format("~-58s ~12.1f us/op~n", [Name, Time / N]),
    Time / N.

loop(_Fun, 0) -> ok;
loop(Fun, N) -> _ = Fun(), loop(Fun, N - 1).

run() ->
    Dir = "/tmp/ra_init_bench",
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

    io:format("~n=== ra_log:init -> my_segrefs cost per segment file ===~n"),
    t("ra_log_segment:info/1 (what my_segrefs calls today)",
      fun () -> #{ref := _} = ra_log_segment:info(Fn) end, 2000),
    t("range-only variant (no index list / ra_seq build)",
      fun () -> {_, _} = range_only(Fn) end, 2000),
    t("ra_log_segment:segref/1 (open in read mode + segref)",
      fun () -> _ = ra_log_segment:segref(Fn) end, 1000),
    t("ra_log_segment:segref_info/1 (finding 11 fix)",
      fun () -> #{ref := _} = ra_log_segment:segref_info(Fn) end, 2000),

    io:format("~n=== ra_seq:in/2 inside parse_index_info (major compaction) ===~n"),
    %% live seq covering half the segment, high -> low
    Live = ra_seq:from_list([I || I <- lists:seq(1, 4096), I rem 2 == 0]),
    io:format("live seq runs: ~b, length ~b~n",
              [length(Live), ra_seq:length(Live)]),
    t("ra_log_segment:info/2 with live seq (per segment, compaction)",
      fun () -> #{live_size := _} = ra_log_segment:info(Fn, Live) end, 500),
    t("ra_log_segment:info/2 with undefined live seq",
      fun () -> #{live_size := _} = ra_log_segment:info(Fn, undefined) end, 500),
    SparseLive = ra_seq:from_list([I || I <- lists:seq(1, 4096),
                                       I rem 2 == 0, I rem 6 =/= 0]),
    io:format("sparse live seq runs: ~b~n", [length(SparseLive)]),
    t("ra_log_segment:info/2 with 1365-run live seq",
      fun () -> #{live_size := _} = ra_log_segment:info(Fn, SparseLive) end, 200),

    io:format("~n=== memory: open read segment, both index modes ===~n"),
    mem(Fn, map),
    mem(Fn, binary),
    _ = os:cmd("rm -rf " ++ Dir),
    ok.

range_only(Filename) ->
    {ok, #file_info{type = _Type}} =
        prim_file:read_link_info(Filename, [raw, {time, posix}]),
    {ok, Fd} = file:open(Filename, [read, raw, binary]),
    try
        {ok, <<"RASG", _V:16/unsigned, MaxCount:16/unsigned>>} =
            file:pread(Fd, 0, ?HEADER_SIZE),
        {ok, Bin} = file:pread(Fd, ?HEADER_SIZE, MaxCount * ?REC),
        scan(Bin, undefined, 0)
    after
        _ = file:close(Fd)
    end.

scan(<<0:64, _/binary>>, Range, DataOffset) ->
    {Range, DataOffset};
scan(<<Idx:64/unsigned, _T:64/unsigned, O:64/unsigned, L:32/unsigned,
       _C:32/integer, Rest/binary>>, Range, _) ->
    scan(Rest, upd(Range, Idx), O + L);
scan(_, Range, DataOffset) ->
    {Range, DataOffset}.

upd(undefined, Idx) -> {Idx, Idx};
upd({F, _}, Idx) -> {min(F, Idx), Idx}.

mem(Fn, Mode) ->
    Parent = self(),
    P = spawn(fun () ->
                      {ok, R} = ra_log_segment:open(Fn, #{mode => read,
                                                          index_mode => Mode}),
                      erlang:garbage_collect(),
                      {memory, M} = process_info(self(), memory),
                      {binary, Bins} = process_info(self(), binary),
                      BinBytes = lists:sum([Sz || {_, Sz, _} <- Bins]),
                      Parent ! {mem, M, BinBytes},
                      receive stop -> ok end,
                      _ = ra_log_segment:close(R)
              end),
    receive {mem, M1, B1} -> ok end,
    io:format("index_mode=~-8w heap ~8b bytes, refc binaries ~8b bytes, "
              "total ~8b~n", [Mode, M1, B1, M1 + B1]),
    P ! stop,
    ok.
