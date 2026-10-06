-module(seg_bench).

-export([run/0]).

-define(HEADER_SIZE, 8).
-define(REC, 32).

t(Name, Fun, N) ->
    _ = Fun(),
    erlang:garbage_collect(),
    {Time, _} = timer:tc(fun () -> loop(Fun, N) end),
    io:format("~-56s ~10.3f us/op~n", [Name, Time / N]),
    Time / N.

loop(_Fun, 0) -> ok;
loop(Fun, N) -> _ = Fun(), loop(Fun, N - 1).

run() ->
    Dir = "/tmp/ra_seg_bench",
    _ = os:cmd("rm -rf " ++ Dir),
    ok = filelib:ensure_dir(filename:join(Dir, "x")),
    [bench(Dir, MaxCount) || MaxCount <- [4096, 1024, 256]],
    _ = os:cmd("rm -rf " ++ Dir),
    ok.

bench(Dir, MaxCount) ->
    io:format("~n--- segment max_count=~b (index region = ~b bytes) ---~n",
              [MaxCount, MaxCount * ?REC]),
    Fn = filename:join(Dir, io_lib:format("seg_~b.segment", [MaxCount])),
    _ = file:delete(Fn),
    {ok, S0} = ra_log_segment:open(Fn, #{max_count => MaxCount}),
    Data = term_to_iovec({enqueue, self(), 1, <<1:256/unit:8>>}),
    Sz = iolist_size(Data),
    S = lists:foldl(fun (I, Acc) ->
                            {ok, A} = ra_log_segment:append(Acc, I, 1, {Sz, Data}),
                            A
                    end, S0, lists:seq(1, MaxCount)),
    ok = ra_log_segment:close(S),

    IndexSize = MaxCount * ?REC,
    t("file:open+close (raw read)",
      fun () ->
              {ok, Fd} = file:open(Fn, [read, raw, binary]),
              _ = file:close(Fd)
      end, 5000),
    {ok, Fd} = file:open(Fn, [read, raw, binary]),
    t("pread whole index region",
      fun () -> {ok, _} = file:pread(Fd, ?HEADER_SIZE, IndexSize) end, 5000),
    {ok, IdxBin} = file:pread(Fd, ?HEADER_SIZE, IndexSize),
    _ = file:close(Fd),
    io:format("  index bin actually read: ~b bytes~n", [byte_size(IdxBin)]),
    t("scan_index_binary equivalent (no map)",
      fun () -> scan(IdxBin, 0, 0, 0, undefined) end, 5000),
    t("parse into map (as recover_index does)",
      fun () -> parse(IdxBin, 0, 0, 0, undefined, #{}) end, 2000),
    t("parse into map via maps:from_list",
      fun () -> maps:from_list(parse_l(IdxBin, 0, [])) end, 2000),
    t("full open mode=read map index",
      fun () ->
              {ok, R} = ra_log_segment:open(Fn, #{mode => read}),
              ok = ra_log_segment:close(R)
      end, 2000),
    t("full open mode=read binary index",
      fun () ->
              {ok, R} = ra_log_segment:open(Fn, #{mode => read,
                                                  index_mode => binary}),
              ok = ra_log_segment:close(R)
      end, 2000),
    t("full open mode=append (recover_index -> map)",
      fun () ->
              {ok, R} = ra_log_segment:open(Fn, #{mode => append,
                                                  max_count => MaxCount}),
              ok = ra_log_segment:close(R)
      end, 500),
    ok.

%% mimic scan_index_binary_loop
scan(Bin, Offset, Num, LastIdx, Range) ->
    case dec(Bin, Offset) of
        eof ->
            {Num, Range};
        {ok, {Idx, _T, O, L, _C}} ->
            case Idx < LastIdx of
                true -> {Num + 1, Range};
                false -> scan(Bin, Offset + ?REC, Num + 1, Idx,
                              upd(Range, Idx, O, L))
            end
    end.

parse(Bin, Offset, Num, LastIdx, Range, Index) ->
    case dec(Bin, Offset) of
        eof -> {Num, Range, Index};
        {ok, {Idx, T, O, L, C}} ->
            Index1 = case Idx < LastIdx of
                         true -> maps:filter(fun (K, _) -> K =< Idx end, Index);
                         false -> Index
                     end,
            parse(Bin, Offset + ?REC, Num + 1, Idx, upd(Range, Idx, O, L),
                  Index1#{Idx => {T, O, L, C}})
    end.

parse_l(Bin, Offset, Acc) ->
    case dec(Bin, Offset) of
        eof -> Acc;
        {ok, {Idx, T, O, L, C}} ->
            parse_l(Bin, Offset + ?REC, [{Idx, {T, O, L, C}} | Acc])
    end.

upd(undefined, Idx, _, _) -> {Idx, Idx};
upd({F, _}, Idx, _, _) -> {min(F, Idx), Idx}.

dec(Bin, Offset) when byte_size(Bin) >= Offset + ?REC ->
    case Bin of
        <<_:Offset/binary, 0:64/unsigned, 0:64/unsigned, 0:64/unsigned,
          0:32/unsigned, 0:32/integer, _/binary>> ->
            eof;
        <<_:Offset/binary, Idx:64/unsigned, Term:64/unsigned,
          DataOffset:64/unsigned, Length:32/unsigned,
          Crc:32/integer, _/binary>> ->
            {ok, {Idx, Term, DataOffset, Length, Crc}}
    end;
dec(_, _) ->
    eof.
