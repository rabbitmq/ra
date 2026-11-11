-module(seg_bench2).

-export([run/0]).

-define(HEADER_SIZE, 8).
-define(REC, 32).

t(Name, Fun, N) ->
    _ = Fun(),
    erlang:garbage_collect(),
    {Time, _} = timer:tc(fun () -> loop(Fun, N) end),
    io:format("~-58s ~10.3f us/op~n", [Name, Time / N]),
    Time / N.

loop(_Fun, 0) -> ok;
loop(Fun, N) -> _ = Fun(), loop(Fun, N - 1).

run() ->
    Dir = "/tmp/ra_seg_bench2",
    _ = os:cmd("rm -rf " ++ Dir),
    ok = filelib:ensure_dir(filename:join(Dir, "x")),
    MaxCount = 4096,
    Fn = filename:join(Dir, "seg.segment"),
    {ok, S0} = ra_log_segment:open(Fn, #{max_count => MaxCount}),
    Data = term_to_iovec({enqueue, self(), 1, <<1:256/unit:8>>}),
    Sz = iolist_size(Data),
    S = lists:foldl(fun (I, Acc) ->
                            {ok, A} = ra_log_segment:append(Acc, I, 1, {Sz, Data}),
                            A
                    end, S0, lists:seq(1, MaxCount)),
    ok = ra_log_segment:close(S),

    io:format("~n--- read costs: map index vs binary index (4096 entry seg) ---~n"),
    {ok, RMap} = ra_log_segment:open(Fn, #{mode => read}),
    {ok, RBin} = ra_log_segment:open(Fn, #{mode => read, index_mode => binary}),
    AccFun = fun (_, _, _, A) -> A end,
    [begin
         Idxs = lists:seq(1000, 1000 + Num - 1),
         t(io_lib:format("map    read_sparse_no_checks ~b consecutive idx", [Num]),
           fun () ->
                   {ok, _, _} = ra_log_segment:read_sparse_no_checks(RMap, Idxs,
                                                                     AccFun, [])
           end, 20000),
         t(io_lib:format("binary read_sparse_no_checks ~b consecutive idx", [Num]),
           fun () ->
                   {ok, _, _} = ra_log_segment:read_sparse_no_checks(RBin, Idxs,
                                                                     AccFun, [])
           end, 20000)
     end || Num <- [1, 8, 64]],
    t("map    term_query", fun () -> ra_log_segment:term_query(RMap, 2000) end,
      200000),
    t("binary term_query", fun () -> ra_log_segment:term_query(RBin, 2000) end,
      200000),
    ok = ra_log_segment:close(RMap),
    ok = ra_log_segment:close(RBin),

    io:format("~n--- index parse strategies (4096 recs, 131072 bytes) ---~n"),
    {ok, Fd} = file:open(Fn, [read, raw, binary]),
    {ok, IdxBin} = file:pread(Fd, ?HEADER_SIZE, MaxCount * ?REC),
    _ = file:close(Fd),
    t("current: offset-skip decode + incremental map",
      fun () -> parse_cur(IdxBin, 0, 0, 0, undefined, #{}) end, 2000),
    t("opt: tail-binary decode + incremental map",
      fun () -> parse_tail(IdxBin, 0, undefined, #{}) end, 2000),
    t("opt: tail-binary decode + reverse list + maps:from_list",
      fun () -> parse_tail_l(IdxBin, [], undefined) end, 2000),
    t("opt: tail-binary scan only (range+count, no map)",
      fun () -> scan_tail(IdxBin, 0, 0, undefined) end, 5000),
    _ = os:cmd("rm -rf " ++ Dir),
    ok.

%% ---- current implementation shape
parse_cur(Bin, Offset, Num, LastIdx, Range, Index) ->
    case dec_offset(Bin, Offset) of
        eof -> {Num, Range, Index};
        {ok, {Idx, T, O, L, C}} ->
            Index1 = case Idx < LastIdx of
                         true -> maps:filter(fun (K, _) -> K =< Idx end, Index);
                         false -> Index
                     end,
            parse_cur(Bin, Offset + ?REC, Num + 1, Idx, upd(Range, Idx),
                      Index1#{Idx => {T, O, L, C}})
    end.

dec_offset(Bin, Offset) when byte_size(Bin) >= Offset + ?REC ->
    case Bin of
        <<_:Offset/binary, 0:64/unsigned, 0:64/unsigned, 0:64/unsigned,
          0:32/unsigned, 0:32/integer, _/binary>> ->
            eof;
        <<_:Offset/binary, Idx:64/unsigned, Term:64/unsigned,
          DataOffset:64/unsigned, Length:32/unsigned,
          Crc:32/integer, _/binary>> ->
            {ok, {Idx, Term, DataOffset, Length, Crc}}
    end;
dec_offset(_, _) ->
    eof.

%% ---- optimised: single match, walk the tail
parse_tail(<<0:64, _/binary>>, Num, Range, Index) ->
    {Num, Range, Index};
parse_tail(<<Idx:64/unsigned, T:64/unsigned, O:64/unsigned, L:32/unsigned,
             C:32/integer, Rest/binary>>, Num, Range, Index) ->
    parse_tail(Rest, Num + 1, upd(Range, Idx), Index#{Idx => {T, O, L, C}});
parse_tail(_, Num, Range, Index) ->
    {Num, Range, Index}.

parse_tail_l(<<0:64, _/binary>>, Acc, Range) ->
    {length(Acc), Range, maps:from_list(Acc)};
parse_tail_l(<<Idx:64/unsigned, T:64/unsigned, O:64/unsigned, L:32/unsigned,
               C:32/integer, Rest/binary>>, Acc, Range) ->
    parse_tail_l(Rest, [{Idx, {T, O, L, C}} | Acc], upd(Range, Idx));
parse_tail_l(_, Acc, Range) ->
    {length(Acc), Range, maps:from_list(Acc)}.

scan_tail(<<0:64, _/binary>>, Num, _LastOff, Range) ->
    {Num, Range};
scan_tail(<<Idx:64/unsigned, _T:64/unsigned, O:64/unsigned, L:32/unsigned,
            _C:32/integer, Rest/binary>>, Num, _, Range) ->
    scan_tail(Rest, Num + 1, O + L, upd(Range, Idx));
scan_tail(_, Num, LastOff, Range) ->
    {Num, LastOff, Range}.

upd(undefined, Idx) -> {Idx, Idx};
upd({F, _}, Idx) -> {min(F, Idx), Idx}.
