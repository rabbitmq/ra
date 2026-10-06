-module(snap_bench).

-export([run/0]).

t(Name, Fun, N) ->
    _ = Fun(),
    erlang:garbage_collect(),
    {Time, _} = timer:tc(fun () -> loop(Fun, N) end),
    io:format("~-52s ~12.1f us/op~n", [Name, Time / N]),
    Time / N.

loop(_Fun, 0) -> ok;
loop(Fun, N) -> _ = Fun(), loop(Fun, N - 1).

run() ->
    Dir0 = "/tmp/ra_snap_bench",
    _ = os:cmd("rm -rf " ++ Dir0),
    io:format("~n=== snapshot validate/read_meta cost (per server at startup) ===~n"),
    [bench(Dir0, N) || N <- [1000, 100000, 1000000]],
    _ = os:cmd("rm -rf " ++ Dir0),
    ok.

bench(Dir0, NumKeys) ->
    Dir = filename:join(Dir0, integer_to_list(NumKeys)),
    ok = filelib:ensure_dir(filename:join(Dir, "x")),
    %% a machine state roughly like a quorum queue index: a map of
    %% NumKeys => {Idx, small tuple}
    MacState = maps:from_list([{I, {I, <<I:64>>, some_atom}}
                               || I <- lists:seq(1, NumKeys)]),
    Meta = #{index => NumKeys, term => 1, cluster => #{},
             machine_version => 1},
    {ok, Bytes} = ra_log_snapshot:write(Dir, Meta, MacState, true),
    io:format("~n-- machine state with ~b keys, snapshot file ~.1f MB~n",
              [NumKeys, Bytes / 1048576]),
    Iters = case NumKeys of
                1000 -> 2000;
                100000 -> 100;
                _ -> 10
            end,
    t("ra_log_snapshot:read_meta/1", fun () ->
                                             {ok, _} = ra_log_snapshot:read_meta(Dir)
                                     end, Iters),
    t("ra_log_snapshot:validate/1 (full recover+b2t)",
      fun () -> ok = ra_log_snapshot:validate(Dir) end, Iters),
    t("crc-only validate (read_file + crc32, no b2t)",
      fun () -> ok = crc_only_validate(Dir) end, Iters),
    t("chunked crc validate (64KB chunks, no full read)",
      fun () -> ok = chunked_validate(Dir) end, Iters),
    ok.

crc_only_validate(Dir) ->
    File = filename:join(Dir, "snapshot.dat"),
    case prim_file:read_file(File) of
        {ok, <<"RASN", 1:8/unsigned, Crc:32/integer, Data/binary>>} ->
            case erlang:crc32(Data) of
                Crc -> ok;
                _ -> {error, checksum_error}
            end
    end.

chunked_validate(Dir) ->
    File = filename:join(Dir, "snapshot.dat"),
    {ok, Fd} = file:open(File, [read, raw, binary, {read_ahead, 65536}]),
    try
        {ok, <<"RASN", 1:8/unsigned, Crc:32/integer>>} = file:read(Fd, 9),
        crc_loop(Fd, erlang:crc32(<<>>), Crc)
    after
        _ = file:close(Fd)
    end.

crc_loop(Fd, Acc, Expect) ->
    case file:read(Fd, 65536) of
        {ok, Bin} ->
            crc_loop(Fd, erlang:crc32(Acc, Bin), Expect);
        eof when Acc == Expect ->
            ok;
        eof ->
            {error, checksum_error}
    end.
