-module(segw_bench).

%% Benchmarks the per-writer, per-WAL-file work the segment writer does.

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
    Dir = "/tmp/ra_segw_bench",
    _ = os:cmd("rm -rf " ++ Dir),
    ok = filelib:ensure_dir(filename:join(Dir, "x")),
    io:format("~n=== per-writer fixed costs in the segment writer ===~n"),
    fixed_costs(Dir),
    io:format("~n=== per-entry flush cost (mem table -> segment) ===~n"),
    flush_costs(Dir),
    _ = os:cmd("rm -rf " ++ Dir),
    ok.

fixed_costs(Base) ->
    [begin
         Dir = filename:join(Base, io_lib:format("d~b", [NumSegs])),
         ok = filelib:ensure_dir(filename:join(Dir, "x")),
         [ok = file:write_file(
                 filename:join(Dir,
                               ra_lib:zpad_filename("", "segment", I)), <<>>)
          || I <- lists:seq(1, NumSegs)],
         io:format("-- directory with ~b segment files~n", [NumSegs]),
         t("prim_file:list_dir + filter + sort (find_segment_files)",
           fun () ->
                   {ok, Fs} = prim_file:list_dir(Dir),
                   lists:reverse(
                     lists:sort([filename:join(Dir, F) || F <- Fs,
                                 filename:extension(F) =:= ".segment"]))
           end, 2000),
         t("ra_lib:sync_dir/1 (once per writer per WAL file today)",
           fun () -> ra_lib:sync_dir(Dir) end, 2000)
     end || NumSegs <- [4, 64, 512]],
    ok.

flush_costs(Base) ->
    Dir = filename:join(Base, "flush"),
    ok = filelib:ensure_dir(filename:join(Dir, "x")),
    Num = 4096,
    Tid = ets:new(mt, [set, public]),
    Cmd = {enqueue, self(), 1, crypto:strong_rand_bytes(256)},
    [true = ets:insert(Tid, {I, 1, Cmd}) || I <- lists:seq(1, Num)],
    Seq = ra_seq:from_list(lists:seq(1, Num)),

    t("ets:lookup/2 per index (4096 entries)",
      fun () ->
              lists:foreach(fun (I) -> [_] = ets:lookup(Tid, I) end,
                            lists:seq(1, Num))
      end, 200),
    t("term_to_iovec + iolist_size per entry (4096 entries)",
      fun () ->
              lists:foreach(fun (I) ->
                                    [{_, _, D}] = ets:lookup(Tid, I),
                                    B = term_to_iovec(D),
                                    _ = iolist_size(B)
                            end, lists:seq(1, Num))
      end, 200),
    t("ets:select whole table (batched alternative)",
      fun () -> _ = ets:select(Tid, [{{'$1', '$2', '$3'}, [], ['$_']}]) end,
      200),
    io:format("~n"),
    t("full append_to_segment-equivalent, 4096 entries -> segment",
      fun () ->
              Fn = filename:join(Dir, "x.segment"),
              _ = prim_file:delete(Fn),
              {ok, S0} = ra_log_segment:open(Fn, #{max_count => Num}),
              S = ra_seq:fold(fun (I, Acc) ->
                                      [{_, T, D}] = ets:lookup(Tid, I),
                                      B = term_to_iovec(D),
                                      Sz = iolist_size(B),
                                      {ok, A} = ra_log_segment:append(Acc, I, T,
                                                                      {Sz, B}),
                                      A
                              end, S0, Seq),
              ok = ra_log_segment:close(S)
      end, 100),
    t("same but data already serialised in the mem table",
      fun () ->
              Fn = filename:join(Dir, "y.segment"),
              _ = prim_file:delete(Fn),
              {ok, S0} = ra_log_segment:open(Fn, #{max_count => Num}),
              Pre = term_to_iovec(Cmd),
              PreSz = iolist_size(Pre),
              S = ra_seq:fold(fun (I, Acc) ->
                                      {ok, A} = ra_log_segment:append(Acc, I, 1,
                                                                      {PreSz,
                                                                       Pre}),
                                      A
                              end, S0, Seq),
              ok = ra_log_segment:close(S)
      end, 100),
    t("same, compute_checksums => false",
      fun () ->
              Fn = filename:join(Dir, "z.segment"),
              _ = prim_file:delete(Fn),
              {ok, S0} = ra_log_segment:open(Fn, #{max_count => Num,
                                                   compute_checksums => false}),
              S = ra_seq:fold(fun (I, Acc) ->
                                      [{_, T, D}] = ets:lookup(Tid, I),
                                      B = term_to_iovec(D),
                                      Sz = iolist_size(B),
                                      {ok, A} = ra_log_segment:append(Acc, I, T,
                                                                      {Sz, B}),
                                      A
                              end, S0, Seq),
              ok = ra_log_segment:close(S)
      end, 100),
    ok.
