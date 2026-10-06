-module(micro_bench).

-export([run/0, t/2, t/3]).

-define(N, 200000).

t(Name, Fun) ->
    t(Name, Fun, ?N).

t(Name, Fun, N) ->
    %% warm up
    _ = Fun(),
    erlang:garbage_collect(),
    {Time, _} = timer:tc(fun () -> loop(Fun, N) end),
    io:format("~-52s ~10.3f us/op  (~b iters, ~b ms total)~n",
              [Name, Time / N, N, Time div 1000]),
    Time / N.

loop(_Fun, 0) -> ok;
loop(Fun, N) -> _ = Fun(), loop(Fun, N - 1).

run() ->
    io:format("~n=== ra_seq micro benchmarks ===~n"),
    bench_ra_seq(),
    io:format("~n=== ETS snapshot state lookup (per-append cost in WAL) ===~n"),
    bench_ets_smallest(),
    io:format("~n=== serialisation / checksum ===~n"),
    bench_ser(),
    io:format("~n=== segment index ===~n"),
    bench_segment(),
    ok.

%%% ------------------------------------------------------------------
bench_ra_seq() ->
    %% contiguous case: appending is O(1)
    Contig = [{1, 1000000}],
    t("ra_seq:append/2 contiguous", fun () -> ra_seq:append(1000001, Contig) end),
    t("ra_seq:first/1 contiguous", fun () -> ra_seq:first(Contig) end),
    t("ra_seq:last/1 contiguous", fun () -> ra_seq:last(Contig) end),
    t("ra_seq:floor/2 contiguous", fun () -> ra_seq:floor(500000, Contig) end),
    t("ra_seq:length/1 contiguous", fun () -> ra_seq:length(Contig) end),

    %% sparse case, e.g. live indexes of a queue with many unacked msgs
    Sparse10k = ra_seq:from_list([I * 3 || I <- lists:seq(1, 10000)]),
    io:format("  (sparse seq of 10k singleton entries)~n"),
    t("ra_seq:first/1 sparse-10k", fun () -> ra_seq:first(Sparse10k) end, 20000),
    t("ra_seq:length/1 sparse-10k", fun () -> ra_seq:length(Sparse10k) end, 20000),
    t("ra_seq:floor/2 sparse-10k", fun () -> ra_seq:floor(15000, Sparse10k) end, 20000),
    t("ra_seq:limit/2 sparse-10k", fun () -> ra_seq:limit(15000, Sparse10k) end, 20000),
    t("ra_seq:in_range/2 sparse-10k",
      fun () -> ra_seq:in_range({10000, 20000}, Sparse10k) end, 20000),
    t("ra_seq:has_overlap/2 sparse-10k",
      fun () -> ra_seq:has_overlap({10000, 20000}, Sparse10k) end, 20000),

    %% ra_seq:add - used per batch per writer in the WAL and in segment writer
    Add1 = [{1, 1}],
    Add64 = ra_seq:from_list(lists:seq(1, 64)),
    Add1024 = ra_seq:from_list(lists:seq(1, 1024)),
    Add8192 = ra_seq:from_list(lists:seq(1, 8192)),
    To = [{100000, 200000}],
    io:format("  (ra_seq:add/2 - called once per writer per WAL batch)~n"),
    t("ra_seq:add/2 add=1 entry", fun () -> ra_seq:add(Add1, To) end, 100000),
    t("ra_seq:add/2 add=64 entries", fun () -> ra_seq:add(Add64, To) end, 100000),
    t("ra_seq:add/2 add=1024 entries", fun () -> ra_seq:add(Add1024, To) end, 20000),
    t("ra_seq:add/2 add=8192 entries", fun () -> ra_seq:add(Add8192, To) end, 2000),
    %% big range add - what the segment writer does with live indexes
    BigAdd = [{1, 250000}],
    t("ra_seq:add/2 add=250k contiguous range",
      fun () -> ra_seq:add(BigAdd, [{250001, 250002}]) end, 20),
    ok.

bench_ets_smallest() ->
    T = ets:new(t, [set, {read_concurrency, true}, {write_concurrency, true},
                    public]),
    UId = <<"01234567890123456789">>,
    ok = ra_log_snapshot_state:insert(T, UId, 100, 101, []),
    t("ra_log_snapshot_state:smallest/2 (ets lookup_element)",
      fun () -> ra_log_snapshot_state:smallest(T, UId) end, 1000000),
    t("ets:lookup_element/4 raw",
      fun () -> ets:lookup_element(T, UId, 3, 0) end, 1000000),
    t("baseline: erlang:system_time(millisecond)",
      fun () -> erlang:system_time(millisecond) end, 1000000),
    ok.

bench_ser() ->
    Small = {enqueue, self(), 1, <<1:256/unit:8>>},   %% 256 byte payload
    Big = {enqueue, self(), 1, <<1:4096/unit:8>>},
    Structy = {some_cmd, #{a => 1, b => 2, c => [1, 2, 3, 4, 5]},
               [{k, v} || _ <- lists:seq(1, 20)]},
    t("term_to_iovec/1 256B payload", fun () -> term_to_iovec(Small) end, 500000),
    t("term_to_iovec/1 4KB payload", fun () -> term_to_iovec(Big) end, 500000),
    t("term_to_iovec/1 structy term", fun () -> term_to_iovec(Structy) end, 500000),
    SmallIo = term_to_iovec(Small),
    BigIo = term_to_iovec(Big),
    t("iolist_size/1 on 256B iovec", fun () -> iolist_size(SmallIo) end, 1000000),
    Entry = [<<1:64, 1:64>> | SmallIo],
    EntryBig = [<<1:64, 1:64>> | BigIo],
    t("adler32/1 256B entry", fun () -> erlang:adler32(Entry) end, 500000),
    t("adler32/1 4KB entry", fun () -> erlang:adler32(EntryBig) end, 200000),
    t("crc32/1 256B entry", fun () -> erlang:crc32(Entry) end, 500000),
    t("crc32/1 4KB entry", fun () -> erlang:crc32(EntryBig) end, 200000),
    Bin = iolist_to_binary(SmallIo),
    t("binary_to_term/1 256B", fun () -> binary_to_term(Bin) end, 500000),
    ok.

bench_segment() ->
    Dir = "/tmp/ra_micro_bench_seg",
    _ = os:cmd("rm -rf " ++ Dir),
    ok = filelib:ensure_dir(filename:join(Dir, "x")),
    Fn = filename:join(Dir, "0001.segment"),
    {ok, S0} = ra_log_segment:open(Fn, #{max_count => 4096}),
    Data = term_to_iovec({enqueue, self(), 1, <<1:256/unit:8>>}),
    Sz = iolist_size(Data),
    S = lists:foldl(fun (I, Acc) ->
                            {ok, A} = ra_log_segment:append(Acc, I, 1, {Sz, Data}),
                            A
                    end, S0, lists:seq(1, 4096)),
    ok = ra_log_segment:close(S),
    io:format("  (segment with 4096 entries, ~b bytes each)~n", [Sz]),
    t("ra_log_segment:open/2 mode=read (map index)",
      fun () ->
              {ok, R} = ra_log_segment:open(Fn, #{mode => read}),
              ok = ra_log_segment:close(R)
      end, 2000),
    t("ra_log_segment:open/2 mode=read index_mode=binary",
      fun () ->
              {ok, R} = ra_log_segment:open(Fn, #{mode => read,
                                                  index_mode => binary}),
              ok = ra_log_segment:close(R)
      end, 2000),
    t("ra_log_segment:info/1",
      fun () -> ra_log_segment:info(Fn) end, 2000),
    {ok, R} = ra_log_segment:open(Fn, #{mode => read}),
    t("ra_log_segment:read_sparse/4 (32 idx, incl is_modified stat)",
      fun () ->
              {ok, _, _} = ra_log_segment:read_sparse(
                             R, lists:seq(100, 131),
                             fun (_, _, _, A) -> A end, [])
      end, 20000),
    t("ra_log_segment:read_sparse_no_checks/4 (32 idx)",
      fun () ->
              {ok, _, _} = ra_log_segment:read_sparse_no_checks(
                             R, lists:seq(100, 131),
                             fun (_, _, _, A) -> A end, [])
      end, 20000),
    t("is_modified only (prim_file:read_handle_info)",
      fun () ->
              {ok, _, _} = ra_log_segment:read_sparse(
                             R, [100], fun (_, _, _, A) -> A end, [])
      end, 20000),
    t("read_sparse_no_checks 1 idx",
      fun () ->
              {ok, _, _} = ra_log_segment:read_sparse_no_checks(
                             R, [100], fun (_, _, _, A) -> A end, [])
      end, 20000),
    ok = ra_log_segment:close(R),
    _ = os:cmd("rm -rf " ++ Dir),
    ok.
