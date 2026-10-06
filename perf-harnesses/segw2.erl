-module(segw2).

%% Finding 8: the two per-writer fixed costs in the segment writer flush,
%% measured against directory size.
%%   - find_segment_files/1: prim_file:list_dir + filter + sort
%%   - a cache hit instead: one stat, via ra_log_segment:open must_exist
%%   - ra_lib:sync_dir/1, which today runs even for a pure append

-export([run/0]).

t(Name, Fun, N) ->
    _ = Fun(),
    erlang:garbage_collect(),
    {Time, _} = timer:tc(fun () -> loop(Fun, N) end),
    io:format("  ~-46s ~9.1f us~n", [Name, Time / N]),
    Time / N.

loop(_Fun, 0) -> ok;
loop(Fun, N) -> _ = Fun(), loop(Fun, N - 1).

run() ->
    Base = "/tmp/ra_segw2",
    _ = os:cmd("rm -rf " ++ Base),
    io:format("~n=== per-writer fixed costs vs segment count ===~n"),
    [bench(Base, N) || N <- [1, 4, 64, 512, 2048]],
    _ = os:cmd("rm -rf " ++ Base),
    ok.

bench(Base, NumSegs) ->
    Dir = filename:join(Base, integer_to_list(NumSegs)),
    ok = filelib:ensure_dir(filename:join(Dir, "x")),
    %% real segment files so that open/2 works on the newest
    [begin
         Fn = filename:join(Dir, ra_lib:zpad_filename("", "segment", I)),
         {ok, S0} = ra_log_segment:open(Fn, #{max_count => 8}),
         {ok, S} = ra_log_segment:append(S0, I, 1, <<"x">>),
         ok = ra_log_segment:close(S)
     end || I <- lists:seq(1, NumSegs)],
    Newest = filename:join(Dir, ra_lib:zpad_filename("", "segment", NumSegs)),
    catch ets:delete(cache),
    _ = ets:new(cache, [set, public, named_table]),
    true = ets:insert(cache, {k, Newest}),
    io:format("~n-- ~b segment files --~n", [NumSegs]),

    Scan = t("scan: list_dir + filter + sort (current)",
             fun () ->
                     {ok, Fs} = prim_file:list_dir(Dir),
                     lists:reverse(
                       lists:sort([filename:join(Dir, F) || F <- Fs,
                                   filename:extension(F) =:= ".segment"]))
             end, 2000),
    Hit = t("cache hit: ets lookup + stat",
            fun () ->
                    [{_, F}] = ets:lookup(cache, k),
                    {ok, _} = prim_file:read_file_info(F)
            end, 2000),
    io:format("     scan/hit ratio ~.1fx~n", [Scan / Hit]),

    %% and the full open as the flush actually does it
    ScanOpen = t("scan + open(append)",
                 fun () ->
                         {ok, Fs} = prim_file:list_dir(Dir),
                         [F | _] = lists:reverse(
                                     lists:sort(
                                       [filename:join(Dir, X) || X <- Fs,
                                        filename:extension(X) =:= ".segment"])),
                         {ok, S} = ra_log_segment:open(F, #{mode => append}),
                         ok = ra_log_segment:close(S)
                 end, 500),
    HitOpen = t("cache hit + open(append, must_exist)",
                fun () ->
                        [{_, F}] = ets:lookup(cache, k),
                        {ok, S} = ra_log_segment:open(F,
                                                      #{mode => append,
                                                        must_exist => true}),
                        ok = ra_log_segment:close(S)
                end, 500),
    io:format("     full open scan/cached ~.1fx~n", [ScanOpen / HitOpen]),
    Sync = t("ra_lib:sync_dir/1 (skipped for appends now)",
             fun () -> ra_lib:sync_dir(Dir) end, 500),
    io:format("     saved per pure-append flush: ~.1f us~n",
              [(ScanOpen - HitOpen) + Sync]),
    _ = Newest,
    ok.
