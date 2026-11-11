-module(livecheck).
%% Differential check: the new merge-scan live_size must agree with the old
%% ra_seq:in/2 per record computation for every live-sequence shape.
-export([run/0]).

run() ->
    rand:seed(exsss, {11, 22, 33}),
    Dir = "/tmp/ra_livecheck",
    _ = os:cmd("rm -rf " ++ Dir),
    ok = filelib:ensure_dir(filename:join(Dir, "x")),
    ok = check(Dir, 2000),
    _ = os:cmd("rm -rf " ++ Dir),
    io:format("all live_size checks agree~n"),
    ok.

check(_Dir, 0) -> ok;
check(Dir, N) ->
    NumEntries = rand:uniform(40),
    Fn = filename:join(Dir, io_lib:format("s~b.segment", [N])),
    _ = file:delete(Fn),
    {ok, S0} = ra_log_segment:open(Fn, #{max_count => 64}),
    Lens = [1 + rand:uniform(20) || _ <- lists:seq(1, NumEntries)],
    S = lists:foldl(fun ({I, L}, Acc) ->
                            D = <<0:L/unit:8>>,
                            {ok, A} = ra_log_segment:append(Acc, I, 1, D),
                            A
                    end, S0, lists:zip(lists:seq(1, NumEntries), Lens)),
    ok = ra_log_segment:close(S),
    %% a random live subset
    Live = ra_seq:from_list([I || I <- lists:seq(1, NumEntries),
                                 rand:uniform(3) =/= 1]),
    #{live_size := Got, num_entries := NE} = ra_log_segment:info(Fn, Live),
    %% model: sum the lengths of entries whose index is in Live
    Want = lists:sum([L || {I, L} <- lists:zip(lists:seq(1, NumEntries), Lens),
                           ra_seq:in(I, Live)]),
    case {Got, NE} of
        {Want, NumEntries} -> ok;
        _ ->
            io:format("MISMATCH n=~b entries=~b live=~w got=~w want=~w ne=~w~n",
                      [N, NumEntries, Live, Got, Want, NE]),
            erlang:error(mismatch)
    end,
    %% and info/1 (no live seq) must count everything
    #{live_size := All} = ra_log_segment:info(Fn),
    case All == lists:sum(Lens) of
        true -> ok;
        false ->
            io:format("MISMATCH info/1 got=~w want=~w~n", [All, lists:sum(Lens)]),
            erlang:error(mismatch)
    end,
    check(Dir, N - 1).
