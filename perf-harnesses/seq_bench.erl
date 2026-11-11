-module(seq_bench).

-export([run/0]).

t(Name, Fun, N) ->
    _ = Fun(),
    erlang:garbage_collect(),
    {Time, _} = timer:tc(fun () -> loop(Fun, N) end),
    io:format("~-62s ~10.3f us/op~n", [Name, Time / N]),
    Time / N.

loop(_Fun, 0) -> ok;
loop(Fun, N) -> _ = Fun(), loop(Fun, N - 1).

run() ->
    io:format("~n=== ra_seq:remove_prefix (once per 'written' event) ===~n"),
    %% pending is contiguous, written prefix is contiguous
    [begin
         Pend = [{1, PendN}],
         Written = [{1, WN}],
         t(io_lib:format("remove_prefix written=~b of pending=~b",
                         [WN, PendN]),
           fun () -> {ok, _} = ra_seq:remove_prefix(Written, Pend) end,
           iters(WN))
     end || {WN, PendN} <- [{1, 1}, {1, 1000}, {64, 1000}, {1000, 1000},
                            {1000, 5000}, {8192, 8192}, {8192, 20000}]],

    io:format("~n=== what remove_prefix could be (range arithmetic) ===~n"),
    [begin
         Pend = [{1, PendN}],
         Written = [{1, WN}],
         t(io_lib:format("fast_remove_prefix written=~b of pending=~b",
                         [WN, PendN]),
           fun () -> {ok, _} = fast_remove_prefix(Written, Pend) end,
           iters(WN))
     end || {WN, PendN} <- [{1, 1}, {1, 1000}, {64, 1000}, {1000, 1000},
                            {1000, 5000}, {8192, 8192}, {8192, 20000}]],

    io:format("~n=== ra_seq:add (WAL update_ranges, per writer per batch) ===~n"),
    [begin
         Add = [{Start, Start + N - 1}],
         To = [{1, Start - 1}],
         t(io_lib:format("ra_seq:add contiguous add=~b to=~b", [N, Start - 1]),
           fun () -> ra_seq:add(Add, To) end, iters(N))
     end || {N, Start} <- [{1, 1000}, {64, 1000}, {1000, 1000},
                           {8192, 10000}, {100000, 200000}]],
    io:format("  --- range-aware alternative ---~n"),
    [begin
         Add = [{Start, Start + N - 1}],
         To = [{1, Start - 1}],
         t(io_lib:format("fast_add contiguous add=~b to=~b", [N, Start - 1]),
           fun () -> fast_add(Add, To) end, iters(N))
     end || {N, Start} <- [{1, 1000}, {64, 1000}, {1000, 1000},
                           {8192, 10000}, {100000, 200000}]],
    ok.

iters(N) when N >= 100000 -> 200;
iters(N) when N >= 8192 -> 2000;
iters(N) when N >= 1000 -> 20000;
iters(_) -> 200000.

%% ---------------------------------------------------------------
%% A prefix removal that works on ranges instead of walking indexes.
%% Semantics: Prefix must be a prefix of Seq; return Seq with Prefix removed.
fast_remove_prefix([], Seq) ->
    {ok, Seq};
fast_remove_prefix(Prefix, Seq) ->
    PLast = ra_seq:last(Prefix),
    PFirst = ra_seq:first(Prefix),
    SFirst = ra_seq:first(Seq),
    case SFirst of
        undefined ->
            {ok, []};
        _ when PFirst =< SFirst ->
            %% cheap check: the prefix covers the start of Seq
            {ok, ra_seq:floor(PLast + 1, Seq)};
        _ ->
            {error, not_prefix}
    end.

%% Range-aware add: merge two sequences without expanding ranges.
fast_add([], To) -> To;
fast_add(Add, []) -> Add;
fast_add(Add, To) ->
    Fst = ra_seq:first(Add),
    merge(lists:reverse(Add), ra_seq:limit(Fst - 1, To)).

merge([], Acc) -> Acc;
merge([E | Rem], Acc) ->
    merge(Rem, push(E, Acc)).

push(Idx, []) when is_integer(Idx) -> [Idx];
push({_, _} = R, []) -> [R];
push(Idx, [Last | Rem]) when is_integer(Idx) ->
    case Last of
        {S, E} when Idx == E + 1 -> [{S, Idx} | Rem];
        E when is_integer(E), Idx == E + 1 -> [{E, Idx} | Rem];
        _ -> [Idx, Last | Rem]
    end;
push({S1, E1}, [Last | Rem]) ->
    case Last of
        {S, E} when S1 == E + 1 -> [{S, E1} | Rem];
        E when is_integer(E), S1 == E + 1 -> [{E, E1} | Rem];
        _ -> [{S1, E1}, Last | Rem]
    end.
