-module(log_bench).

%% Measures the server-side (ra_log) per-append cost and the cost of the
%% written-event handling.

-export([run/0]).

t(Name, Fun, N) ->
    _ = Fun(),
    erlang:garbage_collect(),
    R0 = element(2, process_info(self(), reductions)),
    {Time, _} = timer:tc(fun () -> loop(Fun, N) end),
    R1 = element(2, process_info(self(), reductions)),
    io:format("~-56s ~10.3f us/op ~8.1f reds/op~n",
              [Name, Time / N, (R1 - R0) / N]),
    Time / N.

loop(_Fun, 0) -> ok;
loop(Fun, N) -> _ = Fun(), loop(Fun, N - 1).

run() ->
    io:format("~n=== micro costs paid per ra_log:append ===~n"),
    register(fake_wal, self()),
    t("whereis/1 (ra_log_wal:named_cast per append)",
      fun () -> whereis(fake_wal) end, 1000000),
    t("erlang:system_time(millisecond) (now_ms per append)",
      fun () -> erlang:system_time(millisecond) end, 1000000),
    C = counters:new(40, [write_concurrency]),
    t("counters:add/3", fun () -> counters:add(C, 1, 1) end, 1000000),
    t("counters:put/3", fun () -> counters:put(C, 2, 1) end, 1000000),
    unregister(fake_wal),

    io:format("~n=== ra_log:handle_event({written,...}) share ===~n"),
    %% the two O(n) pieces: ra_seq:remove_prefix and fetch_term
    Tid = ets:new(mt, [set, public]),
    [ets:insert(Tid, {I, 1, x}) || I <- lists:seq(1, 20000)],
    t("ets:lookup_element (fetch_term via mem table)",
      fun () -> 1 = ets:lookup_element(Tid, 8192, 2, undefined) end, 1000000),
    [begin
         Pend = [{1, PendN}],
         Written = [{1, WN}],
         t(io_lib:format("ra_seq:remove_prefix written=~b pending=~b",
                         [WN, PendN]),
           fun () -> {ok, _} = ra_seq:remove_prefix(Written, Pend) end,
           20000)
     end || {WN, PendN} <- [{64, 1000}, {1000, 8000}, {8192, 20000}]],
    ok.
