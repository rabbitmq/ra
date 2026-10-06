-module(notify_bench).

%% Prices the per-writer notification send in the WAL against the
%% alternative of accumulating a list and doing one send per batch to a
%% separate fan-out process.

-export([run/0]).

t(Name, Fun, N) ->
    _ = Fun(),
    erlang:garbage_collect(),
    R0 = element(2, process_info(self(), reductions)),
    {Time, _} = timer:tc(fun () -> loop(Fun, N) end),
    R1 = element(2, process_info(self(), reductions)),
    io:format("~-56s ~9.3f us  ~8.2f reds~n",
              [Name, Time / N, (R1 - R0) / N]),
    (R1 - R0) / N.

loop(_Fun, 0) -> ok;
loop(Fun, N) -> _ = Fun(), loop(Fun, N - 1).

run() ->
    %% a drain process with an off-heap mailbox, as the wal and any
    %% notifier process would have
    Drain = spawn_opt(fun drain/0, [{message_queue_data, off_heap}]),
    Seq = [{1, 100}],
    io:format("~n=== per-writer notification cost ===~n"),
    Send = t("Pid ! {ra_log_event, {written, Term, Seq}}",
             fun () -> Drain ! {ra_log_event, {written, 5, Seq}} end,
             500000),
    Cons = t("[{Pid, Term, Seq} | Acc]  (accumulate instead)",
             fun () -> [{Drain, 5, Seq}] end, 500000),
    io:format("  --> saving per writer per batch: ~.2f reds~n", [Send - Cons]),

    io:format("~n=== one send per batch of the accumulated list ===~n"),
    [begin
         L = [{Drain, 5, Seq} || _ <- lists:seq(1, N)],
         Total = t(io_lib:format("send list of ~b notifications", [N]),
                   fun () -> Drain ! {written_batch, L} end,
                   case N of 512 -> 20000; _ -> 100000 end),
         io:format("  --> ~.3f reds per notification~n", [Total / N])
     end || N <- [1, 8, 64, 512]],

    io:format("~n=== for reference: the other per-writer work ===~n"),
    t("ra_seq:floor(1, [{1,100}])", fun () -> ra_seq:floor(1, Seq) end,
      500000),
    t("ra_seq:add([{1,100}], [{1,0}])",
      fun () -> ra_seq:add(Seq, []) end, 500000),
    M = maps:from_list([{I, [{1, 100}]} || I <- lists:seq(1, 64)]),
    t("map update on a 64 key map (update_ranges)",
      fun () -> M#{32 => Seq} end, 500000),

    io:format("~n=== double copy: does the notifier's forward cost more? ===~n"),
    t("forward one notification (notifier -> writer)",
      fun () -> Drain ! {ra_log_event, {written, 5, Seq}} end, 500000),
    exit(Drain, kill),
    ok.

drain() ->
    receive _ -> drain() end.
