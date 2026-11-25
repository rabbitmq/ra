-module(ra_worker_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

all() ->
    [
     work_is_released_after_it_runs
    ].

init_per_testcase(_TestCase, Config) ->
    {ok, Pid} = ra_worker:start_link(#{id => ?MODULE}),
    unlink(Pid),
    [{worker, Pid} | Config].

end_per_testcase(_TestCase, Config) ->
    exit(?config(worker, Config), kill),
    ok.

%% A snapshot fun captures the machine state, which can be large.
work_is_released_after_it_runs(Config) ->
    Pid = ?config(worker, Config),
    Self = self(),
    Captured = lists:seq(1, 1_000_000),
    ok = ra_worker:queue_work(Pid, fun () -> Self ! {done, length(Captured)} end,
                              fun (_) -> ok end),
    receive {done, 1_000_000} -> ok after 5000 -> ct:fail(work_not_run) end,
    _ = sys:get_state(Pid),
    {total_heap_size, Words} = erlang:process_info(Pid, total_heap_size),
    ?assert(Words < 100_000),
    ok.
