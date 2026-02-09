-module(ra_ioerrors_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include("../src/ra_file.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-define(SYS, default).

all() ->
    [ {group, tests} ].

all_tests() ->
    [ ioerrors_recovery_segment
    , ioerrors_recovery_empty_wal
    ].

groups() ->
    [ {tests, [], all_tests()} ].

suite() -> [{timetrap, {seconds, 20}}].

init_per_suite(Config) ->
    ok = logger:set_primary_config(level, all),
    Config.

end_per_suite(Config) ->
    application:stop(ra),
    Config.

restart_ra(DataDir) ->
    {ok, _} = ra:start_in(DataDir),
    ok.

init_per_group(_G, Config) ->
    PrivDir = ?config(priv_dir, Config),
    DataDir = filename:join([PrivDir, "data"]),
    ra_env:configure_logger(logger),
    ok = logger:set_primary_config(level, debug),
    LogFile = filename:join(?config(priv_dir, Config), "ra.log"),
    logger:add_handler(ra_handler, logger_std_h, #{config => #{file => LogFile}}),
    _ = error_logger:tty(false),
    ok = restart_ra(DataDir),
    ok = logger:set_application_level(ra, all),
    Config.

end_per_group(_, Config) ->
    Config.

init_per_testcase(TestCase, Config) ->
    [{test_name, ra_lib:to_list(TestCase)} | Config].

end_per_testcase(_TestCase, Config) ->
    _ = persistent_term:erase(?file_interceptor_pt),
    Config.

ioerrors_recovery_segment(Config) ->
    PrivDir = ?config(priv_dir, Config),
    DataDir = filename:join([PrivDir, "data"]),

    FailurePT = {?FUNCTION_NAME, inject_failure},
    persistent_term:put(FailurePT, []),
    persistent_term:put(?file_interceptor_pt, fun(Op, Args) ->
        case persistent_term:get(FailurePT, []) of
            [] ->
                file_intercept_op(Op, Args, passthrough);
            [{Op, Fail} | Rest] ->
                persistent_term:put(FailurePT, Rest),
                file_intercept_op(Op, Args, Fail);
            [_ | _] ->
                file_intercept_op(Op, Args, passthrough)
        end
    end),

    N1 = nth_server_name(Config, 1),
    ok = new_server(N1, Config),
    ok = ra:trigger_election(N1),

    %% Fill the log with few entries:
    [{ok, _, _} = ra:process_command(N1, I, 1_000) || I <- lists:seq(1, 5)],

    %% Force first few log entries into a segment file:
    ok = restart_ra(DataDir),
    ok = ra:restart_server(N1),

    %% Simulate transient `enospc`:
    %% 1. Write leader `noop` + `simulated enospc 1` to the WAL.
    %% 2. Writing `simulated enospc 2` fails, WAL process restarts.
    %% 3. WAL is getting flushed into the segment file.
    %%    Writing to the segment file silently fails.
    %% 4. Then `simulated enospc 3` gets into the WAL.
    %%    System restart is simulated.
    %% 5. WAL is again getting flushed into the segment file.
    %%    This time it succeeds.
    %%    Log segment has a "hole" now.
    persistent_term:put(FailurePT, [
        {write, passthrough},
        {write, passthrough},
        {write, {error, enospc}},
        {pwrite, {error, enospc}},
        {pwrite, passthrough}
    ]),
    {ok, _, _} = ra:process_command(N1, {?LINE, <<?MODULE_STRING, ": simulated enospc 1">>}, 1000),
    _ = ra:process_command(N1, {?LINE, <<?MODULE_STRING, ": simulated enospc 2">>}, 1000),
    %% Times out because previous is uncommitted:
    _ = ra:process_command(N1, {?LINE, <<?MODULE_STRING, ": simulated enospc 3">>}, 1000),
    %% System restart:
    ok = restart_ra(DataDir),
    ok = ra:restart_server(N1),

    %% Log segment is corrupted now, having a hole is considered unrecoverable:
    {ok, _, _} = ra:process_command(N1, {?LINE, <<?MODULE_STRING, ": healed">>}),

    terminate_cluster([N1]).

ioerrors_recovery_empty_wal(Config) ->
    PrivDir = ?config(priv_dir, Config),
    DataDir = filename:join([PrivDir, "data"]),

    FailurePT = {?MODULE, file_failure},
    persistent_term:put(FailurePT, []),
    persistent_term:put(?file_interceptor_pt, fun(Op, Args) ->
        case persistent_term:get(FailurePT, []) of
            [] ->
                file_intercept_op(Op, Args, passthrough);
            [Fail | Rest] ->
                persistent_term:put(FailurePT, Rest),
                file_intercept_op(Op, Args, Fail)
        end
    end),

    N1 = nth_server_name(Config, 1),
    ok = new_server(N1, Config),
    ok = ra:trigger_election(N1),

    %% Fill the log with few entries:
    [{ok, _, _} = ra:process_command(N1, I, 1_000) || I <- lists:seq(1, 5)],

    %% Force first few log entries into a segment file:
    ok = restart_ra(DataDir),
    ok = ra:restart_server(N1),

    %% Simulate transient `enospc`:
    %% 1. Writing leader `noop` + `simulated enospc 1` to the WAL fail, WAL crashes.
    %%    WAL is still empty at this point.
    %%    Supervisor restarts the process, it recovers from the blank state.
    %% 2. WAL rotation succeds.
    %%    New empty WAL is accepting writes.
    %% 3. Then `simulated enospc 2` gets into the WAL.
    %%    Entry is flushed into the WAL.
    %%    N1 notices there's a hole in the WAL, resends entries from (1).
    %% 4. Writing resent entries (leader noop + simulated enospc 1`) once again fails.
    %% 5. During WAL recovery, `simulated enospc 2` is flushed into the log segment.
    %%    Log segment has a "hole" now.
    persistent_term:put(FailurePT, [
        {error, enospc},
        passthrough,
        passthrough,
        {error, enospc}
    ]),
    _ = ra:process_command(N1, {?LINE, <<?MODULE_STRING, ": simulated enospc 1">>}, 1000),
    _ = ra:process_command(N1, {?LINE, <<?MODULE_STRING, ": simulated enospc 2">>}, 1000),

    ok = timer:sleep(1000),
    ok = restart_ra(DataDir),
    ok = ra:restart_server(N1),

    %% Log segment is corrupted now, having a hole is considered unrecoverable:
    {ok, _, _} = ra:process_command(N1, {?LINE, <<?MODULE_STRING, ": healed">>}),
    
    terminate_cluster([N1]).

file_intercept_op(write, [Fd, Bytes], Inject) ->
    Ret = case Inject of
        passthrough -> file:write(Fd, Bytes);
        Error -> Error
    end,
    ct:pal("~p: write(FD, <~p>) -> ~p", [self(), iolist_size(Bytes), Ret]),
    Ret;
file_intercept_op(pwrite, [Fd, Loc, Bytes], Inject) ->
    Ret = case Inject of
        passthrough -> file:pwrite(Fd, Loc, Bytes);
        Error -> Error
    end,
    ct:pal("~p: pwrite(FD, ~0p, <~p>) -> ~p", [self(), Loc, iolist_size(Bytes), Ret]),
    Ret.

%%

terminate_cluster(Nodes) ->
    [ra:stop_server(?SYS, P) || P <- Nodes].

new_server(Name, Config) ->
    ClusterName = ?config(test_name, Config),
    ok = ra:start_server(default, ClusterName, Name, add_machine(), []),
    ok.

stop_server(Name) ->
    ok = ra:stop_server(?SYS, Name),
    ok.

add_member(Ref, New) ->
    {ok, _IdxTerm, _Leader} = ra:add_member(Ref, New),
    ok.

issue_op(Name, Op) ->
    {ok, _, Leader} = ra:process_command(Name, Op, 10_000),
    Leader.

dump(T) ->
    ct:pal("DUMP: ~p", [T]),
    T.

nth_server_name(Config, N) when is_integer(N) ->
    {ra_server:name(?config(test_name, Config), erlang:integer_to_list(N)),
     node()}.

add_machine() ->
    {module, ?MODULE, #{}}.

%% machine impl
init(_) -> 0.

apply(_Meta, {Num, _Comment}, State) ->
    {Num + State, Num + State};
apply(_Meta, Num, State) ->
    {Num + State, Num + State}.
