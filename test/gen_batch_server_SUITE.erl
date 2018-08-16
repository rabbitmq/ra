-module(gen_batch_server_SUITE).

-compile(export_all).

-export([
         ]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

%%%===================================================================
%%% Common Test callbacks
%%%===================================================================

all() ->
    [
     {group, tests}
    ].


all_tests() ->
    [
     start_link_calls_init,
     cast_calls_handle_batch,
     cast_many,
     call_calls_handle_batch,
     handle_batch_returns_notification,
     returning_stop_calls_terminate,
     sys_get_status_calls_format_status
    ].

groups() ->
    [
     {tests, [], all_tests()}
    ].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(_Group, Config) ->
    Config.

end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(TestCase, Config) ->

    [{mod, TestCase} | Config].

end_per_testcase(_TestCase, _Config) ->
    meck:unload(),
    ok.

%%%===================================================================
%%% Test cases
%%%===================================================================

start_link_calls_init(Config) ->
    Mod = ?config(mod, Config),
    meck:new(Mod, [non_strict]),
    meck:expect(Mod, init, fun([{some_arg, argh}]) ->
                                   {ok, #{}}
                           end),
    Args = [{some_arg, argh}],
    {ok, Pid} = gen_batch_server:start_link({local, Mod}, Mod, Args, []),
    %% having to wildcard the args as they don't seem to
    %% validate correctly
    ?assertEqual(true, meck:called(Mod, init, '_', Pid)),
    ?assert(meck:validate(Mod)),
    ok.

cast_calls_handle_batch(Config) ->
    Mod = ?config(mod, Config),
    meck:new(Mod, [non_strict]),
    meck:expect(Mod, init, fun(Init) -> {ok, Init} end),
    Args = #{},
    {ok, Pid} = gen_batch_server:start_link({local, Mod}, Mod, Args, []),
    Msg = {put, k, v},
    Self = self(),
    meck:expect(Mod, handle_batch,
                fun([{cast, _Pid,  {put, k, v}}], State) ->
                        Self ! continue,
                        {ok, [], maps:put(k, v, State)}
                end),
    ok = gen_batch_server:cast(Pid, Msg),
    receive continue -> ok after 2000 -> exit(timeout) end,
    ?assertEqual(true, meck:called(Mod, handle_batch, '_', Pid)),
    ?assert(meck:validate(Mod)),
    ok.

cast_many(Config) ->
    Mod = ?config(mod, Config),
    meck:new(Mod, [non_strict]),
    meck:expect(Mod, init, fun(Init) -> {ok, Init} end),
    Args = #{},
    {ok, Pid} = gen_batch_server:start_link({local, Mod}, Mod, Args, []),
    meck:expect(Mod, handle_batch,
                fun(Ops, State) ->
                        {cast, P,  {put, K, V}} = lists:last(Ops),
                        ct:pal("cast_many: batch size ~b~n", [length(Ops)]),
                        {ok, [{notify, P, {done, K, V}}],
                         maps:put(K, V, State)}
                end),
    Num = 20000,
    [gen_batch_server:cast(Pid, {put, I, I}) || I <- lists:seq(1, Num)],
    receive {done, Num, Num} ->
                ok
    after 5000 ->
              exit(timeout)
    end,
    ?assert(meck:validate(Mod)),
    ok.

handle_batch_returns_notification(Config) ->
    Mod = ?config(mod, Config),
    meck:new(Mod, [non_strict]),
    meck:expect(Mod, init, fun(Init) -> {ok, Init} end),
    Args = #{},
    {ok, Pid} = gen_batch_server:start_link({local, Mod}, Mod, Args, []),
    Msg = {put, k, v},
    meck:expect(Mod, handle_batch,
                fun([{cast, P, {put, k, v}}], State) ->
                        {ok, [{notify, P, {done, k}}],
                         maps:put(k, v, State)}
                end),
    ok = gen_batch_server:cast(Pid, Msg),
    receive {done, k} -> ok after 2000 -> exit(timeout) end,
    ?assertEqual(true, meck:called(Mod, handle_batch, '_', Pid)),
    ?assert(meck:validate(Mod)),
    ok.

call_calls_handle_batch(Config) ->
    Mod = ?config(mod, Config),
    meck:new(Mod, [non_strict]),
    meck:expect(Mod, init, fun(Init) -> {ok, Init} end),
    Args = #{},
    {ok, Pid} = gen_batch_server:start_link({local, Mod}, Mod, Args, []),
    Msg = {put, k, v},
    meck:expect(Mod, handle_batch,
                fun([{call, From, {put, k, v}}], State) ->
                        {ok, [{reply, From, {ok, k}}],
                         maps:put(k, v, State)}
                end),
    {ok, k}  = gen_batch_server:call(Pid, Msg),
    ?assertEqual(true, meck:called(Mod, handle_batch, '_', Pid)),
    ?assert(meck:validate(Mod)),
    ok.

returning_stop_calls_terminate(Config) ->
    Mod = ?config(mod, Config),
    %% as we are linked the test process need to also trap exits for this test
    process_flag(trap_exit, true),
    meck:new(Mod, [non_strict]),
    meck:expect(Mod, init, fun(Init) ->
                                   process_flag(trap_exit, true),
                                   {ok, Init}
                           end),
    Args = #{},
    {ok, Pid} = gen_batch_server:start_link({local, Mod}, Mod,
                                            Args, []),
    Msg = {put, k, v},
    meck:expect(Mod, handle_batch,
                fun([{cast, _, {put, k, v}}], _) ->
                        {stop, because}
                end),
    meck:expect(Mod, terminate, fun(because, S) -> S end),
    ok = gen_batch_server:cast(Pid, Msg),
    %% wait for process exit signal
    receive {'EXIT', Pid, because} -> ok after 2000 -> exit(timeout) end,
    %% sleep a little to allow meck to register results
    timer:sleep(10),
    ?assertEqual(true, meck:called(Mod, terminate, '_')),
    ?assert(meck:validate(Mod)),
    ok.

sys_get_status_calls_format_status(Config) ->
    Mod = ?config(mod, Config),
    meck:new(Mod, [non_strict]),
    meck:expect(Mod, init, fun(Init) ->
                                   {ok, Init}
                           end),
    meck:expect(Mod, format_status,
                fun(S) ->
                        {format_status, S}
                end),
    {ok, _Pid} = gen_batch_server:start_link({local, Mod}, Mod,
                                             #{}, []),

    {_, _, _, [_, _, _, _, [_, _ ,S]]} = sys:get_status(Mod),
    ?assertEqual({format_status, #{}}, S),

    ?assertEqual(true, meck:called(Mod, format_status, '_')),
    ?assert(meck:validate(Mod)),
    ok.

%% Utility
