-module(ra_log_ets_SUITE).

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
     deletes_tables
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

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok.

%%%===================================================================
%%% Test cases
%%%===================================================================

deletes_tables(Config) ->
    Conf0 = ra_system:default_config(),
    Conf = Conf0#{data_dir => ?config(priv_dir, Config)},
    Names = maps:get(names, Conf),
    _ = ra_log_ets:start_link(Conf),
    T1 = ets:new(t1, []),
    T2 = ets:new(t2, []),
    T3 = ets:new(t2, [public]),
    ra_log_ets:give_away(Names, T1),
    ra_log_ets:give_away(Names, T2),
    ra_log_ets:give_away(Names, T3),
    ets:delete(T3),
    ra_log_ets:delete_tables(Names, [T1, T2, T3]),
    %% ensure prior messages have been processed
    gen_server:call(ra_log_ets, noop),
    undefined = ets:info(T1),
    undefined = ets:info(T2),
    proc_lib:stop(ra_log_ets),
    ok.

%% Utility
