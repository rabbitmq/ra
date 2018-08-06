-module(ra_log_snapshot_SUITE).

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
     roundtrip,
     read_missing
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
    F = filename:join(?config(priv_dir, Config), TestCase),
    [{file, F} | Config].

end_per_testcase(_TestCase, _Config) ->
    ok.

%%%===================================================================
%%% Test cases
%%%===================================================================

roundtrip(Config) ->
    File = ?config(file, Config),
    Snapshot = {33, 94, [{banana, node@jungle}], my_state},
    ok = ra_log_snapshot:write(File, Snapshot),
    {ok, Snapshot} = ra_log_snapshot:read(File),
    ok.

read_missing(Config) ->
    File = ?config(file, Config),
    {error, enoent} = ra_log_snapshot:read(File),
    ok.

read_other_file(Config) ->
    File = ?config(file, Config),
    file:write_file(File, <<"NOTMAGIC">>),
    {error, invalid_format} = ra_log_snapshot:read(File),
    ok.


%% Utility
