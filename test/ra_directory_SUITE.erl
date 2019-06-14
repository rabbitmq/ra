-module(ra_directory_SUITE).

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
     basics,
     persistence
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

basics(Config) ->
    Dir = ?config(priv_dir, Config),
    ok = ra_directory:init(Dir),
    UId = <<"test1">>,
    Self = self(),
    yes = ra_directory:register_name(UId, Self, undefined,
				     test1, <<"test_cluster_name">>),
    % registrations should always succeed - no negative test
    % no = register_name(Name, spawn(fun() -> ok end), test1),
    Self = ra_directory:where_is(UId),
    UId = ra_directory:uid_of(test1),
    % ensure it can be read from another process
    _ = spawn_link(
          fun () ->
                  UId = ra_directory:uid_of(test1),
                  Self ! done
          end),
    receive done -> ok after 500 -> exit(timeout) end,
    test1 = ra_directory:name_of(UId),
    <<"test_cluster_name">> = ra_directory:cluster_name_of(UId),
    _ = ra_directory:send(UId, hi_Name),
    receive
        hi_Name -> ok
    after 100 ->
              exit(await_msg_timeout)
    end,
    UId = ra_directory:unregister_name(UId),
    undefined = ra_directory:where_is(UId),
    undefined = ra_directory:name_of(UId),
    undefined = ra_directory:cluster_name_of(UId),
    undefined = ra_directory:uid_of(test1),
    ok.

persistence(Config) ->
    Dir = ?config(priv_dir, Config),
    ok = ra_directory:init(Dir),
    UId = <<"test1">>,
    Self = self(),
    yes = ra_directory:register_name(UId, Self, test1),
    UId = ra_directory:uid_of(test1),
    ok = ra_directory:deinit(),
    ok = ra_directory:init(Dir),
    UId = ra_directory:uid_of(test1),
    ok.
%% Utility
