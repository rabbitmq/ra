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
    Name = <<"test1">>,
    Self = self(),
    yes = ra_directory:register_name(Name, Self, test1),
    % registrations should always succeed - no negative test
    % no = register_name(Name, spawn(fun() -> ok end), test1),
    Self = ra_directory:whereis_name(Name),
    Name = ra_directory:registered_name_from_node_name(test1),
    % ensure it can be read from another process
    _ = spawn_link(
          fun () ->
                  Name = ra_directory:registered_name_from_node_name(test1),
                  Self ! done
          end),
    receive done -> ok after 500 -> exit(timeout) end,
    test1 = ra_directory:what_node(Name),
    _ = ra_directory:send(Name, hi_Name),
    receive
        hi_Name -> ok
    after 100 ->
              exit(await_msg_timeout)
    end,
    Name = ra_directory:unregister_name(Name),
    undefined = ra_directory:whereis_name(Name),
    undefined = ra_directory:what_node(Name),
    undefined = ra_directory:registered_name_from_node_name(test1),
    ok.

persistence(Config) ->
    Dir = ?config(priv_dir, Config),
    ok = ra_directory:init(Dir),
    Name = <<"test1">>,
    Self = self(),
    yes = ra_directory:register_name(Name, Self, test1),
    Name = ra_directory:registered_name_from_node_name(test1),
    ok = ra_directory:deinit(),
    ok = ra_directory:init(Dir),
    Name = ra_directory:registered_name_from_node_name(test1),
    ok.
%% Utility
