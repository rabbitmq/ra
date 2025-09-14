%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2017-2025 Broadcom. All Rights Reserved. The term Broadcom refers to Broadcom Inc. and/or its subsidiaries.
%%
-module(ra_directory_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-export([
         ]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

%%%===================================================================
%%% Common Test callbacks
%%%===================================================================

-define(SYS, default).

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
    Dir = ?config(priv_dir, Config),
    Cfg = ra_system:default_config(),
    ra_system:store(Cfg#{data_dir => Dir}),
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

basics(_Config) ->
    ok = ra_directory:init(?SYS),
    UId = atom_to_binary(?FUNCTION_NAME),
    Self = self(),
    ok = ra_directory:register_name(?SYS, UId, Self, undefined,
                                    test1, <<"test_cluster_name">>),
    % registrations should always succeed - no negative test
    Self = ra_directory:where_is(?SYS, UId),
    UId = ra_directory:uid_of(?SYS, test1),
    ?assert(ra_directory:is_registered_uid(?SYS, UId)),
    % ensure it can be read from another process
    _ = spawn_link(
          fun () ->
                  UId = ra_directory:uid_of(?SYS, test1),
                  Self ! done
          end),
    receive done -> ok after 500 -> exit(timeout) end,
    test1 = ra_directory:name_of(?SYS, UId),
    <<"test_cluster_name">> = ra_directory:cluster_name_of(?SYS, UId),
    UId = ra_directory:unregister_name(?SYS, UId),
    undefined = ra_directory:where_is(?SYS, UId),
    undefined = ra_directory:name_of(?SYS, UId),
    undefined = ra_directory:cluster_name_of(?SYS, UId),
    undefined = ra_directory:uid_of(?SYS, test1),
    ?assertNot(ra_directory:is_registered_uid(?SYS, UId)),
    ok.

persistence(_Config) ->
    ok = ra_directory:init(?SYS),
    UId = atom_to_binary(?FUNCTION_NAME),
    UId2 = <<UId/binary, "2">>,
    Self = self(),
    Pid = spawn(fun () -> ok end),
    ok = ra_directory:register_name(?SYS, UId, Self, undefined, test1, <<"name">>),
    ok = ra_directory:register_name(?SYS, UId2, Pid, undefined, test2, <<"name">>),
    true = ra_directory:is_registered_uid(?SYS, UId),
    UId = ra_directory:uid_of(?SYS, test1),
    test1 = ra_directory:name_of(?SYS, UId),
    ok = ra_directory:deinit(?SYS),
    ok = ra_directory:init(?SYS),
    UId = ra_directory:uid_of(?SYS, test1),
    true = ra_directory:is_registered_uid(?SYS, UId),
    test1 = ra_directory:name_of(?SYS, UId),
    UId = ra_directory:unregister_name(?SYS, UId),
    Regd = ra_directory:list_registered(?SYS),
    ?assert(lists:member({test2, UId2}, Regd)),
    ?assertNot(lists:member({test1, UId}, Regd)),
    ok.
