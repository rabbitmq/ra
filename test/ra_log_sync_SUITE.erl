%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2017-2025 Broadcom. All Rights Reserved. The term Broadcom refers to Broadcom Inc. and/or its subsidiaries.
%%
-module(ra_log_sync_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

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
     sync_single_file,
     sync_multiple_files,
     sync_error_propagated,
     sync_concurrent_callers
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
    Name = list_to_atom("ra_log_sync_" ++ atom_to_list(TestCase)),
    {ok, Pid} = ra_log_sync:start_link(#{name => Name}),
    Dir = filename:join(?config(priv_dir, Config), atom_to_list(TestCase)),
    ok = file:make_dir(Dir),
    [{sync_server, Name}, {sync_pid, Pid}, {dir, Dir} | Config].

end_per_testcase(_TestCase, Config) ->
    Pid = ?config(sync_pid, Config),
    gen_batch_server:stop(Pid),
    ok.

%%%===================================================================
%%% Test cases
%%%===================================================================

sync_single_file(Config) ->
    Server = ?config(sync_server, Config),
    Dir = ?config(dir, Config),
    File = filename:join(Dir, "test.dat"),
    ok = file:write_file(File, <<"hello">>),
    ok = ra_log_sync:sync(Server, fun() -> ra_lib:sync_file(File) end),
    %% Verify the file is readable after sync.
    ?assertEqual({ok, <<"hello">>}, file:read_file(File)).

sync_multiple_files(Config) ->
    Server = ?config(sync_server, Config),
    Dir = ?config(dir, Config),
    Files = [filename:join(Dir, "test" ++ integer_to_list(I) ++ ".dat")
             || I <- lists:seq(1, 10)],
    [ok = file:write_file(F, <<"data", (integer_to_binary(I))/binary>>)
     || {I, F} <- lists:zip(lists:seq(1, 10), Files)],
    [ok = ra_log_sync:sync(Server, fun() -> ra_lib:sync_file(F) end)
     || F <- Files],
    %% All files should be readable.
    [begin
         {ok, _} = file:read_file(F)
     end || F <- Files].

sync_error_propagated(Config) ->
    Server = ?config(sync_server, Config),
    Result = ra_log_sync:sync(Server,
                              fun() -> {error, enoent} end),
    ?assertEqual({error, enoent}, Result).

sync_concurrent_callers(Config) ->
    Server = ?config(sync_server, Config),
    Dir = ?config(dir, Config),
    N = 50,
    Files = [filename:join(Dir, "concurrent" ++ integer_to_list(I) ++ ".dat")
             || I <- lists:seq(1, N)],
    [ok = file:write_file(F, <<"concurrent_data">>) || F <- Files],
    Parent = self(),
    Pids = [spawn_link(fun() ->
                               Result = ra_log_sync:sync(
                                          Server,
                                          fun() -> ra_lib:sync_file(F) end),
                               Parent ! {done, self(), Result}
                       end)
            || F <- Files],
    Results = [receive {done, Pid, R} -> R end || Pid <- Pids],
    ?assert(lists:all(fun(R) -> R =:= ok end, Results)).
