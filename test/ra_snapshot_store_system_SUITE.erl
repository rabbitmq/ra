%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2017-2025 Broadcom. All Rights Reserved. The term Broadcom refers to Broadcom Inc. and/or its subsidiaries.
%%
%% Ra servers in a system that keeps small snapshots in the shared snapshot log
-module(ra_snapshot_store_system_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(MAX_SIZE, 16384).

all() ->
    [
     {group, tests}
    ].

all_tests() ->
    [
     snapshots_go_to_the_store,
     server_restart_recovers_from_the_store,
     system_restart_recovers_from_the_store,
     large_snapshots_use_directories,
     delete_server_removes_store_entry,
     disabling_the_store_moves_snapshots_back_to_directories,
     lagging_follower_installs_snapshot_from_the_store
    ].

groups() ->
    [
     {tests, [], all_tests()}
    ].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(ra),
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(_Group, Config) ->
    Config.

end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(TestCase, Config) ->
    Sys = TestCase,
    DataDir = filename:join(?config(priv_dir, Config), TestCase),
    ok = ra_lib:make_dir(DataDir),
    [{sys, Sys}, {data_dir, DataDir} | Config].

end_per_testcase(_TestCase, Config) ->
    _ = ra_system:stop(?config(sys, Config)),
    ok.

%%%===================================================================
%%% Test cases
%%%===================================================================

snapshots_go_to_the_store(Config) ->
    Sys = ?config(sys, Config),
    {ok, _} = start_system(Config),
    [Id] = start_members(Config, [a], #{blob_size => 1000}),
    ok = send_commands(Id, 30),
    UId = uid(a),
    wait_for(fun () -> store_idx(Sys, UId) >= 5 end),
    %% the member has no snapshot directories
    ?assertEqual({ok, []}, file:list_dir(snapshots_dir(Config, a))),
    ?assertEqual(30, count(Id)),
    ok.

server_restart_recovers_from_the_store(Config) ->
    Sys = ?config(sys, Config),
    {ok, _} = start_system(Config),
    [Id] = start_members(Config, [a], #{blob_size => 1000}),
    ok = send_commands(Id, 30),
    UId = uid(a),
    wait_for(fun () -> store_idx(Sys, UId) >= 5 end),
    ok = ra:stop_server(Sys, Id),
    ok = ra:restart_server(Sys, Id),
    ?assertEqual(30, count(Id)),
    ok = send_commands(Id, 5),
    ?assertEqual(35, count(Id)),
    ?assertEqual({ok, []}, file:list_dir(snapshots_dir(Config, a))),
    ok.

system_restart_recovers_from_the_store(Config) ->
    Sys = ?config(sys, Config),
    {ok, _} = start_system(Config),
    [Id] = start_members(Config, [a], #{blob_size => 1000}),
    ok = send_commands(Id, 30),
    UId = uid(a),
    wait_for(fun () -> store_idx(Sys, UId) >= 5 end),
    %% stop the whole system and start it again, the member's snapshot is
    %% found in the store files
    ok = ra_system:stop(Sys),
    {ok, _} = start_system(Config),
    ok = ra:restart_server(Sys, Id),
    ?assertEqual(30, count(Id)),
    ?assert(store_idx(Sys, UId) >= 5),
    ok.

large_snapshots_use_directories(Config) ->
    Sys = ?config(sys, Config),
    {ok, _} = start_system(Config),
    [Id] = start_members(Config, [a], #{blob_size => ?MAX_SIZE * 3}),
    ok = send_commands(Id, 30),
    UId = uid(a),
    Dir = snapshots_dir(Config, a),
    wait_for(fun () -> {ok, []} =/= file:list_dir(Dir) end),
    ?assertEqual(-1, store_idx(Sys, UId)),
    ok = ra:stop_server(Sys, Id),
    ok = ra:restart_server(Sys, Id),
    ?assertEqual(30, count(Id)),
    ok.

delete_server_removes_store_entry(Config) ->
    Sys = ?config(sys, Config),
    {ok, _} = start_system(Config),
    [Id] = start_members(Config, [a], #{blob_size => 1000}),
    ok = send_commands(Id, 30),
    UId = uid(a),
    wait_for(fun () -> store_idx(Sys, UId) >= 5 end),
    ok = ra:force_delete_server(Sys, Id),
    wait_for(fun () -> store_idx(Sys, UId) == -1 end),
    ok.

disabling_the_store_moves_snapshots_back_to_directories(Config) ->
    Sys = ?config(sys, Config),
    {ok, _} = start_system(Config),
    [Id] = start_members(Config, [a], #{blob_size => 1000}),
    ok = send_commands(Id, 30),
    UId = uid(a),
    wait_for(fun () -> store_idx(Sys, UId) >= 5 end),
    ?assertEqual({ok, []}, file:list_dir(snapshots_dir(Config, a))),
    ok = ra_system:stop(Sys),
    %% started again without the snapshot store
    {ok, _} = start_system_without_store(Config),
    ok = ra:restart_server(Sys, Id),
    ?assertEqual(30, count(Id)),
    ?assertMatch({ok, [_ | _]}, file:list_dir(snapshots_dir(Config, a))),
    StoreDir = filename:join(?config(data_dir, Config), "snapshot_store"),
    ?assertNot(ra_log_snap_store:has_files(StoreDir)),
    ok.

lagging_follower_installs_snapshot_from_the_store(Config) ->
    Sys = ?config(sys, Config),
    {ok, _} = start_system(Config),
    Ids = start_members(Config, [a, b, c], #{blob_size => 1000}),
    ok = send_commands(hd(Ids), 3),
    {ok, _, Leader} = ra:members(hd(Ids)),
    [Lagging | _] = Ids -- [Leader],
    ok = ra:stop_server(Sys, Lagging),
    %% enough commands for the others to snapshot and truncate their logs
    ok = send_commands(Leader, 60),
    LeaderUId = uid(element(1, Leader)),
    wait_for(fun () -> store_idx(Sys, LeaderUId) >= 20 end),
    ok = ra:restart_server(Sys, Lagging),
    wait_for(fun () -> catch count(Lagging) == 63 end, 200),
    ?assertEqual(63, count(Leader)),
    %% it was brought up to date by a snapshot the leader read from the store
    Sent = lists:sum([maps:get(snapshots_sent,
                               ra_counters:counters({element(1, Id), node()},
                                                    [snapshots_sent]))
                      || Id <- Ids]),
    ?assert(Sent >= 1),
    ok.

%%%===================================================================
%%% Helpers
%%%===================================================================

start_system(Config) ->
    Sys = ?config(sys, Config),
    ra_system:start(#{name => Sys,
                      names => ra_system:derive_names(Sys),
                      data_dir => ?config(data_dir, Config),
                      snapshot_store => #{max_size => ?MAX_SIZE,
                                          min_file_bytes => 64 * 1024}}).

start_system_without_store(Config) ->
    Sys = ?config(sys, Config),
    ra_system:start(#{name => Sys,
                      names => ra_system:derive_names(Sys),
                      data_dir => ?config(data_dir, Config)}).

start_members(Config, Names, MachineConf) ->
    Sys = ?config(sys, Config),
    ClusterName = ?config(sys, Config),
    ServerIds = [{N, node()} || N <- Names],
    Configs = [#{cluster_name => ClusterName,
                 id => Id,
                 uid => uid(N),
                 initial_members => ServerIds,
                 machine => {module, ?MODULE, MachineConf},
                 log_init_args => #{uid => uid(N),
                                    min_snapshot_interval => 5}}
               || {N, _} = Id <- ServerIds],
    {ok, Started, []} = ra:start_cluster(Sys, Configs),
    ?assertEqual(length(ServerIds), length(Started)),
    ServerIds.

uid(Name) ->
    atom_to_binary(Name, utf8).

snapshots_dir(Config, Name) ->
    filename:join([?config(data_dir, Config), atom_to_list(Name),
                   "snapshots"]).

store_name(Sys) ->
    maps:get(snap_store, ra_system:derive_names(Sys)).

store_idx(Sys, UId) ->
    case ra_log_snap_store:lookup(store_name(Sys), UId) of
        {ok, #{idx := Idx}} -> Idx;
        _ -> -1
    end.

send_commands(Id, N) ->
    lists:foreach(
      fun (_) ->
              {ok, _, _} = ra:process_command(Id, inc)
      end, lists:seq(1, N)).

count(Id) ->
    {ok, Count, _} = ra:consistent_query(Id, {?MODULE, get_count, []}),
    Count.

wait_for(Fun) ->
    wait_for(Fun, 100).

wait_for(_Fun, 0) ->
    ct:fail(condition_never_true);
wait_for(Fun, N) ->
    case Fun() of
        true -> ok;
        _ ->
            timer:sleep(100),
            wait_for(Fun, N - 1)
    end.

get_count(#{count := Count}) ->
    Count.

%% ra_machine
init(#{blob_size := Size}) ->
    #{count => 0, blob => crypto:strong_rand_bytes(Size)}.

apply(#{index := Idx}, inc, #{count := Count} = State0) ->
    State = State0#{count := Count + 1},
    {State, Count + 1, [{release_cursor, Idx, State}]}.
