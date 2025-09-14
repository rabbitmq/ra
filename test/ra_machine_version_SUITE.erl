%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2017-2025 Broadcom. All Rights Reserved. The term Broadcom refers to Broadcom Inc. and/or its subsidiaries.
%%
-module(ra_machine_version_SUITE).
-compile(nowarn_export_all).
-compile(export_all).

-export([
         ]).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-define(SYS, default).

%%%===================================================================
%%% Common Test callbacks
%%%===================================================================

all() ->
    [
     {group, tests}
    ].


all_tests() ->
    [
     server_with_higher_version_needs_quorum_to_be_elected,
     cluster_waits_for_all_members_to_have_latest_version_to_upgrade,
     server_with_lower_version_can_vote_for_higher_if_effective_version_is_higher,
     unversioned_machine_never_sees_machine_version_command,
     unversioned_can_change_to_versioned,
     server_upgrades_machine_state_on_noop_command,
     server_applies_with_new_module,
     initial_machine_version,
     initial_machine_version_quorum
     % snapshot_persists_machine_version
    ].


%% gnarly: for rabbit_fifo we need to capture the active module and version
%% at the index to be snapshotted. This means the release_cursor effect need to be
%% overloaded to support a {version(), module()} field as well

groups() ->
    [
     {tests, [], all_tests()}
    ].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(_, Config) ->
    PrivDir = ?config(priv_dir, Config),
    {ok, _} = ra:start_in(PrivDir),
    Config.

end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(TestCase, Config) ->
    ok = logger:set_primary_config(level, all),
    ra_server_sup_sup:remove_all(?SYS),
    case lists:member(TestCase, machine_upgrade_quorum_tests()) of
        true ->
            ok = application:set_env(ra, machine_upgrade_strategy, quorum),
            _ = ra_system:stop_default(),
            {ok, _} = ra_system:start_default();
        _ ->
            ok
    end,
    ServerName1 = list_to_atom(atom_to_list(TestCase) ++ "1"),
    ServerName2 = list_to_atom(atom_to_list(TestCase) ++ "2"),
    ServerName3 = list_to_atom(atom_to_list(TestCase) ++ "3"),
    Cluster = [{ServerName1, node()},
               {ServerName2, node()},
               {ServerName3, node()}],
    [
     {modname, TestCase},
     {cluster, Cluster},
     {cluster_name, TestCase},
     {uid, atom_to_binary(TestCase, utf8)},
     {server_id, {TestCase, node()}},
     {uid2, atom_to_binary(ServerName2, utf8)},
     {server_id2, {ServerName2, node()}},
     {uid3, atom_to_binary(ServerName3, utf8)},
     {server_id3, {ServerName3, node()}}
     | Config].

end_per_testcase(TestCase, Config) ->
    catch ra:delete_cluster(?config(cluster, Config)),
    meck:unload(),
    case lists:member(TestCase, machine_upgrade_quorum_tests()) of
        true ->
            ok = application:unset_env(ra, machine_upgrade_strategy),
            _ = ra_system:stop_default(),
            {ok, _} = ra_system:start_default();
        _ ->
            ok
    end,
    ok.

machine_upgrade_quorum_tests() ->
    [server_with_lower_version_can_vote_for_higher_if_effective_version_is_higher,
     initial_machine_version_quorum].

%%%===================================================================
%%% Test cases
%%%===================================================================
server_with_higher_version_needs_quorum_to_be_elected(Config) ->
    ok = logger:set_primary_config(level, all),
    Mod = ?config(modname, Config),
    meck:new(Mod, [non_strict]),
    meck:expect(Mod, init, fun (_) -> init_state end),
    meck:expect(Mod, version, fun () -> 1 end),
    meck:expect(Mod, which_module, fun (_) -> Mod end),
    meck:expect(Mod, apply, fun (_, _, S) -> {S, ok} end),
    Cluster = ?config(cluster, Config),
    ClusterName = ?config(cluster_name, Config),
    Leader = start_cluster(ClusterName, {module, Mod, #{}}, Cluster),
    Followers = lists:delete(Leader, Cluster),
    meck:expect(Mod, version, fun () ->
                                      Self = self(),
                                      case whereis(element(1, Leader)) of
                                          Self -> 2;
                                          _ -> 1
                                      end
                              end),
    ra:stop_server(?SYS, Leader),
    ra:restart_server(?SYS, Leader),
    %% assert Leader node has correct machine version
    {ok, _, Leader2} = ra:members(hd(Followers)),
    ?assertNotEqual(Leader, Leader2),

    [LastFollower] = lists:delete(Leader2, Followers),
    meck:expect(Mod, version, fun () ->
                                      New = [whereis(element(1, Leader)),
                                             whereis(element(1, Leader2))],
                                      case lists:member(self(), New) of
                                          true -> 2;
                                          _  -> 1
                                      end
                              end),
    ra:stop_server(?SYS, Leader2),
    ra:restart_server(?SYS, Leader2),
    %% need to stop last follower as it can still be elected now
    ra:stop_server(?SYS, LastFollower),
    %% this last leader must now be a version 2 not 1
    {ok, _, Leader3} = ra:members(Leader2, 60000),

    ?assertNotEqual(LastFollower, Leader3),
    ok.

cluster_waits_for_all_members_to_have_latest_version_to_upgrade(Config) ->
    ok = ra_env:configure_logger(logger),
    LogFile = filename:join([?config(priv_dir, Config), "ra.log"]),
    LogConfig = #{config => #{type => {file, LogFile}}, level => debug},
    logger:add_handler(ra_handler, logger_std_h, LogConfig),
    ok = logger:set_primary_config(level, all),
    ct:pal("handler config ~p", [logger:get_handler_config()]),
    Mod = ?config(modname, Config),
    meck:new(Mod, [non_strict]),
    meck:expect(Mod, init, fun (_) -> init_state end),
    meck:expect(Mod, version, fun () -> 1 end),
    meck:expect(Mod, which_module, fun (_) -> Mod end),
    meck:expect(Mod, apply, fun (_, _, S) -> {S, ok} end),
    Cluster = ?config(cluster, Config),
    ClusterName = ?config(cluster_name, Config),
    Leader = start_cluster(ClusterName, {module, Mod, #{}}, Cluster),
    [Follower1, Follower2] = lists:delete(Leader, Cluster),
    timer:sleep(100),
    %% leader and follower 1 are v2s
    ra:stop_server(?SYS, Leader),
    ra:stop_server(?SYS, Follower1),
    ra:stop_server(?SYS, Follower2),
    meck:expect(Mod, version, fun () ->
                                      New = [whereis(element(1, Leader)),
                                             whereis(element(1, Follower1))],
                                      case lists:member(self(), New) of
                                          true -> 2;
                                          _  -> 1
                                      end
                              end),
    ra:restart_server(?SYS, Leader),
    ra:restart_server(?SYS, Follower1),
    timer:sleep(100),
    {ok, _, _Leader1} = ra:members(Leader, 2000),
    ra:restart_server(?SYS, Follower2),
    %% The cluster is still using v1 even though Leader and Follower2 knows
    %% about v2.
    lists:foreach(
      fun(Member) ->
              await(fun () ->
                            case ra:member_overview(Member) of
                                {ok, #{effective_machine_version := 1,
                                       machine_version := 1}, _}
                                  when Member == Follower2 ->
                                    true;
                                {ok, #{effective_machine_version := 1,
                                       machine_version := 2}, _} ->
                                    true;
                                _ ->
                                    false
                            end
                    end, 100)
      end, Cluster),
    %% Restart Follower2 with v2. The cluster should now upgrade to v2.
    ra:stop_server(?SYS, Follower2),
    meck:expect(Mod, version, fun () -> 2 end),
    ra:restart_server(?SYS, Follower2),
    lists:foreach(
      fun(Member) ->
              await(fun () ->
                            case ra:member_overview(Member) of
                                {ok, #{effective_machine_version := 2,
                                       machine_version := 2}, _} ->
                                    true;
                                _ ->
                                    false
                            end
                    end, 100)
      end, Cluster),

    ok.

server_with_lower_version_can_vote_for_higher_if_effective_version_is_higher(Config) ->
    ok = ra_env:configure_logger(logger),
    LogFile = filename:join([?config(priv_dir, Config), "ra.log"]),
    LogConfig = #{config => #{type => {file, LogFile}}, level => debug},
    logger:add_handler(ra_handler, logger_std_h, LogConfig),
    ok = logger:set_primary_config(level, all),
    ct:pal("handler config ~p", [logger:get_handler_config()]),
    Mod = ?config(modname, Config),
    meck:new(Mod, [non_strict]),
    meck:expect(Mod, init, fun (_) -> init_state end),
    meck:expect(Mod, version, fun () -> 1 end),
    meck:expect(Mod, which_module, fun (_) -> Mod end),
    meck:expect(Mod, apply, fun (_, _, S) -> {S, ok} end),
    Cluster = ?config(cluster, Config),
    ClusterName = ?config(cluster_name, Config),
    Leader = start_cluster(ClusterName, {module, Mod, #{}}, Cluster),
    [Follower1, Follower2] = lists:delete(Leader, Cluster),
    timer:sleep(100),
    %% leader and follower 1 are v2s
    ra:stop_server(?SYS, Leader),
    ra:stop_server(?SYS, Follower1),
    ra:stop_server(?SYS, Follower2),
    meck:expect(Mod, version, fun () ->
                                      New = [whereis(element(1, Leader)),
                                             whereis(element(1, Follower1))],
                                      case lists:member(self(), New) of
                                          true -> 2;
                                          _  -> 1
                                      end
                              end),
    ra:restart_server(?SYS, Leader),
    ra:restart_server(?SYS, Follower1),
    timer:sleep(100),
    {ok, _, Leader2} = ra:members(Leader, 2000),
    ra:restart_server(?SYS, Follower2),
    %% need to wait until the restarted Follower2 discovers the current
    %% effective machine version
    await(fun () ->
                  case ra:member_overview(Follower2) of
                      {ok, #{effective_machine_version := 2,
                             machine_version := 1}, _} ->
                          true;
                      _ ->
                          false
                  end
          end, 100),
    %% at this point the effective machine version known by all members is 2
    %% but Follower2's local machine version is 1 as it hasn't been "upgraded"
    %% yet
    %% stop the leader to trigger an election that Follower2 must not win
    ra:stop_server(?SYS, Leader2),
    ExpectedLeader = case Leader2 of
                         Follower1 -> Leader;
                         _ -> Follower1
                     end,
    %% follower 1 should now be elected
    ?assertMatch({ok, _,  ExpectedLeader}, ra:members(ExpectedLeader, 60000)),

    ok.

unversioned_machine_never_sees_machine_version_command(Config) ->
    Mod = ?config(modname, Config),
    meck:new(Mod, [non_strict]),
    meck:expect(Mod, init, fun (_) -> init_state end),
    meck:expect(Mod, apply, fun (_, dummy, S) ->
                                    {S, ok};
                                (_, {machine_version, _, _}, _) ->
                                    exit(unexpected_machine_version_command);
                                (_, Cmd, _) ->
                                    {Cmd, ok}
                            end),
    ClusterName = ?config(cluster_name, Config),
    ServerId = ?config(server_id, Config),
    _ = start_cluster(ClusterName, {module, Mod, #{}}, [ServerId]),
    % need to execute a command here to ensure the noop command has been fully
    % applied. The wal fsync could take a few ms causing the race
    {ok, ok, _} = ra:process_command(ServerId, dummy),
    %% assert state_v1
    {ok, {_, init_state}, _} = ra:leader_query(ServerId,
                                               fun (S) -> S end),
    ok = ra:stop_server(?SYS, ServerId),
    %% increment version
    % meck:expect(Mod, version, fun () -> 2 end),
    ok = ra:restart_server(?SYS, ServerId),
    {ok, ok, _} = ra:process_command(ServerId, dummy),

    {ok, {_, init_state}, _} = ra:leader_query(ServerId, fun ra_lib:id/1),
    ok.

unversioned_can_change_to_versioned(Config) ->
    Mod = ?config(modname, Config),
    meck:new(Mod, [non_strict]),
    meck:expect(Mod, init, fun (_) -> init_state end),
    meck:expect(Mod, apply, fun (_, dummy, S) -> {S, ok} end),
    ClusterName = ?config(cluster_name, Config),
    ServerId = ?config(server_id, Config),
    _ = start_cluster(ClusterName, {module, Mod, #{}}, [ServerId]),
    % need to execute a command here to ensure the noop command has been fully
    % applied. The wal fsync could take a few ms causing the race
    {ok, ok, _} = ra:process_command(ServerId, dummy),
    %% assert state_v1
    {ok, {_, init_state}, _} = ra:leader_query(ServerId, fun (S) -> S end),
    ok = ra:stop_server(?SYS, ServerId),
    meck:expect(Mod, version, fun () -> 1 end),
    meck:expect(Mod, which_module, fun (_) -> Mod end),
    meck:expect(Mod, apply, fun (_, dummy, S) ->
                                    {S, ok};
                                (#{system_time := Ts},
                                 {machine_version, 0, 1}, init_state) ->
                                    %% cheeky unrelated assertion to ensure
                                    %% the timestamp is a valid timestamp
                                    ?assert(is_integer(Ts)),
                                    {state_v1, ok}
                            end),
    %% increment version
    ok = ra:restart_server(?SYS, ServerId),
    {ok, ok, _} = ra:process_command(ServerId, dummy),

    {ok, {_, state_v1}, _} = ra:leader_query(ServerId, fun ra_lib:id/1),
    ok.

server_upgrades_machine_state_on_noop_command(Config) ->
    Mod = ?config(modname, Config),
    meck:new(Mod, [non_strict]),
    meck:expect(Mod, init, fun (_) -> init_state end),
    meck:expect(Mod, version, fun () -> 1 end),
    meck:expect(Mod, which_module, fun (_) -> Mod end),
    meck:expect(Mod, apply, fun (_, dummy, S) ->
                                    {S, ok};
                                (_, {machine_version, 0, 1}, init_state) ->
                                    {state_v1, ok};
                                (_, {machine_version, 1, 2}, state_v1) ->
                                    {state_v2, ok}
                            end),
    ClusterName = ?config(cluster_name, Config),
    ServerId = ?config(server_id, Config),
    _ = start_cluster(ClusterName, {module, Mod, #{}}, [ServerId]),
    % need to execute a command here to ensure the noop command has been fully
    % applied. The wal fsync could take a few ms causing the race
    {ok, ok, _} = ra:process_command(ServerId, dummy),
    %% assert state_v1
    {ok, {_, state_v1}, _} = ra:leader_query(ServerId,
                                             fun (S) ->
                                                    ct:pal("leader_query ~w", [S]),
                                                   S
                                             end),
    ok = ra:stop_server(?SYS, ServerId),
    %% increment version
    meck:expect(Mod, version, fun () -> 2 end),
    ok = ra:restart_server(?SYS, ServerId),
    {ok, ok, _} = ra:process_command(ServerId, dummy),

    {ok, {_, state_v2}, _} = ra:leader_query(ServerId, fun ra_lib:id/1),
    ok.

server_applies_with_new_module(Config) ->
    Mod = ?config(modname, Config),
    meck:new(Mod, [non_strict]),
    meck:expect(Mod, init, fun (_) -> init_state end),
    meck:expect(Mod, apply, fun (_, dummy, init_state) ->
                                    {init_state, ok}
                            end),
    ClusterName = ?config(cluster_name, Config),
    ServerId = ?config(server_id, Config),
    _ = start_cluster(ClusterName, {module, Mod, #{}}, [ServerId]),
    % need to execute a command here to ensure the noop command has been fully
    % applied. The wal fsync could take a few ms causing the race
    {ok, ok, _} = ra:process_command(ServerId, dummy),
    %% assert state_v1
    {ok, {_, init_state}, _} = ra:leader_query(ServerId, fun (S) -> S end),

    ok = ra:stop_server(?SYS, ServerId),
    %% simulate module upgrade
    Mod0 = mod_v0,
    meck:new(Mod0, [non_strict]),
    meck:expect(Mod, init, fun (_) -> exit(unexpected) end),
    meck:expect(Mod0, init, fun (_) -> init_state end),
    meck:expect(Mod0, apply, fun (_, dummy, init_state) ->
                                    {init_state, ok}
                             end),
    meck:expect(Mod, version, fun () -> 1 end),
    meck:expect(Mod, which_module, fun (0) -> Mod0;
                                       (1) -> Mod
                                    end),
    meck:expect(Mod, apply, fun (_, dummy, state_v1) ->
                                    {state_v1, ok};
                                (_, dummy2, state_v1) ->
                                    {state_v1, ok};
                                (_, {machine_version, 0, 1}, init_state) ->
                                    {state_v1, ok}
                            end),
    ok = ra:restart_server(?SYS, ServerId),
    %% increment version
    {ok, ok, _} = ra:process_command(ServerId, dummy2),
    {ok, state_v1, _} = ra:consistent_query(ServerId, fun ra_lib:id/1),
    ok = ra:stop_server(?SYS, ServerId),
    ok = ra:restart_server(?SYS, ServerId),
    _ = ra:members(ServerId),
    {ok, state_v1, _} = ra:consistent_query(ServerId, fun ra_lib:id/1),
    ok.

snapshot_persists_machine_version(_Config) ->
    error({todo, ?FUNCTION_NAME}).

initial_machine_version(Config) ->
    Mod = ?config(modname, Config),
    meck:new(Mod, [non_strict]),
    meck:expect(Mod, init, fun (#{machine_version := MacVer}) ->
                                   ?assertEqual(3, MacVer),
                                   init_state
                           end),
    meck:expect(Mod, version, fun () -> 5 end),
    meck:expect(Mod, which_module, fun (_) -> Mod end),
    meck:expect(Mod, apply, fun (_, dummy, S) ->
                                    {S, ok};
                                (_, {machine_version, 0, 3}, init_state) ->
                                    exit(booo),
                                    {state_v3, ok};
                                (_, {machine_version, 3, 5}, init_state) ->
                                    ct:pal("3-5"),
                                    {state_v5, ok}
                            end),
    ClusterName = ?config(cluster_name, Config),
    ServerId = ?config(server_id, Config),
    Machine = {module, Mod, #{}},
    Configs = [begin
                   UId = ra:new_uid(ra_lib:to_binary(ClusterName)),
                   #{id => Id,
                     uid => UId,
                     cluster_name => ClusterName,
                     log_init_args => #{uid => UId},
                     initial_members => [ServerId],
                     machine => Machine,
                     initial_machine_version => 3}
               end || Id <- [ServerId]],
    % debugger:start(),
    % int:i(ra_machine),
    % int:i(ra_server_sup_sup),
    % int:break(ra_server_sup_sup, 66),
    {ok, _, _} = ra:start_cluster(?SYS, Configs, 5000),
    await(fun () ->
                  {ok, {_, S}, _} = ra:leader_query(ServerId, fun ra_lib:id/1),
                  S == state_v5
          end, 100),
    ?assertMatch({ok, #{effective_machine_version := 5}, _},
                 ra:member_overview(ServerId)),
    {ok, _} = ra:delete_cluster([ServerId]),
    await(fun () -> whereis(element(1, ServerId)) == undefined end, 100),
    meck:expect(Mod, init, fun (#{machine_version := MacVer}) ->
                                   ?assertEqual(5, MacVer),
                                   init_state
                           end),
    meck:expect(Mod, apply, fun (Meta, meta, _S) ->
                                    {state_v5, Meta};
                                (_, {machine_version, 0, 3}, init_state) ->
                                    exit(booo),
                                    {state_v3, ok};
                                (_, {machine_version, 5, 5}, init_state) ->
                                    ct:pal("5-5"),
                                    {state_v5, ok}
                            end),
    Configs2 = [begin
                   UId = ra:new_uid(ra_lib:to_binary(ClusterName)),
                   #{id => Id,
                     uid => UId,
                     cluster_name => ClusterName,
                     log_init_args => #{uid => UId},
                     initial_members => [ServerId],
                     machine => Machine,
                     initial_machine_version => 9}
               end || Id <- [ServerId]],
    {error, cluster_not_formed} = ra:start_cluster(?SYS, Configs2, 5000),
    ok.

initial_machine_version_quorum(Config) ->
    Mod = ?config(modname, Config),
    meck:new(Mod, [non_strict]),
    meck:expect(Mod, init, fun (#{machine_version := MacVer}) ->
                                   ?assertEqual(3, MacVer),
                                   init_state
                           end),
    meck:expect(Mod, version, fun () -> 5 end),
    meck:expect(Mod, which_module, fun (_) -> Mod end),
    meck:expect(Mod, apply, fun (_, dummy, S) ->
                                    {S, ok};
                                (_, {machine_version, 0, 3}, init_state) ->
                                    exit(booo),
                                    {state_v3, ok};
                                (_, {machine_version, 3, 5}, init_state) ->
                                    ct:pal("3-5"),
                                    {state_v5, ok}
                            end),
    ClusterName = ?config(cluster_name, Config),
    ServerId = ?config(server_id, Config),
    Machine = {module, Mod, #{}},
    Configs = [begin
                   UId = ra:new_uid(ra_lib:to_binary(ClusterName)),
                   #{id => Id,
                     uid => UId,
                     cluster_name => ClusterName,
                     log_init_args => #{uid => UId},
                     initial_members => [ServerId],
                     machine => Machine,
                     initial_machine_version => 3}
               end || Id <- [ServerId]],
    % debugger:start(),
    % int:i(ra_machine),
    % int:i(ra_server_sup_sup),
    % int:break(ra_server_sup_sup, 66),
    {ok, _, _} = ra:start_cluster(?SYS, Configs, 5000),
    await(fun () ->
                  {ok, {_, S}, _} = ra:leader_query(ServerId, fun ra_lib:id/1),
                  S == state_v5
          end, 100),
    ?assertMatch({ok, #{effective_machine_version := 5}, _},
                 ra:member_overview(ServerId)),
    {ok, _} = ra:delete_cluster([ServerId]),
    await(fun () -> whereis(element(1, ServerId)) == undefined end, 100),
    meck:expect(Mod, init, fun (#{machine_version := MacVer}) ->
                                   ?assertEqual(5, MacVer),
                                   init_state
                           end),
    meck:expect(Mod, apply, fun (Meta, meta, _S) ->
                                    {state_v5, Meta};
                                (_, {machine_version, 0, 3}, init_state) ->
                                    exit(booo),
                                    {state_v3, ok};
                                (_, {machine_version, 5, 5}, init_state) ->
                                    ct:pal("5-5"),
                                    {state_v5, ok}
                            end),
    Configs2 = [begin
                   UId = ra:new_uid(ra_lib:to_binary(ClusterName)),
                   #{id => Id,
                     uid => UId,
                     cluster_name => ClusterName,
                     log_init_args => #{uid => UId},
                     initial_members => [ServerId],
                     machine => Machine,
                     initial_machine_version => 9}
               end || Id <- [ServerId]],
    {ok, _, _} = ra:start_cluster(?SYS, Configs2, 5000),
    {ok, #{machine_version := 5}, _} = ra:process_command(ServerId, meta),
    await(fun () ->
                  {ok, {_, S}, _} = ra:leader_query(ServerId, fun ra_lib:id/1),
                  ct:pal("S ~p", [S]),
                  S == state_v5
          end, 100),
    ct:pal("overview ~p", [ra:member_overview(ServerId)]),
    ok.
%% Utility

validate_state_enters(States) ->
    lists:foreach(fun (S) ->
                          receive {ra_event, _, {machine, {state_enter, S}}} -> ok
                          after 250 ->
                                    flush(),
                                    ct:pal("S ~w", [S]),
                                    exit({timeout, S})
                          end
                  end, States).

start_cluster(ClusterName, Machine, ServerIds) ->
    {ok, Started, _} = ra:start_cluster(?SYS, ClusterName, Machine, ServerIds),
    {ok, _, Leader} = ra:members(hd(Started)),
    ?assertEqual(length(ServerIds), length(Started)),
    Leader.

validate_process_down(Name, 0) ->
    exit({process_not_down, Name});
validate_process_down(Name, Num) ->
    case whereis(Name) of
        undefined ->
            ok;
        _ ->
            timer:sleep(100),
            validate_process_down(Name, Num-1)
    end.

flush() ->
    receive
        Any ->
            ct:pal("flush ~p", [Any]),
            flush()
    after 0 ->
              ok
    end.

await(_CondPred, 0) ->
    ct:fail("await condition did not materialize");
await(CondPred, N) ->
    case CondPred() of
        true ->
            ok;
        false ->
            timer:sleep(100),
            await(CondPred, N-1)
    end.
