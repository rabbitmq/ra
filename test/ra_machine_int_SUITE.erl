%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2017-2025 Broadcom. All Rights Reserved. The term Broadcom refers to Broadcom Inc. and/or its subsidiaries.
%%
-module(ra_machine_int_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-export([
         ]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
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
     send_msg_without_options,
     send_msg_with_ra_event_option,
     send_msg_with_cast_option,
     send_msg_with_ra_event_and_cast_options,
     machine_replies,
     leader_monitors,
     down_follows_all_low_priority_commands,
     follower_takes_over_monitor,
     deleted_cluster_emits_eol_effect,
     machine_state_enter_effects,
     meta_data,
     meta_data_2,
     append_effect,
     append_effect_with_notify,
     append_effect_follower,
     timer_effect,
     log_effect,
     aux_eval,
     aux_tick,
     aux_handler_not_impl,
     aux_command,
     aux_command_v2,
     aux_command_v1_and_v2,
     aux_command_timeout,
     aux_monitor_effect,
     aux_and_machine_monitor_same_process,
     aux_and_machine_monitor_same_node,
     aux_and_machine_monitor_leader_change
    ].

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
    ra_server_sup_sup:remove_all(?SYS),
    ServerName2 = list_to_atom(atom_to_list(TestCase) ++ "2"),
    ServerName3 = list_to_atom(atom_to_list(TestCase) ++ "3"),
    [
     {modname, TestCase},
     {cluster_name, TestCase},
     {uid, atom_to_binary(TestCase, utf8)},
     {server_id, {TestCase, node()}},
     {uid2, atom_to_binary(ServerName2, utf8)},
     {server_id2, {ServerName2, node()}},
     {uid3, atom_to_binary(ServerName3, utf8)},
     {server_id3, {ServerName3, node()}}
     | Config].

end_per_testcase(_TestCase, _Config) ->
    meck:unload(),
    ok.

%%%===================================================================
%%% Test cases
%%%===================================================================

send_msg_without_options(Config) ->
    Mod = ?config(modname, Config),
    meck:new(Mod, [non_strict]),
    meck:expect(Mod, init, fun (_) -> the_state end),
    meck:expect(Mod, apply, fun (_, {echo, Pid, Msg}, State) ->
                                    {State, ok, {send_msg, Pid, Msg}}
                            end),
    ClusterName = ?config(cluster_name, Config),
    ServerId = ?config(server_id, Config),
    ok = start_cluster(ClusterName, {module, Mod, #{}}, [ServerId]),
    {ok, ok, _} = ra:process_command(ServerId, {echo, self(), ?FUNCTION_NAME}),
    receive ?FUNCTION_NAME -> ok
    after 250 ->
              flush(),
              exit(receive_msg_timeout)
    end,
    ok.

send_msg_with_ra_event_option(Config) ->
    Mod = ?config(modname, Config),
    meck:new(Mod, [non_strict]),
    meck:expect(Mod, init, fun (_) -> the_state end),
    meck:expect(Mod, apply, fun (_, {echo, Pid, Msg}, State) ->
                                    {State, ok, {send_msg, Pid, Msg, ra_event}}
                            end),
    ClusterName = ?config(cluster_name, Config),
    ServerId = ?config(server_id, Config),
    ok = start_cluster(ClusterName, {module, Mod, #{}}, [ServerId]),
    {ok, ok, _} = ra:process_command(ServerId, {echo, self(), ?FUNCTION_NAME}),
    receive
        {ra_event, ServerId, {machine, ?FUNCTION_NAME}} -> ok
    after 250 ->
              flush(),
              exit(receive_msg_timeout)
    end,
    ok.

send_msg_with_cast_option(Config) ->
    Mod = ?config(modname, Config),
    meck:new(Mod, [non_strict]),
    meck:expect(Mod, init, fun (_) -> the_state end),
    meck:expect(Mod, apply, fun (_, {echo, Pid, Msg}, State) ->
                                    {State, ok, {send_msg, Pid, Msg, cast}}
                            end),
    ClusterName = ?config(cluster_name, Config),
    ServerId = ?config(server_id, Config),
    ok = start_cluster(ClusterName, {module, Mod, #{}}, [ServerId]),
    {ok, ok, _} = ra:process_command(ServerId, {echo, self(), ?FUNCTION_NAME}),
    receive
        {'$gen_cast', ?FUNCTION_NAME} -> ok
    after 250 ->
              flush(),
              exit(receive_msg_timeout)
    end,
    ok.

send_msg_with_ra_event_and_cast_options(Config) ->
    Mod = ?config(modname, Config),
    meck:new(Mod, [non_strict]),
    meck:expect(Mod, init, fun (_) -> the_state end),
    meck:expect(Mod, apply,
                fun (_, {echo, Pid, Msg}, State) ->
                        {State, ok, {send_msg, Pid, Msg, [ra_event, cast]}}
                end),
    ClusterName = ?config(cluster_name, Config),
    ServerId = ?config(server_id, Config),
    ok = start_cluster(ClusterName, {module, Mod, #{}}, [ServerId]),
    {ok, ok, _} = ra:process_command(ServerId, {echo, self(), ?FUNCTION_NAME}),
    receive
        {'$gen_cast', {ra_event, ServerId, {machine, ?FUNCTION_NAME}}} -> ok
    after 250 ->
              flush(),
              exit(receive_msg_timeout)
    end,
    ok.

machine_replies(Config) ->
    Mod = ?config(modname, Config),
    meck:new(Mod, [non_strict]),
    meck:expect(Mod, init, fun (_) -> the_state end),
    meck:expect(Mod, apply, fun (_, c1, State) ->
                                    {State, the_reply};
                                (_, c2, State) ->
                                    {State, {error, some_error_reply}}
                            end),
    ClusterName = ?config(cluster_name, Config),
    ServerId = ?config(server_id, Config),
    ok = start_cluster(ClusterName, {module, Mod, #{}}, [ServerId]),
    {ok, the_reply, ServerId} = ra:process_command(ServerId, c1),
    %% ensure we can return any reply type
    {ok, {error, some_error_reply}, ServerId} =
        ra:process_command(ServerId, c2),
    ok.

leader_monitors(Config) ->
    ClusterName = ?config(cluster_name, Config),
    ServerId = ?config(server_id, Config),
    Name = element(1, ServerId),
    Mod = ?config(modname, Config),
    meck:new(Mod, [non_strict]),
    meck:expect(Mod, init, fun (_) -> [] end),
    meck:expect(Mod, apply, fun (_, {monitor_me, Pid}, State) ->
                                    {[Pid | State], ok, {monitor, process, Pid}}
                            end),
    meck:expect(Mod, state_enter,
                fun (leader, State) ->
                        [{monitor, process, P} || P <- State];
                    (_, _) ->
                        []
                end),
    ok = start_cluster(ClusterName, {module, Mod, #{}}, [ServerId]),
    {ok, ok, ServerId} = ra:process_command(ServerId, {monitor_me, self()}),
    %% it is possible we get a reply before the process has finished setting up the
    %% monitor. A round trip through the state machine should make it more likely
    _ = ra:members(ServerId),
    {monitored_by, [MonitoredBy]} = erlang:process_info(self(), monitored_by),
    ?assert(MonitoredBy =:= whereis(Name)),
    ra:stop_server(?SYS, ServerId),
    _ = ra:restart_server(?SYS, ServerId),
    ra:members(ServerId),
    % check monitors are re-applied after restart
    timer:sleep(200),
    {monitored_by, [MonitoredByAfter]} = erlang:process_info(self(),
                                                             monitored_by),
    ?assert(MonitoredByAfter =:= whereis(Name)),
    ra:stop_server(?SYS, ServerId),
    ok.

down_follows_all_low_priority_commands(Config) ->
    ClusterName = ?config(cluster_name, Config),
    {_Name1, _} = ServerId1 = ?config(server_id, Config),
    {_Name2, _} = ServerId2 = ?config(server_id2, Config),
    {_Name3, _} = ServerId3 = ?config(server_id3, Config),
    Cluster = [ServerId1, ServerId2, ServerId3],
    Mod = ?config(modname, Config),
    Self = self(),
    meck:new(Mod, [non_strict]),
    meck:expect(Mod, init, fun (_) -> [] end),
    meck:expect(Mod, apply,
                fun (_, {monitor_me, Pid}, State) ->
                        ct:pal("monitoring ~p", [Pid]),
                        {[Pid | State], ok, [{monitor, process, Pid}]};
                    (_, {down, Pid, _}, State) ->
                        {lists:delete(Pid, State), ok, []};
                    (_, {cmd, Pid}, State) ->
                        % ct:pal("handling ~p", [Cmd]),
                        case lists:member(Pid, State) of
                            true ->
                                {State, ok};
                            false ->
                                {State, ok, [{send_msg, Self, {unexpected_cmd, Pid}}]}
                        end
                end),
    ok = start_cluster(ClusterName, {module, Mod, #{}}, Cluster),
    %% send some commands then exit swiftly
    spawn(
      fun () ->
              {ok, ok, L} = ra:process_command(ServerId1, {monitor_me, self()}),
              [ra:pipeline_command(L, {cmd, self()}) || _ <- lists:seq(1, 200)],
              Self ! done,
              ok
      end),

    receive
        done ->
            receive
                {unexpected_cmd, _} ->
                    ct:fail("Unexpexted command after down")
            after 2000 ->
                      ok
            end
    after 5000 ->
              exit(done_Timeout)
    end,


    ra:stop_server(?SYS, ServerId1),
    ra:stop_server(?SYS, ServerId2),
    ra:stop_server(?SYS, ServerId3),
    ok.

follower_takes_over_monitor(Config) ->
    ClusterName = ?config(cluster_name, Config),
    {_Name1, _} = ServerId1 = ?config(server_id, Config),
    {Name2, _} = ServerId2 = ?config(server_id2, Config),
    {Name3, _} = ServerId3 = ?config(server_id3, Config),
    Cluster = [ServerId1, ServerId2, ServerId3],
    Mod = ?config(modname, Config),
    meck:new(Mod, [non_strict]),
    meck:expect(Mod, init, fun (_) -> [] end),
    meck:expect(Mod, apply,
                fun (_, {monitor_me, Pid}, State) ->
                        {[Pid | State], ok, [{monitor, process, Pid}]};
                    (_, Cmd, State) ->
                        ct:pal("handling ~p", [Cmd]),
                        %% handle all
                        {State, ok}
                end),
    meck:expect(Mod, state_enter,
                fun (leader, State) ->
                        [{monitor, process, P} || P <- State];
                    (_, _) ->
                        []
                end),
    ok = start_cluster(ClusterName, {module, Mod, #{}}, Cluster),
    {ok, ok, {LeaderName, _}} =
        ra:process_command(ServerId1, {monitor_me, self()}),
    %% sleep here as it seems monitors, or this stat aren't updated synchronously
    timer:sleep(100),
    {monitored_by, [MonitoredBy]} = erlang:process_info(self(), monitored_by),
    ?assert(MonitoredBy =:= whereis(LeaderName)),

    ok = ra:stop_server(?SYS, ServerId1),
    % give the election process a bit of time before issuing a command
    timer:sleep(200),
    {ok, _, _} = ra:process_command(ServerId2, dummy),
    timer:sleep(200),

    {monitored_by, [MonitoredByAfter]} = erlang:process_info(self(),
                                                             monitored_by),
    ?assert((MonitoredByAfter =:= whereis(Name2)) or
            (MonitoredByAfter =:= whereis(Name3))),
    ra:stop_server(?SYS, ServerId1),
    ra:stop_server(?SYS, ServerId2),
    ra:stop_server(?SYS, ServerId3),
    ok.

deleted_cluster_emits_eol_effect(Config) ->
    PrivDir = ?config(priv_dir, Config),
    ServerId = ?config(server_id, Config),
    UId = ?config(uid, Config),
    ClusterName = ?config(cluster_name, Config),
    Mod = ?config(modname, Config),
    meck:new(Mod, [non_strict]),
    meck:expect(Mod, init, fun (_) -> [] end),
    meck:expect(Mod, apply,
                fun (_, {monitor_me, Pid}, State) ->
                        {[Pid | State], ok, [{monitor, process, Pid}]}
                end),
    meck:expect(Mod, state_enter,
                fun (eol, State) ->
                        [{send_msg, P, eol, ra_event} || P <- State];
                    (_, _) ->
                        []
                end),
    ok = start_cluster(ClusterName, {module, Mod, #{}}, [ServerId]),
    {ok, ok, _} = ra:process_command(ServerId, {monitor_me, self()}),
    {ok, _} = ra:delete_cluster([ServerId]),
    % validate
    ok = validate_process_down(element(1, ServerId), 50),
    Dir = filename:join(PrivDir, UId),
    false = filelib:is_dir(Dir),
    timer:sleep(100),
    [] = supervisor:which_children(ra_server_sup_sup),
    % validate an end of life is emitted
    receive
        {ra_event, _, {machine, eol}} -> ok
    after 500 ->
          exit(timeout)
    end,
    ok.

machine_state_enter_effects(Config) ->
    ServerId = ?config(server_id, Config),
    ClusterName = ?config(cluster_name, Config),
    Mod = ?config(modname, Config),
    Self = self(),
    meck:new(Mod, [non_strict]),
    meck:expect(Mod, init, fun (_) -> [] end),
    meck:expect(Mod, apply,
                fun (_, _, State) ->
                        {State, [], ok}
                end),
    meck:expect(Mod, state_enter,
                fun (RaftState, _State) ->
                        Self ! {state_enter, RaftState},
                        []
                end),
    ok = start_cluster(ClusterName, {module, Mod, #{}}, [ServerId]),
    ra:delete_cluster([ServerId]),
    validate_state_enters([recover, recovered, follower,
                           candidate, leader, eol]),
    ok.

meta_data(Config) ->
    Mod = ?config(modname, Config),
    meck:new(Mod, [non_strict]),
    meck:expect(Mod, init, fun (_) -> the_state end),
    meck:expect(Mod, apply, fun (#{index := Idx,
                                   term := Term,
                                   system_time := Ts,
                                   reply_mode := await_consensus}, _, State) ->
                                    {State, {metadata, Idx, Term, Ts}}
                            end),
    ClusterName = ?config(cluster_name, Config),
    ServerId = ?config(server_id, Config),
    T = os:system_time(millisecond),
    ok = start_cluster(ClusterName, {module, Mod, #{}}, [ServerId]),
    {ok, {metadata, Idx, Term, Ts}, ServerId} =
        ra:process_command(ServerId, any_command),

    ?assert(Ts > T),
    ?assert(Idx > 0),
    ?assert(Term > 0),
    ok.

meta_data_2(Config) ->
    Mod = ?config(modname, Config),
    Self = self(),
    meck:new(Mod, [non_strict]),
    meck:expect(Mod, init, fun (_) -> the_state end),
    meck:expect(Mod, apply, fun (#{index := Idx,
                                   term := Term,
                                   system_time := Ts,
                                   reply_mode := {notify, 42, Pid}}, _, State)
                                  when Pid == Self ->
                                    {State, {metadata, Idx, Term, Ts}}
                            end),
    ClusterName = ?config(cluster_name, Config),
    ServerId = ?config(server_id, Config),
    T = erlang:system_time(millisecond),
    timer:sleep(1),
    ok = start_cluster(ClusterName, {module, Mod, #{}}, [ServerId]),
    ok = ra:pipeline_command(ServerId, any_command, 42, normal),
    receive
        {ra_event, _, {applied, [{42, {metadata, _, _, Ts}}]}}
          when Ts > T ->
            ok
    after 5000 ->
              flush(),
              ct:fail("applied not received")
    end.


append_effect(Config) ->
    Mod = ?config(modname, Config),
    Self = self(),
    meck:new(Mod, [non_strict]),
    meck:expect(Mod, init, fun (_) -> the_state end),
    meck:expect(Mod, apply, fun (_, cmd, State) ->
                                    %% timer for 1s
                                    {State, ok, [{append, {cmd2, "yo"}}]};
                                (_, {cmd2, "yo"}, State) ->
                                    {State, ok, [{send_msg, Self, got_cmd2}]}
                            end),
    ClusterName = ?config(cluster_name, Config),
    ServerId = ?config(server_id, Config),
    ok = start_cluster(ClusterName, {module, Mod, #{}}, [ServerId]),
    {ok, _, ServerId} = ra:process_command(ServerId, cmd),
    receive
        got_cmd2 ->
            ok
    after 1000 ->
              flush(),
              exit(cmd2_timeout)
    end,
    ok.

append_effect_with_notify(Config) ->
    Mod = ?config(modname, Config),
    Self = self(),
    meck:new(Mod, [non_strict]),
    meck:expect(Mod, init, fun (_) -> the_state end),
    meck:expect(Mod, apply, fun (_, cmd, State) ->
                                    %% timer for 1s
                                    Notify = {notify, 42, Self},
                                    {State, ok, [{append, {cmd2, "yo"}, Notify}]};
                                (_, {cmd2, "yo"}, State) ->
                                    {State, ok, [{send_msg, Self, got_cmd2}]}
                            end),
    ClusterName = ?config(cluster_name, Config),
    ServerId = ?config(server_id, Config),
    ServerId2 = ?config(server_id2, Config),
    ServerId3 = ?config(server_id3, Config),

    ok = start_cluster(ClusterName, {module, Mod, #{}},
                       [ServerId, ServerId2, ServerId3]),
    {ok, _, _Leader} = ra:process_command(ServerId, cmd),
    receive
        {ra_event, _, {applied, [{42, ok}]}} = Evt ->
            ct:pal("Got ~p", [Evt])
    after 1000 ->
              flush(),
              exit(ra_event_timeout)
    end,
    receive
        got_cmd2 ->
            ok
    after 1000 ->
              flush(),
              exit(cmd2_timeout)
    end,
    flush(),
    ok.

append_effect_follower(Config) ->
    %% the append effect is issued against a follower from an aux handler
    Mod = ?config(modname, Config),
    Self = self(),
    meck:new(Mod, [non_strict]),
    meck:expect(Mod, init, fun (_) -> the_state end),
    meck:expect(Mod, apply, fun
                                (_, {cmd2, "yo"}, State) ->
                                    {State, ok, [{send_msg, Self, got_cmd2}]}
                            end),
    %% have to use the special try_append here as {append, should only be
    %% applied to the leader
    meck:expect(Mod, handle_aux, fun
                                     (_, _, {cmd, ReplyMode}, Aux, Log, _MacState) ->
                                         {no_reply, Aux, Log,
                                          [{try_append, {cmd2, "yo"}, ReplyMode}]};
                                     (_, _, _Evt, Aux, Log, _MacState) ->
                                         {no_reply, Aux, Log}
                                 end),
    ClusterName = ?config(cluster_name, Config),
    ServerId = ?config(server_id, Config),
    ServerId2 = ?config(server_id2, Config),
    ServerId3 = ?config(server_id3, Config),

    ok = start_cluster(ClusterName, {module, Mod, #{}},
                       [ServerId, ServerId2, ServerId3]),
    {ok, Members, Leader} = ra:members(ServerId),
    [Follower | _] = lists:delete(Leader, Members),
    %% send an untracked aux command, which should cause the follower to
    %% forward the append effect to the known leader
    ok = ra:cast_aux_command(Follower, {cmd, noreply}),
    receive
        got_cmd2 ->
            ok
    after 1000 ->
              flush(),
              exit(cmd2_timeout)
    end,

    %% cast a tracked (correlated) command via aux handler
    %% This should be rejected (as it is tracked).
    Corr = make_ref(),
    ok = ra:cast_aux_command(Follower, {cmd, {notify, Corr, self()}}),
    receive
        {ra_event, Follower, {rejected, {not_leader, Leader2, Corr}}}  ->
            %% the command got rejected, resend to leader
            ok = ra:cast_aux_command(Leader2, {cmd, {notify, Corr, self()}}),
            receive
                got_cmd2 ->
                    %% the command was appended and applied
                    flush(),
                    ok
            after 1000 ->
                      flush(),
                      exit(cmd2_timeout)
            end
    after 1000 ->
              flush(),
              exit(ra_event_timeout)
    end,
    ok.

timer_effect(Config) ->
    Mod = ?config(modname, Config),
    Self = self(),
    meck:new(Mod, [non_strict]),
    meck:expect(Mod, init, fun (_) -> the_state end),
    meck:expect(Mod, apply, fun (_, {cmd, Name}, State) ->
                                    %% timer for 1s
                                    {State, ok, {timer, Name, 1000}};
                                (_, {timeout, Name}, State) ->
                                    {State, ok, {send_msg, Self, {got_timeout, Name}}}
                            end),
    ClusterName = ?config(cluster_name, Config),
    ServerId = ?config(server_id, Config),
    ok = start_cluster(ClusterName, {module, Mod, #{}}, [ServerId]),
    T0 = os:system_time(millisecond),
    {ok, _, ServerId} = ra:process_command(ServerId, {cmd, one}),
    timer:sleep(500),
    {ok, _, ServerId} = ra:process_command(ServerId, {cmd, two}),
    receive
        {got_timeout, one} ->
            T = os:system_time(millisecond),
            %% ensure the timer waited
            ?assert(T-T0 >= 1000),
            receive
                {got_timeout, two} ->
                    T1 = os:system_time(millisecond),
                    ?assert(T1-T0 >= 1500),
                    ok
            after 2000 ->
                      flush(),
                      exit(timeout_timeout_two)
            end
    after 5000 ->
              flush(),
              exit(timeout_timeout)
    end,
    ok.

log_effect(Config) ->
    Mod = ?config(modname, Config),
    Self = self(),
    meck:new(Mod, [non_strict]),
    meck:expect(Mod, init, fun (_) -> [] end),
    meck:expect(Mod, apply, fun (#{index := Idx}, {cmd, _Data}, Idxs) ->
                                    %% stash all indexes
                                    {[Idx | Idxs], ok};
                                (_, get_data, Idxs) ->
                                    %% now we need to refresh the data from
                                    %% the log and turn it into a send_msg
                                    %% effect
                                    {[], ok,
                                     {log, lists:reverse(Idxs),
                                      fun (Cmds) ->
                                              Datas = [D || {_, D} <- Cmds],
                                              %% using a plain send here to
                                              %% ensure this effect is only
                                              %% evaluated on leader
                                              Self ! {datas, Datas},
                                              []
                                      end}}
                            end),
    ClusterName = ?config(cluster_name, Config),
    ServerId1 = ?config(server_id, Config),
    ServerId2 = ?config(server_id2, Config),
    ServerId3 = ?config(server_id3, Config),
    ok = start_cluster(ClusterName, {module, Mod, #{}},
                       [ServerId1, ServerId2, ServerId3]),
    {ok, _, ServerId} = ra:process_command(ServerId1, {cmd, <<"hi1">>}),
    {ok, _, ServerId} = ra:process_command(ServerId, {cmd, <<"hi2">>}),
    {ok, ok, ServerId} = ra:process_command(ServerId, get_data),
    receive
        {datas, [<<"hi1">>, <<"hi2">>]} ->
            receive
                {datas, [<<"hi1">>, <<"hi2">>]} ->
                    ct:fail("unexpected second log effect execution"),
                    ok
            after 100 ->
                      ok
            end
    after 5000 ->
              flush(),
              exit(data_timeout)
    end,
    ok.

aux_handler_not_impl(Config) ->
    ClusterName = ?config(cluster_name, Config),
    ServerId1 = ?config(server_id, Config),
    Cluster = [ServerId1,
               ?config(server_id2, Config),
               ?config(server_id3, Config)],
    Mod = ?config(modname, Config),
    meck:new(Mod, [non_strict]),
    meck:expect(Mod, init, fun (_) -> [] end),
    meck:expect(Mod, init_aux, fun (_) -> undefined end),
    meck:expect(Mod, apply,
                fun (_, {monitor_me, Pid}, State) ->
                        {[Pid | State], ok, [{monitor, process, Pid}]};
                    (_, Cmd, State) ->
                        ct:pal("handling ~p", [Cmd]),
                        %% handle all
                        {State, ok}
                end),
    ok = start_cluster(ClusterName, {module, Mod, #{}}, Cluster),
    {ok, _, Leader} = ra:members(ServerId1),
    {error, aux_handler_not_implemented} = ra:aux_command(Leader, emit),
    ok.

aux_command(Config) ->
    ClusterName = ?config(cluster_name, Config),
    ServerId1 = ?config(server_id, Config),
    Cluster = [ServerId1,
               ?config(server_id2, Config),
               ?config(server_id3, Config)],
    Mod = ?config(modname, Config),
    meck:new(Mod, [non_strict]),
    meck:expect(Mod, init, fun (_) -> [] end),
    meck:expect(Mod, init_aux, fun (_) -> undefined end),
    meck:expect(Mod, apply,
                fun (_, {monitor_me, Pid}, State) ->
                        {[Pid | State], ok, [{monitor, process, Pid}]};
                    (_, Cmd, State) ->
                        ct:pal("handling ~p", [Cmd]),
                        %% handle all
                        {State, ok}
                end),
    meck:expect(Mod, handle_aux,
                fun
                    (RaftState, {call, _From}, emit, AuxState, Log, _MacState) ->
                        %% emits aux state
                        {reply, {RaftState, AuxState}, AuxState, Log};
                    (_RaftState, cast, eval, AuxState, Log, _MacState) ->
                        %% replaces aux state
                        {no_reply, AuxState, Log};
                    (_RaftState, cast, NewState, _AuxState, Log, _MacState) ->
                        %% replaces aux state
                        {no_reply, NewState, Log}

                end),
    ok = start_cluster(ClusterName, {module, Mod, #{}}, Cluster),
    {ok, _, Leader} = ra:members(ServerId1),
    ok = ra:cast_aux_command(Leader, banana),
    {leader, banana} = ra:aux_command(Leader, emit),
    [ServerId2, ServerId3] = Cluster -- [Leader],
    {follower, undefined} = ra:aux_command(ServerId2, emit),
    ok = ra:cast_aux_command(ServerId2, apple),
    {follower, apple} = ra:aux_command(ServerId2, emit),
    {follower, undefined} = ra:aux_command(ServerId3, emit),
    ok = ra:cast_aux_command(ServerId3, orange),
    {follower, orange} = ra:aux_command(ServerId3, emit),
    ra:delete_cluster(Cluster),
    ok.

aux_command_v2(Config) ->
    ClusterName = ?config(cluster_name, Config),
    ServerId1 = ?config(server_id, Config),
    Cluster = [ServerId1,
               ?config(server_id2, Config),
               ?config(server_id3, Config)],
    Mod = ?config(modname, Config),
    meck:new(Mod, [non_strict]),
    meck:expect(Mod, init, fun (_) -> [] end),
    meck:expect(Mod, init_aux, fun (_) -> undefined end),
    meck:expect(Mod, apply,
                fun (_, {monitor_me, Pid}, State) ->
                        {[Pid | State], ok, [{monitor, process, Pid}]};
                    (_, Cmd, State) ->
                        ct:pal("handling ~p", [Cmd]),
                        %% handle all
                        {State, ok}
                end),
    meck:expect(Mod, handle_aux,
                fun
                    (RaftState, {call, _From}, emit, AuxState, Opaque) ->
                        %% emits aux state
                        {reply, {RaftState, AuxState}, AuxState, Opaque};
                    (_RaftState, cast, eval, AuxState, Opaque) ->
                        %% replaces aux state
                        {no_reply, AuxState, Opaque};
                    (_RaftState, cast, NewState, _AuxState, Opaque) ->
                        %% replaces aux state
                        {no_reply, NewState, Opaque}

                end),
    ok = start_cluster(ClusterName, {module, Mod, #{}}, Cluster),
    {ok, _, Leader} = ra:members(ServerId1),
    ok = ra:cast_aux_command(Leader, banana),
    {leader, banana} = ra:aux_command(Leader, emit),
    [ServerId2, ServerId3] = Cluster -- [Leader],
    {follower, undefined} = ra:aux_command(ServerId2, emit),
    ok = ra:cast_aux_command(ServerId2, apple),
    {follower, apple} = ra:aux_command(ServerId2, emit),
    {follower, undefined} = ra:aux_command(ServerId3, emit),
    ok = ra:cast_aux_command(ServerId3, orange),
    {follower, orange} = ra:aux_command(ServerId3, emit),
    ra:delete_cluster(Cluster),
    ok.

aux_command_v1_and_v2(Config) ->
    ClusterName = ?config(cluster_name, Config),
    ServerId1 = ?config(server_id, Config),
    Cluster = [ServerId1,
               ?config(server_id2, Config),
               ?config(server_id3, Config)],
    Mod = ?config(modname, Config),
    meck:new(Mod, [non_strict]),
    meck:expect(Mod, init, fun (_) -> [] end),
    meck:expect(Mod, init_aux, fun (_) -> undefined end),
    meck:expect(Mod, apply,
                fun (_, {monitor_me, Pid}, State) ->
                        {[Pid | State], ok, [{monitor, process, Pid}]};
                    (_, Cmd, State) ->
                        ct:pal("handling ~p", [Cmd]),
                        %% handle all
                        {State, ok}
                end),
    meck:expect(Mod, handle_aux,
                fun
                    (_RaftState, _, _, _AuxState, _Log, _MacState) ->
                        exit(wrong_callback)
                end),
    meck:expect(Mod, handle_aux,
                fun
                    (RaftState, {call, _From}, emit, AuxState, Opaque) ->
                        %% emits aux state
                        {reply, {RaftState, AuxState}, AuxState, Opaque};
                    (_RaftState, cast, eval, AuxState, Opaque) ->
                        %% replaces aux state
                        {no_reply, AuxState, Opaque};
                    (_RaftState, cast, tick, AuxState, Opaque) ->
                        %% replaces aux state
                        {no_reply, AuxState, Opaque};
                    (_RaftState, cast, NewState, _AuxState, Opaque0) ->
                        {Idx, _} = ra_aux:log_last_index_term(Opaque0),
                        {{_Term, _Meta, apple}, Opaque} = ra_aux:log_fetch(Idx, Opaque0),
                        %% replaces aux state
                        {no_reply, NewState, Opaque}

                end),
    ok = start_cluster(ClusterName, {module, Mod, #{}}, Cluster),
    {ok, _, Leader} = ra:members(ServerId1),
    {ok, _, _} = ra:process_command(Leader, apple),
    ok = ra:cast_aux_command(Leader, banana),
    {leader, banana} = ra:aux_command(Leader, emit),
    [ServerId2, ServerId3] = Cluster -- [Leader],
    {follower, undefined} = ra:aux_command(ServerId2, emit),
    ok = ra:cast_aux_command(ServerId2, apple),
    {follower, apple} = ra:aux_command(ServerId2, emit),
    {follower, undefined} = ra:aux_command(ServerId3, emit),
    ok = ra:cast_aux_command(ServerId3, orange),
    {follower, orange} = ra:aux_command(ServerId3, emit),
    ra:delete_cluster(Cluster),
    ok.

aux_command_timeout(Config) ->
    ClusterName = ?config(cluster_name, Config),
    ServerId1 = ?config(server_id, Config),
    Cluster = [ServerId1,
               ?config(server_id2, Config),
               ?config(server_id3, Config)],
    Mod = ?config(modname, Config),
    meck:new(Mod, [non_strict]),
    meck:expect(Mod, init, fun (_) -> [] end),
    meck:expect(Mod, init_aux, fun (_) -> undefined end),
    meck:expect(Mod, apply,
                fun
                    (_, Cmd, State) ->
                        ct:pal("handling ~p", [Cmd]),
                        %% handle all
                        {State, ok}
                end),
    meck:expect(Mod, handle_aux,
                fun
                    (_RaftState, {call, _From}, {sleep, Sleep}, AuxState, Opaque) ->
                        timer:sleep(Sleep),
                        {reply, ok, AuxState, Opaque};
                    (_RaftState, cast, _Msg, AuxState, Opaque) ->
                        {no_reply, AuxState, Opaque}
                end),
    ok = start_cluster(ClusterName, {module, Mod, #{}}, Cluster),
    ?assertEqual(ok, ra:aux_command(ServerId1, {sleep, 100}, 500)),
    ?assertExit({timeout, _}, ra:aux_command(ServerId1, {sleep, 1000}, 500)),
    ra:delete_cluster(Cluster),
    ok.

aux_eval(Config) ->
    %% aux handle is automatically passed an eval command after new entries
    %% have been applied
    ok = logger:set_primary_config(level, all),
    ClusterName = ?config(cluster_name, Config),
    ServerId1 = ?config(server_id, Config),
    Cluster = [ServerId1,
               ?config(server_id2, Config),
               ?config(server_id3, Config)],
    Mod = ?config(modname, Config),
    Self = self(),
    meck:new(Mod, [non_strict]),
    meck:expect(Mod, init, fun (_) -> [] end),
    meck:expect(Mod, apply,
                fun (_, Cmd, State) ->
                        ct:pal("handling ~p", [Cmd]),
                        {State, ok}
                end),
    meck:expect(Mod, init_aux, fun (_) -> undefined end),
    meck:expect(Mod, handle_aux,
                fun
                    (_RaftState, _, eval, AuxState, Log, _MacState) ->
                        %% monitors a process
                        Self ! got_eval,
                        {no_reply, AuxState, Log, []};
                    (_RaftState, _, _, AuxState, Log, _MacState) ->
                        {no_reply, AuxState, Log, []}
                end),
    ok = start_cluster(ClusterName, {module, Mod, #{}}, Cluster),
    {ok, _, Leader} = ra:members(ServerId1),

    ok = ra:pipeline_command(Leader, dummy),
    receive
        got_eval -> ok
    after 2500 ->
              flush(),
              exit(got_eval_1)
    end,
    receive
        got_eval -> ok
    after 2500 ->
              flush(),
              exit(got_eval_2)
    end,
    receive
        got_eval -> ok
    after 2500 ->
              flush(),
              exit(got_eval_3)
    end,
    ra:delete_cluster(Cluster),
    ok.

aux_tick(Config) ->
    %% aux handle is automatically passed an eval command after new entries
    %% have been applied
    ok = logger:set_primary_config(level, all),
    ClusterName = ?config(cluster_name, Config),
    ServerId1 = ?config(server_id, Config),
    Cluster = [ServerId1,
               ?config(server_id2, Config),
               ?config(server_id3, Config)],
    Mod = ?config(modname, Config),
    Self = self(),
    meck:new(Mod, [non_strict]),
    meck:expect(Mod, init, fun (_) -> [] end),
    meck:expect(Mod, apply,
                fun (_, Cmd, State) ->
                        ct:pal("handling ~p", [Cmd]),
                        {State, ok}
                end),
    meck:expect(Mod, init_aux, fun (_) -> undefined end),
    meck:expect(Mod, handle_aux,
                fun
                    (_RaftState, _, tick, AuxState, Log, _MacState) ->
                        Self ! got_tick,
                        {no_reply, AuxState, Log, []};
                    (_RaftState, _, eval, AuxState, Log, _MacState) ->
                        {no_reply, AuxState, Log, []}
                end),
    ok = start_cluster(ClusterName, {module, Mod, #{}}, Cluster),
    {ok, _, Leader} = ra:members(ServerId1),

    ok = ra:pipeline_command(Leader, dummy),
    receive
        got_tick -> ok
    after 2500 ->
              flush(),
              exit(got_tick_1)
    end,
    receive
        got_tick -> ok
    after 2500 ->
              flush(),
              exit(got_tick_2)
    end,
    receive
        got_tick -> ok
    after 2500 ->
              flush(),
              exit(got_tick_3)
    end,
    ra:delete_cluster(Cluster),
    ok.

aux_monitor_effect(Config) ->
    ok = logger:set_primary_config(level, all),
    ClusterName = ?config(cluster_name, Config),
    ServerId1 = ?config(server_id, Config),
    Cluster = [ServerId1,
               ?config(server_id2, Config),
               ?config(server_id3, Config)],
    Mod = ?config(modname, Config),
    Self = self(),
    meck:new(Mod, [non_strict]),
    meck:expect(Mod, init, fun (_) -> [] end),
    meck:expect(Mod, apply,
                fun (_, Cmd, State) ->
                        ct:pal("handling ~p", [Cmd]),
                        {State, ok}
                end),
    meck:expect(Mod, init_aux, fun (_) -> undefined end),
    meck:expect(Mod, handle_aux,
                fun
                    (_RaftState, _, eval, AuxState, Log, _MacState) ->
                        {no_reply, AuxState, Log};
                    (_RaftState, _, tick, AuxState, Log, _MacState) ->
                        {no_reply, AuxState, Log};
                    (_RaftState, _, {monitor, Pid}, AuxState, Log, _MacState) ->
                        %% monitors a process
                        {no_reply, AuxState, Log, [{monitor, process, aux, Pid}]};
                    (_RaftState, _, {down, Pid, _Info}, AuxState, Log, _MacState) ->
                        %% replaces aux state
                        Self ! {down_received, Pid},
                        {no_reply, AuxState, Log}
                end),
    ok = start_cluster(ClusterName, {module, Mod, #{}}, Cluster),
    {ok, _, Leader} = ra:members(ServerId1),

    P = spawn(fun () ->
                      receive
                          pls_exit -> ok
                      end
              end),
    ok = ra:cast_aux_command(Leader, {monitor, P}),
    P ! pls_exit,
    receive
        {down_received, P} ->
            ok
    after 2500 ->
              flush(),
              exit(down_recieved_timeout)
    end,
    ok.

aux_and_machine_monitor_same_process(Config) ->
    ok = logger:set_primary_config(level, all),
    ClusterName = ?config(cluster_name, Config),
    ServerId1 = ?config(server_id, Config),
    Cluster = [ServerId1,
               ?config(server_id2, Config),
               ?config(server_id3, Config)],
    Mod = ?config(modname, Config),
    Self = self(),
    meck:new(Mod, [non_strict]),
    meck:expect(Mod, init, fun (_) -> [] end),
    meck:expect(Mod, apply,
                fun
                    (_, {down, P, _} = Cmd, State) ->
                        ct:pal("handling ~p", [Cmd]),
                        {State, ok, {send_msg, Self, {got_down, machine, P}}};
                    (_, {monitor, P} = Cmd, State) ->
                        ct:pal("handling ~p", [Cmd]),
                        {State, ok, {monitor, process, P}}
                end),
    meck:expect(Mod, init_aux, fun (_) -> undefined end),
    meck:expect(Mod, handle_aux,
                fun
                    (_RaftState, _, eval, AuxState, Log, _MacState) ->
                        {no_reply, AuxState, Log};
                    (_RaftState, _, tick, AuxState, Log, _MacState) ->
                        {no_reply, AuxState, Log};
                    (_RaftState, _, {monitor, Pid}, AuxState, Log, _MacState) ->
                        %% monitors a process
                        {no_reply, AuxState, Log,
                         [{monitor, process, aux, Pid}]};
                    (_RaftState, _, {down, P, _Info}, AuxState, Log, _MacState) ->
                        %% replaces aux state
                        Self ! {got_down, aux, P},
                        {no_reply, AuxState, Log}
                end),
    ok = start_cluster(ClusterName, {module, Mod, #{}}, Cluster),
    {ok, _, Leader} = ra:members(ServerId1),
    [Follower1, _Follower2] = Cluster -- [Leader],

    P = spawn(fun () ->
                      receive
                          pls_exit -> ok
                      end
              end),
    {ok, _, _} = ra:process_command(Leader, {monitor, P}),
    ok = ra:cast_aux_command(Follower1, {monitor, P}),
    P ! pls_exit,
    receive
        {got_down, machine, P} ->
            receive
                {got_down, aux, P} ->
                    ok
            after 2500 ->
                      flush(),
                      exit(got_down_aux)
            end
    after 2500 ->
              flush(),
              exit(got_down_machine)
    end,
    ok.

aux_and_machine_monitor_same_node(Config) ->
    ok = logger:set_primary_config(level, all),
    ClusterName = ?config(cluster_name, Config),
    ServerId1 = ?config(server_id, Config),
    Cluster = [ServerId1,
               ?config(server_id2, Config),
               ?config(server_id3, Config)],
    Mod = ?config(modname, Config),
    Self = self(),
    meck:new(Mod, [non_strict]),
    meck:expect(Mod, init, fun (_) -> [] end),
    meck:expect(Mod, apply,
                fun
                    (_, {nodedown, Node} = Cmd, State) ->
                        ct:pal("handling ~p", [Cmd]),
                        Self ! {got_down, machine, Node},
                        {State, ok};
                    (_, {monitor, P} = Cmd, State) ->
                        ct:pal("handling ~p", [Cmd]),
                        {State, ok, {monitor, node, P}}
                end),
    meck:expect(Mod, init_aux, fun (_) -> undefined end),
    meck:expect(Mod, handle_aux,
                fun
                    (_RaftState, _, eval, AuxState, Log, _MacState) ->
                        {no_reply, AuxState, Log};
                    (_RaftState, _, tick, AuxState, Log, _MacState) ->
                        {no_reply, AuxState, Log};
                    (_RaftState, _, {monitor, Node}, AuxState, Log, _MacState) ->
                        %% monitors a process
                        {no_reply, AuxState, Log,
                         [{monitor, node, aux, Node}]};
                    (_RaftState, _, {nodedown, Node}, AuxState, Log, _MacState) ->
                        %% replaces aux state
                        Self ! {got_down, aux, Node},
                        {no_reply, AuxState, Log}
                end),
    ok = start_cluster(ClusterName, {module, Mod, #{}}, Cluster),
    {ok, _, Leader} = ra:members(ServerId1),
    Node = fake_node@banana,
    {ok, _, _} = ra:process_command(Leader, {monitor, Node}),
    ok = ra:cast_aux_command(Leader, {monitor, Node}),
    %% as the fake node isn't connected it should generate a node down immediately
    receive
        {got_down, machine, Node} ->
            receive
                {got_down, aux, Node} ->
                    ok
            after 2500 ->
                      flush(),
                      exit(got_down_aux)
            end
    after 2500 ->
              flush(),
              exit(got_down_machine)
    end,
    ra:delete_cluster(Cluster),
    ok.

aux_and_machine_monitor_leader_change(Config) ->
    ok = logger:set_primary_config(level, all),
    ClusterName = ?config(cluster_name, Config),
    ServerId1 = ?config(server_id, Config),
    Cluster = [ServerId1,
               ?config(server_id2, Config),
               ?config(server_id3, Config)],
    Mod = ?config(modname, Config),
    Self = self(),
    meck:new(Mod, [non_strict]),
    meck:expect(Mod, init, fun (_) -> [] end),
    meck:expect(Mod, apply,
                fun
                    (_, {down, P, _} = Cmd, State) ->
                        ct:pal("down handling ~p", [Cmd]),
                        {lists:delete(P, State), ok,
                         {send_msg, Self, {got_down, machine, P}}};
                    (_, {monitor, P} = Cmd, State) ->
                        ct:pal("handling ~p", [Cmd]),
                        {[P | State], ok, {monitor, process, P}}
                end),
    meck:expect(Mod, state_enter,
                fun (leader, State) ->
                        [{monitor, process, P} || P <- State];
                    (_, _) ->
                        []
                end),
    meck:expect(Mod, init_aux, fun (_) -> undefined end),
    meck:expect(Mod, handle_aux,
                fun
                    (_RaftState, _, eval, AuxState, Log, _MacState) ->
                        {no_reply, AuxState, Log};
                    (_RaftState, _, tick, AuxState, Log, _MacState) ->
                        {no_reply, AuxState, Log};
                    (_RaftState, _, {monitor, Pid}, AuxState, Log, _MacState) ->
                        %% monitors a process
                        {no_reply, AuxState, Log,
                         [{monitor, process, aux, Pid}]};
                    (_RaftState, _, {down, P, _Info}, AuxState, Log, _MacState) ->
                        %% replaces aux state
                        Self ! {got_down, aux, P},
                        {no_reply, AuxState, Log}
                end),
    ok = start_cluster(ClusterName, {module, Mod, #{}}, Cluster),
    {ok, _, Leader} = ra:members(ServerId1),
    [Follower1, Follower2] = Cluster -- [Leader],

    P = spawn(fun () ->
                      receive
                          pls_exit -> ok
                      end
              end),
    {ok, _, _} = ra:process_command(Leader, {monitor, P}),
    ok = ra:cast_aux_command(Follower2, {monitor, P}),
    ok = ra:cast_aux_command(Follower1, {monitor, P}),
    % timer:sleep(100),
    LeaderPid = whereis(element(1, Leader)),
    %% check the leader is monitoring P
    await(fun () ->
                  {monitored_by, M} = process_info(P, monitored_by),
                  lists:member(LeaderPid, M)
          end, 100),
    ok = ra:transfer_leadership(Leader, Follower1),
    {ok, _, Follower1 = _NewLeader} = ra:members(Follower1),
    %% after a leader transfer P should no longer be monitored by the previous
    %% leader as all machine monitors should be invalidated when a leader steps
    %% down
    await(fun () ->
                  {monitored_by, M} = process_info(P, monitored_by),
                  not lists:member(LeaderPid, M)
          end, 100),
    % terminate P with `normal'
    P ! pls_exit,
    %% assert both aux nodes have retained their monitors
    %% and the new leader also got a mechine monitor down
    receive
        {got_down, aux, P} ->
            receive
                {got_down, aux, P} ->
                    receive
                        {got_down, machine, P} ->
                            ok
                    after 2500 ->
                              exit(got_down_machine)
                    end
            after 2500 ->
                      exit(got_down_aux)
            end
    after 2500 ->
              exit(got_down_aux_2)
    end,
    assert_flush(),
    ok.

%% Utility

validate_state_enters(States) ->
    lists:foreach(fun (S) ->
                          receive {state_enter, S} -> ok
                          after 250 ->
                                    flush(),
                                    ct:pal("S ~w", [S]),
                                    exit({timeout, S})
                          end
                  end, States).

start_cluster(ClusterName, Machine, ServerIds) ->
    {ok, Started, _} = ra:start_cluster(?SYS, ClusterName, Machine, ServerIds),
    ?assertEqual(length(ServerIds), length(Started)),
    ok.

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

assert_flush() ->
    receive
        Any ->
            ct:pal("flush ~p", [Any]),
            exit({flush_expected_no_messages, Any})
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

