-module(ra_machine_int_SUITE).

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
     send_msg_without_options,
     send_msg_with_ra_event_option,
     send_msg_with_cast_option,
     send_msg_with_ra_event_and_cast_options,
     machine_replies,
     leader_monitors,
     follower_takes_over_monitor,
     deleted_cluster_emits_eol_effect,
     machine_state_enter_effects
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
    ra_server_sup:remove_all(),
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
    ClusterName = ?config(priv_dir, Config),
    ServerId = ?config(server_id, Config),
    Name = element(1, ServerId),
    Mod = ?config(modname, Config),
    meck:new(Mod, [non_strict]),
    meck:expect(Mod, init, fun (_) -> [] end),
    meck:expect(Mod, apply, fun (_, {monitor_me, Pid}, State) ->
                                    {[Pid | State], ok,  {monitor, process, Pid}}
                            end),
    meck:expect(Mod, state_enter,
                fun (leader, State) ->
                        [{monitor, process, P} || P <- State];
                    (_, _) ->
                        []
                end),
    ok = start_cluster(ClusterName, {module, Mod, #{}}, [ServerId]),
    {ok, ok, ServerId} = ra:process_command(ServerId, {monitor_me, self()}),
    {monitored_by, [MonitoredBy]} = erlang:process_info(self(), monitored_by),
    ?assert(MonitoredBy =:= whereis(Name)),
    ra:stop_server(ServerId),
    _ = ra:restart_server(ServerId),
    ra:members(ServerId),
    % check monitors are re-applied after restart
    timer:sleep(200),
    {monitored_by, [MonitoredByAfter]} = erlang:process_info(self(),
                                                             monitored_by),
    ?assert(MonitoredByAfter =:= whereis(Name)),
    ra:stop_server(ServerId),
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

    ok = ra:stop_server(ServerId1),
    % give the election process a bit of time before issuing a command
    timer:sleep(200),
    {ok, _, _} = ra:process_command(ServerId2, dummy),
    timer:sleep(200),

    {monitored_by, [MonitoredByAfter]} = erlang:process_info(self(),
                                                             monitored_by),
    ?assert((MonitoredByAfter =:= whereis(Name2)) or
            (MonitoredByAfter =:= whereis(Name3))),
    ra:stop_server(ServerId1),
    ra:stop_server(ServerId2),
    ra:stop_server(ServerId3),
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
    [] = supervisor:which_children(ra_server_sup),
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
                        [{send_msg, Self, {state_enter, RaftState}, ra_event}]
                end),
    ok = start_cluster(ClusterName, {module, Mod, #{}}, [ServerId]),
    ra:delete_cluster([ServerId]),
    validate_state_enters([recover, recovered, follower,
                           candidate, leader, eol]),
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
    {ok, Started, _} = ra:start_cluster(ClusterName, Machine, ServerIds),
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
