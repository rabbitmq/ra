%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2017-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%
-module(ra_2_SUITE).
-behaviour(ra_machine).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-define(info, true).
-define(SYS, ?MODULE).

%% common ra_log tests to ensure behaviour is equivalent across
%% ra_log backends

all() ->
    [
     {group, tests}
    ].

all_tests() ->
    [
     start_stopped_server,
     server_is_force_deleted,
     force_deleted_server_mem_tables_are_cleaned_up,
     leave_and_delete_server,
     cluster_is_deleted,
     cluster_is_deleted_with_server_down,
     cluster_cannot_be_deleted_in_minority,
     server_restart_after_application_restart,
     restarted_server_does_not_reissue_side_effects,
     recover,
     supervision_tree,
     recover_after_kill,
     start_server_uid_validation,
     custom_ra_event_formatter,
     config_modification_at_restart,
     segment_writer_handles_server_deletion,
     external_reader,
     add_member_without_quorum,
     initial_members_query
    ].

groups() ->
    [
     {tests, [], all_tests()}
    ].

init_per_suite(Config) ->
    PrivDir = ?config(priv_dir, Config),
    SysDir = filename:join(PrivDir, ?SYS),
    {ok, _} = ra:start([{data_dir, PrivDir},
                        {segment_max_entries, 128}]),
    SysCfg = #{name => ?SYS,
               names => ra_system:derive_names(?SYS),
               segment_max_entries => 128,
               data_dir => SysDir},
    ct:pal("SYS CFG ~p", [SysCfg]),
    {ok, _} = ra_system:start(SysCfg),
    application:ensure_all_started(lg),
    [{sys_cfg, SysCfg} | Config].

end_per_suite(Config) ->
    _ = application:stop(ra),
    Config.

init_per_group(_, Config) ->
    Config.

end_per_group(_, Config) ->
    Config.

init_per_testcase(TestCase, Config) ->
    ra_server_sup_sup:remove_all(?SYS),
    ServerName2 = list_to_atom(atom_to_list(TestCase) ++ "2"),
    ServerName3 = list_to_atom(atom_to_list(TestCase) ++ "3"),
    ServerName4 = list_to_atom(atom_to_list(TestCase) ++ "4"),
    [{test_case, TestCase},
     {modname, TestCase},
     {cluster_name, TestCase},
     {uid, atom_to_binary(TestCase, utf8)},
     {server_id, {TestCase, node()}},
     {uid2, atom_to_binary(ServerName2, utf8)},
     {server_id2, {ServerName2, node()}},
     {uid3, atom_to_binary(ServerName3, utf8)},
     {server_id3, {ServerName3, node()}},
     {uid4, atom_to_binary(ServerName4, utf8)},
     {server_id4, {ServerName4, node()}}
     | Config].

enqueue(Server, Msg) ->
    {ok, _, _} = ra:process_command(Server, {enq, Msg}),
    ok.

dequeue(Server) ->
    {ok, Res, _} = ra:process_command(Server, deq),
    Res.

start_stopped_server(Config) ->
    %% ra:start_server should fail if the server already exists
    ClusterName = ?config(cluster_name, Config),
    PrivDir = ?config(priv_dir, Config),
    ServerId = ?config(server_id, Config),
    UId = ?config(uid, Config),
    Conf = conf(ClusterName, UId, ServerId, PrivDir, []),
    ok = ra:start_server(?SYS, Conf),
    ok = ra:trigger_election(ServerId),
    ok = enqueue(ServerId, msg1),
    %%
    {error, {already_started, _}} = ra:start_server(?SYS, Conf),
    ok = ra:stop_server(?SYS, ServerId),
    {error, not_new} = ra:start_server(?SYS, Conf),
    ok = ra:restart_server(?SYS, ServerId),
    ok.


server_is_force_deleted(Config) ->
    ClusterName = ?config(cluster_name, Config),
    PrivDir = ?config(priv_dir, Config),
    ServerId = ?config(server_id, Config),
    UId = ?config(uid, Config),
    Conf = conf(ClusterName, UId, ServerId, PrivDir, []),
    _ = ra:start_server(?SYS, Conf),
    ok = ra:trigger_election(ServerId),
    ok = enqueue(ServerId, msg1),
    % force roll over
    ok = force_roll_over(),
    Pid = ra_directory:where_is(?SYS, UId),
    ok = ra:force_delete_server(?SYS, ServerId),

    validate_ets_table_deletes([UId], [Pid], [ServerId]),
    % start a node with the same nodeid but different uid
    % simulating the case where a queue got deleted then re-declared shortly
    % afterwards
    UId2 = ?config(uid2, Config),
    ok = ra:start_server(?SYS, Conf#{uid => UId2,
                               log_init_args => #{
                                                  uid => UId2}}),
    ok = ra:trigger_election(ServerId),
    case dequeue(ServerId) of
        empty -> ok;
        _ ->
            exit(unexpected_dequeue_result)
    end,

    ok = ra:force_delete_server(?SYS, ServerId),
    ok.

force_deleted_server_mem_tables_are_cleaned_up(Config) ->
    ClusterName = ?config(cluster_name, Config),
    PrivDir = ?config(priv_dir, Config),
    ServerId = ?config(server_id, Config),
    UId = ?config(uid, Config),
    Conf = conf(ClusterName, UId, ServerId, PrivDir, []),
    _ = ra:start_server(?SYS, Conf),
    ok = ra:trigger_election(ServerId),
    ok = enqueue(ServerId, msg1),

    ok = ra:force_delete_server(?SYS, ServerId),

    #{names := #{open_mem_tbls := OpnMemTbls,
                 closed_mem_tbls := ClosedMemTbls,
                 wal := Wal,
                 segment_writer := SegWriter}} = ra_system:fetch(?SYS),

    [{_, _, _, Tid}] = ets:lookup(OpnMemTbls, UId),
    % force roll over after
    ok = ra_log_wal:force_roll_over(Wal),
    timer:sleep(100),
    ra_log_segment_writer:await(SegWriter),

    %% validate there are no mem tables for this server anymore
    ?assertMatch(undefined, ets:info(Tid)),
    ?assertMatch([], ets:lookup(ClosedMemTbls, UId)),

    ok.

leave_and_delete_server(Config) ->
    ok = logger:set_primary_config(level, all),
    ClusterName = ?config(cluster_name, Config),
    ServerId1 = ?config(server_id, Config),
    ServerId2 = ?config(server_id2, Config),
    ServerId3 = ?config(server_id3, Config),
    Peers = [ServerId1, ServerId2, ServerId3],
    ok = start_cluster(ClusterName, Peers),
    %% due to timing it is possible that cluster changes
    %% are not yet allowed and thus will time out. Safest to synchronously process
    %% a command first
    {ok, ok, _Leader} = ra:process_command(ServerId1, {enq, msg1}),
    ?assert(undefined =/= whereis(element(1, ServerId2))),
    ok = ra:leave_and_delete_server(?SYS, Peers, ServerId2, 2000),
    ?assertEqual(undefined, whereis(element(1, ServerId2))),
    {ok, _} = ra:delete_cluster(Peers),
    ok.

cluster_is_deleted(Config) ->
    ClusterName = ?config(cluster_name, Config),
    ServerId1 = ?config(server_id, Config),
    ServerId2 = ?config(server_id2, Config),
    ServerId3 = ?config(server_id3, Config),
    Peers = [ServerId1, ServerId2, ServerId3],
    ok = start_cluster(ClusterName, Peers),
    % timer:sleep(100),
    UIds = [ ra_directory:uid_of(?SYS, Name) || {Name, _} <- Peers],
    Pids = [ ra_directory:where_is(?SYS, Name) || {Name, _} <- Peers],
    Leader = ra_leaderboard:lookup_leader(ClusterName),
    ?assert(lists:member(Leader, Peers)),
    %% redeclaring the same cluster should fail
    {error, cluster_not_formed} = ra:start_cluster(?SYS, ClusterName,
                                                   {module, ?MODULE, #{}},
                                                   Peers),
    {ok, _} = ra:delete_cluster(Peers),
    %% Assert all ETS tables are deleted for each UId

    validate_ets_table_deletes(UIds, Pids, Peers),
    ?assertEqual(undefined, ra_leaderboard:lookup_leader(ClusterName)),

    ok = start_cluster(ClusterName, Peers),
    ok.

validate_ets_table_deletes(UIds, Pids, Peers) ->
    timer:sleep(500),
    UIdTables = [ra_directory,
                 ra_log_meta,
                 ra_state,
                 ra_log_snapshot_state,
                 ra_log_metrics
                ],
    [begin
         ct:pal("validate_ets_table_deletes ~w in ~w", [Key, Tab]),
         [] = ets:lookup(Tab, Key)
     end || Key <- UIds, Tab <- UIdTables],

    %% validate by registered name is also cleaned up
    [ [] = ets:lookup(T, Name) || {Name, _} <- Peers,
                                    T <-  [ra_metrics,
                                           ra_state]],

    %% validate open file metrics is cleaned up
    [ [] = ets:lookup(T, Pid) || Pid <- Pids,
                                 T <-  [ra_open_file_metrics
                                       ]],
    ok.

cluster_is_deleted_with_server_down(Config) ->
    %% cluster deletion is a coordingated consensus action
    %% The leader will commit and replicate a "poison pill" message
    %% Once each follower applies this messages it terminates and deletes all
    %% it's data
    %% the leader waits until the poison pill message has been replicated to
    %% _all_ followers then terminates and deletes it's own data.
    ClusterName = ?config(cluster_name, Config),
    ServerId1 = ?config(server_id, Config),
    ServerId2 = ?config(server_id2, Config),
    ServerId3 = ?config(server_id3, Config),
    Peers = [ServerId1, ServerId2, ServerId3],
    ok = start_cluster(ClusterName, Peers),
    timer:sleep(100),
    [begin
         UId = ra_directory:uid_of(?SYS, Name),
         ?assert(filelib:is_dir(filename:join([data_dir(), UId])))
     end || {Name, _} <- Peers],

    % check data dirs exist for all nodes
    % Wildcard = lists:flatten(filename:join([PrivDir, "**"])),
    % assert there are three matching data dirs

    ok = ra:stop_server(?SYS, ServerId3),
    {ok, _} = ra:delete_cluster(Peers),
    timer:sleep(100),
    % start node again
    ra:restart_server(?SYS, ServerId3),
    % validate all nodes have been shut down and terminated
    ok = validate_process_down(element(1, ServerId1), 10),
    ok = validate_process_down(element(1, ServerId2), 10),
    ok = validate_process_down(element(1, ServerId3), 10),

    % validate there are no data dirs anymore
    [ begin
          UId = ra_directory:uid_of(?SYS, Name),
          ?assert(false =:= filelib:is_dir(filename:join([data_dir(), UId])))
      end || {Name, _} <- Peers],
    ok.

cluster_cannot_be_deleted_in_minority(Config) ->
    ClusterName = ?config(cluster_name, Config),
    ServerId1 = ?config(server_id, Config),
    ServerId2 = ?config(server_id2, Config),
    ServerId3 = ?config(server_id3, Config),
    Peers = [ServerId1, ServerId2, ServerId3],
    ok = start_cluster(ClusterName, Peers),
    % check data dirs exist for all nodes
    [begin
         UId = ra_directory:uid_of(?SYS, Name),
         ?assert(filelib:is_dir(filename:join([data_dir(), UId])))
     end || {Name, _} <- Peers],
    ra:stop_server(?SYS, ServerId2),
    ra:stop_server(?SYS, ServerId3),
    {error, {no_more_servers_to_try, Err}} = ra:delete_cluster(lists:reverse(Peers), 250),
    ct:pal("Err~p", [Err]),
    ra:stop_server(?SYS, ServerId1),
    ok.

server_restart_after_application_restart(Config) ->
    ServerId = ?config(server_id, Config),
    ClusterName = ?config(cluster_name, Config),
    ok = start_cluster(ClusterName, [ServerId]),
    ok= enqueue(ServerId, msg1),
    application:stop(ra),
    ra:start(),
    %% start system
    {ok, _} = ra_system:start(?config(sys_cfg, Config)),
    %
    % restart node
    ok = ra:restart_server(?SYS, ServerId),
    ok= enqueue(ServerId, msg2),
    ok = ra:stop_server(?SYS, ServerId),
    ok.


% NB: this is not guaranteed not to re-issue side-effects but only tests
% that the likelyhood is small
restarted_server_does_not_reissue_side_effects(Config) ->
    ServerId = ?config(server_id, Config),
    Name = element(1, ServerId),
    ClusterName = ?config(cluster_name, Config),
    ok = start_cluster(ClusterName, [ServerId]),
    ok = enqueue(ServerId, msg1),
    {ok, _, _} = ra:process_command(ServerId, {deq, self()}),
    receive
        {ra_event, _, {machine, msg1}} ->
            ok
    after 2000 ->
              exit(ra_event_timeout)
    end,
    % give the process time to persist the last_applied index
    timer:sleep(1000),
    % kill the process and have it restarted by the supervisor
    exit(whereis(Name), kill),

    %  check message isn't received again
    receive
        {ra_event, _, {machine, msg1}} ->
            exit(unexpected_ra_event)
    after 1000 ->
              ok
    end,
    ok = ra:stop_server(?SYS, ServerId),
    ok.

recover(Config) ->
    ServerId = ?config(server_id, Config),
    ClusterName = ?config(cluster_name, Config),
    ok = start_cluster(ClusterName, [ServerId]),
    ok = enqueue(ServerId, msg1),
    ra:members(ServerId),
    ra:stop_server(?SYS, ServerId),
    ra_log_wal:force_roll_over(ra_log_wal),
    timer:sleep(1000),
    % start_profile(Config, [ra_server,
    %                        ra_server_prop,
    %                        ra_system,
    %                        ra_log,
    %                        ra_lib,
    %                        ra_snapshot,
    %                        ra_log_reader,
    %                        ra_log_segment_writer,
    %                        filelib,
    %                        file,
    %                        prim_file,
    %                        dets,
    %                        ra_directory
    %                        ]),
    {Time, _} = timer:tc(fun () -> ra:restart_server(?SYS, ServerId) end),
    ct:pal("Server restart took ~b", [Time div 1000]),
    % stop_profile(Config),
    ra:members(ServerId),

    msg1 = dequeue(ServerId),

    ok = ra:stop_server(?SYS, ServerId),
    ok.

supervision_tree(Config) ->
    ServerId = {Name, _} = ?config(server_id, Config),
    ClusterName = ?config(cluster_name, Config),
    ok = start_cluster(ClusterName, [ServerId]),
    % start another cluster
    ok = start_cluster(another_cluster, [{another, node()}]),
    %% kill server in close succession
    %% 3 crashes in a 5s period should stop restart attempts
    exit(whereis(Name), kill),
    timer:sleep(1000),
    ?assert(whereis(Name) =/= undefined),
    exit(whereis(Name), kill),
    timer:sleep(250),
    ?assert(whereis(Name) =/= undefined),
    exit(whereis(Name), kill),
    timer:sleep(250),
    %% assert server is permanently down
    ?assert(whereis(Name) == undefined),
    %% assert another is still up
    ?assert(whereis(another) =/= undefined),
    ok.

recover_after_kill(Config) ->
    ServerId = {Name, _} = ?config(server_id, Config),
    ClusterName = ?config(cluster_name, Config),
    ok = start_cluster(ClusterName, [ServerId]),
    ra:members(ServerId),
    ok = enqueue(ServerId, msg1),
    {F2, Deqd} = enq_deq_n(64, ServerId),
    % timer:sleep(100),
    exit(whereis(Name), kill),
    application:stop(ra),
    ra:start(),
    {ok, _} = ra_system:start(?config(sys_cfg, Config)),
    ra:restart_server(?SYS, ServerId),
    ra:members(ServerId),
    %% this should by the ?SYS release cursor interval of 128
    %% create a new snapshot
    {_F3, _AllDeq} = enq_deq_n(65, F2, Deqd),
    {ok, MS, _} = ra:consistent_query(ServerId, fun (S) -> S end),
    %% kill node again to trigger post snapshot recovery
    exit(whereis(Name), kill),
    timer:sleep(250),
    ra:members(ServerId),
    timer:sleep(200),
    % give leader time to commit noop
    {ok, MS2, _} = ra:consistent_query(ServerId, fun (S) -> S end),
    ok = ra:stop_server(?SYS, ServerId),
    ?assertEqual(MS, MS2),
    ok = ra:restart_server(?SYS, ServerId),
    {ok, _, _} = ra:members(ServerId, 30000),
    {ok, MS3, _} = ra:consistent_query(ServerId, fun (S) -> S end),
    ct:pal("~p ~p", [MS2, MS3]),
    ?assertEqual(MS2, MS3),
    ok.

start_server_uid_validation(Config) ->
    ServerId = ?config(server_id, Config),
    UId = <<"ADSFASDFø"/utf8>>,
    Conf = #{cluster_name => ?config(cluster_name, Config),
             id => ServerId,
             uid => UId,
             initial_members => [ServerId],
             log_init_args => #{uid => UId},
             machine => {module, ?MODULE, #{}}},
    {error, invalid_uid} = ra:start_server(?SYS, Conf),
    {error, invalid_uid} = ra:start_server(?SYS, Conf#{uid => <<"">>}),
    ok.

custom_ra_event_formatter(Config) ->
    ServerId = ?config(server_id, Config),
    UId = <<"ADSFASDF"/utf8>>,
    Conf = #{cluster_name => ?config(cluster_name, Config),
             id => ServerId,
             uid => UId,
             initial_members => [ServerId],
             log_init_args => #{uid => UId},
             ra_event_formatter => {?MODULE, format_ra_event, [my_arg]},
             machine => {module, ?MODULE, #{}}},
    ok = ra:start_server(?SYS, Conf),
    ra:trigger_election(ServerId),
    _ = ra:members(ServerId),
    ra:pipeline_command(ServerId, {enq, msg1}, make_ref()),
    receive
        {custom_event, ServerId, {applied, _}} ->
            ok
    after 2000 ->
              flush(),
              exit(custom_event_timeout)
    end,
    ok.

config_modification_at_restart(Config) ->
    ServerId = ?config(server_id, Config),
    UId = <<"ADSFASDF2"/utf8>>,
    Conf = #{cluster_name => ?config(cluster_name, Config),
             id => ServerId,
             uid => UId,
             initial_members => [ServerId],
             log_init_args => #{uid => UId},
             machine => {module, ?MODULE, #{}}},
    ok = ra:start_server(?SYS, Conf),
    ra:trigger_election(ServerId),
    _ = ra:members(ServerId),
    ra:pipeline_command(ServerId, {enq, msg1}, make_ref()),
    receive
        {ra_event, ServerId, {applied, _}} ->
            ok
    after 2000 ->
              exit(ra_event_timeout)
    end,

    ok = ra:stop_server(?SYS, ServerId),

    %% add an event formatter at restart
    AddConfig = #{ra_event_formatter => {?MODULE, format_ra_event, [my_arg]}},
    ok = ra:restart_server(?SYS, ServerId, AddConfig),
    ra:members(ServerId),
    ra:pipeline_command(ServerId, {enq, msg1}, make_ref()),
    receive
        {custom_event, ServerId, {applied, _}} ->
            ok
    after 2000 ->
              flush(),
              exit(custom_event_timeout)
    end,

    %% do another restart
    ok = ra:stop_server(?SYS, ServerId),
    %% add an event formatter at restart
    ok = ra:restart_server(?SYS, ServerId, #{}),
    ra:members(ServerId),
    ra:pipeline_command(ServerId, {enq, msg1}, make_ref()),
    receive
        {custom_event, ServerId, {applied, _}} ->
            ok
    after 2000 ->
              flush(),
              exit(custom_event_timeout)
    end,
    ok.

segment_writer_handles_server_deletion(Config) ->
    %% start single node. bang in a whole load of entries - enough to fill at least
    %% 1 and a bit segments
    %% roll the wal then very shortly after delete the server
    ServerId = ?config(server_id, Config),
    ClusterName = ?config(cluster_name, Config),
    ok = start_cluster(ClusterName, [ServerId]),
    ra:members(ServerId),
    ok = enqueue(ServerId, msg1),
    [begin
         _ = ra:pipeline_command(ServerId, {enq, N})
     end || N <- lists:seq(1, 1023)],
    _ = enqueue(ServerId, final),
    ok = force_roll_over(),
    MRef = erlang:monitor(process, whereis(ra_log_segment_writer)),
    timer:sleep(5),
    ra:delete_cluster([ServerId]),

    %% assert segment writer did not crash
    receive
        {'DOWN', MRef, _Type, _Object, _Info} ->
            exit(down_received)
    after 1000 ->
              ok
    end,
    ok.

external_reader(Config) ->
    ok = logger:set_primary_config(level, all),
    ServerId = ?config(server_id, Config),
    ClusterName = ?config(cluster_name, Config),
    ok = start_cluster(ClusterName, [ServerId]),
    ra:members(ServerId),
    ok = enqueue(ServerId, msg1),
    [begin
         _ = ra:pipeline_command(ServerId, {enq, N}, no_correlation, normal)
     end || N <- lists:seq(1, 1023)],
    _ = enqueue(ServerId, final),
    R0 = ra:register_external_log_reader(ServerId),
    ok = force_roll_over(),
    receive
        {ra_event, _, {machine, {ra_log_update, _, _, _} = E}} ->
            R1 = ra_log_reader:handle_log_update(E, R0),
            {Entries, _, _R2} = ra_log_reader:read(0, 1026, R1),
            ct:pal("read ~w ~w", [length(Entries), lists:last(Entries)]),
            %% read all entries
            ok
    after 3000 ->
              flush(),
              exit(ra_log_update_timeout)
    end,
    ra:delete_cluster([ServerId]),
    ok.

add_member_without_quorum(Config) ->
    ok = logger:set_primary_config(level, all),
    %% ra:start_server should fail if the node already exists
    ClusterName = ?config(cluster_name, Config),
    PrivDir = ?config(priv_dir, Config),
    ServerId1 = ?config(server_id, Config),
    ServerId2 = ?config(server_id2, Config),
    ServerId3 = ?config(server_id3, Config),
    InitialCluster = [ServerId1, ServerId2, ServerId3],
    ok = start_cluster(ClusterName, InitialCluster),
    %% stop followers
    {ok, _, Leader} = ra:members(hd(InitialCluster)),
    ok = enqueue(Leader, msg1),
    Followers = lists:delete(Leader, InitialCluster),
    [ra:stop_server(?SYS, F) || F <- Followers],
    ServerId4 = ?config(server_id4, Config),
    UId4 = ?config(uid4, Config),
    Conf4 = conf(ClusterName, UId4, ServerId4, PrivDir, InitialCluster),
    ok = ra:start_server(?SYS, Conf4),
    %% this works because the leader is still up to append a cluster change to its log
    %% the change won't be applied/replicated though, because there's no quorum
    {ok, _, _} = ra:add_member(Leader, ServerId4),
    ServerId5 = ?config(server_id5, Config),
    %% the previous cluster change could not be applied, so cluster changes are not permitted
    {error, cluster_change_not_permitted} = ra:add_member(Leader, ServerId5),
    {error, cluster_change_not_permitted} = ra:remove_member(Leader, ServerId1),
    %% start one follower
    timer:sleep(1000),
    ok = ra:restart_server(?SYS, hd(Followers)),
    timer:sleep(1000),
    ok = enqueue(Leader, msg1),
    {error, already_member} = ra:add_member(Leader, ServerId4),
    {error, not_member} = ra:remove_member(Leader, ServerId5),
    %%
    % timer:sleep(5000),
    ok.

initial_members_query(Config) ->
    ok = logger:set_primary_config(level, all),
    %% ra:start_server should fail if the node already exists
    ClusterName = ?config(cluster_name, Config),
    PrivDir = ?config(priv_dir, Config),
    ServerId1 = ?config(server_id, Config),
    ServerId2 = ?config(server_id2, Config),
    ServerId3 = ?config(server_id3, Config),
    InitialCluster = [ServerId1, ServerId2, ServerId3],
    ok = start_cluster(ClusterName, InitialCluster),
    {ok, Members, Leader} = ra:members(hd(InitialCluster)),
    {ok, InitialMembers, _} = ra:initial_members(Leader),
    ?assertEqual(lists:sort(Members),
                 lists:sort(InitialMembers)),
    ServerId4 = ?config(server_id4, Config),
    UId4 = ?config(uid4, Config),
    Conf4 = conf(ClusterName, UId4, ServerId4, PrivDir, InitialCluster),
    ok = ra:start_server(?SYS, Conf4),
    {ok, _, _} = ra:add_member(Leader, ServerId4),
    {ok, InitialMembers2, _} = ra:initial_members(Leader),
    ?assertEqual(lists:sort(Members),
                 lists:sort(InitialMembers2)),
    ok.

format_ra_event(SrvId, Evt, my_arg) ->
    {custom_event, SrvId, Evt}.

enq_deq_n(N, ServerId) ->
    enq_deq_n(N, ServerId, []).

enq_deq_n(0, F, Acc) ->
    {F, Acc};
enq_deq_n(N, ServerId, Acc) ->
    ok = enqueue(ServerId, N),
    Deq = dequeue(ServerId),
    true = Deq /= empty,
    enq_deq_n(N-1, ServerId, [Deq | Acc]).

conf(ClusterName, UId, ServerId, _, Peers) ->
    #{cluster_name => ClusterName,
      id => ServerId,
      uid => UId,
      log_init_args => #{uid => UId},
      initial_members => Peers,
      machine => {module, ?MODULE, #{}}}.


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

start_cluster(ClusterName, ServerIds, Config) ->
    {ok, Started, _} = ra:start_cluster(?SYS, ClusterName,
                                        {module, ?MODULE, Config},
                                        ServerIds),
    ?assertEqual(lists:sort(ServerIds), lists:sort(Started)),
    ok.

start_cluster(ClusterName, ServerIds) ->
    start_cluster(ClusterName, ServerIds, #{}).

%% ra_machine test impl
init(_) ->
    queue:new().

'apply'(_Meta, {enq, Msg}, State) ->
    {queue:in(Msg, State), ok};
'apply'(_Meta, deq, State0) ->
    case queue:out(State0) of
        {{value, Item}, State} ->
            {State, Item};
        {empty, _} ->
            {State0, empty}
    end;
'apply'(_Meta, {deq, Pid}, State0) ->
    case queue:out(State0) of
        {{value, Item}, State} ->
            {State, ok, {send_msg, Pid, Item, ra_event}};
        {empty, _} ->
            {State0, ok}
    end.

state_enter(eol, State) ->
    [{send_msg, P, eol, ra_event} || {P, _} <- queue:to_list(State), is_pid(P)];
state_enter(S, _) ->
    ct:pal("state_enter ~w", [S]),
    [].

flush() ->
    receive
        Any ->
            ct:pal("flush ~p", [Any]),
            flush()
    after 0 ->
              ok
    end.

data_dir() ->
    maps:get(data_dir, ra_system:fetch(?SYS)).

force_roll_over() ->
    #{names := #{wal := Wal}} = ra_system:fetch(?SYS),
    ra_log_wal:force_roll_over(Wal).

start_profile(Config, Modules) ->
    Dir = ?config(priv_dir, Config),
    Case = ?config(test_case, Config),
    GzFile = filename:join([Dir, "lg_" ++ atom_to_list(Case) ++ ".gz"]),
    ct:pal("Profiling to ~p", [GzFile]),

    lg:trace(Modules, lg_file_tracer,
             GzFile, #{running => false, mode => profile}).

stop_profile(Config) ->
    Case = ?config(test_case, Config),
    ct:pal("Stopping profiling for ~p", [Case]),
    lg:stop(),
    % this segfaults
    Dir = ?config(priv_dir, Config),
    Name = filename:join([Dir, "lg_" ++ atom_to_list(Case)]),
    lg_callgrind:profile_many(Name ++ ".gz.*", Name ++ ".out",#{}),
    ok.

