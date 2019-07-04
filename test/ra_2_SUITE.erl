-module(ra_2_SUITE).
-behaviour(ra_machine).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-define(info, true).

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
     cluster_is_deleted,
     cluster_is_deleted_with_server_down,
     cluster_cannot_be_deleted_in_minority,
     server_restart_after_application_restart,
     restarted_server_does_not_reissue_side_effects,
     recover,
     supervision_tree,
     recover_after_kill,
     start_server_uid_validation
    ].

groups() ->
    [
     {tests, [], all_tests()}
    ].

init_per_group(_, Config) ->
    PrivDir = ?config(priv_dir, Config),
    {ok, _} = ra:start_in(PrivDir),
    application:ensure_all_started(lg),
    Config.

end_per_group(_, Config) ->
    _ = application:stop(ra),
    Config.

init_per_testcase(TestCase, Config) ->
    ra_server_sup_sup:remove_all(),
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

enqueue(Server, Msg) ->
    {ok, _, _} = ra:process_command(Server, {enq, Msg}),
    ok.

dequeue(Server) ->
    {ok, Res, _} = ra:process_command(Server, deq),
    Res.

start_stopped_server(Config) ->
    %% ra:start_server should fail if the node already exists
    ClusterName = ?config(cluster_name, Config),
    PrivDir = ?config(priv_dir, Config),
    ServerId = ?config(server_id, Config),
    UId = ?config(uid, Config),
    Conf = conf(ClusterName, UId, ServerId, PrivDir, []),
    ok = ra:start_server(Conf),
    ok = ra:trigger_election(ServerId),
    ok = enqueue(ServerId, msg1),
    %%
    {error, {already_started, _}} = ra:start_server(Conf),
    ok = ra:stop_server(ServerId),
    {error, not_new} = ra:start_server(Conf),
    ok = ra:restart_server(ServerId),
    ok.


server_is_force_deleted(Config) ->
    ClusterName = ?config(cluster_name, Config),
    PrivDir = ?config(priv_dir, Config),
    ServerId = ?config(server_id, Config),
    UId = ?config(uid, Config),
    Conf = conf(ClusterName, UId, ServerId, PrivDir, []),
    _ = ra:start_server(Conf),
    ok = ra:trigger_election(ServerId),
    ok = enqueue(ServerId, msg1),
    % force roll over
    ok = ra_log_wal:force_roll_over(ra_log_wal),
    Pid = ra_directory:where_is(UId),
    ok = ra:force_delete_server(ServerId),

    validate_ets_table_deletes([UId], [Pid], [ServerId]),
    % start a node with the same nodeid but different uid
    % simulatin the case where a queue got deleted then re-declared shortly
    % afterwards
    UId2 = ?config(uid2, Config),
    ok = ra:start_server(Conf#{uid => UId2,
                               log_init_args => #{data_dir => PrivDir,
                                                  uid => UId2}}),
    ok = ra:trigger_election(ServerId),
    case dequeue(ServerId) of
        empty -> ok;
        _ ->
            exit(unexpected_dequeue_result)
    end,

    ok = ra:force_delete_server(ServerId),
    ok.

force_deleted_server_mem_tables_are_cleaned_up(Config) ->
    ClusterName = ?config(cluster_name, Config),
    PrivDir = ?config(priv_dir, Config),
    ServerId = ?config(server_id, Config),
    UId = ?config(uid, Config),
    Conf = conf(ClusterName, UId, ServerId, PrivDir, []),
    _ = ra:start_server(Conf),
    ok = ra:trigger_election(ServerId),
    ok = enqueue(ServerId, msg1),

    ok = ra:force_delete_server(ServerId),

    [{_, _, _, Tid}] = ets:lookup(ra_log_open_mem_tables, UId),
    % force roll over after
    ok = ra_log_wal:force_roll_over(ra_log_wal),
    timer:sleep(100),
    ra_log_segment_writer:await(),

    %% validate there are no mem tables for this server anymore
    ?assertMatch(undefined, ets:info(Tid)),
    ?assertMatch([], ets:lookup(ra_log_closed_mem_tables, UId)),

    ok.


cluster_is_deleted(Config) ->
    ClusterName = ?config(cluster_name, Config),
    ServerId1 = ?config(server_id, Config),
    ServerId2 = ?config(server_id2, Config),
    ServerId3 = ?config(server_id3, Config),
    Peers = [ServerId1, ServerId2, ServerId3],
    ok = start_cluster(ClusterName, Peers),
    % timer:sleep(100),
    UIds = [ ra_directory:uid_of(Name) || {Name, _} <- Peers],
    Pids = [ ra_directory:where_is(Name) || {Name, _} <- Peers],
    %% redeclaring the same cluster should fail
    {error, cluster_not_formed} = ra:start_cluster(ClusterName,
                                                   {module, ?MODULE, #{}},
                                                   Peers),
    {ok, _} = ra:delete_cluster(Peers),
    %% Assert all ETS tables are deleted for each UId

    validate_ets_table_deletes(UIds, Pids, Peers),
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
         UId = ra_directory:uid_of(Name),
         ?assert(filelib:is_dir(filename:join([ra_env:data_dir(), UId])))
     end || {Name, _} <- Peers],

    % check data dirs exist for all nodes
    % Wildcard = lists:flatten(filename:join([PrivDir, "**"])),
    % assert there are three matching data dirs

    ok = ra:stop_server(ServerId3),
    {ok, _} = ra:delete_cluster(Peers),
    timer:sleep(100),
    % start node again
    ra:restart_server(ServerId3),
    % validate all nodes have been shut down and terminated
    ok = validate_process_down(element(1, ServerId1), 10),
    ok = validate_process_down(element(1, ServerId2), 10),
    ok = validate_process_down(element(1, ServerId3), 10),

    % validate there are no data dirs anymore
    [ begin
          UId = ra_directory:uid_of(Name),
          ?assert(false =:= filelib:is_dir(filename:join([ra_env:data_dir(), UId])))
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
         UId = ra_directory:uid_of(Name),
         ?assert(filelib:is_dir(filename:join([ra_env:data_dir(), UId])))
     end || {Name, _} <- Peers],
    ra:stop_server(ServerId2),
    ra:stop_server(ServerId3),
    {error, {no_more_servers_to_try, Err}} = ra:delete_cluster(lists:reverse(Peers), 250),
    ct:pal("Err~p", [Err]),
    ra:stop_server(ServerId1),
    ok.

server_restart_after_application_restart(Config) ->
    ServerId = ?config(server_id, Config),
    ClusterName = ?config(cluster_name, Config),
    ok = start_cluster(ClusterName, [ServerId]),
    ok= enqueue(ServerId, msg1),
    application:stop(ra),
    application:start(ra),
    % restart node
    ok = ra:restart_server(ServerId),
    ok= enqueue(ServerId, msg2),
    ok = ra:stop_server(ServerId),
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
    ok = ra:stop_server(ServerId),
    ok.

recover(Config) ->
    ServerId = ?config(server_id, Config),
    ClusterName = ?config(cluster_name, Config),
    ok = start_cluster(ClusterName, [ServerId]),
    ok = enqueue(ServerId, msg1),
    ra:members(ServerId),
    ra:stop_server(ServerId),
    ra:restart_server(ServerId),
    ra:members(ServerId),
    msg1 = dequeue(ServerId),

    ok = ra:stop_server(ServerId),
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
    application:start(ra),
    ra:restart_server(ServerId),
    ra:members(ServerId),
    %% this should by the default release cursor interval of 128
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
    ok = ra:stop_server(ServerId),
    % ct:pal("Indexes ~p ~p~nMS: ~p ~n MS2: ~p~nDequeued ~p~n",
    %        [X, X2, MS, MS2, AllDeq]),
    ?assertEqual(MS, MS2),
    ok = ra:restart_server(ServerId),
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
    {error, invalid_uid} = ra:start_server(Conf),
    {error, invalid_uid} = ra:start_server(Conf#{uid => <<"">>}),
    ok.

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
    {ok, Started, _} = ra:start_cluster(ClusterName,
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
