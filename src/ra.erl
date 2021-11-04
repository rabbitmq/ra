%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2017-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%
%% @doc The primary module for interacting with ra servers and clusters.

-module(ra).

-include("ra.hrl").

-export([
         start/0,
         start/1,
         start_in/1,
         %% command execution
         process_command/2,
         process_command/3,
         pipeline_command/2,
         pipeline_command/3,
         pipeline_command/4,
         %% queries
         members/1,
         members/2,
         initial_members/1,
         initial_members/2,
         local_query/2,
         local_query/3,
         leader_query/2,
         leader_query/3,
         consistent_query/2,
         consistent_query/3,
         % cluster operations
         start_cluster/2,
         start_cluster/3,
         start_cluster/4,
         start_or_restart_cluster/4,
         start_or_restart_cluster/5,
         delete_cluster/1,
         delete_cluster/2,
         % server management
         % deprecated
         start_server/1,
         start_server/2,
         start_server/5,
         % deprecated
         restart_server/1,
         restart_server/2,
         restart_server/3,
         % deprecated
         stop_server/1,
         stop_server/2,
         force_delete_server/2,
         trigger_election/1,
         trigger_election/2,
         %% membership changes
         add_member/2,
         add_member/3,
         remove_member/2,
         remove_member/3,
         leave_and_terminate/3,
         leave_and_terminate/4,
         leave_and_delete_server/3,
         leave_and_delete_server/4,
         %% troubleshooting
         % deprecated
         overview/0,
         overview/1,
         %% helpers
         new_uid/1,
         %% rebalancing
         transfer_leadership/2,
         aux_command/2,
         cast_aux_command/2,
         register_external_log_reader/1
        ]).

%% xref should pick these up
-deprecated({start_server, 1}).
-deprecated({restart_server, 1}).
-deprecated({stop_server, 1}).
-deprecated({overview, 0}).

-define(START_TIMEOUT, ?DEFAULT_TIMEOUT).

-type ra_cmd_ret() :: ra_server_proc:ra_cmd_ret().

-type environment_param() ::
    {data_dir, file:filename()} |
    {wal_data_dir, file:filename()} |
    {segment_max_entries, non_neg_integer()} |
    {wal_max_size_bytes, non_neg_integer()} |
    {wal_compute_checksums, boolean()} |
    {wal_write_strategy, default | o_sync}.

-type query_fun() :: fun((term()) -> term()) |
                     {M :: module(), F :: atom(), A :: list()}.

%% export some internal types
-type index() :: ra_index().
-type server_id() :: ra_server_id().
-type cluster_name() :: ra_cluster_name().

-export_type([index/0,
              server_id/0,
              cluster_name/0,
              query_fun/0,
              from/0]).

%% @doc Starts the ra application.
%% @end
-spec start() -> ok.
start() ->
    {ok, _} = start([]),
    ok.

%% @doc Starts the ra application.
%% If the application is running it will be stopped and restarted.
%% @param DataDir: the data directory to run the application in.
%% @end
-spec start(Params :: [environment_param()]) ->
    {ok, [Started]} | {error, term()} when Started :: term().
start(Params) when is_list(Params) ->
    _ = application:stop(ra),
    _ = application:load(ra),
    [ok = application:set_env(ra, Param, Value)
     || {Param, Value} <- Params],
    Res = application:ensure_all_started(ra),
    _ = ra_system:start_default(),
    Res.

%% @doc Starts the ra application with a provided data directory.
%% The same as ra:start([{data_dir, dir}])
%% If the application is running it will be stopped and restarted.
%% @param DataDir: the data directory to run the application in.
%% @end
-spec start_in(DataDir :: file:filename()) ->
    {ok, [Started]} | {error, term()}
      when Started :: term().
start_in(DataDir) ->
    start([{data_dir, DataDir}]).

%% @doc Restarts a previously successfully started ra server in the default system
%% @param ServerId the ra_server_id() of the server
%% @returns `{ok | error, Error}' where error can be
%% `not_found', `system_not_started' or `name_not_registered' when the
%% ra server has never before been started on the Erlang node.
%% DEPRECATED: use restart_server/2
%% @end
-spec restart_server(ra_server_id()) ->
    ok | {error, term()}.
restart_server(ServerId) ->
    %% TODO: this is a bad overlaod
    restart_server(default, ServerId).

%% @doc Restarts a previously successfully started ra server
%% @param System the system identifier
%% @param ServerId the ra_server_id() of the server
%% @returns `{ok | error, Error}' where error can be
%% `not_found' or `name_not_registered' when the ra server has never before
%% been started on the Erlang node.
%% @end
-spec restart_server(atom(), ra_server_id()) ->
    ok | {error, term()}.
restart_server(System, ServerId)
  when is_atom(System) ->
    % don't match on return value in case it is already running
    case catch ra_server_sup_sup:restart_server(System, ServerId, #{}) of
        {ok, _} -> ok;
        {ok, _, _} -> ok;
        {error, _} = Err -> Err;
        {'EXIT', Err} -> {error, Err}
    end.

%% @doc Restarts a previously successfully started ra server
%% @param System the system identifier
%% @param ServerId the ra_server_id() of the server
%% @param AddConfig additional config parameters to be merged into the
%% original config.
%% @returns `{ok | error, Error}' where error can be
%% `not_found' or `name_not_registered' when the ra server has never before
%% been started on the Erlang node.
%% @end

-spec restart_server(atom(), ra_server_id(), ra_server:mutable_config()) ->
    ok | {error, term()}.
restart_server(System, ServerId, AddConfig)
  when is_atom(System) ->
    % don't match on return value in case it is already running
    case catch ra_server_sup_sup:restart_server(System, ServerId, AddConfig) of
        {ok, _} -> ok;
        {ok, _, _} -> ok;
        {error, _} = Err -> Err;
        {'EXIT', Err} -> {error, Err}
    end.

%% @doc Stops a ra server in the default system
%% @param ServerId the ra_server_id() of the server
%% @returns `{ok | error, nodedown}'
%% DEPRECATED: use stop_server/2
%% @end
-spec stop_server(ra_server_id()) ->
    ok | {error, nodedown | system_not_started}.
stop_server(ServerId) ->
    stop_server(default, ServerId).

%% @doc Stops a ra server
%% @param System the system name
%% @param ServerId the ra_server_id() of the server
%% @returns `{ok | error, nodedown}'
%% @end
-spec stop_server(atom(), ra_server_id()) ->
    ok | {error, nodedown | system_not_started}.
stop_server(System, ServerId)
  when is_atom(System) ->
    try ra_server_sup_sup:stop_server(System, ServerId) of
        ok -> ok;
        {error, not_found} -> ok;
        {error, {badrpc, nodedown}} ->
            {error, nodedown}
    catch
        exit:noproc -> ok;
        exit:{{nodedown, _}, _} ->
            {error, nodedown}
    end.

%% @doc Deletes a ra server
%% The server is forcefully deleted.
%% @param ServerId the ra_server_id() of the server
%% @returns `ok | {error, nodedown} | {badrpc, Reason}'
%% @end
-spec force_delete_server(atom(), ServerId :: ra_server_id()) ->
    ok | {error, term()} | {badrpc, term()}.
force_delete_server(System, ServerId) ->
    ra_server_sup_sup:delete_server(System, ServerId).

%% @doc Starts or restarts a ra cluster.
%%
%% @param An atom of the system name
%% @param ClusterName the name of the cluster.
%% @param Machine The {@link ra_machine:machine/0} configuration.
%% @param ServerIds The list of ra server ids.
%% @returns
%% `{ok, Started, NotStarted}'  if a cluster could be successfully
%% started. A cluster can be successfully started if more than half of the
%% servers provided could be started. Servers that could not be started need to
%% be retried periodically using {@link start_server/1}
%%
%% `{error, cluster_not_formed}' if a cluster could not be started.
%%
%% If there was no existing cluster and a new cluster could not be formed
%% any servers that did manage to start are
%% forcefully deleted.
%% @end
-spec start_or_restart_cluster(atom(), ra_cluster_name(), ra_server:machine_conf(),
                               [ra_server_id()]) ->
    {ok, [ra_server_id()], [ra_server_id()]} |
    {error, cluster_not_formed}.
start_or_restart_cluster(System, ClusterName, Machine, ServerIds) ->
    start_or_restart_cluster(System, ClusterName, Machine, ServerIds, ?START_TIMEOUT).

%% @doc Same as `start_or_restart_cluster/4' but accepts a custom timeout.
%% @param ClusterName the name of the cluster.
%% @param Machine The {@link ra_machine:machine/0} configuration.
%% @param ServerIds The list of ra server ids.
%% @param Timeout The time to wait for any server to restart or start
%% @returns
%% `{ok, Started, NotStarted}'  if a cluster could be successfully
%% started. A cluster can be successfully started if more than half of the
%% servers provided could be started. Servers that could not be started need to
%% be retried periodically using {@link start_server/1}
%%
%% `{error, cluster_not_formed}' if a cluster could not be started.
%%
%% If there was no existing cluster and a new cluster could not be formed
%% any servers that did manage to start are
%% forcefully deleted.
%% @see start_or_restart_cluster/3
%% @end
-spec start_or_restart_cluster(atom(), ra_cluster_name(), ra_server:machine_conf(),
                               [ra_server_id()], non_neg_integer()) ->
    {ok, [ra_server_id()], [ra_server_id()]} |
    {error, cluster_not_formed}.
start_or_restart_cluster(System, ClusterName, Machine,
                         [FirstServer | RemServers] = ServerIds, Timeout) ->
    case ra_server_sup_sup:restart_server(System, FirstServer, #{}) of
        {ok, _} ->
            %% restart the rest of the servers
            _ = [{ok, _} = ra_server_sup_sup:restart_server(System, N, #{})
                 || N <- RemServers],
            {ok, ServerIds, []};
        {error, Err} ->
            ?ERR("start_or_restart_cluster: got an error: ~w", [Err]),
            start_cluster(System, ClusterName, Machine, ServerIds, Timeout)
    end.

%% @doc Starts a new distributed ra cluster.
%%
%% @param ClusterName the name of the cluster.
%% @param Machine The {@link ra_machine:machine/0} configuration.
%% @param ServerIds The list of ra server ids.
%% @returns
%% `{ok, Started, NotStarted}'  if a cluster could be successfully
%% started. A cluster can be successfully started if more than half of the
%% servers provided could be started. Servers that could not be started need to
%% be retried periodically using {@link start_server/1}
%%
%% `{error, cluster_not_formed}' if a cluster could not be started.
%%
%% If a cluster could not be formed any servers that did manage to start are
%% forcefully deleted.
%% @end
-spec start_cluster(atom(),
                    ra_cluster_name(),
                    ra_server:machine_conf(),
                    [ra_server_id()]) ->
    {ok, [ra_server_id()], [ra_server_id()]} |
    {error, cluster_not_formed}.
start_cluster(System, ClusterName, Machine, ServerIds)
  when is_atom(System) ->
    start_cluster(System, ClusterName, Machine, ServerIds, ?START_TIMEOUT).

%% @doc Starts a new distributed ra cluster.
%%
%% @param ClusterName the name of the cluster.
%% @param Machine The {@link ra_machine:machine/0} configuration.
%% @param ServerIds The list of ra server ids.
%% @param Timeout The time to wait for each server to start
%% @returns
%% `{ok, Started, NotStarted}'  if a cluster could be successfully
%% started. A cluster can be successfully started if more than half of the
%% servers provided could be started. Servers that could not be started need to
%% be retried periodically using {@link start_server/1}
%%
%% `{error, cluster_not_formed}' if a cluster could not be started.
%%
%% If a cluster could not be formed any servers that did manage to start are
%% forcefully deleted.
%% @end
-spec start_cluster(atom(),
                    ra_cluster_name(),
                    ra_server:machine_conf(),
                    [ra_server_id()],
                    non_neg_integer()) ->
    {ok, [ra_server_id()], [ra_server_id()]} |
    {error, cluster_not_formed}.
start_cluster(System, ClusterName, Machine, ServerIds, Timeout)
  when is_atom(System) ->
    Configs = [begin
                   UId = new_uid(ra_lib:to_binary(ClusterName)),
                   #{id => Id,
                     uid => UId,
                     cluster_name => ClusterName,
                     log_init_args => #{uid => UId},
                     initial_members => ServerIds,
                     machine => Machine}
               end || Id <- ServerIds],
    start_cluster(System, Configs, Timeout).

%% @doc Starts a new distributed ra cluster.
%%
%% @param ServerConfigs a list of initial server configurations
%% @returns
%% `{ok, Started, NotStarted}'  if a cluster could be successfully
%% started. A cluster can be successfully started if more than half of the
%% servers provided could be started. Servers that could not be started need to
%% be retried periodically using {@link start_server/1}
%%
%% `{error, cluster_not_formed}' if a cluster could not be started.
%%
%% If a cluster could not be formed any servers that did manage to start are
%% forcefully deleted.
%% @end
-spec start_cluster(atom(), [ra_server:ra_server_config()]) ->
    {ok, [ra_server_id()], [ra_server_id()]} |
    {error, cluster_not_formed}.
start_cluster(System, ServerConfigs)
  when is_atom(System) ->
    start_cluster(System, ServerConfigs, ?START_TIMEOUT).

%% @doc Same as `start_cluster/1' but accepts a custom timeout.
%% @param ServerConfigs a list of initial server configurations
%% @param Timeout the timeout to use
%% @end
-spec start_cluster(atom(),
                    [ra_server:ra_server_config()], non_neg_integer()) ->
    {ok, [ra_server_id()], [ra_server_id()]} |
    {error, cluster_not_formed}.
start_cluster(System, [#{cluster_name := ClusterName} | _] = ServerConfigs,
              Timeout) when is_atom(System) ->
    {Started, NotStarted} =
        ra_lib:partition_parallel(
            fun (C) ->
                case start_server(System, C) of
                    ok  -> true;
                    Err ->
                        ?ERR("ra: failed to start a server ~w, error: ~p",
                              [C, Err]),
                        false
                end
            end, ServerConfigs),
    case Started of
        [] ->
            ?ERR("ra: failed to form a new cluster ~w.~n "
                  "No servers were succesfully started.",
                  [ClusterName]),
            {error, cluster_not_formed};
        _ ->
            StartedIds = [I || #{id := I} <- Started],
            NotStartedIds = [I || #{id := I} <- NotStarted],
            %% try triggering elections until one succeeds
            _ = lists:any(fun (N) -> ok == trigger_election(N) end,
                          sort_by_local(StartedIds, [])),
            %% TODO: handle case where no election was successfully triggered
            case members(hd(StartedIds),
                         length(ServerConfigs) * Timeout) of
                {ok, _, Leader} ->
                    ?INFO("ra: started cluster ~s with ~b servers~n"
                          "~b servers failed to start: ~w~n"
                          "Leader: ~w", [ClusterName, length(ServerConfigs),
                                         length(NotStarted), NotStartedIds,
                                         Leader]),
                    % we have a functioning cluster
                    {ok, StartedIds, NotStartedIds};
                Err ->
                    ?WARN("ra: failed to form new cluster ~w.~n "
                          "Error: ~w", [ClusterName, Err]),
                    _ = [force_delete_server(System, N) || N <- StartedIds],
                    % we do not have a functioning cluster
                    {error, cluster_not_formed}
            end
    end.

%% @doc Starts a new distributed ra cluster.
%% @param ClusterName the name of the cluster.
%% @param ServerId the ra_server_id() of the server
%% @param Machine The {@link ra_machine:machine/0} configuration.
%% @param ServerConfigs a list of initial server configurations
%% @returns
%% `{ok, Started, NotStarted}'  if a cluster could be successfully
%% started. A cluster can be successfully started if more than half of the
%% servers provided could be started. Servers that could not be started need to
%% be retried periodically using {@link start_server/1}
%%
%% `{error, cluster_not_formed}' if a cluster could not be started.
%%
%% If a cluster could not be formed any servers that did manage to start are
%% forcefully deleted.
%% @see start_server/1
%% @end
-spec start_server(atom(), ra_cluster_name(), ra_server_id(),
                   ra_server:machine_conf(), [ra_server_id()]) ->
    ok | {error, term()}.
start_server(System, ClusterName, {_, _} = ServerId, Machine, ServerIds)
  when is_atom(System) ->
    UId = new_uid(ra_lib:to_binary(ClusterName)),
    Conf = #{cluster_name => ClusterName,
             id => ServerId,
             uid => UId,
             initial_members => ServerIds,
             log_init_args => #{uid => UId},
             machine => Machine},
    start_server(System, Conf).

%% @doc Starts a ra server in the default system
%% @param Conf a ra_server_config() configuration map.
%% @returns `{ok | error, Error}'
%% DEPRECATED: use start_server/2
%% @end
-spec start_server(ra_server:ra_server_config()) ->
    ok | {error, term()}.
start_server(Conf) ->
    start_server(default, Conf).

%% @doc Starts a ra server
%% @param The system name
%% @param Conf a ra_server_config() configuration map.
%% @returns `{ok | error, Error}'
%% @end
-spec start_server(atom(), ra_server:ra_server_config()) ->
    ok | {error, term()}.
start_server(System, Conf) when is_atom(System) ->
    %% validate UID is safe
    case ra_lib:validate_base64uri(maps:get(uid, Conf)) of
        true ->
            % don't match on return value in case it is already running
            case catch ra_server_sup_sup:start_server(System, Conf) of
                {ok, _} -> ok;
                {ok, _, _} -> ok;
                {error, _} = Err -> Err;
                {'EXIT', Err} -> {error, Err};
                {badrpc, Reason} -> {error, Reason}
            end;
        false ->
            {error, invalid_uid}
    end.

%% @doc Deletes a ra cluster in an orderly fashion.
%% This function commits an end of life command which after each server applies
%% it will cause that server to shut down and delete all its data.
%% The leader will stay up until it has successfully replicated the end of life
%% command to all servers after which it too will shut down and delete all of its
%% data.
%% @param ServerIds the ra_server_ids of the cluster
%% @returns `{{ok, Leader} | error, nodedown}'
%% @end
-spec delete_cluster(ServerIds :: [ra_server_id()]) ->
    {ok, ra_server_id()} | {error, term()}.
delete_cluster(ServerIds) ->
    delete_cluster(ServerIds, ?DEFAULT_TIMEOUT).

%% @doc Same as `delete_cluster/1' but also accepts a timeout.
%% @see delete_cluster/1
%% @end
-spec delete_cluster(ServerIds :: [ra_server_id()], timeout()) ->
    {ok, Leader :: ra_server_id()} | {error, term()}.
delete_cluster(ServerIds, Timeout) ->
    DeleteCmd = {'$ra_cluster', delete, await_consensus},
    case ra_server_proc:command(ServerIds, DeleteCmd, Timeout) of
        {ok, _, Leader} ->
            {ok, Leader};
        {timeout, _} ->
            {error, timeout};
        Err ->
            Err
    end.


%% @doc Add a ra server id to a ra cluster's membership configuration.
%% This commits a join command to the leader log. After this has been replicated
%% the leader will start replicating entries to the new server.
%% This function returns after appending the command to the log.
%% New node does not have to be running at the time of addition but it is expected
%% to come online in a reasonable amount of time. A new member that's been
%% announced to its new cluster but taking a long time to start will
%% affect said cluster's availability characteristics (by increasing quorum node count).
%%
%% @param ServerLoc the ra server or servers to try to send the command to
%% @param ServerId the ra server id of the new server.
%% @end
-spec add_member(ra_server_id() | [ra_server_id()], ra_server_id()) ->
    ra_cmd_ret() |
    {error, already_member} |
    {error, cluster_change_not_permitted}.
add_member(ServerLoc, ServerId) ->
    add_member(ServerLoc, ServerId, ?DEFAULT_TIMEOUT).

%% @doc Same as `add_member/2' but also accepts a timeout.
%% @see add_member/2
%% @end
-spec add_member(ra_server_id() | [ra_server_id()],
                 ra_server_id(), timeout()) ->
    ra_cmd_ret() |
    {error, already_member} |
    {error, cluster_change_not_permitted}.
add_member(ServerLoc, ServerId, Timeout) ->
    ra_server_proc:command(ServerLoc,
                           {'$ra_join', ServerId, after_log_append},
                           Timeout).


%% @doc Removes a server from the cluster's membership configuration.
%% This function returns after appending a cluster membership change
%% command to the log.
%%
%% After a server is removed from its cluster and the membership change is replicated,
%% it would reject any commands it's asked to process.
%%
%% @param ServerRef the ra server to send the command to
%% @param ServerId the ra server id of the server to remove
%% @see leave_and_terminate/2
%% @see leave_and_delete_server/2
%% @see add_member/2
%% @see remove_member/3
%% @end
-spec remove_member(ra_server_id() | [ra_server_id()], ra_server_id()) ->
    ra_cmd_ret() |
    {error, not_member} |
    {error, cluster_change_not_permitted}.
remove_member(ServerRef, ServerId) ->
    remove_member(ServerRef, ServerId, ?DEFAULT_TIMEOUT).

%% @doc Same as `remove_member/2' but also accepts a timeout.
%% @see remove_member/2
%% @end
-spec remove_member(ra_server_id() | [ra_server_id()],
                    ra_server_id(), timeout()) ->
    ra_cmd_ret() |
    {error, not_member} |
    {error, cluster_change_not_permitted}.
remove_member(ServerRef, ServerId, Timeout) ->
    ra_server_proc:command(ServerRef,
                           {'$ra_leave', ServerId, after_log_append},
                           Timeout).

%% @doc Makes the server to enter a pre-vote state and attempt to become the leader.
%% It is necessary to call this function when starting a new cluster as a
%% brand new Ra server (node) will not automatically enter the pre-vote state.
%% This does not apply to recovering (previously started) servers: they will
%% enter the pre-vote state and proceed to participate in an election on boot.
%%
%% @param ServerId the ra server id of the server to trigger the election on.
%% @end
-spec trigger_election(ra_server_id()) -> ok.
trigger_election(ServerId) ->
    trigger_election(ServerId, ?DEFAULT_TIMEOUT).

%% @doc Same as `trigger_election/2' but also accepts a timeout.
%% @end
-spec trigger_election(ra_server_id(), timeout()) -> ok.
trigger_election(ServerId, Timeout) ->
    ra_server_proc:trigger_election(ServerId, Timeout).

%% @doc A safe way to remove an active server from its cluster.
%% The command is added to the log by the `ServerRef' node.
%% Use this to decommission a node that's unable to start
%% or is permanently lost.
%% @param ServerRef the ra server to send the command to and to remove
%% @param ServerId the ra server to remove
%% @see leave_and_terminate/3
%% @end
-spec leave_and_terminate(atom(),
                          ra_server_id() | [ra_server_id()], ra_server_id()) ->
    ok | timeout | {error, noproc | system_not_started}.
leave_and_terminate(System, ServerRef, ServerId) ->
    leave_and_terminate(System, ServerRef, ServerId, ?DEFAULT_TIMEOUT).

%% @doc Same as `leave_and_terminate/2' but also accepts a timeout.
%% @param ServerRef the ra server to send the command to and to remove
%% @param ServerId the ra server to remove
%% @param Timeout timeout to use
%% @see leave_and_terminate/2
%% @end
-spec leave_and_terminate(atom(),
                          ra_server_id() | [ra_server_id()],
                          ra_server_id(), timeout()) ->
    ok | timeout | {error, noproc | system_not_started}.
leave_and_terminate(System, ServerRef, ServerId, Timeout) ->
    LeaveCmd = {'$ra_leave', ServerId, await_consensus},
    case ra_server_proc:command(ServerRef, LeaveCmd, Timeout) of
        {timeout, Who} ->
            ?ERR("Failed to leave the cluster: request to ~w timed out", [Who]),
            timeout;
        {error, noproc} = Err ->
            Err;
        {ok, _, _} ->
            ?INFO("We (Ra node ~w) has successfully left the cluster. Terminating.", [ServerId]),
            stop_server(System, ServerId)
    end.

%% @doc A safe way to remove an active server from its cluster.
%% The server will be force removed after a membership transition command was
%% added to the log.
%% The command is added to the log by the `ServerRef' node.
%% Use this to decommission a node that's unable to start
%% or is permanently lost.
%% @param System the system identifier
%% @param ServerRef the ra server to send the command to and to remove
%% @param ServerId the ra server to force remove
%% @see leave_and_delete_server/3
%% @end
-spec leave_and_delete_server(atom(), ra_server_id() | [ra_server_id()],
                              ra_server_id()) ->
    ok | timeout | {error, noproc}.
leave_and_delete_server(System, ServerRef, ServerId) ->
    leave_and_delete_server(System, ServerRef, ServerId, ?DEFAULT_TIMEOUT).

%% @doc Same as `leave_and_delete_server/2' but also accepts a timeout.
%% @param ServerRef the ra server to send the command to and to remove
%% @param ServerId the ra server to force remove
%% @param Timeout timeout to use
%% @see leave_and_delete_server/2
%% @end
-spec leave_and_delete_server(atom(), ra_server_id() | [ra_server_id()],
                              ra_server_id(), timeout()) ->
    ok | timeout | {error, noproc}.
leave_and_delete_server(System, ServerRef, ServerId, Timeout) ->
    LeaveCmd = {'$ra_leave', ServerId, await_consensus},
    case ra_server_proc:command(ServerRef, LeaveCmd, Timeout) of
        {timeout, Who} ->
            ?ERR("Failed to leave the cluster: request to ~w timed out", [Who]),
            timeout;
        {error, _} = Err ->
            Err;
        {ok, _, _} ->
            ?INFO("Ra node ~w has successfully left the cluster.", [ServerId]),
            force_delete_server(System, ServerId)
    end.

%% @doc generates a random uid using the provided source material for the first
%% 6 characters.
%% @end
new_uid(Source) when is_binary(Source) ->
    Prefix = ra_lib:derive_safe_string(ra_lib:to_binary(Source), 6),
    ra_lib:make_uid(string:uppercase(Prefix)).


%% @doc Returns a map of overview data of the default Ra system on the current Erlang
%% node.
%% DEPRECATED: user overview/1
%% @end
-spec overview() -> map() | system_not_started.
overview() ->
    overview(default).

%% @doc Returns a map of overview data of the Ra system on the current Erlang
%% node.
%% @end
-spec overview(atom()) -> map() | system_not_started.
overview(System) ->

    #{names := #{segment_writer := SegWriter,
                 open_mem_tbls := OpenTbls,
                 closed_mem_tbls := ClosedTbls}} = ra_system:fetch(System),
    #{node => node(),
      servers => ra_directory:overview(System),
      %% TODO:filter counter keys by system
      counters => ra_counters:overview(),
      wal => #{status => lists:nth(5, element(4, sys:get_status(ra_log_wal))),
               open_mem_tables => ets:info(OpenTbls, size),
               closed_mem_tables => ets:info(ClosedTbls, size)},
      segment_writer => ra_log_segment_writer:overview(SegWriter)
     }.

%% @doc Submits a command to a ra server. Returns after the command has
%% been applied to the Raft state machine. If the state machine returned a
%% response it is included in the second element of the response tuple.
%% If no response was returned the second element is the atom `noreply'.
%% If the server receiving the command isn't the current leader it will
%% redirect the call to the leader (if known) or hold on to the command until
%% a leader is known. The leader's server id is returned as the 3rd element
%% of the success reply tuple.
%%
%% If there is no majority of Ra servers online, this function will return
%% a timeout.
%%
%% @param ServerId the server id to send the command to
%% @param Command an arbitrary term that the state machine can handle
%% @param Timeout the time to wait before returning {timeout, ServerId}
%% @end
-spec process_command(ServerId :: ra_server_id() | [ra_server_id()],
                      Command :: term(),
                      Timeout :: timeout()) ->
    {ok, Reply :: term(), Leader :: ra_server_id()} |
    {error, term()} |
    {timeout, ra_server_id()}.
process_command(ServerId, Cmd, Timeout) ->
    ra_server_proc:command(ServerId, usr(Cmd, await_consensus), Timeout).

%% @doc Same as `process_command/3' with the default timeout of 5000 ms.
%% @param ServerId the server id to send the command to
%% @param Command an arbitrary term that the state machine can handle
%% @end
-spec process_command(ServerId :: ra_server_id() | [ra_server_id()],
                      Command :: term()) ->
    {ok, Reply :: term(), Leader :: ra_server_id()} |
    {error, term()} |
    {timeout, ra_server_id()}.
process_command(ServerId, Command) ->
    process_command(ServerId, Command, ?DEFAULT_TIMEOUT).


%% @doc Submits a command to the ra server using a gen_statem:cast, passing
%% an optional process-scoped term as correlation identifier.
%% A correlation id can be included
%% to implement reliable async interactions with the ra system. The calling
%% process can retain a map of commands that have not yet been applied to the
%% state machine successfully and resend them if a notification is not received
%% withing some time window.
%% When the submitted command(s) is applied to the state machine, the ra server will send
%% the calling process a ra_event of the following structure:
%%
%% `{ra_event, CurrentLeader, {applied, [{Correlation, Reply}]}}'
%%
%% Ra will batch notification and thus return a list of correlation
%% and result tuples.
%%
%% If the receiving ra server is not the cluster leader, a ra event of the following
%% structure will be returned informing the caller that it cannot process the
%% message. The message will include the current cluster leader, if one is known:
%%
%% `{ra_event, FromId, {rejected, {not_leader, Leader | undefined, Correlation}}}'
%%
%% The caller must then redirect the command for the correlation identifier to
%% the correct ra server: the leader.
%%
%% If instead the atom `no_correlation' is passed for the correlation argument,
%% the calling process will not receive any notification of command processing
%% success or otherwise.
%%
%% This is the least reliable way to interact with a ra system ("fire and forget")
%% and should only be used if the command is of little importance to the application.
%%
%% @param ServerId the ra server id to send the command to
%% @param Command an arbitrary term that the state machine can handle
%% @param Correlation a correlation identifier to be included to receive an
%%        async notification after the command is applied to the state machine. If the
%%        Correlation is set to `no_correlation' then no notifications will be sent.
%% @param Priority command priority. `low' priority commands will be held back
%%        and appended to the Raft log in batches. NB: A `normal' priority command sent
%%        from the same process can overtake a low priority command that was
%%        sent before. There is no high priority.
%%        Only use priority level of `low' with commands that
%%        do not rely on total execution ordering.
%% @end
-spec pipeline_command(ServerId :: ra_server_id(), Command :: term(),
                       Correlation :: ra_server:command_correlation() |
                       no_correlation,
                       Priority :: normal | low) -> ok.
pipeline_command(ServerId, Command, Correlation, Priority)
  when Correlation /= no_correlation ->
    Cmd = usr(Command, {notify, Correlation, self()}),
    ra_server_proc:cast_command(ServerId, Priority, Cmd);
pipeline_command(ServerId, Command, no_correlation, Priority) ->
    Cmd = usr(Command, noreply),
    ra_server_proc:cast_command(ServerId, Priority, Cmd).

%% @doc Same as `pipeline_command/4' but uses a hardcoded priority of `low'.
%% @param ServerId the ra server id to send the command to
%% @param Command an arbitrary term that the state machine can handle
%% @param Correlation a correlation identifier to be included to receive an
%%        async notification after the command is applied to the state machine.
%% @see pipeline_command/4
%% @end
-spec pipeline_command(ServerId :: ra_server_id(), Command :: term(),
                       Correlation :: ra_server:command_correlation() |
                                      no_correlation) ->
    ok.
pipeline_command(ServerId, Command, Correlation) ->
    pipeline_command(ServerId, Command, Correlation, low).


%% @doc Sends a command to the ra server using a gen_statem:cast without
%% any correlation identifier.
%% Effectively the same as
%% `ra:pipeline_command(ServerId, Command, low, no_correlation)'
%% This is the least reliable way to interact with a ra system ("fire and forget")
%% and should only be used for commands that are of little importance
%% and/or where waiting for a response is prohibitively slow.
%% @param ServerId the ra server id to send the command to
%% @param Command an arbitrary term that the state machine can handle
%% @see pipeline_command/4
%% @end
-spec pipeline_command(ServerId :: ra_server_id(),
                       Command :: term()) -> ok.
pipeline_command(ServerId, Command) ->
    pipeline_command(ServerId, Command, no_correlation, low).

%% @doc Query the machine state on any available server.
%% This allows you to run the QueryFun over the server machine state and
%% return the result. Any ra server can be addressed and will returns its local
%% state at the time of querying.
%% This can return stale results, including infinitely stale ones.
%% @param ServerId the ra server id to send the query to
%% @param QueryFun the query function to run
%% @end
-spec local_query(ServerId :: ra_server_id(),
                  QueryFun :: query_fun()) ->
    ra_server_proc:ra_leader_call_ret({ra_idxterm(), Reply :: term()}).
local_query(ServerId, QueryFun) ->
    local_query(ServerId, QueryFun, ?DEFAULT_TIMEOUT).

%% @doc Same as `local_query/2' but accepts a custom timeout.
%% @param ServerId the ra server id to send the query to
%% @param QueryFun the query function to run
%% @param Timeout the timeout to use
%% @see local_query/2
%% @end
-spec local_query(ServerId :: ra_server_id(),
                  QueryFun :: query_fun(),
                  Timeout :: timeout()) ->
    ra_server_proc:ra_leader_call_ret({ra_idxterm(), Reply :: term()}) |
    {ok, {ra_idxterm(), Reply :: term()}, not_known}.
local_query(ServerId, QueryFun, Timeout) ->
    ra_server_proc:query(ServerId, QueryFun, local, Timeout).


%% @doc Query the machine state on the current leader node.
%% This function works like local_query, but redirects to the current
%% leader node.
%% The leader state may be more up-to-date compared to local state of some followers.
%% This function may still return stale results as it reads the current state
%% and does not wait for commands to be applied.
%% @param ServerId the ra server id(s) to send the query to
%% @param QueryFun the query function to run
%% @end
-spec leader_query(ServerId :: ra_server_id() | [ra_server_id()],
                   QueryFun :: query_fun()) ->
    ra_server_proc:ra_leader_call_ret({ra_idxterm(), Reply :: term()}) |
    {ok, {ra_idxterm(), Reply :: term()}, not_known}.
leader_query(ServerId, QueryFun) ->
    leader_query(ServerId, QueryFun, ?DEFAULT_TIMEOUT).

%% @doc Same as `leader_query/2' but accepts a custom timeout.
%% @param ServerId the ra server id(s) to send the query to
%% @param QueryFun the query function to run
%% @param Timeout the timeout to use
%% @see leader_query/2
%% @end
-spec leader_query(ServerId :: ra_server_id() | [ra_server_id()],
                   QueryFun :: query_fun(),
                   Timeout :: timeout()) ->
    ra_server_proc:ra_leader_call_ret({ra_idxterm(), Reply :: term()}) |
    {ok, {ra_idxterm(), Reply :: term()}, not_known}.
leader_query(ServerId, QueryFun, Timeout) ->
    ra_server_proc:query(ServerId, QueryFun, leader, Timeout).

%% @doc Query the state machine with a consistency guarantee.
%% This allows the caller to query the state machine on the leader node with
%% an additional heartbeat to check that the node is still the leader.
%% Consistency guarantee is that the query will return result containing
%% at least all changes, committed before this query is issued.
%% This may include changes which were committed while the query is running.
%% @param ServerId the ra server id(s) to send the query to
%% @param QueryFun the query function to run
%% @end
-spec consistent_query(ServerId :: ra_server_id() | [ra_server_id()],
                       QueryFun :: query_fun()) ->
    ra_server_proc:ra_leader_call_ret(Reply :: term()).
consistent_query(ServerId, QueryFun) ->
    consistent_query(ServerId, QueryFun, ?DEFAULT_TIMEOUT).

%% @doc Same as `consistent_query/2' but accepts a custom timeout.
%% @param ServerId the ra server id(s) to send the query to
%% @param QueryFun the query function to run
%% @param Timeout the timeout to use
%% @see consistent_query/2
%% @end
-spec consistent_query(ServerId :: ra_server_id() | [ra_server_id()],
                       QueryFun :: query_fun(),
                       Timeout :: timeout()) ->
    ra_server_proc:ra_leader_call_ret(Reply :: term()).
consistent_query(ServerId, QueryFun, Timeout) ->
    ra_server_proc:query(ServerId, QueryFun, consistent, Timeout).

%% @doc Returns a list of cluster members
%%
%% Except if `{local, ServerId}' is passed, the query is sent to the specified
%% server which may redirect it to the leader if it is a follower. It may
%% timeout if there is currently no leader (i.e. an election is in progress).
%%
%% With `{local, ServerId}', the query is always handled by the specified
%% server. It means the returned list might be out-of-date compared to what the
%% leader would have returned.
%%
%% @param ServerId the Ra server(s) to send the query to
%% @end
-spec members(ra_server_id() | [ra_server_id()] | {local, ra_server_id()}) ->
    ra_server_proc:ra_leader_call_ret([ra_server_id()]).
members(ServerId) ->
    members(ServerId, ?DEFAULT_TIMEOUT).

%% @doc Returns a list of cluster members
%%
%% Except if `{local, ServerId}' is passed, the query is sent to the specified
%% server which may redirect it to the leader if it is a follower. It may
%% timeout if there is currently no leader (i.e. an election is in progress).
%%
%% With `{local, ServerId}', the query is always handled by the specified
%% server. It means the returned list might be out-of-date compared to what the
%% leader would have returned.
%%
%% @param ServerId the Ra server(s) to send the query to
%% @param Timeout the timeout to use
%% @end
-spec members(ra_server_id() | [ra_server_id()] | {local, ra_server_id()},
              timeout()) ->
    ra_server_proc:ra_leader_call_ret([ra_server_id()]).
members({local, ServerId}, Timeout) ->
    ra_server_proc:local_state_query(ServerId, members, Timeout);
members(ServerId, Timeout) ->
    ra_server_proc:state_query(ServerId, members, Timeout).

-spec initial_members(ra_server_id() | [ra_server_id()]) ->
    ra_server_proc:ra_leader_call_ret([ra_server_id()] | error).
initial_members(ServerId) ->
    initial_members(ServerId, ?DEFAULT_TIMEOUT).

-spec initial_members(ra_server_id() | [ra_server_id()], timeout()) ->
    ra_server_proc:ra_leader_call_ret([ra_server_id()] | error).
initial_members(ServerId, Timeout) ->
    ra_server_proc:state_query(ServerId, initial_members, Timeout).

%% @doc Transfers leadership from the leader to a follower.
%% Returns `already_leader' if the transfer targer is already the leader.
%% @end
-spec transfer_leadership(ra_server_id(), ra_server_id()) ->
    ok | already_leader | {error, term()} | {timeout, ra_server_id()}.
transfer_leadership(ServerId, TargetServerId) ->
    ra_server_proc:transfer_leadership(ServerId, TargetServerId, ?DEFAULT_TIMEOUT).

-spec aux_command(ra_server_id(), term()) -> term().
aux_command(ServerRef, Cmd) ->
    gen_statem:call(ServerRef, {aux_command, Cmd}).

-spec cast_aux_command(ra_server_id(), term()) -> ok.
cast_aux_command(ServerRef, Cmd) ->
    gen_statem:cast(ServerRef, {aux_command, Cmd}).

%% @doc Registers an external log reader. ServerId needs to be local to the node.
%% Returns an initiated ra_log_reader:state() state.
%% @end
-spec register_external_log_reader(ra_server_id()) ->
    ra_log_reader:state().
register_external_log_reader({_, Node} = ServerId)
 when Node =:= node() ->
    {ok, Reader} = gen_statem:call(ServerId, {register_external_log_reader, self()}),
    Reader.

%% internal

usr(Data, Mode) ->
    {'$usr', Data, Mode}.

sort_by_local([], Acc) ->
    Acc;
sort_by_local([{_, N} = X | Rem], Acc) when N =:= node() ->
    [X | Acc] ++ Rem;
sort_by_local([X | Rem], Acc) ->
    sort_by_local(Rem, [X | Acc]).
