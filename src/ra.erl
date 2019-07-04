%% @doc The primary module for interacting with ra servers and clusters.

-module(ra).

-include("ra.hrl").

-export([
         start/0,
         start/1,
         start_in/1,
         %% new api
         process_command/2,
         process_command/3,
         pipeline_command/2,
         pipeline_command/3,
         pipeline_command/4,
         %% queries
         members/1,
         members/2,
         local_query/2,
         local_query/3,
         leader_query/2,
         leader_query/3,
         consistent_query/2,
         consistent_query/3,
         % cluster operations
         start_cluster/1,
         start_cluster/2,
         start_cluster/3,
         start_cluster/4,
         start_or_restart_cluster/3,
         start_or_restart_cluster/4,
         delete_cluster/1,
         delete_cluster/2,
         % server management
         start_server/1,
         start_server/4,
         restart_server/1,
         stop_server/1,
         force_delete_server/1,
         trigger_election/1,
         trigger_election/2,
         %% membership
         add_member/2,
         add_member/3,
         remove_member/2,
         remove_member/3,
         leave_and_terminate/1,
         leave_and_terminate/2,
         leave_and_terminate/3,
         leave_and_delete_server/1,
         leave_and_delete_server/2,
         leave_and_delete_server/3,
         %%
         overview/0,
         new_uid/1
        ]).

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

-export_type([query_fun/0]).

%% @doc Starts the ra application
-spec start() -> ok.
start() ->
    {ok, _} = application:ensure_all_started(ra),
    ok.

%% @doc Starts the ra application.
%% If the application is running it will be stopped and restarted.
%% @param DataDir: the data directory to run the application in.
-spec start(Params :: [environment_param()]) ->
    {ok, [Started]} | {error, term()} when Started :: term().
start(Params) when is_list(Params) ->
    _ = application:stop(ra),
    _ = application:load(ra),
    [ok = application:set_env(ra, Param, Value)
     || {Param, Value} <- Params],
    application:ensure_all_started(ra).

%% @doc Starts the ra application with a provided data directory.
%% The same as ra:start([{data_dir, dir}])
%% If the application is running it will be stopped and restarted.
%% @param DataDir: the data directory to run the application in.
-spec start_in(DataDir :: file:filename()) ->
    {ok, [Started]} | {error, term()}
      when Started :: term().
start_in(DataDir) ->
    start([{data_dir, DataDir}]).

%% @doc Restarts a previously succesfully started ra server
%% @param ServerId the ra_server_id() of the server
%% @returns `{ok | error, Error}' when error can be
%% `not_found' or `name_not_registered' when the ra server has never before
%% been started on the erlang node.
-spec restart_server(ra_server_id()) -> ok | {error, term()}.
restart_server(ServerId) ->
    % don't match on return value in case it is already running
    case catch ra_server_sup_sup:restart_server(ServerId) of
        {ok, _} -> ok;
        {ok, _, _} -> ok;
        {error, _} = Err -> Err;
        {'EXIT', Err} -> {error, Err}
    end.

%% @doc Stops a ra server
%% @param ServerId the ra_server_id() of the server
%% @returns `{ok | error, nodedown}'
-spec stop_server(ra_server_id()) -> ok | {error, nodedown}.
stop_server(ServerId) ->
    try ra_server_sup_sup:stop_server(ServerId) of
        ok -> ok;
        {error, not_found} -> ok
    catch
        exit:noproc -> ok;
        exit:{{nodedown, _}, _} -> {error, nodedown}
    end.

%% @doc Deletes a ra server
%% The server is forcefully deleted.
%% @param ServerId the ra_server_id() of the server
%% @returns `{ok | error, nodedown}'
-spec force_delete_server(ServerId :: ra_server_id()) -> ok | {error, term()}.
force_delete_server(ServerId) ->
    ra_server_sup_sup:delete_server(ServerId).

%% @doc Starts or restarts a ra cluster.
%%
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
%% If there was no existing cluster and a new cluster could not be formed
%% any servers that did manage to start are
%% forcefully deleted.
-spec start_or_restart_cluster(ra_cluster_name(), ra_server:machine_conf(),
                               [ra_server_id()]) ->
    {ok, [ra_server_id()], [ra_server_id()]} |
    {error, cluster_not_formed}.
start_or_restart_cluster(ClusterName, Machine, ServerIds) ->
    start_or_restart_cluster(ClusterName, Machine, ServerIds, ?START_TIMEOUT).

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
-spec start_or_restart_cluster(ra_cluster_name(), ra_server:machine_conf(),
                               [ra_server_id()], non_neg_integer()) ->
    {ok, [ra_server_id()], [ra_server_id()]} |
    {error, cluster_not_formed}.
start_or_restart_cluster(ClusterName, Machine,
                         [FirstServer | RemServers] = ServerIds, Timeout) ->
    case ra_server_sup_sup:restart_server(FirstServer) of
        {ok, _} ->
            %% restart the rest of the servers
            _ = [{ok, _} = ra_server_sup_sup:restart_server(N) || N <- RemServers],
            {ok, ServerIds, []};
        {error, Err} ->
            ?ERR("start_or_restart_cluster: got an error: ~p~n", [Err]),
            start_cluster(ClusterName, Machine, ServerIds, Timeout)
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
-spec start_cluster(ra_cluster_name(),
                    ra_server:machine_conf(),
                    [ra_server_id()]) ->
    {ok, [ra_server_id()], [ra_server_id()]} |
    {error, cluster_not_formed}.
start_cluster(ClusterName, Machine, ServerIds) ->
    start_cluster(ClusterName, Machine, ServerIds, ?START_TIMEOUT).

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
-spec start_cluster(ra_cluster_name(),
                    ra_server:machine_conf(),
                    [ra_server_id()],
                    non_neg_integer()) ->
    {ok, [ra_server_id()], [ra_server_id()]} |
    {error, cluster_not_formed}.
start_cluster(ClusterName, Machine, ServerIds, Timeout) ->
    Configs = [begin
                   UId = new_uid(ra_lib:to_binary(ClusterName)),
                   #{id => Id,
                     uid => UId,
                     cluster_name => ClusterName,
                     log_init_args => #{uid => UId},
                     initial_members => ServerIds,
                     machine => Machine}
               end || Id <- ServerIds],
    start_cluster(Configs, Timeout).

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
-spec start_cluster([ra_server:ra_server_config()]) ->
    {ok, [ra_server_id()], [ra_server_id()]} |
    {error, cluster_not_formed}.
start_cluster(ServerConfigs) ->
    start_cluster(ServerConfigs, ?START_TIMEOUT).

-spec start_cluster([ra_server:ra_server_config()], non_neg_integer()) ->
    {ok, [ra_server_id()], [ra_server_id()]} |
    {error, cluster_not_formed}.
start_cluster([#{cluster_name := ClusterName} | _] =
               ServerConfigs, Timeout) ->
    {Started, NotStarted} =
        ra_lib:partition_parallel(
            fun (C) ->
                case start_server(C) of
                    ok  -> true;
                    Err ->
                        ?ERR("ra: failed to start a server ~w, error: ~p~n",
                              [C, Err]),
                        false
                end
            end, ServerConfigs),
    case Started of
        [] ->
            ?ERR("ra: failed to form a new cluster ~w.~n "
                  "No servers were succesfully started.~n",
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
                          "Error: ~w~n", [ClusterName, Err]),
                    _ = [force_delete_server(N) || N <- StartedIds],
                    % we do not have a functioning cluster
                    {error, cluster_not_formed}
            end
    end.

-spec start_server(ra_cluster_name(), ra_server_id(),
                   ra_server:machine_conf(), [ra_server_id()]) ->
    ok | {error, term()}.
start_server(ClusterName, ServerId, Machine, ServerIds) ->
    UId = new_uid(ra_lib:to_binary(ClusterName)),
    Conf = #{cluster_name => ClusterName,
             id => ServerId,
             uid => UId,
             initial_members => ServerIds,
             log_init_args => #{uid => UId},
             machine => Machine},
    start_server(Conf).

%% @doc Starts a ra server
%% @param Conf a ra_server_config() configuration map.
%% @returns `{ok | error, Error}'
-spec start_server(ra_server:ra_server_config()) -> ok | {error, term()}.
start_server(Conf) ->
    %% validate UID is safe
    case ra_lib:validate_base64uri(maps:get(uid, Conf)) of
        true ->
            % don't match on return value in case it is already running
            case catch ra_server_sup_sup:start_server(Conf) of
                {ok, _} -> ok;
                {ok, _, _} -> ok;
                {error, _} = Err -> Err;
                {'EXIT', Err} -> {error, Err};
                {badrpc, Reason} -> {error, Reason}
            end;
        false ->
            {error, invalid_uid}
    end.

%% @doc Deletes a ra cluster in an orderly fashion
%% This function commits and end of life command which after each server applies
%% it will cause that server to shut down and delete all it's data.
%% The leader will stay up until it has successfully replicated the end of life
%% command to all servers after which it too will shut down and delete all it's
%% data.
%% @param ServerIds the ra_server_ids of the cluster
%% @returns `{{ok, Leader} | error, nodedown}'
-spec delete_cluster(ServerIds :: [ra_server_id()]) ->
    {ok, ra_server_id()} | {error, term()}.
delete_cluster(ServerIds) ->
    delete_cluster(ServerIds, ?DEFAULT_TIMEOUT).

%% @see delete_cluster/1
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


%% @doc Add a ra server id to a ra cluster's membership configuration
%% This commits a join command to the leader log. After this has been replicated
%% the leader will start replicating entries to the new server.
%% This function returns after appending the command to the log.
%%
%% @param ServerLoc the ra server or servers to try to send the command to
%% @param ServerId the ra server id of the new server
-spec add_member(ra_server_id() | [ra_server_id()], ra_server_id()) ->
    ra_cmd_ret().
add_member(ServerLoc, ServerId) ->
    add_member(ServerLoc, ServerId, ?DEFAULT_TIMEOUT).

%% @see add_member/2
-spec add_member(ra_server_id() | [ra_server_id()],
                 ra_server_id(), timeout()) -> ra_cmd_ret().
add_member(ServerLoc, ServerId, Timeout) ->
    ra_server_proc:command(ServerLoc,
                           {'$ra_join', ServerId, after_log_append},
                           Timeout).


%% @doc Removes a server from the cluster's membership configuration
%% This function returns after appending the command to the log.
%%
%% @param ServerRef the ra server to send the command to
%% @param ServerId the ra server id of the server to remove
-spec remove_member(ra_server_id() | [ra_server_id()], ra_server_id()) ->
    ra_cmd_ret().
remove_member(ServerLoc, ServerId) ->
    remove_member(ServerLoc, ServerId, ?DEFAULT_TIMEOUT).

%% @see remove_member/2
-spec remove_member(ra_server_id() | [ra_server_id()],
                    ra_server_id(), timeout()) -> ra_cmd_ret().
remove_member(ServerLoc, ServerId, Timeout) ->
    ra_server_proc:command(ServerLoc,
                           {'$ra_leave', ServerId, after_log_append},
                           Timeout).

%% @doc Causes the server to entre the pre-vote and attempt become leader
%% It is necessary to call this function when starting a new cluster as a
%% branch new ra server will not automatically enter pre-vote by itself.
%% Previously started servers will however.
%%
%% @param ServerId the ra server id of the server to trigger the election on.
-spec trigger_election(ra_server_id()) -> ok.
trigger_election(ServerId) ->
    trigger_election(ServerId, ?DEFAULT_TIMEOUT).

-spec trigger_election(ra_server_id(), timeout()) -> ok.
trigger_election(ServerId, Timeout) ->
    ra_server_proc:trigger_election(ServerId, Timeout).

% safe way to remove an active server from a cluster
leave_and_terminate(ServerId) ->
    leave_and_terminate(ServerId, ServerId).

-spec leave_and_terminate(ra_server_id(), ra_server_id()) ->
    ok | timeout | {error, noproc}.
leave_and_terminate(ServerRef, ServerId) ->
    leave_and_terminate(ServerRef, ServerId, ?DEFAULT_TIMEOUT).

-spec leave_and_terminate(ra_server_id(), ra_server_id(), timeout()) ->
    ok | timeout | {error, noproc}.
leave_and_terminate(ServerRef, ServerId, Timeout) ->
    LeaveCmd = {'$ra_leave', ServerId, await_consensus},
    case ra_server_proc:command(ServerRef, LeaveCmd, Timeout) of
        {timeout, Who} ->
            ?ERR("Failed to leave the cluster: request to ~w timed out", [Who]),
            timeout;
        {error, noproc} = Err ->
            Err;
        {ok, _, _} ->
            ?INFO("We (Ra node ~w) has successfully left the cluster. Terminating.", [ServerId]),
            stop_server(ServerId)
    end.

% safe way to delete an active server from a cluster
leave_and_delete_server(ServerLoc) ->
    leave_and_delete_server(ServerLoc, ServerLoc).

-spec leave_and_delete_server(ra_server_id() | [ra_server_id()],
                               ra_server_id()) ->
    ok | timeout | {error, noproc}.
leave_and_delete_server(ServerLoc, ServerId) ->
    leave_and_delete_server(ServerLoc, ServerId, ?DEFAULT_TIMEOUT).

-spec leave_and_delete_server(ra_server_id() | [ra_server_id()],
                              ra_server_id(), timeout()) ->
    ok | timeout | {error, noproc}.
leave_and_delete_server(ServerRef, ServerId, Timeout) ->
    LeaveCmd = {'$ra_leave', ServerId, await_consensus},
    case ra_server_proc:command(ServerRef, LeaveCmd, Timeout) of
        {timeout, Who} ->
            ?ERR("Failed to leave the cluster: request to ~w timed out", [Who]),
            timeout;
        {error, _} = Err ->
            Err;
        {ok, _, _} ->
            ?INFO("Ra node ~w has succesfully left the cluster.", [ServerId]),
            force_delete_server(ServerId)
    end.

%% @doc generates a random uid using the provided source material for the first
%% 6 characters
new_uid(Source) when is_binary(Source) ->
    Prefix = ra_lib:derive_safe_string(ra_lib:to_binary(Source), 6),
    ra_lib:make_uid(string:uppercase(Prefix)).


%% @doc return a map of overview data of the ra system on the current erlang
%% node.
-spec overview() -> map().
overview() ->
    #{node => node(),
      servers => ra_directory:overview(),
      wal => #{max_batch_size =>
               lists:max([X || {X, _} <- ets:tab2list(ra_log_wal_metrics)]),
               status => lists:nth(5, element(4, sys:get_status(ra_log_wal))),
               open_mem_tables => ets:info(ra_log_open_mem_tables, size),
               closed_mem_tables => ets:info(ra_log_closed_mem_tables, size)},
      segment_writer => ra_log_segment_writer:overview()
     }.

%% @doc Submits a command to a ra server. Returs after the command has
%% been applied to the Raft state machine. If the state machine returned a
%% response it is included in the second element of the response tuple.
%% If the no response was returned the second element is the atom `noreply'.
%% If the server receiving the command isn't the current leader it will
%% redirect the call to the leader if known or hold on to the command until
%% a leader is known. The leader's server id is returned as the 3rd element
%% of the success reply tuple.
%% @param ServerId the server id to send the command to
%% @param Command an arbitrary term that the state machine can handle
%% @param Timeout the time to wait before returning {timeout, ServerId}
-spec process_command(ServerLoc :: ra_server_id() | [ra_server_id()],
                      Command :: term(),
                      Timeout :: non_neg_integer()) ->
    {ok, Reply :: term(), Leader :: ra_server_id()} |
    {error, term()} |
    {timeout, ra_server_id()}.
process_command(ServerLoc, Cmd, Timeout) ->
    ra_server_proc:command(ServerLoc, usr(Cmd, await_consensus), Timeout).

-spec process_command(ServerLoc :: ra_server_id() | [ra_server_id()],
                      Command :: term()) ->
    {ok, Reply :: term(), Leader :: ra_server_id()} |
    {error, term()} |
    {timeout, ra_server_id()}.
process_command(ServerLoc, Command) ->
    process_command(ServerLoc, Command, ?DEFAULT_TIMEOUT).


%% @doc Submits a command to the ra server using a gen_statem:cast passing
%% an optional process scoped term as correlation identifier.
%% A correlation id can be included
%% to implement reliable async interactions with the ra system. The calling
%% process can retain a map of command that have not yet been applied to the
%% state machine successfully and resend them if a notification is not receied
%% withing some time window.
%% When the command is applied to the state machine the ra server will send
%% the calling process a ra_event of the following structure:
%%
%% `{ra_event, CurrentLeader, {applied, [{Correlation, Reply}]}}'
%%
%% Not that ra will batch notification and thus return a list of correlation
%% and result tuples.
%%
%% If the receving ra server isn't a leader a ra event of the following
%% structure will be returned informing the caller that it cannot process the
%% message including the current leader, if known:
%%
%% `{ra_event, CurrentLeader, {rejected, {not_leader, Leader, Correlation}}}'
%% The caller can then redirect the command for the correlation identifier to
%% the correct ra server.
%%
%% If insteads the atom `no_correlation' is used the calling process will not
%% receive any notification of command processing success or otherwise.
%% This is the
%% least reliable way to interact with a ra system and should only be used
%% if the command is of little importance.
%%
%% @param ServerId the ra server id to send the command to
%% @param Command an arbitrary term that the state machine can handle
%% @param Correlation a correlation identifier to be included to receive an
%% async notification after the command is applied to the state machine. If the
%% Correlation is `no_correlation' no notifications will be sent.
%% @param Priority command priority. `low' priority commands will be held back
%% and appended to the Raft log in batches. NB: A `normal' priority command sent
%% from the same process can overtake a low priority command that was
%% sent before. There is no high priority.
%% Only use pritory level `low' for commands where
%% total ordering does not matter.
-spec pipeline_command(ServerId :: ra_server_id(), Command :: term(),
                       Correlation :: ra_server:command_correlation() |
                                      no_correlation,
                       Priority :: normal | low) ->
    ok.
pipeline_command(ServerId, Command, Correlation, Priority)
  when Correlation /= no_correlation ->
    Cmd = usr(Command, {notify, Correlation, self()}),
    ra_server_proc:cast_command(ServerId, Priority, Cmd);
pipeline_command(ServerId, Command, no_correlation, Priority) ->
    Cmd = usr(Command, noreply),
    ra_server_proc:cast_command(ServerId, Priority, Cmd).

-spec pipeline_command(ServerId :: ra_server_id(), Command :: term(),
                       Correlation :: ra_server:command_correlation() |
                                      no_correlation) ->
    ok.
pipeline_command(ServerId, Command, Correlation) ->
    pipeline_command(ServerId, Command, Correlation, low).


%% @doc Sends a command to the ra server using a gen_statem:cast.
%% Effectively the same as
%% `ra:pipeline_command(ServerId, Command, low, no_correlation)'
%% This is the least reliable way to interact with a ra system and should only
%% be used for commands that are of little importance and where waiting for
%% a response is prohibitively slow.
-spec pipeline_command(ServerId :: ra_server_id(),
                       Command :: term()) -> ok.
pipeline_command(ServerId, Command) ->
    pipeline_command(ServerId, Command, no_correlation, low).

%% @doc Query the machine state on any server
%% This allows you to run the QueryFun over the server machine state and
%% return the result. Any ra server can be addressed.
%% This can return infinitely stale results.
-spec local_query(ServerId :: ra_server_id(),
                  QueryFun :: query_fun()) ->
    ra_server_proc:ra_leader_call_ret({ra_idxterm(), term()}).
local_query(ServerRef, QueryFun) ->
    local_query(ServerRef, QueryFun, ?DEFAULT_TIMEOUT).

-spec local_query(ServerId :: ra_server_id(),
                  QueryFun :: query_fun(),
                  Timeout :: timeout()) ->
    ra_server_proc:ra_leader_call_ret({ra_idxterm(), term()}).
local_query(ServerRef, QueryFun, Timeout) ->
    ra_server_proc:query(ServerRef, QueryFun, local, Timeout).


%% @doc Query the current leader state
%% This function works like local_query, but redirects to the current
%% leader node.
%% The leader state may be more up-to-date compared to local.
%% This function may still return stale results as it reads the current state
%% and does not wait for commands to be applied.
-spec leader_query(ServerId :: ra_server_id(),
                   QueryFun :: query_fun()) ->
    {ok, {ra_idxterm(), term()}, ra_server_id() | not_known}.
leader_query(ServerRef, QueryFun) ->
    leader_query(ServerRef, QueryFun, ?DEFAULT_TIMEOUT).

-spec leader_query(ServerId :: ra_server_id(),
                   QueryFun :: query_fun(),
                   Timeout :: timeout()) ->
    {ok, {ra_idxterm(), term()}, ra_server_id() | not_known}.
leader_query(ServerRef, QueryFun, Timeout) ->
    ra_server_proc:query(ServerRef, QueryFun, leader, Timeout).

%% @doc Query the state machine
%% This allows a caller to query the state machine on the leader node with
%% an additional heartbeat to check that the node is still the leader.
%% Consistency guarantee is that the query will return result containing
%% at least all changes, committed before this query is issued.
%% This may include changes which were committed while the query is running.
-spec consistent_query(Server::ra_server_id(),
                       QueryFun :: query_fun()) ->
    {ok, Reply :: term(), ra_server_id() | not_known}.
consistent_query(Server, QueryFun) ->
    consistent_query(Server, QueryFun, ?DEFAULT_TIMEOUT).

-spec consistent_query(Server::ra_server_id(),
                       QueryFun :: query_fun(),
                       Timeout :: timeout()) ->
    {ok, Reply :: term(), ra_server_id() | not_known}.
consistent_query(Server, QueryFun, Timeout) ->
    ra_server_proc:query(Server, QueryFun, consistent, Timeout).

%% @doc Query the members of a cluster
-spec members(ra_server_id()) ->
    ra_server_proc:ra_leader_call_ret([ra_server_id()]).
members(ServerRef) ->
    members(ServerRef, ?DEFAULT_TIMEOUT).

-spec members(ra_server_id(), timeout()) ->
    ra_server_proc:ra_leader_call_ret([ra_server_id()]).
members(ServerRef, Timeout) ->
    ra_server_proc:state_query(ServerRef, members, Timeout).

%% internal

usr(Data, Mode) ->
    {'$usr', Data, Mode}.

sort_by_local([], Acc) ->
    Acc;
sort_by_local([{_, N} = X | Rem], Acc) when N =:= node() ->
    [X | Acc] ++ Rem;
sort_by_local([X | Rem], Acc) ->
    sort_by_local(Rem, [X | Acc]).
