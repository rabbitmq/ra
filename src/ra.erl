%% @doc The primary module for interacting with ra servers and clusters.

-module(ra).

-include("ra.hrl").

-export([
         start/0,
         send/2,
         send/3,
         send_and_await_consensus/2,
         send_and_await_consensus/3,
         send_and_notify/3,
         send_and_notify/4,
         cast/2,
         cast/3,
         members/1,
         members/2,
         local_query/2,
         local_query/3,
         consistent_query/2,
         consistent_query/3,
         % cluster operations
         start_cluster/3,
         start_or_restart_cluster/3,
         delete_cluster/1,
         delete_cluster/2,
         % cluster management
         start_server/1,
         start_server/4,
         restart_server/1,
         stop_server/1,
         delete_server/1,
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
         overview/0
        ]).

-type ra_cmd_ret() :: ra_server_proc:ra_cmd_ret().

%% @doc Starts the ra application
-spec start() -> ok.
start() ->
    {ok, _} = application:ensure_all_started(ra),
    ok.

%% @doc Starts a ra server
%% @param Conf a ra_server_config() configuration map.
%% @returns `{ok | error, Error}'
-spec start_server(ra_server:ra_server_config()) -> ok | {error, term()}.
start_server(Conf) ->
    %% validate UID is safe
    case ra_lib:validate_base64uri(maps:get(uid, Conf)) of
        true ->
            % don't match on return value in case it is already running
            case catch ra_server_sup:start_server(Conf) of
                {ok, _} -> ok;
                {ok, _, _} -> ok;
                {error, _} = Err -> Err;
                {'EXIT', Err} -> {error, Err};
                {badrpc, Reason} -> {error, Reason}
            end;
        false ->
            {error, invalid_uid}
    end.

%% @doc Restarts a previously succesfully started ra server
%% @param ServerId the ra_server_id() of the server
%% @returns `{ok | error, Error}' when error can be
%% `not_found' or `name_not_registered' when the ra server has never before
%% been started on the erlang node.
-spec restart_server(ra_server_id()) -> ok | {error, term()}.
restart_server(ServerId) ->
    % don't match on return value in case it is already running
    case catch ra_server_sup:restart_server(ServerId) of
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
    try ra_server_sup:stop_server(ServerId) of
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
-spec delete_server(ServerId :: ra_server_id()) -> ok | {error, term()}.
delete_server(ServerId) ->
    ra_server_sup:delete_server(ServerId).

%% @doc Starts or restarts a ra cluster.
%%
%%
%% @param ClusterId the cluster id of the cluster.
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
-spec start_or_restart_cluster(ra_cluster_id(), ra_server:machine_conf(),
                               [ra_server_id()]) ->
    {ok, [ra_server_id()], [ra_server_id()]} |
    {error, cluster_not_formed}.
start_or_restart_cluster(ClusterId, Machine,
                         [FirstServer | RemServers] = ServerIds) ->
    case ra_server_sup:restart_server(FirstServer) of
        {ok, _} ->
            %% restart the rest of the servers
            [{ok, _} = ra_server_sup:restart_server(N) || N <- RemServers],
            {ok, ServerIds, []};
        {error, Err} ->
            ?INFO("start_or_restart_cluster: got error ~p~n", [Err]),
            start_cluster(ClusterId, Machine, ServerIds)
    end.

%% @doc Starts a new distributed ra cluster.
%%
%%
%% @param ClusterId the cluster id of the cluster.
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
-spec start_cluster(ra_cluster_id(), ra_server:machine_conf(), [ra_server_id()]) ->
    {ok, [ra_server_id()], [ra_server_id()]} |
    {error, cluster_not_formed}.
start_cluster(ClusterId, Machine, ServerIds) ->
    {Started, NotStarted} =
        ra_lib:partition_parallel(fun (N) ->
                                          ok ==  start_server(ClusterId, N,
                                                            Machine, ServerIds)
                                  end, ServerIds),
    case Started of
        [] ->
            ?WARN("ra: failed to form new cluster ~w.~n "
                  "No servers were succesfully started.~n", [ClusterId]),
            {error, cluster_not_formed};
        _ ->
            %% try triggering elections until one succeeds
            _ = lists:any(fun (N) -> ok == trigger_election(N) end,
                          sort_by_local(Started, [])),
            %% TODO: handle case where no election was successfully triggered
            case members(hd(Started), length(ServerIds) * ?DEFAULT_TIMEOUT) of
                {ok, _, Leader} ->
                    ?INFO("ra: started cluster ~s with ~b servers~n"
                          "~b servers failed to start: ~w~n"
                          "Leader: ~w", [ClusterId, length(ServerIds),
                                         length(NotStarted), NotStarted,
                                         Leader]),
                    % we have a functioning cluster
                    {ok, Started, NotStarted};
                Err ->
                    ?WARN("ra: failed to form new cluster ~w.~n "
                          "Error: ~w~n", [ClusterId, Err]),
                    [delete_server(N) || N <- Started],
                    % we do not have a functioning cluster
                    {error, cluster_not_formed}
            end
    end.


-spec start_server(ra_cluster_id(), ra_server_id(),
                 ra_server:machine_conf(), [ra_server_id()]) ->
    ok | {error, term()}.
start_server(ClusterId, ServerId, Machine, ServerIds) ->
    Prefix = ra_lib:derive_safe_string(ra_lib:to_binary(ClusterId), 4),
    UId = ra_lib:make_uid(string:uppercase(Prefix)),
    Conf = #{cluster_id => ClusterId,
             id => ServerId,
             uid => UId,
             initial_members => ServerIds,
             log_init_args => #{uid => UId},
             machine => Machine},
    start_server(Conf).

%% @doc Deletes a ra cluster in an orderly fashion
%% This function commits and end of life command which after each server applies
%% it will cause that server to shut down and delete all it's data.
%% The leader will stay up until it has successfully replicated the end of life
%% command to all servers after which it too will shut down and delete all it's
%% data.
%% @param ServerIds the ra_server_ids of the cluster
%% @returns `{ok | error, nodedown}'
-spec delete_cluster(ServerIds :: [ra_server_id()]) ->
    {ok, ra_server_id()} | {error, term()}.
delete_cluster(ServerIds) ->
    delete_cluster(ServerIds, ?DEFAULT_TIMEOUT).

%% @see delete_cluster/1
-spec delete_cluster(ServerIds :: [ra_server_id()], timeout()) ->
    {ok, Leader :: ra_server_id()} | {error, term()}.
delete_cluster(ServerIds, Timeout) ->
    delete_cluster0(ServerIds, Timeout, []).

delete_cluster0([ServerId | Rem], Timeout, Errs) ->
    DeleteCmd = {'$ra_cluster', delete, await_consensus},
    case ra_server_proc:command(ServerId, DeleteCmd, Timeout) of
        {ok, _, Leader} ->
            {ok, Leader};
        {timeout, _} = E ->
            delete_cluster0(Rem, Timeout, [E | Errs]);
        {error, _} = E ->
            delete_cluster0(Rem, Timeout, [{E, ServerId} | Errs])
    end;
delete_cluster0([], _, Errs) ->
    {error, {no_more_servers_to_try, Errs}}.


%% @doc Add a ra server id to a ra cluster's membership configuration
%% This commits a join command to the leader log. After this has been replicated
%% the leader will start replicating entries to the new server.
%% This function returns after appending the command to the log.
%%
%% @param ServerRef the ra server to send the command to
%% @param ServerId the ra server id of the new server
-spec add_member(ra_server_id(), ra_server_id()) -> ra_cmd_ret().
add_member(ServerRef, ServerId) ->
    add_member(ServerRef, ServerId, ?DEFAULT_TIMEOUT).

%% @see add_member/2
-spec add_member(ra_server_id(), ra_server_id(), timeout()) -> ra_cmd_ret().
add_member(ServerRef, ServerId, Timeout) ->
    ra_server_proc:command(ServerRef, {'$ra_join', ServerId, after_log_append},
                         Timeout).


%% @doc Removes a server from the cluster's membership configuration
%% This function returns after appending the command to the log.
%%
%% @param ServerRef the ra server to send the command to
%% @param ServerId the ra server id of the server to remove
-spec remove_member(ra_server_id(), ra_server_id()) -> ra_cmd_ret().
remove_member(ServerRef, ServerId) ->
    remove_member(ServerRef, ServerId, ?DEFAULT_TIMEOUT).

%% @see remove_member/2
-spec remove_member(ra_server_id(), ra_server_id(), timeout()) -> ra_cmd_ret().
remove_member(ServerRef, ServerId, Timeout) ->
    ra_server_proc:command(ServerRef, {'$ra_leave', ServerId, after_log_append},
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
    ok | timeout | {error, no_proc}.
leave_and_terminate(ServerRef, ServerId) ->
    leave_and_terminate(ServerRef, ServerId, ?DEFAULT_TIMEOUT).

-spec leave_and_terminate(ra_server_id(), ra_server_id(), timeout()) ->
    ok | timeout | {error, no_proc}.
leave_and_terminate(ServerRef, ServerId, Timeout) ->
    LeaveCmd = {'$ra_leave', ServerId, await_consensus},
    case ra_server_proc:command(ServerRef, LeaveCmd, Timeout) of
        {timeout, Who} ->
            ?ERR("request to ~p timed out trying to leave the cluster", [Who]),
            timeout;
        {error, no_proc} = Err ->
            Err;
        {ok, _, _} ->
            ?ERR("~p has left the building. terminating", [ServerId]),
            stop_server(ServerId)
    end.

% safe way to delete an active server from a cluster
leave_and_delete_server(ServerId) ->
    leave_and_delete_server(ServerId, ServerId).

-spec leave_and_delete_server(ra_server_id(), ra_server_id()) ->
    ok | timeout | {error, no_proc}.
leave_and_delete_server(ServerRef, ServerId) ->
    leave_and_delete_server(ServerRef, ServerId, ?DEFAULT_TIMEOUT).

-spec leave_and_delete_server(ra_server_id(), ra_server_id(), timeout()) ->
    ok | timeout | {error, no_proc}.
leave_and_delete_server(ServerRef, ServerId, Timeout) ->
    LeaveCmd = {'$ra_leave', ServerId, await_consensus},
    case ra_server_proc:command(ServerRef, LeaveCmd, Timeout) of
        {timeout, Who} ->
            ?ERR("request to ~p timed out trying to leave the cluster", [Who]),
            timeout;
        {error, no_proc} = Err ->
            Err;
        {ok, _, _} ->
            ?ERR("~p has left the building. terminating", [ServerId]),
            delete_server(ServerId)
    end.


%% @doc return a map of overview data of the ra system on the current erlang
%% node.
-spec overview() -> map().
overview() ->
    #{node => node(),
      servers => ra_directory:overview(),
      wal => #{max_batch_size =>
                   lists:max([X || {X, _} <- ets:tab2list(ra_log_wal_metrics)]),
               status => sys:get_state(ra_log_wal),
               open_mem_tables => ets:info(ra_log_open_mem_tables, size),
               closed_mem_tables => ets:info(ra_log_closed_mem_tables, size)},
      segment_writer => ra_log_segment_writer:overview()
      }.

%% @see send/3
-spec send(ra_server_id(), term()) -> ra_cmd_ret().
send(Ref, Data) ->
    send(Ref, Data, ?DEFAULT_TIMEOUT).

%% @doc send a command to the ra server.
%% if the ra server addressed isn't the leader and the leader is known
%% it will automatically redirect the call to the leader server.
%% This function returns after the command has been appended to the leader's
%% raft log.
%%
%% @param ServerRef the ra server id of the server to send the commadn to.
%% @param Command the command, an arbitrary term that the current state
%% machine can understand.
%% @param Timeout a timeout value
%% @returns {@link ra_cmd_ret()}
-spec send(ra_server_id(), term(), timeout()) -> ra_cmd_ret().
send(ServerRef, Command, Timeout) ->
    ra_server_proc:command(ServerRef, usr(Command, after_log_append), Timeout).

-spec send_and_await_consensus(ra_server_id(), term()) -> ra_cmd_ret().
send_and_await_consensus(Ref, Data) ->
    send_and_await_consensus(Ref, Data, ?DEFAULT_TIMEOUT).

%% @doc send a command to the ra server.
%% if the ra server addressed isn't the leader and the leader is known
%% it will automatically redirect the call to the leader server.
%% This function returns after the command has been replicated and applied to
%% the ra state machine. This is a fully synchronous interaction with the
%% ra consensus system.
%% Use this for low throughput actions where simple semantics are needed.
%% if the state machine supports it it may return a result value which will
%% be included in the result tuple.
%%
%% @param ServerRef the ra server id of the server to send the commadn to.
%% @param Command the command, an arbitrary term that the current state
%% machine can understand.
%% @param Timeout a timeout value
%% @returns {@link ra_cmd_ret()}
-spec send_and_await_consensus(ra_server_id(), term(), timeout()) ->
    ra_cmd_ret().
send_and_await_consensus(Ref, Data, Timeout) ->
    ra_server_proc:command(Ref, usr(Data, await_consensus), Timeout).

%% @doc send a command to the ra server using cast.
%% This will send a command to the ra server using a cast.
%% if the server addressed isn't the leader the command will be discarded and
%% and asyncronous notification message returned to the caller of the format:
%% `{ra_event, ra_server_id(), {rejected, {not_leader, Correlation, LeaderId}}'.
%%
%% If the server addressed is the leader the command will be appended to the log
%% and replicated. Once it achieves consensus and asynchronous notification
%% message of the format:
%% `{ra_event, ra_server_id(), {applied, [Correlation]}}'
%%
%% @param ServerRef the ra server id of the server to send the commadn to.
%% @param Command the command, an arbitrary term that the current state
%% machine can understand.
%% @param Timeout a timeout value
%% @returns {@link ra_cmd_ret()}
-spec send_and_notify(ra_server_id(), term(), term()) -> ok.
send_and_notify(ServerRef, Command, Correlation) ->
    Cmd = usr(Command, {notify_on_consensus, Correlation, self()}),
    ra_server_proc:cast_command(ServerRef, Cmd).

-spec send_and_notify(ra_server_id(), high | normal, term(), term()) -> ok.
send_and_notify(ServerRef, Priority, Command, Correlation) ->
    Cmd = usr(Command, {notify_on_consensus, Correlation, self()}),
    ra_server_proc:cast_command(ServerRef, Priority, Cmd).

%% @doc Cast a message to a server
%% This is the least reliable way to interact with a ra server. If the server
%% addressed isn't the leader no notification will be issued.
-spec cast(ra_server_id(), term()) -> ok.
cast(ServerRef, Command) ->
    Cmd = usr(Command, noreply),
    ra_server_proc:cast_command(ServerRef, Cmd).

%% @doc Cast a message to a server with a priority
%% This is the least reliable way to interact with a ra server. If the server
%% addressed isn't the leader no notification will be issued.
-spec cast(ra_server_id(), normal | high, term()) -> ok.
cast(ServerRef, Priority, Command) ->
    Cmd = usr(Command, noreply),
    ra_server_proc:cast_command(ServerRef, Priority, Cmd).

%% @doc query the machine state on any server
%% This allows you to run the QueryFun over the server machine state and
%% return the result. Any ra server can be addressed.
%% This can return infinitely stale results.
-spec local_query(ServerId :: ra_server_id(),
                      QueryFun :: fun((term()) -> term())) ->
    {ok, {ra_idxterm(), term()}, ra_server_id() | not_known}.
local_query(ServerRef, QueryFun) ->
    local_query(ServerRef, QueryFun, ?DEFAULT_TIMEOUT).

-spec local_query(ServerId :: ra_server_id(),
                      QueryFun :: fun((term()) -> term()),
                      Timeout :: timeout()) ->
    {ok, {ra_idxterm(), term()}, ra_server_id() | not_known}.
local_query(ServerRef, QueryFun, Timeout) ->
    ra_server_proc:query(ServerRef, QueryFun, local, Timeout).

%% @doc Query the state machine
%% This allows a caller to query the state machine by appending the query
%% to the log and returning the result once applied. This guarantees the
%% result is consistent.
-spec consistent_query(Server::ra_server_id(),
                       QueryFun::fun((term()) -> term())) ->
    {ok, {ra_idxterm(), term()}, ra_server_id() | not_known}.
consistent_query(Server, QueryFun) ->
    consistent_query(Server, QueryFun, ?DEFAULT_TIMEOUT).

-spec consistent_query(Server::ra_server_id(),
                       QueryFun::fun((term()) -> term()),
                       Timeout :: timeout()) ->
    {ok, {ra_idxterm(), term()}, ra_server_id() | not_known}.
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

