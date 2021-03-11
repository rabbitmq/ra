%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2017-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%
%% @hidden
-module(ra_server_sup_sup).

-behaviour(supervisor).

-define(MUTABLE_CONFIG_KEYS,
        [cluster_name,
         metrics_key,
         broadcast_time,
         tick_timeout,
         await_condition_timeout,
         max_pipeline_count,
         ra_event_formatter]).

%% API functions
-export([start_server/1,
         restart_server/2,
         stop_server/1,
         delete_server/1,
         remove_all/0,
         start_link/0,
         % for rpcs only
         prepare_start_rpc/1,
         prepare_restart_rpc/1,
         delete_server_rpc/1]).

%% Supervisor callbacks
-export([init/1]).

-include("ra.hrl").

-spec start_server(ra_server:ra_server_config()) ->
    supervisor:startchild_ret() | {error, not_new}.
start_server(#{id := NodeId,
               uid := UId} = Config) ->
    %% check that the node isn't already registered
    Node = ra_lib:ra_server_id_node(NodeId),
    case rpc:call(Node, ?MODULE, prepare_start_rpc, [UId]) of
        ok ->
            supervisor:start_child({?MODULE, Node}, [Config]);
        Err ->
            Err
    end.

-spec restart_server(ra_server_id(), ra_server:mutable_config()) ->
    supervisor:startchild_ret().
restart_server({RaName, Node}, AddConfig) ->
    case rpc:call(Node, ?MODULE, prepare_restart_rpc, [RaName]) of
        {ok, Config0} ->
            %% only certain config keys are mutable
            Config = maps:merge(Config0,
                                maps:with(?MUTABLE_CONFIG_KEYS, AddConfig)),
            supervisor:start_child({?MODULE, Node}, [Config]);
        {error, _} = Err ->
            Err;
        Err ->
            {error, Err}
    end.

prepare_start_rpc(UId) ->
    case ra_directory:name_of(UId) of
        undefined ->
            ok;
        Name ->
            case whereis(Name) of
                undefined ->
                    {error, not_new};
                Pid ->
                    {error, {already_started, Pid}}
              end
    end.

prepare_restart_rpc(RaName) ->
    case ra_directory:uid_of(RaName) of
        undefined ->
            name_not_registered;
        UId ->
            Dir = ra_env:server_data_dir(UId),
            case ra_directory:where_is(UId) of
                Pid when is_pid(Pid) ->
                    case is_process_alive(Pid) of
                        true ->
                            {error, {already_started, Pid}};
                        false ->
                            ra_log:read_config(Dir)
                    end;
                _ ->
                    % can it be made generic without already knowing the config state?
                    ra_log:read_config(Dir)
            end
    end.

-spec stop_server(RaNodeId :: ra_server_id()) -> ok | {error, term()}.
stop_server({RaName, Node}) ->
    Pid = rpc:call(Node, ra_directory,
                   where_is_parent, [RaName]),
    case Pid of
        undefined ->
          ok;
        _ when is_pid(Pid) ->
          supervisor:terminate_child({?MODULE, Node}, Pid);
        Err ->
        {error, Err}
    end;
stop_server(RaName) ->
    % local node
    case ra_directory:where_is_parent(RaName) of
        undefined -> ok;
        Pid ->
            supervisor:terminate_child(?MODULE, Pid)
    end.

-spec delete_server(NodeId :: ra_server_id()) ->
    ok | {error, term()} | {badrpc, term()}.
delete_server(NodeId) ->
    Node = ra_lib:ra_server_id_node(NodeId),
    Name = ra_lib:ra_server_id_to_local_name(NodeId),
    case stop_server(NodeId) of
        ok ->
            rpc:call(Node, ?MODULE, delete_server_rpc, [Name]);
        {error, _} = Err -> Err
    end.

delete_server_rpc(RaName) ->
    ?INFO("Deleting server ~w and its data directory.",
          [RaName]),
    %% TODO: better handle and report errors
    UId = ra_directory:uid_of(RaName),
    Pid = ra_directory:where_is(RaName),
    ra_log_meta:delete(UId),
    Dir = ra_env:server_data_dir(UId),
    _ = supervisor:terminate_child(?MODULE, UId),
    _ = delete_data_directory(Dir),
    _ = ra_directory:unregister_name(UId),
    %% forcefully clean up ETS tables
    catch ets:delete(ra_log_metrics, UId),
    catch ets:delete(ra_log_snapshot_state, UId),
    catch ets:delete(ra_metrics, RaName),
    catch ets:delete(ra_state, RaName),
    catch ets:delete(ra_open_file_metrics, Pid),
    ok.

delete_data_directory(Directory) ->
    DeleteFunction = fun() ->
                             try ra_lib:recursive_delete(Directory) of
                                 ok ->
                                     % moving on
                                     ok
                             catch
                                 _:_ = Err ->
                                     ?WARN("ra: delete_server/1 failed to delete directory ~s~n"
                                           "Error: ~p", [Directory, Err]),
                                     error
                             end
                     end,
    case DeleteFunction() of
        ok ->
            ok;
        _ ->
            spawn(fun() ->
                          ra_lib:retry(DeleteFunction, 2)
                  end)
    end.

remove_all() ->
    _ = [begin
             ?DEBUG("ra: terminating child ~w", [Pid]),
             supervisor:terminate_child(?MODULE, Pid)
         end
         || {_, Pid, _, _} <- supervisor:which_children(?MODULE)],
    ok.

-spec start_link() ->
    {ok, pid()} | ignore | {error, term()}.
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    SupFlags = #{strategy => simple_one_for_one},
    ChildSpec = #{id => undefined,
                  type => supervisor,
                  restart => temporary,
                  start => {ra_server_sup, start_link, []}},
    {ok, {SupFlags, [ChildSpec]}}.
