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
-export([start_server/2,
         restart_server/3,
         stop_server/2,
         delete_server/2,
         remove_all/1,
         start_link/1,
         recover_config/2,
         % for rpcs only
         start_server_rpc/3,
         restart_server_rpc/3,
         delete_server_rpc/2,
         prepare_server_stop_rpc/2]).

%% Supervisor callbacks
-export([init/1]).

-include("ra.hrl").

-spec start_server(System :: atom(), ra_server:ra_server_config()) ->
    supervisor:startchild_ret() | {error, not_new | system_not_started}.
start_server(System, #{id := NodeId,
                       uid := UId} = Config)
  when is_atom(System) ->
    Node = ra_lib:ra_server_id_node(NodeId),
    rpc:call(Node, ?MODULE, start_server_rpc, [System, UId, Config]).

-spec restart_server(atom(), ra_server_id(), ra_server:mutable_config()) ->
    supervisor:startchild_ret() | {error, system_not_started}.
restart_server(System, {RaName, Node}, AddConfig) ->
    rpc:call(Node, ?MODULE, restart_server_rpc,
             [System, {RaName, Node}, AddConfig]).

start_server_rpc(System, UId, Config0) ->
    case ra_system:fetch(System) of
        undefined ->
            {error, system_not_started};
        SysCfg ->
            Config = Config0#{system_config => SysCfg},
            %% check that the server isn't already registered
            case ra_directory:name_of(System, UId) of
                undefined ->
                    case ra_system:lookup_name(System, server_sup) of
                        {ok, Name} ->
                            supervisor:start_child({Name, node()}, [Config]);
                        Err ->
                            Err
                    end;
                Name ->
                    case whereis(Name) of
                        undefined ->
                            {error, not_new};
                        Pid ->
                            {error, {already_started, Pid}}
                    end
            end
    end.

restart_server_rpc(System, {RaName, Node}, AddConfig)
  when is_atom(System) ->
    case ra_system:fetch(System) of
        undefined ->
            {error, system_not_started};
        _ ->
            case recover_config(System, RaName) of
                {ok, Config0} ->
                    MutConfig = maps:with(?MUTABLE_CONFIG_KEYS, AddConfig),
                    {ok, Name} = ra_system:lookup_name(System, server_sup),
                    case maps:merge(Config0, MutConfig) of
                        Config0 ->
                            %% the config has not changed
                            Config = Config0#{has_changed => false},
                            supervisor:start_child({Name, Node}, [Config]);
                        Config1 ->
                            Config = Config1#{has_changed => true},
                            supervisor:start_child({Name, Node}, [Config])
                    end;
                Err ->
                    Err
            end
    end.

-spec stop_server(System :: atom(), RaNodeId :: ra_server_id()) ->
    ok | {error, term()}.
stop_server(System, ServerId) when is_atom(System) ->
    Node = ra_lib:ra_server_id_node(ServerId),
    RaName = ra_lib:ra_server_id_to_local_name(ServerId),
    Res = rpc:call(Node, ?MODULE,
                   prepare_server_stop_rpc, [System, RaName]),
    case Res of
        {error, _} = Err ->
            Err;
        {ok, Pid, SrvSup} when is_pid(Pid) ->
            supervisor:terminate_child({SrvSup, Node}, Pid);
        {ok, undefined, _} ->
            %% no parent - no need to stop
            ok;
        Err ->
            {error, Err}
    end.

prepare_server_stop_rpc(System, RaName) ->
    case ra_system:fetch(System) of
        undefined ->
            {error, system_not_started};
        #{names := #{server_sup := SrvSup} = Names} ->
            Parent =  ra_directory:where_is_parent(Names, RaName),
            {ok, Parent, SrvSup}
    end.

-spec delete_server(atom(), NodeId :: ra_server_id()) ->
    ok | {error, term()} | {badrpc, term()}.
delete_server(System, NodeId) when is_atom(System) ->
    Node = ra_lib:ra_server_id_node(NodeId),
    Name = ra_lib:ra_server_id_to_local_name(NodeId),
    case stop_server(System, NodeId) of
        ok ->
            rpc:call(Node, ?MODULE, delete_server_rpc, [System, Name]);
        {error, _} = Err -> Err
    end.

delete_server_rpc(System, RaName) ->
    case ra_system:fetch(System) of
        undefined ->
            {error, system_not_started};
        #{data_dir := _SysDir,
          names := #{log_meta := Meta,
                     server_sup := SrvSup} = Names} ->
            ?INFO("Deleting server ~w and its data directory.~n",
                  [RaName]),
            %% TODO: better handle and report errors
            UId = ra_directory:uid_of(Names, RaName),
            Pid = ra_directory:where_is(Names, RaName),
            ra_log_meta:delete(Meta, UId),
            Dir = ra_env:server_data_dir(System, UId),
            _ = supervisor:terminate_child(SrvSup, UId),
            _ = delete_data_directory(Dir),
            _ = ra_directory:unregister_name(Names, UId),
            %% forcefully clean up ETS tables
            catch ets:delete(ra_log_metrics, UId),
            catch ets:delete(ra_log_snapshot_state, UId),
            catch ets:delete(ra_metrics, RaName),
            catch ets:delete(ra_state, RaName),
            catch ets:delete(ra_open_file_metrics, Pid),
            ok
    end.

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

remove_all(System) when is_atom(System) ->
    #{names := #{server_sup := Sup}} = ra_system:fetch(System),
    _ = [begin
             ?DEBUG("ra: terminating child ~w in system ~s~n", [Pid, System]),
             supervisor:terminate_child(Sup, Pid)
         end
         || {_, Pid, _, _} <- supervisor:which_children(Sup)],
    ok.

recover_config(System, RaName) ->
    case ra_directory:uid_of(System, RaName) of
        undefined ->
            {error, name_not_registered};
        UId ->
            Dir = ra_env:server_data_dir(System, UId),
            case ra_directory:where_is(System, UId) of
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

-spec start_link(ra_system:config()) ->
    {ok, pid()} | ignore | {error, term()}.
start_link(#{names := #{server_sup := Name}}) ->
    supervisor:start_link({local, Name}, ?MODULE, []).

init([]) ->
    SupFlags = #{strategy => simple_one_for_one},
    ChildSpec = #{id => undefined,
                  type => supervisor,
                  restart => temporary,
                  start => {ra_server_sup, start_link, []}},
    {ok, {SupFlags, [ChildSpec]}}.
