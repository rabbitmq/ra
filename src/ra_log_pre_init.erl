%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term Broadcom refers to Broadcom Inc. and/or its subsidiaries.
%% @hidden
-module(ra_log_pre_init).

-behaviour(gen_server).

-include("ra.hrl").
%% API functions
-export([start_link/1]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {}).

-define(ETSTBL, ra_log_snapshot_state).
%%%===================================================================
%%% API functions
%%%===================================================================

start_link(System) when is_atom(System) ->
    gen_server:start_link(?MODULE, [System], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([System]) ->
    %% ra_log:pre_init ensures that the ra_log_snapshot_state table is
    %% populated before WAL recovery begins to avoid writing unnecessary
    %% indexes to segment files.
    Regd = ra_directory:list_registered(System),
    ?INFO("ra system '~ts' running pre init for ~b registered servers",
          [System, length(Regd)]),
    _ = [begin
             try pre_init(System, UId) of
                 ok -> ok
             catch _:Err ->
                       ?ERROR("pre_init failed in system ~s for UId ~ts with name ~ts"
                              " This error may need manual intervention, Error ~p",
                              [System, UId, Name, Err]),
                       ok
             end
         end|| {Name, UId} <- Regd],
    {ok, #state{} , hibernate}.

handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

pre_init(System, UId) ->
    case ets:lookup(?ETSTBL, UId) of
        [{_, _, _, _}] ->
            %% already initialised
            ok;
        [] ->
            case ra_system:fetch(System) of
                undefined ->
                    {error, system_not_started};
                SysCfg ->
                    %% check if the server dir exists, if not
                    %% then just log and return instead of failing.
                    Dir = ra_env:server_data_dir(System, UId),
                    case ra_lib:is_dir(Dir) of
                        true ->
                            case ra_log:read_config(Dir) of
                                {ok, #{log_init_args := Log}} ->
                                    ok = ra_log:pre_init(Log#{system_config => SysCfg}),
                                    ok;
                                {error, Err} ->
                                    ?ERROR("pre_init failed to read config file for UId '~ts', Err ~p",
                                           [UId, Err]),
                                    ok
                            end;
                        false ->
                            ?INFO("pre_init UId '~ts' is registered but no data
                                  directory was found, removing from ra directory",
                                  [UId]),
                            _ = catch ra_directory:unregister_name(System, UId),
                            ok
                    end
            end
    end.

