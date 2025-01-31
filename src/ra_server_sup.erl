%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2017-2025 Broadcom. All Rights Reserved. The term Broadcom refers to Broadcom Inc. and/or its subsidiaries.
%%
%% @hidden
-module(ra_server_sup).

-behaviour(supervisor).

%% API functions
-export([start_link/1]).
-export([start_ra_worker/2]).

%% Supervisor callbacks
-export([init/1]).

%%%===================================================================
%%% API functions
%%%===================================================================

start_link(Config) ->
    supervisor:start_link(?MODULE, Config).

-spec start_ra_worker(pid(), ra_server:config()) ->
    supervisor:startchild_ret().
start_ra_worker(SupPid, Config)
  when is_pid(SupPid) andalso
       is_map(Config) ->
    RaWorker = #{id => ra_worker,
                 type => worker,
                 restart => transient,
                 start => {ra_worker, start_link, [Config]}},
    supervisor:start_child(SupPid, RaWorker).

%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================

%%--------------------------------------------------------------------

init(Config0) ->
    Id = maps:get(id, Config0),
    Config = Config0#{parent => self()},
    Name = ra_lib:ra_server_id_to_local_name(Id),
    SupFlags = #{strategy => one_for_all,
                 intensity => 2,
                 period => 5},
    RaServer = #{id => Name,
                 type => worker,
                 % needs to be transient as may shut itself down by returning
                 % {stop, normal, State}
                 restart => transient,
                 start => {ra_server_proc, start_link, [Config]}},
    {ok, {SupFlags, [RaServer]}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
