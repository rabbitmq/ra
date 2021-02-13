%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2017-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%
%% @hidden
-module(ra_server_sup).

-behaviour(supervisor).

%% API functions
-export([start_link/1]).

%% Supervisor callbacks
-export([init/1]).

-include("ra.hrl").
%%%===================================================================
%%% API functions
%%%===================================================================

start_link(Config) ->
    supervisor:start_link(?MODULE, [Config]).

%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================

%%--------------------------------------------------------------------

init([Config0]) ->
    Id = maps:get(id, Config0),
    Config = Config0#{parent => self()},
    Name = ra_lib:ra_server_id_to_local_name(Id),
    SupFlags = #{strategy => one_for_one,
                 intensity => 2,
                 period => 5},
    ChildSpec = #{id => Name,
                  type => worker,
                  % needs to be transient as may shut itself down by returning
                  % {stop, normal, State}
                  restart => transient,
                  start => {ra_server_proc, start_link, [Config]}},
    {ok, {SupFlags, [ChildSpec]}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
