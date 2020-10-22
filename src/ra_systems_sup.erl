%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2017-2020 VMware, Inc. or its affiliates.  All rights reserved.
%%
%% @hidden
-module(ra_systems_sup).

-behaviour(supervisor).

-include("ra.hrl").

%% API functions
-export([start_link/0,
         start_system/1]).

%% Supervisor callbacks
-export([init/1]).

-spec start_link() ->
    {ok, pid()} | ignore | {error, term()}.
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

-spec start_system(ra_system:config()) -> supervisor:startchild_ret().
start_system(#{name := Name,
               names := _Names,
               data_dir := Dir} = Config) when is_atom(Name) ->
    ?INFO("starting Ra system: ~s in directory: ~s", [Name, Dir]),
    %% TODO: validate configuration
    ok = ra_system:store(Config),
    RaSystemsSup = #{id => Name,
                     type => supervisor,
                     start => {ra_system_sup, start_link, [Config]}},
    supervisor:start_child(?MODULE, RaSystemsSup).


init([]) ->
    SupFlags = #{strategy => one_for_one, intensity => 1, period => 5},
    {ok, {SupFlags, []}}.



