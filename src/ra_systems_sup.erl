%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2017-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%
%% @hidden
-module(ra_systems_sup).

-behaviour(supervisor).

-include("ra.hrl").

%% API functions
-export([start_link/0,
         start_system/1,
         stop_system/1]).

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
    ?INFO("starting Ra system: ~ts in directory: ~ts", [Name, Dir]),
    %% TODO: validate configuration
    ok = ra_system:store(Config),
    RaSystemsSup = #{id => Name,
                     type => supervisor,
                     start => {ra_system_sup, start_link, [Config]}},
    supervisor:start_child(?MODULE, RaSystemsSup).

-spec stop_system(ra_system:config() | atom()) -> ok | {error, any()}.
stop_system(#{name := Name}) when is_atom(Name) ->
    stop_system(Name);
stop_system(Name) when is_atom(Name) ->
    case supervisor:terminate_child(?MODULE, Name) of
        ok ->
            cleanup(Name);
        {error, not_found} ->
            cleanup(Name);
        {error, _} = Error ->
            Error
    end.

cleanup(Name) when is_atom(Name) ->
    _ = supervisor:delete_child(?MODULE, Name),
    _ = persistent_term:erase({'$ra_system', Name}),
    ok.

init([]) ->
    %% This is not something we want to expose. It helps test suites
    %% that crash Ra systems on purpose and may end up crashing
    %% the systems faster than we normally allow.
    {Intensity, Period} = application:get_env(ra, ra_systems_sup_intensity, {1, 5}),
    SupFlags = #{strategy => one_for_one, intensity => Intensity, period => Period},
    {ok, {SupFlags, []}}.



