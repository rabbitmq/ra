%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2017-2025 Broadcom. All Rights Reserved. The term Broadcom refers to Broadcom Inc. and/or its subsidiaries.
%%
%% @hidden
-module(ra_system_sup).

-behaviour(supervisor).

-include("ra.hrl").

%% API functions
-export([start_link/1,
         child_spec/1]).

%% Supervisor callbacks
-export([init/1]).


start_link(Config) ->
    supervisor:start_link(?MODULE, Config).

%% @doc Build a child spec for a Ra system so that it can be started as a
%% static child of a consuming application's own supervision tree, rather than
%% as a dynamic child of `ra_systems_sup'.
%%
%% A system started this way still registers itself (see
%% `ra_system_registration', the first child started by this supervisor) so the
%% rest of the Ra API can discover it by name.
%%
%% This is an alternative to `ra:start_system/1' which places the system under
%% an arbitrary supervision tree rather than `ra_systems_sup'.
-spec child_spec(ra_system:config()) -> supervisor:child_spec().
child_spec(#{name := Name} = Config) ->
    #{id => Name,
      type => supervisor,
      start => {?MODULE, start_link, [Config]}}.

init(#{data_dir := DataDir,
       name := Name,
       names := _Names} = Cfg) ->
    case ra_lib:make_dir(DataDir) of
        ok ->
            ?DEBUG("ra system '~s' starting", [Name]),
            SupFlags = #{strategy => one_for_all, intensity => 1, period => 5},
            %% Owns the {'$ra_system', Name} persistent_term registration
            %% lifecycle. Listed first so that, under one_for_all, it starts
            %% first (registering before the other children come up) and
            %% terminates last (erasing only after every other child has
            %% stopped reading the config).
            Registration = #{id => ra_system_registration,
                             start => {ra_system_registration, start_link, [Cfg]}},
            %% the ra log ets process is supervised by the system to keep mem tables
            %% alive whilst the rest of the log infra might be down
            Ets = #{id => ra_log_ets,
                    start => {ra_log_ets, start_link, [Cfg]}},
            RaLogSup = #{id => ra_log_sup,
                         type => supervisor,
                         start => {ra_log_sup, start_link, [Cfg]}},
            RaServerSupSup = #{id => ra_server_sup_sup,
                               type => supervisor,
                               start => {ra_server_sup_sup, start_link, [Cfg]}},
            Recover = #{id => ra_system_recover,
                        start => {ra_system_recover, start_link, [maps:get(name, Cfg)]}},
            {ok, {SupFlags, [Registration, Ets, RaLogSup, RaServerSupSup, Recover]}};
        {error, Code} ->
            ?ERR("Failed to create Ra data directory at '~ts', file system operation error: ~p", [DataDir, Code]),
            exit({error, "Ra could not create its data directory. See the log for details."})
    end.



