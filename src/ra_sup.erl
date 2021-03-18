%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2017-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%
%% @hidden
-module(ra_sup).
-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

-define(TABLES, [ra_metrics,
                 ra_state,
                 ra_open_file_metrics,
                 ra_io_metrics
                 ]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    _ = [ets:new(Table, [named_table, public, {write_concurrency, true}])
         || Table <- ?TABLES],

    %% configure the logger module from the application config
    Logger = application:get_env(ra, logger_module, logger),
    ok = ra_env:configure_logger(Logger),
    SupFlags = #{strategy => one_for_one, intensity => 1, period => 5},
    RaLogFileMetrics = #{id => ra_metrics_ets,
                         start => {ra_metrics_ets, start_link, []}},
    RaMachineEts = #{id => ra_machine_ets,
                     start => {ra_machine_ets, start_link, []}},
    RaFileHandle = #{id => ra_file_handle,
                     start => {ra_file_handle, start_link, []}},
    RaSystemsSup = #{id => ra_systems_sup,
                     type => supervisor,
                     start => {ra_systems_sup, start_link, []}},
    Procs = [RaMachineEts,
             RaLogFileMetrics,
             RaFileHandle,
             RaSystemsSup],
    {ok, {SupFlags, Procs}}.
