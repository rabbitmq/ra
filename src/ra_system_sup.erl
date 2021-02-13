%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2017-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%
%% @hidden
-module(ra_system_sup).

-behaviour(supervisor).

-include("ra.hrl").

%% API functions
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

-spec start_link() ->
    {ok, pid()} | ignore | {error, term()}.
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    DataDir = ra_env:data_dir(),
    case ra_lib:make_dir(DataDir) of
        ok ->
            %% the ra log ets process is supervised by the system to keep mem tables
            %% alive whilst the rest of the log infra might be down
            Ets = #{id => ra_log_ets,
                    start => {ra_log_ets, start_link, [DataDir]}},
            SupFlags = #{strategy => one_for_all, intensity => 1, period => 5},
            RaLogSup = #{id => ra_log_sup,
                         type => supervisor,
                         start => {ra_log_sup, start_link, [DataDir]}},
            RaServerSupSup = #{id => ra_server_sup_sup,
                               type => supervisor,
                               start => {ra_server_sup_sup, start_link, []}},
            {ok, {SupFlags, [Ets, RaLogSup, RaServerSupSup]}};
        {error, Code} ->
            ?ERR("Failed to create Ra data directory at '~s', file system operation error: ~p~n", [DataDir, Code]),
            exit({error, "Ra could not create its data directory. See the log for details."})
    end.



