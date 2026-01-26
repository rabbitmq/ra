%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2017-2025 Broadcom. All Rights Reserved. The term Broadcom refers to Broadcom Inc. and/or its subsidiaries.
%%
%% @hidden
-module(ra_log_wal_sup).

-behaviour(supervisor).

%% API functions
-export([start_link/1]).

%% Supervisor callbacks
-export([init/1]).

-spec start_link(ra_log_wal:wal_conf()) ->
    {ok, pid()} | ignore | {error, term()}.
start_link(Conf) ->
    supervisor:start_link(?MODULE, [Conf]).

init([WalConf]) ->
    SupFlags = #{strategy => one_for_one, intensity => 2, period => 5},
    Wal = #{id => ra_log_wal,
            start => {ra_log_wal, start_link, [WalConf]}},
    {ok, {SupFlags, [Wal]}}.
