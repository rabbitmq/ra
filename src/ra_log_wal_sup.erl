%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2017-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%
%% @hidden
-module(ra_log_wal_sup).

-behaviour(supervisor).

%% API functions
-export([start_link/1]).

%% Supervisor callbacks
-export([init/1]).

-include("ra.hrl").

-spec start_link(ra_log_wal:wal_conf()) ->
    {ok, pid()} | ignore | {error, term()}.
start_link(Conf) ->
    supervisor:start_link(?MODULE, [Conf]).

init([WalConf]) ->
    SupFlags = #{strategy => one_for_one, intensity => 1, period => 5},
    % MaxSizeBytes = application:get_env(ra, wal_max_size_bytes,
    %                                    ?WAL_DEFAULT_MAX_SIZE_BYTES),
    % ComputeChecksums = application:get_env(ra, wal_compute_checksums, true),
    % WalMaxBatchSize = application:get_env(ra, wal_max_batch_size,
    %                                       ?WAL_DEFAULT_MAX_BATCH_SIZE),
    % WalMaxEntries = application:get_env(ra, wal_max_entries, undefined),
    % Strategy = application:get_env(ra, wal_write_strategy, default),
    % SyncMethod = application:get_env(ra, wal_sync_method, datasync),
    % WalConf = maps:merge(#{compute_checksums => ComputeChecksums,
    %                        write_strategy => Strategy,
    %                        max_size_bytes => MaxSizeBytes,
    %                        max_entries => WalMaxEntries,
    %                        sync_method => SyncMethod},
    %                      WalConf0),
    % WalMaxBatchSize = maps:get(max_batch_size, WalConf,
    %                            ?WAL_DEFAULT_MAX_BATCH_SIZE),
    % GenBatchOpts = case maps:get(hibernate_after, WalConf, undefined) of
    %                    undefined ->
    %                        [{max_batch_size, WalMaxBatchSize}];
    %                    Hib ->
    %                        [{hibernate_after, Hib},
    %                         {max_batch_size, WalMaxBatchSize}]
    %                end,

    Wal = #{id => ra_log_wal,
            start => {ra_log_wal, start_link, [WalConf]}},
    {ok, {SupFlags, [Wal]}}.
