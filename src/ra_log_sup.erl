%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2017-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%
%% @hidden
-module(ra_log_sup).
-behaviour(supervisor).

-include("ra.hrl").

%% API functions
-export([start_link/1]).

%% Supervisor callbacks
-export([init/1]).

-spec start_link(ra_system:config()) ->
    {ok, pid()} | ignore | {error, term()}.
start_link(#{names := #{log_sup := Name}} = Cfg) ->
    supervisor:start_link({local, Name}, ?MODULE, [Cfg]).

init([#{data_dir := DataDir,
        name := System,
        names := #{wal := _WalName,
                   segment_writer := SegWriterName} = _Names} = Cfg]) ->
    %% TODO: make unnamed
    PreInit = #{id => ra_log_pre_init,
                start => {ra_log_pre_init, start_link, [System]}},
    Meta = #{id => ra_log_meta,
             start => {ra_log_meta, start_link, [Cfg]}},
    SegmentMaxEntries = maps:get(segment_max_entries, Cfg, 4096),
    SegWriterConf = #{name => SegWriterName,
                      system => System,
                      data_dir => DataDir,
                      segment_conf => #{max_count => SegmentMaxEntries}},
    SegWriter = #{id => ra_log_segment_writer,
                  start => {ra_log_segment_writer, start_link,
                            [SegWriterConf]}},
    WalConf = make_wal_conf(Cfg),
    SupFlags = #{strategy => one_for_all, intensity => 5, period => 5},
    WalSup = #{id => ra_log_wal_sup,
               type => supervisor,
               start => {ra_log_wal_sup, start_link, [WalConf]}},
    {ok, {SupFlags, [PreInit, Meta, SegWriter, WalSup]}}.


make_wal_conf(#{data_dir := DataDir,
                name := _System,
                names := #{wal := WalName,
                           segment_writer := SegWriterName} = Names} = Cfg) ->
    WalDir = case Cfg of
                 #{wal_data_dir := D} -> D;
                 _ -> DataDir
             end,
    MaxSizeBytes = maps:get(wal_max_size_bytes, Cfg,
                            ?WAL_DEFAULT_MAX_SIZE_BYTES),
    ComputeChecksums = maps:get(wal_compute_checksums, Cfg, true),
    MaxBatchSize = maps:get(wal_max_batch_size, Cfg,
                            ?WAL_DEFAULT_MAX_BATCH_SIZE),
    MaxEntries = maps:get(wal_max_entries, Cfg, undefined),
    Strategy = maps:get(wal_write_strategy, Cfg, default),
    SyncMethod = maps:get(wal_sync_method, Cfg, datasync),
    HibAfter = maps:get(wal_hibernate_after, Cfg, undefined),
    #{name => WalName,
      names => Names,
      dir => WalDir,
      segment_writer => SegWriterName,
      compute_checksums => ComputeChecksums,
      write_strategy => Strategy,
      max_size_bytes => MaxSizeBytes,
      max_entries => MaxEntries,
      sync_method => SyncMethod,
      max_batch_size => MaxBatchSize,
      hibernate_after => HibAfter
     }.
