%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2017-2025 Broadcom. All Rights Reserved. The term Broadcom refers to Broadcom Inc. and/or its subsidiaries.
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
                   log_sync := LogSyncName,
                   segment_writer := SegWriterName}} = Cfg]) ->
    PreInit = #{id => ra_log_pre_init,
                start => {ra_log_pre_init, start_link, [System]}},
    Meta = #{id => ra_log_meta,
             start => {ra_log_meta, start_link, [Cfg]}},
    PoolSize = ra_log_sync:pool_size(),
    LogSyncWorkers = [#{id => {ra_log_sync, I},
                        start => {ra_log_sync, start_link,
                                  [#{name => ra_log_sync:worker_name(LogSyncName, I)}]},
                        shutdown => 5_000}
                      || I <- lists:seq(0, PoolSize - 1)],
    SegmentMaxEntries = maps:get(segment_max_entries, Cfg, ?SEGMENT_MAX_ENTRIES),
    SegmentMaxPending = maps:get(segment_max_pending, Cfg, ?SEGMENT_MAX_PENDING),
    SegmentMaxBytes = maps:get(segment_max_size_bytes, Cfg, ?SEGMENT_MAX_SIZE_B),
    SegmentComputeChecksums = maps:get(segment_compute_checksums, Cfg, true),
    SegWriterConf = #{name => SegWriterName,
                      system => System,
                      data_dir => DataDir,
                      segment_conf =>
                          #{max_count => SegmentMaxEntries,
                            max_pending => SegmentMaxPending,
                            max_size => SegmentMaxBytes,
                            compute_checksums => SegmentComputeChecksums}},
    SegWriter = #{id => ra_log_segment_writer,
                  start => {ra_log_segment_writer, start_link,
                            [SegWriterConf]},
                  shutdown => 30_000},
    WalConf = make_wal_conf(Cfg),
    ok = maybe_migrate_snapshot_store(Cfg),
    SnapStore = snap_store_children(Cfg),
    SupFlags = #{strategy => one_for_all,
                 intensity => 5,
                 period => 5},
    WalSup = #{id => ra_log_wal_sup,
               type => supervisor,
               start => {ra_log_wal_sup, start_link, [WalConf]}},
    %% the snapshot store comes first as everything that initialises a
    %% member's snapshot state (PreInit included) may read from it
    {ok, {SupFlags, SnapStore ++ [PreInit, Meta] ++ LogSyncWorkers ++
              [SegWriter, WalSup]}}.

%% When the snapshot store is not configured but files from an earlier run
%% with it are there, the snapshots in them are moved back to directories
%% before anything reads a member's snapshots, otherwise members would start
%% without the snapshots their (truncated) logs depend on.
maybe_migrate_snapshot_store(#{snapshot_store := _}) ->
    ok;
maybe_migrate_snapshot_store(#{data_dir := DataDir}) ->
    Dir = filename:join(DataDir, "snapshot_store"),
    case ra_log_snap_store:has_files(Dir) of
        true ->
            ?INFO("ra_log_sup: the snapshot store is not configured but ~ts "
                  "has snapshot files, moving them to snapshot directories",
                  [Dir]),
            LiveFun = fun (UId, _Epoch) ->
                              ra_lib:is_dir(filename:join(DataDir,
                                                          ra_lib:to_list(UId)))
                      end,
            case ra_log_snap_store:migrate_out(#{dir => Dir,
                                                 data_dir => DataDir,
                                                 live_fun => LiveFun}) of
                ok ->
                    ok;
                {error, Reason} ->
                    %% carrying on would start members without the snapshots
                    %% their logs depend on
                    ?ERROR("ra_log_sup: could not move snapshots out of the "
                           "snapshot store: ~p", [Reason]),
                    exit({snapshot_store_migration_failed, Reason})
            end;
        false ->
            ok
    end.

snap_store_children(#{snapshot_store := StoreCfg,
                      data_dir := DataDir,
                      name := System,
                      names := Names}) ->
    Name = maps:get(snap_store, Names,
                    maps:get(snap_store, ra_system:derive_names(System))),
    MaxSize = maps:get(max_size, StoreCfg, ?SNAPSHOT_STORE_MAX_SIZE),
    MinFileBytes = maps:get(min_file_bytes, StoreCfg,
                            ?SNAPSHOT_STORE_MIN_FILE_BYTES),
    %% a snapshot is dead when its member's directory is gone
    LiveFun = fun (UId, _Epoch) ->
                      ra_lib:is_dir(filename:join(DataDir, ra_lib:to_list(UId)))
              end,
    Conf = #{name => Name,
             dir => filename:join(DataDir, "snapshot_store"),
             min_file_bytes => MinFileBytes,
             live_fun => LiveFun,
             registry => {ra_log_snap_store:registry_key(DataDir),
                          #{name => Name, max_size => MaxSize}}},
    [#{id => ra_log_snap_store,
       start => {ra_log_snap_store, start_link, [Conf]},
       shutdown => 30_000}];
snap_store_children(_) ->
    [].


make_wal_conf(#{data_dir := DataDir,
                name := System,
                names := #{} = Names} = Cfg) ->
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
    SyncMethod = maps:get(wal_sync_method, Cfg, datasync),
    HibAfter = maps:get(wal_hibernate_after, Cfg, infinity),
    Gc = maps:get(wal_garbage_collect, Cfg, false),
    PreAlloc = maps:get(wal_pre_allocate, Cfg, false),
    MinBinVheapSize = maps:get(wal_min_bin_vheap_size, Cfg,
                               ?MIN_BIN_VHEAP_SIZE),
    MinHeapSize = maps:get(wal_min_heap_size, Cfg, ?MIN_HEAP_SIZE),
    #{names => Names,
      system => System,
      dir => WalDir,
      compute_checksums => ComputeChecksums,
      max_size_bytes => MaxSizeBytes,
      max_entries => MaxEntries,
      sync_method => SyncMethod,
      max_batch_size => MaxBatchSize,
      hibernate_after => HibAfter,
      garbage_collect => Gc,
      pre_allocate => PreAlloc,
      min_heap_size => MinHeapSize,
      min_bin_vheap_size => MinBinVheapSize
     }.
