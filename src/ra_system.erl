-module(ra_system).

-include("ra.hrl").
-include("ra_server.hrl").
-export([
         start/1,
         start_default/0,
         default_config/0,
         derive_names/1,
         store/1,
         fetch/1,
         fetch/2,
         lookup_name/2,
         stop/1,
         stop_default/0
         ]).

-type names() :: #{wal := atom(),
                   wal_sup := atom(),
                   log_sup := atom(),
                   log_ets := atom(),
                   log_meta := atom(),
                   open_mem_tbls :=  atom(),
                   closed_mem_tbls := atom(),
                   segment_writer := atom(),
                   server_sup := atom(),
                   directory := atom(),
                   directory_rev := atom()}.


%% NB: keep this below 32 keys to ensure it is always a small map
-type config() :: #{name := atom(),
                    names := names(),
                    data_dir := file:filename(),
                    wal_data_dir => file:filename(),
                    wal_max_size_bytes => non_neg_integer(),
                    wal_compute_checksums => boolean(),
                    wal_max_batch_size => non_neg_integer(),
                    wal_max_entries => undefined | non_neg_integer(),
                    wal_write_strategy => default | o_sync | sync_after_notify,
                    wal_sync_method => datasync | sync | none,
                    wal_hibernate_after => non_neg_integer(),
                    wal_garbage_collect => boolean(),
                    wal_pre_allocate => boolean(),
                    segment_max_entries => non_neg_integer(),
                    segment_max_pending => non_neg_integer(),
                    segment_compute_checksums => boolean(),
                    snapshot_chunk_size => non_neg_integer(),
                    receive_snapshot_timeout => non_neg_integer(),
                    default_max_pipeline_count => non_neg_integer(),
                    default_max_append_entries_rpc_batch_size => non_neg_integer(),
                    message_queue_data => on_heap | off_heap,
                    wal_min_heap_size => non_neg_integer(),
                    wal_min_bin_vheap_size => non_neg_integer(),
                    server_min_bin_vheap_size => non_neg_integer(),
                    server_min_heap_size => non_neg_integer(),
                    compress_mem_tables => boolean(),
                    low_priority_commands_flush_size => non_neg_integer(),
                    low_priority_commands_in_memory_size => non_neg_integer(),
                    server_recovery_strategy => undefined |
                                               registered |
                                               {module(), atom(), list()}
                   }.

-export_type([
              config/0,
              names/0
              ]).

-spec start(ra_system:config()) -> supervisor:startchild_ret().
start(#{name := Name} = Config) ->
    ?INFO("ra: starting system ~ts", [Name]),
    ?DEBUG("ra: starting system ~ts with config: ~tp", [Name, Config]),
    ra_systems_sup:start_system(Config).

-spec start_default() -> supervisor:startchild_ret().
start_default() ->
    start(ra_system:default_config()).

-spec default_config() -> ra_system:config().
default_config() ->
    SegmentMaxEntries = application:get_env(ra, segment_max_entries,
                                            ?SEGMENT_MAX_ENTRIES),
    SegmentMaxPending = application:get_env(ra, segment_max_pending,
                                            ?SEGMENT_MAX_PENDING),
    SegmentComputeChecksums = application:get_env(ra, segment_compute_checksums,
                                                  true),
    WalMaxSizeBytes = application:get_env(ra, wal_max_size_bytes,
                                          ?WAL_DEFAULT_MAX_SIZE_BYTES),
    WalComputeChecksums = application:get_env(ra, wal_compute_checksums, true),
    WalMaxBatchSize = application:get_env(ra, wal_max_batch_size,
                                          ?WAL_DEFAULT_MAX_BATCH_SIZE),
    WalMaxEntries = application:get_env(ra, wal_max_entries, undefined),
    WalWriteStrategy = application:get_env(ra, wal_write_strategy, default),
    WalSyncMethod = application:get_env(ra, wal_sync_method, datasync),
    DataDir = ra_env:data_dir(),
    WalDataDir = application:get_env(ra, wal_data_dir, DataDir),
    WalGarbageCollect = application:get_env(ra, wal_garbage_collect, false),
    WalPreAllocate = application:get_env(ra, wal_pre_allocate, false),
    WalMinBinVheapSize = application:get_env(ra, wal_min_bin_vheap_size,
                                             ?MIN_BIN_VHEAP_SIZE),
    WalMinHeapSize = application:get_env(ra, wal_min_heap_size, ?MIN_HEAP_SIZE),
    ServerMinHeapSize = application:get_env(ra, server_min_heap_size, ?MIN_HEAP_SIZE),
    ServerMinBinVheapSize = application:get_env(ra, server_min_bin_vheap_size,
                                                ?MIN_BIN_VHEAP_SIZE),
    DefaultMaxPipelineCount = application:get_env(ra, default_max_pipeline_count,
                                                  ?DEFAULT_MAX_PIPELINE_COUNT),
    DefaultAERBatchSize = application:get_env(ra, default_max_append_entries_rpc_batch_size,
                                              ?AER_CHUNK_SIZE),
    MessageQueueData = application:get_env(ra, server_message_queue_data, off_heap),
    CompressMemTables = application:get_env(ra, compress_mem_tables, false),
    SnapshotChunkSize = application:get_env(ra, snapshot_chunk_size,
                                            ?DEFAULT_SNAPSHOT_CHUNK_SIZE),
    ReceiveSnapshotTimeout = application:get_env(ra, receive_snapshot_timeout,
                                                 ?DEFAULT_RECEIVE_SNAPSHOT_TIMEOUT),
    LowPriorityCommandsFlushSize = application:get_env(ra, low_priority_commands_flush_size ,
                                                       ?FLUSH_COMMANDS_SIZE),
    LowPriorityInMemSize = application:get_env(ra, low_priority_commands_in_memory_size,
                                               ?FLUSH_COMMANDS_SIZE),
    #{name => default,
      data_dir => DataDir,
      wal_data_dir => WalDataDir,
      wal_max_size_bytes => WalMaxSizeBytes,
      wal_compute_checksums => WalComputeChecksums,
      wal_max_batch_size => WalMaxBatchSize,
      wal_max_entries => WalMaxEntries,
      wal_write_strategy => WalWriteStrategy,
      wal_garbage_collect => WalGarbageCollect,
      wal_pre_allocate => WalPreAllocate,
      wal_sync_method => WalSyncMethod,
      wal_min_heap_size => WalMinHeapSize,
      wal_min_bin_vheap_size => WalMinBinVheapSize,
      segment_max_entries => SegmentMaxEntries,
      segment_max_pending => SegmentMaxPending,
      segment_compute_checksums => SegmentComputeChecksums,
      default_max_pipeline_count => DefaultMaxPipelineCount,
      default_max_append_entries_rpc_batch_size => DefaultAERBatchSize,
      message_queue_data => MessageQueueData,
      compress_mem_tables => CompressMemTables,
      snapshot_chunk_size => SnapshotChunkSize,
      server_min_bin_vheap_size => ServerMinBinVheapSize,
      server_min_heap_size => ServerMinHeapSize,
      receive_snapshot_timeout => ReceiveSnapshotTimeout,
      low_priority_commands_flush_size => LowPriorityCommandsFlushSize,
      low_priority_commands_in_memory_size => LowPriorityInMemSize,
      names => #{wal => ra_log_wal,
                 wal_sup => ra_log_wal_sup,
                 log_sup => ra_log_sup,
                 log_ets => ra_log_ets,
                 log_meta => ra_log_meta,
                 open_mem_tbls =>  ra_log_open_mem_tables,
                 closed_mem_tbls =>  ra_log_closed_mem_tables,
                 segment_writer => ra_log_segment_writer,
                 server_sup => ra_server_sup_sup,
                 directory => ra_directory,
                 directory_rev => ra_directory_reverse
                }}.

derive_names(SysName) when is_atom(SysName) ->
    #{wal => derive(SysName, <<"log_wal">>),
      wal_sup => derive(SysName, <<"log_wal_sup">>),
      log_sup => derive(SysName, <<"log_sup">>),
      log_ets => derive(SysName, <<"log_ets">>),
      log_meta => derive(SysName, <<"log_meta">>),
      open_mem_tbls => derive(SysName, <<"log_open_mem_tables">>),
      closed_mem_tbls => derive(SysName, <<"log_closed_mem_tables">>),
      segment_writer => derive(SysName, <<"segment_writer">>),
      server_sup => derive(SysName, <<"server_sup_sup">>),
      directory => derive(SysName, <<"directory">>),
      directory_rev => derive(SysName, <<"directory_reverse">>)
     }.

-spec store(config()) -> ok.
store(#{name := Name} = Config) ->
    persistent_term:put({'$ra_system', Name}, Config).

-spec fetch(atom()) -> config() | undefined.
fetch(Name) when is_atom(Name) ->
    persistent_term:get({'$ra_system', Name}, undefined).

-spec fetch(atom(), Node :: atom()) -> config() | undefined | {badrpc, term()}.
fetch(Name, Node) when is_atom(Name) andalso is_atom(Node) ->
    rpc:call(Node, persistent_term, get, [{'$ra_system', Name}, undefined]).

-spec lookup_name(atom(), atom()) ->
    {ok, atom()} | {error, system_not_started}.
lookup_name(System, Key) when is_atom(System) ->
    case fetch(System) of
        undefined ->
            {error, system_not_started};
        #{names := #{Key := Name}} ->
            {ok, Name}
    end.

derive(N, Suff) ->
    S = atom_to_binary(N, utf8),
    binary_to_atom(<<"ra_", S/binary, "_", Suff/binary>>, utf8).

-spec stop(ra_system:config() | atom()) -> ok | {error, any()}.
stop(System) ->
    ra_systems_sup:stop_system(System).

-spec stop_default() -> ok | {error, any()}.
stop_default() ->
    #{name := Name} = default_config(),
    stop(Name).
