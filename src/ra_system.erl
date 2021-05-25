-module(ra_system).

-include("ra.hrl").
-export([
         start/1,
         start_default/0,
         default_config/0,
         derive_names/1,
         store/1,
         fetch/1,
         fetch/2,
         lookup_name/2
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

-type config() :: #{name := atom(),
                    names := names(),
                    data_dir := file:filename(),
                    wal_data_dir => file:filename(),
                    segment_max_entries => non_neg_integer(),
                    wal_max_size_bytes => non_neg_integer(),
                    wal_compute_checksums => boolean(),
                    wal_max_batch_size => non_neg_integer(),
                    wal_max_entries => non_neg_integer(),
                    wal_write_strategy => default | o_sync,
                    wal_sync_method => datasync | sync,
                    wal_hibernate_after => non_neg_integer(),
                    snapshot_chunk_size => non_neg_integer(),
                    receive_snapshot_timeout => non_neg_integer()
                   }.

-export_type([
              config/0,
              names/0
              ]).

-spec start(ra_system:config()) -> supervisor:startchild_ret().
start(#{name := Name} = Config) ->
    ?INFO("ra: starting system ~s", [Name]),
    ?DEBUG("ra: starting system ~s with config: ~p", [Name, Config]),
    ra_systems_sup:start_system(Config).

-spec start_default() -> supervisor:startchild_ret().
start_default() ->
    start(ra_system:default_config()).

-spec default_config() -> ra_system:config().
default_config() ->
    SegmentMaxEntries = application:get_env(ra, segment_max_entries, 4096),
    WalMaxSizeBytes = application:get_env(ra, wal_max_size_bytes,
                                          ?WAL_DEFAULT_MAX_SIZE_BYTES),
    WalComputeChecksums = application:get_env(ra, wal_compute_checksums, true),
    WalMaxBatchSize = application:get_env(ra, wal_max_batch_size,
                                          ?WAL_DEFAULT_MAX_BATCH_SIZE),
    WalMaxEntries = application:get_env(ra, wal_max_entries, undefined),
    WalWriteStrategy = application:get_env(ra, wal_write_strategy, default),
    WalSyncMethod = application:get_env(ra, wal_sync_method, datasync),
    #{name => default,
      data_dir => ra_env:data_dir(),
      wal_data_dir => ra_env:data_dir(),
      wal_max_size_bytes => WalMaxSizeBytes,
      wal_compute_checksums => WalComputeChecksums,
      wal_max_batch_size => WalMaxBatchSize,
      wal_max_entries => WalMaxEntries,
      wal_write_strategy => WalWriteStrategy,
      wal_sync_method => WalSyncMethod,
      segment_max_entries => SegmentMaxEntries,
      names =>
      #{wal => ra_log_wal,
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

