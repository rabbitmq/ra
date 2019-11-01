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
    supervisor:start_link({local, ?MODULE}, ?MODULE, [Conf]).

init([WalConf0]) ->
    SupFlags = #{strategy => one_for_one, intensity => 1, period => 5},
    MaxSizeBytes = application:get_env(ra, wal_max_size_bytes,
                                       ?WAL_MAX_SIZE_BYTES),
    ComputeChecksums = application:get_env(ra, wal_compute_checksums, true),
    WalMaxBatchSize = application:get_env(ra, wal_max_batch_size, 32768),
    Strategy = application:get_env(ra, wal_write_strategy, default),
    SyncMethod = application:get_env(ra, wal_sync_method, datasync),
    WalConf = maps:merge(#{compute_checksums => ComputeChecksums,
                           write_strategy => Strategy,
                           max_size_bytes => MaxSizeBytes,
                           sync_method => SyncMethod},
                         WalConf0),
    GenBatchOpts = [{max_batch_size, WalMaxBatchSize}],
    Wal = #{id => ra_log_wal,
            start => {ra_log_wal, start_link, [WalConf, GenBatchOpts]}},
    {ok, {SupFlags, [Wal]}}.
