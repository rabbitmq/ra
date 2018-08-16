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
    Strategy = application:get_env(ra, wal_write_strategy, default),
    WalConf = maps:merge(#{compute_checksums => ComputeChecksums,
                           write_strategy => Strategy,
                           max_size_bytes => MaxSizeBytes},
                         WalConf0),
    Wal = #{id => ra_log_wal,
            start => {ra_log_wal, start_link, [WalConf, []]}},
    {ok, {SupFlags, [Wal]}}.
