-module(ra_sup).
-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).


start_link() ->
	supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    _ = ets:new(ra_metrics, [named_table, public, {write_concurrency, true}]),
    Heartbeat = #{id => ra_heartbeat_monitor,
                  start => {ra_heartbeat_monitor, start_link, []},
                  restart => permanent,
                  shutdown => 30000,
                  type => worker,
                  modules => [ra_heartbeat_monitor]},
    SupFlags = #{strategy => one_for_one, intensity => 1, period => 5},
    RaLogFileMetrics = #{id => ra_metrics_ets,
                         start => {ra_metrics_ets, start_link, []}},
    SnapshotWriter = #{id => ra_log_file_snapshot_writer,
                       start => {ra_log_file_snapshot_writer, start_link, []}},
    RaSystemSup = #{id => ra_system_sup,
                    type => supervisor,
                    start => {ra_system_sup, start_link, []}},
    Procs = [RaLogFileMetrics,
             SnapshotWriter,
             RaSystemSup,
             Heartbeat],
	{ok, {SupFlags, Procs}}.
