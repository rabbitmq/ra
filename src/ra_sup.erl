-module(ra_sup).
-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

-define(TABLES, [ra_metrics, ra_state, ra_open_file_metrics, ra_io_metrics]).

start_link() ->
	supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    [ets:new(Table, [named_table, public, {write_concurrency, true}])
     || Table <- ?TABLES],

    SupFlags = #{strategy => one_for_one, intensity => 1, period => 5},
    RaLogFileMetrics = #{id => ra_metrics_ets,
                         start => {ra_metrics_ets, start_link, []}},
    SnapshotWriter = #{id => ra_log_file_snapshot_writer,
                       start => {ra_log_file_snapshot_writer, start_link, []}},
    RaFileHandle = #{id => ra_file_handle,
                     start => {ra_file_handle, start_link, []}},
    RaSystemSup = #{id => ra_system_sup,
                    type => supervisor,
                    start => {ra_system_sup, start_link, []}},
    Procs = [RaLogFileMetrics,
             RaFileHandle,
             SnapshotWriter,
             RaSystemSup],
	{ok, {SupFlags, Procs}}.
