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
    {ok, DataDir} = application:get_env(data_dir),
    Modes = [{delayed_write, 1024 * 1024 * 4, 60 * 1000}],
    WalConf = #{dir => DataDir,
                additional_wal_file_modes => Modes},
    Wal = #{id => ra_log_wal,
            start => {ra_log_wal, start_link, [WalConf, []]}},
    SegWriterConf = #{data_dir => DataDir},
    SegWriter = #{id => ra_log_file_segment_writer,
                  start => {ra_log_file_segment_writer, start_link, [SegWriterConf]}},
    Procs = [Wal, SegWriter, Heartbeat],
	{ok, {SupFlags, Procs}}.
