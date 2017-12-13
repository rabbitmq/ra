-module(ra_log_wal_sup).

-behaviour(supervisor).

%% API functions
-export([start_link/1]).

%% Supervisor callbacks
-export([init/1]).

-spec start_link(ra_log_wal:wal_conf()) ->
    {ok, pid()} | ignore | {error, term()}.
start_link(Conf) ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, [Conf]).

init([WalConf0]) ->
    SupFlags = #{strategy => one_for_one, intensity => 1, period => 5},
    AddModes = [{delayed_write, 1024 * 1024 * 4, 60 * 1000}],
    WalConf = maps:merge(#{additional_wal_file_modes => AddModes}, WalConf0),
    Wal = #{id => ra_log_wal,
            start => {ra_log_wal, start_link, [WalConf, []]}},
    {ok, {SupFlags, [Wal]}}.
