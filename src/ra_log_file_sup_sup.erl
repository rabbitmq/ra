-module(ra_log_file_sup_sup).

-behaviour(supervisor).

%% API functions
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

-spec start_link() ->
    {ok, pid()} | ignore | {error, term()}.
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    Ets = #{id => ra_log_file_ets,
            start => {ra_log_file_ets, start_link, []}},
    SupFlags = #{strategy => one_for_all, intensity => 1, period => 5},
    RaLogFileSup = #{id => ra_log_file_sup,
                     type => supervisor,
                     start => {ra_log_file_sup, start_link, []}},
    {ok, {SupFlags, [Ets, RaLogFileSup]}}.



