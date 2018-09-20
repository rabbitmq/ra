-module(ra_system_sup).

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
    Ets = #{id => ra_log_ets,
            start => {ra_log_ets, start_link, []}},
    SupFlags = #{strategy => one_for_all, intensity => 1, period => 5},
    RaLogFileSup = #{id => ra_log_sup,
                     type => supervisor,
                     start => {ra_log_sup, start_link, []}},
    RaNodesSup = #{id => ra_server_sup,
                   type => supervisor,
                   start => {ra_server_sup, start_link, []}},
    {ok, {SupFlags, [Ets, RaLogFileSup, RaNodesSup]}}.



