%% @hidden
-module(ra_system_sup).

-behaviour(supervisor).

-include("ra.hrl").

%% API functions
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

-spec start_link() ->
    {ok, pid()} | ignore | {error, term()}.
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    DataDir = ra_env:data_dir(),
    case ra_lib:make_dir(DataDir) of
        ok ->
            %% the ra log ets process is supervised by the system to keep mem tables
            %% alive whilst the rest of the log infra might be down
            Ets = #{id => ra_log_ets,
                    start => {ra_log_ets, start_link, [DataDir]}},
            SupFlags = #{strategy => one_for_all, intensity => 1, period => 5},
            RaLogSup = #{id => ra_log_sup,
                         type => supervisor,
                         start => {ra_log_sup, start_link, [DataDir]}},
            RaServerSupSup = #{id => ra_server_sup_sup,
                               type => supervisor,
                               start => {ra_server_sup_sup, start_link, []}},
            {ok, {SupFlags, [Ets, RaLogSup, RaServerSupSup]}};
        {error, Code} = Error ->
            ?ERR("Failed to create Ra data directory, file system operation error: ~p~n", [Code]),
            Error
    end.



