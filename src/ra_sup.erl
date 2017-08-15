-module(ra_sup).
-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).


start_link() ->
	supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    _ = ets:new(ra_metrics, [named_table, public, {write_concurrency, true}]),
    SupFlags = #{strategy => one_for_one, intensity => 1, period => 5},
	Procs = [],
	{ok, {SupFlags, Procs}}.
