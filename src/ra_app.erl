-module(ra_app).
-behaviour(application).

-export([start/2]).
-export([stop/1]).

start(_Type, _Args) ->
    _ = ets:new(ra_metrics, [named_table, public, {write_concurrency, true}]),
	ra_sup:start_link().

stop(_State) ->
	ok.
