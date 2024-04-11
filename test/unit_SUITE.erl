-module(unit_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-export([
         ]).

% -include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

%%%===================================================================
%%% Common Test callbacks
%%%===================================================================

all() ->
    [unit_tests].

mods() ->
    [
     ra_flru,
     ra_lib,
     ra_log,
     ra_log_reader,
     ra_log_segment,
     ra_monitors,
     ra_server,
     ra_snapshot
    ].

groups() ->
    [{M, [],
      [F || {F, 0} <- M:module_info(functions),
            re:run(atom_to_list(F), "_test$") =/= nomatch]}
     || M <- mods()].

init_per_group(_, Config) ->
    Config.

end_per_group(_Group, _Config) ->
    ok.

%%%===================================================================
%%% Test cases
%%%===================================================================

all_tests() ->
    [{M, [F || {F, 0} <- M:module_info(functions),
                  re:run(atom_to_list(F), "_test$") =/= nomatch]}
     || M <- mods()].

unit_tests(_Config) ->
    [begin
         ct:pal("Running ~s ~b tests", [M, length(Tests)]),
         [M:F() || F <- Tests],
         ok
     end || {M, Tests} <- all_tests()],
    ok.
