-module(ra_leaderboard_SUITE).

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
    [lookup_leader].

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
    [{tests, all()}].

init_per_group(_, Config) ->
    Config.

end_per_group(_Group, _Config) ->
    ok.

%%%===================================================================
%%% Test cases
%%%===================================================================

lookup_leader(_Config) ->
    ClusterName = <<"mah-cluster">>,
    ?assertEqual(undefined, ra_leaderboard:lookup_leader(ClusterName)),
    ra_leaderboard:init(),
    ?assertEqual(undefined, ra_leaderboard:lookup_leader(ClusterName)),
    Me = {me, node()},
    ra_leaderboard:record(ClusterName, Me, [Me]),
    ?assertEqual(Me, ra_leaderboard:lookup_leader(ClusterName)),
    ?assertEqual([Me], ra_leaderboard:lookup_members(ClusterName)),
    You = {you, node()},
    ra_leaderboard:record(ClusterName, You, [Me, You]),
    ?assertEqual(You, ra_leaderboard:lookup_leader(ClusterName)),
    ?assertEqual([Me, You],ra_leaderboard:lookup_members(ClusterName)),
    ets:delete(ra_leaderboard),
    ok.
