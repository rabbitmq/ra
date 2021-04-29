%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2017-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%
-module(ra_leaderboard).

-export([
         init/0,
         record/3,
         clear/1,
         lookup_leader/1,
         lookup_members/1,
         overview/0
         ]).

-spec init() -> ok.
init() ->
    _ = ets:new(?MODULE, [set, named_table, public]),
    ok.

-spec record(ra:cluster_name(), ra:server_id(), [ra:server_id()]) -> ok.
record(ClusterName, Leader, Members) ->
    true = ets:insert(?MODULE, {ClusterName, Leader, Members}),
    ok.

-spec clear(ra:cluster_name()) -> ok.
clear(ClusterName) ->
    true = ets:delete(?MODULE, ClusterName),
    ok.

-spec lookup_leader(ra:cluster_name()) -> ra:server_id() | undefined.
lookup_leader(ClusterName) ->
    case lookup(ClusterName) of
        {_, Leader, _} ->
            Leader;
        _ ->
            undefined
    end.

-spec lookup_members(ra:cluster_name()) -> [ra:server_id()] | undefined.
lookup_members(ClusterName) ->
    case lookup(ClusterName) of
        {_, _, Members} ->
            Members;
        _ ->
            undefined
    end.

-spec overview() -> list().
overview() ->
    ets:tab2list(?MODULE).

%% internal

lookup(ClusterName) ->
    try ets:lookup(?MODULE, ClusterName) of
        [Record] ->
            Record;
        [] ->
            undefined
    catch
        error:badarg ->
            undefined
    end.
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

lookup_leader_test() ->
    ClusterName = <<"mah-cluster">>,
    ?assertEqual(undefined, lookup_leader(ClusterName)),
    init(),
    ?assertEqual(undefined, lookup_leader(ClusterName)),
    Me = {me, node()},
    record(ClusterName, Me, [Me]),
    ?assertEqual(Me, lookup_leader(ClusterName)),
    ?assertEqual([Me], lookup_members(ClusterName)),
    You = {you, node()},
    record(ClusterName, You, [Me, You]),
    ?assertEqual(You, lookup_leader(ClusterName)),
    ?assertEqual([Me, You], lookup_members(ClusterName)),

    ok.
-endif.
