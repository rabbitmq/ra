%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2017-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%
-module(ra_props_SUITE).
-compile(export_all).

-include("src/ra.hrl").
-include_lib("proper/include/proper.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

% just here to silence unused warning
-export_type([op/0]).

all() ->
    [
     non_assoc
    ].

groups() ->
    [ {tests, [], all()} ].

-type op() :: {add, 1..100} | {subtract, 1..100} | {divide, 2..10} | {mult, 1..10}.

init_per_suite(Config) ->
    {ok, _} = ra:start_in(?config(priv_dir, Config)),
    Config.

init_per_testcase(non_assoc, Config) ->
    {ok, Cluster, _} = ra:start_cluster(default, non_assoc,
                                        {simple, fun non_assoc_apply/2, 0},
                                        [{n1, node()},
                                         {n2, node()},
                                         {n3, node()}]
                                        ),
    [{ra_cluster, Cluster} | Config].

end_per_testcase(_, Config) ->
    terminate_cluster(?config(ra_cluster, Config)),
    Config.

end_per_suite(Config) ->
    application:stop(ra),
    Config.

%% this test mixes associative with non-associative operations to tests that
%% all nodes apply operations in the same order
non_assoc_prop([A, B, C], {Ops, Initial}) ->
    ct:pal("non_assoc_prop Ops: ~p Initial: ~p", [Ops, Initial]),
    Expected = lists:foldl(fun non_assoc_apply/2, Initial, Ops),
    % set cluster to
    {ok, _, Leader} = ra:process_command(A, {set, Initial}),
    [ra:process_command(Leader, Op) || Op <- Ops],
    {ok, _, Leader} = ra:consistent_query(A, fun(_) -> ok end),
    timer:sleep(100),
    {ok, {_, ARes}, _} = ra:local_query(A, fun id/1),
    {ok, {_, BRes}, _} = ra:local_query(B, fun id/1),
    {ok, {_, CRes}, _} = ra:local_query(C, fun id/1),
    % ct:pal("Result ~p ~p ~p Expected ~p Initial ~p",
    %        [ARes, BRes, CRes, Expected, Initial]),
    % assert all nodes have the same final state
    Expected == ARes andalso Expected == BRes andalso Expected == CRes.

non_assoc(Config) ->
    run_proper(
      fun () ->
              Cluster = ?config(ra_cluster, Config),
              ?FORALL(N, {list(op()), integer(1, 1000)},
                      non_assoc_prop(Cluster, N))
      end, [], 25).

id(X) -> X.

non_assoc_apply({add, N}, State) -> State + N;
non_assoc_apply({subtract, N}, State) -> State - N;
non_assoc_apply({divide, N}, State) -> State / N;
non_assoc_apply({mult, N}, State) -> State * N;
non_assoc_apply({set, N}, _) -> N.

terminate_cluster(Nodes) ->
    [gen_statem:stop(P, normal, 2000) || {P, _} <- Nodes].

run_proper(Fun, Args, NumTests) ->
    ?assertEqual(
       true,
       proper:counterexample(erlang:apply(Fun, Args),
			     [{numtests, NumTests},
			      {on_output, fun(".", _) -> ok; % don't print the '.'s on new lines
					     (F, A) -> ct:pal(?LOW_IMPORTANCE, F, A) end}])).
