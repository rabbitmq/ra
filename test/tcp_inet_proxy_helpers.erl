%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2017-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%
-module(tcp_inet_proxy_helpers).

-export([configure_dist_proxy/1,
         block_traffic_between/2,
         allow_traffic_between/2]).
-export([wait_for_blocked/3]).
configure_dist_proxy(Config) ->
    [{erlang_dist_module, inet_tcp_proxy_dist} | Config].

block_traffic_between(NodeA, NodeB) ->
    ok = retry_rpc(20, fun () -> rpc:call(NodeA, inet_tcp_proxy_dist, block, [NodeB]) end),
    ok = retry_rpc(20, fun () -> rpc:call(NodeB, inet_tcp_proxy_dist, block, [NodeA]) end),
    wait_for_blocked(NodeA, NodeB, 10).

allow_traffic_between(NodeA, NodeB) ->
    ok = retry_rpc(20, fun () -> rpc:call(NodeA, inet_tcp_proxy_dist, allow, [NodeB]) end),
    ok = retry_rpc(20, fun () -> rpc:call(NodeB, inet_tcp_proxy_dist, allow, [NodeA]) end),
    wait_for_unblocked(NodeA, NodeB, 10).

retry_rpc(1, Fun) ->
    Fun();
retry_rpc(N, Fun) ->
    try Fun() of
        {badrpc, _} ->
            timer:sleep(1000),
            retry_rpc(N-1, Fun);
        Result -> Result
    catch _:_ ->
            timer:sleep(1000),
            retry_rpc(N-1, Fun)
    end.

wait_for_blocked(NodeA, NodeB, 0) ->
    error({failed_to_block, NodeA, NodeB, no_more_attempts});
wait_for_blocked(NodeA, NodeB, Attempts) ->
    case rpc:call(NodeA, rpc, call, [NodeB, erlang, node, []], 1000) of
        {badrpc, _} -> ok;
        _           ->
            timer:sleep(50),
            wait_for_blocked(NodeA, NodeB, Attempts - 1)
    end.

wait_for_unblocked(NodeA, NodeB, 0) ->
    error({failed_to_unblock, NodeA, NodeB, no_more_attempts});
wait_for_unblocked(NodeA, NodeB, Attempts) ->
    case rpc:call(NodeA, rpc, call, [NodeB, erlang, node, []], 1000) of
        NodeB       -> ok;
        {badrpc, _} ->
            timer:sleep(50),
            wait_for_unblocked(NodeA, NodeB, Attempts - 1)
    end.
