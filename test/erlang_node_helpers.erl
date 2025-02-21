%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term Broadcom refers to Broadcom Inc. and/or its subsidiaries.
%%
-module(erlang_node_helpers).

-export([start_erlang_nodes/2, start_erlang_node/2, stop_erlang_nodes/1, stop_erlang_node/1]).
-include_lib("common_test/include/ct.hrl").

start_erlang_nodes(Nodes, Config) ->
    [start_erlang_node(Node, Config) || Node <- Nodes].

start_erlang_node(Node, Config) ->
    DistMod = ?config(erlang_dist_module, Config),
    StartArgs = case DistMod of
                    undefined ->
                        [];
                    _ ->
                        DistModS = atom_to_list(DistMod),
                        DistModPath = filename:absname(
                                        filename:dirname(
                                          code:where_is_file(DistModS ++ ".beam"))),
                        DistArg = re:replace(DistModS, "_dist$", "",
                                             [{return, list}]),
                        ["-pa", DistModPath, "-proto_dist", DistArg,
                         "-kernel", "prevent_overlapping_partitions", "false"]
                end,
    ct:log("Starting node ~p, with ~p", [Node, StartArgs]),
    {ok, Peer, _} = peer:start(#{name => Node, args => StartArgs, connection => standard_io}),
    wait_for_distribution(Node, 50),
    add_lib_dir(Node),
    Peer.

add_lib_dir(Node) ->
    ct_rpc:call(Node, code, add_paths, [code:get_path()]).

wait_for_distribution(Node, 0) ->
    error({distribution_failed_for, Node, no_more_attempts});
wait_for_distribution(Node, Attempts) ->
    ct:pal("Waiting for node ~p", [Node]),
    case ct_rpc:call(Node, net_kernel, set_net_ticktime, [15]) of
        {badrpc, nodedown} ->
            timer:sleep(100),
            wait_for_distribution(Node, Attempts - 1);
        _ -> ok
    end.

stop_erlang_nodes(Nodes) ->
    [stop_erlang_node(Node) || Node <- Nodes].

stop_erlang_node(Peer) ->
    peer:stop(Peer).
