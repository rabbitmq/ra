%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2017-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%
-module(nemesis).

-behaviour(gen_server).

%% API functions
-export([start_link/1,
         wait_on_scenario/2]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-export([partition/1]).

-include("src/ra.hrl").

-define(SYS, default).

-type scenario() :: [{wait, non_neg_integer()} |
                     {part, [node()], non_neg_integer()} |
                     {app_restart, [ra_server_id()]} |
                     heal].

-type config() :: #{nodes := [node()],
                    scenario := scenario()}.

-record(state, {config :: config(),
                nodes :: [node()],
                steps :: scenario(),
                waiting :: term()}).


%%%===================================================================
%%% API functions
%%%===================================================================

-spec start_link(config()) -> {ok, pid()} | ignore | {error, term()}.
start_link(Config) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [Config], []).

wait_on_scenario(Pid, Timeout) ->
    gen_server:call(Pid, wait_on_scenario, Timeout).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([#{scenario := Steps,
        nodes := Nodes} = Config]) ->
    State = handle_step(#state{config = Config,
                               nodes = Nodes,
                               steps = Steps}),
    {ok, State}.

handle_call(wait_on_scenario, _From, #state{steps = []} = State) ->
    {reply, ok, State};
handle_call(wait_on_scenario, From, State) ->
    {noreply, State#state{waiting = From}}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(next_step, State0) ->
    case handle_step(State0) of
        done ->
            case State0#state.waiting of
                undefined ->
                    {noreply, State0};
                From ->
                    gen_server:reply(From, ok),
                    {noreply, State0}
            end;
        State ->
            {noreply, State}
    end.


terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

partition(Partitions) ->
    partition(Partitions, fun tcp_inet_proxy_helpers:block_traffic_between/2).

%%%===================================================================
%%% Internal functions
%%%===================================================================

handle_step(#state{steps = [{wait, Time} | Rem]} = State) ->
    erlang:send_after(Time, self(), next_step),
    State#state{steps = Rem};
handle_step(#state{steps = [{part, Partition0, Time} | Rem],
                   nodes = Nodes} = State) ->
    %% first we need to always heal
    heal(State#state.nodes),
    Partitions = {lists:usort(Partition0), Nodes -- Partition0},
    _ = erlang:send_after(Time, self(), next_step),
    ok = partition(Partitions),
    % always heal after part
    State#state{steps = [heal | Rem]};
handle_step(#state{steps = [heal | Rem]} = State) ->
    heal(State#state.nodes),
    handle_step(State#state{steps = Rem});
handle_step(#state{steps = [{app_restart, Servers} | Rem]} = State) ->
    ct:pal("doing app restart of ~w", [Servers]),
    [begin
         rpc:call(N, application, stop, [ra]),
         rpc:call(N, ra, start, []),
         rpc:call(N, ra, restart_server, [?SYS, Id])
     end || {_, N} = Id <- Servers],
    handle_step(State#state{steps = Rem});
handle_step(#state{steps = []}) ->
    done.



partition({Partition1, Partition2}, PartitionFun) ->
    lists:foreach(
      fun(Node) ->
              [PartitionFun(Node, OtherNode) || OtherNode <- Partition2]
      end,
      Partition1),
    ok.

heal(Nodes) ->
    ct:pal("Rejoining all nodes"),
    [tcp_inet_proxy_helpers:allow_traffic_between(Node, OtherNode)
     || OtherNode <- Nodes,
        Node <- Nodes,
        OtherNode =/= Node],
    [net_kernel:connect_node(N)
     || N <- Nodes].
