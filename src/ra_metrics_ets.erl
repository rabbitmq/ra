%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2017-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%
%% @hidden
-module(ra_metrics_ets).
-behaviour(gen_server).

-export([start_link/0]).

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {}).

-include("ra.hrl").

%%% here to own metrics ETS tables

%%%===================================================================
%%% API functions
%%%===================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
    TableFlags =  [named_table,
                   {read_concurrency, true},
                   {write_concurrency, true},
                   public],
    _ = ets:new(ra_log_metrics, [set | TableFlags]),
    _ = ra_counters:init(),
    _ = ra_leaderboard:init(),

    %% Table for ra processes to record their current snapshot index so that
    %% other processes such as the segment writer can use this value to skip
    %% stale records and avoid flushing unnecessary data to disk.
    %% This is written from the ra process so will need write_concurrency.
    %% {RaUId, ra_index()}
    _ = ets:new(ra_log_snapshot_state, [set | TableFlags]),
    {ok, #state{}}.

handle_call(_, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
