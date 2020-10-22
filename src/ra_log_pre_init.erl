%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2017-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%
-module(ra_log_pre_init).

-behaviour(gen_server).

-include("ra.hrl").
%% API functions
-export([start_link/1]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {}).

%%%===================================================================
%%% API functions
%%%===================================================================

start_link(System) when is_atom(System) ->
    gen_server:start_link(?MODULE, [System], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([System]) ->
    %% ra_log:pre_init ensures that the ra_log_snapshot_state table is
    %% populated before WAL recovery begins to avoid writing unnecessary
    %% indexes to segment files.
    Regd = ra_directory:list_registered(System),
    _ = [catch(pre_init(System, Name)) || {Name, _U} <- Regd],
    {ok, #state{} , hibernate}.

handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

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

pre_init(System, Name) ->
    {ok, #{log_init_args := Log}} = ra_server_sup_sup:recover_config(System, Name),
    _ = ra_log:pre_init(Log),
    ok.

