%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2017-2025 Broadcom. All Rights Reserved. The term Broadcom refers to Broadcom Inc. and/or its subsidiaries.
%%
%% @hidden
-module(ra_log_sync).
-behaviour(gen_batch_server).

-export([start_link/1,
         sync/2]).

-export([init/1,
         handle_batch/2,
         terminate/2,
         format_status/1]).

-type sync_fun() :: fun(() -> ok | {error, term()}).

-record(state, {}).

%%%===================================================================
%%% API
%%%===================================================================

-spec start_link(#{name := atom()}) ->
    {ok, pid()} | {error, term()}.
start_link(#{name := Name} = _Config) ->
    gen_batch_server:start_link({local, Name}, ?MODULE, {},
                                [{reversed_batch, true}]).

-spec sync(gen_batch_server:server_ref(), sync_fun()) ->
    ok | {error, term()}.
sync(Server, SyncFun) when is_function(SyncFun, 0) ->
    gen_batch_server:call(Server, {sync, SyncFun}, infinity).

%%%===================================================================
%%% gen_batch_server callbacks
%%%===================================================================

init({}) ->
    {ok, #state{}}.

handle_batch(Ops, State) ->
    %% Ops arrive in reverse arrival order (most recent first) because
    %% reversed_batch = true. On journaling filesystems (ext4, XFS),
    %% syncing the most recently written file first flushes the journal,
    %% which tends to make subsequent syncs near-instant since the earlier
    %% files are already durable.
    lists:foreach(
      fun({call, From, {sync, SyncFun}}) ->
              Result = try SyncFun()
                       catch _:Err -> {error, Err}
                       end,
              gen:reply(From, Result);
         (_) ->
              ok
      end, Ops),
    {ok, [], State}.

terminate(_Reason, _State) ->
    ok.

format_status(State) ->
    State.
