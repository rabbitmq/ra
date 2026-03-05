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
         pool_size/0,
         worker_name/2,
         sync/2]).

-export([init/1,
         handle_batch/2,
         terminate/2,
         format_status/1]).

-type sync_fun() :: fun(() -> ok | {error, term()}).
-type pool_ref() :: {pool, BaseName :: atom(), PoolSize :: pos_integer()}.

-export_type([pool_ref/0]).

-record(state, {}).

%%%===================================================================
%%% API
%%%===================================================================

-spec pool_size() -> pos_integer().
pool_size() ->
    max(1, erlang:system_info(schedulers) div 4).

-spec worker_name(atom(), non_neg_integer()) -> atom().
worker_name(BaseName, Idx) ->
    list_to_atom(atom_to_list(BaseName) ++ "_" ++ integer_to_list(Idx)).

-spec start_link(#{name := atom()}) ->
    {ok, pid()} | {error, term()}.
start_link(#{name := Name} = _Config) ->
    gen_batch_server:start_link({local, Name}, ?MODULE, {},
                                [{reversed_batch, true}]).

-spec sync(pool_ref(), sync_fun()) ->
    ok | {error, term()}.
sync({pool, BaseName, PoolSize}, SyncFun) when is_function(SyncFun, 0) ->
    Idx = rand:uniform(PoolSize) - 1,
    Worker = worker_name(BaseName, Idx),
    gen_batch_server:call(Worker, {sync, SyncFun}, infinity).

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
