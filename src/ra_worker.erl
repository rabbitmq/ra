%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term Broadcom refers to Broadcom Inc. and/or its subsidiaries.
%%
%% @hidden
-module(ra_worker).
-behaviour(gen_server).

-export([start_link/1,
         queue_work/3]).

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {}).

%%% ra worker responsible for doing background work for a ra server.
%%%
%%% this could include, writing snapshots or checkpoints or log
%%% compaction

%%%===================================================================
%%% API functions
%%%===================================================================

start_link(Config) ->
    gen_server:start_link(?MODULE, Config, []).

queue_work(Pid, FunOrMfa, ErrFun) when is_pid(Pid) ->
    gen_server:cast(Pid, {work, FunOrMfa, ErrFun}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init(Config) when is_map(Config) ->
    process_flag(trap_exit, true),
    {ok, #state{}}.

handle_call(_, _From, State) ->
    {reply, ok, State}.

handle_cast({work, FunOrMfa, ErrFun}, State) ->
    case FunOrMfa of
        {M, F, Args} ->
            try erlang:apply(M, F, Args) of
                _ ->
                    ok
            catch Type:Err:_Stack ->
                      %% TODO: log
                      ErrFun({Type, Err}),
                      ok
            end;
        _ when is_function(FunOrMfa) ->
            try FunOrMfa() of
                _ ->
                    ok
            catch Type:Err:_Stack ->
                      %% TODO: log
                      ErrFun({Type, Err})
            end,
            ok
    end,
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
