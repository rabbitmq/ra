%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2017-2025 Broadcom. All Rights Reserved. The term Broadcom refers to Broadcom Inc. and/or its subsidiaries.

%% @hidden
%%
%% Owns the {'$ra_system', Name} persistent_term registration lifecycle for a
%% single Ra system.
-module(ra_system_registration).

-behaviour(gen_server).

-export([start_link/1]).

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-spec start_link(ra_system:config()) ->
    {ok, pid()} | ignore | {error, term()}.
start_link(#{name := _Name} = Config) ->
    gen_server:start_link(?MODULE, Config, []).

init(#{name := _Name} = Config) ->
    process_flag(trap_exit, true),
    ok = ra_system:store(Config),
    {ok, Config}.

handle_call(_Request, _From, Config) ->
    {reply, ok, Config}.

handle_cast(_Msg, Config) ->
    {noreply, Config}.

handle_info(_Msg, Config) ->
    {noreply, Config}.

terminate(_Reason, #{name := Name}) ->
    _ = persistent_term:erase({'$ra_system', Name}),
    ok.

code_change(_OldVsn, Config, _Extra) ->
    {ok, Config}.
