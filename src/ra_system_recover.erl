%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term Broadcom refers to Broadcom Inc. and/or its subsidiaries.
-module(ra_system_recover).

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
    Conf = ra_system:fetch(System),
    case Conf of
        #{server_recovery_strategy := registered = Strat} ->
            Regd = ra_directory:list_registered(System),
            ?INFO("~s: ra system '~ts' server recovery strategy ~w,
                   num servers ~b",
                  [?MODULE, System, Strat, length(Regd)]),
            [begin
                 case ra:restart_server(System, {N, node()}) of
                     ok ->
                         ok;
                     Err ->
                         ?WARN("~s: ra:restart_server/2 failed with ~p",
                               [?MODULE, Err]),
                         ok
                 end
             end || {N, _Uid} <- Regd],
            ok;
        #{server_recovery_strategy := {Mod, Fun, Args}} ->
            ?INFO("~s: ra system '~ts' server recovery strategy ~s:~s",
                  [?MODULE, System, Mod, Fun]),
            try apply(Mod, Fun, [System | Args]) of
                ok ->
                    ok
            catch C:E:S ->
                      ?ERROR("~s: ~s encountered during server recovery ~p~n "
                             "stack ~p",
                             [?MODULE, C, E, S]),
                      ok
            end;
        _ ->
            ?DEBUG("~s: no server recovery configured", [?MODULE]),
            ok
    end,
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
