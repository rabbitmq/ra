%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2017-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%
%% @hidden
-module(ra_machine_ets).
-behaviour(gen_server).

-export([start_link/0,
         create_table/2]).

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {current = #{}}).

-include("ra.hrl").

%%% machine ets owner

%%%===================================================================
%%% API functions
%%%===================================================================

create_table(Name, Opts) ->
    gen_server:call(?MODULE, {new_ets, Name, Opts}).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
    {ok, #state{}}.

handle_call({new_ets, Name, Opts}, _From, State) ->
    {reply, ok, make_table(Name, Opts, State)}.

handle_cast({new_ets, Name, Opts}, State) ->
    {noreply, make_table(Name, Opts, State)}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

make_table(Name, Opts, #state{current = Curr} = State) ->
    case Curr of
        #{Name := _} ->
            % table exists - do nothing
            State;
        _ ->
            _ = ets:new(Name, Opts),
            State#state{current = Curr#{Name => ok}}
    end.
