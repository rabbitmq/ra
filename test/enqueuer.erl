%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2017-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%
-module(enqueuer).

-behaviour(gen_server).

%% API functions
-export([start_link/1,
         start_link/2,
         wait/2]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-include("src/ra.hrl").

-type config() :: #{cluster_name := ra_cluster_name(),
                    servers := [ra_server_id()],
                    num_messages := non_neg_integer(),
                    spec := {Interval :: non_neg_integer(), Tag :: atom()}
                    }.

-record(state, {state :: ra_fifo_client:state(),
                next = 1 :: pos_integer(),
                max = 10 :: non_neg_integer(),
                tag :: atom(),
                applied = [] :: [non_neg_integer()],
                interval :: non_neg_integer(),
                waiting :: term()}).


%%%===================================================================
%%% API functions
%%%===================================================================

-spec start_link(config()) -> {ok, pid()} | ignore | {error, term()}.
start_link(Config) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [Config], []).

start_link(Name, Config) ->
    gen_server:start_link({local, Name}, ?MODULE, [Config], []).

wait(Pid, Timeout) ->
    gen_server:call(Pid, wait, Timeout).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([#{spec := {Interval, Tag},
        num_messages := Max,
        cluster_name := ClusterName,
        servers := Servers}]) ->
    erlang:send_after(Interval, self(), enqueue),
    F = ra_fifo_client:init(ClusterName, Servers),
    {ok, #state{state = F,
                max = Max,
                interval = Interval,
                tag = Tag}}.

handle_call(wait, _From, #state{next = N, max = N,
                                applied = Appd,
                                state = F} = State) ->
    {reply, {applied, Appd, F}, State};
handle_call(wait, From, State) ->
    {noreply, State#state{waiting = From}}.

handle_cast(Msg, State) ->
    ct:pal("enqeuer unhandled cast ~w", [Msg]),
    {noreply, State}.

handle_info(enqueue, #state{tag = Tag, next = Next0,
                            max = Max,
                            interval = Interval,
                            state = F0} = State0) ->
    Msg = {Tag, Next0},
    Next = Next0 + 1,
    case ra_fifo_client:enqueue(Next0, Msg, F0) of
        {T, F} when T =/= error ->
            case Max of
                Next0 ->
                    State = State0#state{state = F},
                    erlang:send_after(Interval, self(), reply),
                    {noreply, State};
                _ ->
                    State = State0#state{next = Next, state = F},
                    erlang:send_after(Interval, self(), enqueue),
                    {noreply, State}
            end;
        Err ->
            ?WARN("Enqueuer: error enqueue ~W", [Err, 5]),
            erlang:send_after(10, self(), enqueue),
            {noreply, State0}
    end;
handle_info({ra_event, From, Evt},
            #state{state = F0, applied = Appd} = State0) ->
    {internal, Applied, F} = ra_fifo_client:handle_ra_event(From, Evt, F0),
    {noreply, State0#state{state = F, applied = Appd ++ Applied}};
handle_info(reply, #state{waiting = undefined} = State0) ->
    {noreply, State0};
handle_info(reply, #state{state = F, waiting = From,
                          applied = Applied} = State0) ->
    gen_server:reply(From, {applied, Applied, F}),
    {noreply, State0}.




terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
