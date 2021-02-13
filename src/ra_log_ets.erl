%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2017-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%
%% @hidden
-module(ra_log_ets).
-behaviour(gen_server).

-export([start_link/1,
         give_away/1,
         delete_tables/1]).

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-include("ra.hrl").

-record(state, {}).

%%% ra_log_ets - owns mem_table ETS tables

%%%===================================================================
%%% API functions
%%%===================================================================

start_link(DataDir) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [DataDir], []).

-spec give_away(ets:tid()) -> true.
give_away(Tid) ->
    ets:give_away(Tid, whereis(?MODULE), undefined).

-spec delete_tables([ets:tid()]) -> ok.
delete_tables(Tids) ->
    gen_server:cast(?MODULE, {delete_tables, Tids}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([DataDir]) ->
    process_flag(trap_exit, true),
    TableFlags =  [named_table,
                   {read_concurrency, true},
                   {write_concurrency, true},
                   public],
    % create mem table lookup table to be used to map ra cluster name
    % to table identifiers to query.
    _ = ets:new(ra_log_open_mem_tables, [set | TableFlags]),
    _ = ets:new(ra_log_closed_mem_tables, [bag | TableFlags]),

    _ = ra_counters:init(),
    _ = ra_leaderboard:init(),

    %% Table for ra processes to record their current snapshot index so that
    %% other processes such as the segment writer can use this value to skip
    %% stale records and avoid flushing unnecessary data to disk.
    %% This is written from the ra process so will need write_concurrency.
    %% {RaUId, ra_index()}
    _ = ets:new(ra_log_snapshot_state, [set | TableFlags]),

    ok = ra_directory:init(DataDir),
    {ok, #state{}}.

handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast({delete_tables, Tids}, State) ->
    %% delete ets tables,
    %% we need to be defensive here.
    %% it is better to leak a table than to crash them all
    [begin
         try ets:delete(Tid) of
             true -> ok
         catch
             _:Err ->
                 ?WARN("ra_log_ets: failed to delete ets table ~w with ~w "
                       "This table may need to be cleaned up manually~n",
                       [Tid, Err]),
                 ok
         end
     end || Tid <- Tids],
    {noreply, State};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok = ra_directory:deinit(),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================






