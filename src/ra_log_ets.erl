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
         give_away/2,
         delete_tables/2]).

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-include("ra.hrl").

-record(state, {names :: ra_system:names()}).

%%% ra_log_ets - owns mem_table ETS tables

%%%===================================================================
%%% API functions
%%%===================================================================

start_link(#{names := #{log_ets := Name}} = Cfg) ->
    gen_server:start_link({local, Name}, ?MODULE, [Cfg], []).

-spec give_away(ra_system:names(), ets:tid()) -> true.
give_away(#{log_ets := Name}, Tid) ->
    ets:give_away(Tid, whereis(Name), undefined).

-spec delete_tables(ra_system:names(), [ets:tid()]) -> ok.
delete_tables(#{log_ets := Name}, Tids) ->
    gen_server:cast(Name, {delete_tables, Tids}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([#{data_dir := DataDir,
        names := #{open_mem_tbls := OpenTbl,
                   closed_mem_tbls := ClosedTbl} = Names}]) ->
    process_flag(trap_exit, true),
    TableFlags =  [named_table,
                   {read_concurrency, true},
                   {write_concurrency, true},
                   public],
    % create mem table lookup table to be used to map ra cluster name
    % to table identifiers to query.
    _ = ets:new(OpenTbl, [set | TableFlags]),
    _ = ets:new(ClosedTbl, [bag | TableFlags]),
    ok = ra_directory:init(DataDir, Names),
    {ok, #state{names = Names}}.

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
                       "This table may need to be cleaned up manually",
                       [Tid, Err]),
                 ok
         end
     end || Tid <- Tids],
    {noreply, State};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{names = Names}) ->
    ok = ra_directory:deinit(Names),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
