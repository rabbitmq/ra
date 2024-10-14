%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term Broadcom refers to Broadcom Inc. and/or its subsidiaries.
%%
%% @hidden
-module(ra_log_ets).
-behaviour(gen_server).

-export([start_link/1]).

-export([
         mem_table_please/2,
         mem_table_please/3,
         new_mem_table_please/3,
         delete_mem_table/2,
         execute_delete/3]).

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-include("ra.hrl").

-record(state, {names :: ra_system:names()}).

%%% ra_log_ets - owns and creates mem_table ETS tables

%%%===================================================================
%%% API functions
%%%===================================================================

start_link(#{names := #{log_ets := Name}} = Cfg) ->
    gen_server:start_link({local, Name}, ?MODULE, [Cfg], []).

-spec mem_table_please(ra_system:names(), ra:uid()) ->
    {ok, ra_log_membtbl:state()} | {error, term()}.
mem_table_please(Names, UId) ->
    mem_table_please(Names, UId, read_write).

-spec mem_table_please(ra_system:names(), ra:uid(), read | read_write) ->
    {ok, ra_log_membtbl:state()} | {error, term()}.
mem_table_please(#{log_ets := Name,
                   open_mem_tbls := OpnMemTbls}, UId, Mode) ->
    case ets:lookup(OpnMemTbls, UId) of
        [] ->
            case gen_server:call(Name, {mem_table_please, UId}) of
                {ok, [Tid]} ->
                    %% TODO: theoretically there could be multiple
                    {ok, ra_log_memtbl:init(Tid, Mode)};
                Err ->
                    Err
            end;
        [{_, Tid} | Rem] ->
            Mt = lists:foldl(
                   fun ({_, T}, Acc) ->
                           ra_log_memtbl:init_successor(T, Mode, Acc)
                   end, ra_log_memtbl:init(Tid, Mode), Rem),
            {ok, Mt}
    end.

-spec new_mem_table_please(ra_system:names(), ra:uid(), ra_log_membtbl:state()) ->
    {ok, ra_log_membtbl:state()} | {error, term()}.
new_mem_table_please(#{log_ets := Name}, UId, Prev) ->
    case gen_server:call(Name, {new_mem_table_please, UId}, infinity) of
        {ok, Tid} ->
            {ok, ra_log_memtbl:init_successor(Tid, read_write, Prev)};
        Err ->
            Err
    end.

delete_mem_table(#{log_ets := Name}, UId) ->
    gen_server:cast(Name, {delete_mem_table, UId}).

-spec execute_delete(ra_system:names(),
                     ra_log_memtbl:delete_spec(),
                     ra_log_memtbl:state()) ->
    ok.
execute_delete(#{}, undefined, _Mt) ->
    ok;
execute_delete(#{log_ets := Name}, Spec, Mt) ->
    gen_server:cast(Name, {exec_delete, Spec, Mt}).


%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([#{data_dir := DataDir,
        name := System,
        names := #{log_ets := LogEts,
                   open_mem_tbls := OpenMemTbls} = Names} = _Config]) ->
    process_flag(trap_exit, true),
    ?INFO("~s: in system ~s initialising...", [LogEts, System]),
    _ = ets:new(OpenMemTbls, [bag, protected, named_table]),
    ok = ra_directory:init(DataDir, Names),
    {ok, #state{names = Names}}.

handle_call({mem_table_please, UId}, _From,
            #state{names = #{open_mem_tbls := OpnMemTbls}} = State) ->
    case ets:lookup(OpnMemTbls, UId) of
        [] ->
            Tid = ets:new(memtbl, [set,
                                   {write_concurrency, true},
                                   {decentralized_counters, true},
                                   public
                                  ]),
            true = ets:insert(OpnMemTbls, {UId, Tid}),
            {reply, {ok, [Tid]}, State};
        Tids ->
            {reply, {ok, Tids}, State#state{}}
    end;
handle_call({new_mem_table_please, UId}, _From,
            #state{names = #{open_mem_tbls := OpnMemTbls}} = State) ->
    Tid = ets:new(memtbl, [set,
                           {write_concurrency, true},
                           {decentralized_counters, true},
                           public
                          ]),
    true = ets:insert(OpnMemTbls, {UId, Tid}),
    {reply, {ok, Tid}, State};
handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast({exec_delete, Spec, Mt}, State) ->
    try timer:tc(fun () -> ra_log_memtbl:delete(Spec, Mt) end) of
        {Time, Num} ->
            ?DEBUG("ra_log_ets: ets:delete/1 took ~bms to delete ~w ~b entries",
                   [Time div 1000, Spec, Num]),
            ok
    catch
        _:Err ->
            ?WARN("ra_log_ets: failed to delete ~w ~w ",
                  [Spec, Err]),
            ok
    end,
    {noreply, State};
handle_cast({delete_mem_table, UId},
            #state{names = #{open_mem_tbls := OpenMemTbls}}  = State) ->
    %% delete ets tables,
    %% we need to be defensive here.
    %% it is better to leak a table than to crash them all

    [begin
         try timer:tc(fun () -> ets_delete(Tid) end) of
             {Time, true} ->
                 ?DEBUG("ra_log_ets: ets:delete/1 took ~bms to delete ~w",
                        [Time div 1000, Tid]),
                 ok
         catch
             _:Err ->
                 ?WARN("ra_log_ets: failed to delete ets table ~w with ~w "
                       "This table may need to be cleaned up manually",
                       [Tid, Err]),
                 ok
         end
     end || {ok, Tid} <- ets:lookup(OpenMemTbls, UId)],
    {noreply, State};

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

ets_delete(Tid) ->
    _ = ets:delete(Tid),
    true.

terminate(Reason, #state{names = Names}) ->
    ?DEBUG("ra_log_ets: terminating with ~p", [Reason]),
    ok = ra_directory:deinit(Names),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
