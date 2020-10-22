%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2017-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%
%% @hidden
-module(ra_log_meta).
-behaviour(gen_batch_server).

-export([start_link/1,
         init/1,
         handle_batch/2,
         terminate/2,
         format_status/1,
         store/4,
         store_sync/4,
         delete/2,
         delete_sync/2,
         fetch/3,
         fetch/4
        ]).

-include("ra.hrl").

%% centralised meta data storage server for ra servers.

-type key() :: current_term | voted_for | last_applied.
-type value() :: non_neg_integer() | atom() | {atom(), atom()}.

-define(TIMEOUT, 30000).
-define(SYNC_INTERVAL, 5000).

-record(?MODULE, {ref :: reference(),
                  table_name :: atom()}).

-opaque state() :: #?MODULE{}.

-export_type([state/0]).

-spec start_link(ra_system:config()) ->
    {ok, pid()} | {error, {already_started, pid()}}.
start_link(#{names := #{log_meta := Name}} = Cfg) ->
    gen_batch_server:start_link({local, Name}, ?MODULE, Cfg, []).

-spec init(ra_system:config()) -> {ok, state()}.
init(#{name := System,
       data_dir := Dir,
       names := #{log_meta := TblName}}) ->
    process_flag(trap_exit, true),
    ok = ra_lib:make_dir(Dir),
    MetaFile = filename:join(Dir, "meta.dets"),
    {ok, Ref} = dets:open_file(TblName, [{file, MetaFile},
                                         {auto_save, ?SYNC_INTERVAL}]),
    _ = ets:new(TblName, [named_table, public, {read_concurrency, true}]),
    TblName = dets:to_ets(TblName, TblName),
    ?INFO("ra: meta data store initialised for system ~s. ~b record(s) recovered",
          [System, ets:info(TblName, size)]),
    {ok, #?MODULE{ref = Ref,
                  table_name = TblName}}.

handle_batch(Commands, #?MODULE{ref = Ref,
                                table_name = TblName} = State) ->
    DoInsert =
        fun (Id, Key, Value, Inserts0) ->
                case Inserts0 of
                    #{Id := Data} ->
                        Inserts0#{Id => update_key(Key, Value, Data)};
                    _ ->
                        case dets:lookup(Ref, Id) of
                            [{Id, T, V, A}] ->
                                Data = {Id, T, V, A},
                                Inserts0#{Id => update_key(Key, Value, Data)};
                            [] ->
                                Data = {Id, undefined, undefined, undefined},
                                Inserts0#{Id => update_key(Key, Value, Data)}
                        end
                end
        end,
    {Inserts, Replies, ShouldSync} =
        lists:foldl(
          fun ({cast, {store, Id, Key, Value}},
               {Inserts0, Replies, DoSync}) ->
                  {DoInsert(Id, Key, Value, Inserts0), Replies, DoSync};
              ({call, From, {store, Id, Key, Value}},
               {Inserts0, Replies, _DoSync}) ->
                  {DoInsert(Id, Key, Value, Inserts0),
                   [{reply, From, ok} | Replies], true};
              ({cast, {delete, Id}},
               {Inserts0, Replies, DoSync}) ->
                  {handle_delete(TblName, Id, Ref, Inserts0), Replies, DoSync};
              ({call, From, {delete, Id}},
               {Inserts0, Replies, _DoSync}) ->
                  {handle_delete(TblName, Id, Ref, Inserts0),
                   [{reply, From, ok} | Replies], true}
          end, {#{}, [], false}, Commands),
    Objects = maps:values(Inserts),
    ok = dets:insert(TblName, Objects),
    true = ets:insert(TblName, Objects),
    case ShouldSync of
        true ->
            ok = dets:sync(TblName);
        false ->
            ok
    end,
    {ok, Replies, State}.

terminate(_, #?MODULE{ref = Ref,
                      table_name = TblName}) ->
    ok = dets:sync(TblName),
    _ = dets:close(Ref),
    ok.

format_status(State) ->
    State.

%% send a message to the meta data store using cast
-spec store(atom(), ra_uid(), key(), value()) -> ok.
store(Name, UId, Key, Value) when is_atom(Name) ->
    gen_batch_server:cast(Name, {store, UId, Key, Value}).

%% waits until batch has been processed and synced.
%% when it returns the store request has been safely flushed to disk
-spec store_sync(atom(), ra_uid(), key(), value()) -> ok.
store_sync(Name, UId, Key, Value) ->
    gen_batch_server:call(Name, {store, UId, Key, Value}, ?TIMEOUT).

-spec delete(atom(), ra_uid()) -> ok.
delete(Name, UId) ->
    gen_batch_server:cast(Name, {delete, UId}).

-spec delete_sync(atom(), ra_uid()) -> ok.
delete_sync(Name, UId) ->
    gen_batch_server:call(Name, {delete, UId}, ?TIMEOUT).

%% READER API

-spec fetch(atom(), ra_uid(), key()) -> value() | undefined.
fetch(MetaName, Id, current_term) ->
    maybe_fetch(MetaName, Id, 2);
fetch(MetaName, Id, voted_for) ->
    maybe_fetch(MetaName, Id, 3);
fetch(MetaName, Id, last_applied) ->
    maybe_fetch(MetaName, Id, 4).

-spec fetch(atom(), ra_uid(), key(), term()) -> value().
fetch(MetaName, Id, Key, Default) ->
    case fetch(MetaName, Id, Key) of
        undefined -> Default;
        Value -> Value
    end.

%%% internal

maybe_fetch(MetaName, Id, Pos) ->
    try ets:lookup_element(MetaName, Id, Pos) of
        E -> E
    catch
        _:badarg ->
            undefined
    end.

handle_delete(TblName, Id, Ref, Inserts) ->
  _ = dets:delete(Ref, Id),
  _ = ets:delete(TblName, Id),
  maps:remove(Id, Inserts).

update_key(current_term, Value, Data) ->
    case element(2, Data) of
        %% current term matches the new value, nothing to do
        Value -> Data;
        %% current term has changed. Clear voted_for field as part of the update.
        %% See rabbitmq/ra#111.
        _     ->
          Data1 = setelement(3, Data, undefined),
          setelement(2, Data1, Value)
    end;
update_key(voted_for, Value, Data) ->
    setelement(3, Data, Value);
update_key(last_applied, Value, Data) ->
    setelement(4, Data, Value).
