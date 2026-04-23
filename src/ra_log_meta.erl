%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2017-2025 Broadcom. All Rights Reserved. The term Broadcom refers to Broadcom Inc. and/or its subsidiaries.
%%
%% @hidden
-module(ra_log_meta).
-behaviour(gen_batch_server).

-export([start_link/1,
         init/1,
         handle_batch/2,
         handle_info/2,
         terminate/2,
         format_status/1,
         store/4,
         store_sync/4,
         delete/2,
         delete_sync/2,
         fetch/3,
         fetch/4,
         await/1
        ]).

-include("ra.hrl").

%% centralised meta data storage server for ra servers.

-type key() :: current_term | voted_for | last_applied.
-type value() :: non_neg_integer() | atom() | {atom(), atom()}.

-define(TIMEOUT, 30000).
% -define(SYNC_INTERVAL, 5000).

-record(?MODULE, {shu            :: shu:state(),
                  table_name     :: atom(),
                  data_dir       :: file:filename_all(),
                  compact_pid    :: undefined | pid(),
                  compact_mref   :: undefined | reference(),
                  compact_watermark = 0.8 :: float()}).

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
    MetaShu = filename:join(Dir, "meta.shu"),
    MetaDets = filename:join(Dir, "meta.dets"),

    Schema = schema(),
    {ok, ShuState0} = shu:open(MetaShu, Schema),

    %% Create ETS table as today
    _ = ets:new(TblName, [named_table, public, {read_concurrency, true}]),

    %% Migration from DETS if present
    {RecoveredCount, ShuState1} = case filelib:is_file(MetaDets) of
                                      true ->
                                          migrate_from_dets(MetaDets, ShuState0,
                                                            TblName);
                                      false ->
                                          {0, ShuState0}
                                  end,

    %% Populate ETS from shu
    ok = populate_ets_from_shu(TblName, ShuState1),
    ETSCount = ets:info(TblName, size),

    ?INFO("ra: meta data store initialised for system ~ts. ~b record(s) "
          "recovered from ~s, ~b total records",
          [System, RecoveredCount,
           case RecoveredCount of
               0 -> "shu";
               _ -> "dets"
           end,
           ETSCount]),

    {ok, #?MODULE{shu = ShuState1,
                  table_name = TblName,
                  data_dir = Dir}}.

handle_batch(Commands, #?MODULE{shu = S0,
                                table_name = TblName} = State) ->
    DoInsert =
        fun (Id, Key, Value, Inserts0) ->
                case Inserts0 of
                    #{Id := Data} ->
                        Inserts0#{Id => update_key(Key, Value, Data)};
                    _ ->
                        case ets:lookup(TblName, Id) of
                            [Data] ->
                                Inserts0#{Id => update_key(Key, Value, Data)};
                            [] ->
                                Data = {Id, undefined, undefined, undefined},
                                Inserts0#{Id => update_key(Key, Value, Data)}
                        end
                end
        end,
    {Inserts, Replies} =
        lists:foldl(
          fun ({cast, {store, Id, Key, Value}},
               {Inserts0, Replies}) ->
                  {DoInsert(Id, Key, Value, Inserts0), Replies};
              ({call, From, {store, Id, Key, Value}},
               {Inserts0, Replies}) ->
                  {DoInsert(Id, Key, Value, Inserts0),
                   [{reply, From, ok} | Replies]};
              ({cast, {delete, Id}},
               {Inserts0, Replies}) ->
                  {handle_delete(TblName, Id, Inserts0), Replies};
              ({call, From, {delete, Id}},
               {Inserts0, Replies}) ->
                  {handle_delete(TblName, Id, Inserts0),
                   [{reply, From, ok} | Replies]};
              ({call, From, ping},
               {Inserts0, Replies}) ->
                  {Inserts0, [{reply, From, ok} | Replies]}
          end, {#{}, []}, Commands),

    Objects = maps:values(Inserts),
    true = ets:insert(TblName, Objects),

    %% Translate to shu write_batch format
    WriteOps = [to_shu_write_op(Obj) || Obj <- Objects],

    %% Write to shu - shu handles syncing based on schema frequency config
    case shu:write_batch(S0, WriteOps) of
        {ok, S1} ->
            %% Check if we should proactively compact
            State1 = check_and_start_compaction(State#?MODULE{shu = S1}),
            {ok, Replies, State1};
        {wal_full, S1} ->
            %% WAL is full, kick off compaction and retry
            State1 = do_sync_compact(State#?MODULE{shu = S1}),
            %% After sync compact, retry the write
            case shu:write_batch(State1#?MODULE.shu, WriteOps) of
                {ok, S2} ->
                    State2 = State1#?MODULE{shu = S2},
                    State3 = check_and_start_compaction(State2),
                    {ok, Replies, State3};
                {wal_full, _S2} ->
                    %% Still full after compact - crash with descriptive error
                    ?ERROR("ra_log_meta: WAL still full after compaction for ~ts", [TblName]),
                    exit({wal_full_after_compaction, TblName});
                {error, Reason} = Err ->
                    ?ERROR("ra_log_meta: write_batch failed: ~p", [Reason]),
                    exit(Err)
            end;
        {error, Reason} = Err ->
            ?ERROR("ra_log_meta: write_batch failed: ~p", [Reason]),
            exit(Err)
    end.

handle_info({'DOWN', MRef, process, _Pid, {compact_result, Result}},
            #?MODULE{compact_mref = MRef, shu = S0} = State) ->
    case shu:finish_compact(Result, S0) of
        {ok, S1} ->
            {ok, State#?MODULE{shu = S1, compact_pid = undefined,
                               compact_mref = undefined}};
        {error, Reason} ->
            ?ERROR("ra_log_meta: compaction finish failed: ~p", [Reason]),
            exit({compaction_failed, Reason})
    end;
handle_info({'DOWN', _MRef, process, Pid, Reason},
            #?MODULE{compact_mref = _MRef2} = _State) ->
    ?ERROR("ra_log_meta: compaction worker ~p crashed: ~p", [Pid, Reason]),
    exit({compaction_worker_crashed, Reason});
handle_info(Info, State) ->
    ?ERROR("ra_log_meta: unexpected info message: ~p", [Info]),
    {ok, State}.

terminate(_, #?MODULE{shu = S0, compact_mref = MRef} = State) ->
    ?DEBUG("ra: meta data store is terminating", []),
    %% If a compaction is in flight, wait for it to finish
    S1 = case MRef of
             undefined -> S0;
             _ ->
                 await_compaction(State, 30_000)
         end,
    ok = shu:close(S1),
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

%% Wait for the metadata store to be ready (used in tests)
-spec await(atom()) -> ok.
await(Name) ->
    gen_batch_server:call(Name, ping, ?TIMEOUT).

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
    try ets:lookup_element(MetaName, Id, Pos)
    catch
        _:badarg ->
            undefined
    end.

handle_delete(TblName, Id, Inserts) ->
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

%% Helper to convert ETS row {UId, CT, VF, LA} to shu write operations
to_shu_write_op({UId, CurrentTerm, VotedFor, LastApplied}) ->
    FieldValues1 = case CurrentTerm of
                       undefined ->
                           [];
                       _ ->
                           [{current_term, CurrentTerm}]
                   end,
    FieldValues2 = case VotedFor of
                       undefined ->
                           FieldValues1;
                       _ ->
                           {Node, ServerName} = decode_voted_for(VotedFor),
                           ServerNameBin = case ServerName of
                                               undefined -> undefined;
                                               S when is_atom(S) ->
                                                   atom_to_binary(S, utf8);
                                               S -> S
                                           end,
                           [{voted_for_name, ServerNameBin},
                            {voted_for_node, Node} | FieldValues1]
                   end,
    FieldValues3 = case LastApplied of
                       undefined ->
                           FieldValues2;
                       _ ->
                           [{last_applied, LastApplied} | FieldValues2]
                   end,
    {UId, FieldValues3}.

%% Decode voted_for from ETS representation to (Node, ServerName) tuple
%% If VotedFor is an atom (old format), convert to {undefined, Atom}
%% If VotedFor is a {Node, ServerName} tuple, return as-is
%% If VotedFor is undefined, return {undefined, undefined}
decode_voted_for({_, _} = ServerId) ->
    ServerId;
decode_voted_for(undefined) ->
    {undefined, undefined};
decode_voted_for(Atom) when is_atom(Atom) ->
    {undefined, Atom}.

%% Schema definition for shu
schema() ->
    #{fields => [#{name => current_term,
                    type => {integer, 64},
                    frequency => low},
                  #{name => voted_for_name,
                    type => {binary, 255},
                    frequency => low},
                  #{name => voted_for_node,
                    type => {atom, 255},
                    frequency => low},
                  #{name => last_applied,
                    type => {integer, 64},
                    frequency => high}],
       key => {binary, 64},
       expected_count => 50000}.

%% Populate ETS table from shu on startup
populate_ets_from_shu(TblName, ShuState) ->
    shu:fold(
        fun(Key, _Acc) ->
            {ok, Fields} = shu:read_all(ShuState, Key),
            CT = maps:get(current_term, Fields, undefined),
            Node = maps:get(voted_for_node, Fields, undefined),
            ServerNameBin = maps:get(voted_for_name, Fields, undefined),
            ServerName = case ServerNameBin of
                             undefined ->
                                 undefined;
                             B when is_binary(B) ->
                                 binary_to_atom(B, utf8);
                             _ ->
                                 ServerNameBin
                         end,
            VF = encode_voted_for(Node, ServerName),
            LA = maps:get(last_applied, Fields, undefined),
            ?DEBUG("ra_log_meta: recovered from shu - Key=~p, CT=~p, VF=~p, LA=~p",
                   [Key, CT, VF, LA]),
            ets:insert(TblName, {Key, CT, VF, LA}),
            _Acc
        end,
        ok,
        ShuState),
    ok.

%% Encode voted_for back into ETS representation
%% If both fields are undefined, return undefined
%% If only ServerName is set, return it as an atom (legacy format)
%% If both are set, return {Node, ServerName} tuple
encode_voted_for(undefined, undefined) -> undefined;
encode_voted_for(undefined, ServerName) -> ServerName;
encode_voted_for(Node, ServerName) -> {Node, ServerName}.

%% Migrate from DETS to shu
migrate_from_dets(MetaDets, ShuState0, _TblName) ->
    {ok, DetsTable} = dets:open_file(ra_log_meta_migration, [{file, MetaDets}]),
    try
        Count = dets:info(DetsTable, size),
        ?INFO("ra_log_meta: migrating ~b records from DETS", [Count]),

        %% Collect all DETS rows and convert to shu write operations
        Ops = dets:foldl(
            fun({UId, CurrentTerm, VotedFor, LastApplied}, Acc) ->
                {ServerName, Node} = decode_voted_for(VotedFor),
                ServerNameBin = case ServerName of
                                    undefined -> undefined;
                                    S when is_atom(S) -> atom_to_binary(S, utf8);
                                    S -> S
                                end,
                WriteOp = {UId, [{current_term, CurrentTerm},
                                 {voted_for_name, ServerNameBin},
                                 {voted_for_node, Node},
                                 {last_applied, LastApplied}]},
                [WriteOp | Acc]
            end,
            [],
            DetsTable),

        ?DEBUG("ra_log_meta: migration write ops = ~p", [lists:reverse(Ops)]),

        %% Write all to shu in a single batch and sync
        {ok, ShuState1} = shu:write_batch(ShuState0, lists:reverse(Ops)),
        {ok, ShuState2} = shu:sync(ShuState1),

        ?INFO("ra_log_meta: migration completed, wrote to shu and synced", []),

        {Count, ShuState2}
    after
        _ = dets:close(DetsTable),
        %% Rename DETS file to .migrated
        _ = file:rename(MetaDets, MetaDets ++ ".migrated")
    end.

%% Check if WAL usage exceeds watermark and start compaction if needed
check_and_start_compaction(#?MODULE{shu = S, compact_pid = undefined, compact_watermark = Watermark} = State) ->
    #{wal_usage := Usage} = shu:info(S),
    case Usage >= Watermark of
        true -> start_compact(State);
        false -> State
    end;
check_and_start_compaction(State) ->
    State.

%% Start async compaction
-dialyzer({nowarn_function, start_compact/1}).
start_compact(#?MODULE{compact_pid = undefined, shu = S0} = State) ->
    {Work, S1} = shu:prepare_compact(S0),
    {Pid, MRef} = spawn_monitor(fun () -> exit({compact_result, shu:do_compact(Work)}) end),
    State#?MODULE{shu = S1, compact_pid = Pid, compact_mref = MRef};
start_compact(#?MODULE{compact_pid = Pid} = State) when is_pid(Pid) ->
    %% already compacting
    State.

%% Synchronous compaction (used when WAL is full and we need to retry immediately)
do_sync_compact(#?MODULE{shu = S0} = State) ->
    {Work, S1} = shu:prepare_compact(S0),
    case shu:do_compact(Work) of
        ok ->
            case shu:finish_compact(ok, S1) of
                {ok, S2} ->
                    State#?MODULE{shu = S2};
                {error, Reason} ->
                    ?ERROR("ra_log_meta: sync compaction finish failed: ~p", [Reason]),
                    exit({sync_compaction_failed, Reason})
            end;
        {error, Reason} ->
            ?ERROR("ra_log_meta: do_compact failed: ~p", [Reason]),
            exit({do_compact_failed, Reason})
    end.

%% Wait for compaction to finish with timeout (used in terminate)
await_compaction(#?MODULE{compact_mref = MRef, shu = S0}, Timeout) ->
    receive
        {'DOWN', MRef, process, _Pid, {compact_result, Result}} ->
            case shu:finish_compact(Result, S0) of
                {ok, S} -> S;
                {error, Reason} ->
                    ?ERROR("ra_log_meta: compaction finish during shutdown failed: ~p", [Reason]),
                    S0
            end
    after Timeout ->
        ?ERROR("ra_log_meta: compaction worker did not finish during shutdown", []),
        S0
    end.
