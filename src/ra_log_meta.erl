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
-type value() :: non_neg_integer() | atom() | {atom() | binary(), atom()}.

-define(TIMEOUT, 30000).
-define(SYNC_INTERVAL, 5000).
%% shu's default WAL size (see shu.hrl ?DEFAULT_WAL_SIZE). Kept in sync here
%% as the fallback when no override is configured.
-define(WAL_SIZE_DEFAULT, 16777216).
%% Maximum number of distinct ra servers whose metadata this store can hold.
%% Unlike DETS this is a fixed-size store, so the ceiling is pre-allocated;
%% override with the ra_log_meta_expected_count env for very large deployments.
-define(EXPECTED_COUNT_DEFAULT, 50000).
%% Size of shu's atom table, which backs the (small, slowly-growing) set of
%% distinct voted_for node names. Raised well above shu's 256 default so node
%% churn cannot exhaust it in practice; override with ra_log_meta_atom_slots.
-define(ATOM_SLOTS_DEFAULT, 4096).

-record(?MODULE, {shu          :: shu:state(),
                  table_name   :: atom(),
                  compact_pid  :: undefined | pid(),
                  compact_mref :: undefined | reference()}).

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

    ShuState0 =
        case shu:open(MetaShu, schema()) of
            {ok, S} ->
                S;
            {error, OpenReason} ->
                %% e.g. schema_mismatch (schema changed vs the on-disk file),
                %% unsupported_version, or a corrupt/invalid header. There is no
                %% safe automatic recovery for Raft metadata, so fail loudly and
                %% leave the file untouched for operator intervention rather than
                %% silently starting with empty metadata.
                ?ERROR("ra_log_meta: cannot open shu store ~ts: ~p",
                       [MetaShu, OpenReason]),
                error({shu_open_failed, MetaShu, OpenReason})
        end,

    %% Create the ETS hot-cache table as today.
    _ = ets:new(TblName, [named_table, public, {read_concurrency, true}]),

    %% Migrate from a legacy DETS file if one is present.
    {RecoveredCount, ShuState1} =
        case filelib:is_file(MetaDets) of
            true ->
                migrate_from_dets(MetaDets, ShuState0);
            false ->
                {0, ShuState0}
        end,

    %% Populate the ETS cache from shu.
    ok = populate_ets_from_shu(TblName, ShuState1),
    ETSCount = ets:info(TblName, size),

    case RecoveredCount of
        0 ->
            ?INFO("ra: meta data store initialised for system ~ts. "
                  "~b record(s) recovered", [System, ETSCount]);
        _ ->
            ?INFO("ra: meta data store initialised for system ~ts. "
                  "~b record(s) migrated from DETS, ~b record(s) total",
                  [System, RecoveredCount, ETSCount])
    end,

    ok = schedule_sync(),

    {ok, #?MODULE{shu = ShuState1,
                  table_name = TblName}}.

handle_batch(Commands, #?MODULE{table_name = TblName} = State) ->
    {Inserts, Changed, Replies, DoSync, State1} =
        lists:foldl(fun handle_command/2,
                    {#{}, #{}, [], false, State}, Commands),

    %% Apply the merged rows to the ETS cache.
    Objects = maps:values(Inserts),
    true = ets:insert(TblName, Objects),

    %% Build shu write ops containing only the fields that actually changed
    %% in this batch. Fields cleared to 'undefined' are written explicitly so
    %% the change is persisted - in particular the voted_for reset on a term
    %% change (see rabbitmq/ra#111). Writing only changed fields also keeps a
    %% pure last_applied update a high-frequency WAL write (no fsync).
    WriteOps = lists:filtermap(
                 fun (Id) ->
                         case maps:get(Id, Changed) of
                             [] ->
                                 false;
                             Fields ->
                                 {true,
                                  to_shu_write_op(maps:get(Id, Inserts),
                                                  Fields)}
                         end
                 end, maps:keys(Changed)),

    State2 = write_ops(State1, WriteOps),
    State3 = case DoSync of
                 true -> sync_shu(State2);
                 false -> State2
             end,
    %% Only the periodic timer message reschedules itself; a store_sync that
    %% set DoSync must not create additional timers.
    case lists:member({info, sync_meta}, Commands) of
        true -> ok = schedule_sync();
        false -> ok
    end,
    {ok, Replies, State3}.

handle_command({cast, {store, Id, Key, Value}}, Acc) ->
    apply_store(Id, Key, Value, Acc);
handle_command({call, From, {store, Id, Key, Value}}, Acc0) ->
    %% store_sync must be durable on return, so force a sync after the batch
    %% (a low-frequency field is synced by shu already; a high-frequency field
    %% such as last_applied would otherwise only reach the WAL)
    {I, C, R, _DoSync, S} = apply_store(Id, Key, Value, Acc0),
    {I, C, [{reply, From, ok} | R], true, S};
handle_command({cast, {delete, Id}}, Acc) ->
    apply_delete(Id, Acc);
handle_command({call, From, {delete, Id}}, Acc0) ->
    %% delete_sync must be durable on return: force a batch sync so any
    %% high-frequency write co-batched with the delete is flushed too
    {I, C, R, _DoSync, S} = apply_delete(Id, Acc0),
    {I, C, [{reply, From, ok} | R], true, S};
handle_command({call, From, ping}, Acc) ->
    add_reply(From, Acc);
handle_command({info, sync_meta}, {I, C, R, _DoSync, S}) ->
    {I, C, R, true, S};
handle_command({info, {'DOWN', MRef, process, _Pid, Reason}},
               {I, C, R, DoSync, #?MODULE{compact_mref = MRef} = S}) ->
    %% The compaction worker exited. Apply its result (finish on success,
    %% abort on failure/crash) without ever crashing the meta store.
    {I, C, R, DoSync, apply_compaction_result(Reason, S)};
handle_command({info, Info}, Acc) ->
    ?ERROR("ra_log_meta: unexpected info message: ~p", [Info]),
    Acc;
handle_command(Unhandled, Acc) ->
    ?DEBUG("ra: meta data unhandled ~p", [Unhandled]),
    Acc.

add_reply(From, {I, C, R, DoSync, S}) ->
    {I, C, [{reply, From, ok} | R], DoSync, S}.

apply_store(Id, Key, Value,
            {Inserts, Changed, R, DoSync,
             #?MODULE{table_name = TblName} = State}) ->
    Old = case Inserts of
              #{Id := D} ->
                  D;
              _ ->
                  case ets:lookup(TblName, Id) of
                      [D] -> D;
                      [] -> {Id, undefined, undefined, undefined}
                  end
          end,
    New = update_key(Key, Value, Old),
    Fields = add_changed_fields(Old, New, maps:get(Id, Changed, [])),
    {Inserts#{Id => New}, Changed#{Id => Fields}, R, DoSync, State}.

apply_delete(Id, {Inserts, Changed, R, DoSync,
                  #?MODULE{table_name = TblName, shu = S0} = State}) ->
    _ = ets:delete(TblName, Id),
    S1 = case shu:delete(S0, Id) of
             {ok, S} ->
                 S;
             {error, not_found} ->
                 %% never persisted (e.g. deleted in the same batch it was
                 %% created) - nothing to remove from shu
                 S0
         end,
    {maps:remove(Id, Inserts), maps:remove(Id, Changed), R, DoSync,
     State#?MODULE{shu = S1}}.

%% Determine which shu fields changed between the old and new ETS rows and
%% add their names to the accumulator (deduplicated).
add_changed_fields(Old, New, Acc0) ->
    Acc1 = case element(2, Old) =:= element(2, New) of
               true -> Acc0;
               false -> add_field(current_term, Acc0)
           end,
    Acc2 = case element(3, Old) =:= element(3, New) of
               true -> Acc1;
               false -> add_field(voted_for_node,
                                  add_field(voted_for_name, Acc1))
           end,
    case element(4, Old) =:= element(4, New) of
        true -> Acc2;
        false -> add_field(last_applied, Acc2)
    end.

add_field(Field, Acc) ->
    case lists:member(Field, Acc) of
        true -> Acc;
        false -> [Field | Acc]
    end.

terminate(_, #?MODULE{compact_mref = MRef} = State0) ->
    ?DEBUG("ra: meta data store is terminating", []),
    %% If a compaction is in flight, wait for it to finish so we can close
    %% cleanly. The ra_log_meta child spec sets shutdown => 30_000 to give this
    %% the same budget as await_compaction (see ra_log_sup).
    State = case MRef of
                undefined -> State0;
                _ -> await_compaction(State0, 30_000)
            end,
    case shu:close(State#?MODULE.shu) of
        ok ->
            ok;
        {error, Reason} ->
            %% e.g. compaction_in_progress when a compaction could not be
            %% finished in time. The WAL is intact on disk and will be
            %% replayed on the next start, so this is recoverable.
            ?WARN("ra: meta data store could not cleanly close shu: ~p",
                  [Reason]),
            ok
    end.

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

%% Wait for the metadata store to have processed all prior requests. Useful
%% in tests to ensure the process has finished (re)initialising.
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

%% Convert an ETS row {UId, CT, VF, LA} to a shu write op containing only the
%% requested fields (using the current row values, which may be undefined).
to_shu_write_op({UId, CurrentTerm, VotedFor, LastApplied}, Fields) ->
    %% Only decode voted_for when a voted_for field actually changed; the
    %% dominant write is a pure last_applied update which must stay cheap.
    {ServerNameBin, Node} =
        case lists:member(voted_for_name, Fields) orelse
             lists:member(voted_for_node, Fields) of
            true -> split_voted_for(VotedFor);
            false -> {undefined, undefined}
        end,
    FieldValues =
        lists:foldl(
          fun (current_term, Acc) ->
                  [{current_term, CurrentTerm} | Acc];
              (voted_for_name, Acc) ->
                  [{voted_for_name, ServerNameBin} | Acc];
              (voted_for_node, Acc) ->
                  [{voted_for_node, Node} | Acc];
              (last_applied, Acc) ->
                  [{last_applied, LastApplied} | Acc]
          end, [], Fields),
    {UId, FieldValues}.

%% Split a voted_for value into its shu representation:
%% {ServerNameBinary | undefined, Node :: atom() | undefined}.
split_voted_for(VotedFor) ->
    {ServerName, Node} = decode_voted_for(VotedFor),
    ServerNameBin = case ServerName of
                        undefined ->
                            undefined;
                        S when is_atom(S) ->
                            atom_to_binary(S, utf8);
                        B when is_binary(B) ->
                            B
                    end,
    {ServerNameBin, Node}.

%% Decode a voted_for value into a {ServerName, Node} pair.
%% - a {ServerName, Node} tuple is returned as-is
%% - undefined maps to {undefined, undefined}
%% - a bare atom (legacy format) maps to {Atom, undefined} so it round-trips
%%   back to the same bare atom via encode_voted_for/2
decode_voted_for({_, _} = ServerId) ->
    ServerId;
decode_voted_for(undefined) ->
    {undefined, undefined};
decode_voted_for(Atom) when is_atom(Atom) ->
    {Atom, undefined}.

%% Encode a {ServerName, Node} pair back into the ETS voted_for representation.
%% - both undefined -> undefined
%% - only ServerName set -> the bare atom (legacy format)
%% - both set -> a {ServerName, Node} tuple
encode_voted_for(undefined, undefined) -> undefined;
encode_voted_for(ServerName, undefined) -> ServerName;
encode_voted_for(ServerName, Node) -> {ServerName, Node}.

%% Schema definition for shu.
schema() ->
    WalSize = application:get_env(ra, ra_log_meta_wal_size, ?WAL_SIZE_DEFAULT),
    ExpectedCount = application:get_env(ra, ra_log_meta_expected_count,
                                        ?EXPECTED_COUNT_DEFAULT),
    AtomSlots = application:get_env(ra, ra_log_meta_atom_slots,
                                    ?ATOM_SLOTS_DEFAULT),
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
      %% ra_uid() is an arbitrary binary; shu caps keys at 255 bytes, which
      %% comfortably covers all generated and practical user-supplied uids.
      key => {binary, 255},
      expected_count => ExpectedCount,
      atom_table_slots => AtomSlots,
      wal_size => WalSize}.

%% Populate the ETS cache from shu on startup.
populate_ets_from_shu(TblName, ShuState) ->
    _ = shu:fold(
          fun(Key, Fields, _Acc) ->
                  CT = maps:get(current_term, Fields, undefined),
                  Node = maps:get(voted_for_node, Fields, undefined),
                  ServerNameBin = maps:get(voted_for_name, Fields, undefined),
                  ServerName = case ServerNameBin of
                                   undefined ->
                                       undefined;
                                   B when is_binary(B) ->
                                       binary_to_atom(B, utf8)
                               end,
                  VF = encode_voted_for(ServerName, Node),
                  LA = maps:get(last_applied, Fields, undefined),
                  ets:insert(TblName, {Key, CT, VF, LA}),
                  ok
          end,
          ok,
          ShuState),
    ok.

%% Migrate a legacy DETS file into shu. The DETS file is only renamed (and
%% thereby retired) once its contents have been durably written to shu. If the
%% migration fails it raises and meta.dets is left untouched, so the migration
%% is retried on the next start with no data loss. A corrupt or unexpectedly
%% shaped DETS file fails loudly rather than silently discarding metadata.
migrate_from_dets(MetaDets, ShuState0) ->
    case dets:open_file(ra_log_meta_migration, [{file, MetaDets}]) of
        {ok, DetsTable} ->
            Result =
                try
                    do_migrate_from_dets(DetsTable, ShuState0)
                catch
                    Class:Reason:Stack ->
                        ?ERROR("ra_log_meta: DETS migration failed; ~ts is "
                               "preserved for inspection: ~p:~p~n~p",
                               [MetaDets, Class, Reason, Stack]),
                        _ = dets:close(DetsTable),
                        erlang:raise(Class, Reason, Stack)
                end,
            _ = dets:close(DetsTable),
            %% Only reached on success; retire the source file.
            ok = rename_migrated(MetaDets),
            Result;
        {error, Reason} ->
            ?ERROR("ra_log_meta: cannot open legacy DETS ~ts for migration; "
                   "it is preserved for inspection: ~p", [MetaDets, Reason]),
            error({dets_open_failed, MetaDets, Reason})
    end.

do_migrate_from_dets(DetsTable, ShuState0) ->
    Count = dets:info(DetsTable, size),
    ?INFO("ra_log_meta: migrating ~b record(s) from DETS", [Count]),
    Ops = dets:foldl(fun dets_row_to_write_op/2, [], DetsTable),
    case migrate_write(ShuState0, lists:reverse(Ops)) of
        {ok, ShuState1} ->
            {ok, ShuState2} = shu:sync(ShuState1),
            {Count, ShuState2};
        {error, Reason} ->
            error({migration_write_failed, Reason})
    end.

%% Write all migration ops in a single batch (one fsync) in the common case.
%% If the configured WAL is too small to hold the batch's high-frequency
%% (last_applied) entries, fall back to incremental writes with compaction
%% between them, which cannot overflow the WAL.
migrate_write(ShuState, Ops) ->
    case shu:write_batch(ShuState, Ops) of
        {ok, S1} ->
            {ok, S1};
        {wal_full, S1} ->
            ?WARN("ra_log_meta: WAL too small for bulk migration; migrating "
                  "incrementally", []),
            migrate_incremental(S1, Ops);
        {error, _} = Err ->
            Err
    end.

migrate_incremental(ShuState, []) ->
    {ok, ShuState};
migrate_incremental(ShuState0, [{Key, FieldValues} | Rest]) ->
    case shu:write(ShuState0, Key, FieldValues) of
        {ok, S1} ->
            migrate_incremental(S1, Rest);
        {wal_full, S1} ->
            %% flush the WAL into the record area, then retry this op into the
            %% now-empty WAL
            {Work, S2} = shu:prepare_compact(S1),
            case shu:do_compact(Work) of
                ok ->
                    {ok, S3} = shu:finish_compact(ok, S2),
                    migrate_incremental(S3, [{Key, FieldValues} | Rest]);
                {error, _} = Err ->
                    Err
            end;
        {error, _} = Err ->
            Err
    end.

dets_row_to_write_op({UId, CurrentTerm, VotedFor, LastApplied}, Acc) ->
    {ServerNameBin, Node} = split_voted_for(VotedFor),
    Op = {UId, [{current_term, CurrentTerm},
                {voted_for_name, ServerNameBin},
                {voted_for_node, Node},
                {last_applied, LastApplied}]},
    [Op | Acc].

rename_migrated(MetaDets) ->
    Migrated = MetaDets ++ ".migrated",
    case file:rename(MetaDets, Migrated) of
        ok ->
            ?INFO("ra_log_meta: DETS migration complete, renamed ~ts to ~ts",
                  [MetaDets, Migrated]),
            ok;
        {error, Reason} ->
            %% the data is already durably in shu; a failed rename just means
            %% migration is retried (idempotently) on the next start
            ?WARN("ra_log_meta: could not rename ~ts after migration: ~p",
                  [MetaDets, Reason]),
            ok
    end.

%% Start an asynchronous compaction. shu is flipped into 'compacting' mode
%% which buffers subsequent writes in memory until finish_compact/2 is called.
-dialyzer({nowarn_function, start_compact/1}).
start_compact(#?MODULE{compact_pid = undefined, shu = S0} = State) ->
    {Work, S1} = shu:prepare_compact(S0),
    {Pid, MRef} = spawn_monitor(
                    fun () -> exit({compact_result, shu:do_compact(Work)}) end),
    State#?MODULE{shu = S1, compact_pid = Pid, compact_mref = MRef};
start_compact(#?MODULE{compact_pid = Pid} = State) when is_pid(Pid) ->
    %% already compacting
    State.

%% Apply the outcome of a compaction worker (its DOWN exit reason), returning a
%% non-compacting state. do_compact/1 never raises, so the worker normally
%% exits {compact_result, ok | {error, _}}; any other reason is an abnormal
%% crash. A failed or crashed compaction is aborted (WAL left intact, buffered
%% values retained in the WAL cache) rather than crashing the meta store.
apply_compaction_result({compact_result, ok}, #?MODULE{shu = S0} = State) ->
    case shu:finish_compact(ok, S0) of
        {ok, S1} ->
            clear_compaction(State#?MODULE{shu = S1});
        {error, Reason} ->
            ?ERROR("ra_log_meta: finish_compact failed: ~p; aborting", [Reason]),
            {ok, S1} = shu:abort_compact(S0),
            clear_compaction(State#?MODULE{shu = S1})
    end;
apply_compaction_result({compact_result, {error, Reason}},
                        #?MODULE{shu = S0} = State) ->
    ?ERROR("ra_log_meta: compaction failed: ~p; aborting", [Reason]),
    {ok, S1} = shu:abort_compact(S0),
    clear_compaction(State#?MODULE{shu = S1});
apply_compaction_result(Reason, #?MODULE{shu = S0} = State) ->
    ?ERROR("ra_log_meta: compaction worker crashed: ~p; aborting", [Reason]),
    {ok, S1} = shu:abort_compact(S0),
    clear_compaction(State#?MODULE{shu = S1}).

clear_compaction(State) ->
    State#?MODULE{compact_pid = undefined, compact_mref = undefined}.

%% Block until the in-flight compaction worker exits, then apply its result.
%% On timeout the state is returned unchanged (still compacting); callers decide
%% how to handle that.
await_compaction(#?MODULE{compact_mref = MRef} = State, Timeout) ->
    receive
        {'DOWN', MRef, process, _Pid, Reason} ->
            apply_compaction_result(Reason, State)
    after Timeout ->
              ?ERROR("ra_log_meta: compaction did not finish within ~bms",
                     [Timeout]),
              State
    end.

%% Write the batch to shu. A full WAL triggers a background compaction; while it
%% runs, writes are buffered in shu's bounded in-memory WAL. If that buffer also
%% fills before the compaction completes, block until it does (reclaiming the
%% WAL) and retry - degrading to synchronous only under sustained pressure.
%% Thrown shu errors are converted into a controlled exit that restarts the log
%% subtree and recovers from shu + WAL.
write_ops(State, []) ->
    State;
write_ops(#?MODULE{shu = S0} = State, WriteOps) ->
    try shu:write_batch(S0, WriteOps) of
        {ok, S1} ->
            State#?MODULE{shu = S1};
        {wal_full, S1} ->
            State1 = State#?MODULE{shu = S1},
            State2 = case State1#?MODULE.compact_pid of
                         undefined ->
                             %% no compaction running yet: start one, then
                             %% retry (the write buffers into the fresh WAL)
                             start_compact(State1);
                         _ ->
                             %% a compaction is running and its buffer is full;
                             %% wait for it to complete before retrying
                             ensure_compaction_finished(State1)
                     end,
            write_ops(State2, WriteOps);
        {error, Reason} = Err ->
            ?ERROR("ra_log_meta: write_batch failed: ~p", [Reason]),
            exit(Err)
    catch
        throw:{error, atom_table_full} = ATErr ->
            ?ERROR("ra_log_meta: cannot persist metadata, shu atom table is "
                   "full (too many distinct node names): ~p", [ATErr]),
            exit({shu_write_failed, ATErr});
        throw:{unknown_field, _} = UFErr ->
            ?ERROR("ra_log_meta: cannot persist metadata: ~p", [UFErr]),
            exit({shu_write_failed, UFErr})
    end.

%% Wait for the in-flight compaction to complete so the WAL is reclaimed. A
%% compaction that never completes leaves the store unable to make progress and
%% is fatal.
ensure_compaction_finished(State0) ->
    State = await_compaction(State0, ?TIMEOUT),
    case State#?MODULE.compact_pid of
        undefined ->
            State;
        _ ->
            ?ERROR("ra_log_meta: compaction did not complete; cannot reclaim "
                   "WAL", []),
            exit(compaction_stuck)
    end.

%% Periodically fsync the shu store to bound loss of WAL-buffered
%% (high-frequency) fields such as last_applied, similar to the old DETS
%% auto_save interval.
sync_shu(#?MODULE{shu = S0} = State) ->
    case shu:sync(S0) of
        {ok, S1} ->
            State#?MODULE{shu = S1};
        {error, compaction_in_progress} ->
            %% pending writes are buffered and flushed when compaction ends
            State;
        {error, Reason} ->
            ?ERROR("ra_log_meta: sync failed: ~p", [Reason]),
            State
    end.

schedule_sync() ->
    _ = erlang:send_after(?SYNC_INTERVAL, self(), sync_meta),
    ok.
