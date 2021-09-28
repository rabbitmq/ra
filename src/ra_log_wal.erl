%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2017-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%
%% @hidden
-module(ra_log_wal).
-behaviour(gen_batch_server).

-export([start_link/1,
         write/5,
         write_batch/2,
         truncate_write/5,
         force_roll_over/1,
         init/1,
         handle_batch/2,
         terminate/2,
         format_status/1
        ]).

-export([wal2list/1]).

-compile([inline_list_funcs]).
-compile(inline).

-include("ra.hrl").

-define(CURRENT_VERSION, 1).
-define(MAGIC, "RAWA").
-define(HEADER_SIZE, 5).

-define(COUNTER_FIELDS,
        [wal_files,
         batches,
         writes
         ]).

-define(C_WAL_FILES, 1).
-define(C_BATCHES, 2).
-define(C_WRITES, 3).
% a writer_id consists of a unqique local name (see ra_directory) and a writer's
% current pid().
% The pid is used for the immediate writer notification
% The atom is used by the segment writer to send the segments
% This has the effect that a restarted server has a different identity in terms
% of it's write notification but the same identity in terms of it's ets
% tables and segment notification
-type writer_id() :: {binary(), pid()}.

-record(batch_writer, {tbl_start :: ra_index(),
                       uid :: term(),
                       tid :: term(), %% TODO
                       from :: ra_index(),
                       to :: ra_index(),
                       term :: ra_term(),
                       inserts = [] :: list()}).

-record(batch, {writes = 0 :: non_neg_integer(),
                waiting = #{} :: #{pid() => #batch_writer{}},
                pending = [] :: iolist()
               }).

-type wal_write_strategy() ::
    % writes all pending in one write(2) call then calls fsync(1)
    default |
    % like delay writes but tries to open the file using synchronous io
    % (O_SYNC) rather than a write(2) followed by an fsync.
    o_sync.

-type writer_name_cache() :: {NextIntId :: non_neg_integer(),
                              #{writer_id() => binary()}}.

-record(conf, {file_modes :: [term()],
               dir :: string(),
               segment_writer = ra_log_segment_writer :: atom(),
               compute_checksums = false :: boolean(),
               max_size_bytes :: non_neg_integer(),
               max_entries :: undefined | non_neg_integer(),
               recovery_chunk_size = ?WAL_RECOVERY_CHUNK_SIZE :: non_neg_integer(),
               write_strategy = default :: wal_write_strategy(),
               sync_method = datasync :: sync | datasync,
               counter :: counters:counters_ref(),
               open_mem_tbls_name :: atom(),
               closed_mem_tbls_name :: atom(),
               names :: ra_system:names()
              }).

-record(wal, {fd :: maybe(file:io_device()),
              filename :: maybe(file:filename()),
              writer_name_cache = {0, #{}} :: writer_name_cache(),
              max_size :: non_neg_integer(),
              entry_count = 0 :: non_neg_integer()
              }).

-record(state, {conf = #conf{},
                file_num = 0 :: non_neg_integer(),
                wal :: #wal{} | undefined,
                file_size = 0 :: non_neg_integer(),
                % writers that have attempted to write an non-truncating
                % out of seq % entry.
                % No further writes are allowed until the missing
                % index has been received.
                % out_of_seq are kept after a roll over or until
                % a truncating write is received.
                % no attempt is made to recover this information after a crash
                % beyond the available WAL files
                % all writers seen within the lifetime of a WAL file
                % and the last index seen
                writers = #{} :: #{ra_uid() =>
                                   {in_seq | out_of_seq, ra_index()}},
                batch :: maybe(#batch{})
               }).

-type state() :: #state{}.
-type wal_conf() :: #{name := atom(), %% the name to register the wal as
                      names := ra_system:names(),
                      dir := file:filename_all(),
                      max_size_bytes => non_neg_integer(),
                      max_entries => non_neg_integer(),
                      segment_writer => atom() | pid(),
                      compute_checksums => boolean(),
                      write_strategy => wal_write_strategy(),
                      sync_method => sync | datasync,
                      recovery_chunk_size  => non_neg_integer(),
                      hibernate_after => non_neg_integer(),
                      max_batch_size => non_neg_integer()
                     }.

-export_type([wal_conf/0,
              wal_write_strategy/0]).

-type wal_command() ::
    {append | truncate, writer_id(), ra_index(), ra_term(), term()}.

-type wal_op() :: {cast, wal_command()} |
                  {call, from(), wal_command()}.

-spec write(writer_id(), atom(), ra_index(), ra_term(), term()) ->
    ok | {error, wal_down}.
write(From, Wal, Idx, Term, Entry) ->
    named_cast(Wal, {append, From, Idx, Term, Entry}).

-spec truncate_write(writer_id(), atom(), ra_index(), ra_term(), term()) ->
    ok | {error, wal_down}.
truncate_write(From, Wal, Idx, Term, Entry) ->
   named_cast(Wal, {truncate, From, Idx, Term, Entry}).

-spec write_batch(Wal :: atom() | pid(), [wal_command()]) ->
    ok | {error, wal_down}.
write_batch(Wal, WalCommands) when is_pid(Wal) ->
    gen_batch_server:cast_batch(Wal, WalCommands);
write_batch(Wal, WalCommands) when is_atom(Wal) ->
    case whereis(Wal) of
        undefined ->
            {error, wal_down};
        Pid ->
            write_batch(Pid, WalCommands)
    end.

named_cast(To, Msg) when is_pid(To) ->
    gen_batch_server:cast(To, Msg);
named_cast(Wal, Msg) ->
    case whereis(Wal) of
        undefined ->
            {error, wal_down};
        Pid ->
            named_cast(Pid, Msg)
    end.

% force a wal file to roll over to a new file
% mostly useful for testing
force_roll_over(Wal) ->
    ok = gen_batch_server:cast(Wal, rollover),
    ok.

%% ra_log_wal
%%
%% Writes Raft entries to shared persistent storage for multiple "writers"
%% fsyncs in batches, typically the write requests
%% received in the mailbox during
%% the previous fsync operation. Notifies all writers after each fsync batch.
%% Also have got a dynamically increasing max writes limit that grows in order
%% to trade-off latency for throughput.
%%
%% Entries are written to the .wal file as well as a per-writer mem table (ETS).
%% In order for writers to locate an entry by an index a lookup ETS table
%% (ra_log_open_mem_tables) keeps the current range of indexes
%% a mem_table as well
%% as the mem_table tid(). This lookup table is updated on every write.
%%
%% Once the current .wal file is full a new one is closed. All the entries in
%% ra_log_open_mem_tables are moved to ra_log_closed_mem_tables so that writers
%% can still locate the tables whilst they are being flushed ot disk. The
%% ra_log_segment_writer is notified of all the mem tables written to during
%% the lifetime of the .wal file and will begin writing these to on-disk segment
%% files. Once it has finished the current set of mem_tables it will delete the
%% corresponding .wal file.

-spec start_link(Config :: wal_conf()) ->
    {ok, pid()} | {error, {already_started, pid()}}.
start_link(#{name := Name} = Config)
  when is_atom(Name) ->
    WalMaxBatchSize = maps:get(max_batch_size, Config,
                               ?WAL_DEFAULT_MAX_BATCH_SIZE),
    Options0 = case maps:get(hibernate_after, Config, undefined) of
                   undefined ->
                       [{max_batch_size, WalMaxBatchSize}];
                   Hib ->
                       [{hibernate_after, Hib},
                        {max_batch_size, WalMaxBatchSize}]
               end,
    Options = [{reversed_batch, true} | Options0],
    gen_batch_server:start_link({local, Name}, ?MODULE, Config, Options).

%%% Callbacks

-spec init(wal_conf()) -> {ok, state()}.
init(#{dir := Dir} = Conf0) ->
    #{max_size_bytes := MaxWalSize,
      max_entries := MaxEntries,
      recovery_chunk_size := RecoveryChunkSize,
      segment_writer := SegWriter,
      compute_checksums := ComputeChecksums,
      write_strategy := WriteStrategy,
      sync_method := SyncMethod,
      names := #{wal := WalName,
                 open_mem_tbls := OpenTblsName,
                 closed_mem_tbls := ClosedTblsName} = Names} =
        merge_conf_defaults(Conf0),
    ?NOTICE("WAL: ~s init, open tbls: ~w, closed tbls: ~w",
            [WalName, OpenTblsName, ClosedTblsName]),
    process_flag(trap_exit, true),
    % given ra_log_wal is effectively a fan-in sink it is likely that it will
    % at times receive large number of messages from a large number of
    % writers
    process_flag(message_queue_data, off_heap),
    CRef = ra_counters:new(WalName, ?COUNTER_FIELDS),
    % wait for the segment writer to process anything in flight
    ok = ra_log_segment_writer:await(SegWriter),
    %% TODO: recover wal should return {stop, Reason} if it fails
    %% rather than crash
    FileModes = [raw, write, read, binary],
    Conf = #conf{file_modes = FileModes,
                 dir = Dir,
                 segment_writer = SegWriter,
                 compute_checksums = ComputeChecksums,
                 max_size_bytes = max(?WAL_MIN_SIZE, MaxWalSize),
                 max_entries = MaxEntries,
                 recovery_chunk_size = RecoveryChunkSize,
                 write_strategy = WriteStrategy,
                 sync_method = SyncMethod,
                 counter = CRef,
                 open_mem_tbls_name = OpenTblsName,
                 closed_mem_tbls_name = ClosedTblsName,
                 names = Names},
    {ok, recover_wal(Dir, Conf)}.

-spec handle_batch([wal_op()], state()) ->
    {ok, [gen_batch_server:action()], state()}.
handle_batch(Ops, State0) ->
    State = lists:foldr(fun handle_op/2, start_batch(State0), Ops),
    %% process all ops
    {ok, [garbage_collect], complete_batch(State)}.

terminate(_Reason, State) ->
    _ = cleanup(State),
    ok.

format_status(#state{conf = #conf{write_strategy = Strat,
                                  compute_checksums = Cs,
                                  max_size_bytes = MaxSize},
                     writers = Writers,
                     file_size = FSize,
                     wal = #wal{filename = Fn}}) ->
    #{write_strategy => Strat,
      compute_checksums => Cs,
      writers => maps:size(Writers),
      filename => filename:basename(Fn),
      current_size => FSize,
      max_size_bytes => MaxSize}.

%% Internal

handle_op({cast, WalCmd}, State) ->
    handle_msg(WalCmd, State).

recover_wal(Dir, #conf{segment_writer = SegWriter,
                       open_mem_tbls_name = OpenTbl,
                       closed_mem_tbls_name = ClosedTbl,
                       recovery_chunk_size = RecoveryChunkSize} = Conf) ->
    % ensure configured directory exists
    ok = ra_lib:make_dir(Dir),
    %  recover each mem table and notify segment writer
    %  this may result in duplicated segments but that is better than
    %  losing any data
    %  As we have waited for the segment writer to finish processing it is
    %  assumed that any remaining wal files need to be re-processed.
    WalFiles = lists:sort(filelib:wildcard(filename:join(Dir, "*.wal"))),
    % First we recover all the tables using a temporary lookup table.
    % Then we update the actual lookup tables atomically.
    RecoverTid = ets:new(ra_log_recover_mem_tables,
                         [set, {read_concurrency, true}, private]),
    % compute all closed mem table lookups required so we can insert them
    % all at once, atomically
    % It needs to be atomic so that readers don't accidentally
    % read partially recovered
    % tables mixed with old tables
    RecoverConf = Conf#conf{open_mem_tbls_name = RecoverTid},
    All = [begin
               FBase = filename:basename(F),
               ?DEBUG("wal: recovering ~s", [FBase]),
               Fd = open_at_first_record(F),
               {Time, ok} = timer:tc(
                              fun () ->
                                      recover_wal_chunks(RecoverConf, Fd,
                                                         RecoveryChunkSize)
                              end),
               ?DEBUG("wal: recovered ~s time taken ~bms",
                      [FBase, Time div 1000]),
               close_existing(Fd),
               recovering_to_closed(RecoverTid, F)
           end || F <- WalFiles],
    % get all the recovered tables and insert them into closed
    Closed = lists:append([C || {C, _, _} <- All]),
    true = ets:insert(ClosedTbl, Closed),
    % send all the mem tables to segment writer for processing
    % This could result in duplicate segments
    [ok = ra_log_segment_writer:accept_mem_tables(SegWriter, M, F)
     || {_, M, F} <- All],

    FileNum = extract_file_num(lists:reverse(WalFiles)),
    State = roll_over(RecoverTid, #state{conf = Conf,
                                         file_num = FileNum}),
    % we can now delete all open mem tables as should be covered by recovered
    % closed tables
    Open = ets:tab2list(OpenTbl),
    true = ets:delete_all_objects(OpenTbl),
    % delete all open ets tables
    [true = ets:delete(Tid) || {_, _, _, Tid} <- Open],
    true = ets:delete(RecoverTid),
    %% force garbage cleanup
    true = erlang:garbage_collect(),
    State.

extract_file_num([]) ->
    0;
extract_file_num([F | _]) ->
    ra_lib:zpad_extract_num(filename:basename(F)).


cleanup(#state{wal = #wal{fd = undefined}}) ->
    ok;
cleanup(#state{wal = #wal{fd = Fd}}) ->
    _ = ra_file_handle:sync(Fd),
    ok.

serialize_header(UId, Trunc, {Next, Cache} = WriterCache) ->
    T = case Trunc of true -> 1; false -> 0 end,
    case Cache of
        #{UId := BinId} ->
            {<<T:1/unsigned, BinId/bitstring>>, 2, WriterCache};
        _ ->
            % TODO: check overflows of Next
            % cache the last 23 bits of the header word
            BinId = <<1:1/unsigned, Next:22/unsigned>>,
            IdDataLen = byte_size(UId),
            Prefix = <<T:1/unsigned, 0:1/unsigned, Next:22/unsigned,
                       IdDataLen:16/unsigned>>,
            MarkerId = [Prefix, UId],
            {MarkerId, 4 + IdDataLen,
             {Next + 1, Cache#{UId => BinId}}}
    end.

write_data({UId, _} = Id, Idx, Term, Data0, Trunc,
           #state{conf = #conf{compute_checksums = ComputeChecksum},
                  wal = #wal{writer_name_cache = Cache0,
                             entry_count = Count} = Wal} = State00) ->
    EntryData = to_binary(Data0),
    EntryDataLen = byte_size(EntryData),
    {HeaderData, HeaderLen, Cache} = serialize_header(UId, Trunc, Cache0),
    % fixed overhead =
    % 24 bytes 2 * 64bit ints (idx, term) + 2 * 32 bit ints (checksum, datalen)
    DataSize = HeaderLen + 24 + EntryDataLen,
    % if the next write is going to exceed the configured max wal size
    % we roll over to a new wal.
    case should_roll_wal(State00) of
        true ->
            State = roll_over(State00),
            % TODO: there is some redundant computation performed by
            % recursing here it probably doesn't matter as it only happens
            % when a wal file fills up
            write_data(Id, Idx, Term, Data0, Trunc, State);
        false ->
            State0 = State00#state{wal = Wal#wal{writer_name_cache = Cache,
                                                 entry_count = Count + 1}},
            Entry = [<<Idx:64/unsigned,
                       Term:64/unsigned>>,
                     EntryData],
            Checksum = case ComputeChecksum of
                           true -> erlang:adler32(Entry);
                           false -> 0
                       end,
            Record = [HeaderData,
                      <<Checksum:32/integer, EntryDataLen:32/unsigned>>,
                      Entry],
            append_data(State0, Id, Idx, Term, Data0,
                        DataSize, Record, Trunc)
    end.


handle_msg({append, {UId, Pid} = Id, Idx, Term, Entry},
           #state{writers = Writers} = State0) ->
    case maps:find(UId, Writers) of
        {ok, {_, PrevIdx}} when Idx =< PrevIdx + 1 ->
            write_data(Id, Idx, Term, Entry, false, State0);
        error ->
            write_data(Id, Idx, Term, Entry, false, State0);
        {ok, {out_of_seq, _}} ->
            % writer is out of seq simply ignore drop the write
            % TODO: capture metric for dropped writes
            State0;
        {ok, {in_seq, PrevIdx}} ->
            % writer was in seq but has sent an out of seq entry
            % notify writer
            ?DEBUG("WAL: requesting resend from `~w`, "
                   "last idx ~b idx received ~b",
                   [UId, PrevIdx, Idx]),
            Pid ! {ra_log_event, {resend_write, PrevIdx + 1}},
            State0#state{writers = Writers#{UId => {out_of_seq, PrevIdx}}}
    end;
handle_msg({truncate, Id, Idx, Term, Entry}, State0) ->
    write_data(Id, Idx, Term, Entry, true, State0);
handle_msg(rollover, State) ->
    roll_over(State).

append_data(#state{conf = Cfg,
                   file_size = FileSize,
                   batch = Batch0,
                   writers = Writers} = State,
            {UId, Pid}, Idx, Term, Entry, DataSize, Data, Truncate) ->
    Batch = incr_batch(Cfg, Batch0, UId, Pid,
                       {Idx, Term}, Data, Entry, Truncate),
    State#state{file_size = FileSize + DataSize,
                batch = Batch,
                writers = Writers#{UId => {in_seq, Idx}} }.

incr_batch(#conf{open_mem_tbls_name = OpnMemTbl} = Cfg,
           #batch{writes = Writes,
                  waiting = Waiting0,
                  pending = Pend} = Batch,
           UId, Pid, {Idx, Term}, Data, Entry, Truncate) ->
    Waiting = case Waiting0 of
                  #{Pid := #batch_writer{tbl_start = TblStart0,
                                         tid = _Tid,
                                         from = From,
                                         inserts = Inserts0} = W} ->
                      TblStart = case Truncate of
                                     true ->
                                         Idx;
                                     false ->
                                         % take the min of the First item in
                                         % case we are overwriting before
                                         % the previously first seen entry
                                         min(TblStart0, Idx)
                                 end,
                      Inserts = [{Idx, Term, Entry} | Inserts0],
                      Waiting0#{Pid => W#batch_writer{from = min(Idx, From),
                                                      tbl_start = TblStart,
                                                      to = Idx,
                                                      term = Term,
                                                      inserts = Inserts}};
                  _ ->
                      %% no batch_writer
                      {Tid, TblStart} =
                          case ets:lookup(OpnMemTbl, UId) of
                              [{_UId, TblStart0, _To, T}] ->
                                  {T, case Truncate of
                                          true ->
                                              Idx;
                                          false ->
                                              min(TblStart0, Idx)
                                      end};
                              _ ->
                                  %% there is no table so need
                                  %% to open one
                                  T = open_mem_table(Cfg, UId),
                                  true = ets:insert_new(OpnMemTbl,
                                                        {UId, Idx, Idx, T}),
                                  {T, Idx}
                          end,
                      Writer = #batch_writer{tbl_start = TblStart,
                                             from = Idx,
                                             to = Idx,
                                             tid = Tid,
                                             uid = UId,
                                             term = Term,
                                             inserts = [{Idx, Term, Entry}]},
                      Waiting0#{Pid => Writer}
              end,

    Batch#batch{writes = Writes + 1,
                waiting = Waiting,
                pending = [Pend | Data]}.

update_mem_table(#conf{open_mem_tbls_name = OpnMemTbl} = Cfg,
                 UId, Idx, Term, Entry, Truncate) ->
    % TODO: if Idx =< First we could truncate the entire table and save
    % some disk space when it later is flushed to disk
    case ets:lookup(OpnMemTbl, UId) of
        [{_UId, From0, _To, Tid}] ->
            true = ets:insert(Tid, {Idx, Term, Entry}),
            From = case Truncate of
                       true ->
                           Idx;
                       false ->
                           % take the min of the First item in case we are
                           % overwriting before the previously first seen entry
                           min(From0, Idx)
                   end,
            % update Last idx for current tbl
            % this is how followers overwrite previously seen entries
            % TODO: OPTIMISATION
            % Writers don't need this updated for every entry. As they keep
            % a local cache of unflushed entries it is sufficient to update
            % ra_log_open_mem_tables before completing the batch.
            % Instead the `From` and `To` could be kept in the batch.
            _ = ets:update_element(OpnMemTbl, UId,
                                   [{2, From}, {3, Idx}]);
        [] ->
            % open new ets table
            Tid = open_mem_table(Cfg, UId),
            true = ets:insert_new(OpnMemTbl, {UId, Idx, Idx, Tid}),
            true = ets:insert(Tid, {Idx, Term, Entry})
    end.

roll_over(#state{conf = #conf{open_mem_tbls_name = Tbl}} = State0) ->
    State = complete_batch(State0),
    roll_over(Tbl, start_batch(State)).

roll_over(OpnMemTbls, #state{wal = Wal0, file_num = Num0,
                             conf = #conf{dir = Dir,
                                          max_size_bytes = MaxBytes
                                         } = Conf0} = State0) ->
    counters:add(Conf0#conf.counter, ?C_WAL_FILES, 1),
    Num = Num0 + 1,
    Fn = ra_lib:zpad_filename("", "wal", Num),
    NextFile = filename:join(Dir, Fn),
    ?DEBUG("wal: opening new file ~ts open mem tables: ~w", [Fn, OpnMemTbls]),
    %% if this is the first wal since restart randomise the first
    %% max wal size to reduce the likelyhood that each erlang node will
    %% flush mem tables at the same time
    NextMaxBytes = case Wal0 of
                       undefined ->
                           Half = MaxBytes div 2,
                           Half + rand:uniform(Half);
                       _ ->
                           ok = close_file(Wal0#wal.fd),
                           MemTables = ets:tab2list(OpnMemTbls),
                           ok = close_open_mem_tables(MemTables, Conf0,
                                                      Wal0#wal.filename),
                           MaxBytes
                   end,
    {Conf, Wal} = open_wal(NextFile, NextMaxBytes, Conf0),
    State0#state{conf = Conf,
                 wal = Wal,
                 file_size = 0,
                 file_num = Num}.

open_wal(File, Max, #conf{write_strategy = o_sync,
                          file_modes = Modes0} = Conf) ->
        Modes = [sync | Modes0],
        case prepare_file(File, Modes) of
            {ok, Fd} ->
                % many platforms implement O_SYNC a bit like O_DSYNC
                % perform a manual sync here to ensure metadata is flushed
                {Conf, #wal{fd = Fd,
                            max_size = Max,
                            filename = File}};
            {error, enotsup} ->
                ?WARN("wal: o_sync write strategy not supported. "
                      "Reverting back to default strategy.", []),
                open_wal(File, Max, Conf#conf{write_strategy = default})
        end;
open_wal(File, Max, #conf{file_modes = Modes} = Conf0) ->
    {ok, Fd} = prepare_file(File, Modes),
    Conf = maybe_pre_allocate(Conf0, Fd, Max),
    {Conf, #wal{fd = Fd,
                max_size = Max,
                filename = File}}.

prepare_file(File, Modes) ->
    Tmp = make_tmp(File),
    %% rename is atomic-ish so we will never accidentally write an empty wal file
    %% using prim_file here as file:rename/2 uses the file server
    ok = prim_file:rename(Tmp, File),
    case ra_file_handle:open(File, Modes) of
        {ok, Fd2} ->
            {ok, ?HEADER_SIZE} = file:position(Fd2, ?HEADER_SIZE),
            {ok, Fd2};
        {error, _} = Err ->
            Err
    end.

make_tmp(File) ->
    Tmp = filename:rootname(File) ++ ".tmp",
    {ok, Fd} = file:open(Tmp, [write, binary, raw]),
    ok = file:write(Fd, <<?MAGIC, ?CURRENT_VERSION:8/unsigned>>),
    ok = file:sync(Fd),
    ok = file:close(Fd),
    Tmp.

maybe_pre_allocate(#conf{sync_method = datasync} = Conf, Fd, Max0) ->
    Max = Max0 - ?HEADER_SIZE,
    case file:allocate(Fd, ?HEADER_SIZE, Max) of
        ok ->
            {ok, Max} = file:position(Fd, Max),
            ok = file:truncate(Fd),
            {ok, ?HEADER_SIZE} = file:position(Fd, ?HEADER_SIZE),
            Conf;
        {error, _} ->
            %% fallocate may not be supported, fall back to fsync instead
            %% of fdatasync
            ?INFO("wal: preallocation may not be supported by the file system"
                  " falling back to fsync instead of fdatasync", []),
            Conf#conf{sync_method = sync}
    end;
maybe_pre_allocate(Conf, _Fd, _Max) ->
    Conf.

close_file(undefined) ->
    ok;
close_file(Fd) ->
    ok = ra_file_handle:sync(Fd),
    ra_file_handle:close(Fd).

close_open_mem_tables(MemTables,
                      #conf{segment_writer = TblWriter,
                            open_mem_tbls_name = OpnMemTbls,
                            closed_mem_tbls_name = CloseMemTbls},
                      Filename) ->
    % insert into closed mem tables
    % so that readers can still resolve the table whilst it is being
    % flushed to persistent tables asynchronously
    [begin
         % In order to ensure that reads are done in the correct causal order
         % we need to append a monotonically increasing value for readers to
         % sort by
         M = erlang:unique_integer([monotonic, positive]),
         _ = ets:insert(CloseMemTbls, erlang:insert_element(2, T, M))
     end || T <- MemTables],
    % reset open mem tables table
    true = ets:delete_all_objects(OpnMemTbls),

    % notify segment_writer of new unflushed memtables
    ok = ra_log_segment_writer:accept_mem_tables(TblWriter, MemTables, Filename),
    ok.

recovering_to_closed(RecoverTid, Filename) ->
    MemTables = ets:tab2list(RecoverTid),
    Closed = [begin
                  M = erlang:unique_integer([monotonic, positive]),
                  erlang:insert_element(2, T, M)
              end || T <- MemTables],
    true = ets:delete_all_objects(RecoverTid),
    {Closed, MemTables, Filename}.


open_mem_table(Cfg, {UId, _Pid}) ->
    open_mem_table(Cfg, UId);
open_mem_table(#conf{names = Names}, UId) ->
    % lookup the locally registered name of the process to use as ets
    % name
    ServerName = ra_directory:name_of(Names, UId),
    Tid = ets:new(ServerName, [set, {read_concurrency, true}, public]),
    % immediately give away ownership to ets process
    true = ra_log_ets:give_away(Names, Tid),
    Tid.

start_batch(#state{conf = #conf{counter = CRef}} = State) ->
    ok = counters:add(CRef, ?C_BATCHES, 1),
    State#state{batch = #batch{}}.


flush_pending(#state{wal = #wal{fd = Fd},
                     batch = #batch{pending = Pend} = Batch,
                     conf =  #conf{write_strategy = WriteStrategy,
                                   sync_method = SyncMeth}} = State0) ->

    case WriteStrategy of
        default ->
            ok = ra_file_handle:write(Fd, Pend),
            ok = ra_file_handle:SyncMeth(Fd),
            ok;
        o_sync ->
            ok = ra_file_handle:write(Fd, Pend)
    end,
    State0#state{batch = Batch#batch{pending = []}}.

complete_batch(#state{batch = undefined} = State) ->
    State;
complete_batch(#state{batch = #batch{waiting = Waiting,
                                     writes = NumWrites},
                      conf = #conf{open_mem_tbls_name = OpnTbl} = Cfg
                      } = State00) ->
    % TS = erlang:system_time(microsecond),
    State0 = flush_pending(State00),
    % SyncTS = erlang:system_time(microsecond),
    counters:add(Cfg#conf.counter, ?C_WRITES, NumWrites),
    State = State0#state{batch = undefined},

    %% process writers
    _ = maps:map(fun (Pid, #batch_writer{tbl_start = TblStart,
                                         uid = UId,
                                         from = From,
                                         to = To,
                                         term = Term,
                                         inserts = Inserts,
                                         tid = Tid}) ->
                         %% need to reverse inserts in case an index overwrite
                         %% came to be processed in the same batch.
                         %% Unlikely, but possible
                         true = ets:insert(Tid, lists:reverse(Inserts)),
                         true = ets:update_element(OpnTbl, UId,
                                                   [{2, TblStart}, {3, To}]),
                         Pid ! {ra_log_event, {written, {From, To, Term}}},
                         ok
                 end, Waiting),
    State.

wal2list(File) ->
    Data = open_existing(File),
    dump_records(Data, []).

open_existing(File) ->
    case file:read_file(File) of
        {ok, <<?MAGIC, ?CURRENT_VERSION:8/unsigned, Data/binary>>} ->
            %% the only version currently supported
            Data;
        {ok, <<Magic:4/binary, UnknownVersion:8/unsigned, _/binary>>} ->
            exit({unknown_wal_file_format, Magic, UnknownVersion})
    end.

open_at_first_record(File) ->
    {ok, Fd} = file:open(File, [read, binary, raw]),
    case file:read(Fd, 5) of
        {ok, <<?MAGIC, ?CURRENT_VERSION:8/unsigned>>} ->
            %% the only version currently supported
            Fd;
        {ok, <<Magic:4/binary, UnknownVersion:8/unsigned>>} ->
            exit({unknown_wal_file_format, Magic, UnknownVersion})
    end.

close_existing(Fd) ->
    case file:close(Fd) of
        ok ->
            ok;
        {error, Reason} ->
            exit({could_not_close, Reason})
    end.

dump_records(<<_:1/unsigned, 0:1/unsigned, _:22/unsigned,
               IdDataLen:16/unsigned, _:IdDataLen/binary,
               _:32/integer,
               0:32/unsigned,
               _Idx:64/unsigned, _Term:64/unsigned,
               _EntryData:0/binary,
               _Rest/binary>>, Entries) ->
    Entries;
dump_records(<<_:1/unsigned, 0:1/unsigned, _:22/unsigned,
               IdDataLen:16/unsigned, _:IdDataLen/binary,
               Crc:32/integer,
               EntryDataLen:32/unsigned,
               Idx:64/unsigned, Term:64/unsigned,
               EntryData:EntryDataLen/binary,
               Rest/binary>>, Entries) ->
    % TODO: recover writers info, i.e. last index seen
    case erlang:adler32(<<Idx:64/unsigned, Term:64/unsigned, EntryData/binary>>) of
        Crc ->
            dump_records(Rest, [{Idx, Term, binary_to_term(EntryData)} | Entries]);
        _ ->
            exit({crc_failed_for, Idx, EntryData})
    end;
dump_records(<<_:1/unsigned, 1:1/unsigned, _:22/unsigned,
               Crc:32/integer,
               EntryDataLen:32/unsigned,
               Idx:64/unsigned, Term:64/unsigned,
               EntryData:EntryDataLen/binary,
               Rest/binary>>, Entries) ->
    case erlang:adler32(<<Idx:64/unsigned, Term:64/unsigned, EntryData/binary>>) of
        Crc ->
            dump_records(Rest, [{Idx, Term, binary_to_term(EntryData)} | Entries]);
        _ ->
            exit({crc_failed_for, Idx, EntryData})
    end;
dump_records(<<>>, Entries) ->
    Entries.

% TODO: recover writers info, i.e. last index seen
recover_wal_chunks(Conf, Fd, RecoveryChunkSize) ->
    Chunk = read_from_wal_file(Fd, RecoveryChunkSize),
    recover_records(Conf, Fd, Chunk, #{}, RecoveryChunkSize).
% All zeros indicates end of a pre-allocated wal file
recover_records(_, _Fd, <<0:1/unsigned, 0:1/unsigned, 0:22/unsigned,
                       IdDataLen:16/unsigned, _:IdDataLen/binary,
                       0:32/integer, 0:32/unsigned, _/binary>>,
                _Cache, _ChunkSize) ->
    ok;
% First record or different UID to last record
recover_records(Conf, Fd,
                <<Trunc:1/unsigned, 0:1/unsigned, IdRef:22/unsigned,
                  IdDataLen:16/unsigned, UId:IdDataLen/binary,
                  Checksum:32/integer,
                  EntryDataLen:32/unsigned,
                  Idx:64/unsigned, Term:64/unsigned,
                  EntryData:EntryDataLen/binary,
                  Rest/binary>>,
                Cache, RecoveryChunkSize) ->
    true = validate_and_update(Conf, UId, Checksum, Idx, Term, EntryData, Trunc),
    Cache0 = Cache#{IdRef => {UId, <<1:1/unsigned, IdRef:22/unsigned>>}},
    recover_records(Conf, Fd, Rest, Cache0, RecoveryChunkSize);
% Same UID as last record
recover_records(Conf, Fd,
                <<Trunc:1/unsigned, 1:1/unsigned, IdRef:22/unsigned,
                  Checksum:32/integer,
                  EntryDataLen:32/unsigned,
                  Idx:64/unsigned, Term:64/unsigned,
                  EntryData:EntryDataLen/binary,
                  Rest/binary>>,
                Cache, RecoveryChunkSize) ->
    #{IdRef := {UId, _}} = Cache,
    true = validate_and_update(Conf, UId, Checksum, Idx, Term, EntryData, Trunc),
    recover_records(Conf, Fd, Rest, Cache, RecoveryChunkSize);
% Not enough remainder to parse another record, need to read
recover_records(Conf, Fd, Chunk, Cache, RecoveryChunkSize) ->
    NextChunk = read_from_wal_file(Fd, RecoveryChunkSize),
    case NextChunk of
        <<>> ->
            %% we have reached the end of the file
            ok;
        _ ->
            %% append this chunk to the remainder of the last chunk
            Chunk0 = <<Chunk/binary, NextChunk/binary>>,
            recover_records(Conf, Fd, Chunk0, Cache, RecoveryChunkSize)
    end.

read_from_wal_file(Fd, Len) ->
    case file:read(Fd, Len) of
        {ok, <<Data/binary>>} ->
            Data;
        eof ->
            <<>>;
        {error, Reason} ->
            exit({could_not_read_wal_chunk, Reason})
    end.

validate_and_update(Conf, UId, Checksum, Idx, Term, EntryData, Trunc) ->
    validate_checksum(Checksum, Idx, Term, EntryData),
    true = update_mem_table(Conf, UId, Idx, Term,
                            binary_to_term(EntryData), Trunc =:= 1).

validate_checksum(0, _, _, _) ->
    % checksum not used
    ok;
validate_checksum(Checksum, Idx, Term, Data) ->
    % building a binary just for the checksum may feel a bit wasteful
    % but this is only called during recovery which should be a rare event
    case erlang:adler32(<<Idx:64/unsigned, Term:64/unsigned, Data/binary>>) of
        Checksum ->
            ok;
        _ ->
            exit(wal_checksum_validation_failure)
    end.

merge_conf_defaults(Conf) ->
    maps:merge(#{segment_writer => ra_log_segment_writer,
                 max_size_bytes => ?WAL_DEFAULT_MAX_SIZE_BYTES,
                 max_entries => undefined,
                 recovery_chunk_size => ?WAL_RECOVERY_CHUNK_SIZE,
                 compute_checksums => true,
                 write_strategy => default,
                 sync_method => datasync}, Conf).

to_binary(Term) ->
    term_to_binary(Term).

should_roll_wal(#state{conf = #conf{max_entries = MaxEntries},
                       file_size = FileSize,
                       wal = #wal{max_size = MaxWalSize,
                                  entry_count = Count}}) ->
    TooManyEntries = case MaxEntries of
                         undefined -> false;
                         _ ->
                             Count + 1 > MaxEntries
                     end,
    %% Initially, MaxWalSize was a hard limit for the file size: if FileSize +
    %% DataSize went over that limit, we would use a new file. This was an
    %% issue when DataSize was larger than the limit alone.
    %%
    %% The chosen solution is to only consider the current file size in the
    %% calculation. It means that after DataSize bytes are written, the file
    %% will be larger than the configured maximum size. But at least it will
    %% accept Ra commands larger than the max WAL size.
    FileSize > MaxWalSize orelse TooManyEntries.
