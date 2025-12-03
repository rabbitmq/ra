%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2017-2025 Broadcom. All Rights Reserved. The term Broadcom refers to Broadcom Inc. and/or its subsidiaries.
%%
%% @hidden
-module(ra_log_wal).
-behaviour(gen_batch_server).

-export([start_link/1,
         init/1,
         handle_batch/2,
         terminate/2,
         format_status/1
        ]).

-export([
         write/6,
         write_batch/2,
         last_writer_seq/2,
         force_roll_over/1]).

-export([wal2list/1]).

-compile([inline_list_funcs]).
-compile(inline).

-include("ra.hrl").

-define(CURRENT_VERSION, 1).
-define(MAGIC, "RAWA").
-define(HEADER_SIZE, 5).

-define(C_WAL_FILES, 1).
-define(C_BATCHES, 2).
-define(C_WRITES, 3).
-define(C_BYTES_WRITTEN, 4).
-define(COUNTER_FIELDS,
        [{wal_files, ?C_WAL_FILES, counter, "Number of write-ahead log files created"},
         {batches, ?C_BATCHES, counter, "Number of batches written"},
         {writes, ?C_WRITES, counter, "Number of entries written"},
         {bytes_written, ?C_BYTES_WRITTEN, counter, "Number of bytes written"}
         ]).

-define(FILE_MODES, [raw, write, read, binary]).

% a writer_id consists of a unique local name (see ra_directory) and a writer's
% current pid().
% The pid is used for the immediate writer notification
% The atom is used by the segment writer to send the segments
% This has the effect that a restarted server has a different identity in terms
% of it's write notification but the same identity in terms of it's ets
% tables and segment notification
-type writer_id() :: {ra_uid(), pid()}.

-record(batch_writer, {snap_idx :: ra_index(),
                       tid :: ets:tid(),
                       uid :: term(),
                       range :: ra:range(),
                       term :: ra_term(),
                       old :: undefined | #batch_writer{}
                      }).

-record(batch, {num_writes = 0 :: non_neg_integer(),
                waiting = #{} :: #{pid() => #batch_writer{}},
                pending = [] :: iolist()
               }).

-type wal_write_strategy() ::
    % writes all pending in one write(2) call then calls fsync(1)
    default |
    % like default but tries to open the file using synchronous io
    % (O_SYNC) rather than a write(2) followed by an fsync.
    o_sync |
    %% low latency mode where writers are notifies _before_ syncing
    %% but after writing.
    sync_after_notify.

-type writer_name_cache() :: {NextIntId :: non_neg_integer(),
                              #{writer_id() => binary()}}.

-record(conf, {dir :: file:filename_all(),
               segment_writer = ra_log_segment_writer :: atom() | pid(),
               compute_checksums = false :: boolean(),
               max_size_bytes :: non_neg_integer(),
               max_entries :: undefined | non_neg_integer(),
               recovery_chunk_size = ?WAL_RECOVERY_CHUNK_SIZE :: non_neg_integer(),
               write_strategy = default :: wal_write_strategy(),
               sync_method = datasync :: sync | datasync | none,
               counter :: counters:counters_ref(),
               mem_tables_tid :: ets:tid(),
               names :: ra_system:names(),
               explicit_gc = false :: boolean(),
               pre_allocate = false :: boolean(),
               ra_log_snapshot_state_tid :: ets:tid()
              }).

-record(wal, {fd :: option(file:io_device()),
              filename :: option(file:filename()),
              file_size = 0 :: non_neg_integer(),
              writer_name_cache = {0, #{}} :: writer_name_cache(),
              max_size :: non_neg_integer(),
              entry_count = 0 :: non_neg_integer(),
              ranges = #{} :: #{ra_uid() =>
                                [{ets:tid(), {ra:index(), ra:index()}}]}
              }).

-record(recovery, {mode :: initial | post_boot,
                   ranges = #{} :: #{ra_uid() =>
                                     [{ets:tid(), {ra:index(), ra:index()}}]},
                   tables = #{} :: #{ra_uid() => ra_mt:state()},
                   writers = #{} :: #{ra_uid() => {in_seq, ra:index()}}
                  }).
-record(state, {conf = #conf{},
                file_num = 0 :: non_neg_integer(),
                wal :: #wal{} | undefined,
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
                batch :: option(#batch{})
               }).

-type state() :: #state{}.
-type wal_conf() :: #{name := atom(), %% the name to register the wal as
                      system := atom(),
                      names := ra_system:names(),
                      dir := file:filename_all(),
                      max_size_bytes => non_neg_integer(),
                      max_entries => non_neg_integer(),
                      segment_writer => atom() | pid(),
                      compute_checksums => boolean(),
                      pre_allocate => boolean(),
                      write_strategy => wal_write_strategy(),
                      sync_method => sync | datasync,
                      recovery_chunk_size  => non_neg_integer(),
                      hibernate_after => non_neg_integer(),
                      max_batch_size => non_neg_integer(),
                      garbage_collect => boolean(),
                      min_heap_size => non_neg_integer(),
                      min_bin_vheap_size => non_neg_integer()
                     }.

-export_type([wal_conf/0,
              wal_write_strategy/0]).

-type wal_command() ::
    {append | truncate, writer_id(), ra_index(), ra_term(), term()}.

-type wal_op() :: {cast, wal_command()} |
                  {call, from(), wal_command()}.
-type wal_cmd() :: term() | {ttb, iodata()}.

-spec write(atom() | pid(), writer_id(), ets:tid(), ra_index(), ra_term(),
            wal_cmd()) ->
    {ok, pid()} | {error, wal_down}.
write(Wal, {_, _} = From, MtTid, Idx, Term, Cmd)
  when is_integer(Idx) andalso
       is_integer(Term) ->
    named_cast(Wal, {append, From, MtTid, Idx, Term, Cmd}).

-spec write_batch(Wal :: atom() | pid(), [wal_command()]) ->
    {ok, pid()} | {error, wal_down}.
write_batch(Wal, WalCommands) when is_pid(Wal) ->
    case is_process_alive(Wal) of
        true ->
            gen_batch_server:cast_batch(Wal, WalCommands),
            {ok, Wal};
        false ->
            {error, wal_down}
    end;
write_batch(Wal, WalCommands) when is_atom(Wal) ->
    case whereis(Wal) of
        undefined ->
            {error, wal_down};
        Pid ->
            write_batch(Pid, WalCommands)
    end.

-spec last_writer_seq(Wal :: atom() | pid(), ra:uid()) ->
    {ok, undefined | ra:index()} | {error, wal_down}.
last_writer_seq(Wal, UId) when is_pid(Wal) ->
    case is_process_alive(Wal) of
        true ->
            {ok, gen_batch_server:call(Wal, {?FUNCTION_NAME, UId}, infinity)};
        false ->
            {error, wal_down}
    end;
last_writer_seq(Wal, UId) when is_atom(Wal) ->
    case whereis(Wal) of
        undefined ->
            {error, wal_down};
        Pid ->
            last_writer_seq(Pid, UId)
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
%% can still locate the tables whilst they are being flushed to disk. The
%% ra_log_segment_writer is notified of all the mem tables written to during
%% the lifetime of the .wal file and will begin writing these to on-disk segment
%% files. Once it has finished the current set of mem_tables it will delete the
%% corresponding .wal file.

-spec start_link(Config :: wal_conf()) ->
    {ok, pid()} |
    {error, {already_started, pid()}} |
    {error, wal_checksum_validation_failure}.
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

-spec init(wal_conf()) ->
    {ok, state()} |
    {stop, wal_checksum_validation_failure} | {stop, term()}.
init(#{dir := Dir, system := System} = Conf0) ->
    #{max_size_bytes := MaxWalSize,
      max_entries := MaxEntries,
      recovery_chunk_size := RecoveryChunkSize,
      segment_writer := SegWriter,
      compute_checksums := ComputeChecksums,
      pre_allocate := PreAllocate,
      write_strategy := WriteStrategy,
      sync_method := SyncMethod,
      garbage_collect := Gc,
      min_heap_size := MinHeapSize,
      min_bin_vheap_size := MinBinVheapSize,
      names := #{wal := WalName,
                 open_mem_tbls := MemTablesName} = Names} =
        merge_conf_defaults(Conf0),
    ?NOTICE("WAL: ~ts init, mem-tables table name: ~w",
            [WalName, MemTablesName]),
    process_flag(trap_exit, true),
    % given ra_log_wal is effectively a fan-in sink it is likely that it will
    % at times receive large number of messages from a large number of
    % writers
    process_flag(message_queue_data, off_heap),
    process_flag(min_bin_vheap_size, MinBinVheapSize),
    process_flag(min_heap_size, MinHeapSize),
    CRef = ra_counters:new(WalName,
                           ?COUNTER_FIELDS,
                           #{ra_system => System, module => ?MODULE}),
    Conf = #conf{dir = Dir,
                 segment_writer = SegWriter,
                 compute_checksums = ComputeChecksums,
                 max_size_bytes = max(?WAL_MIN_SIZE, MaxWalSize),
                 max_entries = MaxEntries,
                 recovery_chunk_size = RecoveryChunkSize,
                 write_strategy = WriteStrategy,
                 sync_method = SyncMethod,
                 counter = CRef,
                 mem_tables_tid = ets:whereis(MemTablesName),
                 names = Names,
                 explicit_gc = Gc,
                 pre_allocate = PreAllocate,
                 ra_log_snapshot_state_tid = ets:whereis(ra_log_snapshot_state)},
    try recover_wal(Dir, Conf) of
        Result ->
            % wait for the segment writer to process any flush requests
            % generated during recovery
            ok = ra_log_segment_writer:await(SegWriter),
            {ok, Result}
    catch _:Err:_Stack ->
              {stop, Err}
    end.

-spec handle_batch([wal_op()], state()) ->
    {ok, [gen_batch_server:action()], state()}.
handle_batch(Ops, #state{conf = #conf{explicit_gc = Gc}} = State0) ->
    Actions0 = case Gc of
                   true ->
                       [garbage_collect];
                   false ->
                       []
               end,
    {State, Actions} = lists:foldr(fun handle_op/2,
                                   {start_batch(State0), Actions0}, Ops),
    %% process all ops
    {ok, Actions, complete_batch(State)}.

terminate(Reason, State) ->
    ?DEBUG("wal: terminating with ~W", [Reason, 20]),
    _ = cleanup(State),
    ok.

format_status(#state{conf = #conf{write_strategy = Strat,
                                  sync_method = SyncMeth,
                                  compute_checksums = Cs,
                                  names = #{wal := WalName},
                                  max_size_bytes = MaxSize},
                     writers = Writers,
                     wal = #wal{file_size = FSize,
                                filename = Fn}}) ->
    #{write_strategy => Strat,
      sync_method => SyncMeth,
      compute_checksums => Cs,
      writers => maps:size(Writers),
      filename => filename:basename(Fn),
      current_size => FSize,
      max_size_bytes => MaxSize,
      counters => ra_counters:overview(WalName)
     }.

%% Internal

handle_op({cast, WalCmd}, {State, Actions}) ->
    {handle_msg(WalCmd, State), Actions};
handle_op({call, From, {last_writer_seq, UId}},
           {#state{writers = Writers} = State, Actions}) ->
    {_, Res} = maps:get(UId, Writers, {undefined, undefined}),
    {State, [{reply, From, Res} | Actions]};
handle_op({info, {'EXIT', _, Reason}}, _State) ->
    %% this is here for testing purposes only
    throw({stop, Reason}).

recover_wal(Dir, #conf{segment_writer = SegWriter,
                       mem_tables_tid = MemTblsTid} = Conf) ->
    % ensure configured directory exists
    ok = ra_lib:make_dir(Dir),

    %% TODO: provede a proper ra_log_ets API to discover recovery mode
    Mode = case ets:info(MemTblsTid, size) of
               0 ->
                   %% there are no mem tables
                   initial;
               _ ->
                   %% in this case it is possible that the segment writer
                   %% could be flushing wal data right now so we need to
                   %% wait for the segment writer to finish the current work
                   %% before we get the wal files to recover
                   ok = ra_log_segment_writer:await(SegWriter),
                   post_boot
           end,
    {ok, Files0} = file:list_dir(Dir),
    Files = [begin
                 ra_lib:zpad_upgrade(Dir, File, ".wal")
             end || File <- Files0,
                    filename:extension(File) == ".wal"],
    WalFiles = lists:sort(Files),
    AllWriters =
        [begin
             ?DEBUG("wal: recovering ~ts, Mode ~s", [F, Mode]),
             WalFile = filename:join(Dir, F),
             Fd = open_at_first_record(WalFile),
             {Time, #recovery{ranges = Ranges,
                              writers = Writers}} =
                 timer:tc(fun () -> recover_wal_chunks(Conf, Fd, Mode) end),

             ok = ra_log_segment_writer:accept_mem_tables(SegWriter, Ranges, WalFile),

             close_existing(Fd),
             ?DEBUG("wal: recovered ~ts time taken ~bms - recovered ~b writers",
                    [F, Time div 1000, map_size(Writers)]),
             Writers
         end || F <- WalFiles],

    FinalWriters = lists:foldl(fun (New, Acc) ->
                                       maps:merge(Acc, New)
                               end, #{}, AllWriters),

    ?DEBUG("wal: recovered ~b writers", [map_size(FinalWriters)]),

    FileNum = extract_file_num(lists:reverse(WalFiles)),
    State = roll_over(#state{conf = Conf,
                             writers = FinalWriters,
                             file_num = FileNum}),
    true = erlang:garbage_collect(),
    State.

extract_file_num([]) ->
    0;
extract_file_num([F | _]) ->
    ra_lib:zpad_extract_num(filename:basename(F)).


cleanup(#state{wal = #wal{fd = undefined}}) ->
    ok;
cleanup(#state{wal = #wal{fd = Fd}}) ->
    _ = ra_file:sync(Fd),
    ok.

serialize_header(UId, Trunc, {Next, Cache} = WriterCache) ->
    case Cache of
        #{UId := <<_:1, BinId:23/bitstring>>} when Trunc ->
            {<<1:1/unsigned, BinId/bitstring>>, 2, WriterCache};
        #{UId := BinId} ->
            {BinId, 3, WriterCache};
        _ ->
            % TODO: check overflows of Next
            % cache the header index binary to avoid re-creating it every time
            % sets Truncate  = false initially as this is the most common case
            T = case Trunc of true -> 1; false -> 0 end,
            BinId = <<0:1/unsigned, 1:1/unsigned, Next:22/unsigned>>,
            IdDataLen = byte_size(UId),
            Header = <<T:1/unsigned, 0:1/unsigned, Next:22/unsigned,
                       IdDataLen:16/unsigned, UId/binary>>,
            {Header, byte_size(Header),
             {Next + 1, Cache#{UId => BinId}}}
    end.

write_data({UId, Pid} = Id, MtTid, Idx, Term, Data0, Trunc, SnapIdx,
           #state{conf = #conf{counter = Counter,
                               compute_checksums = ComputeChecksum} = _Cfg,
                  batch = Batch0,
                  writers = Writers,
                  wal = #wal{writer_name_cache = Cache0,
                             file_size = FileSize,
                             entry_count = Count} = Wal} = State0) ->
    % if the next write is going to exceed the configured max wal size
    % we roll over to a new wal.
    case should_roll_wal(State0) of
        true ->
            State = complete_batch_and_roll(State0),
            write_data(Id, MtTid, Idx, Term, Data0, Trunc, SnapIdx, State);
        false ->
            EntryData = case Data0 of
                            {ttb, Bin} ->
                                Bin;
                            _ ->
                                to_binary(Data0)
                        end,
            EntryDataLen = iolist_size(EntryData),
            {HeaderData, HeaderLen, Cache} = serialize_header(UId, Trunc, Cache0),
            % fixed overhead =
            % 24 bytes 2 * 64bit ints (idx, term) + 2 * 32 bit ints (checksum, datalen)
            DataSize = HeaderLen + 24 + EntryDataLen,

            Entry = [<<Idx:64/unsigned,
                       Term:64/unsigned>> |
                     EntryData],
            Checksum = case ComputeChecksum of
                           true -> erlang:adler32(Entry);
                           false -> 0
                       end,
            Record = [HeaderData,
                      <<Checksum:32/integer, EntryDataLen:32/unsigned>> |
                      Entry],
            Batch = incr_batch(Batch0, UId, Pid, MtTid,
                               Idx, Term, Record, SnapIdx),
            counters:add(Counter, ?C_BYTES_WRITTEN, DataSize),
            State0#state{batch = Batch,
                         wal = Wal#wal{writer_name_cache = Cache,
                                       file_size = FileSize + DataSize,
                                       entry_count = Count + 1},
                         writers = Writers#{UId => {in_seq, Idx}}}
    end.


handle_msg({append, {UId, Pid} = Id, MtTid, Idx, Term, Entry},
           #state{conf = Conf,
                  writers = Writers} = State0) ->
    SnapIdx = snap_idx(Conf, UId),
    %% detect if truncating flag should be set
    Trunc = Idx == SnapIdx + 1,
    case maps:find(UId, Writers) of
        _ when Idx =< SnapIdx ->
            %% a snapshot already exists that is higher - just drop the write
            State0#state{writers = Writers#{UId => {in_seq, SnapIdx}}};
        {ok, {_, PrevIdx}}
          when Idx =< PrevIdx + 1 orelse
               Trunc ->
            write_data(Id, MtTid, Idx, Term, Entry, Trunc, SnapIdx, State0);
        error ->
            write_data(Id, MtTid, Idx, Term, Entry, false, SnapIdx, State0);
        {ok, {out_of_seq, _}} ->
            % writer is out of seq simply ignore drop the write
            % TODO: capture metric for dropped writes?
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
handle_msg({truncate, Id, MtTid, Idx, Term, Entry},
           #state{conf = Conf} = State0) ->
    SnapIdx = snap_idx(Conf, Id),
    write_data(Id, MtTid, Idx, Term, Entry, true, SnapIdx, State0);
handle_msg({query, Fun}, State) ->
    %% for testing
    _ = catch Fun(State),
    State;
handle_msg(rollover, State) ->
    complete_batch_and_roll(State).

incr_batch(#batch{num_writes = Writes,
                  waiting = Waiting0,
                  pending = Pend} = Batch,
           UId, Pid, MT_TID = MtTid,
           Idx, TERM = Term, Data, SnapIdx) ->
    Waiting = case Waiting0 of
                  #{Pid := #batch_writer{term = TERM,
                                         tid = MT_TID,
                                         range = Range0
                                        } = W} ->
                      %% The Tid and term is the same so add to current batch_writer
                      Range = ra_range:extend(Idx, ra_range:truncate(SnapIdx, Range0)),
                      Waiting0#{Pid => W#batch_writer{range = Range,
                                                      snap_idx = SnapIdx,
                                                      term = Term
                                                     }};
                  _ ->
                      %% The tid is different, open a new batch writer for the
                      %% new tid and term
                      PrevBatchWriter = maps:get(Pid, Waiting0, undefined),
                      Writer = #batch_writer{snap_idx = SnapIdx,
                                             tid = MtTid,
                                             range = ra_range:new(Idx),
                                             uid = UId,
                                             term = Term,
                                             old = PrevBatchWriter
                                            },
                      Waiting0#{Pid => Writer}
              end,

    Batch#batch{num_writes = Writes + 1,
                waiting = Waiting,
                pending = [Pend | Data]}.

complete_batch_and_roll(#state{} = State0) ->
    State = complete_batch(State0),
    roll_over(start_batch(State)).

roll_over(#state{wal = Wal0, file_num = Num0,
                 conf = #conf{dir = Dir,
                              segment_writer = SegWriter,
                              max_size_bytes = MaxBytes} = Conf0} = State0) ->
    counters:add(Conf0#conf.counter, ?C_WAL_FILES, 1),
    Num = Num0 + 1,
    Fn = ra_lib:zpad_filename("", "wal", Num),
    NextFile = filename:join(Dir, Fn),
    ?DEBUG("wal: opening new file ~ts", [Fn]),
    %% if this is the first wal since restart randomise the first
    %% max wal size to reduce the likelihood that each erlang node will
    %% flush mem tables at the same time
    NextMaxBytes = case Wal0 of
                       undefined ->
                           Half = MaxBytes div 2,
                           Half + rand:uniform(Half);
                       #wal{ranges = Ranges,
                            filename = Filename} ->
                           _ = file:advise(Wal0#wal.fd, 0, 0, dont_need),
                           ok = close_file(Wal0#wal.fd),
                           MemTables = Ranges,
                           %% TODO: only keep base name in state
                           ok = ra_log_segment_writer:accept_mem_tables(SegWriter,
                                                                        MemTables,
                                                                        Filename),
                           MaxBytes
                   end,
    {Conf, Wal} = open_wal(NextFile, NextMaxBytes, Conf0),
    State0#state{conf = Conf,
                 wal = Wal,
                 file_num = Num}.

open_wal(File, Max, #conf{write_strategy = o_sync} = Conf) ->
        Modes = [sync | ?FILE_MODES],
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
open_wal(File, Max, #conf{} = Conf0) ->
    {ok, Fd} = prepare_file(File, ?FILE_MODES),
    Conf = maybe_pre_allocate(Conf0, Fd, Max),
    {Conf, #wal{fd = Fd,
                max_size = Max,
                filename = File}}.

prepare_file(File, Modes) ->
    Tmp = make_tmp(File),
    %% rename is atomic-ish so we will never accidentally write an empty wal file
    %% using prim_file here as file:rename/2 uses the file server
    ok = prim_file:rename(Tmp, File),
    case file:open(File, Modes) of
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
    ok = ra_file:sync(Fd),
    ok = file:close(Fd),
    Tmp.

maybe_pre_allocate(#conf{pre_allocate = true,
                         write_strategy = Strat} = Conf, Fd, Max0)
  when Strat /= o_sync ->
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
            Conf#conf{pre_allocate = false}
    end;
maybe_pre_allocate(Conf, _Fd, _Max) ->
    Conf.

close_file(undefined) ->
    ok;
close_file(Fd) ->
    file:close(Fd).

start_batch(#state{conf = #conf{counter = CRef}} = State) ->
    ok = counters:add(CRef, ?C_BATCHES, 1),
    State#state{batch = #batch{}}.


post_notify_flush(#state{wal = #wal{fd = Fd},
                         conf = #conf{write_strategy = sync_after_notify,
                                      sync_method = SyncMeth}}) ->
    sync(Fd, SyncMeth);
post_notify_flush(_State) ->
    ok.

flush_pending(#state{wal = #wal{fd = Fd},
                     batch = #batch{pending = Pend},
                     conf = #conf{write_strategy = WriteStrategy,
                                  sync_method = SyncMeth}} = State0) ->

    case WriteStrategy of
        default ->
            ok = file:write(Fd, Pend),
            sync(Fd, SyncMeth);
        _ ->
            ok = file:write(Fd, Pend)
    end,
    State0#state{batch = undefined}.

sync(_Fd, none) ->
    ok;
sync(Fd, Meth) ->
    ok = file:Meth(Fd),
    ok.

complete_batch(#state{batch = undefined} = State) ->
    State;
complete_batch(#state{batch = #batch{waiting = Waiting,
                                     num_writes = NumWrites},
                      wal = Wal,
                      conf = Cfg} = State0) ->
    % TS = erlang:system_time(microsecond),
    State = flush_pending(State0),
    % SyncTS = erlang:system_time(microsecond),
    counters:add(Cfg#conf.counter, ?C_WRITES, NumWrites),

    %% process writers
    Ranges = maps:fold(fun (Pid, BatchWriter, Acc) ->
                               complete_batch_writer(Pid, BatchWriter, Acc)
                       end, Wal#wal.ranges, Waiting),
    ok = post_notify_flush(State),
    State#state{wal = Wal#wal{ranges = Ranges}}.

complete_batch_writer(Pid, #batch_writer{snap_idx = SnapIdx,
                         tid = MtTid,
                         uid = UId,
                         range = Range,
                         term = Term,
                         old = undefined
                        }, Ranges) ->
    Pid ! {ra_log_event, {written, Term, Range}},
    update_ranges(Ranges, UId, MtTid, SnapIdx, Range);
complete_batch_writer(Pid, #batch_writer{old = #batch_writer{} = OldBw} = Bw,
                      Ranges0) ->
    Ranges = complete_batch_writer(Pid, OldBw, Ranges0),
    complete_batch_writer(Pid, Bw#batch_writer{old = undefined}, Ranges).

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

recover_wal_chunks(#conf{} = Conf, Fd, Mode) ->
    Chunk = read_wal_chunk(Fd, Conf#conf.recovery_chunk_size),
    recover_records(Conf, Fd, Chunk, #{}, #recovery{mode = Mode}).
% All zeros indicates end of a pre-allocated wal file
recover_records(_, _Fd, <<0:1/unsigned, 0:1/unsigned, 0:22/unsigned,
                          IdDataLen:16/unsigned, _:IdDataLen/binary,
                          0:32/integer, 0:32/unsigned, _/binary>>,
                _Cache, State) ->
    State;
% First encounter of UId in this file
recover_records(#conf{names = Names} = Conf, Fd,
                <<Trunc:1/unsigned, 0:1/unsigned, IdRef:22/unsigned,
                  IdDataLen:16/unsigned, UId0:IdDataLen/binary,
                  Checksum:32/integer,
                  EntryDataLen:32/unsigned,
                  Idx:64/unsigned, Term:64/unsigned,
                  EntryData:EntryDataLen/binary,
                  Rest/binary>> = Chunk,
                Cache0, State0) ->
    UId = binary:copy(UId0),
    case ra_directory:is_registered_uid(Names, UId) of
        true ->
            Cache = Cache0#{IdRef => {UId, <<1:1/unsigned, IdRef:22/unsigned>>}},
            SnapIdx = recover_snap_idx(Conf, UId, Trunc == 1, Idx),
            case validate_checksum(Checksum, Idx, Term, EntryData) of
                ok when Idx > SnapIdx ->
                    State1 = handle_trunc(Trunc == 1, UId, Idx, State0),
                    case recover_entry(Names, UId,
                                       {Idx, Term, binary_to_term(EntryData)},
                                       SnapIdx, State1) of
                        {ok, State} ->
                            recover_records(Conf, Fd, Rest, Cache, State);
                        {retry, State} ->
                            recover_records(Conf, Fd, Chunk, Cache, State)
                    end;
                ok ->
                    %% best the the snapshot index as the last
                    %% writer index
                    Writers = case State0#recovery.writers of
                                  #{UId := {in_seq, SnapIdx}} = W ->
                                      W;
                                  W ->
                                      W#{UId => {in_seq, SnapIdx}}
                              end,
                    recover_records(Conf, Fd, Rest, Cache,
                                    State0#recovery{writers = Writers});
                error ->
                    ?DEBUG("WAL: record failed CRC check. If this is the last record"
                           " recovery can resume", []),
                    %% if this is the last entry in the wal we can just drop the
                    %% record;
                    ok = is_last_record(Fd, Rest),
                    State0
            end;
        false ->
            recover_records(Conf, Fd, Rest, Cache0, State0)
    end;
recover_records(#conf{names = Names} = Conf, Fd,
                <<Trunc:1/unsigned, 1:1/unsigned, IdRef:22/unsigned,
                  Checksum:32/integer,
                  EntryDataLen:32/unsigned,
                  Idx:64/unsigned, Term:64/unsigned,
                  EntryData:EntryDataLen/binary,
                  Rest/binary>> = Chunk,
                Cache, State0) ->
    case Cache of
        #{IdRef := {UId, _}} ->
            SnapIdx = recover_snap_idx(Conf, UId, Trunc == 1, Idx),
            case validate_checksum(Checksum, Idx, Term, EntryData) of
                ok when Idx > SnapIdx ->
                    State1 = handle_trunc(Trunc == 1, UId, Idx, State0),
                    case recover_entry(Names, UId,
                                       {Idx, Term, binary_to_term(EntryData)},
                                       SnapIdx, State1) of
                        {ok, State} ->
                            recover_records(Conf, Fd, Rest, Cache, State);
                        {retry, State} ->
                            recover_records(Conf, Fd, Chunk, Cache, State)
                    end;
                ok ->
                    recover_records(Conf, Fd, Rest, Cache, State0);
                error ->
                    ?DEBUG("WAL: record failed CRC check. If this is the last record"
                           " recovery can resume", []),
                    %% if this is the last entry in the wal we can just drop the
                    %% record;
                    ok = is_last_record(Fd, Rest),
                    State0
            end;
        _ ->
            %% if the IdRef is not in the cache this refers to a deleted
            %% UId and we can just move on
            recover_records(Conf, Fd, Rest, Cache, State0)
    end;
recover_records(Conf, Fd, Chunk, Cache, State) ->
    % Not enough remainder to parse a whole record, need to read
    NextChunk = read_wal_chunk(Fd, Conf#conf.recovery_chunk_size),
    case NextChunk of
        <<>> ->
            %% we have reached the end of the file
            State;
        _ ->
            %% append this chunk to the remainder of the last chunk
            Chunk0 = <<Chunk/binary, NextChunk/binary>>,
            recover_records(Conf, Fd, Chunk0, Cache, State)
    end.

recover_snap_idx(Conf, UId, Trunc, CurIdx) ->
    case Trunc of
        true ->
            max(CurIdx-1, snap_idx(Conf, UId));
        false ->
            snap_idx(Conf, UId)
    end.

is_last_record(_Fd, <<0:104, _/binary>>) ->
    ok;
is_last_record(Fd, Rest) ->
    case byte_size(Rest) < 13 of
        true ->
            case read_wal_chunk(Fd, 256) of
                <<>> ->
                    ok;
                Next ->
                    is_last_record(Fd, <<Rest/binary, Next/binary>>)
            end;
        false ->
            ?ERROR("WAL: record failed CRC check during recovery. "
                   "Unable to recover WAL data safely", []),
            throw(wal_checksum_validation_failure)

    end.

read_wal_chunk(Fd, Len) ->
    case file:read(Fd, Len) of
        {ok, <<Data/binary>>} ->
            Data;
        eof ->
            <<>>;
        {error, Reason} ->
            exit({could_not_read_wal_chunk, Reason})
    end.

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
            error
    end.

merge_conf_defaults(Conf) ->
    maps:merge(#{segment_writer => ra_log_segment_writer,
                 max_size_bytes => ?WAL_DEFAULT_MAX_SIZE_BYTES,
                 max_entries => undefined,
                 recovery_chunk_size => ?WAL_RECOVERY_CHUNK_SIZE,
                 compute_checksums => true,
                 pre_allocate => false,
                 write_strategy => default,
                 garbage_collect => false,
                 sync_method => datasync,
                 min_bin_vheap_size => ?MIN_BIN_VHEAP_SIZE,
                 min_heap_size => ?MIN_HEAP_SIZE}, Conf).

to_binary(Term) ->
    term_to_iovec(Term).

should_roll_wal(#state{conf = #conf{max_entries = MaxEntries},
                       wal = #wal{max_size = MaxWalSize,
                                  file_size = FileSize,
                                  entry_count = Count}}) ->
    %% Initially, MaxWalSize was a hard limit for the file size: if FileSize +
    %% DataSize went over that limit, we would use a new file. This was an
    %% issue when DataSize was larger than the limit alone.
    %%
    %% The chosen solution is to only consider the current file size in the
    %% calculation. It means that after DataSize bytes are written, the file
    %% will be larger than the configured maximum size. But at least it will
    %% accept Ra commands larger than the max WAL size.
    FileSize > MaxWalSize orelse case MaxEntries of
                                     undefined -> false;
                                     _ ->
                                         Count + 1 > MaxEntries
                                 end.

snap_idx(#conf{ra_log_snapshot_state_tid = Tid}, ServerUId) ->
    ets:lookup_element(Tid, ServerUId, 2, -1).

update_ranges(Ranges, UId, MtTid, SnapIdx, {Start, _} = AddRange) ->
    case Ranges of
        #{UId := [{MtTid, Range0} | Rem]} ->
            %% SnapIdx might have moved to we truncate the old range first
            %% before extending
            Range1 = ra_range:truncate(SnapIdx, Range0),
            %% limit the old range by the add end start as in some resend
            %% cases we may have got back before the prior range.
            Range = ra_range:add(AddRange, ra_range:limit(Start, Range1)),
            Ranges#{UId => [{MtTid, Range} | Rem]};
        #{UId := [{OldMtTid, OldMtRange} | Rem]} ->
            %% new Tid, need to add a new range record for this
            Ranges#{UId => [{MtTid, AddRange},
                            ra_range:truncate(SnapIdx, {OldMtTid, OldMtRange})
                            | Rem]};
        _ ->
            Ranges#{UId => [{MtTid, AddRange}]}
    end.

recover_entry(Names, UId, {Idx, _, _} = Entry, SnapIdx,
              #recovery{mode = initial,
                        ranges = Ranges0,
                        writers = Writers,
                        tables = Tables} = State) ->
    Mt0 = case Tables of
              #{UId := M} -> M;
              _ ->
                  {ok, M} = ra_log_ets:mem_table_please(Names, UId),
                  M
          end,
    case ra_mt:insert(Entry, Mt0) of
        {ok, Mt1} ->
            Ranges = update_ranges(Ranges0, UId, ra_mt:tid(Mt1),
                                   SnapIdx, ra_range:new(Idx)),
            {ok, State#recovery{ranges = Ranges,
                                writers = Writers#{UId => {in_seq, Idx}},
                                tables = Tables#{UId => Mt1}}};
        {error, overwriting} ->
            %% create successor memtable
            {ok, Mt1} = ra_log_ets:new_mem_table_please(Names, UId, Mt0),
            {retry, State#recovery{tables = Tables#{UId => Mt1}}}
    end;
recover_entry(Names, UId, {Idx, Term, _}, SnapIdx,
              #recovery{mode = post_boot,
                        ranges = Ranges0,
                        writers = Writers,
                        tables = Tables} = State) ->
    Mt0 = case Tables of
              #{UId := M} -> M;
              _ ->
                  {ok, M} = ra_log_ets:mem_table_please(Names, UId),
                  M
          end,
    %% find the tid for the given idxterm
    case ra_mt:tid_for(Idx, Term, Mt0) of
        undefined ->
            %% not found, this entry may already have been flushed
            %% skip, and reset ranges but update writers as we need to
            %% recover the last idx
            Ranges = maps:remove(UId, Ranges0),
            {ok, State#recovery{ranges = Ranges,
                                writers = Writers#{UId => {in_seq, Idx}},
                                tables = Tables#{UId => Mt0}}};
        Tid ->
            Ranges = update_ranges(Ranges0, UId, Tid,
                                   SnapIdx, ra_range:new(Idx)),
            {ok, State#recovery{ranges = Ranges,
                                writers = Writers#{UId => {in_seq, Idx}},
                                tables = Tables#{UId => Mt0}}}
    end.

handle_trunc(false, _UId, _Idx, State) ->
    State;
handle_trunc(true, UId, Idx, #recovery{mode = Mode,
                                       tables = Tbls} = State) ->
    case Tbls of
        #{UId := Mt0} when Mode == initial ->
            %% only meddle with mem table data in initial mode
            {Specs, Mt} = ra_mt:set_first(Idx-1, Mt0),
            [_ = ra_mt:delete(Spec) || Spec <- Specs],
            State#recovery{tables = Tbls#{UId => Mt}};
        _ ->
            State
    end.

named_cast(To, Msg) when is_pid(To) ->
    gen_batch_server:cast(To, Msg),
    {ok, To};
named_cast(Wal, Msg) ->
    case whereis(Wal) of
        undefined ->
            {error, wal_down};
        Pid ->
            named_cast(Pid, Msg)
    end.

