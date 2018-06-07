-module(ra_log_wal).

-export([start_link/2,
         write/5,
         truncate_write/5,
         force_roll_over/1,
         init/3,
         system_continue/3,
         system_terminate/4,
         write_debug/3]).

-compile([inline_list_funcs]).

-include("ra.hrl").

-define(MIN_MAX_BATCH_SIZE, 16).
-define(MAX_MAX_BATCH_SIZE, 16 * 128).
-define(METRICS_WINDOW_SIZE, 100).
-define(MAX_WAL_SIZE_BYTES, 1000 * 1000 * 128).
% maximum number of entries for any one writer before requesting flush to disk
-define(MAX_WRITER_ENTRIES_PER_WAL, 4096 * 4).

% a writer_id consists of a unqique local name (see ra_directory) and a writer's
% current pid().
% The pid is used for the immediate writer notification
% The atom is used by the segment writer to send the segments
% This has the effect that a restarted node has a different identity in terms
% of it's write notification but the same identity in terms of it's ets
% tables and segment notification
-type writer_id() :: {binary(), pid()}.

-record(batch, {writes = 0 :: non_neg_integer(),
                waiting = #{} :: #{writer_id() =>
                                   {From :: ra_index(), To :: ra_index()}},
                start_time :: maybe(integer()),
                pending = [] :: iolist()
               }).

-type wal_write_strategy() ::
    % delay writes until batch completion
    % reduces the number of syscalls at the expense of memory use
    delay_writes |
    % like delay writes but tries to open the file using synchronous io
    % (O_SYNC) rather than a write(2) followed by an fsync.
    delay_writes_sync |
    % each write calls write(2) and fsyncs at batch completion
    % Allows data to be gcd as soon as possible
    no_delay.

-type writer_name_cache() :: {NextIntId :: non_neg_integer(),
                              #{writer_id() => binary()}}.

-record(wal, {fd :: maybe(file:io_device()),
              filename :: maybe(file:filename()),
              writer_name_cache = {0, #{}} :: writer_name_cache(),
              wal_file_size = 0 :: non_neg_integer()}).

-record(state, {file_num = 0 :: non_neg_integer(),
                wal :: #wal{} | undefined,
                batch :: maybe(#batch{}),
                file_modes :: [term()],
                dir :: string(),
                max_batch_size = ?MIN_MAX_BATCH_SIZE :: non_neg_integer(),
                max_wal_size_bytes = ?MAX_WAL_SIZE_BYTES :: non_neg_integer(),
                segment_writer = ra_log_file_segment_writer :: atom(),
                % writers that have attempted to write an non-truncating
                % out of seq % entry.
                % No further writes are allowed until the missing
                % index has been received.
                % out_of_seq are kept after a roll over or until
                % a truncating write is received.
                % no attempt is made to recover this information after a crash
                % beyond the available WAL files
                % all writers seen withing the lifetime of a WAL file
                % and the last index seen
                writers = #{} :: #{writer_id() => {in_seq | out_of_seq, ra_index()}},
                metrics_cursor = 0 :: non_neg_integer(),
                compute_checksums = false :: boolean(),
                write_strategy = delay_writs :: wal_write_strategy()
               }).

-type state() :: #state{}.
-type wal_conf() :: #{dir => file:filename_all(),
                      max_wal_size_bytes => non_neg_integer(),
                      %TODO implement
                      max_writer_entries_per_wal => non_neg_integer(),
                      segment_writer => atom() | pid(),
                      compute_checksums => boolean(),
                      wal_strategy => wal_write_strategy()}.

-export_type([wal_conf/0,
              wal_write_strategy/0]).


-spec write(writer_id(), atom(), ra_index(), ra_term(), term()) ->
    ok | {error, wal_down}.
write(From, Wal, Idx, Term, Entry) ->
    send_write(Wal, {append, From, Idx, Term, Entry}).

-spec truncate_write(writer_id(), atom(), ra_index(), ra_term(), term()) ->
    ok | {error, wal_down}.
truncate_write(From, Wal, Idx, Term, Entry) ->
    send_write(Wal, {truncate, From, Idx, Term, Entry}).

% force a wal file to roll over to a new file
% mostly useful for testing
force_roll_over(Wal) ->
    Wal ! rollover,
    ok.

%% ra_log_wal
%%
%% Writes Raft entries to shared persistent storage for multiple "writers"
%% Fsyncs in batches, typically the write requests received in the mailbox during
%% the previous fsync operation. Notifies all writers after each fsync batch.
%% Also have got a dynamically increasing max writes limit that grows in order
%% to trade-off latency for throughput.
%%
%% Entries are written to the .wal file as well as a per-writer mem table (ETS).
%% In order for writers to locate an entry by an index a lookup ETS table
%% (ra_log_open_mem_tables) keeps the current range of indexes a mem_table as well
%% as the mem_table tid(). This lookup table is updated on every write.
%%
%% Once the current .wal file is full a new one is closed. All the entries in
%% ra_log_open_mem_tables are moved to ra_log_closed_mem_tables so that writers
%% can still locate the tables whilst they are being flushed ot disk. The
%% ra_log_file_segment_writer is notified of all the mem tables written to during
%% the lifetime of the .wal file and will begin writing these to on-disk segment
%% files. Once it has finished the current set of mem_tables it will delete the
%% corresponding .wal file.

-spec start_link(Config :: wal_conf(), Options :: list()) ->
    {ok, pid()} | {error, {already_started, pid()}}.
start_link(Config, Options) ->
    % this is racy
    case whereis(?MODULE) of
        undefined ->
            % ?INFO("WAL START_LINK", []),
            {ok, Pid} = proc_lib:start_link(?MODULE, init,
                                            [Config, self(), Options]),
            register(?MODULE, Pid),
            {ok, Pid};
        Pid ->
            {error, {already_started, Pid}}
    end.

-spec init(wal_conf(), pid(), list()) -> state().
init(#{dir := Dir} = Conf0, Parent, Options) ->
    Conf = merge_conf_defaults(Conf0),
    process_flag(trap_exit, true),
    % test that off_heap is actuall beneficial
    % given ra_log_wal is effectively a fan-in sink it is likely that it will
    % at times receive large number of messages from a large number of
    % writers
    process_flag(message_queue_data, off_heap),
    _ = ets:new(ra_log_wal_metrics,
                [set, named_table, {read_concurrency, true}, protected]),
    % seed metrics table with data
    [true = ets:insert(ra_log_wal_metrics, {I, undefined})
     || I <- lists:seq(0, ?METRICS_WINDOW_SIZE-1)],
    % wait for the segment writer to process anything in flight
    #{segment_writer := SegWriter} = Conf,
    ok = ra_log_file_segment_writer:await(SegWriter),

    Debug0 = sys:debug_options(Options),
    {State, Debug} = recover_wal(Dir, Conf, Debug0),
    ok = proc_lib:init_ack(Parent, {ok, self()}),
    loop_wait(State, Parent, Debug).


%% Internal

recover_wal(Dir, #{max_wal_size_bytes := MaxWalSize,
                   segment_writer := TblWriter,
                   compute_checksums := ComputeChecksum,
                   write_strategy := WriteStrategy},
           Dbg0) ->
    % ensure configured directory exists
    ok = filelib:ensure_dir(Dir),
    _ = file:make_dir(Dir),
    %  recover each mem table and notify segment writer
    %  this may result in duplicated segments but that is better than
    %  losing any data
    %  As we have waited for the segment writer to finish processing it is
    %  assumed that any remaining wal files need to be re-processed.
    WalFiles = lists:sort(filelib:wildcard(filename:join(Dir, "*.wal"))),
    ?INFO("WAL: recovering ~p", [WalFiles]),
    % First we recover all the tables using a temporary lookup table.
    % Then we update the actual lookup tables atomically.
    _ = ets:new(ra_log_recover_mem_tables,
                [set, named_table, {read_concurrency, true}, private]),
    % compute all closed mem table lookups required so we can insert them
    % all at once, atomically
    % It needs to be atomic so that readers don't accidentally read partially recovered
    % tables mixed with old tables
    All = [begin
               {ok, Data} = file:read_file(F),
               ok = recover_records(Data, #{}),
               recovering_to_closed(F)
           end || F <- WalFiles],
    % get all the recovered tables and insert them into closed
    Closed = lists:append([C || {C, _, _} <- All]),
    true = ets:insert(ra_log_closed_mem_tables, Closed),
    % send all the mem tables to segment writer for processing
    % This could result in duplicate segments
    [ok = ra_log_file_segment_writer:accept_mem_tables(TblWriter, M,F)
     || {_, M, F} <- All],

    Modes = [raw, append, binary],
    FileNum = extract_file_num(lists:reverse(WalFiles)),
    StateDbg = roll_over(ra_log_recover_mem_tables,
                         #state{dir = Dir,
                                file_num = FileNum,
                                file_modes = Modes,
                                max_wal_size_bytes = MaxWalSize,
                                compute_checksums = ComputeChecksum,
                                segment_writer = TblWriter,
                                write_strategy = WriteStrategy},
                         Dbg0),
    % we can now delete all open mem tables as should be covered by recovered
    % closed tables
    Open = ets:tab2list(ra_log_open_mem_tables),
    true = ets:delete_all_objects(ra_log_open_mem_tables),
    % delete all open ets tables
    [true = ets:delete(Tid) || {_, _, _, Tid} <- Open],
    true = ets:delete(ra_log_recover_mem_tables),
    StateDbg.

extract_file_num([]) ->
    0;
extract_file_num([F | _]) ->
    ra_lib:zpad_extract_num(filename:basename(F)).

loop_wait(State0, Parent, Debug0) ->
    receive
        {system, From, Request} ->
            sys:handle_system_msg(Request, From, Parent, ?MODULE, Debug0, State0);
        {'EXIT', Parent, Reason} ->
            cleanup(State0#state.wal),
            exit(Reason);
        Msg ->
            Debug1 = handle_debug_in(Debug0, Msg),
            {State, Debug} = handle_msg(Msg, State0, Debug1),
            loop_batched(State, Parent, Debug)
    end.

loop_batched(#state{max_batch_size = Written,
                    batch = #batch{writes = Written}} = State0,
             Parent, Debug0) ->
    % complete batch after seeing max_batch_size writes
    {State, Debug} = complete_batch(State0, Debug0),
    % grow max batch size
    NewBatchSize = min(?MAX_MAX_BATCH_SIZE, Written * 2),
    loop_wait(State#state{max_batch_size = NewBatchSize}, Parent, Debug);
loop_batched(State0, Parent, Debug0) ->
    receive
        {system, From, Request} ->
            sys:handle_system_msg(Request, From, Parent, ?MODULE, Debug0, State0);
        {'EXIT', Parent, Reason} ->
            cleanup(State0#state.wal),
            exit(Reason);
        Msg ->
            Debug1 = handle_debug_in(Debug0, Msg),
            {State, Debug} = handle_msg(Msg, State0, Debug1),
            loop_batched(State, Parent, Debug)
    after 0 ->
              {State, Debug} = complete_batch(State0, Debug0),
              NewBatchSize = max(?MIN_MAX_BATCH_SIZE,
                                 State0#state.max_batch_size / 2),
              loop_wait(State#state{max_batch_size = NewBatchSize},
                        Parent, Debug)
    end.

cleanup(#wal{fd = undefined}) ->
    ok;
cleanup(#wal{fd = Fd}) ->
    _ = file:sync(Fd),
    ok.

handle_debug_in(Debug, Msg) ->
    sys:handle_debug(Debug, fun write_debug/3,
                     ?MODULE, {in, Msg}).

serialize_header(Id, Trunc, {Next, Cache} = WriterCache) ->
    T = case Trunc of true -> 1; false -> 0 end,
    case Cache of
        #{Id := BinId} ->
            {<<T:1/integer, BinId/bitstring>>, 2, WriterCache};
        _ ->
            % TODO: check overflows of Next
            % cache the last 15 bits of the header word
            BinId = <<1:1/integer, Next:14/integer>>,
            IdData = term_to_binary(Id),
            IdDataLen = byte_size(IdData),
            MarkerId = <<T:1/integer, 0:1/integer, Next:14/integer,
                         IdDataLen:16/integer, IdData/binary>>,
            {MarkerId, byte_size(MarkerId), {Next+1, Cache#{Id => BinId}}}
    end.

write_data(Id, Idx, Term, Data0, Trunc,
           #state{batch = undefined} = State, Dbg) ->
    write_data(Id, Idx, Term, Data0, Trunc, start_batch(State), Dbg);
write_data(Id, Idx, Term, Data0, Trunc,
           #state{max_wal_size_bytes = MaxWalSize,
                  compute_checksums = ComputeChecksum,
                  wal = #wal{wal_file_size = FileSize,
                             writer_name_cache = Cache0} = Wal} = State00,
          Dbg0) ->
    EntryData = to_binary(Data0),
    EntryDataLen = byte_size(EntryData),
    {HeaderData, HeaderLen, Cache} = serialize_header(Id, Trunc, Cache0),
    % fixed overhead = 24 bytes 2 * 64bit ints (idx, term) + 2 * 32 bit ints (checksum, datalen)
    DataSize = HeaderLen + 24 + EntryDataLen,
    % if the next write is going to exceed the configured max wal size
    % we roll over to a new wal.
    case FileSize + DataSize > MaxWalSize of
        true ->
            {State, Dbg} = roll_over(State00, Dbg0),
            % TODO: there is some redundant computation performed by recursing here
            % it probably doesn't matter as it only happens when a wal file fills up
            write_data(Id, Idx, Term, Data0, Trunc, State, Dbg);
        false ->
            State0 = State00#state{wal = Wal#wal{writer_name_cache = Cache}},
            Entry = <<Idx:64/integer,
                      Term:64/integer,
                      EntryData/binary>>,
            Checksum = case ComputeChecksum of
                           true -> erlang:adler32(Entry);
                           false -> 0
                       end,
            Record = <<HeaderData/binary,
                       Checksum:32/integer,
                       EntryDataLen:32/integer,
                       Entry/binary>>,
            {append_data(State0, Id, Idx, Term, Data0, DataSize, Record, Trunc),
             Dbg0}
    end.

handle_msg({append, {_, Pid} = Id, Idx, Term, Entry},
           #state{writers = Writers} = State0,
          Dbg) ->
    case maps:find(Id, Writers) of
        {ok, {_, PrevIdx}} when Idx =< PrevIdx + 1 ->
            write_data(Id, Idx, Term, Entry, false, State0, Dbg);
        error ->
            write_data(Id, Idx, Term, Entry, false, State0, Dbg);
        {ok, {out_of_seq, _}} ->
            % writer is out of seq simply ignore drop the write
            % TODO: capture metric for dropped writes
            {State0, Dbg};
        {ok, {in_seq, PrevIdx}} ->
            % writer was in seq but has sent an out of seq entry
            % notify writer
            ?WARN("WAL: requesting resend from `~p`, last idx ~b idx received ~b",
                 [Id, PrevIdx, Idx]),
            Pid ! {ra_log_event, {resend_write, PrevIdx + 1}},
            {State0#state{writers = Writers#{Id => {out_of_seq, PrevIdx}}},
             Dbg}
    end;
handle_msg({truncate, Id, Idx, Term, Entry}, State0, Dbg) ->
    write_data(Id, Idx, Term, Entry, true, State0, Dbg);
handle_msg(rollover, State, Dbg) ->
    roll_over(State, Dbg).

append_data(#state{wal = #wal{fd = Fd,
                              wal_file_size = FileSize} = Wal,
                   batch = Batch,
                   writers = Writers,
                   write_strategy = WriteStrat} = State,
            Id, Idx, Term, Entry, DataSize, Data, Truncate) ->
    case WriteStrat of
        no_delay ->
            ok = file:write(Fd, Data);
        _ ->
            ok
    end,
    true = update_mem_table(ra_log_open_mem_tables, Id, Idx, Term, Entry,
                            Truncate),
    State#state{wal = Wal#wal{wal_file_size = FileSize + DataSize},
                batch = incr_batch(Batch, Id, {Idx, Term}, Data),
                writers = Writers#{Id => {in_seq, Idx}} }.

update_mem_table(OpnMemTbl, {UId, _}, Idx, Term, Entry, Truncate) ->
    % TODO: if Idx =< First we could truncate the entire table and safe
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
            Tid = open_mem_table(UId),
            true = ets:insert(OpnMemTbl, {UId, Idx, Idx, Tid}),
            true = ets:insert(Tid, {Idx, Term, Entry})
    end.

roll_over(State0, Dbg) ->
    {State, Dbg} = complete_batch(State0, Dbg),
    roll_over(ra_log_open_mem_tables, State, Dbg).

roll_over(OpnMemTbls, #state{wal = Wal0, dir = Dir, file_num = Num0,
                             segment_writer = SegWriter} = State0, Dbg) ->
    Num = Num0 + 1,
    Fn = ra_lib:zpad_filename("", "wal", Num),
    NextFile = filename:join(Dir, Fn),
    ?INFO("wal: opening new file ~p~n", [Fn]),
    case Wal0 of
        undefined ->
            ok;
        Wal ->
            ok = close_file(Wal#wal.fd),
            ok = close_open_mem_tables(OpnMemTbls, Wal#wal.filename, SegWriter)
    end,
    State = open_file(NextFile, State0),
    {State#state{file_num = Num}, Dbg}.

open_file(File, #state{write_strategy = delay_writes_sync,
                       file_modes = Modes0} = State) ->
        Modes = [sync | Modes0],
        case file:open(File, Modes) of
            {ok, Fd} ->
                % many platforms implement O_SYNC a bit like O_DSYNC
                % perform a manual sync here to ensure metadata is flushed
                ok = file:sync(Fd),
                State#state{file_modes = Modes,
                            wal = #wal{fd = Fd, filename = File}};
            {error, enotsup} ->
                ?WARN("WAL: delay_writes_sync not supported. "
                      "Reverting back to delay_writes strategy.", []),
                open_file(File, State#state{write_strategy = delay_writes})
        end;
open_file(File, #state{file_modes = Modes} = State) ->
    {ok, Fd} = file:open(File, Modes),
    State#state{file_modes = Modes, wal = #wal{fd = Fd, filename = File}}.

close_file(undefined) ->
    ok;
close_file(Fd) ->
    ok = file:sync(Fd),
    file:close(Fd).

close_open_mem_tables(OpnMemTbls, Filename, TblWriter) ->
    MemTables = ets:tab2list(OpnMemTbls),
    % insert into closed mem tables
    % so that readers can still resolve the table whilst it is being
    % flushed to persistent tables asynchronously
    [begin
         % In order to ensure that reads are done in the correct causal order
         % we need to append a monotonically increasing value for readers to
         % sort by
         M = erlang:unique_integer([monotonic, positive]),
         _ = ets:insert(ra_log_closed_mem_tables,
                        erlang:insert_element(2, T, M))
     end || T <- MemTables],
    % reset open mem tables table
    true = ets:delete_all_objects(OpnMemTbls),

    % notify segment_writer of new unflushed memtables
    ok = ra_log_file_segment_writer:accept_mem_tables(TblWriter, MemTables,
                                                      Filename),
    ok.

recovering_to_closed(Filename) ->
    MemTables = ets:tab2list(ra_log_recover_mem_tables),
    Closed = [begin
                  M = erlang:unique_integer([monotonic, positive]),
                  erlang:insert_element(2, T, M)
              end || T <- MemTables],
    true = ets:delete_all_objects(ra_log_recover_mem_tables),
    {Closed, MemTables, Filename}.


open_mem_table({UId, _Pid}) ->
    open_mem_table(UId);
open_mem_table(UId) ->
    % lookup the locally registered name of the process to use as ets
    % name
    NodeName = ra_directory:what_node(UId),
    Tid = ets:new(NodeName, [set, {read_concurrency, true}, public]),
    % immediately give away ownership to ets process
    true = ra_log_file_ets:give_away(Tid),
    Tid.

start_batch(State) ->
    State#state{batch = #batch{start_time = os:system_time(millisecond)}}.

complete_batch(#state{batch = undefined} = State, Debug) ->
    {State, Debug};
complete_batch(#state{wal = #wal{fd = Fd},
                      batch = #batch{waiting = Waiting,
                                     writes = NumWrites,
                                     start_time = ST,
                                     pending = Pend},
                      metrics_cursor = Cursor,
                      write_strategy = WriteStrat} = State0,
               Debug0) ->
    TS = os:system_time(millisecond),
    case WriteStrat of
        delay_writes_sync ->
            ok = file:write(Fd, lists:reverse(Pend));
        delay_writes ->
            ok = file:write(Fd, lists:reverse(Pend)),
            ok = file:sync(Fd);
        no_delay ->
            ok = file:sync(Fd)
    end,

    SyncTS = os:system_time(millisecond),
    _ = ets:update_element(ra_log_wal_metrics, Cursor,
                           {2, {NumWrites, TS-ST, SyncTS-TS}}),
    NextCursor = (Cursor + 1) rem ?METRICS_WINDOW_SIZE,
    State = State0#state{metrics_cursor = NextCursor,
                         batch = undefined},

    % notify processes that have synced map(Pid, Token)
    Debug = maps:fold(fun ({_, Pid}, WrittenInfo, Dbg) ->
                              Msg = {ra_log_event, {written, WrittenInfo}},
                              % As we now use a pid it is no longer
                              % possible to know if the send failed.
                              % Ah well...
                              Pid ! Msg,
                              Evt = {out, {self(), Msg}, Pid},
                              sys:handle_debug(Dbg, fun write_debug/3,
                                               ?MODULE, Evt)
                      end, Debug0, Waiting),
    {State, Debug}.

incr_batch(#batch{writes = Writes,
                  waiting = Waiting0,
                  pending = Pend} = Batch, Id, {Idx, Term}, Data) ->
    Waiting = maps:update_with(Id, fun ({From, _, _}) ->
                                           {min(Idx, From), Idx, Term}
                                   end, {Idx, Idx, Term}, Waiting0),
    Batch#batch{writes = Writes + 1,
                waiting = Waiting,
                pending = [Data | Pend]}.

recover_records(<<Trunc:1/integer, 0:1/integer, IdRef:14/integer,
                  IdDataLen:16/integer, IdData:IdDataLen/binary,
                  Checksum:32/integer,
                  EntryDataLen:32/integer,
                  Idx:64/integer, Term:64/integer,
                  EntryData:EntryDataLen/binary,
                  Rest/binary>>, Cache) ->
    % first writer appearance in WAL
    validate_checksum(Checksum, Idx, Term, EntryData),
    Id = binary_to_term(IdData),
    true = update_mem_table(ra_log_recover_mem_tables, Id, Idx, Term,
                            binary_to_term(EntryData), Trunc =:= 1),
    % TODO: recover writers info, i.e. last index seen
    recover_records(Rest,
                    Cache#{IdRef => {Id, <<1:1/integer, IdRef:14/integer>>}});
recover_records(<<Trunc:1/integer, 1:1/integer, IdRef:14/integer,
                  Checksum:32/integer,
                  EntryDataLen:32/integer,
                  Idx:64/integer, Term:64/integer,
                  EntryData:EntryDataLen/binary,
                  Rest/binary>>, Cache) ->
    #{IdRef := {Id, _}} = Cache,

    validate_checksum(Checksum, Idx, Term, EntryData),

    true = update_mem_table(ra_log_recover_mem_tables, Id, Idx, Term,
                            binary_to_term(EntryData), Trunc =:= 1),
    % TODO: recover writers info, i.e. last index seen
    recover_records(Rest, Cache);
recover_records(<<>>, _Cache) ->
    ok.

validate_checksum(0, _Idx, _Term, _Data) ->
    % checksum not used
    ok;
validate_checksum(Checksum, Idx, Term, Data) ->
    % building a binary just for the checksum may feel a bit wasteful
    % but this is only called during recovery which should be a rare event
    case erlang:adler32(<<Idx:64/integer, Term:64/integer, Data/binary>>) of
        Checksum ->
            ok;
        _ ->
            exit(wal_checksum_validation_failure)
    end.

send_write(Wal, Msg) ->
    try
        Wal ! Msg,
        ok
    catch
        error:badarg ->
            % wal name lookup failed
            {error, wal_down}
    end.


merge_conf_defaults(Conf) ->
    maps:merge(#{segment_writer => ra_log_file_segment_writer,
                 max_wal_size_bytes => ?MAX_WAL_SIZE_BYTES,
                 max_writer_entries_per_wal => ?MAX_WRITER_ENTRIES_PER_WAL,
                 compute_checksums => true,
                 write_strategy => delay_writes},
               Conf).

to_binary(Term) ->
    term_to_binary(Term).

%% Here are the sys call back functions

system_continue(Parent, Debug, State) ->
    % TODO check if we've written to the current batch or not
    loop_batched(State, Parent, Debug).

system_terminate(Reason, _Parent, _Debug, State) ->
    cleanup(State#state.wal),
    exit(Reason).

write_debug(Dev, Event, Name) ->
    io:format(Dev, "~p event = ~p~n", [Name, Event]).
