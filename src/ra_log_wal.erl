%% @hidden
-module(ra_log_wal).
-behaviour(gen_batch_server).

-export([start_link/2,
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

-include("ra.hrl").

-define(MAX_SIZE_BYTES, 512 * 1000 * 1000).
-define(METRICS_WINDOW_SIZE, 100).
-define(CURRENT_VERSION, 1).
-define(MAGIC, "RAWA").

% a writer_id consists of a unqique local name (see ra_directory) and a writer's
% current pid().
% The pid is used for the immediate writer notification
% The atom is used by the segment writer to send the segments
% This has the effect that a restarted server has a different identity in terms
% of it's write notification but the same identity in terms of it's ets
% tables and segment notification
-type writer_id() :: {binary(), pid()}.

-record(batch, {writes = 0 :: non_neg_integer(),
                waiting = #{} :: #{pid() =>
                                   {From :: ra_index(), To :: ra_index(),
                                    Term :: ra_term()}},
                start_time :: maybe(integer()),
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

-record(wal, {fd :: maybe(file:io_device()),
              filename :: maybe(file:filename()),
              writer_name_cache = {0, #{}} :: writer_name_cache(),
              file_size = 0 :: non_neg_integer()}).

-record(state, {file_num = 0 :: non_neg_integer(),
                wal :: #wal{} | undefined,
                file_modes :: [term()],
                dir :: string(),
                segment_writer = ra_log_segment_writer :: atom(),
                max_size_bytes = ?MAX_SIZE_BYTES :: non_neg_integer(),
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
                writers = #{} :: #{ra_uid() =>
                                   {in_seq | out_of_seq, ra_index()}},
                metrics_cursor = 0 :: non_neg_integer(),
                compute_checksums = false :: boolean(),
                write_strategy = default :: wal_write_strategy(),
                batch :: maybe(#batch{})
               }).

-type state() :: #state{}.
-type wal_conf() :: #{dir => file:filename_all(),
                      max_size_bytes => non_neg_integer(),
                      segment_writer => atom() | pid(),
                      compute_checksums => boolean(),
                      write_strategy => wal_write_strategy()}.

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

-spec start_link(Config :: wal_conf(), Options :: list()) ->
    {ok, pid()} | {error, {already_started, pid()}}.
start_link(Config, Options) ->
    gen_batch_server:start_link({local, ?MODULE}, ?MODULE, Config, Options).

%%% Callbacks

-spec init(wal_conf()) -> {ok, state()}.
init(#{dir := Dir} = Conf0) ->
    Conf = merge_conf_defaults(Conf0),
    process_flag(trap_exit, true),
    % TODO: test that off_heap is actuall beneficial
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
    ok = ra_log_segment_writer:await(SegWriter),
    %% TODO: recover wal shoudl return {stop, Reason} if it fails
    %% rather than crash
    {ok, recover_wal(Dir, Conf)}.

-spec handle_batch([wal_op()], state()) ->
    {ok, state()}.
handle_batch(Ops, State0) ->
    State = lists:foldl(fun handle_op/2, start_batch(State0), Ops),
    %% process all ops
    complete_batch(State).

terminate(_Reason, State) ->
    _ = cleanup(State),
    ok.

format_status(#state{write_strategy = Strat,
                     compute_checksums = Cs,
                     max_size_bytes = MaxSize,
                     writers = Writers,
                     wal = #wal{file_size = FSize,
                                filename = Fn}}) ->
    #{write_strategy => Strat,
      compute_checksums => Cs,
      writers => maps:size(Writers),
      filename => filename:basename(Fn),
      current_size => FSize,
      max_size_bytes => MaxSize}.

%% Internal

handle_op({cast, WalCmd}, State) ->
    handle_msg(WalCmd, State).

recover_wal(Dir, #{max_size_bytes := MaxWalSize,
                   segment_writer := TblWriter,
                   compute_checksums := ComputeChecksum,
                   write_strategy := WriteStrategy}) ->
    % ensure configured directory exists
    ok = ra_lib:make_dir(Dir),
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
    % It needs to be atomic so that readers don't accidentally
    % read partially recovered
    % tables mixed with old tables
    All = [begin
               Data = open_existing(F),
               ok = try_recover_records(Data, #{}),
               recovering_to_closed(F)
           end || F <- WalFiles],
    % get all the recovered tables and insert them into closed
    Closed = lists:append([C || {C, _, _} <- All]),
    true = ets:insert(ra_log_closed_mem_tables, Closed),
    % send all the mem tables to segment writer for processing
    % This could result in duplicate segments
    [ok = ra_log_segment_writer:accept_mem_tables(TblWriter, M, F)
     || {_, M, F} <- All],

    Modes = [raw, append, binary],
    FileNum = extract_file_num(lists:reverse(WalFiles)),
    State = roll_over(ra_log_recover_mem_tables,
                      #state{dir = Dir,
                             file_num = FileNum,
                             file_modes = Modes,
                             max_size_bytes = MaxWalSize,
                             compute_checksums = ComputeChecksum,
                             segment_writer = TblWriter,
                             write_strategy = WriteStrategy}),
    % we can now delete all open mem tables as should be covered by recovered
    % closed tables
    Open = ets:tab2list(ra_log_open_mem_tables),
    true = ets:delete_all_objects(ra_log_open_mem_tables),
    % delete all open ets tables
    [true = ets:delete(Tid) || {_, _, _, Tid} <- Open],
    true = ets:delete(ra_log_recover_mem_tables),
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
           #state{max_size_bytes = MaxWalSize,
                  compute_checksums = ComputeChecksum,
                  wal = #wal{file_size = FileSize,
                             writer_name_cache = Cache0} = Wal} = State00) ->
    EntryData = to_binary(Data0),
    EntryDataLen = byte_size(EntryData),
    {HeaderData, HeaderLen, Cache} = serialize_header(UId, Trunc, Cache0),
    % fixed overhead =
    % 24 bytes 2 * 64bit ints (idx, term) + 2 * 32 bit ints (checksum, datalen)
    DataSize = HeaderLen + 24 + EntryDataLen,
    % if the next write is going to exceed the configured max wal size
    % we roll over to a new wal.
    case FileSize + DataSize > MaxWalSize of
        true ->
            State = roll_over(State00),
            % TODO: there is some redundant computation performed by
            % recursing here it probably doesn't matter as it only happens
            % when a wal file fills up
            write_data(Id, Idx, Term, Data0, Trunc, State);
        false ->
            State0 = State00#state{wal = Wal#wal{writer_name_cache = Cache}},
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
            ?DEBUG("WAL: requesting resend from `~p`, "
                   "last idx ~b idx received ~b",
                   [UId, PrevIdx, Idx]),
            Pid ! {ra_log_event, {resend_write, PrevIdx + 1}},
            State0#state{writers = Writers#{UId => {out_of_seq, PrevIdx}}}
    end;
handle_msg({truncate, Id, Idx, Term, Entry}, State0) ->
    write_data(Id, Idx, Term, Entry, true, State0);
handle_msg(rollover, State) ->
    roll_over(State).

append_data(#state{wal = #wal{file_size = FileSize} = Wal,
                   batch = Batch,
                   writers = Writers} = State,
            {UId, Pid}, Idx, Term, Entry, DataSize, Data, Truncate) ->
    true = update_mem_table(ra_log_open_mem_tables, UId, Idx, Term, Entry,
                            Truncate),
    State#state{wal = Wal#wal{file_size = FileSize + DataSize},
                batch = incr_batch(Batch, Pid, {Idx, Term}, Data),
                writers = Writers#{UId => {in_seq, Idx}} }.

update_mem_table(OpnMemTbl, UId, Idx, Term, Entry, Truncate) ->
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
            true = ets:insert_new(OpnMemTbl, {UId, Idx, Idx, Tid}),
            true = ets:insert(Tid, {Idx, Term, Entry})
    end.

roll_over(State0) ->
    State = flush_pending(State0),
    roll_over(ra_log_open_mem_tables, State).

roll_over(OpnMemTbls, #state{wal = Wal0, dir = Dir, file_num = Num0,
                             segment_writer = SegWriter} = State0) ->
    Num = Num0 + 1,
    Fn = ra_lib:zpad_filename("", "wal", Num),
    NextFile = filename:join(Dir, Fn),
    ?DEBUG("wal: opening new file ~p~n", [Fn]),
    case Wal0 of
        undefined ->
            ok;
        Wal ->
            ok = close_file(Wal#wal.fd),
            ok = close_open_mem_tables(OpnMemTbls, Wal#wal.filename, SegWriter)
    end,
    State = open_file(NextFile, State0),
    State#state{file_num = Num}.

open_file(File, #state{write_strategy = o_sync,
                       file_modes = Modes0} = State) ->
        Modes = [sync | Modes0],
        case ra_file_handle:open(File, Modes) of
            {ok, Fd} ->
                ok = write_header(Fd),
                % many platforms implement O_SYNC a bit like O_DSYNC
                % perform a manual sync here to ensure metadata is flushed
                ok = ra_file_handle:sync(Fd),
                State#state{file_modes = Modes,
                            wal = #wal{fd = Fd, filename = File}};
            {error, enotsup} ->
                ?WARN("WAL: o_sync write stragegy not supported. "
                      "Reverting back to default strategy.", []),
                open_file(File, State#state{write_strategy = default})
        end;
open_file(File, #state{file_modes = Modes} = State) ->
    {ok, Fd} = ra_file_handle:open(File, Modes),
    ok = write_header(Fd),
    State#state{file_modes = Modes, wal = #wal{fd = Fd, filename = File}}.

write_header(Fd) ->
    ok = ra_file_handle:write(Fd, <<?MAGIC>>),
    ok = ra_file_handle:write(Fd, <<?CURRENT_VERSION:8/unsigned>>).

close_file(undefined) ->
    ok;
close_file(Fd) ->
    ok = ra_file_handle:sync(Fd),
    ra_file_handle:close(Fd).

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
    ok = ra_log_segment_writer:accept_mem_tables(TblWriter, MemTables,
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
    ServerName = ra_directory:name_of(UId),
    Tid = ets:new(ServerName, [set, {read_concurrency, true}, public]),
    % immediately give away ownership to ets process
    true = ra_log_ets:give_away(Tid),
    Tid.

start_batch(State) ->
    State#state{batch = #batch{start_time = os:system_time(microsecond)}}.


flush_pending(#state{wal = #wal{fd = Fd},
                     batch = #batch{pending = Pend} = Batch,
                     write_strategy = WriteStrat} = State0) ->
    case WriteStrat of
        default ->
            ok = ra_file_handle:write(Fd, lists:reverse(Pend)),
            ok = ra_file_handle:sync(Fd),
            ok;
        o_sync ->
            ok = ra_file_handle:write(Fd, lists:reverse(Pend))
    end,
    State0#state{batch = Batch#batch{pending = []}}.

complete_batch(#state{batch = undefined} = State) ->
    State;
complete_batch(#state{batch = #batch{waiting = Waiting,
                                     writes = NumWrites,
                                     start_time = ST},
                      metrics_cursor = Cursor
                      } = State00) ->
    TS = os:system_time(microsecond),
    State0 = flush_pending(State00),
    SyncTS = os:system_time(microsecond),
    _ = ets:update_element(ra_log_wal_metrics, Cursor,
                           {2, {NumWrites, TS-ST, SyncTS-TS}}),
    NextCursor = (Cursor + 1) rem ?METRICS_WINDOW_SIZE,
    State = State0#state{metrics_cursor = NextCursor,
                         batch = undefined},


    %% notify writers
    _ = maps:map(fun (Pid, WrittenInfo) ->
                         Pid ! {ra_log_event, {written, WrittenInfo}},
                         ok
                 end, Waiting),
    {ok, [garbage_collect], State}.

incr_batch(#batch{writes = Writes,
                  waiting = Waiting0,
                  pending = Pend} = Batch, Pid, {Idx, Term}, Data) ->
    Waiting = case Waiting0 of
                  #{Pid := {From, _, _}} ->
                      Waiting0#{Pid => {min(Idx, From), Idx, Term}};
                  _ ->
                      Waiting0#{Pid => {Idx, Idx, Term}}
              end,

    Batch#batch{writes = Writes + 1,
                waiting = Waiting,
                pending = [Data | Pend]}.

wal2list(File) ->
    Data = open_existing(File),
    dump_records(Data, []).

open_existing(File) ->
    case file:read_file(File) of
        {ok, <<?MAGIC, ?CURRENT_VERSION:8/unsigned, Data/binary>>} ->
            %% the only version currently supported
            Data;
        {ok, <<Magic:64/binary, UnknownVersion:8/unsigned, _/binary>>} ->
            exit({unknown_wal_file_format, Magic, UnknownVersion})
    end.


dump_records(<<_:1/unsigned, 0:1/unsigned, _:22/unsigned,
               IdDataLen:16/unsigned, _:IdDataLen/binary,
               _:32/integer,
               EntryDataLen:32/unsigned,
               Idx:64/unsigned, _:64/unsigned,
               EntryData:EntryDataLen/binary,
               Rest/binary>>, Entries) ->
    % TODO: recover writers info, i.e. last index seen
    dump_records(Rest, [{Idx, binary_to_term(EntryData)} | Entries]);
dump_records(<<_:1/unsigned, 1:1/unsigned, _:22/unsigned,
               _:32/integer,
               EntryDataLen:32/unsigned,
               Idx:64/unsigned, _:64/unsigned,
               EntryData:EntryDataLen/binary,
               Rest/binary>>, Entries) ->
    dump_records(Rest, [{Idx, binary_to_term(EntryData)} | Entries]);
dump_records(<<>>, Entries) ->
    Entries.

try_recover_records(Data, Cache) ->
    try recover_records(Data, Cache) of
        ok -> ok
    catch _:_ = Err ->
              ?WARN("wal: encountered error during recovery: ~w~n"
                    "Continuing.~n", [Err]),
              ok
    end.

recover_records(<<Trunc:1/unsigned, 0:1/unsigned, IdRef:22/unsigned,
                  IdDataLen:16/unsigned, UId:IdDataLen/binary,
                  Checksum:32/integer,
                  EntryDataLen:32/unsigned,
                  Idx:64/unsigned, Term:64/unsigned,
                  EntryData:EntryDataLen/binary,
                  Rest/binary>>, Cache) ->
    % first writer appearance in WAL
    true = validate_and_update(UId, Checksum, Idx, Term, EntryData, Trunc),
    % TODO: recover writers info, i.e. last index seen
    recover_records(Rest,
                    Cache#{IdRef =>
                           {UId, <<1:1/unsigned, IdRef:22/unsigned>>}});
recover_records(<<Trunc:1/unsigned, 1:1/unsigned, IdRef:22/unsigned,
                  Checksum:32/integer,
                  EntryDataLen:32/unsigned,
                  Idx:64/unsigned, Term:64/unsigned,
                  EntryData:EntryDataLen/binary,
                  Rest/binary>>, Cache) ->
    #{IdRef := {UId, _}} = Cache,
    true = validate_and_update(UId, Checksum, Idx, Term, EntryData, Trunc),

    % TODO: recover writers info, i.e. last index seen
    recover_records(Rest, Cache);
recover_records(<<>>, _Cache) ->
    ok.

validate_and_update(UId, Checksum, Idx, Term, EntryData, Trunc) ->
    validate_checksum(Checksum, Idx, Term, EntryData),
    true = update_mem_table(ra_log_recover_mem_tables, UId, Idx, Term,
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
                 max_size_bytes => ?WAL_MAX_SIZE_BYTES,
                 compute_checksums => true,
                 write_strategy => default}, Conf).

to_binary(Term) ->
    term_to_binary(Term).
