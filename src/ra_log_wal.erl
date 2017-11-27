-module(ra_log_wal).

-export([start_link/2,
         write/5,
         truncate_write/5,
         force_roll_over/1,
         init/3,
         mem_tbl_read/2,
         system_continue/3,
         system_terminate/4,
         write_debug/3]).

-include("ra.hrl").

-define(MIN_MAX_BATCH_SIZE, 20).
-define(MAX_MAX_BATCH_SIZE, 1000).
-define(METRICS_WINDOW_SIZE, 100).
-define(MAX_WAL_SIZE_BYTES, 1000 * 1000 * 128).

-type writer_id() :: atom(). % currently has to be a locally registered name

-record(batch, {writes = 0 :: non_neg_integer(),
                waiting = #{} :: #{writer_id() => ra_index()},
                start_time :: maybe(integer())
               }).

-record(state, {file_num = 0 :: non_neg_integer(),
                fd :: maybe(file:io_device()),
                filename :: maybe(file:filename()),
                file_modes :: [term()],
                dir :: string(),
                max_batch_size = ?MIN_MAX_BATCH_SIZE :: non_neg_integer(),
                max_wal_size_bytes = ?MAX_WAL_SIZE_BYTES :: non_neg_integer(),
                segment_writer = ra_log_file_segment_writer :: atom(),
                batch = #batch{} :: #batch{},
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
                writer_name_cache = {0, #{}} :: {NextIntId :: non_neg_integer(), #{atom() => binary()}},
                metrics_cursor = 0 :: non_neg_integer(),
                wal_file_size = 0 :: non_neg_integer()
               }).

-type state() :: #state{}.
-type wal_conf() :: #{dir => file:filename_all(),
                      max_wal_size_bytes => non_neg_integer(),
                      segment_writer => atom(),
                      additional_wal_file_modes => [term()]
                     }.


-spec write(pid() | atom(), atom(), ra_index(), ra_term(), term()) -> ok.
write(From, Wal, Idx, Term, Entry) ->
    % in a future where we might have a pool of WALs they may not always
    % be named, and wal could be a pid(). If so this will result in a lost
    % write message rather than a name lookup failure (badarg).
    Wal ! {append, From, Idx, Term, Entry},
    ok.

-spec truncate_write(pid() | atom(), atom(), ra_index(), ra_term(), term()) ->
    ok.
truncate_write(From, Wal, Idx, Term, Entry) ->
    Wal ! {truncate, From, Idx, Term, Entry},
    ok.

% force a wal file to roll over to a new file
% mostly useful for testing
force_roll_over(Wal) ->
    Wal ! rollover,
    ok.

mem_tbl_read(Id, Idx) ->
    case ets:lookup(ra_log_open_mem_tables, Id) of
        [{_, Fst, _, _}] = Tids when Idx >= Fst ->
            tbl_lookup(Tids, Idx);
        _ ->
            closed_mem_tbl_read(Id, Idx)
    end.

closed_mem_tbl_read(Id, Idx) ->
    case ets:lookup(ra_log_closed_mem_tables, Id) of
        [] ->
            undefined;
        Tids0 ->
            Tids = lists:sort(fun(A, B) -> B > A end, Tids0),
            closed_tbl_lookup(Tids, Idx)
    end.

closed_tbl_lookup([], _Idx) ->
    undefined;
closed_tbl_lookup([{_, _, _First, Last, Tid} | Tail], Idx) when Last >= Idx ->
    % TODO: it is possible the ETS table has been deleted at this
    % point so should catch the error
    case ets:lookup(Tid, Idx) of
        [] ->
            closed_tbl_lookup(Tail, Idx);
        [Entry] -> Entry
    end;
closed_tbl_lookup([_ | Tail], Idx) ->
    closed_tbl_lookup(Tail, Idx).

tbl_lookup([], _Idx) ->
    undefined;
tbl_lookup([{_, _First, Last, Tid} | Tail], Idx) when Last >= Idx ->
    % TODO: it is possible the ETS table has been deleted at this
    % point so should catch the error
    case ets:lookup(Tid, Idx) of
        [] ->
            tbl_lookup(Tail, Idx);
        [Entry] -> Entry
    end;
tbl_lookup([_ | Tail], Idx) ->
    tbl_lookup(Tail, Idx).


%% Memtables meta data
%% {Queue, [tid()]} - the first tid is the currently active memtable for the
%% queue. Ideally there should only be one or two but compaction lag
%% may cause it to stash more.
%% registration is implicit in a write (TODO: cleanup?)
%%
%% Memtable per "queue" format:
%% {ra_index(), {ra_term(), entry()}} | {first_idx, ra_index()} | {term, ra_term()
%% | {voted_for, peer()}
%% any integer key is a log entry - anything else is metadata
%% kv data, the first key should always be present

%% Mem Tables - ra_log_wal_meta_data
%% There should only ever be one "open" table
%% i.e. a table that is currently being written to
%% Num is a monotonically incrementing id to be used to
%% determine the order the tables were written to
%% ETS with {tid(), Num :: non_neg_integer(), open | closed}

-spec start_link(Config :: wal_conf(), Options :: list()) ->
    {ok, pid()} | {error, {already_started, pid()}}.
start_link(Config, Options) ->
    case whereis(?MODULE) of
        undefined ->
            Pid = proc_lib:spawn_link(?MODULE, init, [Config, self(), Options]),
            register(?MODULE, Pid),
            {ok, Pid};
        Pid ->
            {error, {already_started, Pid}}
    end.

-spec init(wal_conf(), pid(), list()) -> state().
init(#{dir := Dir} = Conf0, Parent, Options) ->
    Conf = merge_conf_defaults(Conf0),
    process_flag(trap_exit, true),
    % create mem table lookup table to be used to map ra cluster name
    % to table identifiers to query.
    _ = ets:new(ra_log_open_mem_tables,
                [set, named_table, {read_concurrency, true}, protected]),
    _ = ets:new(ra_log_closed_mem_tables,
                [bag, named_table, {read_concurrency, true}, public]),
    _ = ets:new(ra_log_wal_metrics,
                [set, named_table, {read_concurrency, true}, protected]),
    % seed metrics table with data
    [true = ets:insert(ra_log_wal_metrics, {I, undefined})
     || I <- lists:seq(0, ?METRICS_WINDOW_SIZE-1)],

    State = recover_wal(Dir, Conf),
    Debug = sys:debug_options(Options),
    loop_wait(State, Parent, Debug).

recover_wal(Dir, #{max_wal_size_bytes := MaxWalSize,
                   segment_writer := TblWriter,
                   additional_wal_file_modes := AdditionalModes}) ->
    % ensure configured directory exists
    ok = filelib:ensure_dir(Dir),
    _ = file:make_dir(Dir),
    %  recover each mem table and notify segment writer
    %  this may result in duplicated segments but that is better than
    %  losing any data
    WalFiles = filelib:wildcard(filename:join(Dir, "*.wal")),
    ?DBG("WAL: recovering ~p", [WalFiles]),
    [begin
         % TOOD: avoid reading the whole file at once
         {ok, Data} = file:read_file(F),
         ok = recover_records(Data, #{}),
         ok = close_open_mem_tables(F, TblWriter)
     end || F <- lists:sort(WalFiles)],
    Modes = [raw, append, binary] ++ AdditionalModes,
    roll_over(#state{fd = undefined,
                     dir = Dir,
                     file_num = extract_file_num(WalFiles),
                     file_modes = Modes,
                     max_wal_size_bytes = MaxWalSize,
                     segment_writer = TblWriter
                    }).

extract_file_num([]) ->
    0;
extract_file_num([F | _]) ->
    ra_lib:zpad_extract_num(filename:basename(F)).

loop_wait(State0, Parent, Debug0) ->
    receive
        {system, From, Request} ->
            sys:handle_system_msg(Request, From, Parent, ?MODULE, Debug0, State0);
        {'EXIT', Parent, Reason} ->
            cleanup(State0),
            exit(Reason);
        Msg ->
            Debug = handle_debug_in(Debug0, Msg),
            % start a new batch
            State1 = start_batch(State0),
            State = handle_msg(Msg, State1),
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
            cleanup(State0),
            exit(Reason);
        Msg ->
            Debug = handle_debug_in(Debug0, Msg),
            State = handle_msg(Msg, State0),
            loop_batched(State, Parent, Debug)
    after 0 ->
              {State, Debug} = complete_batch(State0, Debug0),
              NewBatchSize = max(?MIN_MAX_BATCH_SIZE,
                                 State0#state.max_batch_size / 2),
              loop_wait(State#state{max_batch_size = NewBatchSize}, Parent, Debug)
    end.

cleanup(#state{fd = undefined}) ->
    ok;
cleanup(#state{fd = Fd}) ->
    _ = file:sync(Fd),
    ok.

handle_debug_in(Debug, Msg) ->
    sys:handle_debug(Debug, fun write_debug/3,
                     ?MODULE, {in, Msg}).

serialize_header(Id, Trunc, #state{writer_name_cache = {Next, Cache}} = State) ->
    T = case Trunc of true -> 1; false -> 0 end,
    case Cache of
        #{Id := BinId} ->
            {<<T:1/integer, BinId/bitstring>>, State};
        _ ->
            % TODO: check overflows of Next
            % cache the last 15 bits of the header
            BinId = <<1:1/integer, Next:14/integer>>,
            IdData = to_binary(Id),
            IdDataLen = byte_size(IdData),
            MarkerId = <<T:1/integer, 0:1/integer, Next:14/integer,
                         IdDataLen:16/integer, IdData/binary>>,
            {MarkerId,
             State#state{writer_name_cache =
                         {Next+1, Cache#{Id => BinId}}}}
    end.

write_data(Id, Idx, Term, Entry, Trunc,
           #state{max_wal_size_bytes = MaxWalSize,
                  wal_file_size = FileSize} = State00) ->
    EntryData = to_binary(Entry),
    EntryDataLen = byte_size(EntryData),
    {HeaderData, State0} = serialize_header(Id, Trunc, State00),
    % TODO optional adler32 checksum check for EntryData
    Data = <<HeaderData/binary,
             Idx:64/integer,
             Term:64/integer,
             EntryDataLen:32/integer,
             EntryData/binary>>,

    % fixed overhead = 20 bytes 2 * 64bit ints + 1 32 bit int
    DataSize = byte_size(HeaderData) + 20 + EntryDataLen,
    % if the next write is going to exceed the configured max wal size
    % we roll over to a new wal.
    case FileSize + DataSize > MaxWalSize of
        true ->
            State = roll_over(State0),
            append_data(State, Id, Idx, Term, Entry, DataSize, Data, Trunc);
        false ->
            append_data(State0, Id, Idx, Term, Entry, DataSize, Data, Trunc)
    end.

handle_msg({append, Id, Idx, Term, Entry},
           #state{writers = Writers} = State0) ->
    case maps:find(Id, Writers) of
        error ->
            write_data(Id, Idx, Term, Entry, false, State0);
        {ok, {_, PrevIdx}} when Idx =< PrevIdx + 1 ->
            write_data(Id, Idx, Term, Entry, false, State0);
        {ok, {out_of_seq, _}} ->
            % writer is out of seq simply ignore drop the write
            % TODO: capture metric for dropped writes
            State0;
        {ok, {in_seq, PrevIdx}} ->
            % writer was in seq but has sent an out of seq entry
            % notify writer
            ?DBG("WAL: requesting resend from `~p`, last idx ~b idx received ~b",
                 [Id, PrevIdx, Idx]),
            Id ! {ra_log_event, {resend_write, PrevIdx + 1}},
            State0#state{writers = Writers#{Id => {out_of_seq, PrevIdx}}}
    end;
handle_msg({truncate, Id, Idx, Term, Entry}, State0) ->
    write_data(Id, Idx, Term, Entry, true, State0);
handle_msg(rollover, State) ->
    roll_over(State).

append_data(#state{fd = Fd, batch = Batch,
                   writers = Writers,
                   wal_file_size = FileSize} = State,
            Id, Idx, Term, Entry, DataSize, Data, Truncate) ->
    ok = file:write(Fd, Data),
    true = update_mem_table(Id, Idx, Term, Entry, Truncate),
    State#state{batch = incr_batch(Batch, Id, {Idx, Term}),
                writers = Writers#{Id => {in_seq, Idx}},
                wal_file_size = FileSize + DataSize}.

update_mem_table(Id, Idx, Term, Entry, Truncate) ->
    % TODO: cache current tables to avoid ets lookup?
    % TODO: if Idx =< First we could truncate the entire table
    case ets:lookup(ra_log_open_mem_tables, Id) of
        [{_Id, From0, _To, Tid}] ->
            _ = ets:insert(Tid, {Idx, Term, Entry}),
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
            _ = ets:update_element(ra_log_open_mem_tables, Id,
                                   [{2, From}, {3, Idx}]);
        [] ->
            % open new ets table
            Tid = open_mem_table(Id, Idx),
            true = ets:insert(Tid, {Idx, Term, Entry})
    end.

roll_over(#state{fd = Fd0, file_num = Num0, dir = Dir,
                 file_modes = Modes, filename = Filename,
                 segment_writer = TblWriter} = State) ->
    Num = Num0 + 1,
    ?DBG("wal: rolling over to ~p~n", [Num]),
    NextFile = filename:join(Dir, ra_lib:zpad_filename("", "wal", Num)),
    ra_lib:iter_maybe(Fd0, fun (F) -> ok = file:close(F) end),
    {ok, Fd} = file:open(NextFile, Modes),

    ok = close_open_mem_tables(Filename, TblWriter),

    State#state{fd = Fd, filename = NextFile, wal_file_size = 0,
                writer_name_cache = {0, #{}},
                file_num = Num}.

close_open_mem_tables(Filename, TblWriter) ->
    MemTables = ets:tab2list(ra_log_open_mem_tables),
    % insert into closed mem tables
    % so that readers can still resolve the table whilst it is being
    % flushed to persistent tables asynchronously
    % Also give away ets ownership to the ra node as it will be responsible
    % for deleting it
    % TODO: alternatively we could have a separate ETS cleanup process
    [begin
         % TODO: in order to ensure that reads are done in the correct causal order
         % we need to append a monotonically increasing value for readers to sort
         % by
         M = erlang:unique_integer([monotonic, positive]),
         _ = ets:insert(ra_log_closed_mem_tables,
                        erlang:insert_element(2, T, M)),
         % TODO: better handle give_away errors
         catch ets:give_away(Tid, whereis(Id), undefined)
     end || {Id, _, _, Tid} = T <- MemTables],
    % reset open mem tables table
    true = ets:delete_all_objects(ra_log_open_mem_tables),

    % notify segment_writer of new unflushed memtables
    ok = ra_log_file_segment_writer:accept_mem_tables(TblWriter, MemTables,
                                                      Filename),
    ok.


open_mem_table(Id, Idx) ->
    Tid = ets:new(Id, [set, protected, {read_concurrency, true}]),
    true = ets:insert(ra_log_open_mem_tables, {Id, Idx, Idx, Tid}),
    Tid.

start_batch(State) ->
    State#state{batch = #batch{start_time = os:system_time(millisecond)}}.

complete_batch(#state{batch = #batch{waiting = Waiting,
                                     writes = NumWrites,
                                     start_time = ST},
                      fd = Fd, metrics_cursor = Cursor} = State0,
               Debug0) ->
    TS = os:system_time(millisecond),
    ok = file:sync(Fd),
    SyncTS = os:system_time(millisecond),
    _ = ets:update_element(ra_log_wal_metrics, Cursor,
                           {2, {NumWrites, TS-ST, SyncTS-TS}}),
    NextCursor = (Cursor + 1) rem ?METRICS_WINDOW_SIZE,
    State = State0#state{metrics_cursor = NextCursor},
    % error_logger:info_msg("completing batch ~p~n", [Waiting]),

    % TODO emit metrics of time taken to sync and write batch size
    % notify processes that have synced map(Pid, Token)
    Debug = maps:fold(fun (Id, IdxTerm, Dbg) ->
                              Msg = {ra_log_event, {written, IdxTerm}},
                              try Id ! Msg  of
                                  _ -> ok
                              catch
                                  error:badarg ->
                                      % this will happen if Id is no longer alive
                                      % and registered
                                      error_logger:warning_msg("wal: failed to send written notification to ~p~n", [Id])
                              end,
                              Evt = {out, {self(), Msg}, Id},
                              sys:handle_debug(Dbg, fun write_debug/3,
                                               ?MODULE, Evt)
                      end, Debug0, Waiting),
    {State, Debug}.

incr_batch(#batch{writes = Writes,
                  waiting = Waiting} = Batch, Id, IdxTerm) ->
    Batch#batch{writes = Writes + 1,
                waiting = Waiting#{Id => IdxTerm}}.

recover_records(<<Trunc:1/integer, 0:1/integer, IdRef:14/integer,
                  IdDataLen:16/integer, IdData:IdDataLen/binary,
                  Idx:64/integer, Term:64/integer,
                  EntryDataLen:32/integer, EntryData:EntryDataLen/binary,
                  Rest/binary>>, Cache) ->
    % first writer appearance in WAL
    Id = binary_to_term(IdData),
    true = update_mem_table(Id, Idx, Term, binary_to_term(EntryData),
                            Trunc =:= 1),
    % TODO: recover writers info, i.e. last index seen
    recover_records(Rest,
                    Cache#{IdRef => {Id, <<1:1/integer, IdRef:14/integer>>}});
recover_records(<<Trunc:1/integer, 1:1/integer, IdRef:14/integer,
                  Idx:64/integer, Term:64/integer,
                  EntryDataLen:32/integer, EntryData:EntryDataLen/binary,
                  Rest/binary>>, Cache) ->
    #{IdRef := {Id, _}} = Cache,
    true = update_mem_table(Id, Idx, Term, binary_to_term(EntryData),
                            Trunc =:= 1),
    % TODO: recover writers info, i.e. last index seen
    recover_records(Rest, Cache);
recover_records(<<>>, _Cache) ->
    ok.

%% Here are the sys call back functions

system_continue(Parent, Debug, State) ->
    % TODO check if we've written to the current batch or not
    loop_batched(State, Parent, Debug).

system_terminate(Reason, _Parent, _Debug, State) ->
    cleanup(State),
    exit(Reason).

write_debug(Dev, Event, Name) ->
    io:format(Dev, "~p event = ~p~n", [Name, Event]).

merge_conf_defaults(Conf) ->
    maps:merge(#{segment_writer => ra_log_file_segment_writer,
                 max_wal_size_bytes => ?MAX_WAL_SIZE_BYTES,
                 additional_wal_file_modes => []},
               Conf).

to_binary(Term) ->
    term_to_binary(Term).
