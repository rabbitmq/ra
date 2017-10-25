-module(ra_log_wal).

-export([start_link/2,
         write/5,
         init/3,
         mem_tbl_read/2,
         system_continue/3,
         system_terminate/4,
         write_debug/3]).

-include("ra.hrl").

-define(MIN_MAX_BATCH_SIZE, 20).
-define(MAX_MAX_BATCH_SIZE, 1000).
-define(METRICS_WINDOW_SIZE, 100).

% a token to notify the writer of the last request written
% typically this would be a ra_index()
-type token() :: term().

-type writer_id() :: atom(). % currently has to be a locally registered name

-record(batch, {writes = 0 :: non_neg_integer(),
                waiting = #{} :: #{writer_id() => token()},
                start_time :: maybe(integer())
               }).

-record(state, {file_num = 0 :: non_neg_integer(),
                fd :: maybe(file:io_device()),
                filename :: maybe(file:filename()),
                file_modes :: [file:mode()],
                dir :: string(),
                max_batch_size = ?MIN_MAX_BATCH_SIZE :: non_neg_integer(),
                max_wal_size_bytes = unlimited :: non_neg_integer(), % TODO: better default
                segment_writer = ra_log_file_segment_writer :: atom(),
                batch = #batch{} :: #batch{},
                metrics_cursor = 0 :: non_neg_integer(),
                wal_file_size = 0 :: non_neg_integer()
               }).

-type state() :: #state{}.
-type wal_conf() :: #{dir => file:filename_all(),
                      max_wal_size_bytes => non_neg_integer(),
                      segment_writer => atom(),
                      additional_wal_file_modes => [file:mode()]
                     }.


-spec write(pid() | atom(), atom(), ra_index(), ra_term(), term()) ->
    ok.
write(From, Wal, Idx, Term, Entry) ->
    Wal ! {log, From, sized_binary(From), Idx, Term, Entry},
    ok.

sized_binary(Bin) when is_binary(Bin) ->
    {byte_size(Bin), Bin};
sized_binary(Term) ->
    Bin = to_binary(Term),
    {byte_size(Bin), Bin}.

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
        Tids ->
            tbl_lookup(Tids, Idx)
    end.

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

make_file_name(Num) ->
    lists:flatten(io_lib:format("~5..0B.wal", [Num])).

% parse_file_name(File) ->
%     Name = filename:basename(File, ".wal"),
%     {Int, _} = string:to_integer(Name),
%     Int.

recover_wal(Dir, #{max_wal_size_bytes := MaxWalSize,
                   segment_writer := TblWriter,
                   additional_wal_file_modes := AdditionalModes}) ->
    % ensure configured directory exists
    ok = filelib:ensure_dir(Dir),
    _ = file:make_dir(Dir),
    % TODO: we don't actually recover anything ATM so delete anything that was
    % previously there for now
    [ _ = file:delete(F)
      || F <- filelib:wildcard(filename:join(Dir, "*.wal"))],
    Modes = [raw, append, binary] ++ AdditionalModes,
    roll_over(#state{fd = undefined,
                     dir = Dir,
                     file_modes = Modes,
                     max_wal_size_bytes = MaxWalSize,
                     segment_writer = TblWriter
                    }).

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

loop_batched(State0 = #state{max_batch_size = Written,
                             batch = #batch{writes = Written}}, Parent, Debug0) ->
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

cleanup(_State) ->
    ok.

handle_debug_in(Debug, Msg) ->
    sys:handle_debug(Debug, fun write_debug/3,
                     ?MODULE, {in, Msg}).

handle_msg({log, Id, {IdDataLen, IdData}, Idx, Term, Entry},
           #state{max_wal_size_bytes = MaxWalSize,
                  wal_file_size = FileSize} = State0) ->
    % log on disk format:
    % <<IdLen:16/integer, Id/binary,
    % Idx:64/integer, Term:64/integer, Length:32/integer, Data/binary>>
    % TODO: needing the "Id" in the shared wal is pretty wasteful
    % can we create someting fixed length to use instead?
    %% TODO: cache binary Id representation?
    EntryData = to_binary(Entry),
    EntryDataLen = byte_size(EntryData),
    % TODO adler32 checksum check for EntryData
    Data = <<IdDataLen:16/integer, % 2
             IdData/binary,
             Idx:64/integer,
             Term:64/integer,
             EntryDataLen:32/integer,
             EntryData/binary>>,

    % fixed overhead = 22 bytes
    DataSize = IdDataLen + 22 + EntryDataLen,
    % if the next write is going to exceed the configured max wal size
    % we roll over to a new wal.
    case FileSize + DataSize > MaxWalSize of
        true ->
            State = roll_over(State0),
            append_data(State, Id, Idx, Term, Entry, DataSize, Data);
        false ->
            append_data(State0, Id, Idx, Term, Entry, DataSize, Data)
    end.

append_data(#state{fd = Fd, batch = Batch,
                   wal_file_size = FileSize} = State,
            Id, Idx, Term, Entry, DataSize, Data) ->
    ok = file:write(Fd, Data),
    true = update_mem_table(Id, Idx, Term, Entry),
    State#state{batch = incr_batch(Batch, Id, {Idx, Term}),
                wal_file_size = FileSize + DataSize}.

update_mem_table(Id, Idx, Term, Entry) ->
    % TODO: cache current tables to avoid ets lookup?
    case ets:lookup(ra_log_open_mem_tables, Id) of
        [{_Id, _First, _Last, Tid}] ->
            % TODO: check Last + 1 == Idx or handle missing?
            _ = ets:insert(Tid, {Idx, Term, Entry}),
            % update Last idx for current tbl
            % this is how followers "truncate" previously seen entries
            _ = ets:update_element(ra_log_open_mem_tables, Id, {3, Idx});
        [] ->
            % open new ets table
            Tid = open_mem_table(Id, Idx),
            true = ets:insert(Tid, {Idx, Term, Entry})
    end.

roll_over(#state{fd = Fd0, filename = Filename, file_num = Num0, dir = Dir,
                 file_modes = Modes, segment_writer = TblWriter} = State) ->
    Num = Num0 + 1,
    ?DBG("wal: rolling over to ~p~n", [Num]),
    NextFile = filename:join(Dir, make_file_name(Num)),
    ra_lib:iter_maybe(Fd0, fun (F) -> ok = file:close(F) end),
    {ok, Fd} = file:open(NextFile, Modes),

    % roll over to a new file
    % close file and open new, update fd
    % read all entries from ra_log_open_mem_tables
    MemTables = ets:tab2list(ra_log_open_mem_tables),
    % insert into closed mem tables
    % so that readers can still resolve the table whilst it is being
    % flushed to persistent tables asynchronously
    [_ = ets:insert(ra_log_closed_mem_tables, T) || T <- MemTables],
    % reset open mem tables table
    true = ets:delete_all_objects(ra_log_open_mem_tables),

    % notify segment_writer of new unflushed memtables
    ok = ra_log_file_segment_writer:accept_mem_tables(TblWriter, MemTables,
                                                      Filename),

    State#state{fd = Fd, filename = NextFile, wal_file_size = 0,
                file_num = Num}.

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
                              Msg = {written, IdxTerm},
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

%% Here are the sys call back functions

system_continue(Parent, Debug, State) ->
    % TODO check if we've written to the curren batch or not
    loop_batched(State, Parent, Debug).

system_terminate(Reason, _Parent, _Debug, State) ->
    cleanup(State),
    exit(Reason).

write_debug(Dev, Event, Name) ->
    io:format(Dev, "~p event = ~p~n", [Name, Event]).

merge_conf_defaults(Conf) ->
    maps:merge(#{segment_writer => ra_log_file_segment_writer,
                 max_wal_size_bytes => unlimited, % TODO: better default
                 additional_wal_file_modes => []},
               Conf).

to_binary(Term) ->
    term_to_binary(Term).
