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

-record(state, {fd :: file:io_device(),
                max_batch_size = ?MIN_MAX_BATCH_SIZE :: non_neg_integer(),
                batch = #batch{} :: #batch{},
                cursor = 0 :: non_neg_integer()
               }).

-type state() :: #state{}.


write(From, Wal, Idx, Term, Value) ->
    Wal ! {log, From, Idx, Term, Value},
    ok.

mem_tbl_read(Id, Idx) ->
    case ets:lookup(ra_log_open_mem_tables, Id) of
        [] -> undefined;
        Tids ->
            tbl_lookup(Tids, Idx)
    end.

tbl_lookup([], _Idx) ->
    undefined;
tbl_lookup([{_, _First, Last, Tid} | Tail], Idx) when Last >= Idx ->
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
%% There should only ever be one "open" tableggj
%% i.e. a table that is currently being written to
%% Num is a monotonically incrementing id to be used to
%% determine the order the tables were written to
%% ETS with {tid(), Num :: non_neg_integer(), open | closed}

start_link(Config, Options) ->
    case whereis(?MODULE) of
        undefined ->
            Pid = proc_lib:spawn_link(?MODULE, init, [Config, self(), Options]),
            register(?MODULE, Pid),
            {ok, Pid};
        Pid ->
            {error, {already_started, Pid}}
    end.

-spec init(map(), pid(), list()) -> state().
init(#{dir := Dir} = Conf, Parent, Options) ->
    process_flag(trap_exit, true),
    % TODO: init wal here
    % create mem table lookup table to be used to map ra cluster name
    % to table identifiers to query.
    _ = ets:new(ra_log_open_mem_tables,
                [set, named_table, {read_concurrency, true}, protected]),
    _ = ets:new(ra_log_wal_metrics,
                [set, named_table, {read_concurrency, true}, protected]),
    % seed metrics table with data
    [true = ets:insert(ra_log_wal_metrics, {I, undefined})
     || I <- lists:seq(0, ?METRICS_WINDOW_SIZE-1)],

    State = recover_wal(Dir, maps:get(additional_wal_file_modes, Conf, [])),
    Debug = sys:debug_options(Options),
    loop_wait(State, Parent, Debug).

recover_wal(Dir, AdditionalModes) ->
    File = filename:join(Dir, "00001.wal"),
    % TODO: we don't actually recover anything so delete anything that was
    % previously there
    _ = file:delete(File),
    ok = filelib:ensure_dir(File),
    Modes = [raw, append, binary] ++ AdditionalModes,
    {ok, Fd} = file:open(File, Modes),
    #state{fd = Fd}.


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
            % ct:pal("started batch ~p", [State1#state.batch]),
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

handle_msg({log, Id, Idx, Term, Entry},
           State = #state{fd = Fd, batch = Batch}) ->
    % log on disk format:
    % <<LogOrKv:8/integer, IdLen:16/integer, Id/binary,
    % Idx:64/integer, Term:64/integer, Length:32/integer, Data/binary>>
    % needing the "Id" in the shared wal is pretty wasteful
    % can we create someting fixed length to use instead?
    %% TODO: offload all this serialization to the calling process instead
    IdData = atom_to_binary(Id, utf8),
    IdDataLen = byte_size(IdData),
    EntryData = term_to_binary(Entry),
    EntryDataLen = byte_size(EntryData),
    Data = <<0:8/integer, % log flag
             IdDataLen:16/integer,
             IdData/binary,
             Idx:64/integer,
             Term:64/integer,
             EntryDataLen:32/integer,
             EntryData/binary>>,

    ok = file:write(Fd, Data),
    case ets:lookup(ra_log_open_mem_tables, Id) of
        [{Id, _First, _Last, Tid}] ->
            % TODO: check Last + 1 == Idx or handle missing?
            true = ets:insert(Tid, {Idx, Term, Entry}),
            % update Last idx for current tbl
            % this is how followers "truncate" previously seen entries
            true = ets:update_element(ra_log_open_mem_tables, Id, {3, Idx}),
            State#state{batch = incr_batch(Batch, Id, Idx)};
        [] ->
            % open new ets table
            Tid = open_mem_table(Id, Idx),
            true = ets:insert(Tid, {Idx, Term, Entry}),
            State#state{batch = incr_batch(Batch, Id, Idx)}
    end.

open_mem_table(Id, Idx) ->
    Tid = ets:new(Id, [set, protected, {read_concurrency, true}]),
    true = ets:insert(ra_log_open_mem_tables, {Id, Idx, Idx,Tid}),
    Tid.

start_batch(State) ->
    State#state{batch = #batch{start_time = os:system_time(millisecond)}}.

complete_batch(#state{batch = #batch{waiting = Waiting,
                                     writes = NumWrites,
                                     start_time = ST},
                      fd = Fd, cursor = Cursor} = State0,
               Debug0) ->
    TS = os:system_time(millisecond),
    _ = file:sync(Fd),
    SyncTS = os:system_time(millisecond),
    _ = ets:update_element(ra_log_wal_metrics, Cursor,
                           {2, {NumWrites, TS-ST, SyncTS-TS}}),
    NextCursor = (Cursor + 1) rem ?METRICS_WINDOW_SIZE,
    State = State0#state{cursor = NextCursor},
    % error_logger:info_msg("completing batch ~p~n", [Waiting]),

    % TODO emit metrics of time taken to sync and write batch size
    % notify processes that have synced map(Pid, Token)
    Debug = maps:fold(fun (Id, Tok, Dbg) ->
                              Msg = {written, Tok},
                              catch (Id ! Msg),
                              Evt = {out, {self(), Msg}, Id},
                              sys:handle_debug(Dbg, fun write_debug/3,
                                               ?MODULE, Evt)
                      end, Debug0, Waiting),
    {State, Debug}.

incr_batch(#batch{writes = Writes,
                  waiting = Waiting} = Batch, Id, Idx) ->
    Batch#batch{writes = Writes + 1,
                waiting = Waiting#{Id => Idx}}.

%% Here are the sys call back functions

system_continue(Parent, Debug, State) ->
    % TODO check if we've written to the curren batch or not
    loop_batched(State, Parent, Debug).

system_terminate(Reason, _Parent, _Debug, State) ->
    cleanup(State),
    exit(Reason).

write_debug(Dev, Event, Name) ->
    io:format(Dev, "~p event = ~p~n", [Name, Event]).
