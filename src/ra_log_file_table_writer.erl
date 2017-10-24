-module(ra_log_file_table_writer).
-behaviour(gen_server).

-export([start_link/1,
         receive_tables/2
        ]).

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {data_dir :: filename:filename_all(),
                active_tables = #{} :: #{atom() => {atom(), file:filename_all()}}}).

%%% ra_log_file_table_writer
%%% receives a set of closed mem_tables from the wal
%%% appends to the current dets table for the target and flushes
%%% each dets table has a "range" key with the start and end indexes
%%% is is also named in sequential order (?? what about compaction)
%%% schedules ETS table deletions
%%% processes compaction requests
%%% TODO: work out how overwrites should work and synchronise with readers

%%%===================================================================
%%% API functions
%%%===================================================================

start_link(Config) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [Config], []).


receive_tables(Tables, WalFile) ->
    gen_server:cast(?MODULE, {receive_mem_tables, Tables, WalFile}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([#{data_dir := DataDir}]) ->
    {ok, #state{data_dir = DataDir}}.

handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast({receive_mem_tables, Tables, WalFile}, State0) ->
    State = lists:foldl(fun do_table/2, State0, Tables),
    % delete wal file once done
    ok = file:delete(WalFile),

    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{active_tables = ActiveTables}) ->
    % ensure any open dets tables are closed
    [dets:close(Name) || {_,  {Name, _}} <- maps:to_list(ActiveTables)],
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

do_table({RaNodeId, StartIdx, EndIdx, Tid},
         #state{data_dir = DataDir,
                active_tables = ActiveTables} = State) ->
    Table = case ActiveTables of
                #{RaNodeId := T} -> T;
                _ ->
                    Dir = filename:join(DataDir, atom_to_list(RaNodeId)),
                    open_file(RaNodeId, Dir)
            end,
    {TblName, _} = Table,

    % brief memory explosion
    Objs =
        ets:foldl(fun ({Idx, _Term, _Data} = E, Acc)
                     when Idx >= StartIdx orelse Idx =< EndIdx ->
                          [E | Acc];
                      (_, Acc) ->
                          Acc
                  end, [], Tid),
    ok = dets:insert(TblName, Objs),
    % fsync
    ok = dets:sync(TblName),

    % TODO: check size and roll over to new tables

    % notify writerid of new table update
    % TODO: should StartIdx be the first index in the table or the first
    % index in the update?
    RaNodeId ! {log_event, {new_tables, [{StartIdx, EndIdx, Tid, Table}]}},

    State#state{active_tables = ActiveTables#{RaNodeId => Table}}.

find_dets_files(Dir) ->
    lists:sort(filelib:wildcard(filename:join(Dir, "*.dets"))).

open_file(Id0, Dir) ->
    Id = atom_to_list(Id0),
    case find_dets_files(Dir) of
        [] ->
            Name0 = Id ++ "_0.dets",
            File = filename:join(Dir, Name0),
            _ = file:make_dir(Dir),
            {ok, Name} = dets:open_file(list_to_atom(Name0), [{file, File}]),
            {Name, File};
        [File | _Old] ->
            Name = list_to_atom(filename:basename(File)),
            {ok, Name} = dets:open_file(Name, [{file, File}]),
            {Name, File}
    end.


