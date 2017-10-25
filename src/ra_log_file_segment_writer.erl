-module(ra_log_file_segment_writer).
-behaviour(gen_server).

-export([start_link/1,
         accept_mem_tables/2,
         accept_mem_tables/3
        ]).

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {data_dir :: filename:filename(),
                active_segments = #{} :: #{atom() => ra_log_file_segment:state()}}).

-include("ra.hrl").

%%% ra_log_file_segment_writer
%%% receives a set of closed mem_segments from the wal
%%% appends to the current segment for the ra node
%%% notifies the ra node of any new/updates segments


%%%===================================================================
%%% API functions
%%%===================================================================

start_link(Config) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [Config], []).


accept_mem_tables(Tables, WalFile) ->
    accept_mem_tables(?MODULE, Tables, WalFile).

accept_mem_tables(_SegmentWriter, [], undefined) ->
    ok;
accept_mem_tables(SegmentWriter, Tables, WalFile) ->
    gen_server:cast(SegmentWriter, {mem_tables, Tables, WalFile}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([#{data_dir := DataDir}]) ->
    {ok, #state{data_dir = DataDir}}.

handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast({mem_tables, Tables, WalFile}, State0) ->
    State = lists:foldl(fun do_segment/2, State0, Tables),
    % delete wal file once done
    ok = file:delete(WalFile),

    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{active_segments = Segments}) ->
    % ensure any open segments are closed
    [ok = ra_log_file_segment:close(Seg) || Seg <- maps:values(Segments)],
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

do_segment({RaNodeId, StartIdx, EndIdx, Tid},
         #state{data_dir = DataDir,
                active_segments = Segments} = State) ->
    % ?DBG("do_segment: range: ~p-~p ETS: ~p", [StartIdx, EndIdx, ets:tab2list(Tid)]),
    Segment0 = case Segments of
                  #{RaNodeId := S} -> S;
                  _ ->
                      Dir = filename:join(DataDir, atom_to_list(RaNodeId)),
                      open_file(RaNodeId, Dir)
              end,

    % TODO: replace with recursive function
    Segment =
        lists:foldl(fun (Idx, Seg0) ->
                         [{Idx, Term, Data0}] = ets:lookup(Tid, Idx),
                          Data = term_to_binary(Data0),
                          {ok, Seg} = ra_log_file_segment:append(Seg0, Idx, Term, Data),
                          Seg
                  end, Segment0, lists:seq(StartIdx, EndIdx)),
    % fsync
    ok = ra_log_file_segment:sync(Segment),
    % TODO: check size and roll over to new segement(s)

    % notify writerid of new segment update
    % TODO: should StartIdx be the first index in the segment or the first
    % index in the update?
    SegFile = ra_log_file_segment:filename(Segment),
    RaNodeId ! {ra_log_event, {new_segments, [{StartIdx, EndIdx, Tid, SegFile}]}},

    State#state{active_segments = Segments#{RaNodeId => Segment}}.

find_segment_files(Dir) ->
    lists:sort(filelib:wildcard(filename:join(Dir, "*.segment"))).

open_file(Id0, Dir) ->
    Id = atom_to_list(Id0),
    case find_segment_files(Dir) of
        [] ->
            Name0 = Id ++ "_0.segment",
            File = filename:join(Dir, Name0),
            _ = file:make_dir(Dir),
            {ok, Segment} = ra_log_file_segment:open(File, #{mode => append}),
            Segment;
        [File | _Old] ->
            {ok, Segment} = ra_log_file_segment:open(File, #{mode => append}),
            Segment
    end.


