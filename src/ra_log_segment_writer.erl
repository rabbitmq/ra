-module(ra_log_segment_writer).
-behaviour(gen_server).

-export([start_link/1,
         accept_mem_tables/2,
         accept_mem_tables/3,
         delete_segments/3,
         delete_segments/4,
         release_segments/2,
         my_segments/1,
         my_segments/2,
         await/0,
         await/1
        ]).

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {data_dir :: file:filename(),
                segment_conf = #{} :: ra_log_segment:ra_log_segment_options(),
                active_segments = #{} :: #{ra_uid() =>
                                           ra_log_segment:state()}}).

-include("ra.hrl").

-define(AWAIT_TIMEOUT, 30000).

%%% ra_log_segment_writer
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

-spec delete_segments(ra_uid(), ra_index(),
                      [ra_log:ra_segment_ref()]) -> ok.
delete_segments(Who, SnapIdx, SegmentFiles) ->
    delete_segments(?MODULE, Who, SnapIdx, SegmentFiles).

-spec delete_segments(atom() | pid(), ra_uid(),
                      ra_index(), [ra_log:ra_segment_ref()]) -> ok.
delete_segments(SegWriter, Who, SnapIdx, [MaybeActive | SegmentFiles]) ->
    % delete all closed segment files
    [_ = file:delete(F) || {_, _, F} <- SegmentFiles],
    gen_server:cast(SegWriter, {delete_segment, Who, SnapIdx, MaybeActive}).

-spec release_segments(atom() | pid(), ra_uid()) -> ok.
release_segments(SegWriter, Who) ->
    gen_server:call(SegWriter, {release_segments, Who}).

-spec my_segments(ra_uid()) -> [file:filename()].
my_segments(Who) ->
    my_segments(?MODULE, Who).

-spec my_segments(atom() | pid(), ra_uid()) -> [file:filename()].
my_segments(SegWriter, Who) ->
    gen_server:call(SegWriter, {my_segments, Who}, infinity).

% used to wait for the segment writer to finish processing anything in flight
await() ->
    await(?MODULE).

await(SegWriter)  ->
    IsAlive = fun IsAlive(undefined) -> false;
                  IsAlive(P) when is_pid(P) ->
                            is_process_alive(P);
                  IsAlive(A) when is_atom(A) ->
                            IsAlive(whereis(A))
              end,
    case IsAlive(SegWriter) of
        true ->
            gen_server:call(SegWriter, await, ?AWAIT_TIMEOUT);
        false ->
            % if it is down it isn't processing anything
            ok
    end.

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([#{data_dir := DataDir} = Conf]) ->
    process_flag(trap_exit, true),
    SegmentConf = maps:get(segment_conf, Conf, #{}),
    {ok, #state{data_dir = DataDir,
                segment_conf = SegmentConf}}.

handle_call(await, _From, State) ->
    {reply, ok, State};
handle_call({release_segments, Who}, _From,
            #state{active_segments = ActiveSegments0} = State0) ->
    case maps:take(Who, ActiveSegments0) of
        {Seg, ActiveSegments} ->
            % defensive
            _ = ra_log_segment:close(Seg),
            {reply, ok, State0#state{active_segments = ActiveSegments}};
        error ->
            {reply, ok, State0}
    end;
handle_call({my_segments, Who}, _From,
            #state{data_dir = DataDir} = State) ->
    Dir = filename:join(DataDir, ra_lib:to_list(Who)),
    SegFiles = lists:sort(filelib:wildcard(filename:join(Dir, "*.segment"))),
    {reply, SegFiles, State}.

handle_cast({mem_tables, Tables, WalFile}, State0) ->
    State = lists:foldl(fun do_segment/2, State0, Tables),
    % delete wal file once done
    % TODO: test scenario when node crashes after segments but before
    % deleting walfile
    % can we make segment writer idempotent somehow
    ?INFO("segment_writer: deleting wal file: ~p", [WalFile]),
    %% temporarily disable wal deletion
    %% TODO: this shoudl be a debug option config?
    % Base = filename:basename(WalFile),
    % BkFile = filename:join([State0#state.data_dir, "wals", Base]),
    % filelib:ensure_dir(BkFile),
    % file:copy(WalFile, BkFile),
    _ = file:delete(WalFile),
    {noreply, State};
handle_cast({delete_segment, Who, Idx, {_, _, SegmentFile}},
            #state{active_segments = ActiveSegments} = State0) ->
    case ActiveSegments of
        #{Who := Seg} ->
            case ra_log_segment:filename(Seg) of
                SegmentFile ->
                    % the segment file is the correct one
                    case ra_log_segment:range(Seg) of
                        {_From, To} when To =< Idx ->
                            % segment can be deleted
                            ok = ra_log_segment:close(Seg),
                            ok = file:delete(SegmentFile),
                            {noreply,
                             State0#state{active_segments =
                                          maps:remove(Who, ActiveSegments)}};
                        _ ->
                            {noreply, State0}
                    end;
                _ ->
                    %% don't validate the file got deleted as it is possible
                    %% to get multiple requests under high load
                    _ = file:delete(SegmentFile),
                    {noreply, State0}
            end;
        _ ->
            % if it isn't active we can just delete it
            _ = file:delete(SegmentFile),
            {noreply, State0}
    end.


handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{active_segments = ActiveSegments}) ->
    % ensure any open segments are closed
    [ok = ra_log_segment:close(Seg)
     || Seg <- maps:values(ActiveSegments)],
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

do_segment({RaNodeUId, StartIdx, EndIdx, Tid},
           #state{data_dir = DataDir,
                  segment_conf = SegConf,
                  active_segments = ActiveSegments} = State) ->
    Dir = filename:join(DataDir, binary_to_list(RaNodeUId)),
    case filelib:is_dir(Dir) of
        true ->
            %% check that the directory exists -
            %% if it does not exist the node has
            %% never been started or has been deleted
            %% and we should just continue
            Segment0 = case ActiveSegments of
                           #{RaNodeUId := S} -> S;
                           _ -> open_file(Dir, SegConf)
                       end,
            case Segment0 of
                undefined ->
                    State;
                _ ->
                    {Segment1, Closed0} =
                        append_to_segment(Tid, StartIdx, EndIdx,
                                          Segment0, SegConf),
                    % fsync
                    {ok, Segment} = ra_log_segment:sync(Segment1),

                    % notify writerid of new segment update
                    % includes the full range of the segment
                    Segments = [begin
                                    {Start, End} = ra_log_segment:range(S),
                                    {Start, End, ra_log_segment:filename(S)}
                                end || S <- [Segment | Closed0]],

                    Msg = {ra_log_event, {segments, Tid, Segments}},
                    try ra_directory:send(RaNodeUId, Msg) of
                        _ -> ok
                    catch
                        ErrType:Err ->
                            ?INFO("ra_log_segment_writer: error sending "
                                  "ra_log_event to: "
                                  "~s. Error:~n~w:~W~n",
                                  [RaNodeUId, ErrType, Err, 7])
                    end,
                    State#state{active_segments =
                                ActiveSegments#{RaNodeUId => Segment}}
            end;
        false ->
            ?INFO("segment_writer: skipping segment as directory ~s does "
                  "not exist~n", [Dir]),
            State
    end.

append_to_segment(Tid, StartIdx, EndIdx, Seg, SegConf) ->
    % EndIdx + 1 because FP
    append_to_segment(Tid, StartIdx, EndIdx+1, Seg, [], SegConf).

append_to_segment(_Tid, EndIdx, EndIdx, Seg, Closed, _SegConf) ->
    {Seg, Closed};
append_to_segment(Tid, Idx, EndIdx, Seg0, Closed, SegConf) ->
    [{Idx, Term, Data0}] = ets:lookup(Tid, Idx),
    Data = term_to_binary(Data0),
    case ra_log_segment:append(Seg0, Idx, Term, Data) of
        {ok, Seg} ->
            append_to_segment(Tid, Idx+1, EndIdx, Seg, Closed, SegConf);
        {error, full} ->
            % close and open a new segment
            Seg1 = open_successor_segment(Seg0, SegConf),
            {ok, Seg} = ra_log_segment:append(Seg1, Idx, Term, Data),
            append_to_segment(Tid, Idx+1, EndIdx, Seg, [Seg0 | Closed], SegConf)
    end.

find_segment_files(Dir) ->
    lists:reverse(
      lists:sort(filelib:wildcard(filename:join(Dir, "*.segment")))).

open_successor_segment(CurSeg, SegConf) ->
    Fn0 = ra_log_segment:filename(CurSeg),
    Fn = ra_lib:zpad_filename_incr(Fn0),
    ok = ra_log_segment:close(CurSeg),
    {ok, Seg} = ra_log_segment:open(Fn, SegConf),
    Seg.

open_file(Dir, SegConf) ->
    File = case find_segment_files(Dir) of
               [] ->
                   F = ra_lib:zpad_filename("", "segment", 1),
                   Fn = filename:join(Dir, F),
                   _ = file:make_dir(Dir),
                   Fn;
               [F | _Old] ->
                   F
           end,
    %% there is a small chance we'll get here without the target directory
    %% existing this could happend during node deletion
    case filelib:ensure_dir(File) of
        ok ->
            case ra_log_segment:open(File, SegConf#{mode => append}) of
                {ok, Segment} ->
                    Segment;
                Err ->
                    ?WARN("segment_writer: failed to open segment file ~w"
                          "error: ~W", [File, Err]),
                    undefined
            end;
        {error, Err} ->
            ?WARN("segment_writer: failed to create directory ~w, Err: ~w~n",
                  [File, Err]),
              undefined
    end.
