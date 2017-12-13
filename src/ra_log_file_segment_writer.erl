-module(ra_log_file_segment_writer).
-behaviour(gen_server).

-export([start_link/1,
         accept_mem_tables/2,
         accept_mem_tables/3,
         delete_segments/3,
         delete_segments/4,
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
                segment_conf = #{} :: #{atom() => term()}, % TODO refine type
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

-spec delete_segments(pid() | atom(), ra_index(),
                      [ra_log:ra_segment_ref()]) -> ok.
delete_segments(Who, SnapIdx, SegmentFiles) ->
    delete_segments(?MODULE, Who, SnapIdx, SegmentFiles).

-spec delete_segments(pid() | atom(), pid() | atom(),
                      ra_index(), [ra_log:ra_segment_ref()]) ->
    ok.
delete_segments(SegWriter, Who, SnapIdx, [MaybeActive | SegmentFiles]) ->
    % delete all closed segment files
    % TODO: this is simplistic
    [ok = file:delete(F) || {_, _, F} <- SegmentFiles],
    gen_server:cast(SegWriter, {delete_segment, Who , SnapIdx, MaybeActive}).

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
            gen_server:call(SegWriter, await, 30000);
        false ->
            % if it is down it isn't processing anything
            ok
    end.

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([#{data_dir := DataDir} = Conf]) ->
    SegmentConf = maps:get(segment_conf, Conf, #{}),
    {ok, #state{data_dir = DataDir,
                segment_conf = SegmentConf}}.

handle_call(await, _From, State) ->
    {reply, ok, State}.

handle_cast({mem_tables, Tables, WalFile}, State0) ->
    State = lists:foldl(fun do_segment/2, State0, Tables),
    % delete wal file once done
    % TODO: test scenario when node crashes after segments but before
    % deleting walfile
    % can we make segment writer idempotent somehow
    _ = file:delete(WalFile),

    {noreply, State};
handle_cast({delete_segment, Who, Idx, {_, _, SegmentFile}},
            #state{active_segments = ActiveSegments} = State0) ->
    case ActiveSegments of
        #{Who := Seg} ->
            case ra_log_file_segment:filename(Seg) of
                SegmentFile ->
                    % the segment file is the correct one
                    case ra_log_file_segment:range(Seg) of
                        {_From, To} when To =< Idx ->
                            % segment can be deleted
                            ok = ra_log_file_segment:close(Seg),
                            ok = file:delete(SegmentFile),
                            {noreply,
                             State0#state{active_segments = maps:remove(Who, ActiveSegments)}};
                        _ ->
                            {noreply, State0}
                    end;
                _ ->
                    ok = file:delete(SegmentFile),
                    {noreply, State0}
            end;
        _ ->
            % if it isn't active we can just delete it
            ok = file:delete(SegmentFile),
            {noreply, State0}
    end.



handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{active_segments = ActiveSegments}) ->
    % ensure any open segments are closed
    [ok = ra_log_file_segment:close(Seg)
     || Seg <- maps:values(ActiveSegments)],
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

do_segment({RaNodeId, StartIdx, EndIdx, Tid},
           #state{data_dir = DataDir,
                  segment_conf = SegConf,
                  active_segments = ActiveSegments} = State) ->
    Dir = filename:join(DataDir, atom_to_list(RaNodeId)),
    Segment0 = case ActiveSegments of
                  #{RaNodeId := S} -> S;
                  _ -> open_file(Dir, SegConf)
              end,

    % TODO: replace with recursive function to avoid creating a potentially
    % vary large list of integers
    {Segment, Closed0} =
    lists:foldl(fun (Idx, {Seg0, Segs}) ->
                        % TODO: the question here is whether we should allow
                        % missing indexes or not?
                        % TODO: how to handle cases when the Tid is no longer around
                        % due to the writer process having exited?
                        [{Idx, Term, Data0}] = ets:lookup(Tid, Idx),
                        Data = term_to_binary(Data0),
                        case ra_log_file_segment:append(Seg0, Idx, Term, Data) of
                            {ok, Seg} ->
                                {Seg, Segs};
                            {error, full} ->
                                % close and open a new segment
                                ok = ra_log_file_segment:sync(Seg0),
                                ok = ra_log_file_segment:close(Seg0),
                                Seg1 = open_successor_segment(Seg0, SegConf),
                                {ok, Seg} = ra_log_file_segment:append(Seg1, Idx, Term, Data),
                                {Seg, [Seg0 | Segs]}
                        end
                end, {Segment0, []}, lists:seq(StartIdx, EndIdx)),
    % fsync
    ok = ra_log_file_segment:sync(Segment),

    % notify writerid of new segment update
    % includes the full range of the segment
    Segments = [begin
                    {Start, End} = ra_log_file_segment:range(S),
                    {Start, End, ra_log_file_segment:filename(S)}
                end || S <- [Segment | Closed0]],

    % TODO: better handle and log errors
    catch (RaNodeId ! {ra_log_event, {segments, Tid, Segments}}),

    State#state{active_segments = ActiveSegments#{RaNodeId => Segment}}.

find_segment_files(Dir) ->
    lists:reverse(
      lists:sort(filelib:wildcard(filename:join(Dir, "*.segment")))).

open_successor_segment(CurSeg, SegConf) ->
    Fn0 = ra_log_file_segment:filename(CurSeg),
    Fn = ra_lib:zpad_filename_incr(Fn0),
    ok = ra_log_file_segment:close(CurSeg),
    {ok, Seg} = ra_log_file_segment:open(Fn, SegConf),
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
    {ok, Segment} = ra_log_file_segment:open(File, SegConf#{mode => append}),
    Segment.


