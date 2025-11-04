%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2017-2025 Broadcom. All Rights Reserved. The term Broadcom refers to Broadcom Inc. and/or its subsidiaries.
%%
%% @hidden
-module(ra_log_segment_writer).
-behaviour(gen_server).

-export([start_link/1,
         accept_mem_tables/3,
         truncate_segments/3,
         my_segments/2,
         await/1,
         overview/1
        ]).

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3
         ]).

-record(state, {data_dir :: file:filename(),
                system :: atom(),
                counter :: counters:counters_ref(),
                segment_conf = #{} :: ra_log_segment:ra_log_segment_options()}).

-include("ra.hrl").

-define(AWAIT_TIMEOUT, 60000).
-define(SEGMENT_WRITER_RECOVERY_TIMEOUT, 30000).

-define(C_MEM_TABLES, 1).
-define(C_SEGMENTS, 2).
-define(C_ENTRIES, 3).
-define(C_BYTES_WRITTEN, 4).

-define(COUNTER_FIELDS,
        [{mem_tables, ?C_MEM_TABLES, counter,
          "Number of in-memory tables handled"},
         {segments, ?C_SEGMENTS, counter,
          "Number of segments written"},
         {entries, ?C_ENTRIES, counter,
          "Number of entries written"},
         {bytes_written, ?C_BYTES_WRITTEN, counter,
          "Number of bytes written"}
        ]).


%%% ra_log_segment_writer
%%% receives a set of closed mem_segments from the wal
%%% appends to the current segment for the ra server
%%% notifies the ra server of any new/updates segments


%%%===================================================================
%%% API functions
%%%===================================================================

start_link(#{name := Name} = Config) ->
    gen_server:start_link({local, Name}, ?MODULE, [Config], []).

-spec accept_mem_tables(atom() | pid(),
                        #{ra_uid() => [{ets:tid(), ra_seq:state()}]},
                        string()) -> ok.
accept_mem_tables(_SegmentWriter, Tables, undefined)
  when map_size(Tables) == 0 ->
    ok;
accept_mem_tables(SegmentWriter, UIdTidRanges, WalFile)
  when is_map(UIdTidRanges) ->
    gen_server:cast(SegmentWriter, {mem_tables, UIdTidRanges, WalFile}).

-spec truncate_segments(atom() | pid(), ra_uid(), ra_log:segment_ref()) -> ok.
truncate_segments(SegWriter, Who, SegRef) ->
    maybe_wait_for_segment_writer(SegWriter, ?SEGMENT_WRITER_RECOVERY_TIMEOUT),
    % truncate all closed segment files
    gen_server:cast(SegWriter, {truncate_segments, Who, SegRef}).

-spec my_segments(atom() | pid(), ra_uid()) -> [file:filename()].
my_segments(SegWriter, Who) ->
    maybe_wait_for_segment_writer(SegWriter, ?SEGMENT_WRITER_RECOVERY_TIMEOUT),
    gen_server:call(SegWriter, {my_segments, Who}, infinity).

-spec overview(atom() | pid()) -> #{}.
overview(SegWriter) ->
    gen_server:call(SegWriter, overview).

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

-define(UPGRADE_MARKER, "segment_name_upgrade_marker").

%%%==================================================================
%%% gen_server callbacks
%%%===================================================================

init([#{data_dir := DataDir,
        name := SegWriterName,
        system := System} = Conf]) ->
    process_flag(trap_exit, true),
    CRef = ra_counters:new(SegWriterName, ?COUNTER_FIELDS,
                           #{ra_system => System,
                             module => ?MODULE}),
    SegmentConf = maps:get(segment_conf, Conf, #{}),
    maybe_upgrade_segment_file_names(System, DataDir),
    {ok, #state{system = System,
                data_dir = DataDir,
                counter = CRef,
                segment_conf = SegmentConf}}.

handle_call(await, _From, State) ->
    {reply, ok, State};
handle_call({my_segments, Who}, _From, State) ->
    SegFiles = segments_for(Who, State),
    {reply, SegFiles, State};
handle_call(overview, _From, State) ->
    {reply, get_overview(State), State}.

segments_for(UId, #state{data_dir = DataDir}) ->
    Dir = filename:join(DataDir, ra_lib:to_list(UId)),
    segment_files(Dir).

handle_cast({mem_tables, UIdTidRanges, WalFile},
            #state{data_dir = Dir,
                   system = System} = State) ->
    T1 = erlang:monotonic_time(),
    ok = counters:add(State#state.counter, ?C_MEM_TABLES, map_size(UIdTidRanges)),
    #{names := Names} = ra_system:fetch(System),
    Degree = erlang:system_info(schedulers),
    %% TODO: refactor to make better use of time where each uid has an
    %% uneven amount of work to do.
    RangesList = maps:fold(
                   fun (UId, TidRanges, Acc) ->
                           case ra_directory:is_registered_uid(Names, UId) of
                               true ->
                                   [{UId, TidRanges} | Acc];
                               false ->
                                   %% delete all tids as the uid is not
                                   %% registered
                                   ?DEBUG("segment_writer in '~ts': deleting memtable "
                                          "for ~ts as not a registered uid",
                                          [System, UId]),
                                   ok = ra_log_ets:delete_mem_tables(Names, UId),
                                   Acc
                           end
                   end, [], UIdTidRanges),

    _ = [begin
             {ok, _, Failures} =
                ra_lib:partition_parallel(
                    fun (TidRange) ->
                            ok = flush_mem_table_ranges(TidRange, State),
                            true
                    end, Tabs, infinity),
             case Failures of
                 [] ->
                     %% this is what we expect
                     ok;
                 _ ->
                     ?ERROR("segment_writer: ~b failures encountered during segment"
                            " flush. Errors: ~P", [length(Failures), Failures, 32]),
                     exit({segment_writer_segment_write_failure, Failures})
             end
         end || Tabs <- ra_lib:lists_chunk(Degree, RangesList)],
    %% delete wal file once done
    ok = prim_file:delete(filename:join(Dir, WalFile)),
    T2 = erlang:monotonic_time(),
    Diff = erlang:convert_time_unit(T2 - T1, native, millisecond),
    ?DEBUG("segment_writer in '~w': completed flush of ~b writers from wal file "
           "~s in ~bms",
          [System, length(RangesList), WalFile, Diff]),
    {noreply, State};
handle_cast({truncate_segments, Who, {Name, _Range} = SegRef},
            #state{segment_conf = SegConf,
                   system = System} = State0) ->
    %% remove all segments below the provided SegRef
    %% Also delete the segref if the file hasn't changed
    T1 = erlang:monotonic_time(),
    Files = segments_for(Who, State0),
    {_Keep, Discard} = lists:splitwith(
                         fun (F) ->
                                 ra_lib:to_binary(filename:basename(F)) =/= Name
                         end, lists:reverse(Files)),
    case Discard of
        [] ->
            %% should this be possible?
            {noreply, State0};
        [Pivot | Remove] ->
            %% remove all old files
            _ = [_ = prim_file:delete(F) || F <- Remove],
            %% check if the pivot has changed
            case ra_log_segment:open(Pivot, #{mode => read}) of
                {ok, Seg} ->
                    case ra_log_segment:segref(Seg) of
                        SegRef ->
                            _ = ra_log_segment:close(Seg),
                            %% it has not changed - we can delete that too
                            _ = prim_file:delete(Pivot),
                            %% as we are deleting the last segment - create an empty
                            %% successor
                            T2 = erlang:monotonic_time(),
                            Diff = erlang:convert_time_unit(T2 - T1, native,
                                                            millisecond),
                            ?DEBUG("segment_writer in '~w': ~s for ~s took ~bms",
                                   [System, ?FUNCTION_NAME, Who, Diff]),
                            case open_successor_segment(Seg, SegConf) of
                                enoent ->
                                    %% directory must have been deleted after the pivot
                                    %% segment was opened
                                    {noreply, State0};
                                Succ ->
                                    _ = ra_log_segment:close(Succ),
                                    {noreply, State0}
                            end;
                        _ ->
                            %% the segment has changed - leave it in place
                            T2 = erlang:monotonic_time(),
                            Diff = erlang:convert_time_unit(T2 - T1, native,
                                                            millisecond),
                            ?DEBUG("segment_writer in '~w': ~s for ~s took ~bms",
                                   [System, ?FUNCTION_NAME, Who, Diff]),
                            _ = ra_log_segment:close(Seg),
                            {noreply, State0}
                    end;
                {error, enoent} ->
                    %% concurrent deletion of segment - assume this ra server
                    %% is gone
                    {noreply, State0}
            end
    end.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%%%===================================================================
%%% Internal functions
%%%===================================================================

get_overview(#state{data_dir = Dir,
                    segment_conf = Conf}) ->
    #{data_dir => Dir,
      segment_conf => Conf}.

flush_mem_table_ranges({ServerUId, TidSeqs0},
                       #state{system = System} = State) ->
    SmallestIdx = smallest_live_idx(ServerUId),
    LiveIndexes = live_indexes(ServerUId),
    LastLive = ra_seq:last(LiveIndexes),
    %% TidSeqs arrive here sorted new -> old.

    %% TODO: use live indexes from ra_log_snapshot_state table to only
    %% write live entries below the snapshot index

    %% truncate and limit all seqa to create a contiguous non-overlapping
    %% list of tid ranges to flush to disk
    TidSeqs = lists:foldl(
                fun ({T, Seq0}, []) ->
                        case ra_seq:floor(SmallestIdx, Seq0) of
                            [] ->
                                [];
                            Seq when LiveIndexes == []->
                                [{T, Seq}];
                            Seq ->
                                L = ra_seq:in_range(ra_seq:range(Seq),
                                                    LiveIndexes),

                                [{T, ra_seq:add(ra_seq:floor(LastLive + 1, Seq), L)}]
                        end;
                    ({T, Seq0}, [{_T, PrevSeq} | _] = Acc) ->
                        Start = ra_seq:first(PrevSeq),
                        Seq1 = ra_seq:floor(SmallestIdx, Seq0),
                        case ra_seq:limit(Start, Seq1) of
                            [] ->
                                Acc;
                            Seq when LiveIndexes == [] ->
                                [{T, Seq} | Acc];
                            Seq ->
                                L = ra_seq:in_range(ra_seq:range(Seq),
                                                    LiveIndexes),
                                [{T, ra_seq:add(ra_seq:floor(LastLive + 1, Seq), L)}
                                | Acc]
                        end
                end, [], TidSeqs0),

    SegRefs0 = lists:append(
                 lists:reverse(
                   %% segrefs are returned in appended order so new -> old
                   %% so we need to reverse them so that the final appended list
                   %% of segrefs is in the old -> new order
                   [flush_mem_table_range(ServerUId, TidSeq, State)
                    || TidSeq <- TidSeqs])),

    %% compact cases where a segment was appended in a subsequent call to
    %% flush_mem_table_range
    %% the list of segrefs is returned in new -> old order which is the same
    %% order they are kept by the ra_log
    SegRefs = lists:reverse(
                lists:foldl(
                  fun ({FILE, _}, [{FILE, _} | _] = Acc) ->
                          Acc;
                      (Seg, Acc) ->
                          [Seg | Acc]
                  end, [], SegRefs0)),

    ok = send_segments(System, ServerUId, TidSeqs0, SegRefs),
    ok.

flush_mem_table_range(ServerUId, {Tid, Seq},
                      #state{data_dir = DataDir,
                             segment_conf = SegConf} = State) ->
    Dir = filename:join(DataDir, binary_to_list(ServerUId)),
    case open_file(Dir, SegConf) of
        enoent ->
            ?DEBUG("segment_writer: skipping segment as directory ~ts does "
                   "not exist", [Dir]),
            %% TODO: delete mem table
            %% clean up the tables for this process
            [];
        Segment0 ->
            case append_to_segment(ServerUId, Tid, Seq, Segment0, State) of
                undefined ->
                    ?WARN("segment_writer: skipping segments for ~w as
                           directory ~ts disappeared whilst writing",
                           [ServerUId, Dir]),
                    [];
                {Segment, Closed0} ->
                    % notify writerid of new segment update
                    % includes the full range of the segment
                    % filter out any undefined segrefs
                    ClosedSegRefs = [ra_log_segment:segref(S)
                                     || S <- Closed0,
                                        %% ensure we don't send undefined seg refs
                                        is_tuple(ra_log_segment:segref(S))],
                    SegRefs = case ra_log_segment:segref(Segment) of
                                  undefined ->
                                      ClosedSegRefs;
                                  SRef ->
                                      [SRef | ClosedSegRefs]
                              end,

                    _ = ra_log_segment:close(Segment),
                    SegRefs
            end
    end.

start_index(ServerUId, StartIdx0) ->
    max(smallest_live_idx(ServerUId), StartIdx0).

smallest_live_idx(ServerUId) ->
    ra_log_snapshot_state:smallest(ra_log_snapshot_state, ServerUId).

live_indexes(ServerUId) ->
    ra_log_snapshot_state:live_indexes(ra_log_snapshot_state, ServerUId).

send_segments(System, ServerUId, TidRanges, SegRefs) ->
    case ra_directory:pid_of(System, ServerUId) of
        undefined ->
            ?DEBUG("ra_log_segment_writer: unable to send "
                   "ra_log_event to: "
                   "~ts. Reason: ~s",
                   [ServerUId, "No Pid"]),
            %% delete from the memtable on the non-running server's behalf
            [begin
                 %% TODO: HACK: this is a hack to get a full range out of a
                 %% sequent, ideally the mt should take the ra_seq and
                 %% delete from that
                 _  = catch ra_mt:delete({indexes, Tid, Seq})
             end || {Tid, Seq} <- TidRanges],
            ok;
        Pid ->
            Pid ! {ra_log_event, {segments, TidRanges, SegRefs}},
            ok
    end.

append_to_segment(UId, Tid, Seq0, Seg, State) ->
    FirstIdx = case ra_seq:first(Seq0) of
                   undefined ->
                       0;
                   Fst ->
                       Fst
               end,
    StartIdx = start_index(UId, FirstIdx),
    %% TODO combine flor and iterator into one operation
    Seq = ra_seq:floor(StartIdx, Seq0),
    SeqIter = ra_seq:iterator(Seq),
    append_to_segment(UId, Tid, ra_seq:next(SeqIter), Seg, [], State).

append_to_segment(_, _, end_of_seq, Seg, Closed, _State) ->
    {Seg, Closed};
append_to_segment(UId, Tid, {Idx, SeqIter} = Cur, Seg0, Closed, State) ->
    try ets:lookup(Tid, Idx) of
        [] ->
            StartIdx = start_index(UId, Idx),
            case Idx < StartIdx of
                true ->
                    %% a snapshot must have been completed after we last checked
                    %% the start idx, continue flush from new start index.
                    append_to_segment(UId, Tid, next_ra_seq(StartIdx, SeqIter), Seg0,
                                      Closed, State);
                false ->
                    %% oh dear, an expected index was not found in the mem table.
                    ?WARN("segment_writer: missing index ~b in mem table for uid ~s"
                          "start index ~b checking to see if UId has been unregistered",
                          [Idx, UId, StartIdx]),
                    case ra_directory:is_registered_uid(State#state.system, UId) of
                        true ->
                            ?ERROR("segment_writer: uid ~ts is registered, exiting...",
                                   [UId]),
                            exit({missing_index, UId, Idx});
                        false ->
                            ?INFO("segment_writer: uid ~ts was not registered, skipping",
                                  [UId]),
                            undefined
                    end
            end;
        [{Idx, Term, Data0}] ->
            Data = term_to_iovec(Data0),
            DataSize = iolist_size(Data),
            case ra_log_segment:append(Seg0, Idx, Term, {DataSize, Data}) of
                {ok, Seg} ->
                    ok = counters:add(State#state.counter, ?C_ENTRIES, 1),
                    %% this isn't completely accurate as firstly the segment may not
                    %% have written it to disk and it doesn't include data written to
                    %% the segment index but is probably good enough to get comparative
                    %% data rates for different Ra components
                    ok = counters:add(State#state.counter, ?C_BYTES_WRITTEN, DataSize),
                    append_to_segment(UId, Tid, ra_seq:next(SeqIter), Seg, Closed, State);
                {error, full} ->
                    % close and open a new segment
                    case open_successor_segment(Seg0, State#state.segment_conf) of
                        enoent ->
                            %% a successor cannot be opened - this is most likely due
                            %% to the directory having been deleted.
                            undefined;
                        Seg ->
                            ok = counters:add(State#state.counter, ?C_SEGMENTS, 1),
                            %% re-evaluate snapshot state for the server in case
                            %% a snapshot has completed during segment flush
                            StartIdx = start_index(UId, Idx),
                            Next = case StartIdx == Idx of
                                       true ->
                                           Cur;
                                       false ->
                                           next_ra_seq(StartIdx, SeqIter)
                                   end,
                            append_to_segment(UId, Tid, Next,
                                              Seg, [Seg0 | Closed], State)
                    end;
                {error, Posix} ->
                    FileName = ra_log_segment:filename(Seg0),
                    exit({segment_writer_append_error, FileName, Posix})
            end
    catch _:badarg ->
              ?INFO("segment_writer: uid ~s ets table deleted", [UId]),
              %% ets table has been deleted.
              %% this could be due to two reasons
              %% 1. the ra server has been deleted.
              %% 2. an old mem table has been deleted due to snapshotting
              %% but the member is still active
              %% skipping this table
              undefined

    end.

find_segment_files(Dir) ->
    lists:reverse(segment_files(Dir)).

segment_files(Dir) ->
    case prim_file:list_dir(Dir) of
        {ok, Files0} ->
            Files = [filename:join(Dir, F)
                     || F <- Files0, filename:extension(F) =:= ".segment"],
            lists:sort(Files);
        {error, enoent} ->
            []
    end.

open_successor_segment(CurSeg, SegConf) ->
    Fn0 = ra_log_segment:filename(CurSeg),
    Fn = ra_lib:zpad_filename_incr(Fn0),
    ok = ra_log_segment:close(CurSeg),
    case ra_log_segment:open(Fn, SegConf) of
        {error, enoent} ->
            %% the directory has been deleted whilst segments were being
            %% written
            enoent;
        {ok, Seg} ->
            Seg
    end.

open_file(Dir, SegConf) ->
    File = case find_segment_files(Dir) of
               [] ->
                   F = ra_lib:zpad_filename("", "segment", 1),
                   filename:join(Dir, F);
               [F | _] ->
                   F
           end,
    %% There is a chance we'll get here without the target directory
    %% existing which could happen after server deletion
    case ra_log_segment:open(File, SegConf#{mode => append}) of
        {ok, Segment} ->
            Segment;
        {error, missing_segment_header} ->
            %% a file was created by the segment header had not been
            %% synced. In this case it is typically safe to just delete
            %% and retry.
            ?WARN("segment_writer: missing header in segment file ~ts "
                  "deleting file and retrying recovery", [File]),
            _ = prim_file:delete(File),
            open_file(Dir, SegConf);
        {error, enoent} ->
            ?DEBUG("segment_writer: failed to open segment file ~ts "
                  "error: enoent", [File]),
            enoent;
        Err ->
            %% Any other error should be considered a hard error or else
            %% we'd risk data loss
            ?WARN("segment_writer: failed to open segment file ~ts "
                  "error: ~W. Exiting", [File, Err, 10]),
            exit(Err)
    end.

maybe_wait_for_segment_writer(_SegWriter, TimeRemaining)
  when TimeRemaining < 0 ->
    error(segment_writer_not_available);
maybe_wait_for_segment_writer(SegWriter, TimeRemaining)
  when is_atom(SegWriter) ->
    case whereis(SegWriter) of
        undefined ->
            %% segment writer isn't available yet, sleep a bit
            timer:sleep(10),
            maybe_wait_for_segment_writer(SegWriter, TimeRemaining - 10);
        _ ->
            ok
    end;
maybe_wait_for_segment_writer(_SegWriter, _TimeRemaining) ->
    ok.

maybe_upgrade_segment_file_names(System, DataDir) ->
    Marker = filename:join(DataDir, ?UPGRADE_MARKER),
    case ra_lib:is_file(Marker) of
        false ->
            ?INFO("segment_writer: upgrading segment file names to "
                  "new format in dirctory ~ts",
                  [DataDir]),
            [begin
                 Dir = filename:join(DataDir, UId),
                 case prim_file:list_dir(Dir) of
                     {ok, Files} ->
                         [ra_lib:zpad_upgrade(Dir, F, ".segment")
                          || F <- Files, filename:extension(F) =:= ".segment"];
                     {error, enoent} ->
                         ok
                 end
             end || {_, UId} <- ra_directory:list_registered(System)],

            ok = ra_lib:write_file(Marker, <<>>);
        true ->
            ok
    end.

next_ra_seq(Idx, Iter0) ->
    case ra_seq:next(Iter0) of
        end_of_seq ->
            end_of_seq;
        {I, _} = Next
          when I >= Idx ->
            Next;
        {_, Iter} ->
            next_ra_seq(Idx, Iter)
    end.
