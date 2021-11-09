%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2017-2021 VMware, Inc. or its affiliates.  All rights reserved.
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

-define(AWAIT_TIMEOUT, 30000).

-define(COUNTER_FIELDS,
        [mem_tables,
         segments,
         entries
         ]).

-define(C_MEM_TABLES, 1).
-define(C_SEGMENTS, 2).
-define(C_ENTRIES, 3).

%%% ra_log_segment_writer
%%% receives a set of closed mem_segments from the wal
%%% appends to the current segment for the ra server
%%% notifies the ra server of any new/updates segments


%%%===================================================================
%%% API functions
%%%===================================================================

start_link(#{name := Name} = Config) ->
    gen_server:start_link({local, Name}, ?MODULE, [Config], []).

accept_mem_tables(_SegmentWriter, [], undefined) ->
    ok;
accept_mem_tables(SegmentWriter, Tables, WalFile) ->
    gen_server:cast(SegmentWriter, {mem_tables, Tables, WalFile}).

-spec truncate_segments(atom() | pid(), ra_uid(), ra_log:segment_ref()) -> ok.
truncate_segments(SegWriter, Who, SegRef) ->
    % truncate all closed segment files
    gen_server:cast(SegWriter, {truncate_segments, Who, SegRef}).

-spec my_segments(atom() | pid(), ra_uid()) -> [file:filename()].
my_segments(SegWriter, Who) ->
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

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([#{data_dir := DataDir,
        system := System} = Conf]) ->
    process_flag(trap_exit, true),
    CRef = ra_counters:new(?MODULE, ?COUNTER_FIELDS),
    SegmentConf = maps:get(segment_conf, Conf, #{}),
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

handle_cast({mem_tables, Tables, WalFile}, State) ->
    ok = counters:add(State#state.counter, ?C_MEM_TABLES, length(Tables)),
    Degree = erlang:system_info(schedulers),
    _ = [begin
             {_, Failures} = ra_lib:partition_parallel(
                               fun (E) ->
                                       ok = do_segment(E, State),
                                       true
                               end, Tabs, infinity),
             case Failures of
                 [] ->
                     %% this is what we expect
                     ok;
                 _ ->
                     ?ERROR("segment_writer: ~b failures encounted during segment"
                            " flush. Errors: ~P", [length(Failures), Failures, 32]),
                     exit(segment_writer_segment_write_failure)
             end
         end || Tabs <- ra_lib:lists_chunk(Degree, Tables)],
    % delete wal file once done
    % TODO: test scenario when server crashes after segments but before
    % deleting walfile
    % can we make segment writer idempotent somehow
    ?DEBUG("segment_writer: deleting wal file: ~s",
          [filename:basename(WalFile)]),
    %% temporarily disable wal deletion
    %% TODO: this shoudl be a debug option config?
    % Base = filename:basename(WalFile),
    % BkFile = filename:join([State0#state.data_dir, "wals", Base]),
    % filelib:ensure_dir(BkFile),
    % file:copy(WalFile, BkFile),
    _ = prim_file:delete(WalFile),
    %% ensure we release any bin refs that might have been acquired during
    %% segment write
    true = erlang:garbage_collect(),
    {noreply, State};
handle_cast({truncate_segments, Who, {_From, _To, Name} = SegRef},
            #state{segment_conf = SegConf} = State0) ->
    %% remove all segments below the provided SegRef
    %% Also delete the segref if the file hasn't changed
    Files = segments_for(Who, State0),
    {_Keep, Discard} = lists:splitwith(
                         fun (F) ->
                                 ra_lib:to_string(filename:basename(F)) =/= Name
                         end, lists:reverse(Files)),
    case Discard of
        [] ->
            %% should this be possible?
            {noreply, State0};
        [Pivot | Remove] ->
            %% remove all old files
            _ = [_ = prim_file:delete(F) || F <- Remove],
            %% check if the pivot has changed
            {ok, Seg} = ra_log_segment:open(Pivot, #{mode => read}),
            case ra_log_segment:segref(Seg) of
                SegRef ->
                    %% it has not changed - we can delete that too
                    %% as we are deleting the last segment - create an empty
                    %% successor
                    _ = ra_log_segment:close(
                          open_successor_segment(Seg, SegConf)),
                    _ = ra_log_segment:close(Seg),
                    _ = prim_file:delete(Pivot),
                    {noreply, State0};
                _ ->
                    %% the segment has changed - leave it in place
                    _ = ra_log_segment:close(Seg),
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
      segment_conf => Conf
     }.

do_segment({ServerUId, StartIdx0, EndIdx, Tid},
           #state{system = System,
                  data_dir = DataDir,
                  segment_conf = SegConf} = State) ->
    Dir = filename:join(DataDir, binary_to_list(ServerUId)),

    case open_file(Dir, SegConf) of
        enoent ->
            ?WARN("segment_writer: skipping segment as directory ~s does "
                  "not exist", [Dir]),
            %% clean up the tables for this process
            _ = ets:delete(Tid),
            _ = clean_closed_mem_tables(System, ServerUId, Tid),
            ok;
        Segment0 ->
            case append_to_segment(ServerUId, Tid, StartIdx0, EndIdx,
                                   Segment0, State) of
                undefined ->
                    ?WARN("segment_writer: skipping segments for ~w as
                           directory ~s disappeared whilst writing",
                           [ServerUId, Dir]),
                    ok;
                {Segment1, Closed0} ->
                    % fsync
                    {ok, Segment} = ra_log_segment:sync(Segment1),

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

                    ok = send_segments(System, ServerUId, Tid, SegRefs),
                    ok
            end
    end.

start_index(ServerUId, StartIdx0) ->
    case ets:lookup(ra_log_snapshot_state, ServerUId) of
        [{_, SnapIdx}] ->
            max(SnapIdx + 1, StartIdx0);
        [] ->
            StartIdx0
    end.

send_segments(System, ServerUId, Tid, Segments) ->
    Msg = {ra_log_event, {segments, Tid, Segments}},
    case ra_directory:pid_of(System, ServerUId) of
        undefined ->
            ?DEBUG("ra_log_segment_writer: error sending "
                   "ra_log_event to: "
                   "~s. Error: ~s",
                   [ServerUId, "No Pid"]),
            _ = ets:delete(Tid),
            _ = clean_closed_mem_tables(System, ServerUId, Tid),
            ok;
        Pid ->
            Pid ! Msg,
            ok
    end.

clean_closed_mem_tables(System, UId, Tid) ->
    {ok, ClosedTbl} = ra_system:lookup_name(System, closed_mem_tbls),
    Tables = ets:lookup(ClosedTbl, UId),
    [begin
         ?DEBUG("~w: cleaning closed table for '~s' range: ~b-~b",
                [?MODULE, UId, From, To]),
         %% delete the entry in the closed table lookup
         true = ets:delete_object(ClosedTbl, O)
     end || {_, _, From, To, T} = O <- Tables, T == Tid].

append_to_segment(UId, Tid, StartIdx0, EndIdx, Seg, State) ->
    StartIdx = start_index(UId, StartIdx0),
    % EndIdx + 1 because FP
    append_to_segment(UId, Tid, StartIdx, EndIdx+1, Seg, [], State).

append_to_segment(_, _, StartIdx, EndIdx, Seg, Closed, _State)
  when StartIdx >= EndIdx ->
    {Seg, Closed};
append_to_segment(UId, Tid, Idx, EndIdx, Seg0, Closed, State) ->
    [{_, Term, Data0}] = ets:lookup(Tid, Idx),
    Data = term_to_binary(Data0),
    case ra_log_segment:append(Seg0, Idx, Term, Data) of
        {ok, Seg} ->
            ok = counters:add(State#state.counter, ?C_ENTRIES, 1),
            append_to_segment(UId, Tid, Idx+1, EndIdx, Seg, Closed, State);
        {error, full} ->
            % close and open a new segment
            case open_successor_segment(Seg0, State#state.segment_conf) of
                undefined ->
                    %% a successor cannot be opened - this is most likely due
                    %% to the directory having been deleted.
                    %% clear close mem tables here
                    _ = ets:delete(Tid),
                    _ = clean_closed_mem_tables(State#state.system, UId, Tid),
                    undefined;
                Seg ->
                    ok = counters:add(State#state.counter, ?C_SEGMENTS, 1),
                    %% re-evaluate snapshot state for the server in case
                    %% a snapshot has completed during segment flush
                    StartIdx = start_index(UId, Idx),
                    % recurse
                    append_to_segment(UId, Tid, StartIdx, EndIdx, Seg,
                                      [Seg0 | Closed], State)
            end;
        {error, Posix} ->
            FileName = ra_log_segment:filename(Seg0),
            exit({segment_writer_append_error, FileName, Posix})
    end.

find_segment_files(Dir) ->
    lists:reverse(segment_files(Dir)).

segment_files(Dir) ->
    case prim_file:list_dir(Dir) of
        {ok, Files0} ->
            Files = [filename:join(Dir, F) || F <- Files0, filename:extension(F) == ".segment"],
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
            undefined;
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
            ?WARN("segment_writer: missing header in segment file ~s "
                  "deleting file and retrying recovery", [File]),
            _ = prim_file:delete(File),
            open_file(Dir, SegConf);
        {error, enoent} ->
            ?WARN("segment_writer: failed to open segment file ~s "
                  "error: enoent", [File]),
            enoent;
        Err ->
            %% Any other error should be considered a hard error or else
            %% we'd risk data loss
            ?WARN("segment_writer: failed to open segment file ~s "
                  "error: ~W. Exiting", [File, Err, 10]),
            exit(Err)
    end.
