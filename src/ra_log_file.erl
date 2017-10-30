-module(ra_log_file).
-behaviour(ra_log).
-export([init/1,
         close/1,
         append/3,
         take/3,
         last_index_term/1,
         handle_event/2,
         last_written/1,
         fetch/2,
         fetch_term/2,
         next_index/1,
         write_snapshot/2,
         read_snapshot/1,
         read_meta/2,
         write_meta/3,
         sync_meta/1
        ]).

-include("ra.hrl").

% -type tid() :: reference() | atom().

-record(state,
        {first_index = -1 :: ra_index(),
         last_index = -1 :: -1 | ra_index(),
         last_written_index_term = {0, 0} :: ra_idxterm(),
         id :: atom(),
         segment_refs = [] :: [ra_log:ra_segment_ref()],
         open_segments = #{} :: #{file:filename() => ra_log_file_segment:state()},
         directory :: list(),
         kv :: reference(),
         cache = #{} :: #{ra_index() => {ra_term(), log_entry()}}
        }).

-type ra_log_file_state() :: #state{}.


-spec init(ra_log:ra_log_init_args()) -> ra_log_file_state().
init(#{directory := Dir, id := Id}) ->
    Dets = filename:join(Dir, "ra_log_kv.dets"),
    ok = filelib:ensure_dir(Dets),
    {ok, Kv} = dets:open_file(Dets, []),
    % index recovery is done by the storage engine
    % at some point we need to recover some kind of index of
    % flushed segments
    {FirstIndex, LastIndex} = last_index_from_mem_table(Id),
    State0 = #state{directory = Dir, id = Id,
                    first_index = FirstIndex,
                    last_index = LastIndex, kv = Kv},

    % initialized with a default 0 index 0 term dummy value
    % and an empty meta data map
    State = maybe_append_0_0_entry(State0),
    ?DBG("ra_log_file recovered last_index_term ~p~n", [last_index_term(State)]),
    State.

-spec close(ra_log_file_state()) -> ok.
close(#state{kv = Kv}) ->
    % deliberately ignoring return value
    _ = dets:close(Kv),
    ok.

write(State = #state{id = Id, cache = Cache},
      {Idx, Term, Data}) ->
    ok = ra_log_wal:write(Id, ra_log_wal, Idx, Term, Data),
    State#state{last_index = Idx, cache = Cache#{Idx => {Term, Data}}}.

-spec append(Entry :: log_entry(),
             overwrite | no_overwrite,
             State :: ra_log_file_state()) ->
    {queued, ra_log_file_state()} |
    {error, integrity_error}.
append(Entry, no_overwrite, #state{last_index = LastIdx})
      when element(1, Entry) =< LastIdx ->
    {error, integrity_error};
append(Entry, overwrite, State) ->
    {queued, write(State, Entry)};
append(Entry, no_overwrite, State = #state{last_index = LastIdx})
      when element(1, Entry) > LastIdx ->
    {queued, write(State, Entry)}.


-spec take(ra_index(), non_neg_integer(), ra_log_file_state()) ->
    {[log_entry()], ra_log_file_state()}.
take(Start, Num, #state{} = State) ->
    % TODO: refactor
    lists:foldl(fun (Idx, {Acc0, St0}) ->
                        {Res, St} = fetch(Idx, St0),
                        {maybe_append(Res, Acc0), St}
                end, {[], State}, lists:seq(Start + Num - 1, Start, -1)).

maybe_append(undefined, L) ->
    L;
maybe_append(I, L) ->
    [I | L].

-spec last_index_term(ra_log_file_state()) -> maybe(ra_idxterm()).
last_index_term(#state{last_index = LastIdx} = State0) ->
    % TODO: stash last term as well as index to avoid lookup cost
    case fetch_term(LastIdx, State0) of
        {undefined, State} ->
            % If not found fall back on snapshot if snapshot matches last term.
            case read_snapshot(State) of
                {LastIdx, LastTerm, _, _} ->
                    {LastIdx, LastTerm};
                _ ->
                    undefined
            end;
        {Term, _State} ->
            {LastIdx, Term}
    end.

-spec last_written(ra_log_file_state()) -> ra_idxterm().
last_written(#state{last_written_index_term = LWTI}) ->
    LWTI.

-spec handle_event(ra_log:ra_log_event(), ra_log_file_state()) ->
    ra_log_file_state().
handle_event({written, {Idx, Term} = IdxTerm}, State0) ->
    case fetch_term(Idx, State0) of
        {Term, State} ->
            % TODO: this case truncation shouldn't be too expensive as the cache
            % only containes the unflushed window of entries typically less than
            % 10ms worth of entries
            truncate_cache(Idx, State#state{last_written_index_term = IdxTerm});
        _ ->
            State0
    end;
handle_event({segments, Tid, NewSegs},
             #state{id = Id, segment_refs = SegmentRefs} = State0) ->
    % Append new segment refs
    % TODO: some segments could possibly be recovered at this point if the new
    % segments already cover their ranges
    % mem_table cleanup
    % TODO: measure - if this proves expensive it could be done in a separate processes
    % First remove the closed mem table reference
    ClsdTbl = lists_find(fun ({_, _, _, T}) -> T =:= Tid end,
                         ets:lookup(ra_log_closed_mem_tables, Id)),
    false = ClsdTbl =:= undefined, % assert table was found undefined
    true = ets:delete_object(ra_log_closed_mem_tables, ClsdTbl),
    % Then delete the actual ETS table
    true = ets:delete(Tid),
    State0#state{segment_refs = NewSegs ++ SegmentRefs}.

-spec next_index(ra_log_file_state()) -> ra_index().
next_index(#state{last_index = LastIdx}) ->
    LastIdx + 1.

-spec fetch(ra_index(), ra_log_file_state()) ->
    {maybe(log_entry()), ra_log_file_state()}.
fetch(Idx, #state{last_index = Last, first_index = First} = State)
  when Idx > Last orelse Idx < First ->
    {undefined, State};
fetch(Idx, #state{id = Id, cache = Cache} = State0) ->
    case Cache of
        #{Idx := {Term, Data}} ->
            {{Idx, Term, Data}, State0};
        _ ->
            case ra_log_wal:mem_tbl_read(Id, Idx) of
                undefined ->
                    % check segments
                    case get_segment(Idx, State0) of
                        undefined ->
                            {undefined, State0};
                        {Seg, State} ->
                            % unless the segment is corrup the entry should
                            % be in there
                            [Entry] = ra_log_file_segment:read(Seg, Idx, 1),
                            {Entry, State}
                    end;
                Entry ->
                    {Entry, State0}
            end
    end.


get_segment(Idx, #state{segment_refs = SegmentRefs, open_segments = Open} = State) ->
    case find_segment_file(Idx, SegmentRefs) of
        undefined ->
            undefined;
        Filename ->
            case Open of
                #{Filename := Seg} ->
                    {Seg, State};
                _ ->
                    {ok, Seg} = ra_log_file_segment:open(Filename, #{mode => read}),
                    {Seg, State#state{open_segments = Open#{Filename => Seg}}}
            end
    end.

find_segment_file(_Idx, []) ->
    undefined;
find_segment_file(Idx, [{Start, End, Fn} | _Tail])
  when Idx >= Start andalso Idx =< End ->
    Fn;
find_segment_file(Idx, [_ | Tail]) ->
    find_segment_file(Idx, Tail).




-spec fetch_term(ra_index(), ra_log_file_state()) ->
    {maybe(ra_term()), ra_log_file_state()}.
fetch_term(Idx, #state{cache = Cache} = State0) ->
    % needs to check cache first
    case Cache of
        #{Idx := {Term, _}} ->
            {Term, State0};
        _ ->
            % TODO: optimise this as we do not need to
            % read the full body
            case fetch(Idx, State0) of
                {undefined, State} ->
                    {undefined, State};
                {{_, Term, _}, State} ->
                    {Term, State}
            end
    end.

truncate_cache(Idx, State = #state{cache = Cache0}) ->
    Cache = maps:filter(fun (K, _) when K > Idx -> true;
                            (_, _) -> false
                        end, Cache0),
    State#state{cache = Cache}.

lookup_element(Tid, Key, Pos) ->
    try
        ets:lookup_element(Tid, Key, Pos)
    catch
        _:badarg ->
            undefined
    end.


-spec write_snapshot(Snapshot :: ra_log:ra_log_snapshot(),
                     State :: ra_log_file_state()) ->
    ra_log_file_state().
write_snapshot({Idx, _, _, _} = Snapshot, State = #state{directory = Dir}) ->
    File = filename:join(Dir, "ra.snapshot"),
    file:write_file(File, term_to_binary(Snapshot)),
    % update first index to the index following the latest snapshot
    truncate_cache(Idx, State#state{first_index = Idx+1}).

-spec read_snapshot(State :: ra_log_file_state()) ->
    maybe(ra_log:ra_log_snapshot()).
read_snapshot(#state{directory = Dir}) ->
    File = filename:join(Dir, "ra.snapshot"),
    case file:read_file(File) of
        {ok, Bin} ->
            binary_to_term(Bin);
        {error, enoent} ->
            undefined
    end.

-spec read_meta(Key :: ra_log:ra_meta_key(),
                State :: ra_log_file_state()) ->
    maybe(term()).
read_meta(Key, #state{kv = Kv}) ->
    case dets:lookup(Kv, Key) of
        [] -> undefined;
        [Value] ->
            element(2, Value)
    end.

-spec write_meta(Key :: ra_log:ra_meta_key(), Value :: term(),
                 State :: ra_log_file_state()) ->
    {ok,  ra_log_file_state()} | {error, term()}.
write_meta(Key, Value, State = #state{kv = Kv}) ->
    ok = dets:insert(Kv, {Key, Value}),
    {ok, State}.

sync_meta(#state{kv = Kv}) ->
    ok = dets:sync(Kv),
    ok.

%%% Local functions

maybe_append_0_0_entry(State0 = #state{last_index = -1}) ->
    {queued, State} = append({0, 0, undefined}, no_overwrite, State0),
    receive
        {ra_log_event, {written, {0, 0}}} -> ok
    end,
    State#state{first_index = 0, last_written_index_term = {0, 0}};
maybe_append_0_0_entry(State) ->
    State.

last_index_from_mem_table(Id) ->
    Last = ra_lib:default(lookup_element(ra_log_open_mem_tables, Id, 3), -1),
    First = ra_lib:default(lookup_element(ra_log_open_mem_tables, Id, 2), -1),
    {First, Last}.

lists_find(_Pred, []) ->
    undefined;
lists_find(Pred, [H | Tail]) ->
    case Pred(H) of
        true -> H;
        false ->
            lists_find(Pred, Tail)
    end.

