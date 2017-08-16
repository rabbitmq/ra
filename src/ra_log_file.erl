-module(ra_log_file).
-behaviour(ra_log).
-export([init/1,
         close/1,
         append/4,
         sync/1,
         take/3,
         last/1,
         last_index_term/1,
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

-type offset() :: non_neg_integer().

-record(state, {last_index = -1 :: -1 | ra_index(),
                directory :: list(),
                file :: file:io_device(),
                kv :: reference(),
                index = #{} :: #{ra_index() => {ra_term(), offset()}},
                cache = undefined :: maybe(log_entry()) % last written log entry
               }).

-type ra_log_file_state() :: term().



-spec init(ra_log:ra_log_init_args()) -> ra_log_file_state().
init(#{directory := Dir}) ->
    % initialized with a deafault 0 index 0 term dummy value
    % and an empty meta data map
    % TODO: rebuild from persisted state if present
    File = filename:join(Dir, "ra_log.log"),
    Dets = filename:join(Dir, "ra.dets"),
    ok = filelib:ensure_dir(File),
    {ok, Fd} = file:open(File, [raw, binary, read, append]),
    {ok, Kv} = dets:open_file(Dets, []),
    Index = recover_index(Fd),
    LastIndex = last_index_from_index(Index),
    State0 = #state{directory = Dir, file = Fd,
                    last_index = LastIndex,
                    kv = Kv, index = Index},

    State = maybe_append_0_0_entry(State0),
    ?DBG("ra_log_file recovered last_index_term ~p~n", [last_index_term(State)]),
    State.

-spec close(ra_log_file_state()) -> ok.
close(#state{file = Fd, kv = Kv}) ->
    % deliberately ignoring return value
    _ = file:close(Fd),
    _ = dets:close(Kv),
    ok.

sync(#state{file = Fd}) ->
    ok = file:sync(Fd),
    ok.

write(State = #state{file = Fd, index = Index},
      {Idx, Term, Data} = Entry, Sync) ->
    {ok, Pos} = file:position(Fd, eof),
    DataB = term_to_binary(Data),
    Size = size(DataB),
    ok = file:write(Fd, [<<Idx:64>>, <<Term:64>>,
                         <<Size:64>>, DataB]),
    case Sync of
        sync -> ok = file:sync(Fd);
        no_sync -> ok
    end,

    State#state{last_index = Idx,
                cache = Entry,
                index = Index#{Idx => {Term, Pos}}}.

-spec append(Entry :: log_entry(),
                 overwrite | no_overwrite,
                 sync | no_sync,
                 State :: ra_log_file_state()) ->
    {ok, ra_log_file_state()} | {error, integrity_error}.
append(Entry, no_overwrite, Sync, State = #state{last_index = LastIdx})
      when element(1, Entry) > LastIdx ->
    {ok, write(State, Entry, Sync)};
append(_Entry, no_overwrite, _Sync, _State) ->
    {error, integrity_error};
append({Idx, _, _} = Entry, overwrite, Sync, State0 = #state{last_index = LastIdx})
  when LastIdx > Idx ->
    % we're overwriting entries
    {ok, truncate_index(Idx + 1, LastIdx, write(State0, Entry, Sync))};
append(Entry, overwrite, Sync, State) ->
    {ok, write(State, Entry, Sync)}.


-spec take(ra_index(), non_neg_integer(), ra_log_file_state()) ->
    [log_entry()].
take(Start, Num, #state{file = _Fd, index = _Index} = State) ->
    lists:foldl(fun (Idx, Acc) ->
                        maybe_append(fetch(Idx, State), Acc)
                end, [], lists:seq(Start + Num - 1, Start, -1)).

maybe_append(undefined, L) ->
    L;
maybe_append(I, L) ->
    [I | L].

-spec last(ra_log_file_state()) ->
    maybe(log_entry()).
last(#state{last_index = LastIdx} = State) ->
    fetch(LastIdx, State).

-spec last_index_term(ra_log_file_state()) -> maybe(ra_idxterm()).
last_index_term(#state{last_index = LastIdx,
                       index = Index} = State) ->
    case Index of
        #{LastIdx := {LastTerm, _Offset}} ->
            {LastIdx, LastTerm};
        _ ->
            % If not found fall back on snapshot if snapshot matches last term.
            case read_snapshot(State) of
                {LastIdx, LastTerm, _, _} ->
                    {LastIdx, LastTerm};
                _ ->
                    undefined
            end
    end.

-spec next_index(ra_log_file_state()) -> ra_index().
next_index(#state{last_index = LastIdx}) ->
    LastIdx + 1.

-spec fetch(ra_index(), ra_log_file_state()) ->
    maybe(log_entry()).
fetch(Idx, #state{cache = {Idx, _, _} = Entry}) ->
    Entry;
fetch(Idx, #state{file = Fd, index = Index}) ->
    case Index of
        #{Idx := {Term, Offset}} ->
            read_entry_at(Idx, Term,  Offset, Fd);
        _ ->
            undefined
    end.

-spec fetch_term(ra_index(), ra_log_file_state()) ->
    maybe(ra_term()).
fetch_term(Idx, #state{index = Index}) ->
    case Index of
        #{Idx := {Term, _}} ->
            Term;
        _ ->
            undefined
    end.

-spec write_snapshot(Snapshot :: ra_log:ra_log_snapshot(),
                     State :: ra_log_file_state()) ->
    ra_log_file_state().
write_snapshot({Idx, _, _, _} = Snapshot,
               State = #state{directory = Dir, index = Index}) ->
    File = filename:join(Dir, "ra.snapshot"),
    file:write_file(File, term_to_binary(Snapshot)),
    % just deleting entries in the index here no on disk gc atm
    State#state{index = maps:filter(fun(K, _) -> K > Idx end, Index),
                cache = undefined}.

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


read_entry_at(Idx, Term, Offset, Fd) ->
    {ok, _} = file:position(Fd, Offset + 16),
    {ok, <<Length:64/integer>>} = file:read(Fd, 8),
    {ok, Bin} = file:read(Fd, Length),
    {Idx, Term, binary_to_term(Bin)}.

read_index_entry(Fd) ->
    {ok, Pos} = file:position(Fd, cur),
    case file:read(Fd, 24) of
        eof ->
            undefined;
        {ok, <<Idx:64/integer,
               Term:64/integer,
               Length:64/integer>>} ->
            {ok, _NewPos} = file:position(Fd, {cur, Length}),
            {Idx, {Term, Pos}}
    end.

recover_index(Fd) ->
    {ok, 0} = file:position(Fd, 0),
    Gen = fun () -> read_index_entry(Fd) end,
    recover_index0(Gen, Gen(), {-1, -1, #{}}).

recover_index0(_GenFun, undefined, {LastIdx, MaxIdx, Index0}) ->
    maps:without(lists:seq(LastIdx+1, MaxIdx), Index0);
recover_index0(GenFun, {Idx, TermPos}, {_LastIdx, MaxIdx, Index0}) ->
    % remove "higher indices in the log as they would have been overwritten
    % and we don't want them to hang around.
    Index = maps:filter(fun(K, _) when K > Idx -> false;
                           (_, _) -> true
                         end, Index0),
    recover_index0(GenFun, GenFun(), {Idx, max(Idx, MaxIdx), Index#{Idx => TermPos}}).

truncate_index(From, To, State = #state{index = Index0}) ->
    Index = maps:without(lists:seq(From, To), Index0),
    State#state{index = Index}.

maybe_append_0_0_entry(State0 = #state{last_index = -1}) ->
    {ok, State} = append({0, 0, undefined}, no_overwrite, sync, State0),
    State;
maybe_append_0_0_entry(State) ->
    State.

last_index_from_index(Map) when map_size(Map) =:= 0 ->
    -1;
last_index_from_index(Index) ->
    lists:max(maps:keys(Index)).


