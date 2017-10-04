-module(ra_log_file).
-behaviour(ra_log).
-export([init/1,
         close/1,
         append/3,
         take/3,
         last/1,
         last_index_term/1,
         handle_written/2,
         last_written/1,
         fetch/2,
         fetch_term/2,
         flush/2,
         next_index/1,
         write_snapshot/2,
         read_snapshot/1,
         read_meta/2,
         write_meta/3,
         sync_meta/1
        ]).

-include("ra.hrl").

-type tid() :: reference() | atom().

-record(state, {first_index = -1 :: ra_index(),
                last_index = -1 :: -1 | ra_index(),
                last_written_index_term = {0, 0} :: ra_idxterm(),
                id :: atom(),
                tid :: maybe(tid()),
                directory :: list(),
                kv :: reference(),
                cache = #{} :: #{ra_index() => {ra_term(), log_entry()}}
               }).

-type ra_log_file_state() :: term().


-spec init(ra_log:ra_log_init_args()) -> ra_log_file_state().
init(#{directory := Dir, id := Id}) ->
    % TODO: rebuild from persisted state if present
    Dets = filename:join(Dir, "ra.dets"),
    ok = filelib:ensure_dir(Dets),
    {ok, Kv} = dets:open_file(Dets, []),
    % index recovery is done by the storage engine
    % at some point we need to recover some kind of index of
    % flushed segments
    {FirstIndex, LastIndex} = last_index_from_mem_table(Id),
    State0 = #state{directory = Dir, id = Id,
                    first_index = FirstIndex,
                    last_index = LastIndex, kv = Kv},

    % initialized with a deafault 0 index 0 term dummy value
    % and an empty meta data map
    State = maybe_append_0_0_entry(State0),
    % ?DBG("ra_log_file recovered last_index_term ~p~n", [last_index_term(State)]),
    State#state{tid = get_mem_tbl(Id)}.

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
    [log_entry()].
take(Start, Num, #state{} = State) ->
    lists:foldl(fun (Idx, Acc) ->
                        maybe_append(fetch(Idx, State), Acc)
                end, [], lists:seq(Start + Num - 1, Start, -1)).

get_mem_tbl(Id) ->
    lookup_element(ra_log_open_mem_tables, Id, 4).

maybe_append(undefined, L) ->
    L;
maybe_append(I, L) ->
    [I | L].

-spec last(ra_log_file_state()) ->
    maybe(log_entry()).
last(#state{last_index = LastIdx} = State) ->
    fetch(LastIdx, State).

-spec last_index_term(ra_log_file_state()) -> maybe(ra_idxterm()).
last_index_term(#state{last_index = LastIdx} = State) ->
    % TODO is it safe to do a cache hit here?
    case fetch_term(LastIdx, State) of
        undefined ->
            % If not found fall back on snapshot if snapshot matches last term.
            case read_snapshot(State) of
                {LastIdx, LastTerm, _, _} ->
                    {LastIdx, LastTerm};
                _ ->
                    undefined
            end;
        Term ->
            {LastIdx, Term}
    end.

-spec last_written(ra_log_file_state()) -> ra_idxterm().
last_written(#state{last_written_index_term = LWTI}) ->
    LWTI.

-spec handle_written(ra_index(), ra_log_file_state()) ->  ra_log_file_state().
handle_written(Idx, State) ->
    Term = fetch_term(Idx, State),
    State#state{last_written_index_term = {Idx, Term}}.

-spec next_index(ra_log_file_state()) -> ra_index().
next_index(#state{last_index = LastIdx}) ->
    LastIdx + 1.

-spec fetch(ra_index(), ra_log_file_state()) ->
    maybe(log_entry()).
fetch(Idx, #state{last_index = Last, first_index = First})
  when Idx > Last orelse Idx < First ->
    undefined;
fetch(Idx, #state{tid = Tid, cache = Cache}) ->
    case Cache of
        #{Idx := {Term, Data}} ->
            {Idx, Term, Data};
        _ ->
            case ets:lookup(Tid, Idx) of
                [Entry] ->
                    Entry;
                _ ->
                    undefined
            end
    end.

-spec fetch_term(ra_index(), ra_log_file_state()) ->
    maybe(ra_term()).
fetch_term(Idx, #state{tid = Tid, cache = Cache}) ->
    ra_lib:lazy_default(lookup_element(Tid, Idx, 2),
                        fun () ->
                                case Cache of
                                    #{Idx := {Term, _}} ->
                                        Term;
                                    _ ->
                                        undefined
                                end
                        end).

flush(Idx, State = #state{cache = Cache0}) ->
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
    flush(Idx, State#state{first_index = Idx+1}).

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


% recover_index(Fd) ->
%     {ok, 0} = file:position(Fd, 0),
%     Gen = fun () -> read_index_entry(Fd) end,
%     recover_index0(Gen, Gen(), {-1, -1, #{}}).

% recover_index0(_GenFun, undefined, {LastIdx, MaxIdx, Index0}) ->
%     maps:without(lists:seq(LastIdx+1, MaxIdx), Index0);
% recover_index0(GenFun, {Idx, TermPos}, {_LastIdx, MaxIdx, Index0}) ->
%     % remove "higher indices in the log as they would have been overwritten
%     % and we don't want them to hang around.
%     Index = maps:filter(fun(K, _) when K > Idx -> false;
%                            (_, _) -> true
%                          end, Index0),
%     recover_index0(GenFun, GenFun(), {Idx, max(Idx, MaxIdx), Index#{Idx => TermPos}}).

% truncate_index(From, To, State = #state{index = Index0}) ->
%     Index = maps:without(lists:seq(From, To), Index0),
%     State#state{index = Index}.

maybe_append_0_0_entry(State0 = #state{last_index = -1}) ->
    {queued, State} = append({0, 0, undefined}, no_overwrite, State0),
    receive
        {written, 0} -> ok
    end,
    State#state{first_index = 0, last_written_index_term = {0, 0}};
maybe_append_0_0_entry(State) ->
    State.

last_index_from_mem_table(Id) ->
    Last = ra_lib:default(lookup_element(ra_log_open_mem_tables, Id, 3), -1),
    First = ra_lib:default(lookup_element(ra_log_open_mem_tables, Id, 2), -1),
    {First, Last}.
