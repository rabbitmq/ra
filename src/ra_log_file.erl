-module(ra_log_file).
-behaviour(ra_log).
-export([init/1,
         close/1,
         append/3,
         take/3,
         last/1,
         fetch/2,
         next_index/1,
         % release/2,
         write_snapshot/2,
         read_snapshot/1,
         read_meta/2,
         write_meta/3
        ]).

-include("ra.hrl").

-type offset() :: non_neg_integer().

-record(state, {last_index = -1 :: -1 | ra_index(),
                directory :: list(),
                file :: file:io_device(),
                kv :: reference(),
                index = #{} :: #{ra_index() => {ra_term(), offset()}}
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
    ok = filelib:ensure_dir(Dets),
    {ok, Fd} = file:open(File, [raw, binary, read, append]),
    {ok, Kv} = dets:open_file(Dets, []),
    Index = recover_index(Fd),
    LastIndex = last_index_from_index(Index),
    State0 = #state{directory = Dir, file = Fd,
                    last_index = LastIndex,
                    kv = Kv, index = Index},
    maybe_append_0_0_entry(State0).

-spec close(ra_log_file_state()) -> ok.
close(#state{file = Fd, kv = Kv}) ->
    % deliberately ignoring return value
    _ = file:close(Fd),
    _ = dets:close(Kv),
    ok.

write(State = #state{file = Fd, index = Index}, {Idx, Term, Data}) ->
    {ok, Pos} = file:position(Fd, eof),
    DataB = term_to_binary(Data),
    Size = size(DataB),
    ok = file:write(Fd, [<<Idx:64>>, <<Term:64>>,
                         <<Size:64>>, DataB]),
    ok = file:sync(Fd),
    State#state{last_index = Idx,
                index = Index#{Idx => {Term, Pos}} }.

truncate_index(From, State = #state{index = Index0,
                                    last_index = LastIdx}) ->
    Index = maps:without(lists:seq(From, LastIdx), Index0),
    State#state{index = Index}.

-spec append(Entry::log_entry(), Overwrite::boolean(),
             State::ra_log_file_state()) ->
    {ok, ra_log_file_state()} | {error, integrity_error}.
append(Entry, false, State = #state{last_index = LastIdx})
      when element(1, Entry) > LastIdx ->
    {ok, write(State, Entry)};
append(_Entry, false, _State) ->
    {error, integrity_error};
append({Idx, _, _} = Entry, true, State0 = #state{last_index = LastIdx})
  when LastIdx > Idx ->
    % we're overwriting entries
    {ok, truncate_index(Idx + 1, write(State0, Entry))};
append(Entry, true, State) ->
    {ok, write(State, Entry)}.


-spec take(ra_index(), non_neg_integer(), ra_log_file_state()) ->
    [log_entry()].
take(Start, Num, #state{file = Fd, index = Index}) ->
    IdxEntries = maps:with(lists:seq(Start, Start + Num - 1), Index),
    Entries = maps:fold(fun (Idx, {Term, Offset}, Acc) ->
                                [read_entry_at(Idx, Term, Offset, Fd) | Acc]
                        end, [], IdxEntries),
    lists:reverse(Entries).

% % this allows for missing entries in the log
% sparse_take(Idx, _Log, Num, Max, Res)
%     when length(Res) =:= Num orelse
%          Idx > Max ->
%     lists:reverse(Res);
% sparse_take(Idx, Log, Num, Max, Res) ->
%     case Log of
%         #{Idx := {T, D}} ->
%             sparse_take(Idx+1, Log, Num, Max, [{Idx, T, D} | Res]);
%         _ ->
%             sparse_take(Idx+1, Log, Num, Max, Res)
%     end.


-spec last(ra_log_file_state()) ->
    maybe(log_entry()).
last(#state{last_index = LastIdx} = State) ->
    fetch(LastIdx, State).

-spec next_index(ra_log_file_state()) -> ra_index().
next_index(#state{last_index = LastIdx}) ->
    LastIdx + 1.

-spec fetch(ra_index(), ra_log_file_state()) ->
    maybe(log_entry()).
fetch(Idx, #state{file = Fd, index = Index}) ->
    case Index of
        #{Idx := {Term, Offset}} ->
            read_entry_at(Idx, Term,  Offset, Fd);
        _ ->
            undefined
    end.

% -spec release(Indices :: [ra_index()], State :: ra_log_file_state()) ->
%     ra_log_file_state().
% release(_Indices, State) ->
%     State. % noop

-spec write_snapshot(Snapshot :: ra_log:ra_log_snapshot(),
                     State :: ra_log_file_state()) ->
    ra_log_file_state().
write_snapshot({Idx, _, _, _} = Snapshot,
               State = #state{directory = Dir, index = Index}) ->
    File = filename:join(Dir, "ra.snapshot"),
    file:write_file(File, term_to_binary(Snapshot)),
    % just deleting entries in the index here no on disk gc atm
    State#state{index = maps:filter(fun(K, _) -> K > Idx end, Index)}.

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
    dets:insert(Kv, {Key, Value}),
    dets:sync(Kv),
    {ok, State}.

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
    recover_index0(Gen, Gen(), #{}).

recover_index0(_GenFun, undefined, Index) ->
    Index;
recover_index0(GenFun, {Idx, TermPos}, Index) ->
    recover_index0(GenFun, GenFun(), Index#{Idx => TermPos}).

maybe_append_0_0_entry(State0 = #state{last_index = -1}) ->
    {ok, State} = append({0, 0, undefined}, false, State0),
    State;
maybe_append_0_0_entry(State) ->
    State.

last_index_from_index(Map) when map_size(Map) =:= 0 ->
    -1;
last_index_from_index(Index) ->
    lists:max(maps:keys(Index)).


