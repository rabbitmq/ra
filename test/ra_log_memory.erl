%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term Broadcom refers to Broadcom Inc. and/or its subsidiaries.
%%
-module(ra_log_memory).

-export([init/1,
         close/1,
         append/2,
         write/2,
         take/3,
         fold/5,
         last_index_term/1,
         set_last_index/2,
         handle_event/2,
         last_written/1,
         fetch/2,
         fetch_term/2,
         flush/2,
         next_index/1,
         snapshot_state/1,
         set_snapshot_state/2,
         install_snapshot/3,
         read_snapshot/1,
         recover_snapshot/1,
         snapshot_index_term/1,
         update_release_cursor/5,
         read_meta/2,
         write_meta/3,
         write_meta_f/3,
         can_write/1,
         overview/1,
         write_config/2,
         read_config/1,
         delete_everything/1,
         release_resources/3,
         to_list/1
        ]).

-include("src/ra.hrl").

-type ra_log_memory_meta() :: #{atom() => term()}.

-record(state, {last_index = 0 :: ra:index(),
                last_written = {0, 0} :: ra_idxterm(), % only here to fake the async api of the file based one
                entries = #{0 => {0, undefined}} ::
                    #{ra:term() => {ra:index(), term()}},
                meta = #{} :: ra_log_memory_meta(),
                snapshot :: option({ra_snapshot:meta(), term()})}).

-type ra_log_memory_state() :: #state{} | ra_log:state().

-export_type([ra_log_memory_state/0]).

-spec init(_) -> ra_log_memory_state().
init(_Args) ->
    % initialized with a default 0 index 0 term dummy value
    % and an empty meta data map
    #state{}.

-spec close(ra_log_memory_state()) -> ok.
close(_State) ->
    % not much to do here
    ok.

-spec append(Entry::log_entry(), State::ra_log_memory_state()) ->
    ra_log_memory_state() | no_return().
append({Idx, Term, Data}, #state{last_index = LastIdx,
                                 entries = Log} = State)
  when Idx > LastIdx ->
    State#state{last_index = Idx,
                entries = Log#{Idx => {Term, Data}}};
append(_Entry, _State) ->
    exit({integrity_error, undefined}).

-spec write(Entries :: [log_entry()], State::ra_log_memory_state()) ->
    {ok, ra_log_memory_state()} |
    {error, {integrity_error, term()}}.
write([{FirstIdx, _, _} | _] = Entries,
      #state{last_index = LastIdx, entries = Log0} = State)
  when FirstIdx =< LastIdx + 1 ->
    % overwrite
    Log1 = case FirstIdx < LastIdx of
               true ->
                   maps:without(lists:seq(FirstIdx+1, LastIdx), Log0);
               false ->
                   Log0
           end,
    {Log, LastInIdx} = lists:foldl(fun ({Idx, Term, Data}, {Acc, _}) ->
                                           {Acc#{Idx => {Term, Data}}, Idx}
                                   end, {Log1, FirstIdx}, Entries),
    {ok, State#state{last_index = LastInIdx,
                          entries = Log}};
write([{FirstIdx, _, _} | _] = Entries,
      #state{snapshot = {#{index := SnapIdx}, _}, entries = Log0} = State)
 when SnapIdx + 1 =:= FirstIdx ->
    {Log, LastInIdx} = lists:foldl(fun ({Idx, Term, Data}, {Acc, _}) ->
                                           {Acc#{Idx => {Term, Data}}, Idx}
                                   end, {Log0, FirstIdx}, Entries),
    {ok, State#state{last_index = LastInIdx,
                     entries = Log}};
write(_Entries, _State) ->
    {error, {integrity_error, undefined}}.


take(Start, Num, #state{last_index = LastIdx, entries = Log} = State) ->
    Entries = sparse_take(Start, Log, Num, LastIdx, []),
    {Entries, length(Entries), State}.

fold(From, To, Fun, Acc0, #state{last_index = LastIdx,
                                 entries = Log} = State) ->
    Entries = sparse_take(From, Log, To - From + 1, LastIdx, []),
    Acc = lists:foldl(Fun, Acc0, Entries),
    {Acc, State}.

% this allows for missing entries in the log
sparse_take(Idx, _Log, Num, Max, Res)
    when length(Res) =:= Num orelse
         Idx > Max ->
    lists:reverse(Res);
sparse_take(Idx, Log, Num, Max, Res) ->
    case Log of
        #{Idx := {T, D}} ->
            sparse_take(Idx+1, Log, Num, Max, [{Idx, T, D} | Res]);
        _ ->
            sparse_take(Idx+1, Log, Num, Max, Res)
    end.


-spec last_index_term(ra_log_memory_state()) -> option(ra_idxterm()).
last_index_term(#state{last_index = LastIdx,
                       entries = Log,
                       snapshot = Snapshot}) ->
    case Log of
        #{LastIdx := {LastTerm, _Data}} ->
            {LastIdx, LastTerm};
        _ ->
            % If not found fall back on snapshot if snapshot matches last term.
            case Snapshot of
                {#{index := LastIdx, term := LastTerm}, _} ->
                    {LastIdx, LastTerm};
                _ ->
                    undefined
            end
    end.

-spec set_last_index(ra:index(), ra_log_memory_state()) ->
    {ok, ra_log_memory_state()} | {not_found, ra_log_memory_state()}.
set_last_index(Idx, #state{last_written = {LWIdx, _}} = State0) ->
    case fetch_term(Idx, State0) of
        {undefined, State} ->
            {not_found, State};
        {Term, State1} when Idx < LWIdx ->
            %% need to revert last_written too
            State = State1#state{last_index = Idx,
                                 last_written = {Idx, Term}},
            {ok, State};
        {_, State1} ->
            State = State1#state{last_index = Idx},
            {ok, State}
    end.

-spec last_written(ra_log_memory_state()) -> ra_idxterm().
last_written(#state{last_written = LastWritten}) ->
    % we could just use the last index here but we need to "fake" it to
    % remain api compatible with  ra_log, for now at least.
    LastWritten.

-spec handle_event(ra_log:event_body(), ra_log_memory_state()) ->
    {ra_log_memory_state(), list()}.
handle_event({written, Term, {_From, Idx} = Range0}, State0) ->
    case fetch_term(Idx, State0) of
        {Term, State} ->
            {State#state{last_written = {Idx, Term}}, []};
        _ ->
            case ra_range:limit(Idx, Range0) of
                undefined ->
                    % if the term doesn't match we just ignore it
                    {State0, []};
                Range ->
                    handle_event({written, Term, Range}, State0)
            end
    end;
handle_event(_Evt, State0) ->
            {State0, []}.

-spec next_index(ra_log_memory_state()) -> ra:index().
next_index(#state{last_index = LastIdx}) ->
    LastIdx + 1.

-spec fetch(ra:index(), ra_log_memory_state()) ->
    {option(log_entry()), ra_log_memory_state()}.
fetch(Idx, #state{entries = Log} = State) ->
    case Log of
        #{Idx := {T, D}} ->
            {{Idx, T, D}, State};
        _ -> {undefined, State}
    end.

-spec fetch_term(ra:index(), ra_log_memory_state()) ->
    {option(ra:term()), ra_log_memory_state()}.
fetch_term(Idx, #state{entries = Log} = State) ->
    case Log of
        #{Idx := {T, _}} ->
            {T, State};
        _ -> {undefined, State}
    end.

flush(_Idx, Log) -> Log.

install_snapshot({Index, Term}, Data, #state{entries = Log0} = State) ->
    % Index  = maps:get(index, Meta),
    % Term  = maps:get(term, Meta),
    % discard log
    Log = maps:filter(fun (K, _) -> K > Index end, Log0),
    {State#state{entries = Log,
                 last_index = Index,
                 last_written = {Index, Term},
                 snapshot = Data}, []};
install_snapshot(_Meta, _Data, State) ->
    {State, []}.

-spec read_snapshot(State :: ra_log_memory_state()) ->
    {ok, ra_snapshot:meta(), term()}.
read_snapshot(#state{snapshot = {Meta, Data}}) ->
    {ok, Meta, Data}.

-spec recover_snapshot(State :: ra_log_memory_state()) ->
    undefined | {ok, ra_snapshot:meta(), term()}.
recover_snapshot(#state{snapshot = undefined}) ->
    undefined;
recover_snapshot(#state{snapshot = {Meta, MacState}}) ->
    {Meta, MacState}.

set_snapshot_state(SnapState, State) ->
    State#state{snapshot = SnapState}.

snapshot_state(State) ->
    State#state.snapshot.

-spec read_meta(Key :: ra_log:ra_meta_key(), State :: ra_log_memory_state()) ->
    option(term()).
read_meta(Key, #state{meta = Meta}) ->
    maps:get(Key, Meta, undefined).

-spec snapshot_index_term(State :: ra_log_memory_state()) ->
    ra_idxterm().
snapshot_index_term(#state{snapshot = undefined}) ->
    undefined;
snapshot_index_term(#state{snapshot = {#{index := Idx,
                                         term := Term}, _}}) ->
    {Idx, Term}.

-spec update_release_cursor(ra:index(), ra_cluster(),
                            ra_machine:version(), term(),
                            ra_log_memory_state()) ->
    {ra_log_memory_state(), []}.
update_release_cursor(_Idx, _Cluster, _, _MacState, State) ->
    {State, []}.

write_meta(_Key, _Value, _State) ->
    ok.

-spec write_meta_f(term(), term(), ra_log_memory_state()) ->
    ra_log_memory_state().
write_meta_f(Key, Value, #state{meta = Meta} = State) ->
    State#state{meta = Meta#{Key => Value}};
write_meta_f(_Key, _Value, State) ->
    %% dummy case to satisfy dialyzer
    State.

can_write(_Log) ->
    true.

overview(Log) ->
    #{type => ?MODULE,
      last_index => Log#state.last_index,
      last_written => Log#state.last_written,
      num_entries => maps:size(Log#state.entries)}.

write_config(_Config, _Log) ->
    ok.

read_config(_Log) ->
    not_found.

delete_everything(_Log) -> ok.

release_resources(_, _, State) ->
    State.

to_list(#state{entries = Log}) ->
    [{Idx, Term, Data} || {Idx, {Term, Data}} <- maps:to_list(Log)].

