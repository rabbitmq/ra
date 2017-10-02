-module(ra_log_memory).
-behaviour(ra_log).
-export([init/1,
         close/1,
         append/3,
         take/3,
         last/1,
         last_index_term/1,
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

-type ra_log_memory_meta() :: #{atom() => term()}.

-type ra_log_memory_state() ::
    {ra_index(),
     #{ra_term() => {ra_index(), term()}},
     ra_log_memory_meta(),
     maybe(ra_log:ra_log_snapshot())}.

-spec init(_) -> ra_log_memory_state().
init(_Args) ->
    % initialized with a deafault 0 index 0 term dummy value
    % and an empty meta data map
    {0, #{0 => {0, undefined}}, #{}, undefined}.

-spec close(ra_log_memory_state()) -> ok.
close(_State) ->
    % not much to do here
    ok.

-spec append(Entry::log_entry(),
             Overwrite :: overwrite | no_overwrite,
             State::ra_log_memory_state()) ->
    {written, ra_log_memory_state()} |
    {error, integrity_error}.
append({Idx, Term, Data}, no_overwrite, {LastIdx, Log, Meta, Snapshot})
      when Idx > LastIdx ->
    {written, {Idx, Log#{Idx => {Term, Data}}, Meta, Snapshot}};
append(_Entry, no_overwrite, _State) ->
    {error, integrity_error};
append({Idx, Term, Data}, overwrite, {LastIdx, Log, Meta, Snapshot})
  when LastIdx > Idx ->
    Log1 = maps:without(lists:seq(Idx+1, LastIdx), Log),
    {written, {Idx, Log1#{Idx => {Term, Data}}, Meta, Snapshot}};
append({Idx, Term, Data}, overwrite, {_LastIdx, Log, Meta, Snapshot}) ->
    {written, {Idx, Log#{Idx => {Term, Data}}, Meta, Snapshot}}.


-spec take(ra_index(), non_neg_integer(), ra_log_memory_state()) ->
    [log_entry()].
take(Start, Num, {LastIdx, Log, _Meta, _Snapshot}) ->
    sparse_take(Start, Log, Num, LastIdx, []).

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


-spec last(ra_log_memory_state()) ->
    maybe(log_entry()).
last({LastIdx, _Data, _Meta, _Snapshot} = LogState) ->
    fetch(LastIdx, LogState).

-spec last_index_term(ra_log_memory_state()) -> maybe(ra_idxterm()).
last_index_term({LastIdx, Log, _Meta, Snapshot}) ->
    case Log of
        #{LastIdx := {LastTerm, _Data}} ->
            {LastIdx, LastTerm};
        _ ->
            % If not found fall back on snapshot if snapshot matches last term.
            case Snapshot of
                {LastIdx, LastTerm, _, _} ->
                    {LastIdx, LastTerm};
                _ ->
                    undefined
            end
    end.

-spec next_index(ra_log_memory_state()) -> ra_index().
next_index({LastIdx, _Data, _Meta, _Snapshot}) ->
    LastIdx + 1.

-spec fetch(ra_index(), ra_log_memory_state()) ->
    maybe(log_entry()).
fetch(Idx, {_LastIdx, Log, _Meta, _Snapshot}) ->
    case Log of
        #{Idx := {T, D}} ->
            {Idx, T, D};
        _ -> undefined
    end.

-spec fetch_term(ra_index(), ra_log_memory_state()) ->
    maybe(ra_term()).
fetch_term(Idx, {_LastIdx, Log, _Meta, _Snapshot}) ->
    case Log of
        #{Idx := {T, _}} ->
            T;
        _ -> undefined
    end.

flush(_Idx, Log) -> Log.

-spec write_snapshot(Snapshot :: ra_log:ra_log_snapshot(),
                     State :: ra_log_memory_state()) ->
    ra_log_memory_state().
write_snapshot(Snapshot, State) ->
    Log0 = element(2, State),
    Index  = element(1, Snapshot),
    % discard log
    Log = maps:filter(fun (K, _) -> K > Index end, Log0),
    setelement(2, setelement(4, State, Snapshot), Log).

-spec read_snapshot(State :: ra_log_memory_state()) ->
    ra_log:ra_log_snapshot().
read_snapshot(State) ->
    element(4, State).

-spec read_meta(Key :: ra_log:ra_meta_key(), State ::  ra_log_memory_state()) ->
    maybe(term()).
read_meta(Key, {_LastIdx, _Log, Meta, _Snapshot}) ->
    maps:get(Key, Meta, undefined).

-spec write_meta(Key :: ra_log:ra_meta_key(), Value :: term(),
                     State :: ra_log_memory_state()) ->
    {ok,  ra_log_memory_state()} | {error, term()}.
write_meta(Key, Value, {_LastIdx, _Log, Meta, _Snapshot} = State) ->
    {ok, erlang:setelement(3, State, Meta#{Key => Value})}.

sync_meta(_Log) ->
    ok.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

% append_test() ->
%     {0, #{}, _, _} = S = init([]),
%     {ok, {1, #{1 := {1, <<"hi">>}}, _, _}} =
%     append({1, 1, <<"hi">>}, no_overwrite, sync, S).

% append_twice_test() ->
%     {0, #{}, _, _} = S = init([]),
%     Entry = {1, 1, <<"hi">>},
%     {ok, S2} = append(Entry, no_overwrite, sync, S),
%     {error, integrity_error} = append(Entry, no_overwrite, sync, S2).

% append_overwrite_test() ->
%     {0, #{}, _, _} = S = init([]),
%     Entry = {1, 1, <<"hi">>},
%     {ok, S2} = append(Entry, overwrite, sync, S),
%     % TODO: a proper implementation should validate the term isn't decremented
%     % also it should truncate any item newer than the last written index
%     {ok,  {1, #{1 := {1, <<"hi">>}}, _, _}} = append(Entry, overwrite, sync, S2).

% take_test() ->
%     Log = #{1 => {8, <<"one">>},
%             2 => {8, <<"two">>},
%             3 => {8, <<"three">>}},
%     [{1, 8, <<"one">>},
%      {2, 8, <<"two">>}] = take(1, 2, {3, Log, #{}, undefined}),
%     [{3, 8, <<"three">>}] = take(3, 2, {3, Log, #{}, undefined}).

% last_test() ->
%     Log = #{1 => {8, <<"one">>},
%             2 => {8, <<"two">>},
%             3 => {8, <<"three">>}},
%     {3, 8, <<"three">>} = last({3, Log, #{}, undefined}).

% next_index_test() ->
%     Log = #{1 => {8, <<"one">>},
%             2 => {8, <<"two">>},
%             3 => {8, <<"three">>}},
%     4 = next_index({3, Log, #{}, undefined}).

% fetch_test() ->
%     Log = #{1 => {8, <<"one">>},
%             2 => {8, <<"two">>},
%             3 => {8, <<"three">>}},
%     {2, 8, <<"two">>} = fetch(2, {3, Log, #{}, undefined}).

% meta_test() ->
%     State0 = {0, #{}, #{}, undefined},
%     {ok, State} = write_meta(current_term, 23, State0),
%     23 = read_meta(current_term, State).

-endif.
