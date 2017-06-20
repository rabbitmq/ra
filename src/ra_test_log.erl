-module(ra_test_log).
-behaviour(ra_log).
-export([init/1,
         append/3,
         take/3,
         last/1,
         fetch/2,
         next_index/1,
         read_meta/2,
         write_meta/3
        ]).

-include("ra.hrl").

-type ra_test_log_meta() :: #{atom() => term()}.

-type ra_test_log_state() ::
    {ra_index(), #{ra_term() => {ra_index(), term()}}, ra_test_log_meta()}.

-spec init([term()]) -> ra_test_log_state().
init(_Args) ->
    % initialized with a deafault 0 index 0 term dummy value
    % and an empty meta data map
    {0, #{0 => {0, dummy}}, #{}}.

-spec append(Entry::log_entry(), Overwrite::boolean(),
                 State::ra_test_log_state()) ->
    {ok, ra_test_log_state()} | {error, integrity_error}.
append({Idx, Term, Data}, false, {LastIdx, Log, Meta})
      when Idx == LastIdx+1 ->
    {ok, {Idx, Log#{Idx => {Term, Data}}, Meta}};
append(_Entry, false, _State) ->
    {error, integrity_error};
append({Idx, Term, Data}, true, {LastIdx, Log, Meta})  when LastIdx > Idx ->
    Log1 = maps:without(lists:seq(Idx+1, LastIdx), Log),
    {ok, {Idx, Log1#{Idx => {Term, Data}}, Meta}};
append({Idx, Term, Data}, true, {_LastIdx, Log, Meta}) ->
    {ok, {Idx, Log#{Idx => {Term, Data}}, Meta}}.


-spec take(ra_index(), non_neg_integer(), ra_test_log_state()) ->
    [log_entry()].
take(Start, Num, {_, Log, _Meta}) ->
    lists:filtermap(fun(I) -> case Log of
                                  #{I := {T, D}} ->
                                      {true, {I, T, D}};
                                  _ -> false
                              end
                    end, lists:seq(Start, Start + Num - 1)).

-spec last(ra_test_log_state()) ->
    maybe(log_entry()).
last({LastIdx, _Data, _Meta} = Log) ->
    fetch(LastIdx, Log).

-spec next_index(ra_test_log_state()) -> ra_index().
next_index({LastIdx, _Data, _Meta}) ->
    LastIdx + 1.

-spec fetch(ra_index(), ra_test_log_state()) ->
    maybe(log_entry()).
fetch(Idx, {_LastIdx, Log, _Meta}) ->
    case Log of
        #{Idx := {T, D}} ->
            {Idx, T, D};
        _ -> undefined
    end.

-spec read_meta(Key :: ra_log:ra_meta_key(), State ::  ra_test_log_state()) ->
    maybe(term()).
read_meta(Key, {_LastIdx, _Log, Meta}) ->
    maps:get(Key, Meta, undefined).

-spec write_meta(Key :: ra_log:ra_meta_key(), Value :: term(),
                     State :: ra_test_log_state()) ->
    {ok,  ra_test_log_state()} | {error, term()}.
write_meta(Key, Value, {_LastIdx, _Log, Meta} = State) ->
    {ok, erlang:setelement(3, State, Meta#{Key => Value})}.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

append_test() ->
    {0, #{}, _} = S = init([]),
    {ok, {1, #{1 := {1, <<"hi">>}}, _}} = append({1, 1, <<"hi">>}, false, S).

append_gap_test() ->
    {0, #{}, _} = S = init([]),
    {error, integrity_error} = append({2, 1, <<"hi">>}, false, S).

append_twice_test() ->
    {0, #{}, _} = S = init([]),
    Entry = {1, 1, <<"hi">>},
    {ok, S2} = append(Entry, false, S),
    {error, integrity_error} = append(Entry, false, S2).

append_overwrite_test() ->
    {0, #{}, _} = S = init([]),
    Entry = {1, 1, <<"hi">>},
    {ok, S2} = append(Entry, true, S),
    % TODO: a proper implementation should validate the term isn't decremented
    % also it should truncate any item newer than the last written index
    {ok,  {1, #{1 := {1, <<"hi">>}}, _}} = append(Entry, true, S2).

take_test() ->
    Log = #{1 => {8, <<"one">>},
            2 => {8, <<"two">>},
            3 => {8, <<"three">>}},
    [{1, 8, <<"one">>},
     {2, 8, <<"two">>}] = take(1, 2, {3, Log, #{}}),
    [{3, 8, <<"three">>}] = take(3, 2, {3, Log, #{}}).

last_test() ->
    Log = #{1 => {8, <<"one">>},
            2 => {8, <<"two">>},
            3 => {8, <<"three">>}},
    {3, 8, <<"three">>} = last({3, Log, #{}}).

next_index_test() ->
    Log = #{1 => {8, <<"one">>},
            2 => {8, <<"two">>},
            3 => {8, <<"three">>}},
    4 = next_index({3, Log, #{}}).

fetch_test() ->
    Log = #{1 => {8, <<"one">>},
            2 => {8, <<"two">>},
            3 => {8, <<"three">>}},
    {2, 8, <<"two">>} = fetch(2, {3, Log, #{}}).

meta_test() ->
    State0 = {0, #{}, #{}},
    {ok, State} = write_meta(current_term, 23, State0),
    23 = read_meta(current_term, State).

-endif.
