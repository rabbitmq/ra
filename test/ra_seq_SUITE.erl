-module(ra_seq_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-export([
         ]).

%% NB: proper must be included before eunit as both define ?LET
-include_lib("proper/include/proper.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

%%%===================================================================
%%% Common Test callbacks
%%%===================================================================

all() ->
    [
     {group, tests},
     {group, props}
    ].


all_tests() ->
    [
     append,
     floor,
     limit,
     add,
     subtract,
     iter,
     list_chunk,
     remove_prefix,
     remove_prefix_2,
     from_list_with_duplicates,
     has_overlap,
     in
    ].

%% Property tests. `add/2' and `remove_prefix/2' merge and split runs
%% rather than walking individual indexes, so these check both the values
%% they compute and that the representation stays canonical - a sequence
%% never holds a range shorter than three indexes, which `append/2',
%% `floor/2' and `limit/2' all rely on.
prop_tests() ->
    [
     prop_from_list_canonical,
     prop_expand_descending,
     prop_add_model,
     prop_add_canonical,
     prop_add_appendable,
     prop_remove_prefix_model,
     prop_remove_prefix_canonical,
     prop_remove_prefix_roundtrip,
     prop_remove_prefix_detects_gaps,
     prop_in_model
    ].

groups() ->
    [{tests, [], all_tests()},
     {props, [], prop_tests()}].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(_Group, Config) ->
    Config.

end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok.

%%%===================================================================
%%% Test cases
%%%===================================================================

append(_Config) ->
    S1 = [1] = ra_seq:append(1, []),
    S2 = [2, 1] = ra_seq:append(2, S1),
    S3 = [{1, 3}] = ra_seq:append(3, S2),
    S4 = [{1, 4}] = ra_seq:append(4, S3),
    S5 = [6, {1, 4}] = ra_seq:append(6, S4),

    ?assertError(function_clause, ra_seq:append(2, S4)),
    ?assertError(function_clause, ra_seq:append(6, S5)),

    ok.

floor(_Config) ->
    S = ra_seq:from_list([1, 2, 3, 5, 6, 7, 8, 9, 11]),
    [11] = ra_seq:floor(11, S),
    [11, 9] = ra_seq:floor(9, S),
    [11, 9, 8] = ra_seq:floor(8, S),
    [11, {7, 9}] = ra_seq:floor(7, S),
    [11, {6, 9}] = ra_seq:floor(6, S),
    [11, {5, 9}] = ra_seq:floor(5, S),
    [11, {5, 9}] = ra_seq:floor(4, S),
    [11, {5, 9}, 3] = ra_seq:floor(3, S),
    [11, {5, 9}, 3, 2] = ra_seq:floor(2, S),
    [11, {5, 9}, {1, 3}] = ra_seq:floor(1, S),
    [11, {5, 9}, {1, 3}] = ra_seq:floor(0, S),
    ok.

limit(_Config) ->
    S = ra_seq:from_list([1, 2, 3, 5, 6, 7, 8, 9, 11]),
    [11, {5, 9}, {1, 3}] = ra_seq:limit(11, S),
    [{5, 9}, {1, 3}] = ra_seq:limit(10, S),
    [{5, 9}, {1, 3}] = ra_seq:limit(9, S),
    [{5, 8}, {1, 3}] = ra_seq:limit(8, S),
    [{5, 7}, {1, 3}] = ra_seq:limit(7, S),
    [6, 5, {1, 3}] = ra_seq:limit(6, S),
    [5, {1, 3}] = ra_seq:limit(5, S),
    [{1, 3}] = ra_seq:limit(4, S),
    [{1, 3}] = ra_seq:limit(3, S),
    [2, 1] = ra_seq:limit(2, S),
    [1] = ra_seq:limit(1, S),
    [] = ra_seq:limit(0, S),
    ok.

add(_Config) ->
    S1 = ra_seq:from_list([1, 2, 3, 5, 6]),
    S2 = ra_seq:from_list([7, 8, 9, 11]),
    [11, {5, 9}, {1, 3}] = ra_seq:add(S2, S1),

    S3 = ra_seq:from_list([1, 2, 3, 5, 6, 7, 8]),
    S4 = ra_seq:from_list([7, 8, 9, 11]),
    [11, {5, 9}, {1, 3}] = ra_seq:add(S4, S3),
    ok.

subtract(_Config) ->
    [11, {7, 9}, {1, 3}] = ra_seq:subtract([{1, 11}], [10, {4, 6}]),
    ok.

iter(_Config) ->
    S = ra_seq:from_list([1, 2, 3, 5, 6, 8, 9, 10, 12]),
    I0 = ra_seq:iterator(S),
    {1, I1} = ra_seq:next(I0),
    {2, I2} = ra_seq:next(I1),
    {3, I3} = ra_seq:next(I2),
    {5, I4} = ra_seq:next(I3),
    {6, I5} = ra_seq:next(I4),
    {8, I6} = ra_seq:next(I5),
    {9, I7} = ra_seq:next(I6),
    {10, I8} = ra_seq:next(I7),
    {12, I9} = ra_seq:next(I8),
    end_of_seq = ra_seq:next(I9),
    ok.

list_chunk(_Config) ->
    %% Test with empty sequence
    end_of_seq = ra_seq:list_chunk(3, []),

    %% Test with sequence smaller than chunk size
    S1 = ra_seq:from_list([1, 2]),
    {[1, 2], I1} = ra_seq:list_chunk(5, S1),
    end_of_seq = ra_seq:list_chunk(5, I1),

    %% Test chunking a sequence with gaps
    S2 = ra_seq:from_list([1, 2, 3, 5, 6, 8, 9, 10, 12]),
    {[1, 2, 3], I2} = ra_seq:list_chunk(3, S2),
    {[5, 6, 8], I3} = ra_seq:list_chunk(3, I2),
    {[9, 10, 12], I4} = ra_seq:list_chunk(3, I3),
    end_of_seq = ra_seq:list_chunk(3, I4),

    %% Test with chunk size of 1
    S3 = ra_seq:from_list([1, 2, 3]),
    {[1], I5} = ra_seq:list_chunk(1, S3),
    {[2], I6} = ra_seq:list_chunk(1, I5),
    {[3], I7} = ra_seq:list_chunk(1, I6),
    end_of_seq = ra_seq:list_chunk(1, I7),

    %% Test that final chunk can be smaller than chunk size
    S4 = ra_seq:from_list([1, 2, 3, 4, 5]),
    {[1, 2, 3], I8} = ra_seq:list_chunk(3, S4),
    {[4, 5], I9} = ra_seq:list_chunk(3, I8),
    end_of_seq = ra_seq:list_chunk(3, I9),

    %% Test with a large range (verifies lazy behavior works)
    S5 = [{1, 100}],
    {Chunk1, I10} = ra_seq:list_chunk(16, S5),
    ?assertEqual(lists:seq(1, 16), Chunk1),
    {Chunk2, _I11} = ra_seq:list_chunk(16, I10),
    ?assertEqual(lists:seq(17, 32), Chunk2),

    ok.

remove_prefix(_Config) ->
    S0 = ra_seq:from_list([2, 3, 5, 6, 8, 9, 10, 12]),
    Pref1 = ra_seq:from_list([2, 3, 5]),
    {ok, S1} = ra_seq:remove_prefix(Pref1, S0),
    [12, 10, 9, 8, 6] = ra_seq:expand(S1),

    %% prefix includes already removed items
    Pref2 = ra_seq:from_list([1, 2, 3, 5]),
    {ok, S2} = ra_seq:remove_prefix(Pref2, S0),
    [12, 10, 9, 8, 6] = ra_seq:expand(S2),
    %% not a prefix
    Pref3 = ra_seq:from_list([5, 6, 8]),
    {error, not_prefix} = ra_seq:remove_prefix(Pref3, S0),

    {ok, []} = ra_seq:remove_prefix(S0, S0),
    ok.

remove_prefix_2(_Config) ->
    S1 = ra_seq:from_list([2, 3, 4, 5]),
    S2 = ra_seq:from_list([1, 2, 3]),
    {ok, [5, 4]} = ra_seq:remove_prefix(S2, S1),
    ok.

from_list_with_duplicates(_Config) ->
    S1 = ra_seq:from_list([1, 2, 2, 3]),
    [{1, 3}] = S1,
    [3, 2, 1] = ra_seq:expand(S1),

    S2 = ra_seq:from_list([5, 5, 5, 5]),
    [5] = S2,
    [5] = ra_seq:expand(S2),

    S3 = ra_seq:from_list([3, 1, 2, 1, 3, 2]),
    [{1, 3}] = S3,
    [3, 2, 1] = ra_seq:expand(S3),

    S4 = ra_seq:from_list([1, 2, 3, 3, 5, 6, 7, 7, 10, 11, 11]),
    [11, 10, {5, 7}, {1, 3}] = S4,
    [11, 10, 7, 6, 5, 3, 2, 1] = ra_seq:expand(S4),

    ok.

has_overlap(_Config) ->
    %% Test with empty sequence
    ?assertEqual(false, ra_seq:has_overlap({1, 10}, [])),

    %% Test with undefined range
    S = ra_seq:from_list([1, 2, 3, 5, 6, 7, 8, 9, 11]),
    ?assertEqual(false, ra_seq:has_overlap(undefined, S)),

    %% Test overlap with single integer elements
    %% S = [11, {5, 9}, {1, 3}]
    ?assertEqual(true, ra_seq:has_overlap({11, 11}, S)),
    ?assertEqual(true, ra_seq:has_overlap({10, 12}, S)),
    ?assertEqual(false, ra_seq:has_overlap({12, 15}, S)),

    %% Test overlap with range elements
    ?assertEqual(true, ra_seq:has_overlap({5, 5}, S)),
    ?assertEqual(true, ra_seq:has_overlap({7, 8}, S)),
    ?assertEqual(true, ra_seq:has_overlap({1, 3}, S)),
    ?assertEqual(true, ra_seq:has_overlap({2, 2}, S)),

    %% Test no overlap - gap in sequence
    ?assertEqual(false, ra_seq:has_overlap({4, 4}, S)),
    ?assertEqual(false, ra_seq:has_overlap({10, 10}, S)),

    %% Test range at the very start of sequence (index 0 and 1)
    ?assertEqual(true, ra_seq:has_overlap({0, 1}, S)),

    %% Test range entirely above sequence
    ?assertEqual(false, ra_seq:has_overlap({100, 200}, S)),

    %% Test partial overlap at boundaries
    ?assertEqual(true, ra_seq:has_overlap({0, 1}, S)),
    ?assertEqual(true, ra_seq:has_overlap({9, 15}, S)),

    %% Test range spanning entire sequence
    ?assertEqual(true, ra_seq:has_overlap({0, 100}, S)),

    %% Test with sequence containing only single integers (no ranges)
    S2 = [10, 5, 2],
    ?assertEqual(true, ra_seq:has_overlap({5, 5}, S2)),
    ?assertEqual(true, ra_seq:has_overlap({1, 3}, S2)),
    ?assertEqual(false, ra_seq:has_overlap({3, 4}, S2)),
    ?assertEqual(false, ra_seq:has_overlap({6, 9}, S2)),

    %% Test with sequence containing only ranges
    S3 = [{100, 200}, {50, 75}, {1, 25}],
    ?assertEqual(true, ra_seq:has_overlap({150, 160}, S3)),
    ?assertEqual(true, ra_seq:has_overlap({60, 70}, S3)),
    ?assertEqual(true, ra_seq:has_overlap({10, 20}, S3)),
    ?assertEqual(false, ra_seq:has_overlap({26, 49}, S3)),
    ?assertEqual(false, ra_seq:has_overlap({76, 99}, S3)),

    %% Verify consistency with in_range (non-empty result means overlap)
    ?assertEqual(true, ra_seq:has_overlap({5, 9}, S)),
    ?assertNotEqual([], ra_seq:in_range({5, 9}, S)),
    ?assertEqual(false, ra_seq:has_overlap({4, 4}, S)),
    ?assertEqual([], ra_seq:in_range({4, 4}, S)),

    ok.

in(_Config) ->
    ?assertEqual(false, ra_seq:in(1, [])),

    %% S = [11, {5, 9}, {1, 3}]
    S = ra_seq:from_list([1, 2, 3, 5, 6, 7, 8, 9, 11]),
    ?assertEqual(true, ra_seq:in(11, S)),
    ?assertEqual(true, ra_seq:in(1, S)),
    ?assertEqual(true, ra_seq:in(3, S)),
    ?assertEqual(true, ra_seq:in(5, S)),
    ?assertEqual(true, ra_seq:in(7, S)),
    ?assertEqual(true, ra_seq:in(9, S)),

    %% absent, above the whole sequence - must exit early rather than scan
    ?assertEqual(false, ra_seq:in(12, S)),
    %% absent, in the gap directly above a range
    ?assertEqual(false, ra_seq:in(10, S)),
    %% absent, in the gap between two ranges
    ?assertEqual(false, ra_seq:in(4, S)),
    %% absent, below the whole sequence
    ?assertEqual(false, ra_seq:in(0, S)),

    ok.

%%%===================================================================
%%% Property tests
%%%===================================================================

prop_from_list_canonical(_Config) ->
    run_proper(
      fun () ->
              ?FORALL(L, list(index()),
                      begin
                          S = ra_seq:from_list(L),
                          is_canonical(S) andalso
                          lists:usort(L) == lists:sort(ra_seq:expand(S))
                      end)
      end, [], 1000).

%% expand/1 must always yield strictly descending, duplicate free indexes.
%% add/2 and remove_prefix/2 both rely on this ordering.
prop_expand_descending(_Config) ->
    run_proper(
      fun () ->
              ?FORALL(S, seq(),
                      begin
                          E = ra_seq:expand(S),
                          E == lists:reverse(lists:usort(E))
                      end)
      end, [], 1000).

prop_add_model(_Config) ->
    run_proper(
      fun () ->
              ?FORALL({Add, To}, {seq(), seq()},
                      ra_seq:expand(ra_seq:add(Add, To)) ==
                          model_add(Add, To))
      end, [], 2000).

prop_add_canonical(_Config) ->
    run_proper(
      fun () ->
              ?FORALL({Add, To}, {seq(), seq()},
                      is_canonical(ra_seq:add(Add, To)))
      end, [], 2000).

%% the result of add/2 must still be appendable, i.e. append/2 must not
%% raise a function_clause on the next index
prop_add_appendable(_Config) ->
    run_proper(
      fun () ->
              ?FORALL({Add, To}, {non_empty_seq(), seq()},
                      begin
                          S = ra_seq:add(Add, To),
                          Next = ra_seq:last(S) + 1,
                          is_canonical(ra_seq:append(Next, S))
                      end)
      end, [], 2000).

prop_remove_prefix_model(_Config) ->
    run_proper(
      fun () ->
              ?FORALL({Prefix, Seq}, prefix_and_seq(),
                      begin
                          Expected = model_remove_prefix(Prefix, Seq),
                          case ra_seq:remove_prefix(Prefix, Seq) of
                              {ok, S} ->
                                  {ok, ra_seq:expand(S)} == Expected;
                              {error, not_prefix} = Err ->
                                  Err == Expected
                          end
                      end)
      end, [], 3000).

prop_remove_prefix_canonical(_Config) ->
    run_proper(
      fun () ->
              ?FORALL({Prefix, Seq}, prefix_and_seq(),
                      case ra_seq:remove_prefix(Prefix, Seq) of
                          {ok, S} ->
                              is_canonical(S);
                          {error, not_prefix} ->
                              true
                      end)
      end, [], 3000).

%% splitting a sequence at an arbitrary point and removing the lower half
%% must always yield exactly the upper half
prop_remove_prefix_roundtrip(_Config) ->
    run_proper(
      fun () ->
              ?FORALL({Seq, N}, {seq(), integer(0, 30)},
                      begin
                          %% expand/1 is descending, take the N lowest
                          Asc = lists:reverse(ra_seq:expand(Seq)),
                          K = min(N, length(Asc)),
                          {Low, High} = lists:split(K, Asc),
                          Prefix = ra_seq:from_list(Low),
                          Expected = ra_seq:from_list(High),
                          case ra_seq:remove_prefix(Prefix, Seq) of
                              {ok, Expected} ->
                                  true;
                              _ ->
                                  false
                          end
                      end)
      end, [], 2000).

%% if the prefix omits an index that the sequence holds below the prefix's
%% last index, that must be reported rather than silently dropped
prop_remove_prefix_detects_gaps(_Config) ->
    run_proper(
      fun () ->
              ?FORALL(Seq, seq_of_at_least(2),
                      begin
                          Asc = lists:reverse(ra_seq:expand(Seq)),
                          %% a prefix of everything but the lowest index,
                          %% so it always covers something it should not
                          Prefix = ra_seq:from_list(tl(Asc)),
                          {error, not_prefix} ==
                              ra_seq:remove_prefix(Prefix, Seq)
                      end)
      end, [], 2000).

%% in/2 relies on the high -> low ordering to exit early; check it against
%% the naive membership test on the fully expanded sequence.
prop_in_model(_Config) ->
    run_proper(
      fun () ->
              ?FORALL({Idx, S}, {index(), seq()},
                      ra_seq:in(Idx, S) == lists:member(Idx, ra_seq:expand(S)))
      end, [], 2000).

%%%===================================================================
%%% Generators
%%%===================================================================

index() ->
    integer(0, 40).

%% A mix of sparse sequences (mostly singletons, exercises the gap
%% handling) and run based sequences (multi index ranges, exercises the
%% range merging and splitting paths).
seq() ->
    union([sparse_seq(), run_seq()]).

sparse_seq() ->
    ?LET(L, list(index()), ra_seq:from_list(L)).

run_seq() ->
    ?LET(Runs, list({index(), integer(1, 8)}),
         ra_seq:from_list(
           lists:append([lists:seq(S, S + Len - 1) || {S, Len} <- Runs]))).

non_empty_seq() ->
    ?SUCHTHAT(S, seq(), S =/= []).

seq_of_at_least(N) ->
    ?SUCHTHAT(S, seq(), ra_seq:length(S) >= N).

%% Prefix/sequence pairs. Weighted towards genuine prefixes so that the
%% success path is well covered, but includes prefixes with extra lower
%% indexes (legal, they have already been removed) and entirely unrelated
%% sequences (which must be rejected).
prefix_and_seq() ->
    ?LET(Seq, seq(),
         frequency(
           [{5, {genuine_prefix(Seq), Seq}},
            {3, {prefix_with_extras(Seq), Seq}},
            {2, {seq(), Seq}}])).

genuine_prefix(Seq) ->
    ?LET(N, integer(0, 30),
         begin
             Asc = lists:reverse(ra_seq:expand(Seq)),
             ra_seq:from_list(lists:sublist(Asc, min(N, length(Asc))))
         end).

prefix_with_extras(Seq) ->
    ?LET({N, Extras}, {integer(0, 30), list(index())},
         begin
             Asc = lists:reverse(ra_seq:expand(Seq)),
             Taken = lists:sublist(Asc, min(N, length(Asc))),
             Ceil = case Taken of
                        [] -> 0;
                        _ -> lists:max(Taken)
                    end,
             %% extras must stay at or below the prefix's last index, else
             %% they would extend the prefix over indexes of Seq that are
             %% deliberately not being removed
             ra_seq:from_list(Taken ++ [E || E <- Extras, E =< Ceil])
         end).

%%%===================================================================
%%% Property helpers
%%%===================================================================

%% from_list/1 is usort + repeated append/2, so it produces the canonical
%% representation by construction. Any sequence must round-trip through it.
is_canonical(S) ->
    S == ra_seq:from_list(ra_seq:expand(S)).

model_add(Add, To) ->
    A = ra_seq:expand(Add),
    T = ra_seq:expand(To),
    case {A, T} of
        {[], _} ->
            T;
        {_, []} ->
            A;
        _ ->
            Fst = lists:min(A),
            lists:reverse(lists:sort([I || I <- T, I < Fst] ++ A))
    end.

model_remove_prefix(Prefix, Seq) ->
    P = ra_seq:expand(Prefix),
    S = ra_seq:expand(Seq),
    case {P, S} of
        {[], _} ->
            {ok, S};
        {_, []} ->
            {ok, []};
        _ ->
            PrefLast = lists:max(P),
            Low = [I || I <- S, I =< PrefLast],
            case Low -- P of
                [] ->
                    {ok, [I || I <- S, I > PrefLast]};
                _ ->
                    {error, not_prefix}
            end
    end.

run_proper(Fun, Args, NumTests) ->
    ?assertEqual(
       true,
       proper:counterexample(erlang:apply(Fun, Args),
                             [{numtests, NumTests},
                              {on_output,
                               fun(".", _) -> ok;
                                  (F, A) -> ct:pal(?LOW_IMPORTANCE, F, A)
                               end}])).
