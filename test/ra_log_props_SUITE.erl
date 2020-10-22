%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2017-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%
-module(ra_log_props_SUITE).
-compile(export_all).

-include_lib("proper/include/proper.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

all() ->
    [
     {group, tests}
    ].

%% these tests were useful during the early days but it isn't clear how
%% much they now contribute
%% TODO: consider refactoring using a more condensed set of properties
%% that only test clear log invariants (e.g. overwritten entries are never read).
all_tests() ->
    [
     write,
     % write_missing_entry,
     % multi_write_missing_entry,
     write_overwrite_entry
     % write_index_starts_zero,
     % append,
     % append_missing_entry,
     % append_overwrite_entry,
     % append_index_starts_one,
     % take,
     % take_out_of_range,
     % fetch,
     % fetch_out_of_range,
     % last_index_term,
     % fetch_term,
     % fetch_out_of_range_term,
     % next_index_term,
     % last_written,
     % last_written_with_wal,
     % last_written_with_segment_writer,
     % last_written_with_crashing_segment_writer
    ].

groups() ->
    [
     {tests, [], all_tests()}
    ].

init_per_suite(Config) ->
    {ok, _} = ra:start([{data_dir, ?config(priv_dir, Config)},
                        {segment_max_entries, 128}]),
    Config.

end_per_suite(Config) ->
    application:stop(ra),
    Config.

init_per_testcase(TestCase, Config) ->
    application:stop(ra),
    PrivDir = ?config(priv_dir, Config),
    Dir = filename:join(PrivDir, TestCase),
    ra:start_in(Dir),
    % register(TestCase, self()),
    UId = atom_to_binary(TestCase, utf8),
    ok = ra_directory:register_name(default, UId, self(), undefined,
                                    TestCase, TestCase),
    [{test_case, UId}, {wal_dir, Dir} | Config].

%%------------------
%% Generators
%%------------------

log_entries_gen(N) ->
    ?LET(Length, choose(N, 100),
         ?LET(Terms, term_sequence_gen(Length),
              [{Idx, Term, <<Idx:64/integer>>}
               || {Idx, Term} <- lists:zip(lists:seq(1, Length),
                                           Terms)])).

term_sequence_gen(N) ->
    ?LET(List, vector(N, non_neg_integer()),
         lists:sort(List)).

wait_sequence_gen(N) ->
    ?LET(List, vector(N, wait_gen()), List).

wait_gen() ->
    frequency([{8, 0}, {5, choose(0, 20)}, {1, choose(25, 150)}]).

consume_gen(N) ->
    ?LET(List, vector(N, boolean()), List).

log_entry_but_one_gen() ->
    ?LET(Idx, ?SUCHTHAT(Int, integer(), Int =/= 1),
         {Idx, 1, <<Idx:64/integer>>}).

log_entry_but_one_zero_gen() ->
    ?LET(Idx, ?SUCHTHAT(Int, integer(), (Int =/= 1) and (Int =/= 0)),
         {Idx, 1, <<Idx:64/integer>>}).

slice_gen(Entries) ->
    %% Head might be an empty list
    ?LET(N, choose(1, max_length(Entries)),
         begin
             {Head, [NEntry | Tail]} = lists:split(N - 1, Entries),
             {Head, NEntry, Tail}
         end).

sorted_subset_gen(Entries) ->
    ?LET(Subset, list(elements(Entries)), lists:sort(Subset)).

max_length(Entries) when length(Entries) > 1 ->
    length(Entries) - 1;
max_length(_) ->
    1.

range_gen(Entries) ->
    %% Range can finish anywhere after total number of entries
    ?LET(Start, between_gen(1, length(Entries)),
         ?LET(Num, greater_than_gen(1),
              {Start, Num})).

out_of_range_begin_gen() ->
    %% The range starts before the initial index
    ?LET(Start, less_than_gen(0),
         ?LET(Num, greater_than_gen(0),
              {Start, Num})).

out_of_range_end_gen(Entries) ->
    %% The range starts after the last index
    ?LET(Start, greater_than_gen(length(Entries)),
         ?LET(Num, non_neg_integer(),
              {Start, Num})).

between_gen(N, M) ->
    choose(N, M).

greater_than_gen(N) ->
    integer(N + 1, inf).

less_than_gen(N) ->
    integer(inf, N - 1).

out_of_range_gen(Entries) ->
    oneof([out_of_range_begin_gen(),
           out_of_range_end_gen(Entries)]).

%%------------------
%% Properties
%%------------------

write(Config) ->
    %% There is no way to create a log file from a list of entries without the write
    %% API. We have to prove first that writting a consecutive log file succeeds,
    %% so we can use it as a base for our tests
    TestCase = ?config(test_case, Config),
    run_proper(fun write_prop/1, [TestCase], 100).

write_prop(TestCase) ->
    ?FORALL(
       Entries, log_entries_gen(1),
       begin
           {ok, Log0} = ra_log:write(
                          Entries,
                          ra_log_init(#{uid => TestCase})),
           {LogEntries, _, Log} = ra_log:take(1, length(Entries), Log0),
           reset(Log),
           ?WHENFAIL(io:format("Entries taken from the log: ~p~nRa log state: ~p",
                               [LogEntries, Log]),
                     Entries == LogEntries)
       end).

append_all([], Log) ->
    Log;
append_all([Entry | Entries], Log0) ->
    Log = ra_log:append(Entry, Log0),
    append_all(Entries, Log).

write_missing_entry(Config) ->
    TestCase = ?config(test_case, Config),
    run_proper(fun write_missing_entry_prop/1, [TestCase], 100).

write_missing_entry_prop(TestCase) ->
    ?FORALL(
       Entries, log_entries_gen(3),
       ?FORALL(
          {Head, _Entry, Tail}, slice_gen(Entries),
          begin
              Log = ra_log_init(#{uid => TestCase}),
              Reply = ra_log:write(Head ++ Tail, Log),
              reset(Log),
              ?WHENFAIL(ct:pal("Reply: ~p", [Reply]),
                        case Reply of
                            {error, {integrity_error, _}} -> true;
                            _ -> false
                        end)
          end)).

write_overwrite_entry(Config) ->
    TestCase = ?config(test_case, Config),
    run_proper(fun write_overwrite_entry_prop/1, [TestCase], 250).

write_overwrite_entry_prop(TestCase) ->
    ?FORALL(
       Entries, log_entries_gen(3),
       ?FORALL(
          {Head, {Idx, Term, _Value} = _Entry, _Tail}, slice_gen(Entries),
          begin
              {ok, Log0} = ra_log:write(
                                Entries,
                                ra_log_init(#{uid => TestCase})),
              NewEntry = [{Idx, Term, <<"overwrite">>}],
              {ok, Log} = ra_log:write(NewEntry, Log0),
              {LogEntries, _, Log1} = ra_log:take(1, length(Entries), Log),
              reset(Log1),
              ?WHENFAIL(io:format("Head: ~p~n New entry: ~p~n"
                                  "Entries taken from the log: ~p~n"
                                  "Ra log state: ~p",
                                  [Head, NewEntry, LogEntries, Log1]),
                        ((Head ++ NewEntry) == LogEntries))
          end)).

multi_write_missing_entry(Config) ->
    TestCase = ?config(test_case, Config),
    run_proper(fun multi_write_missing_entry_prop/1, [TestCase], 100).

multi_write_missing_entry_prop(TestCase) ->
    ?FORALL(
       Entries, log_entries_gen(3),
       ?FORALL(
          {Head, _Entry, Tail}, slice_gen(Entries),
          begin
              {ok, Log0} = ra_log:write(
                                Head,
                                ra_log_init(#{uid => TestCase})),
              Reply = ra_log:write(Tail, Log0),
              reset(Log0),
              ?WHENFAIL(io:format("Reply: ~p", [Reply]),
                        case Reply of
                            {error, {integrity_error, _}} -> true;
                            _ -> false
                        end)
          end)).

append_missing_entry(Config) ->
    TestCase = ?config(test_case, Config),
    run_proper(fun append_missing_entry_prop/1, [TestCase], 100).

append_missing_entry_prop(TestCase) ->
    ?FORALL(
       Entries, log_entries_gen(3),
       ?FORALL(
          {Head, _Entry, Tail}, slice_gen(Entries),
          begin
              Log0 = append_all(Head,
                               ra_log_init(#{uid => TestCase})),
              Failed = try
                           ra_log:append(hd(Tail), Log0),
                           false
                       catch
                           exit:{integrity_error, _} ->
                               true
                       end,
              {LogEntries, _, Log} = ra_log:take(1, length(Head), Log0),
              reset(Log),
              ?WHENFAIL(io:format("Failed: ~p~nHead: ~p~n Tail: ~p~n"
                                  "Entries taken from the log: ~p~n"
                                  "Ra log state: ~p",
                                  [Failed, Head, Tail, LogEntries, Log]),
                        (Head == LogEntries) and Failed)
          end)).

write_index_starts_zero(Config) ->
    TestCase = ?config(test_case, Config),
    run_proper(fun write_index_starts_zero_prop/1, [TestCase], 100).

write_index_starts_zero_prop(TestCase) ->
    ?FORALL(
       Entry, log_entry_but_one_zero_gen(),
       begin
           Log = ra_log_init(#{uid => TestCase}),
           Reply = ra_log:write([Entry], Log),
           reset(Log),
           ?WHENFAIL(io:format("Reply: ~p", [Reply]),
                     case Reply of
                         {error, {integrity_error, _}} -> true;
                         _ -> false
                     end)
       end).

append(Config) ->
    %% There is no way to create a log file from a list of entries without the
    %% write
    %% API. We have to prove first that writting a consecutive log file succeeds,
    %% so we can use it as a base for our tests
    TestCase = ?config(test_case, Config),
    run_proper(fun append_prop/1, [TestCase], 100).

append_prop(TestCase) ->
    ?FORALL(
       Entries, log_entries_gen(1),
       begin
           Log0 = append_all(
                   Entries,
                   ra_log_init(#{uid => TestCase})),
           {LogEntries, _, Log} = ra_log:take(1, length(Entries), Log0),
           reset(Log),
           ?WHENFAIL(io:format("Entries taken from the log: ~p~nRa log state: ~p",
                               [LogEntries, Log]),
                     Entries == LogEntries)
       end).

append_overwrite_entry(Config) ->
    TestCase = ?config(test_case, Config),
    run_proper(fun append_overwrite_entry_prop/1, [TestCase], 100).

append_overwrite_entry_prop(TestCase) ->
    ?FORALL(
       Entries, log_entries_gen(3),
       ?FORALL(
          {_Head, {Idx, Term, _Value} = _Entry, _Tail}, slice_gen(Entries),
          begin
              {ok, Log} = ra_log:write(
                                Entries,
                                ra_log_init(#{uid => TestCase})),
              Failed = try
                           ra_log:append({Idx, Term, <<"overwrite">>}, Log),
                           false
                       catch
                           exit:{integrity_error, _} ->
                               true
                       end,
              reset(Log),
              ?WHENFAIL(io:format("Failed: ~p", [Failed]),
                        Failed)
          end)).

append_index_starts_one(Config) ->
    TestCase = ?config(test_case, Config),
    run_proper(fun append_index_starts_one_prop/1, [TestCase], 100).

append_index_starts_one_prop(TestCase) ->
    ?FORALL(
       Entry, log_entry_but_one_gen(),
       begin
           Log = ra_log_init(#{uid => TestCase}),
           Failed = try
                       ra_log:append(Entry, Log),
                       false
                   catch
                       exit:{integrity_error, _} ->
                           true
                   end,
           reset(Log),
           ?WHENFAIL(io:format("Failed: ~p Entry: ~p", [Failed, Entry]), Failed)
       end).

take(Config) ->
    TestCase = ?config(test_case, Config),
    run_proper(fun take_prop/1, [TestCase], 100).

take_prop(TestCase) ->
    ?FORALL(
       Entries, log_entries_gen(1),
       ?FORALL(
          {Start, Num}, range_gen(Entries),
          begin
              {ok, Log0} = ra_log:write(
                                 Entries,
                                 ra_log_init(#{uid => TestCase})),
              {Selected, _, Log} = ra_log:take(Start, Num, Log0),
              Expected = lists:sublist(Entries, Start, Num),
              reset(Log),
              ?WHENFAIL(io:format("Selected: ~p~nExpected: ~p",
                                  [Selected, Expected]),
                        Selected == Expected)
          end)).

take_out_of_range(Config) ->
    TestCase = ?config(test_case, Config),
    run_proper(fun take_out_of_range_prop/1, [TestCase], 100).

take_out_of_range_prop(TestCase) ->
    ?FORALL(
       Entries, log_entries_gen(1),
       ?FORALL(
          {Start, Num}, out_of_range_gen(Entries),
          begin
              {ok, Log0} = ra_log:write(
                                Entries,
                                ra_log_init(#{uid => TestCase})),
              {Reply, _, Log} = ra_log:take(Start, Num, Log0),
              reset(Log),
              ?WHENFAIL(io:format("Start: ~p Num: ~p~nReply: ~p", [Start, Num, Reply]),
                        Reply == [])
          end)).

fetch(Config) ->
    TestCase = ?config(test_case, Config),
    run_proper(fun fetch_prop/1, [TestCase], 100).

fetch_prop(TestCase) ->
    ?FORALL(
       Entries, log_entries_gen(1),
       ?FORALL(
          {_Head, {Idx, _Term, _Value} = Entry, _Tail}, slice_gen(Entries),
          begin
              {ok, Log0} = ra_log:write(
                                Entries,
                                ra_log_init(#{uid => TestCase})),
              {Got, Log} = ra_log:fetch(Idx, Log0),
              reset(Log),
              ?WHENFAIL(io:format("Got: ~p Expected: ~p", [Got, Entry]),
                        Entry == Got)
          end)).

fetch_out_of_range(Config) ->
    TestCase = ?config(test_case, Config),
    run_proper(fun fetch_out_of_range_prop/1, [TestCase], 100).

fetch_out_of_range_prop(TestCase) ->
    ?FORALL(
       Entries, log_entries_gen(1),
       ?FORALL(
          {Start, _Num}, out_of_range_gen(Entries),
          begin
              {ok, Log0} = ra_log:write(
                                Entries,
                                ra_log_init(#{uid => TestCase})),
              {Reply, Log} = ra_log:fetch(Start, Log0),
              reset(Log),
              ?WHENFAIL(io:format("Got: ~p Expected: undefined", [Reply]),
                        Reply == undefined)
          end)).

last_index_term(Config) ->
    TestCase = ?config(test_case, Config),
    run_proper(fun last_index_term_prop/1, [TestCase], 100).

last_index_term_prop(TestCase) ->
    ?FORALL(
       Entries, log_entries_gen(0),
       begin
           {ok, Log} = ra_log:write(
                             Entries,
                             ra_log_init(#{uid => TestCase})),
           {LastIdx, LastTerm} = case Entries of
                                     [] ->
                                         {0, 0};
                                     _ ->
                                         {LI, LT, _} = lists:last(Entries),
                                         {LI, LT}
                                 end,
           {Idx, Term} = ra_log:last_index_term(Log),
           reset(Log),
           ?WHENFAIL(io:format("Got: ~p Expected: ~p", [{Idx, Term}, {LastIdx, LastTerm}]),
                     (LastIdx == Idx) and (LastTerm == Term))
       end).

fetch_term(Config) ->
    TestCase = ?config(test_case, Config),
    run_proper(fun fetch_term_prop/1, [TestCase], 100).

fetch_term_prop(TestCase) ->
    ?FORALL(
       Entries, log_entries_gen(1),
       ?FORALL(
          {_Head, {Idx, ExpectedTerm, _}, _Tail}, slice_gen(Entries),
          begin
              {ok, Log0} = ra_log:write(
                                Entries,
                                ra_log_init(#{uid => TestCase})),
              {Term, Log} = ra_log:fetch_term(Idx, Log0),
              reset(Log),
              ?WHENFAIL(io:format("Got: ~p Expected: ~p", [Term, ExpectedTerm]),
                        (ExpectedTerm == Term))
          end)).

fetch_out_of_range_term(Config) ->
    TestCase = ?config(test_case, Config),
    run_proper(fun fetch_out_of_range_term_prop/1, [TestCase], 100).

fetch_out_of_range_term_prop(TestCase) ->
    ?FORALL(
       Entries, log_entries_gen(1),
       ?FORALL(
          {Start, _}, out_of_range_gen(Entries),
          begin
              {ok, Log0} = ra_log:write(
                                 Entries,
                                 ra_log_init(#{uid => TestCase})),
              {Term, Log} = ra_log:fetch_term(Start, Log0),
              reset(Log),
              ?WHENFAIL(io:format("Got: ~p for index: ~p", [Term, Start]),
                        (undefined == Term) orelse ((0 == Term) and (Start == 0)))
          end)).

next_index_term(Config) ->
    TestCase = ?config(test_case, Config),
    run_proper(fun last_index_term_prop/1, [TestCase], 100).

next_index_term_prop(TestCase) ->
    ?FORALL(
       Entries, log_entries_gen(1),
       begin
           {ok, Log} = ra_log:write(
                              Entries,
                              ra_log_init(#{uid => TestCase})),
           {LastIdx, _LastTerm, _} = lists:last(Entries),
           Idx = ra_log:next_index(Log),
           reset(Log),
           ?WHENFAIL(io:format("Got: ~p Expected: ~p", [Idx, LastIdx + 1]),
                     LastIdx + 1 == Idx)
       end).


last_written_with_wal(Config) ->
    TestCase = ?config(test_case, Config),
    run_proper(fun last_written_with_wal_prop/1, [TestCase], 15).

build_action_list(Entries, Actions) ->
    lists:flatten(lists:map(fun(Index) ->
                                    E = lists:nth(Index, Entries),
                                    A = lists:foldl(fun({A0, I}, Acc) when I == Index ->
                                                            [A0 | Acc];
                                                       (_, Acc) ->
                                                            Acc
                                                    end, [], Actions),
                                    [E | A]
                            end, lists:seq(1, length(Entries)))).

position(Entries) ->
    choose(1, length(Entries)).

last_written_with_wal_prop(TestCase) ->
    ok = logger:set_primary_config(level, all),
    ?FORALL(
       Entries, log_entries_gen(1),
       ?FORALL(
          Actions, list(frequency([{5, {{wait, wait_gen()}, position(Entries)}},
                                   {3, {consume, position(Entries)}},
                                   {2, {roll_wal, position(Entries)}},
                                   {2, {stop_wal, position(Entries)}},
                                   {2, {start_wal, position(Entries)}}])),
          begin
              flush(),
              All = build_action_list(Entries, Actions),
              Log0 = ra_log_init(#{uid => TestCase}),
              {Log, Last, LastIdx, _Status} =
                  lists:foldl(fun({wait, Wait}, Acc) ->
                                      timer:sleep(Wait),
                                      Acc;
                                 (consume, {Acc0, Last0, LastIdx, St}) ->
                                      {Acc1, Last1} = consume_events(Acc0, Last0),
                                      {Acc1, Last1, LastIdx, St};
                                 (roll_wal, {_, _, _, wal_down} = Acc) ->
                                      Acc;
                                 (roll_wal, Acc) ->
                                      ra_log_wal:force_roll_over(ra_log_wal),
                                      Acc;
                                 (stop_wal, {Acc0, Last0, LastIdx, wal_up}) ->
                                      ok = supervisor:terminate_child(wal_sup(), ra_log_wal),
                                      {Acc0, Last0, LastIdx, wal_down};
                                 (stop_wal, {_, _, _, wal_down} = Acc) ->
                                      Acc;
                                 (start_wal, {Acc0, Last0, LastIdx, wal_down}) ->
                                      supervisor:restart_child(wal_sup(), ra_log_wal),
                                      {Acc0, Last0, LastIdx, wal_up};
                                 (start_wal, {_, _, _, wal_up} = Acc) ->
                                      Acc;
                                 ({Idx, _, _} = Entry, {Acc0, _, LastIdx, _} = Acc) when Idx > LastIdx + 1 ->
                                      {error, {integrity_error, _}} = ra_log:write([Entry], Acc0),
                                      Acc;
                                 (Entry, {Acc0, _, _, wal_down} = Acc) ->
                                      {error, wal_down} = ra_log:write([Entry], Acc0),
                                      Acc;
                                 ({Idx, _, _} = Entry, {Acc0, Last0, _LastIdx, St}) ->
                                      {ok, Acc} = ra_log:write([Entry], Acc0),
                                      {Acc, Last0, Idx, St}
                              end, {Log0, {0, 0}, 0, wal_up}, All),
              Got = ra_log:last_written(Log),
              {Written, _, Log1} = ra_log:take(1, LastIdx, Log),
              reset(Log1),
              ?WHENFAIL(io:format("Got: ~p, Expected: ~p Written: ~p~n Actions: ~p",
                                  [Got, Last, Written, All]),
                        (Got ==  Last) and (Written == lists:sublist(Entries, 1, LastIdx)))
          end)).

last_written_with_segment_writer(Config) ->
    TestCase = ?config(test_case, Config),
    run_proper(fun last_written_with_segment_writer_prop/1, [TestCase], 25).

last_written_with_segment_writer_prop(TestCase) ->
    ?FORALL(
       Entries, log_entries_gen(1),
       ?FORALL(
          Actions, list(frequency([{5, {{wait, wait_gen()}, position(Entries)}},
                                   {3, {consume, position(Entries)}},
                                   {2, {stop_segment_writer, position(Entries)}},
                                   {2, {start_segment_writer, position(Entries)}}])),
          begin
              flush(),
              All = build_action_list(Entries, Actions),
              _ = supervisor:restart_child(ra_log_sup, ra_log_segment_writer),
              Log0 = ra_log_init(#{uid => TestCase}),
              {Log, Last, LastIdx, _Status} =
                  lists:foldl(fun({wait, Wait}, Acc) ->
                                      timer:sleep(Wait),
                                      Acc;
                                 (consume, {Acc0, Last0, LastIdx, St}) ->
                                      {Acc1, Last1} = consume_events(Acc0, Last0),
                                      {Acc1, Last1, LastIdx, St};
                                 (stop_segment_writer, {Acc0, Last0, LastIdx, sw_up}) ->
                                      ok = supervisor:terminate_child(ra_log_sup, ra_log_segment_writer),
                                      {Acc0, Last0, LastIdx, sw_down};
                                 (stop_segment_writer, {_, _, _, sw_down} = Acc) ->
                                      Acc;
                                 (start_segment_writer, {Acc0, Last0, LastIdx, sw_down}) ->
                                      {ok, _} = supervisor:restart_child(ra_log_sup, ra_log_segment_writer),
                                      {Acc0, Last0, LastIdx, sw_up};
                                 (start_segment_writer, {_, _, _, sw_up} = Acc) ->
                                      Acc;
                                 ({Idx, _, _} = Entry, {Acc0, _, LastIdx, _} = Acc) when Idx > LastIdx + 1 ->
                                      {error, {integrity_error, _}} = ra_log:write([Entry], Acc0),
                                      Acc;
                                 ({Idx, _, _} = Entry, {Acc0, Last0, _LastIdx, St}) ->
                                      {ok, Acc} = ra_log:write([Entry], Acc0),
                                      {Acc, Last0, Idx, St}
                              end, {Log0, {0, 0}, 0, sw_up}, All),
              Got = ra_log:last_written(Log),
              {Written, _, Log1} = ra_log:take(1, LastIdx, Log),
              reset(Log1),
              ?WHENFAIL(ct:pal("Got: ~p, Expected: ~p Written: ~p~n Actions: ~p",
                                  [Got, Last, Written, All]),
                        (Got ==  Last) and (Written == lists:sublist(Entries, 1, LastIdx)))
          end)).

last_written_with_crashing_segment_writer(Config) ->
    TestCase = ?config(test_case, Config),
    run_proper_noshrink(fun last_written_with_crashing_segment_writer_prop/1,
                        [TestCase], 1).

last_written_with_crashing_segment_writer_prop(TestCase) ->
    ?FORALL(
       Entries, log_entries_gen(1),
       ?FORALL(
          Actions, list(frequency([{5, {{wait, wait_gen()}, position(Entries)}},
                                   {3, {consume, position(Entries)}},
                                   {2, {crash_segment_writer, position(Entries)}}])),
          begin
              flush(),
              All = build_action_list(Entries, Actions),
              _ = supervisor:restart_child(ra_log_sup, ra_log_segment_writer),
              Log0 = ra_log_init(#{uid => TestCase,
                                   resend_window => 2}),
              ra_log:take(1, 10, Log0),
              {Log, _Last, Ts} =
                  lists:foldl(fun({wait, Wait}, Acc) ->
                                      timer:sleep(Wait),
                                      Acc;
                                 (consume, {Acc0, Last0, Ts}) ->
                                      Acc1 = deliver_log_events(Acc0, 500),
                                      {Acc1, Last0, Ts};
                                 (crash_segment_writer, {Acc0, Last0, _Ts}) ->
                                      Acc = case whereis(ra_log_segment_writer) of
                                                undefined ->
                                                    Acc0;
                                                P ->
                                                    Acc1 = deliver_log_events(Acc0, 500),
                                                    exit(P, kill),
                                                    Acc1
                                            end,
                                      {Acc, Last0, get_timestamp()};
                                 (Entry, {Acc0, Last0, Ts}) ->
                                      case ra_log:write([Entry], Acc0) of
                                          {ok, Acc} ->
                                              {Acc, Last0, Ts};
                                          {error, wal_down} ->
                                              wait_for_wal(50, 0),
                                              {ok, Acc} = ra_log:write([Entry], Acc0),
                                              {Acc, Last0, Ts}
                                      end
                              end, {Log0, {0, 0}, get_timestamp()}, All),
              %% We want to check that eventually we get the last written as the last entry,
              %% despite the segment writer crash. The log file might have to resend
              %% some entries after it, so it needs time to recover.
              timer:sleep(time_diff_to(Ts, 3000)),
              % write an entry to trigger resend protocol if required
              {LastIdx, LastTerm} = ra_log:last_index_term(Log),
              E = {LastIdx+1, LastTerm, <<>>},
              ActuallyLastIdxTerm = {LastIdx+1, LastTerm},
              {ok, Log1a} = ra_log:write([E], Log),
              Log1 = deliver_log_events(Log1a, 500),
              % Log1c =  deliver_log_events(Log1b, 500),
              %% Consume all events
              % {Log1, Last1} = consume_events(Log1b, Last),
              %% Request last written
              LastWritten = ra_log:last_written(Log1),
              %% Request entries available, which should be all generated by this test
              {EIdx, ETerm, _} = lists:last(Entries),
              LastEntry = {EIdx, ETerm},
              ct:pal("Log1 ~p~nopen ~p~nclosed~p", [Log1,
                                                      ets:tab2list(ra_log_open_mem_tables),
                                                      ets:tab2list(ra_log_closed_mem_tables)
                                                     ]),
              {Written, _, Log2} = ra_log:take(1, EIdx, Log1),
              %% We got all the data, can reset now
              basic_reset(Log2),
              ?WHENFAIL(ct:pal("Last written entry: ~p; actually last idx term: ~p;"
                               " last entry written: ~p~nEntries taken: ~p~n Actions: ~p",
                               [LastWritten, ActuallyLastIdxTerm, LastEntry, Written, Entries]),
                        (LastWritten == ActuallyLastIdxTerm)
                        and (Written == Entries))
          end)).

get_timestamp() ->
    {Mm, S, Mi} = os:timestamp(),
    (Mm * 1000000 + S) * 1000 + round(Mi / 1000).

time_diff_to(Ts, To) ->
    Tnow = get_timestamp(),
    case To - (Tnow - Ts) of
        T when T < 0 ->
            0;
        T ->
            T
    end.

wait_for_wal(N, N) ->
    exit(wait_for_wal_timeout);
wait_for_wal(M, N) ->
    timer:sleep(100),
    case whereis(ra_log_wal) of
        undefined ->
            wait_for_wal(M, N+1);
        _ -> ok
    end.


last_written(Config) ->
    TestCase = ?config(test_case, Config),
    run_proper(fun last_written_prop/1, [TestCase], 10).

last_written_prop(TestCase) ->
    ?FORALL(
       Entries, log_entries_gen(1),
       ?FORALL(
          {Waits, Consumes}, {wait_sequence_gen(length(Entries)), consume_gen(length(Entries))},
          begin
              flush(),
              Actions = lists:zip3(Entries, Waits, Consumes),
              Log0 = ra_log_init(#{uid => TestCase}),
              {Log, Last} = lists:foldl(fun({Entry, Wait, Consume}, {Acc0, Last0}) ->
                                                {ok, Acc} = ra_log:write([Entry], Acc0),
                                                timer:sleep(Wait),
                                                case Consume of
                                                    true ->
                                                        consume_events(Acc, Last0);
                                                    false ->
                                                        {Acc, Last0}
                                                end
                                end, {Log0, {0, 0}}, Actions),
              Got = ra_log:last_written(Log),
              reset(Log),
              ?WHENFAIL(io:format("Got: ~p, Expected: ~p~n Actions: ~p",
                                  [Got, Last, Actions]),
                        Got ==  Last)
          end)).

flush() ->
    receive
        {ra_log_event, _} ->
            flush()
    after 100 ->
            ok
    end.

deliver_log_events(Log0, Timeout) ->
    receive
        {ra_log_event, Evt} ->
            ct:pal("ra_log_evt: ~w", [Evt]),
            {Log, _} = ra_log:handle_event(Evt, Log0),
            deliver_log_events(Log, Timeout)
    after Timeout ->
            Log0
    end.

consume_events(Log0, Last) ->
    receive
        {ra_log_event, {written, {_, To, Term}} = Evt} ->
            {Log, _} = ra_log:handle_event(Evt, Log0),
            consume_events(Log, {To, Term})
    after 0 ->
            {Log0, Last}
    end.

consume_all_events(Log0, Last) ->
    receive
        {ra_log_event, {written, {_, To, Term}} = Evt} ->
            {Log, _} = ra_log:handle_event(Evt, Log0),
            consume_events(Log, {To, Term})
    after 15000 ->
            {Log0, Last}
    end.

last_idx_term([]) ->
    {0,0};
last_idx_term(List) ->
    {Idx, Term, _} = lists:last(lists:sort(List)),
    {Idx, Term}.

%% TODO refactor
run_proper(Fun, Args, NumTests) ->
    ?assertEqual(
       true,
       proper:counterexample(erlang:apply(Fun, Args),
			     [{numtests, NumTests},
			      {on_output, fun(".", _) -> ok; % don't print the '.'s on new lines
					     (F, A) -> ct:pal(?LOW_IMPORTANCE, F, A) end}])).

run_proper_noshrink(Fun, Args, NumTests) ->
    ?assertEqual(
       true,
       proper:counterexample(erlang:apply(Fun, Args),
			     [{numtests, NumTests},
                  noshrink,
			      {on_output, fun(".", _) -> ok; % don't print the '.'s on new lines
					     (F, A) -> ct:pal(?LOW_IMPORTANCE, F, A) end}])).
basic_reset(Log) ->
    ra_log:write([{0, 0, empty}], Log),
    receive
        {ra_log_event, {written, {_, 0, 0}}} ->
            ok
    end,
    ra_log:close(Log).

reset(Log) ->
    WalSup = wal_sup(),
    supervisor:restart_child(WalSup, ra_log_segment_writer),
    supervisor:restart_child(WalSup, ra_log_wal),
    basic_reset(Log).

wal_sup() ->
    [WalSup] = [P || {ra_log_wal_sup, P, _, _}
                     <- supervisor:which_children(ra_log_sup)],
    WalSup.

ra_log_init(Cfg) ->
    %% augment with default system config
    ra_log:init(Cfg#{system_config => ra_system:default_config()}).
