-module(ra_log_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

%% common ra_log tests to ensure behaviour is equivalent across
%% ra_log backends

all() ->
    [
     {group, ra_log_memory},
     {group, ra_log_file}
    ].

all_tests() ->
    [
     fetch_when_empty,
     fetch_not_found,
     append_then_fetch,
     write_then_fetch,
     append_then_fetch_no_wait,
     write_then_overwrite,
     append_integrity_error,
     take,
     last,
     meta
    ].

groups() ->
    [
     {ra_log_memory, [], all_tests()},
     {ra_log_file, [], [
                        init_close_init,
                        write_recover_then_overwrite,
                        write_overwrite_then_recover,
                        snapshot
                        | all_tests()]}
    ].

init_per_group(ra_log_memory, Config) ->
    InitFun = fun (_) -> ra_log:init(ra_log_memory, #{}) end,
    [{init_fun, InitFun} | Config];
init_per_group(ra_log_file, Config) ->
    PrivDir = ?config(priv_dir, Config),
    _ = application:load(ra),
    ok = application:set_env(ra, data_dir, PrivDir),
    application:ensure_all_started(ra),
    InitFun = fun (TestCase) ->
                      try
                          register(TestCase, self())
                      catch
                          _:_ -> ok
                      end,
                      ra_log:init(ra_log_file, #{data_dir => PrivDir,
                                                 id => TestCase})
              end,
    [{init_fun, InitFun} | Config].

end_per_group(_, Config) ->
    _ = application:stop(ra),
    Config.

init_per_testcase(TestCase, Config) ->
    Fun = ?config(init_fun, Config),
    Log = Fun(TestCase),
    [{ra_log, Log} | Config].

fetch_when_empty(Config) ->
    Log0 = ?config(ra_log, Config),
    {{0, 0, undefined}, Log1} = ra_log:fetch(0, Log0),
    {0, _} = ra_log:fetch_term(0, Log1),
    ok.

fetch_not_found(Config) ->
    Log0 = ?config(ra_log, Config),
    {undefined, Log} = ra_log:fetch(99, Log0),
    {undefined, _} = ra_log:fetch_term(99, Log),
    ok.

append_then_fetch(Config) ->
    Log0 = ?config(ra_log, Config),
    Term = 1,
    Idx = ra_log:next_index(Log0),
    Entry = {Idx, Term, "entry"},
    Log1 = ra_log:append_sync(Entry, Log0),
    {{Idx, Term, "entry"}, Log} = ra_log:fetch(Idx, Log1),
    {Idx, Term} = ra_log:last_written(Log),
    {Term, _} = ra_log:fetch_term(Idx, Log),
    ok.

write_then_fetch(Config) ->
    Log0 = ?config(ra_log, Config),
    Term = 1,
    Idx = ra_log:next_index(Log0),
    LastIdx = Idx + 1,
    Entries = [{Idx, Term, "entry"}, {Idx+1, Term, "entry2"}],
    Log1 = ra_log:write_sync(Entries, Log0),
    {{Idx, Term, "entry"}, Log2} = ra_log:fetch(Idx, Log1),
    {{LastIdx, Term, "entry2"}, Log} = ra_log:fetch(Idx+1, Log2),
    {LastIdx, Term} = ra_log:last_written(Log),
    {Term, _} = ra_log:fetch_term(Idx, Log),
    ok.

append_then_fetch_no_wait(Config) ->
    Log0 = ?config(ra_log, Config),
    Term = 1,
    Idx = ra_log:next_index(Log0),
    Entry = {Idx, Term, "entry"},
    Log1 = case ra_log:append(Entry, Log0) of
               {written, L} -> L;
               {queued, L} ->
                   % check last written hasn't been incremented
                   {0, 0} = ra_log:last_written(L),
                   L
           end,
    % log entry should be immediately visible to allow
    % leaders to send append entries for entries not yet
    % flushed
    {{Idx, Term, "entry"}, Log2} = ra_log:fetch(Idx, Log1),
    {Term, Log} = ra_log:fetch_term(Idx, Log2),
    % if we get async written notification check that handling that
    % results in the last written being updated
    receive
        {log_event, {written, _} = Evt} ->
            Log1 = ra_log:handle_event(Evt, Log),
            {Idx, Term} = ra_log:last_written(Log1)
    after 0 ->
              ok
    end,

    ok.


write_then_overwrite(Config) ->
    Log0 = ?config(ra_log, Config),
    Term = 1,
    Idx = ra_log:next_index(Log0),
    Log1 = write_two(Idx, Term, Log0),
    % overwrite Idx
    Entry2 = {Idx, Term, "entry0_2"},
    Log2 = ra_log:write_sync([Entry2], Log1),
    {{Idx, Term, "entry0_2"}, Log} = ra_log:fetch(Idx, Log2),
    ExpectedNextIndex = Idx + 1,
    % ensure last index is updated after overwrite
    ExpectedNextIndex = ra_log:next_index(Log),
    ok.

write_recover_then_overwrite(Config) ->
    Log0 = ?config(ra_log, Config),
    InitFun = ?config(init_fun, Config),
    Term = 1,
    Idx = ra_log:next_index(Log0),
    Log1 = write_two(Idx, Term, Log0),
    ok = ra_log:close(Log1),
    Log2 = InitFun(write_recover_then_overwrite),
    % overwrite Idx
    Entry2 = {Idx, Term, "entry0_2"},
    Log3 = ra_log:write_sync([Entry2], Log2),
    {{Idx, Term, "entry0_2"}, Log} = ra_log:fetch(Idx, Log3),
    ExpectedNextIndex = Idx+1,
    % ensure last index is updated after overwrite
    ExpectedNextIndex = ra_log:next_index(Log),
    % ensure previous indices aren't accessible
    {undefined, _} = ra_log:fetch(Idx+1, Log),
    ok.

write_overwrite_then_recover(Config) ->
    Log0 = ?config(ra_log, Config),
    InitFun = ?config(init_fun, Config),
    Term = 1,
    Idx = ra_log:next_index(Log0),
    Log1 = write_two(Idx, Term, Log0),
    % overwrite Idx
    Entry2 = {Idx, Term, "entry0_2"},
    Log2 = ra_log:write_sync([Entry2], Log1),
    % close log
    ok = ra_log:close(Log2),
    % recover
    Log3 = InitFun(write_overwrite_then_recover),
    {{Idx, Term, "entry0_2"}, Log} = ra_log:fetch(Idx, Log3),
    ExpectedNextIndex = Idx+1,
    % ensure last index is updated after overwrite
    ExpectedNextIndex = ra_log:next_index(Log),
    % ensure previous indices aren't accessible
    {undefined, _} = ra_log:fetch(Idx+1, Log),
    ok.

append_two(Idx, Term, Log0) ->
    Entry0 = {Idx, Term, "entry0"},
    Log1 = ra_log:append_sync(Entry0, Log0),
    Entry1 = {ra_log:next_index(Log1), Term, "entry1"},
    Log2 = ra_log:append_sync(Entry1, Log1),
    Log2.

write_two(Idx, Term, Log0) ->
    Entry0 = {Idx, Term, "entry0"},
    Entry1 = {Idx+1, Term, "entry1"},
    ra_log:write_sync([Entry0, Entry1], Log0).

append_integrity_error(Config) ->
    % allow "missing entries" but do not allow overwrites
    % unless overwrite flag is set
    Log0 = ?config(ra_log, Config),
    Term = 1,
    Next = ra_log:next_index(Log0),
    % this is ok even though entries are missing
    Log1 = ra_log:append_sync({Next, Term, "NextIndex"}, Log0),
    % going backwards should fail with integrity error
    Entry = {Next-1, Term, "NextIndex-1"},
    ?assertExit(integrity_error, ra_log:append(Entry, Log1)),
    _Log = ra_log:write_sync([Entry], Log1),
    ok.

-define(IDX(T), {T, _, _}).

take(Config) ->
    Log0 = ?config(ra_log, Config),
    Term = 1,
    Idx = ra_log:next_index(Log0),
    LastIdx = Idx + 9,
    Log1 = lists:foldl(fun (I, L0) ->
                               Entry = {I, Term, "entry" ++ integer_to_list(I)},
                               ra_log:append_sync(Entry, L0)
                       end, Log0, lists:seq(Idx, LastIdx)),
    % wont work for memory
    {[?IDX(1)], Log2} = ra_log:take(1, 1, Log1),
    {[?IDX(1), ?IDX(2)], Log3} = ra_log:take(1, 2, Log2),
    % partly out of range
    {[?IDX(9), ?IDX(10)], Log4} = ra_log:take(9, 3, Log3),
    % completely out of range
    {[], Log5} = ra_log:take(11, 3, Log4),
    % take all
    {Taken, _} = ra_log:take(1, 10, Log5),
    ?assertEqual(10, length(Taken)),
    ok.


last(Config) ->
    Log0 = ?config(ra_log, Config),
    Term = 1,
    Idx = ra_log:next_index(Log0),
    Entry = {Idx, Term, "entry"},
    Log = ra_log:append_sync(Entry, Log0),
    {Idx, Term} = ra_log:last_index_term(Log),
    ok.

meta(Config) ->
    Log0 = ?config(ra_log, Config),
    {ok, Log} = ra_log:write_meta(current_term, 87, Log0),
    87 = ra_log:read_meta(current_term, Log),
    undefined = ra_log:read_meta(missing_key, Log),
    ok.

snapshot(Config) ->
    % tests explicit externally triggered snaphostting
    Log0 = ?config(ra_log, Config),
    % no snapshot yet
    undefined = ra_log:read_snapshot(Log0),
    Log1 = append_in(1, "entry1", Log0),
    Log2 = append_in(1, "entry2", Log1),
    {LastIdx, LastTerm} = ra_log:last_index_term(Log2),
    Cluster = #{node1 => #{}},
    Snapshot = {LastIdx, LastTerm, Cluster, "entry1+2"},
    Log3 = ra_log:write_snapshot(Snapshot, Log2),
    Log4 = receive
               {ra_log_event, Evt} ->
                   ra_log:handle_event(Evt, Log3)
           after 2000 ->
                 throw(ra_log_event_timeout)
           end,

    % ensure entries prior to snapshot are no longer there
    {undefined, Log5} = ra_log:fetch(LastIdx, Log4),
    {undefined, _} = ra_log:fetch_term(LastIdx, Log5),
    {undefined, Log} = ra_log:fetch(LastIdx-1, Log5),
    {undefined, _} = ra_log:fetch_term(LastIdx-1, Log5),
    % falls back to snapshot idxterm
    {LastIdx, LastTerm}  = ra_log:last_index_term(Log),
    Snapshot = ra_log:read_snapshot(Log),
    % initialise another log
    LogB = ra_log:init(ra_log_file, #{data_dir => ?config(priv_dir, Config),
                                      id => snapshot}),
    {LastIdx, LastTerm}  = ra_log:last_index_term(LogB),
    {LastTerm, _} = ra_log:fetch_term(LastIdx, LogB),
    Snapshot = ra_log:read_snapshot(LogB),
    ok.


% persistent ra_log implementations only
init_close_init(Config) ->
    InitFun = ?config(init_fun, Config),
    Log0 = ?config(ra_log, Config),
    Log1 = append_in(1, "entry1", Log0),
    Log2 = append_in(2, "entry2", Log1),
    {ok, Log} = ra_log:write_meta(current_term, 2, Log2),
    ok = ra_log:close(Log),
    LogA = InitFun(init_close_init),
    {2, 2} = ra_log:last_index_term(LogA),
    {{2, 2, "entry2"}, LogA1} = ra_log:fetch(2, LogA),
    {{1, 1, "entry1"}, LogA2} = ra_log:fetch(1, LogA1),
    2 = ra_log:read_meta(current_term, LogA),
    % ensure we can append after recovery
    LogB = append_in(2, "entry3", LogA2),
    {{1, 1, "entry1"}, LogB1} = ra_log:fetch(1, LogB),
    {{3, 2, "entry3"}, LogB2} = ra_log:fetch(3, LogB1),
    {{2, 2, "entry2"}, _} = ra_log:fetch(2, LogB2),
    ok.

append_in(Term, Data, Log0) ->
    Idx = ra_log:next_index(Log0),
    Entry = {Idx, Term, Data},
    ra_log:append_sync(Entry, Log0).
