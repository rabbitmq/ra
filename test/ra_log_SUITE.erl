-module(ra_log_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

%% common ra_log tests to ensure behaviour is equivalent across
%% ra_log backends

-define(SYS, default).

all() ->
    [
     {group, tests}
    ].

all_tests() ->
    [
     fetch_when_empty,
     fetch_not_found,
     append_then_fetch,
     write_then_fetch,
     write_sparse_then_fetch,
     append_then_fetch_no_wait,
     write_then_overwrite,
     append_integrity_error,
     take,
     take_cache,
     last
    ].

groups() ->
    [
     {tests, [], [
                  init_close_init,
                  write_recover_then_overwrite,
                  write_overwrite_then_recover
                  | all_tests()]}
    ].

init_per_group(tests, Config) ->
    ra_env:configure_logger(logger),
    PrivDir = ?config(priv_dir, Config),
    {ok, _} = ra:start_in(PrivDir),
    SysCfg = ra_system:fetch(default),
    InitFun = fun (TestCase) ->
                      UId = atom_to_binary(TestCase, utf8),
                      ra_directory:register_name(?SYS, UId, self(), undefined,
                                                 TestCase, TestCase),
                      ra_log:init(#{uid => UId,
                                    system_config => SysCfg})
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
    {ok, Log1} = ra_log:write_sync(Entries, Log0),
    {{Idx, Term, "entry"}, Log2} = ra_log:fetch(Idx, Log1),
    {{LastIdx, Term, "entry2"}, Log} = ra_log:fetch(Idx+1, Log2),
    {LastIdx, Term} = ra_log:last_written(Log),
    {Term, _} = ra_log:fetch_term(Idx, Log),
    ok.

write_sparse_then_fetch(Config) ->
    Log0 = ?config(ra_log, Config),
    Term = 1,
    Idx = ra_log:next_index(Log0),
    Idx5 = Idx + 5,
    Entry1 = {Idx, Term, "entry"},
    %% sparse
    Entry2 = {Idx5, Term, "entry+5"},

    {LastIdx0, _} = ra_log:last_index_term(Log0),
    Log1 = ra_log:write_sparse(Entry1, LastIdx0, Log0),
    {{Idx, Term, "entry"}, Log2} = ra_log:fetch(Idx, Log1),
    Log3 = ra_log:write_sparse(Entry2, Idx, Log2),
    Log = await_written_idx(Idx5, Term, Log3),
    {Idx5, Term} = ra_log:last_written(Log),
    {Idx5, _} = ra_log:last_index_term(Log),
    % debugger:start(),
    % int:i(ra_log),
    % int:break(ra_log, 524),
    {{Idx5, Term, "entry+5"}, _Log} = ra_log:fetch(Idx5, Log),
    ok.

append_then_fetch_no_wait(Config) ->
    Log0 = ?config(ra_log, Config),
    Term = 1,
    Idx = ra_log:next_index(Log0),
    Entry = {Idx, Term, "entry"},
    ?assertMatch(#{num_pending := 0}, ra_log:overview(Log0)),
    Log1 = ra_log:append(Entry, Log0),
    ?assertMatch(#{num_pending := 1}, ra_log:overview(Log1)),
    % check last written hasn't been incremented
    {0, 0} = ra_log:last_written(Log1),
    % log entry should be immediately visible to allow
    % leaders to send append entries for entries not yet
    % flushed
    {{Idx, Term, "entry"}, Log2} = ra_log:fetch(Idx, Log1),
    {Term, Log3} = ra_log:fetch_term(Idx, Log2),
    % if we get async written notification check that handling that
    % results in the last written being updated
    receive
        {ra_log_event, {written, _, _} = Evt} ->
            ct:pal("written ~p", [Evt]),
            {Log, _} = ra_log:handle_event(Evt, Log3),
            {Idx, Term} = ra_log:last_written(Log),
            ?assertMatch(#{num_pending := 0}, ra_log:overview(Log))
    after 1000 ->
              flush(),
              ct:pal("fail written event not received")
    end,
    ok.


write_then_overwrite(Config) ->
    Log0 = ?config(ra_log, Config),
    Term = 1,
    Idx = ra_log:next_index(Log0),
    Log1 = write_two(Idx, Term, Log0),
    % overwrite Idx
    Term2 = Term+1,
    Entry2 = {Idx, Term2, "entry0_2"},
    {ok, Log2} = ra_log:write_sync([Entry2], Log1),
    {{Idx, Term2, "entry0_2"}, Log} = ra_log:fetch(Idx, Log2),
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
    Log2 = InitFun(?FUNCTION_NAME),
    % overwrite Idx
    Entry2 = {Idx, Term, "entry0_2"},
    {ok, Log3} = ra_log:write_sync([Entry2], Log2),
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
    {ok, Log2} = ra_log:write_sync([Entry2], Log1),
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
    {ok, Log} = ra_log:write_sync([Entry0, Entry1], Log0),
    Log.

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
    ?assertExit({integrity_error, _}, ra_log:append(Entry, Log1)),
    {ok, _Log} = ra_log:write_sync([Entry], Log1),
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
    % won't work for memory
    {[?IDX(1)], Log2} = ra_log_take(1, 1, Log1),
    {[?IDX(1), ?IDX(2)], Log3} = ra_log_take(1, 2, Log2),
    % partly out of range
    {[?IDX(9), ?IDX(10)], Log4} = ra_log_take(9, 11, Log3),
    % completely out of range
    {[], Log5} = ra_log_take(11, 14, Log4),
    % take all
    {Taken, _} = ra_log_take(1, 10, Log5),
    ?assertEqual(10, length(Taken)),
    %% take 0
    {[], _} = ra_log_take(5, 4, Log5),
    ok.

take_cache(Config) ->
    Log0 = ?config(ra_log, Config),
    Term = 1,
    Idx = ra_log:next_index(Log0),
    Log1 = ra_log:append_sync({Idx, Term, <<"one">>}, Log0),
    Idx2 = Idx +1,
    Log = ra_log:append({Idx2, Term, <<"two">>}, Log1),
    {[?IDX(Idx), ?IDX(Idx2)], _Log2} = ra_log_take(1, 2, Log),
    ok.

last(Config) ->
    Log0 = ?config(ra_log, Config),
    Term = 1,
    Idx = ra_log:next_index(Log0),
    Entry = {Idx, Term, "entry"},
    Log = ra_log:append_sync(Entry, Log0),
    {Idx, Term} = ra_log:last_index_term(Log),
    ok.

% persistent ra_log implementations only
init_close_init(Config) ->
    InitFun = ?config(init_fun, Config),
    Log0 = ?config(ra_log, Config),
    Log1 = append_in(1, "entry1", Log0),
    Log2 = append_in(2, "entry2", Log1),
    ok = ra_log_meta:store_sync(ra_log_meta, ?config(uid, Config), current_term, 2),
    ok = ra_log:close(Log2),
    LogA = InitFun(init_close_init),
    {2, 2} = ra_log:last_index_term(LogA),
    {{2, 2, "entry2"}, LogA1} = ra_log:fetch(2, LogA),
    {{1, 1, "entry1"}, LogA2} = ra_log:fetch(1, LogA1),
    2 = ra_log_meta:fetch(ra_log_meta, ?config(uid, Config), current_term),
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

ra_log_take(From, To, Log0) ->
    {Acc, Log} = ra_log:fold(From, To, fun (E, Acc) -> [E | Acc] end, [], Log0),
    {lists:reverse(Acc), Log}.

flush() ->
    receive
        Any ->
            ct:pal("flush ~p", [Any]),
            flush()
    after 0 ->
              ok
    end.

await_written_idx(Idx, Term, Log0) ->
    receive
        {ra_log_event, {written, Term, _Seq} = Evt} ->
            ct:pal("written ~p", [Evt]),
            {Log, _} = ra_log:handle_event(Evt, Log0),
            case ra_log:last_written(Log) of
                {Idx, Term} ->
                    Log;
                _ ->
                    await_written_idx(Idx, Term, Log)
            end
    after 1000_000 ->
              flush(),
              throw(ra_log_append_timeout)
    end.
