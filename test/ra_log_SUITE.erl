-module(ra_log_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

%% common ra_log tests to ensure behaviour is equivalent across
%% ra_log backends

all() ->
    [
     {group, ra_test_log}
    ].

all_tests() ->
    [
     fetch_when_empty,
     fetch_not_found,
     append_then_fetch,
     append_then_overwrite,
     append_integrity_error,
     take,
     last,
     meta
    ].

groups() ->
    [{ra_test_log, [], all_tests()}].

init_per_group(ra_test_log, Config) ->
    Log = ra_log:init(ra_test_log, []),
    [{ra_log, Log} | Config].

end_per_group(_, Config) ->
    Config.

fetch_when_empty(Config) ->
    Log = ?config(ra_log, Config),
    {0, 0, undefined} = ra_log:fetch(0, Log),
    ok.

fetch_not_found(Config) ->
    Log = ?config(ra_log, Config),
    undefined = ra_log:fetch(99, Log),
    ok.

append_then_fetch(Config) ->
    Log0 = ?config(ra_log, Config),
    Term = 1,
    Idx = ra_log:next_index(Log0),
    Entry = {Idx, Term, "entry"},
    {ok, Log} = ra_log:append(Entry, false, Log0),
    {Idx, Term, "entry"} = ra_log:fetch(Idx, Log),
    ok.

append_then_overwrite(Config) ->
    Log0 = ?config(ra_log, Config),
    Term = 1,
    Idx = ra_log:next_index(Log0),
    Entry = {Idx, Term, "entry"},
    {ok, Log} = ra_log:append(Entry, false, Log0),
    Entry2 = {Idx, Term, "entry2"},
    {ok, Log1} = ra_log:append(Entry2, true, Log),
    {Idx, Term, "entry2"} = ra_log:fetch(Idx, Log1),
    ok.

append_integrity_error(Config) ->
    Log0 = ?config(ra_log, Config),
    Term = 1,
    Idx = 99, % way out of range
    Entry = {Idx, Term, "entry"},
    {error, integrity_error} = ra_log:append(Entry, false, Log0),
    ok.

-define(IDX(T), {T, _, _}).

take(Config) ->
    Log0 = ?config(ra_log, Config),
    Term = 1,
    Idx = ra_log:next_index(Log0),
    Log = lists:foldl(fun (I, L0) ->
                        Entry = {I, Term, "entry" ++ integer_to_list(I)},
                        {ok, L} = ra_log:append(Entry, false, L0),
                        L
                      end, Log0, lists:seq(Idx, Idx + 9)),
    [?IDX(1)] = ra_log:take(1, 1, Log),
    [?IDX(1), ?IDX(2)] = ra_log:take(1, 2, Log),
    % partly out of range
    [?IDX(9), ?IDX(10)] = ra_log:take(9, 3, Log),
    % completely out of range
    [] = ra_log:take(11, 3, Log),
    % take all
    Taken = ra_log:take(1, 10, Log),
    ?assertEqual(10, length(Taken)),
    ok.

last(Config) ->
    Log0 = ?config(ra_log, Config),
    Term = 1,
    Idx = ra_log:next_index(Log0),
    Entry = {Idx, Term, "entry"},
    {ok, Log} = ra_log:append(Entry, false, Log0),
    {Idx, Term, "entry"} = ra_log:last(Log),
    ok.

meta(Config) ->
    Log0 = ?config(ra_log, Config),
    {ok, Log} = ra_log:write_meta(current_term, 87, Log0),
    87 = ra_log:read_meta(current_term, Log),
    undefined = ra_log:read_meta(missing_key, Log),
    ok.
