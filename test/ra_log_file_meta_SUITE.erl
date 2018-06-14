-module(ra_log_file_meta_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

%% common ra_log tests to ensure behaviour is equivalent across
%% ra_log backends

all() ->
    [
     {group, tests}
    ].

all_tests() ->
    [
     init,
     roundtrip,
     empty
    ].

groups() ->
    [
     {tests, [], all_tests()}
    ].

init_per_group(_, Config) ->
    Config.

end_per_group(_, Config) ->
    Config.

init_per_testcase(TestCase, Config) ->
    Dir = ?config(priv_dir, Config),
    File = filename:join(Dir, atom_to_list(TestCase)),
    _ = ets:new(ra_open_file_metrics, [named_table, public, {write_concurrency, true}]),
    _ = ets:new(ra_io_metrics, [named_table, public, {write_concurrency, true}]),
    ra_file_handle:start_link(),
    [{file, File} |  Config].

init(Config) ->
    Kv = ra_log_file_meta:init(?config(file, Config)),
    0 = ra_log_file_meta:fetch(last_applied, Kv),
    0 = ra_log_file_meta:fetch(current_term, Kv),
    undefined = ra_log_file_meta:fetch(voted_for, Kv),
    ok.

roundtrip(Config) ->
    Kv = ra_log_file_meta:init(?config(file, Config)),
    ok = ra_log_file_meta:store(last_applied, 199, Kv),
    199 = ra_log_file_meta:fetch(last_applied, Kv),
    ok = ra_log_file_meta:store(current_term, 5, Kv),
    5 = ra_log_file_meta:fetch(current_term, Kv),
    ok = ra_log_file_meta:store(voted_for, 'cøstard', Kv),
    'cøstard' = ra_log_file_meta:fetch(voted_for, Kv),
    ok = ra_log_file_meta:store(voted_for, undefined, Kv),
    undefined = ra_log_file_meta:fetch(voted_for, Kv),
    undefined = ra_log_file_meta:fetch(voted_for, Kv),
    ok = ra_log_file_meta:store(voted_for, {custard, cream}, Kv),
    {custard, cream} = ra_log_file_meta:fetch(voted_for, Kv),
    ok = ra_log_file_meta:close(Kv),
    % reooen
    Kv2 = ra_log_file_meta:init(?config(file, Config)),
    199 = ra_log_file_meta:fetch(last_applied, Kv2),
    5 = ra_log_file_meta:fetch(current_term, Kv2),
    {custard, cream} = ra_log_file_meta:fetch(voted_for, Kv2),
    ok = ra_log_file_meta:close(Kv2),
    ok.

empty(Config) ->
    Kv = ra_log_file_meta:init(?config(file, Config)),
    ok = ra_log_file_meta:store(voted_for, '', Kv),
    '' = ra_log_file_meta:fetch(voted_for, Kv),
    ok.


