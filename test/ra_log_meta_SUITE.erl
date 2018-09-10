-module(ra_log_meta_SUITE).

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
     roundtrip,
     delete
    ].

groups() ->
    [
     {tests, [], all_tests()}
    ].

init_per_group(_, Config) ->
    PrivDir = ?config(priv_dir, Config),
    _ = application:load(ra),
    ok = application:set_env(ra, data_dir, PrivDir),
    application:ensure_all_started(ra),
    Config.

end_per_group(_, Config) ->
    Config.

init_per_testcase(TestCase, Config) ->
    [{key, TestCase} | Config].

end_per_testcase(_, Config) ->
    exit(whereis(ra_file_handle), normal),
    Config.

roundtrip(Config) ->
    Id = ?config(key, Config),
    ok = ra_log_meta:store_sync(Id, last_applied, 199),
    199 = ra_log_meta:fetch(Id, last_applied),
    ok = ra_log_meta:store_sync(Id, current_term, 5),
    5 = ra_log_meta:fetch(Id, current_term),
    ok = ra_log_meta:store(Id, voted_for, 'cream'),
    ok = ra_log_meta:store_sync(Id, voted_for, 'cøstard'),
    'cøstard' = ra_log_meta:fetch(Id, voted_for),
    ok = ra_log_meta:store_sync(Id, voted_for, undefined),
    undefined = ra_log_meta:fetch(Id, voted_for),
    ok = ra_log_meta:store_sync(Id, voted_for, {custard, cream}),
    {custard, cream} = ra_log_meta:fetch(Id, voted_for),
    %% lose and re-open
    proc_lib:stop(whereis(ra_log_meta), killed, infinity),
    timer:sleep(200),
    % give it some time to restart
    199 = ra_log_meta:fetch(Id, last_applied),
    5 = ra_log_meta:fetch(Id, current_term),
    {custard, cream} = ra_log_meta:fetch(Id, voted_for),
    ok.

delete(Config) ->
    Id = ?config(key, Config),
    ok = ra_log_meta:store_sync(Id, last_applied, 199),
    Oth = <<"some_other_id">>,
    ok = ra_log_meta:store_sync(Oth, last_applied, 1),
    ok = ra_log_meta:delete(Oth), %% async
    ok = ra_log_meta:delete_sync(Id), %% async
    %% store some other id just to make sure the delete is processed
    undefined = ra_log_meta:fetch(Oth, last_applied),
    undefined = ra_log_meta:fetch(Id, last_applied),
    ok.
