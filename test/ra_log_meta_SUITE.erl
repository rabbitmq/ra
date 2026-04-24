%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2017-2025 Broadcom. All Rights Reserved. The term Broadcom refers to Broadcom Inc. and/or its subsidiaries.
%%
-module(ra_log_meta_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-define(SYS, default).

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
    {ok, _} = ra:start_in(PrivDir),
    Config.

end_per_group(_, Config) ->
    application:stop(ra),
    Config.

init_per_testcase(TestCase, Config) ->
    %% Convert test case name (atom) to binary for use as ra_uid
    [{key, atom_to_binary(TestCase, utf8)} | Config].

end_per_testcase(_, Config) ->
    Config.

roundtrip(Config) ->
    Id = ?config(key, Config),
    ok = ra_log_meta:store_sync(ra_log_meta, Id, last_applied, 199),
    199 = ra_log_meta:fetch(ra_log_meta, Id, last_applied),
    ok = ra_log_meta:store_sync(ra_log_meta, Id, current_term, 5),
    5 = ra_log_meta:fetch(ra_log_meta, Id, current_term),
    ok = ra_log_meta:store(ra_log_meta, Id, voted_for, 'cream'),
    ok = ra_log_meta:store_sync(ra_log_meta, Id, voted_for, 'cøstard'),
    'cøstard' = ra_log_meta:fetch(ra_log_meta, Id, voted_for),
    ok = ra_log_meta:store_sync(ra_log_meta, Id, voted_for, undefined),
    undefined = ra_log_meta:fetch(ra_log_meta, Id, voted_for),
    ok = ra_log_meta:store_sync(ra_log_meta, Id, voted_for, {custard, cream}),
    {custard, cream} = ra_log_meta:fetch(ra_log_meta, Id, voted_for),
    %% lose and re-open
    199 = ra_log_meta:fetch(ra_log_meta, Id, last_applied),
    proc_lib:stop(whereis(ra_log_meta), shutdown, infinity),
    timer:sleep(100),
    % give it some time to restart and be ready
    ok = ra_log_meta:await(ra_log_meta),
    5 = ra_log_meta:fetch(ra_log_meta, Id, current_term),
    {custard, cream} = ra_log_meta:fetch(ra_log_meta, Id, voted_for),
    199 = ra_log_meta:fetch(ra_log_meta, Id, last_applied),
    ok.

delete(Config) ->
    Id = ?config(key, Config),
    ok = ra_log_meta:store_sync(ra_log_meta, Id, last_applied, 199),
    Oth = <<"some_other_id">>,
    ok = ra_log_meta:store_sync(ra_log_meta, Oth, last_applied, 1),
    ok = ra_log_meta:delete(ra_log_meta, Oth), %% async
    ok = ra_log_meta:delete_sync(ra_log_meta, Id), %% async
    %% store some other id just to make sure the delete is processed
    undefined = ra_log_meta:fetch(ra_log_meta, Oth, last_applied),
    undefined = ra_log_meta:fetch(ra_log_meta, Id, last_applied),
    ok.

migrate_from_dets(Config) ->
    Id = ?config(key, Config),
    PrivDir = ?config(priv_dir, Config),
    
    %% First, stop ra so we can create a hand-made DETS file
    application:stop(ra),
    timer:sleep(200),
    
    %% Create a temporary DETS file with known data
    MetaDetsPath = filename:join(PrivDir, "meta.dets"),
    {ok, DetsTable} = dets:open_file(test_dets_migration, [{file, MetaDetsPath}]),
    dets:insert(DetsTable, {Id, 42, 'node1@host', 100}),
    dets:close(DetsTable),
    
    %% Restart ra - should migrate DETS to shu
    {ok, _} = ra:start_in(PrivDir),
    timer:sleep(500),
    
    %% Verify migrated data is accessible via ETS
    %% Note: we only test the simple values that round-trip well
    42 = ra_log_meta:fetch(ra_log_meta, Id, current_term),
    100 = ra_log_meta:fetch(ra_log_meta, Id, last_applied),
    'node1@host' = ra_log_meta:fetch(ra_log_meta, Id, voted_for),
    
    %% Verify DETS file was renamed to .migrated
    true = filelib:is_file(MetaDetsPath ++ ".migrated"),
    false = filelib:is_file(MetaDetsPath),
    
    ok.

