%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2017-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%
-module(ra_log_meta_SUITE).

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
    Config.

init_per_testcase(TestCase, Config) ->
    [{key, TestCase} | Config].

end_per_testcase(_, Config) ->
    exit(whereis(ra_file_handle), normal),
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
    proc_lib:stop(whereis(ra_log_meta), killed, infinity),
    timer:sleep(200),
    % give it some time to restart
    199 = ra_log_meta:fetch(ra_log_meta, Id, last_applied),
    5 = ra_log_meta:fetch(ra_log_meta, Id, current_term),
    {custard, cream} = ra_log_meta:fetch(ra_log_meta, Id, voted_for),
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
