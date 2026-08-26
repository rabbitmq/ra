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
     delete,
     trigger_compaction,
     migrate_from_dets,
     migrate_failure_preserves_dets,
     atom_table_full_is_controlled
    ].

groups() ->
    [
     {tests, [], all_tests()}
    ].

init_per_group(_, Config) ->
    Config.

end_per_group(_, Config) ->
    Config.

%% Each test case runs against its own ra system in its own data directory so
%% that a restart (or crash) in one test cannot leave shared state that masks a
%% regression in another.
init_per_testcase(TestCase, Config) ->
    PrivDir = ?config(priv_dir, Config),
    DataDir = filename:join(PrivDir, atom_to_list(TestCase)),
    Key = atom_to_binary(TestCase, utf8),
    case TestCase of
        trigger_compaction ->
            %% small WAL so the compaction path is reached with a modest
            %% number of writes
            ok = application:set_env(ra, ra_log_meta_wal_size, 64 * 1024);
        _ ->
            ok = application:unset_env(ra, ra_log_meta_wal_size)
    end,
    case TestCase of
        atom_table_full_is_controlled ->
            %% shrink the atom table so a handful of distinct node names
            %% exhausts it quickly
            ok = application:set_env(ra, ra_log_meta_atom_slots, 8);
        _ ->
            ok = application:unset_env(ra, ra_log_meta_atom_slots)
    end,
    case TestCase of
        migrate_from_dets ->
            %% two voted_for forms: a legacy bare atom, and the {Name, Node}
            %% tuple that production ra actually persisted
            ok = write_dets(DataDir,
                            [{Key, 42, 'node1@host', 100},
                             {<<"migrate_tuple_uid">>, 7,
                              {my_server, 'rabbit@host'}, 55}]),
            {ok, _} = ra:start_in(DataDir);
        migrate_failure_preserves_dets ->
            %% a uid larger than shu's 255-byte key limit makes the migration
            %% write fail; ra is started by the test itself
            BigKey = binary:copy(<<"x">>, 300),
            ok = write_dets(DataDir, {BigKey, 5, undefined, 10});
        _ ->
            {ok, _} = ra:start_in(DataDir)
    end,
    [{key, Key}, {data_dir, DataDir} | Config].

end_per_testcase(_, Config) ->
    catch application:stop(ra),
    application:unset_env(ra, ra_log_meta_wal_size),
    application:unset_env(ra, ra_log_meta_atom_slots),
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
    restart_meta(),
    5 = ra_log_meta:fetch(ra_log_meta, Id, current_term),
    {custard, cream} = ra_log_meta:fetch(ra_log_meta, Id, voted_for),
    199 = ra_log_meta:fetch(ra_log_meta, Id, last_applied),
    %% advancing the term clears voted_for (rabbitmq/ra#111); the cleared
    %% value must be persisted and survive a restart (regression guard)
    ok = ra_log_meta:store_sync(ra_log_meta, Id, current_term, 6),
    undefined = ra_log_meta:fetch(ra_log_meta, Id, voted_for),
    restart_meta(),
    6 = ra_log_meta:fetch(ra_log_meta, Id, current_term),
    undefined = ra_log_meta:fetch(ra_log_meta, Id, voted_for),
    199 = ra_log_meta:fetch(ra_log_meta, Id, last_applied),
    ok.

delete(Config) ->
    Id = ?config(key, Config),
    ok = ra_log_meta:store_sync(ra_log_meta, Id, last_applied, 199),
    Oth = <<"some_other_id">>,
    ok = ra_log_meta:store_sync(ra_log_meta, Oth, last_applied, 1),
    ok = ra_log_meta:delete(ra_log_meta, Oth), %% async
    ok = ra_log_meta:delete_sync(ra_log_meta, Id), %% sync
    undefined = ra_log_meta:fetch(ra_log_meta, Oth, last_applied),
    undefined = ra_log_meta:fetch(ra_log_meta, Id, last_applied),
    %% the delete must be persisted to shu: after a restart the records must
    %% not resurrect from the shu store (regression guard)
    restart_meta(),
    undefined = ra_log_meta:fetch(ra_log_meta, Oth, last_applied),
    undefined = ra_log_meta:fetch(ra_log_meta, Id, last_applied),
    ok.

trigger_compaction(Config) ->
    Id = ?config(key, Config),
    %% last_applied is the only high-frequency (WAL-backed) field. Each
    %% store_sync is processed as its own batch, so a stream of increasing
    %% values appends one WAL entry per call and grows the WAL until it fills
    %% and triggers a background compaction. store within a single batch is
    %% coalesced, so store_sync (one batch per call) is required to actually
    %% fill the WAL. With the small WAL configured in init_per_testcase this
    %% cycles over during the loop.
    Pid = whereis(ra_log_meta),
    ?assert(is_pid(Pid)),
    N = 6000,
    [ok = ra_log_meta:store_sync(ra_log_meta, Id, last_applied, I)
     || I <- lists:seq(1, N)],
    %% The process must have survived the wal_full / compaction cycles (a
    %% crash would restart it under a new pid) and the final value must read
    %% back correctly, including after a restart (recovered from the compacted
    %% record area plus the tail of the WAL).
    Pid = whereis(ra_log_meta),
    N = ra_log_meta:fetch(ra_log_meta, Id, last_applied),
    restart_meta(),
    N = ra_log_meta:fetch(ra_log_meta, Id, last_applied),
    ok.

migrate_from_dets(Config) ->
    Id = ?config(key, Config),
    DataDir = ?config(data_dir, Config),
    %% ra was started in init_per_testcase with a pre-populated meta.dets, so
    %% the migration has already run.
    ok = ra_log_meta:await(ra_log_meta),
    42 = ra_log_meta:fetch(ra_log_meta, Id, current_term),
    100 = ra_log_meta:fetch(ra_log_meta, Id, last_applied),
    %% legacy bare-atom voted_for must round-trip back to the same bare atom
    'node1@host' = ra_log_meta:fetch(ra_log_meta, Id, voted_for),
    %% the {Name, Node} tuple form (what production ra persisted) must also
    %% round-trip through migration
    Id2 = <<"migrate_tuple_uid">>,
    7 = ra_log_meta:fetch(ra_log_meta, Id2, current_term),
    55 = ra_log_meta:fetch(ra_log_meta, Id2, last_applied),
    {my_server, 'rabbit@host'} = ra_log_meta:fetch(ra_log_meta, Id2, voted_for),
    %% the DETS file is renamed only after a successful migration
    MetaDets = meta_dets_path(DataDir),
    true = filelib:is_file(MetaDets ++ ".migrated"),
    false = filelib:is_file(MetaDets),
    ok.

migrate_failure_preserves_dets(Config) ->
    DataDir = ?config(data_dir, Config),
    MetaDets = meta_dets_path(DataDir),
    true = filelib:is_file(MetaDets),
    %% starting ra runs the migration, which fails on the oversized key. The
    %% failure must not rename (and thereby lose) the source DETS file.
    _ = (catch ra:start_in(DataDir)),
    timer:sleep(300),
    true = filelib:is_file(MetaDets),
    false = filelib:is_file(MetaDets ++ ".migrated"),
    ok.

atom_table_full_is_controlled(Config) ->
    Id = ?config(key, Config),
    Pid = whereis(ra_log_meta),
    ?assert(is_pid(Pid)),
    MRef = erlang:monitor(process, Pid),
    %% Writing voted_for values with many distinct node atoms exhausts shu's
    %% fixed 256-slot atom table. The resulting error must surface as a
    %% controlled exit (so gen_batch_server terminates cleanly and the log
    %% subtree restarts) rather than leaking as an uncaught throw. The writes
    %% run in a separate process so the failing store_sync call does not take
    %% down the test process.
    _ = spawn(fun () ->
                      catch [ra_log_meta:store_sync(
                               ra_log_meta, Id, voted_for,
                               {<<"n">>,
                                list_to_atom("atomfull_" ++
                                             integer_to_list(I))})
                             || I <- lists:seq(1, 400)]
              end),
    receive
        {'DOWN', MRef, process, Pid, Reason} ->
            ?assertMatch({shu_write_failed, {error, atom_table_full}}, Reason)
    after 10000 ->
              ct:fail(meta_process_did_not_exit)
    end,
    ok.

%%% helpers

%% Restart the ra_log_meta process (as its supervisor would on a crash) and
%% wait for it to finish re-initialising from the shu store. Stopping it
%% triggers a one_for_all restart of the whole log subtree, which on a loaded
%% CI can take a while, so poll for the new process rather than sleeping a
%% fixed amount.
restart_meta() ->
    OldPid = whereis(ra_log_meta),
    proc_lib:stop(OldPid, shutdown, infinity),
    await_meta_restarted(OldPid, 200).

await_meta_restarted(_OldPid, 0) ->
    ct:fail(ra_log_meta_did_not_restart);
await_meta_restarted(OldPid, N) ->
    case whereis(ra_log_meta) of
        Pid when is_pid(Pid), Pid =/= OldPid ->
            %% the new process is registered; make sure it has finished init
            try ra_log_meta:await(ra_log_meta) of
                ok -> ok
            catch
                _:_ ->
                    timer:sleep(25),
                    await_meta_restarted(OldPid, N - 1)
            end;
        _ ->
            timer:sleep(25),
            await_meta_restarted(OldPid, N - 1)
    end.

%% ra stores metadata under <data_dir>/<node>/meta.{shu,dets}.
meta_dir(DataDir) ->
    filename:join(DataDir, atom_to_list(node())).

meta_dets_path(DataDir) ->
    filename:join(meta_dir(DataDir), "meta.dets").

write_dets(DataDir, Row) ->
    MetaDir = meta_dir(DataDir),
    ok = filelib:ensure_dir(filename:join(MetaDir, "ignore")),
    MetaDets = filename:join(MetaDir, "meta.dets"),
    {ok, T} = dets:open_file(ra_log_meta_SUITE_dets, [{file, MetaDets}]),
    ok = dets:insert(T, Row),
    ok = dets:close(T),
    ok.
