%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2017-2025 Broadcom. All Rights Reserved. The term Broadcom refers to Broadcom Inc. and/or its subsidiaries.
%%
-module(ra_log_snap_store_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

all() ->
    [
     {group, tests}
    ].

all_tests() ->
    [
     put_lookup_read,
     put_semantics,
     epoch_change_replaces,
     reconcile_delete_release,
     recovery,
     recovery_torn_tail,
     recovery_skips_an_invalid_record,
     recovery_stops_at_an_unreadable_record_header,
     recovery_live_fun,
     batches_are_aligned,
     concurrent_puts_are_batched,
     roll_and_retire,
     cold_entries_survive_retire,
     restart_after_retire,
     write_error_fails_batch_and_is_not_visible,
     fsync_error_unacked_record_ignored_after_restart,
     cannot_create_file_recovers,
     unknown_call_gets_an_error,
     batch_failure_when_the_next_file_can_be_created,
     counters_count_what_happens,
     status_follows_failures,
     duplicate_put_in_a_failed_batch_is_not_acked,
     delete_is_ordered_before_later_puts,
     retire_skips_an_invalid_record_and_copies_the_rest,
     retire_never_deletes_a_file_that_is_still_referenced,
     retire_open_error_backs_off,
     unreadable_file_stops_the_store_starting,
     damaged_header_in_an_old_file_is_set_aside,
     torn_file_creation_is_deleted,
     stray_files_in_the_directory_are_ignored,
     reconcile_is_ordered_after_puts_in_flight,
     delete_is_ordered_after_puts_in_flight,
     migrate_out_creates_snapshot_directories,
     migrate_out_skips_newer_directories_and_dead_members,
     migrate_out_replaces_a_partial_directory,
     migrate_out_keeps_the_log_when_a_write_fails
    ].

groups() ->
    [
     {tests, [], all_tests()}
    ].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(_Group, Config) ->
    Config.

end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(TestCase, Config) ->
    Dir = filename:join([?config(priv_dir, Config), TestCase, "store"]),
    Name = list_to_atom("snap_store_" ++ atom_to_list(TestCase)),
    persistent_term:erase({?MODULE, fail}),
    persistent_term:erase({?MODULE, block_sync}),
    [{store_dir, Dir}, {store_name, Name} | Config].

end_per_testcase(_TestCase, Config) ->
    persistent_term:erase({?MODULE, block_sync}),
    catch ra_log_snap_store:stop(?config(store_name, Config)),
    persistent_term:erase({?MODULE, fail}),
    ok.

%%%===================================================================
%%% Test cases
%%%===================================================================

put_lookup_read(Config) ->
    N = start(Config, #{}),
    ?assertEqual(not_found, ra_log_snap_store:lookup(N, <<"a">>)),
    ?assertEqual({error, not_found},
                 ra_log_snap_store:read(N, <<"a">>, {1, 1})),
    Img = image(2000),
    Ind = ra_seq:from_list([1, 2, 3, 10]),
    ok = ra_log_snap_store:put(N, <<"a">>, <<"e1">>, {5, 2}, Img, Ind),
    ?assertMatch({ok, #{idx := 5, term := 2, epoch := <<"e1">>}},
                 ra_log_snap_store:lookup(N, <<"a">>)),
    {ok, Img1, Ind1} = ra_log_snap_store:read(N, <<"a">>, {5, 2}),
    ?assertEqual(Img, Img1),
    ?assertEqual(Ind, Ind1),
    %% not the snapshot we asked for
    ?assertEqual({error, superseded},
                 ra_log_snap_store:read(N, <<"a">>, {4, 2})),
    ?assertEqual({error, superseded},
                 ra_log_snap_store:read(N, <<"a">>, {5, 1})),
    ?assertEqual({error, not_found},
                 ra_log_snap_store:read(N, <<"b">>, {5, 2})),
    ok.

put_semantics(Config) ->
    N = start(Config, #{}),
    E = <<"e">>,
    ok = ra_log_snap_store:put(N, <<"a">>, E, {10, 1}, image(100), []),
    %% same snapshot again is a no-op
    ok = ra_log_snap_store:put(N, <<"a">>, E, {10, 1}, image(100), []),
    %% older is stale
    ?assertEqual({error, stale},
                 ra_log_snap_store:put(N, <<"a">>, E, {9, 1}, image(100), [])),
    ?assertEqual({error, stale},
                 ra_log_snap_store:put(N, <<"a">>, E, {10, 0}, image(100), [])),
    %% newer replaces
    Img = image(100),
    ok = ra_log_snap_store:put(N, <<"a">>, E, {11, 1}, Img, []),
    ?assertMatch({ok, #{idx := 11}}, ra_log_snap_store:lookup(N, <<"a">>)),
    {ok, Img, []} = ra_log_snap_store:read(N, <<"a">>, {11, 1}),
    %% same index, higher term replaces
    ok = ra_log_snap_store:put(N, <<"a">>, E, {11, 2}, Img, []),
    ?assertMatch({ok, #{idx := 11, term := 2}},
                 ra_log_snap_store:lookup(N, <<"a">>)),
    #{stale_puts := 2} = ra_log_snap_store:info(N),
    ok.

epoch_change_replaces(Config) ->
    %% a re-created member (new epoch) starts again from low indexes
    N = start(Config, #{}),
    ok = ra_log_snap_store:put(N, <<"a">>, <<"old">>, {500, 3}, image(10), []),
    Img = image(10),
    ok = ra_log_snap_store:put(N, <<"a">>, <<"new">>, {5, 1}, Img, []),
    ?assertMatch({ok, #{idx := 5, epoch := <<"new">>}},
                 ra_log_snap_store:lookup(N, <<"a">>)),
    {ok, Img, []} = ra_log_snap_store:read(N, <<"a">>, {5, 1}),
    %% and a late put from the old incarnation does not come back unless it
    %% is newer than what is there, which for a different epoch it is: the
    %% reconcile call is what lets the owner detect that
    ?assertEqual(not_found, ra_log_snap_store:reconcile(N, <<"a">>, <<"old">>)),
    ?assertEqual(not_found, ra_log_snap_store:lookup(N, <<"a">>)),
    ok.

reconcile_delete_release(Config) ->
    N = start(Config, #{}),
    E = <<"e">>,
    ok = ra_log_snap_store:put(N, <<"a">>, E, {10, 1}, image(10), []),
    ok = ra_log_snap_store:put(N, <<"b">>, E, {10, 1}, image(10), []),
    ok = ra_log_snap_store:put(N, <<"c">>, E, {10, 1}, image(10), []),
    ?assertEqual({ok, #{idx => 10, term => 1}},
                 ra_log_snap_store:reconcile(N, <<"a">>, E)),
    ?assertEqual(not_found, ra_log_snap_store:reconcile(N, <<"zz">>, E)),
    %% delete only removes the right incarnation
    ok = ra_log_snap_store:delete(N, <<"b">>, <<"other">>),
    ?assertMatch({ok, _}, ra_log_snap_store:lookup(N, <<"b">>)),
    ok = ra_log_snap_store:delete(N, <<"b">>, E),
    ?assertEqual(not_found, ra_log_snap_store:lookup(N, <<"b">>)),
    %% release only removes the exact snapshot it names: a snapshot that
    %% failed to be written must not drop the current one
    [begin
         ok = ra_log_snap_store:release(N, <<"c">>, IdxTerm),
         _ = ra_log_snap_store:info(N),
         ?assertMatch({ok, _}, ra_log_snap_store:lookup(N, <<"c">>))
     end || IdxTerm <- [{9, 1}, {10, 0}, {10, 2}, {11, 1}]],
    ok = ra_log_snap_store:release(N, <<"c">>, {10, 1}),
    _ = ra_log_snap_store:info(N),
    ?assertEqual(not_found, ra_log_snap_store:lookup(N, <<"c">>)),
    #{live_bytes := Live} = ra_log_snap_store:info(N),
    {ok, _, _} = ra_log_snap_store:read(N, <<"a">>, {10, 1}),
    ?assert(Live > 0),
    ok.

recovery(Config) ->
    N = start(Config, #{}),
    E = <<"e">>,
    Expected = populate(N, E, 30, 3),
    #{live_bytes := Live} = ra_log_snap_store:info(N),
    ok = ra_log_snap_store:stop(N),
    N = start(Config, #{}),
    verify(N, E, Expected),
    #{live_bytes := Live2} = ra_log_snap_store:info(N),
    ?assertEqual(Live, Live2),
    %% new writes after a restart win over recovered ones
    Img = image(50),
    ok = ra_log_snap_store:put(N, <<"u1">>, E, {1000, 5}, Img, []),
    {ok, Img, []} = ra_log_snap_store:read(N, <<"u1">>, {1000, 5}),
    ok = ra_log_snap_store:stop(N),
    N = start(Config, #{}),
    {ok, Img, []} = ra_log_snap_store:read(N, <<"u1">>, {1000, 5}),
    ok.

recovery_torn_tail(Config) ->
    N = start(Config, #{}),
    E = <<"e">>,
    ok = ra_log_snap_store:put(N, <<"a">>, E, {1, 1}, image(100), []),
    Good = image(100),
    ok = ra_log_snap_store:put(N, <<"b">>, E, {2, 1}, Good, []),
    #{active_file := No, active_offset := Off} = ra_log_snap_store:info(N),
    %% a record that was being written when we crashed
    ok = ra_log_snap_store:put(N, <<"c">>, E, {3, 1}, image(3000), []),
    ok = ra_log_snap_store:stop(N),
    File = filename:join(?config(store_dir, Config),
                         lists:flatten(io_lib:format("~8..0b.snap", [No]))),
    %% tear the last record in half
    {ok, Fd} = file:open(File, [read, write, raw, binary]),
    {ok, _} = file:position(Fd, Off + 1000),
    ok = file:truncate(Fd),
    ok = file:close(Fd),
    N = start(Config, #{}),
    ?assertMatch({ok, _}, ra_log_snap_store:lookup(N, <<"a">>)),
    {ok, Good, []} = ra_log_snap_store:read(N, <<"b">>, {2, 1}),
    ?assertEqual(not_found, ra_log_snap_store:lookup(N, <<"c">>)),
    %% the store keeps working and the torn file is never appended to
    Img = image(300),
    ok = ra_log_snap_store:put(N, <<"c">>, E, {3, 1}, Img, []),
    ok = ra_log_snap_store:stop(N),
    N = start(Config, #{}),
    {ok, Img, []} = ra_log_snap_store:read(N, <<"c">>, {3, 1}),
    {ok, Good, []} = ra_log_snap_store:read(N, <<"b">>, {2, 1}),
    ok.

recovery_skips_an_invalid_record(Config) ->
    %% a record whose body does not validate is skipped, the records after it
    %% are independent and are kept
    N = start(Config, #{}),
    E = <<"e">>,
    Img = image(100),
    ok = ra_log_snap_store:put(N, <<"a">>, E, {1, 1}, Img, []),
    ok = ra_log_snap_store:put(N, <<"b">>, E, {1, 1}, image(100), []),
    Imgc = image(100),
    ok = ra_log_snap_store:put(N, <<"c">>, E, {1, 1}, Imgc, []),
    %% every put was its own 4KB aligned batch: a at 64, b at 4096, c at 8192
    ok = ra_log_snap_store:stop(N),
    [File] = filelib:wildcard(filename:join(?config(store_dir, Config),
                                            "*.snap")),
    {ok, Fd} = file:open(File, [read, write, raw, binary]),
    ok = file:pwrite(Fd, 4096 + 9 + 20, <<255>>),
    ok = file:close(Fd),
    N = start(Config, #{}),
    %% found when recovering and again when the file is retired
    #{corrupt_records := Corrupt} = ra_log_snap_store:info(N),
    ?assert(Corrupt >= 1),
    {ok, Img, []} = ra_log_snap_store:read(N, <<"a">>, {1, 1}),
    ?assertEqual(not_found, ra_log_snap_store:lookup(N, <<"b">>)),
    {ok, Imgc, []} = ra_log_snap_store:read(N, <<"c">>, {1, 1}),
    %% and it keeps accepting writes
    Img2 = image(100),
    ok = ra_log_snap_store:put(N, <<"b">>, E, {2, 1}, Img2, []),
    {ok, Img2, []} = ra_log_snap_store:read(N, <<"b">>, {2, 1}),
    ok.

recovery_stops_at_an_unreadable_record_header(Config) ->
    %% when not even the length of a record can be trusted nothing after it
    %% can be found
    N = start(Config, #{}),
    E = <<"e">>,
    Img = image(100),
    ok = ra_log_snap_store:put(N, <<"a">>, E, {1, 1}, Img, []),
    ok = ra_log_snap_store:put(N, <<"b">>, E, {1, 1}, image(100), []),
    ok = ra_log_snap_store:put(N, <<"c">>, E, {1, 1}, image(100), []),
    ok = ra_log_snap_store:stop(N),
    [File] = filelib:wildcard(filename:join(?config(store_dir, Config),
                                            "*.snap")),
    {ok, Fd} = file:open(File, [read, write, raw, binary]),
    ok = file:pwrite(Fd, 4096, <<255>>),
    ok = file:close(Fd),
    N = start(Config, #{}),
    {ok, Img, []} = ra_log_snap_store:read(N, <<"a">>, {1, 1}),
    ?assertEqual(not_found, ra_log_snap_store:lookup(N, <<"b">>)),
    ?assertEqual(not_found, ra_log_snap_store:lookup(N, <<"c">>)),
    ok.

recovery_live_fun(Config) ->
    N = start(Config, #{}),
    ok = ra_log_snap_store:put(N, <<"a">>, <<"e1">>, {1, 1}, image(10), []),
    ok = ra_log_snap_store:put(N, <<"b">>, <<"e1">>, {1, 1}, image(10), []),
    ok = ra_log_snap_store:put(N, <<"c">>, <<"e2">>, {1, 1}, image(10), []),
    ok = ra_log_snap_store:stop(N),
    %% the owner says only "a" at epoch e1 and "c" at epoch e2 still exist
    Live = fun (<<"a">>, <<"e1">>) -> true;
               (<<"c">>, <<"e2">>) -> true;
               (_, _) -> false
           end,
    N = start(Config, #{live_fun => Live}),
    ?assertMatch({ok, _}, ra_log_snap_store:lookup(N, <<"a">>)),
    ?assertEqual(not_found, ra_log_snap_store:lookup(N, <<"b">>)),
    ?assertMatch({ok, _}, ra_log_snap_store:lookup(N, <<"c">>)),
    ok.

batches_are_aligned(Config) ->
    N = start(Config, #{}),
    [begin
         ok = ra_log_snap_store:put(N, <<"a">>, <<"e">>, {I, 1}, image(I * 7),
                                    []),
         #{active_offset := Off} = ra_log_snap_store:info(N),
         ?assertEqual(0, Off rem 4096)
     end || I <- lists:seq(1, 40)],
    ok.

concurrent_puts_are_batched(Config) ->
    N = start(Config, #{}),
    E = <<"e">>,
    Parent = self(),
    Num = 300,
    Pids = [spawn_link(
              fun () ->
                      UId = integer_to_binary(I),
                      [ok = ra_log_snap_store:put(N, UId, E, {R, 1},
                                                  image(500), [])
                       || R <- lists:seq(1, 5)],
                      Parent ! {done, self()}
              end) || I <- lists:seq(1, Num)],
    [receive {done, P} -> ok after 30000 -> exit(timeout) end || P <- Pids],
    #{puts := Puts, batches := Batches} = ra_log_snap_store:info(N),
    ?assertEqual(Num * 5, Puts),
    ct:pal("~b puts in ~b batches", [Puts, Batches]),
    ?assert(Batches < Puts),
    [?assertMatch({ok, #{idx := 5}},
                  ra_log_snap_store:lookup(N, integer_to_binary(I)))
     || I <- lists:seq(1, Num)],
    ok.

roll_and_retire(Config) ->
    %% a tiny minimum file size forces many rolls; superseded data must be
    %% reclaimed and the latest snapshot of every member must stay readable
    N = start(Config, #{min_file_bytes => 64 * 1024,
                        retire_chunk_bytes => 16 * 1024}),
    E = <<"e">>,
    Members = 20,
    Rounds = 60,
    Expected =
        lists:foldl(
          fun (R, Acc) ->
                  lists:foldl(
                    fun (M, A) ->
                            UId = integer_to_binary(M),
                            Img = image(2000 + M),
                            ok = ra_log_snap_store:put(N, UId, E, {R, 1}, Img,
                                                       ra_seq:from_list([R])),
                            A#{UId => {{R, 1}, Img}}
                    end, Acc, lists:seq(1, Members))
          end, #{}, lists:seq(1, Rounds)),
    wait_quiescent(N),
    #{rolls := Rolls, retired_files := Retired, rolled_files := 0} =
        ra_log_snap_store:info(N),
    ?assert(Rolls >= 5),
    ?assert(Retired >= 4),
    maps:foreach(
      fun (UId, {IdxTerm, Img}) ->
              {Idx, _} = IdxTerm,
              ?assertMatch({ok, Img, [Idx]},
                           ra_log_snap_store:read(N, UId, IdxTerm))
      end, Expected),
    %% bounded disk use: old files were deleted
    Files = filelib:wildcard(filename:join(?config(store_dir, Config),
                                           "*.snap")),
    Total = lists:sum([filelib:file_size(F) || F <- Files]),
    Live = Members * 2100,
    ct:pal("~b files, ~b bytes for ~b live", [length(Files), Total, Live]),
    ?assert(length(Files) =< 4),
    ?assert(Total < 12 * Live + 2 * 64 * 1024),
    ok.

cold_entries_survive_retire(Config) ->
    N = start(Config, #{min_file_bytes => 32 * 1024,
                        retire_chunk_bytes => 8 * 1024}),
    E = <<"e">>,
    Cold = [{integer_to_binary(I), image(1500)} || I <- lists:seq(1, 10)],
    [ok = ra_log_snap_store:put(N, UId, E, {1, 1}, Img, [])
     || {UId, Img} <- Cold],
    %% one hot member churns through many files
    [ok = ra_log_snap_store:put(N, <<"hot">>, E, {R, 1}, image(3000), [])
     || R <- lists:seq(1, 400)],
    wait_quiescent(N),
    #{rolls := Rolls, copies := Copies} = ra_log_snap_store:info(N),
    ?assert(Rolls >= 10),
    ?assert(Copies >= 10),
    [?assertMatch({ok, Img, []}, ra_log_snap_store:read(N, UId, {1, 1}))
     || {UId, Img} <- Cold],
    ok.

restart_after_retire(Config) ->
    Opts = #{min_file_bytes => 32 * 1024, retire_chunk_bytes => 8 * 1024},
    N = start(Config, Opts),
    E = <<"e">>,
    Expected = populate(N, E, 15, 40),
    wait_quiescent(N),
    ok = ra_log_snap_store:stop(N),
    N = start(Config, Opts),
    verify(N, E, Expected),
    %% and again with files that were rolled but not yet retired
    [ok = ra_log_snap_store:put(N, <<"x">>, E, {R, 1}, image(4000), [])
     || R <- lists:seq(1, 50)],
    ok = ra_log_snap_store:stop(N),
    N = start(Config, Opts),
    verify(N, E, Expected),
    ?assertMatch({ok, #{idx := 50}}, ra_log_snap_store:lookup(N, <<"x">>)),
    wait_quiescent(N),
    verify(N, E, Expected),
    ok.

write_error_fails_batch_and_is_not_visible(Config) ->
    N = start(Config, #{io => io_with_faults()}),
    E = <<"e">>,
    Img = image(100),
    ok = ra_log_snap_store:put(N, <<"a">>, E, {1, 1}, Img, []),
    inject_failure(pwrite),
    ?assertEqual({error, eio},
                 ra_log_snap_store:put(N, <<"a">>, E, {2, 1}, image(100), [])),
    #{errors := 1} = ra_log_snap_store:info(N),
    %% nothing was published
    ?assertMatch({ok, #{idx := 1}}, ra_log_snap_store:lookup(N, <<"a">>)),
    {ok, Img, []} = ra_log_snap_store:read(N, <<"a">>, {1, 1}),
    %% and the store recovers by moving on to a new file
    clear_failure(),
    Img2 = image(100),
    ok = ra_log_snap_store:put(N, <<"a">>, E, {3, 1}, Img2, []),
    {ok, Img2, []} = ra_log_snap_store:read(N, <<"a">>, {3, 1}),
    #{active_file := No} = ra_log_snap_store:info(N),
    ?assert(No >= 2),
    ok.

fsync_error_unacked_record_ignored_after_restart(Config) ->
    %% the data of a batch whose fsync failed is in the file and valid, but
    %% was never acknowledged and may not be durable, so it must not come back
    N = start(Config, #{io => io_with_faults()}),
    E = <<"e">>,
    Img = image(100),
    ok = ra_log_snap_store:put(N, <<"a">>, E, {1, 1}, Img, []),
    inject_failure(sync),
    ?assertEqual({error, eio},
                 ra_log_snap_store:put(N, <<"a">>, E, {2, 1}, image(100), [])),
    clear_failure(),
    ok = ra_log_snap_store:put(N, <<"b">>, E, {1, 1}, image(100), []),
    ok = ra_log_snap_store:stop(N),
    N = start(Config, #{}),
    ?assertMatch({ok, #{idx := 1}}, ra_log_snap_store:lookup(N, <<"a">>)),
    {ok, Img, []} = ra_log_snap_store:read(N, <<"a">>, {1, 1}),
    ?assertMatch({ok, _}, ra_log_snap_store:lookup(N, <<"b">>)),
    ok.

cannot_create_file_recovers(Config) ->
    %% creating the next file fails: puts get errors (rather than crashing the
    %% store) until it works again. Each put is a block, the file rolls at four.
    N = start(Config, #{io => io_with_faults(), min_file_bytes => 16384}),
    E = <<"e">>,
    [ok = ra_log_snap_store:put(N, <<"a">>, E, {I, 1}, image(100), [])
     || I <- [1, 2, 3]],
    inject_failure(create),
    %% this put rolls the file and the roll fails
    ok = ra_log_snap_store:put(N, <<"a">>, E, {4, 1}, image(100), []),
    ?assertMatch({error, _},
                 ra_log_snap_store:put(N, <<"a">>, E, {5, 1}, image(100), [])),
    ?assertMatch({ok, #{idx := 4}}, ra_log_snap_store:lookup(N, <<"a">>)),
    clear_failure(),
    Img = image(100),
    ok = ra_log_snap_store:put(N, <<"a">>, E, {6, 1}, Img, []),
    {ok, Img, []} = ra_log_snap_store:read(N, <<"a">>, {6, 1}),
    ok.

%% the failure is of the batch only, so the file after it can be created: the
%% usual shape of a transient error. The store must carry on (it used to crash,
%% taking the log supervisor with it).
batch_failure_when_the_next_file_can_be_created(Config) ->
    [begin
         Name = list_to_atom(atom_to_list(?config(store_name, Config)) ++
                             "_" ++ atom_to_list(Fault)),
         Dir = filename:join(?config(store_dir, Config), atom_to_list(Fault)),
         {ok, Pid} = ra_log_snap_store:start_link(
                       #{name => Name, dir => Dir, io => io_with_faults()}),
         E = <<"e">>,
         Img1 = image(100),
         ok = ra_log_snap_store:put(Name, <<"a">>, E, {1, 1}, Img1, []),
         inject_failure(Fault, 1),
         ?assertEqual({error, eio},
                      ra_log_snap_store:put(Name, <<"a">>, E, {2, 1},
                                            image(100), [])),
         ?assertEqual(Pid, whereis(Name)),
         ?assertMatch({ok, #{idx := 1}}, ra_log_snap_store:lookup(Name, <<"a">>)),
         Img3 = image(100),
         ok = ra_log_snap_store:put(Name, <<"a">>, E, {3, 1}, Img3, []),
         ?assertEqual(ok, ra_log_snap_store:status(Name)),
         #{errors := 1} = ra_log_snap_store:info(Name),
         %% what was not acknowledged does not come back
         ok = ra_log_snap_store:stop(Name),
         {ok, _} = ra_log_snap_store:start_link(
                     #{name => Name, dir => Dir, io => io_with_faults()}),
         ?assertMatch({ok, #{idx := 3}}, ra_log_snap_store:lookup(Name, <<"a">>)),
         {ok, Img3, []} = ra_log_snap_store:read(Name, <<"a">>, {3, 1}),
         ok = ra_log_snap_store:stop(Name)
     end || Fault <- [pwrite, sync]],
    ok.

counters_count_what_happens(Config) ->
    N = start(Config, #{}),
    E = <<"e">>,
    [ok = ra_log_snap_store:put(N, <<"a">>, E, {I, 1}, image(100), [])
     || I <- [1, 2, 3]],
    ok = ra_log_snap_store:put(N, <<"b">>, E, {1, 1}, image(100), []),
    ?assertEqual({error, stale},
                 ra_log_snap_store:put(N, <<"b">>, E, {0, 1}, image(100), [])),
    #{puts := 4, batches := Batches, stale_puts := 1, errors := 0,
      bytes_written := Bytes, fsync_time_us := FsyncUs, live_bytes := Live,
      health := []} = ra_log_snap_store:info(N),
    ?assert(Batches >= 1),
    ?assert(Bytes >= 4096),
    ?assert(FsyncUs > 0),
    ?assert(Live > 0),
    ?assertEqual(ok, ra_log_snap_store:status(N)),
    ok.

status_follows_failures(Config) ->
    N = start(Config, #{io => io_with_faults()}),
    E = <<"e">>,
    ok = ra_log_snap_store:put(N, <<"a">>, E, {1, 1}, image(100), []),
    ?assertEqual(ok, ra_log_snap_store:status(N)),
    inject_failure(pwrite),
    ?assertEqual({error, eio},
                 ra_log_snap_store:put(N, <<"a">>, E, {2, 1}, image(100), [])),
    %% the batch failed and no new file could be made
    ?assertEqual({degraded, [no_active_file, write_errors]},
                 ra_log_snap_store:status(N)),
    #{errors := 1} = ra_log_snap_store:info(N),
    clear_failure(),
    %% it recovers by itself with the next put
    ok = ra_log_snap_store:put(N, <<"a">>, E, {3, 1}, image(100), []),
    ?assertEqual(ok, ra_log_snap_store:status(N)),
    ok.

unknown_call_gets_an_error(Config) ->
    N = start(Config, #{}),
    ?assertEqual({error, unknown_request},
                 gen_batch_server:call(N, something_else, 5000)),
    ok.

%% two identical puts in a batch that fails to be written: the second must not
%% be acknowledged because the first looked like it made it durable
duplicate_put_in_a_failed_batch_is_not_acked(Config) ->
    Self = self(),
    N = start(Config, #{io => io_blocking_faulty(Self)}),
    E = <<"e">>,
    ok = ra_log_snap_store:put(N, <<"a">>, E, {1, 1}, image(10), []),
    block_syncs(),
    _ = spawn(fun () ->
                      ra_log_snap_store:put(N, <<"b">>, E, {1, 1}, image(10), [])
              end),
    receive in_sync -> ok after 5000 -> ct:fail(put_not_in_sync) end,
    inject_failure(pwrite),
    Img = image(10),
    [spawn(fun () ->
                   Self ! {put, I, ra_log_snap_store:put(N, <<"a">>, E, {2, 1},
                                                         Img, [])}
           end) || I <- [1, 2]],
    wait_queued(N, 2),
    unblock_syncs(),
    Replies = [receive {put, I, R} -> R after 5000 -> ct:fail(no_reply) end
               || I <- [1, 2]],
    ?assertEqual([{error, eio}, {error, eio}], Replies),
    clear_failure(),
    ?assertMatch({ok, #{idx := 1}}, ra_log_snap_store:lookup(N, <<"a">>)),
    ok.

%% a delete and a put that came after it, in the same batch, must not undo the put
delete_is_ordered_before_later_puts(Config) ->
    Self = self(),
    N = start(Config, #{io => io_blocking_sync(Self)}),
    E = <<"e">>,
    ok = ra_log_snap_store:put(N, <<"a">>, E, {1, 1}, image(10), []),
    block_syncs(),
    _ = spawn(fun () ->
                      ra_log_snap_store:put(N, <<"b">>, E, {1, 1}, image(10), [])
              end),
    receive in_sync -> ok after 5000 -> ct:fail(put_not_in_sync) end,
    _ = spawn(fun () ->
                      Self ! {deleted, ra_log_snap_store:delete(N, <<"a">>, any)}
              end),
    wait_queued(N, 1),
    _ = spawn(fun () ->
                      Self ! {put, ra_log_snap_store:put(N, <<"a">>, E, {5, 1},
                                                         image(10), [])}
              end),
    wait_queued(N, 2),
    unblock_syncs(),
    receive {deleted, ok} -> ok after 5000 -> ct:fail(no_delete_reply) end,
    receive {put, ok} -> ok after 5000 -> ct:fail(no_put_reply) end,
    ?assertMatch({ok, #{idx := 5}}, ra_log_snap_store:lookup(N, <<"a">>)),
    ok.

%% The first file holds a1 (dead, later replaced by a2), a2, b and c in four
%% batches of one 4KB block each and is rolled by the fourth.
rolled_file_with_a_hole(Config, Corrupt) ->
    Self = self(),
    N = start(Config, #{min_file_bytes => 16384,
                        io => io_blocking_sync(Self)}),
    E = <<"e">>,
    ok = ra_log_snap_store:put(N, <<"a">>, E, {1, 1}, image(100), []),
    A2 = image(100),
    ok = ra_log_snap_store:put(N, <<"a">>, E, {2, 1}, A2, []),
    B = image(100),
    ok = ra_log_snap_store:put(N, <<"b">>, E, {1, 1}, B, []),
    block_syncs(),
    C = image(100),
    _ = spawn(fun () ->
                      Self ! {put_c, ra_log_snap_store:put(N, <<"c">>, E, {1, 1},
                                                           C, [])}
              end),
    receive in_sync -> ok after 5000 -> ct:fail(put_not_in_sync) end,
    %% damage the file before the retiring of it starts
    [File] = filelib:wildcard(filename:join(?config(store_dir, Config),
                                            "*.snap")),
    {ok, Fd} = file:open(File, [read, write, raw, binary]),
    ok = Corrupt(Fd),
    ok = file:close(Fd),
    unblock_syncs(),
    receive {put_c, ok} -> ok after 5000 -> ct:fail(no_put_reply) end,
    {N, File, A2, B, C}.

retire_skips_an_invalid_record_and_copies_the_rest(Config) ->
    %% a1 (dead) is damaged, a2, b and c must all make it into a new file
    {N, File, A2, B, C} =
        rolled_file_with_a_hole(Config,
                                fun (Fd) ->
                                        file:pwrite(Fd, 64 + 9 + 20, <<255>>)
                                end),
    wait_quiescent(N),
    ?assertNot(filelib:is_file(File)),
    [?assertMatch({ok, Img, []}, ra_log_snap_store:read(N, UId, IdxTerm))
     || {UId, IdxTerm, Img} <- [{<<"a">>, {2, 1}, A2}, {<<"b">>, {1, 1}, B},
                                {<<"c">>, {1, 1}, C}]],
    ok = ra_log_snap_store:stop(N),
    N = start(Config, #{min_file_bytes => 16384}),
    [?assertMatch({ok, Img, []}, ra_log_snap_store:read(N, UId, IdxTerm))
     || {UId, IdxTerm, Img} <- [{<<"a">>, {2, 1}, A2}, {<<"b">>, {1, 1}, B},
                                {<<"c">>, {1, 1}, C}]],
    ok.

retire_never_deletes_a_file_that_is_still_referenced(Config) ->
    %% b's record can not be got past (its type is damaged) so b and c, which
    %% follow it, can not be copied. The file must stay as they live in it.
    {N, File, A2, _B, C} =
        rolled_file_with_a_hole(Config,
                                fun (Fd) ->
                                        file:pwrite(Fd, 8192, <<255>>)
                                end),
    wait_for(fun () ->
                     maps:get(retire_blocked, ra_log_snap_store:info(N), 0) == 1
             end),
    ?assert(filelib:is_file(File)),
    ?assertEqual({degraded, [files_blocked]}, ra_log_snap_store:status(N)),
    %% c was not damaged, it is still served from the file
    ?assertMatch({ok, C, []}, ra_log_snap_store:read(N, <<"c">>, {1, 1})),
    %% a2 was before the damage and was copied
    ?assertMatch({ok, A2, []}, ra_log_snap_store:read(N, <<"a">>, {2, 1})),
    #{rolled_files := 0, retiring := false} = ra_log_snap_store:info(N),
    ok.

retire_open_error_backs_off(Config) ->
    IO = #{open_read => fun (Path) ->
                                case failing(open_read) of
                                    true -> {error, eio};
                                    false -> file:open(Path, [read, raw, binary])
                                end
                        end},
    N = start(Config, #{min_file_bytes => 16384, io => IO}),
    inject_failure(open_read),
    E = <<"e">>,
    %% the fourth block rolls the file
    [ok = ra_log_snap_store:put(N, <<"a">>, E, {I, 1}, image(100), [])
     || I <- [1, 2, 3, 4]],
    timer:sleep(100),
    Pid = whereis(N),
    {reductions, R0} = process_info(Pid, reductions),
    timer:sleep(500),
    {reductions, R1} = process_info(Pid, reductions),
    %% it is waiting to retry, not retrying in a loop
    ?assert(R1 - R0 < 10000),
    ?assert(maps:get(rolled_files, ra_log_snap_store:info(N)) >= 1),
    clear_failure(),
    %% it does retry
    wait_quiescent(N),
    ok.

%% an I/O error is not damage: the snapshots in the file must not be given up
%% on (renamed, or deleted by retiring) because it could not be read once
unreadable_file_stops_the_store_starting(Config) ->
    N = start(Config, #{}),
    E = <<"e">>,
    Img = image(100),
    ok = ra_log_snap_store:put(N, <<"a">>, E, {1, 1}, Img, []),
    ok = ra_log_snap_store:stop(N),
    [File] = filelib:wildcard(filename:join(?config(store_dir, Config),
                                            "*.snap")),
    %% a directory where the file should be can not be opened as a file
    ok = file:rename(File, File ++ ".orig"),
    ok = file:make_dir(File),
    process_flag(trap_exit, true),
    ?assertMatch({error, _},
                 ra_log_snap_store:start_link(
                   #{name => N, dir => ?config(store_dir, Config)})),
    process_flag(trap_exit, false),
    ok = file:del_dir(File),
    ok = file:rename(File ++ ".orig", File),
    N = start(Config, #{}),
    {ok, Img, []} = ra_log_snap_store:read(N, <<"a">>, {1, 1}),
    ?assertNot(filelib:is_file(File ++ ".bad")),
    ok.

damaged_header_in_an_old_file_is_set_aside(Config) ->
    N = start(Config, #{}),
    E = <<"e">>,
    ok = ra_log_snap_store:put(N, <<"a">>, E, {1, 1}, image(100), []),
    ok = ra_log_snap_store:put(N, <<"b">>, E, {1, 1}, image(100), []),
    ok = ra_log_snap_store:stop(N),
    [File] = filelib:wildcard(filename:join(?config(store_dir, Config),
                                            "*.snap")),
    {ok, Fd} = file:open(File, [read, write, raw, binary]),
    ok = file:pwrite(Fd, 12, <<255>>),
    ok = file:close(Fd),
    N = start(Config, #{}),
    ?assertEqual(not_found, ra_log_snap_store:lookup(N, <<"a">>)),
    %% not deleted: someone might want to look at it
    ?assert(filelib:is_file(File ++ ".bad")),
    ?assertNot(filelib:is_file(File)),
    ok = ra_log_snap_store:put(N, <<"a">>, E, {2, 1}, image(100), []),
    ok.

torn_file_creation_is_deleted(Config) ->
    N = start(Config, #{}),
    E = <<"e">>,
    Img = image(100),
    ok = ra_log_snap_store:put(N, <<"a">>, E, {1, 1}, Img, []),
    #{active_file := No} = ra_log_snap_store:info(N),
    ok = ra_log_snap_store:stop(N),
    %% the next file was being created when the node went down
    Torn = filename:join(?config(store_dir, Config),
                         lists:flatten(io_lib:format("~8..0b.snap", [No + 1]))),
    ok = file:write_file(Torn, <<"RASS", 1, 0, 0>>),
    N = start(Config, #{}),
    ?assertNot(filelib:is_file(Torn ++ ".bad")),
    {ok, Img, []} = ra_log_snap_store:read(N, <<"a">>, {1, 1}),
    ok.

stray_files_in_the_directory_are_ignored(Config) ->
    N = start(Config, #{}),
    ok = ra_log_snap_store:stop(N),
    Dir = ?config(store_dir, Config),
    ok = file:write_file(filename:join(Dir, "notes.snap"), <<"hi">>),
    ok = file:write_file(filename:join(Dir, "other.txt"), <<"hi">>),
    N = start(Config, #{}),
    Img = image(10),
    ok = ra_log_snap_store:put(N, <<"a">>, <<"e">>, {1, 1}, Img, []),
    {ok, Img, []} = ra_log_snap_store:read(N, <<"a">>, {1, 1}),
    ok.

%% A put that was sent before a reconcile, e.g. by a worker that died with its
%% member and was then restarted, is applied before the reconcile answers. The
%% store is held in the fsync of the batch that has the put.
reconcile_is_ordered_after_puts_in_flight(Config) ->
    Self = self(),
    N = start(Config, #{io => io_blocking_sync(Self)}),
    E = <<"e">>,
    %% the first sync (the new file's header) is not blocked
    ok = ra_log_snap_store:put(N, <<"a">>, E, {1, 1}, image(10), []),
    block_syncs(),
    Worker = spawn(fun () ->
                           Self ! {put_result,
                                   ra_log_snap_store:put(N, <<"a">>, E, {300, 1},
                                                         image(10), [])}
                   end),
    receive in_sync -> ok after 5000 -> ct:fail(put_not_in_sync) end,
    %% the member and its worker are gone, the member starts again
    exit(Worker, kill),
    _ = spawn(fun () ->
                               Self ! {reconciled,
                                       ra_log_snap_store:reconcile(N, <<"a">>, E)}
                       end),
    wait_queued(N, 1),
    unblock_syncs(),
    receive
        {reconciled, Res} ->
            ?assertEqual({ok, #{idx => 300, term => 1}}, Res)
    after 5000 ->
              ct:fail(reconcile_timeout)
    end,
    ok.

delete_is_ordered_after_puts_in_flight(Config) ->
    Self = self(),
    N = start(Config, #{io => io_blocking_sync(Self)}),
    E = <<"e">>,
    ok = ra_log_snap_store:put(N, <<"a">>, E, {1, 1}, image(10), []),
    block_syncs(),
    _ = spawn(fun () ->
                      ra_log_snap_store:put(N, <<"a">>, E, {300, 1}, image(10), [])
              end),
    receive in_sync -> ok after 5000 -> ct:fail(put_not_in_sync) end,
    %% the member is deleted whilst a put from it is being written
    _ = spawn(fun () ->
                      Self ! {deleted, ra_log_snap_store:delete(N, <<"a">>, any)}
              end),
    wait_queued(N, 1),
    unblock_syncs(),
    receive {deleted, ok} -> ok after 5000 -> ct:fail(delete_timeout) end,
    ?assertEqual(not_found, ra_log_snap_store:lookup(N, <<"a">>)),
    ok.

io_blocking_sync(Test) ->
    #{sync => fun (Fd) ->
                      maybe_block(Test),
                      ra_file:sync(Fd)
              end}.

%% blocks in sync when asked to, and fails when asked to
io_blocking_faulty(Test) ->
    (io_with_faults())#{sync => fun (Fd) ->
                                        maybe_block(Test),
                                        case failing(sync) of
                                            true -> {error, eio};
                                            false -> ra_file:sync(Fd)
                                        end
                                end}.

maybe_block(Test) ->
    case persistent_term:get({?MODULE, block_sync}, false) of
        true ->
            Test ! in_sync,
            receive unblock -> ok after 10000 -> ok end;
        false ->
            ok
    end.

wait_queued(N, Len) ->
    wait_for(fun () ->
                     {message_queue_len, L} =
                         process_info(whereis(N), message_queue_len),
                     L >= Len
             end).

wait_for(Fun) ->
    wait_for(Fun, 200).

wait_for(_Fun, 0) ->
    ct:fail(condition_never_true);
wait_for(Fun, N) ->
    case Fun() of
        true -> ok;
        false ->
            timer:sleep(25),
            wait_for(Fun, N - 1)
    end.

block_syncs() ->
    persistent_term:put({?MODULE, block_sync}, true).

%% lets the blocked sync (if any) and later ones through
unblock_syncs() ->
    persistent_term:put({?MODULE, block_sync}, false),
    Store = [P || P <- processes(),
                  {registered_name, Name} <- [process_info(P, registered_name)],
                  lists:prefix("snap_store_", atom_to_list(Name))],
    [P ! unblock || P <- Store],
    ok.

migrate_out_creates_snapshot_directories(Config) ->
    N = start(Config, #{min_file_bytes => 32 * 1024,
                        retire_chunk_bytes => 8 * 1024}),
    DataDir = filename:join(?config(priv_dir, Config), "data"),
    Members = [<<"m1">>, <<"m2">>, <<"m3">>],
    [ok = ra_lib:make_dir(filename:join(DataDir, M)) || M <- Members],
    E = <<"e">>,
    %% several rounds so the files have been rolled and retired
    Expected =
        lists:foldl(
          fun (R, Acc) ->
                  lists:foldl(
                    fun (M, A) ->
                            MacState = crypto:strong_rand_bytes(1500),
                            Meta = snapshot_meta(R, 2),
                            {Image, _} = ra_log_snapshot:encode(Meta, MacState),
                            Indexes = ra_seq:from_list([R, R + 3]),
                            ok = ra_log_snap_store:put(N, M, E, {R, 2}, Image,
                                                       Indexes),
                            A#{M => {Meta, MacState, Indexes}}
                    end, Acc, Members)
          end, #{}, lists:seq(1, 40)),
    wait_quiescent(N),
    ok = ra_log_snap_store:stop(N),
    StoreDir = ?config(store_dir, Config),
    ?assert(ra_log_snap_store:has_files(StoreDir)),
    ok = ra_log_snap_store:migrate_out(#{dir => StoreDir,
                                         data_dir => DataDir}),
    ?assertNot(ra_log_snap_store:has_files(StoreDir)),
    maps:foreach(
      fun (M, {Meta, MacState, Indexes}) ->
              SnapDir = ra_snapshot:make_snapshot_dir(
                          filename:join([DataDir, M, "snapshots"]), 40, 2),
              ?assertEqual({ok, Meta, MacState},
                           ra_log_snapshot:recover(SnapDir)),
              ?assertEqual({ok, Indexes}, ra_snapshot:indexes(SnapDir))
      end, Expected),
    %% running it again changes nothing
    ok = ra_log_snap_store:migrate_out(#{dir => StoreDir,
                                         data_dir => DataDir}),
    ok.

migrate_out_skips_newer_directories_and_dead_members(Config) ->
    N = start(Config, #{}),
    DataDir = filename:join(?config(priv_dir, Config), "data2"),
    %% m1 has a newer snapshot directory already, m2's directory is gone
    [ok = ra_lib:make_dir(filename:join(DataDir, M)) || M <- [<<"m1">>, <<"m3">>]],
    NewerDir = ra_snapshot:make_snapshot_dir(
                 filename:join([DataDir, <<"m1">>, "snapshots"]), 90, 2),
    ok = ra_lib:make_dir(filename:join([DataDir, <<"m1">>, "snapshots"])),
    ok = ra_lib:make_dir(NewerDir),
    {ok, _} = ra_log_snapshot:write(NewerDir, snapshot_meta(90, 2), m1_newer,
                                    true),
    E = <<"e">>,
    [begin
         {Image, _} = ra_log_snapshot:encode(snapshot_meta(50, 2), M),
         ok = ra_log_snap_store:put(N, M, E, {50, 2}, Image, [])
     end || M <- [<<"m1">>, <<"m2">>, <<"m3">>]],
    ok = ra_log_snap_store:stop(N),
    StoreDir = ?config(store_dir, Config),
    Live = fun (UId, _) -> ra_lib:is_dir(filename:join(DataDir, UId)) end,
    ok = ra_log_snap_store:migrate_out(#{dir => StoreDir, data_dir => DataDir,
                                         live_fun => Live}),
    ?assertNot(ra_log_snap_store:has_files(StoreDir)),
    %% m1 is untouched, m2 was not recreated, m3 was moved out
    ?assertMatch({ok, #{index := 90}, m1_newer},
                 ra_log_snapshot:recover(NewerDir)),
    ?assertEqual({ok, ["0000000000000002_000000000000005A"]},
                 file:list_dir(filename:join([DataDir, <<"m1">>, "snapshots"]))),
    ?assertNot(filelib:is_dir(filename:join(DataDir, <<"m2">>))),
    ?assertMatch({ok, #{index := 50}, <<"m3">>},
                 ra_log_snapshot:recover(
                   ra_snapshot:make_snapshot_dir(
                     filename:join([DataDir, <<"m3">>, "snapshots"]), 50, 2))),
    ok.

migrate_out_replaces_a_partial_directory(Config) ->
    %% an earlier run was interrupted part way through writing a snapshot
    %% directory: it has the right name but is not a complete snapshot
    N = start(Config, #{}),
    DataDir = filename:join(?config(priv_dir, Config), "data3"),
    SnapshotsDir = filename:join([DataDir, <<"m1">>, "snapshots"]),
    ok = ra_lib:make_dir(DataDir),
    ok = ra_lib:make_dir(filename:join(DataDir, <<"m1">>)),
    ok = ra_lib:make_dir(SnapshotsDir),
    Partial = ra_snapshot:make_snapshot_dir(SnapshotsDir, 50, 2),
    ok = ra_lib:make_dir(Partial),
    {Image, _} = ra_log_snapshot:encode(snapshot_meta(50, 2), <<"state">>),
    Whole = iolist_to_binary(Image),
    ok = file:write_file(filename:join(Partial, "snapshot.dat"),
                         binary:part(Whole, 0, byte_size(Whole) - 3)),
    ok = ra_log_snap_store:put(N, <<"m1">>, <<"e">>, {50, 2}, Image, []),
    ok = ra_log_snap_store:stop(N),
    StoreDir = ?config(store_dir, Config),
    ok = ra_log_snap_store:migrate_out(#{dir => StoreDir, data_dir => DataDir}),
    ?assertMatch({ok, #{index := 50}, <<"state">>},
                 ra_log_snapshot:recover(Partial)),
    ?assertNot(ra_log_snap_store:has_files(StoreDir)),
    ?assertEqual({ok, ["snapshots"]}, file:list_dir(filename:join(DataDir, <<"m1">>))),
    ok.

migrate_out_keeps_the_log_when_a_write_fails(Config) ->
    N = start(Config, #{}),
    DataDir = filename:join(?config(priv_dir, Config), "data4"),
    ok = ra_lib:make_dir(DataDir),
    ok = ra_lib:make_dir(filename:join(DataDir, <<"m1">>)),
    {Image, _} = ra_log_snapshot:encode(snapshot_meta(50, 2), <<"state">>),
    ok = ra_log_snap_store:put(N, <<"m1">>, <<"e">>, {50, 2}, Image, []),
    ok = ra_log_snap_store:stop(N),
    %% a file where the snapshots directory should be
    ok = file:write_file(filename:join([DataDir, <<"m1">>, "snapshots"]), <<>>),
    StoreDir = ?config(store_dir, Config),
    ?assertMatch({error, _},
                 ra_log_snap_store:migrate_out(#{dir => StoreDir,
                                                 data_dir => DataDir})),
    ?assert(ra_log_snap_store:has_files(StoreDir)),
    ok.

%%%===================================================================
%%% Helpers
%%%===================================================================

snapshot_meta(Idx, Term) ->
    #{index => Idx, term => Term, cluster => #{}, machine_version => 1}.

start(Config, Opts) ->
    Name = ?config(store_name, Config),
    {ok, _} = ra_log_snap_store:start_link(
                Opts#{name => Name, dir => ?config(store_dir, Config)}),
    Name.

image(Size) ->
    crypto:strong_rand_bytes(Size).

%% puts `Rounds' snapshots for each of `Members' members, returns the latest
%% for each
populate(N, E, Members, Rounds) ->
    lists:foldl(
      fun (R, Acc) ->
              lists:foldl(
                fun (M, A) ->
                        UId = <<"u", (integer_to_binary(M))/binary>>,
                        Img = image(200 + M * 13),
                        ok = ra_log_snap_store:put(N, UId, E, {R, 1}, Img,
                                                   ra_seq:from_list([R, R + 1])),
                        A#{UId => {{R, 1}, Img}}
                end, Acc, lists:seq(1, Members))
      end, #{}, lists:seq(1, Rounds)).

verify(N, E, Expected) ->
    maps:foreach(
      fun (UId, {{Idx, _} = IdxTerm, Img}) ->
              ?assertMatch({ok, #{epoch := E, idx := Idx}},
                           ra_log_snap_store:lookup(N, UId)),
              ?assertEqual({ok, Img, ra_seq:from_list([Idx, Idx + 1])},
                           ra_log_snap_store:read(N, UId, IdxTerm))
      end, Expected).

wait_quiescent(N) ->
    wait_quiescent(N, 200).

wait_quiescent(_N, 0) ->
    ct:fail(store_never_quiescent);
wait_quiescent(N, Tries) ->
    case ra_log_snap_store:info(N) of
        #{rolled_files := 0, retiring := false} ->
            ok;
        _ ->
            timer:sleep(25),
            wait_quiescent(N, Tries - 1)
    end.

io_with_faults() ->
    #{pwrite => fun (Fd, Off, IO) ->
                        case failing(pwrite) of
                            true -> {error, eio};
                            false -> file:pwrite(Fd, Off, IO)
                        end
                end,
      sync => fun (Fd) ->
                      case failing(sync) of
                          true -> {error, eio};
                          false -> ra_file:sync(Fd)
                      end
              end,
      create => fun (Path) ->
                        case failing(create) of
                            true -> {error, enospc};
                            false -> file:open(Path, [write, raw, binary])
                        end
                end}.

inject_failure(What) ->
    inject_failure(What, infinity).

%% fail the next `Count' times the operation is done, then work again
inject_failure(What, Count) ->
    persistent_term:put({?MODULE, fail}, {What, Count}).

clear_failure() ->
    persistent_term:erase({?MODULE, fail}).

failing(What) ->
    case persistent_term:get({?MODULE, fail}, undefined) of
        {What, infinity} ->
            true;
        {What, N} when N > 0 ->
            persistent_term:put({?MODULE, fail}, {What, N - 1}),
            true;
        _ ->
            false
    end.
