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
     recovery_corrupt_record_ignores_rest_of_file,
     recovery_live_fun,
     batches_are_aligned,
     concurrent_puts_are_batched,
     roll_and_retire,
     cold_entries_survive_retire,
     restart_after_retire,
     write_error_fails_batch_and_is_not_visible,
     fsync_error_unacked_record_ignored_after_restart,
     cannot_create_file_recovers
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
    [{store_dir, Dir}, {store_name, Name} | Config].

end_per_testcase(_TestCase, Config) ->
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
    %% release only removes entries that are not newer
    ok = ra_log_snap_store:release(N, <<"c">>, 9),
    _ = ra_log_snap_store:info(N),
    ?assertMatch({ok, _}, ra_log_snap_store:lookup(N, <<"c">>)),
    ok = ra_log_snap_store:release(N, <<"c">>, 10),
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

recovery_corrupt_record_ignores_rest_of_file(Config) ->
    %% an invalid record in the middle of a file: the records before it are
    %% kept, the rest of that file is ignored and the store still starts
    N = start(Config, #{}),
    E = <<"e">>,
    Img = image(100),
    ok = ra_log_snap_store:put(N, <<"a">>, E, {1, 1}, Img, []),
    ok = ra_log_snap_store:put(N, <<"b">>, E, {1, 1}, image(100), []),
    ok = ra_log_snap_store:put(N, <<"c">>, E, {1, 1}, image(100), []),
    %% every put was its own 4KB aligned batch: a at 64, b at 4096, c at 8192
    ok = ra_log_snap_store:stop(N),
    [File] = filelib:wildcard(filename:join(?config(store_dir, Config),
                                            "*.snap")),
    {ok, Fd} = file:open(File, [read, write, raw, binary]),
    ok = file:pwrite(Fd, 4096 + 9 + 20, <<255>>),
    ok = file:close(Fd),
    N = start(Config, #{}),
    {ok, Img, []} = ra_log_snap_store:read(N, <<"a">>, {1, 1}),
    ?assertEqual(not_found, ra_log_snap_store:lookup(N, <<"b">>)),
    ?assertEqual(not_found, ra_log_snap_store:lookup(N, <<"c">>)),
    %% and it keeps accepting writes
    Img2 = image(100),
    ok = ra_log_snap_store:put(N, <<"b">>, E, {2, 1}, Img2, []),
    {ok, Img2, []} = ra_log_snap_store:read(N, <<"b">>, {2, 1}),
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
    %% store) until it works again
    N = start(Config, #{io => io_with_faults(),
                        min_file_bytes => 1}),
    E = <<"e">>,
    ok = ra_log_snap_store:put(N, <<"a">>, E, {1, 1}, image(100), []),
    inject_failure(create),
    %% this put rolls the file (min size is tiny) and the roll fails
    ok = ra_log_snap_store:put(N, <<"a">>, E, {2, 1}, image(100), []),
    ?assertMatch({error, _},
                 ra_log_snap_store:put(N, <<"a">>, E, {3, 1}, image(100), [])),
    ?assertMatch({ok, #{idx := 2}}, ra_log_snap_store:lookup(N, <<"a">>)),
    clear_failure(),
    Img = image(100),
    ok = ra_log_snap_store:put(N, <<"a">>, E, {4, 1}, Img, []),
    {ok, Img, []} = ra_log_snap_store:read(N, <<"a">>, {4, 1}),
    ok.

%%%===================================================================
%%% Helpers
%%%===================================================================

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
    persistent_term:put({?MODULE, fail}, What).

clear_failure() ->
    persistent_term:erase({?MODULE, fail}).

failing(What) ->
    persistent_term:get({?MODULE, fail}, undefined) =:= What.
