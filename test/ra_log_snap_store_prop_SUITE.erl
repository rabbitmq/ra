%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2017-2025 Broadcom. All Rights Reserved. The term Broadcom refers to Broadcom Inc. and/or its subsidiaries.
%%
%% Model based and concurrency tests of the snapshot log. The log is run
%% against a simple model of what has been acknowledged, with random
%% sequences of puts (including stale, repeated and ones that hit injected
%% write and fsync errors), restarts, deletes and the file rolling and
%% retiring that goes on in the background because the files are tiny.
%% NB: proper must be included before other libraries that define ?LET
-module(ra_log_snap_store_prop_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("proper/include/proper.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(UIDS, 4).
-define(EPOCH, <<"e">>).

all() ->
    [
     model,
     readers_during_rolls_and_retires
    ].

init_per_testcase(TestCase, Config) ->
    Dir = filename:join([?config(priv_dir, Config), TestCase]),
    persistent_term:erase({?MODULE, fail}),
    [{base_dir, Dir} | Config].

end_per_testcase(_TestCase, _Config) ->
    persistent_term:erase({?MODULE, fail}),
    ok.

%%%===================================================================
%%% Test cases
%%%===================================================================

model(Config) ->
    Base = ?config(base_dir, Config),
    Counter = counters:new(1, []),
    Prop = ?FORALL(Ops, list(op()),
                   begin
                       counters:add(Counter, 1, 1),
                       Run = counters:get(Counter, 1),
                       Dir = filename:join(Base, integer_to_list(Run)),
                       Name = list_to_atom("prop_store_" ++ integer_to_list(Run)),
                       _ = file:del_dir_r(Dir),
                       try
                           run(Name, Dir, Ops)
                       after
                           catch ra_log_snap_store:stop(Name),
                           persistent_term:erase({?MODULE, fail})
                       end
                   end),
    ?assertEqual(true,
                 proper:counterexample(Prop,
                                       [{numtests, 60},
                                        {on_output,
                                         fun(".", _) -> ok;
                                            (F, A) -> ct:pal(?LOW_IMPORTANCE, F, A)
                                         end}])),
    ok.

%% A writer replaces the snapshots of a few members as fast as it can whilst
%% the files roll and are retired (tiny files), and readers keep reading the
%% snapshot the store says is current. A read may find that it was superseded,
%% it must never fail or return the wrong bytes.
readers_during_rolls_and_retires(Config) ->
    Dir = ?config(base_dir, Config),
    Name = prop_store_readers,
    {ok, _} = start(Name, Dir),
    UIds = [uid(I) || I <- lists:seq(1, ?UIDS)],
    %% every member has a snapshot to start with
    [ok = ra_log_snap_store:put(Name, U, ?EPOCH, {1, 1}, image(U, 1, 800), [])
     || U <- UIds],
    Self = self(),
    Stop = erlang:monotonic_time(millisecond) + 3000,
    _ = spawn_link(
               fun () ->
                       writer_loop(Name, UIds, 2, Stop),
                       Self ! writer_done
               end),
    Readers = [spawn_link(fun () -> Self ! {reader_done, reader_loop(Name, U, Stop, 0, 0)} end)
               || U <- UIds],
    receive writer_done -> ok after 20000 -> ct:fail(writer_timeout) end,
    Results = [receive {reader_done, R} -> R after 20000 -> ct:fail(reader_timeout) end
               || _ <- Readers],
    ct:pal("reads ok/superseded per reader: ~p", [Results]),
    [?assertMatch({_, _}, R) || R <- Results],
    ?assert(lists:sum([Ok || {Ok, _} <- Results]) > 100),
    #{retired_files := Retired} = ra_log_snap_store:info(Name),
    ?assert(Retired > 2),
    ok = ra_log_snap_store:stop(Name),
    ok.

%%%===================================================================
%%% model
%%%===================================================================

op() ->
    frequency([{12, {put, range(1, ?UIDS), range(-1, 3), range(10, 3000),
                     oneof([none, none, none, pwrite, sync])}},
               {2, restart},
               {1, wait},
               {1, {release, range(1, ?UIDS)}},
               {1, {delete, range(1, ?UIDS)}}]).

%% model: UId => {Idx, Image} for what has been acknowledged. unsure: members
%% whose entry was dropped by something that is not durable (release, delete)
%% so may or may not be there after a restart.
-record(m, {name, dir, model = #{}, unsure = #{}, n = 0}).

run(Name, Dir, Ops) ->
    {ok, _} = start(Name, Dir),
    #m{} = lists:foldl(fun (Op, M) -> step(Op, check(M)) end,
                       #m{name = Name, dir = Dir}, Ops),
    true.

step({put, I, Delta, Size, Fault}, #m{name = Name, model = Model,
                                      unsure = Unsure, n = N} = M) ->
    UId = uid(I),
    Known = case Model of
                #{UId := {Idx0, _}} -> Idx0;
                _ -> -1
            end,
    %% a member that may have an entry we do not know about (and must be
    %% allowed to replace) starts again well above anything before
    Idx = case Unsure of
              #{UId := _} -> 100000 + N;
              _ -> max(0, Known + Delta)
          end,
    Image = image(UId, Idx, Size),
    Fault == none orelse inject_failure(Fault),
    Res = ra_log_snap_store:put(Name, UId, ?EPOCH, {Idx, 1}, Image, []),
    clear_failure(),
    case Res of
        ok ->
            %% repeating the current snapshot is a no-op, otherwise it has to
            %% be newer (or the member has nothing we know of)
            case Model of
                #{UId := {Known, _}} when Idx == Known ->
                    M#m{n = N + 1};
                _ when Idx > Known orelse is_map_key(UId, Unsure) ->
                    M#m{model = Model#{UId => {Idx, Image}},
                        unsure = maps:remove(UId, Unsure), n = N + 1}
            end;
        {error, stale} ->
            true = Idx =< Known,
            M#m{n = N + 1};
        {error, _} ->
            %% not acknowledged: whatever was there stays
            M#m{n = N + 1}
    end;
step(restart, #m{name = Name, dir = Dir} = M) ->
    ok = ra_log_snap_store:stop(Name),
    {ok, _} = start(Name, Dir),
    M;
step(wait, #m{name = Name} = M) ->
    wait_quiescent(Name),
    M;
step({release, I}, #m{name = Name, model = Model, unsure = Unsure} = M) ->
    UId = uid(I),
    case Model of
        #{UId := {Idx, _}} ->
            ok = ra_log_snap_store:release(Name, UId, {Idx, 1}),
            _ = ra_log_snap_store:info(Name),
            M#m{model = maps:remove(UId, Model),
                unsure = Unsure#{UId => true}};
        _ ->
            M
    end;
step({delete, I}, #m{name = Name, model = Model, unsure = Unsure} = M) ->
    UId = uid(I),
    ok = ra_log_snap_store:delete(Name, UId, any),
    M#m{model = maps:remove(UId, Model), unsure = Unsure#{UId => true}}.

%% what the store says must match what has been acknowledged
check(#m{name = Name, model = Model} = M) ->
    maps:foreach(
      fun (UId, {Idx, Image}) ->
              case ra_log_snap_store:lookup(Name, UId) of
                  {ok, #{idx := Idx}} -> ok;
                  Other -> exit({wrong_entry, UId, Idx, Other})
              end,
              case read(Name, UId, {Idx, 1}) of
                  {ok, Image, []} -> ok;
                  Other2 -> exit({wrong_read, UId, Idx, Other2})
              end
      end, Model),
    M.

%%%===================================================================
%%% helpers
%%%===================================================================

start(Name, Dir) ->
    %% four blocks is the smallest a file can be, so they roll and retire all
    %% the time
    ra_log_snap_store:start_link(#{name => Name,
                                   dir => Dir,
                                   min_file_bytes => 16384,
                                   retire_chunk_bytes => 4096,
                                   io => io_with_faults()}).

uid(I) ->
    <<"uid", (integer_to_binary(I))/binary>>.

%% the contents are a function of the member and the index so that a reader
%% can tell if it got the wrong snapshot
image(UId, Idx, Size) ->
    Seed = <<UId/binary, Idx:64>>,
    binary:part(binary:copy(Seed, Size div byte_size(Seed) + 1), 0, Size).

%% reads, retrying when the answer is that the store is not there
read(Name, UId, IdxTerm) ->
    ra_log_snap_store:read(Name, UId, IdxTerm).

wait_quiescent(Name) ->
    wait_quiescent(Name, 400).

wait_quiescent(Name, 0) ->
    exit({never_quiescent, ra_log_snap_store:info(Name)});
wait_quiescent(Name, N) ->
    case ra_log_snap_store:info(Name) of
        #{rolled_files := 0, retiring := false} ->
            ok;
        #{has_active_file := false} ->
            %% nowhere to copy to until a put makes a file
            ok;
        _ ->
            timer:sleep(10),
            wait_quiescent(Name, N - 1)
    end.

writer_loop(Name, UIds, Round, Stop) ->
    case erlang:monotonic_time(millisecond) >= Stop of
        true ->
            ok;
        false ->
            [ok = ra_log_snap_store:put(Name, U, ?EPOCH, {Round, 1},
                                        image(U, Round, 800), [])
             || U <- UIds],
            writer_loop(Name, UIds, Round + 1, Stop)
    end.

reader_loop(Name, UId, Stop, Ok, Superseded) ->
    case erlang:monotonic_time(millisecond) >= Stop of
        true ->
            {Ok, Superseded};
        false ->
            {ok, #{idx := Idx}} = ra_log_snap_store:lookup(Name, UId),
            case ra_log_snap_store:read(Name, UId, {Idx, 1}) of
                {ok, Image, []} ->
                    case image(UId, Idx, 800) of
                        Image ->
                            reader_loop(Name, UId, Stop, Ok + 1, Superseded);
                        _ ->
                            exit({wrong_bytes, UId, Idx})
                    end;
                {error, superseded} ->
                    reader_loop(Name, UId, Stop, Ok, Superseded + 1);
                Other ->
                    exit({bad_read, UId, Idx, Other})
            end
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
              end}.

inject_failure(What) ->
    persistent_term:put({?MODULE, fail}, What).

clear_failure() ->
    persistent_term:erase({?MODULE, fail}).

failing(What) ->
    persistent_term:get({?MODULE, fail}, undefined) =:= What.
