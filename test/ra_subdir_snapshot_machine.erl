%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2017-2025 Broadcom. All Rights Reserved. The term Broadcom refers to Broadcom Inc. and/or its subsidiaries.
%%
%% @hidden
%% A counter machine that declares its own snapshot module via the
%% ra_machine:snapshot_module/0 callback. The snapshot module wraps
%% ra_log_snapshot but stores the snapshot in a subdirectory so that the
%% default snapshot module cannot read it.
-module(ra_subdir_snapshot_machine).

-behaviour(ra_machine).
-behaviour(ra_snapshot).

%% ra_machine
-export([init/1,
         apply/3,
         snapshot_module/0]).

%% ra_snapshot
-export([prepare/2,
         write/4,
         sync/1,
         begin_read/2,
         read_chunk/3,
         begin_accept/2,
         accept_chunk/2,
         complete_accept/2,
         recover/1,
         validate/1,
         read_meta/1,
         context/0,
         get_size/1]).

-define(SUB, "sub").

init(#{release_cursor_interval := Interval}) ->
    {0, Interval}.

apply(#{index := Idx}, incr, {N0, Interval}) ->
    N = N0 + 1,
    State = {N, Interval},
    case Idx rem Interval of
        0 ->
            {State, N, [{release_cursor, Idx, State}]};
        _ ->
            {State, N}
    end.

snapshot_module() ->
    ?MODULE.

%% ra_snapshot

prepare(Idx, State) ->
    ra_log_snapshot:prepare(Idx, State).

write(Dir, Meta, Ref, Sync) ->
    Sub = sub(Dir),
    ok = ra_lib:make_dir(Sub),
    ra_log_snapshot:write(Sub, Meta, Ref, Sync).

sync(Dir) ->
    ra_log_snapshot:sync(sub(Dir)).

begin_read(Dir, Context) ->
    ra_log_snapshot:begin_read(sub(Dir), Context).

read_chunk(ReadState, Size, Dir) ->
    ra_log_snapshot:read_chunk(ReadState, Size, sub(Dir)).

begin_accept(Dir, Meta) ->
    Sub = sub(Dir),
    ok = ra_lib:make_dir(Sub),
    ra_log_snapshot:begin_accept(Sub, Meta).

accept_chunk(Chunk, State) ->
    ra_log_snapshot:accept_chunk(Chunk, State).

complete_accept(Chunk, State) ->
    ra_log_snapshot:complete_accept(Chunk, State).

recover(Dir) ->
    ra_log_snapshot:recover(sub(Dir)).

validate(Dir) ->
    ra_log_snapshot:validate(sub(Dir)).

read_meta(Dir) ->
    ra_log_snapshot:read_meta(sub(Dir)).

context() ->
    ra_log_snapshot:context().

get_size(Dir) ->
    ra_log_snapshot:get_size(sub(Dir)).

sub(Dir) ->
    filename:join(Dir, ?SUB).
