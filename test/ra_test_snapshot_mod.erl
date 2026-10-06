%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2017-2025 Broadcom. All Rights Reserved. The term Broadcom refers to Broadcom Inc. and/or its subsidiaries.
%%
%% A snapshot module for tests whose prepare/2 returns something other than the
%% machine state. It writes the snapshots with ra_log_snapshot.
-module(ra_test_snapshot_mod).

-behaviour(ra_snapshot).

-export([prepare/2,
         write/4,
         sync/1,
         begin_accept/2,
         accept_chunk/2,
         complete_accept/2,
         begin_read/2,
         read_chunk/3,
         recover/1,
         validate/1,
         read_meta/1,
         context/0,
         get_size/1]).

prepare(_Meta, State) ->
    {prepared, State}.

write(Dir, Meta, {prepared, State}, Sync) ->
    ra_log_snapshot:write(Dir, Meta, State, Sync).

sync(Dir) -> ra_log_snapshot:sync(Dir).
begin_accept(Dir, Meta) -> ra_log_snapshot:begin_accept(Dir, Meta).
accept_chunk(Chunk, St) -> ra_log_snapshot:accept_chunk(Chunk, St).
complete_accept(Chunk, St) -> ra_log_snapshot:complete_accept(Chunk, St).
begin_read(Dir, Ctx) -> ra_log_snapshot:begin_read(Dir, Ctx).
read_chunk(RS, Size, Dir) -> ra_log_snapshot:read_chunk(RS, Size, Dir).
recover(Dir) -> ra_log_snapshot:recover(Dir).
validate(Dir) -> ra_log_snapshot:validate(Dir).
read_meta(Dir) -> ra_log_snapshot:read_meta(Dir).
context() -> ra_log_snapshot:context().
get_size(Dir) -> ra_log_snapshot:get_size(Dir).
