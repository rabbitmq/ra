
%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term Broadcom refers to Broadcom Inc. and/or its subsidiaries.
%%
%% @hidden

-module(ra_blob_store).
-behaviour(gen_batch_server).

-export([start_link/1,
         init/1,
         handle_batch/2,
         terminate/2,
         format_status/1
        ]).

-include("ra.hrl").

%% per ra server blob store for payload binary data
%%
%% each blob is written to a fixed max size segment file,
%% The name of each segment file is the first index written to the file + 0-padding.
%%
%% ra_blobc is the module implemnting the client API
%%
%% Write path:
%% 1. ra_blobc writes to the shared ETS table of all blobs, including the payload.
%%  the key is the ra_index of the log entry
%% 2. ra_blobc sends a small message to ra_blob_store with the index just written
%% 3. ra_blob_store reads from the ETS table, writes the payload to a segment file
%% 4. ra_blob_store replaces the ETS field with the binary data with a locator
%% reference: {FileNameIndex, Pos, Size}
%% 5. ra_blob_store updates a shared (with any client that init a session)
%% atomic value of key information
%% in this case the last written item (it will also maintain the smallest available
%% index which will allow easy range checks).
%% ra_log will use this shared atomic value to determine it's actual last written index.
%% the assumption is that due to the ra_blob_store not fsyncing it will be faster
%% than the proper fsynced log itself. If the WAL written event comes in and
%% the ra_blob shared value is lower it can then register for an async update
%% when a certain index has been written.
%%


-record(?MODULE, {}).

-opaque state() :: #?MODULE{}.

-type config() :: #{data_dir := filename:all()}.

-export_type([state/0]).

-spec start_link(config()) ->
    {ok, pid()} | {error, {already_started, pid()}}.
start_link(#{} = Cfg) ->
    gen_batch_server:start_link(?MODULE, Cfg, []).

-spec init(config()) -> {ok, state()}.
init(#{data_dir := Dir}) ->
    process_flag(trap_exit, true),
    ok = ra_lib:make_dir(Dir),
    _MetaFile = filename:join(Dir, "meta.dets"),
    % _ = ets:new(TblName, [named_table, public, {read_concurrency, true}]),
    {ok, #?MODULE{}}.

handle_batch(_Commands, #?MODULE{} = State) ->
    {ok, [], State}.

terminate(_, #?MODULE{}) ->
    ?DEBUG("ra: blob store store is terminating", []),
    ok.

format_status(State) ->
    State.
