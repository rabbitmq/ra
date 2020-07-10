%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2017-2020 VMware, Inc. or its affiliates.  All rights reserved.
%%
%% @hidden
-module(ra_log_sup).

-behaviour(supervisor).

%% API functions
-export([start_link/1]).

%% Supervisor callbacks
-export([init/1]).

-spec start_link(DataDir :: file:filename()) ->
    {ok, pid()} | ignore | {error, term()}.
start_link(DataDir) ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, [DataDir]).

init([DataDir]) ->
    PreInit = #{id => ra_log_pre_init,
                start => {ra_log_pre_init, start_link, [DataDir]}},
    Meta = #{id => ra_log_meta,
             start => {ra_log_meta, start_link, [DataDir]}},
    SegmentMaxEntries = application:get_env(ra, segment_max_entries, 4096),
    SegWriterConf = #{data_dir => DataDir,
                      segment_conf => #{max_count => SegmentMaxEntries}},
    SegWriter = #{id => ra_log_segment_writer,
                  start => {ra_log_segment_writer, start_link,
                            [SegWriterConf]}},
    WalConf = #{dir => ra_env:wal_data_dir()},
    SupFlags = #{strategy => one_for_all, intensity => 5, period => 5},
    WalSup = #{id => ra_log_wal_sup,
               type => supervisor,
               start => {ra_log_wal_sup, start_link, [WalConf]}},
    {ok, {SupFlags, [PreInit, Meta, SegWriter, WalSup]}}.
