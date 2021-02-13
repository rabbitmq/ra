%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2017-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%
-module(ra_env).

-export([
         data_dir/0,
         server_data_dir/1,
         wal_data_dir/0,
         configure_logger/1
         ]).

-export_type([
              ]).

data_dir() ->
    DataDir = case application:get_env(ra, data_dir) of
                  {ok, Dir} ->
                      Dir;
                  undefined ->
                      {ok, Cwd} = file:get_cwd(),
                      Cwd
              end,
    Node = ra_lib:to_list(node()),
    filename:join(DataDir, Node).

server_data_dir(UId) ->
    Me = ra_lib:to_list(UId),
    filename:join(data_dir(), Me).

wal_data_dir() ->
    %% allows the wal director to be overridden or fall back to the default
    %% data directory
    case application:get_env(ra, wal_data_dir) of
        {ok, Dir} -> Dir;
        _ ->
            data_dir()
    end.

%% use this when interacting with Ra from a node without Ra running on it
configure_logger(Module) ->
    persistent_term:put('$ra_logger', Module).
