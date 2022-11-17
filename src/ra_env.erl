%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2017-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%
-module(ra_env).

-export([
         data_dir/0,
         server_data_dir/2,
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
    Node = ra_lib:to_binary(node()),
    filename:join(ra_lib:to_binary(DataDir), Node).

server_data_dir(System, UId) when is_atom(System) ->
    #{data_dir := Dir} = ra_system:fetch(System),
    filename:join(Dir, UId).

%% use this when interacting with Ra from a node without Ra running on it
configure_logger(Module) ->
    persistent_term:put('$ra_logger', Module).
