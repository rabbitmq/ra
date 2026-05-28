%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2017-2025 Broadcom. All Rights Reserved. The term Broadcom refers to Broadcom Inc. and/or its subsidiaries.
%%
-module(ra_env).

-export([
         data_dir/0,
         server_data_dir/2,
         configure_logger/1,
         logger_mod/0,
         log/1
         ]).

-export_type([
              ]).

-spec data_dir() -> file:filename_all().
data_dir() ->
    DataDir = case application:get_env(ra, data_dir) of
                  {ok, Dir} when is_list(Dir) orelse
                                 is_binary(Dir) ->
                      Dir;
                  undefined ->
                      {ok, Cwd} = file:get_cwd(),
                      Cwd
              end,
    Node = ra_lib:to_list(node()),
    filename:join(DataDir, Node).

server_data_dir(System, UId) when is_atom(System) ->
    #{data_dir := Dir} = ra_system:fetch(System),
    Me = ra_lib:to_list(UId),
    filename:join(Dir, Me).

%% use this when interacting with Ra from a node without Ra running on it
configure_logger(Module) when is_atom(Module) ->
    persistent_term:put('$ra_logger', Module).

-spec logger_mod() -> module().
logger_mod() ->
    case persistent_term:get('$ra_logger', undefined) of
        M when is_atom(M) ->
            M;
        undefined ->
            ?MODULE
    end.

%% dummy log function
log(_) -> ok.
