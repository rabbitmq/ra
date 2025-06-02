%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term Broadcom refers to Broadcom Inc. and/or its subsidiaries.
%%
-module(ra_log_read_plan).


-export([execute/2,
         execute/3,
         info/1]).

-spec execute(ra_log:read_plan(), undefined | ra_flru:state()) ->
    {#{ra:index() => Command :: term()}, ra_flru:state()}.
execute(Plan, Flru) ->
    execute(Plan, Flru, #{access_pattern => random,
                          file_advise => normal}).

-spec execute(ra_log:read_plan(), undefined | ra_flru:state(),
              ra_log_segments:read_plan_options()) ->
    {#{ra:index() => Command :: term()}, ra_flru:state()}.
execute(Plan, Flru, Options) ->
    ra_log:execute_read_plan(Plan, Flru,
                             fun ra_server:transform_for_partial_read/3,
                             Options).

-spec info(ra_log:read_plan()) -> map().
info(Plan) ->
    ra_log:read_plan_info(Plan).
