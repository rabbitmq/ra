%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2017-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%
%% @hidden
-module(ra_machine_simple).
-behaviour(ra_machine).

-export([
         init/1,
         apply/3
         ]).

init(#{simple_fun := Fun,
       initial_state := Initial}) ->
    {simple, Fun, Initial}.

apply(_, Cmd, {simple, Fun, State}) ->
    Next = Fun(Cmd, State),
    %% return the next state as the reply as well
    {{simple, Fun, Next}, Next}.


