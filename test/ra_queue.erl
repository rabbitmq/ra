%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2017-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%
-module(ra_queue).

-behaviour(ra_machine).

-export([
         init/1,
         apply/3,
         state_enter/2,
         tick/2,
         overview/1
        ]).
init(_) -> [].

apply(#{index := Idx}, {enq, Msg}, State) ->
    {State ++ [{Idx, Msg}], ok};
apply(_Meta, {deq, ToPid}, [{EncIdx, Msg} | State]) ->
    {State, ok, [{send_msg, ToPid, Msg},
                 {release_cursor, EncIdx, []}]};
apply(_Meta, deq, [{EncIdx, Msg} | State]) ->
    {State, Msg, {release_cursor, EncIdx, State}};
apply(_Meta, _, [] = State) ->
    {State, ok}.


state_enter(_, _) -> [].

tick(_, _) -> [].


overview(_) -> #{}.
