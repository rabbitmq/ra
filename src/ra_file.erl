%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term Broadcom refers to Broadcom Inc. and/or its subsidiaries.
%%
%% @hidden
-module(ra_file).

-include("ra.hrl").

-define(RETRY_ON_ERROR(Op),
    case Op of
        {error, E} when E =:= eagain orelse E =:= eacces ->
            ?DEBUG("Error `~p` during file operation, retrying once in 20ms...", [E]),
            timer:sleep(20),
            case Op of
                {error, eagain} = Err ->
                    ?DEBUG("Error `~p` again during file operation", [E]),
                    Err;
                Res ->
                    Res
            end;
        Res ->
            Res
    end).

-export([
         sync/1,
         rename/2
        ]).

sync(Fd) ->
    ?RETRY_ON_ERROR(file:sync(Fd)).

rename(Src, Dst) ->
    ?RETRY_ON_ERROR(prim_file:rename(Src, Dst)).
