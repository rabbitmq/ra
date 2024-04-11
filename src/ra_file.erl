%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term Broadcom refers to Broadcom Inc. and/or its subsidiaries.
%%
%% @hidden
-module(ra_file).

-include("ra.hrl").

-define(HANDLE_EAGAIN(Op),
    case Op of
        {error, eagain} ->
            ?INFO("EAGAIN during file operation, retrying once in 10ms...", []),
            timer:sleep(10),
            case Op of
                {error, eagain} = Err ->
                    ?INFO("EAGAIN again during file operation", []),
                    Err;
                Res ->
                    Res
            end;
        Res ->
            Res
    end).

-export([
         % open/1,
         % write/2,
         sync/1
        ]).

sync(Fd) ->
    ?HANDLE_EAGAIN(file:sync(Fd)).
