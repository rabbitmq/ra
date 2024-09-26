%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term Broadcom refers to Broadcom Inc. and/or its subsidiaries.
%%
%% @hidden
-module(ra_range).

-export([
         new/1,
         new/2
        ]).


-type range() :: undefined | {ra:index(), ra:index()}.

-export_type([range/0]).

-spec new(ra:index()) -> range().
new(Start) when is_integer(Start) ->
    {Start, Start}.

-spec new(ra:index(), ra:index()) -> range().
new(Start, End)
  when is_integer(Start) andalso
       is_integer(End) andalso
       Start =< End ->
    {Start, End}.
