%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2017-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%
-module(ra_counters).

-export([
         init/0,
         new/2,
         fetch/1,
         overview/0,
         overview/1,
         delete/1
         ]).

-type name() :: term().
-type seshat_field_spec() ::
    {Name :: atom(), Position :: pos_integer(),
     Type :: counter | gauge, Description :: string()}.

-spec init() -> ok.
init() ->
    _ = application:ensure_all_started(seshat),
    _ = seshat:new_group(ra),
    ok.

-spec new(name(),  [seshat_field_spec()]) ->
    counters:counters_ref().
new(Name, Fields)
  when is_list(Fields) ->
    seshat:new(ra, Name, Fields).

-spec fetch(name()) -> undefined | counters:counters_ref().
fetch(Name) ->
    seshat:fetch(ra, Name).

-spec delete(term()) -> ok.
delete(Name) ->
    seshat:delete(ra, Name).

-spec overview() -> #{name() => #{atom() => non_neg_integer()}}.
overview() ->
    seshat:overview(ra).

-spec overview(name()) -> #{atom() => non_neg_integer()}.
overview(Name) ->
    seshat:overview(ra, Name).
