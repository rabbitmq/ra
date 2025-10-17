%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2017-2025 Broadcom. All Rights Reserved. The term Broadcom refers to Broadcom Inc. and/or its subsidiaries.
%%
-module(ra_counters).
-include("ra.hrl").

-export([
         init/1,
         new/3,
         new/4,
         fetch/2,
         overview/1,
         overview/2,
         counters/3,
         delete/2
         ]).

-type name() :: term().


-spec init(atom()) -> ok.
init(Group) ->
    _ = application:ensure_all_started(seshat),
    _ = seshat:new_group(Group),
    persistent_term:put(?FIELDSPEC_KEY, ?RA_COUNTER_FIELDS),
    ok.

-spec new(atom(), name(), seshat:fields_spec()) ->
    counters:counters_ref().
new(Group, Name, FieldsSpec) ->
    seshat:new(Group, Name, FieldsSpec).

new(Group, Name, FieldsSpec, MetricLabels) ->
    seshat:new(Group, Name, FieldsSpec, MetricLabels).

-spec fetch(atom(), name()) -> undefined | counters:counters_ref().
fetch(Group, Name) ->
    seshat:fetch(Group, Name).

-spec delete(atom(), term()) -> ok.
delete(Group, Name) ->
    seshat:delete(Group, Name).

-spec overview(atom()) -> #{name() => #{atom() => non_neg_integer()}}.
overview(Group) ->
    seshat:counters(Group).

-spec overview(atom(), name()) -> #{atom() => non_neg_integer()} | undefined.
overview(Group, Name) ->
    seshat:counters(Group, Name).

-spec counters(atom(), name(), [atom()]) ->
    #{atom() => non_neg_integer()} | undefined.
counters(Group, Name, Fields) ->
    seshat:counters(Group, Name, Fields).
