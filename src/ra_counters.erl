%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term Broadcom refers to Broadcom Inc. and/or its subsidiaries.
%%
-module(ra_counters).
-include("ra.hrl").

-export([
         init/0,
         new/2,
         new/3,
         fetch/1,
         overview/0,
         overview/1,
         counters/2,
         delete/1
         ]).

-type name() :: term().


-spec init() -> ok.
init() ->
    _ = application:ensure_all_started(seshat),
    _ = seshat:new_group(ra),
    persistent_term:put(?FIELDSPEC_KEY, ?RA_COUNTER_FIELDS),
    ok.

-spec new(name(), seshat:fields_spec()) ->
    counters:counters_ref().
new(Name, FieldsSpec) ->
    seshat:new(ra, Name, FieldsSpec).

-spec new(name(), seshat:fields_spec(), seshat:labels_map()) ->
    counters:counters_ref().
new(Name, FieldsSpec, MetricLabels) ->
    seshat:new(ra, Name, FieldsSpec, MetricLabels).

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

-spec counters(name(), [atom()]) ->
    #{atom() => non_neg_integer()} | undefined.
counters(Name, Fields) ->
    seshat:counters(ra, Name, Fields).
