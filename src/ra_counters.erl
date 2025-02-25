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
         new/4,
         new/3,
         fetch/2,
         overview/0,
         overview/1,
         overview/2,
         counters/3,
         delete/2
         ]).

-type name() :: term().


-spec init(atom()) -> ok.
init(System) ->
    _ = application:ensure_all_started(seshat),
    _ = seshat:new_group(System),
    persistent_term:put(?FIELDSPEC_KEY, ?RA_COUNTER_FIELDS),
    ok.

-spec new(atom(), name(), seshat:fields_spec()) ->
    counters:counters_ref().
new(System, Name, FieldsSpec) ->
    seshat:new(System, Name, FieldsSpec).

new(System, Name, FieldsSpec, MetricLabels) ->
    seshat:new(System, Name, FieldsSpec, MetricLabels).

-spec fetch(atom(), name()) -> undefined | counters:counters_ref().
fetch(System, Name) ->
    seshat:fetch(System, Name).

-spec delete(atom(), term()) -> ok.
delete(System, Name) ->
    seshat:delete(System, Name).

-spec overview() -> #{name() => #{atom() => non_neg_integer()}}.
overview() ->
    seshat:counters(quorum_queues).

-spec overview(atom()) -> #{atom() => non_neg_integer()} | undefined.
overview(System) ->
    seshat:counters(System).

-spec overview(atom(), name()) -> #{atom() => non_neg_integer()} | undefined.
overview(System, Name) ->
    seshat:counters(System, Name).

-spec counters(atom(), name(), [atom()]) ->
    #{atom() => non_neg_integer()} | undefined.
counters(System, Name, Fields) ->
    seshat:counters(System, Name, Fields).
