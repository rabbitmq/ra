%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term Broadcom refers to Broadcom Inc. and/or its subsidiaries.
%%
-module(ra_counters).
-include("ra.hrl").

-export([
         init/1,
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
init(Namespace) ->
    _ = application:ensure_all_started(seshat),
    _ = seshat:new_group(Namespace),
    persistent_term:put(?FIELDSPEC_KEY, ?RA_COUNTER_FIELDS),
    ok.

-spec new(atom(), name(), seshat:fields_spec()) ->
    counters:counters_ref().
new(Namespace, Name, FieldsSpec) ->
    seshat:new(Namespace, Name, FieldsSpec).

-spec fetch(atom(), name()) -> undefined | counters:counters_ref().
fetch(Namespace, Name) ->
    seshat:fetch(Namespace, Name).

-spec delete(atom(), term()) -> ok.
delete(Namespace, Name) ->
    seshat:delete(Namespace, Name).

-spec overview() -> #{name() => #{atom() => non_neg_integer()}}.
overview() ->
    %% TODO - this should return counters for all systems
    seshat:overview(quorum_queues).

-spec overview(atom()) -> #{seshat:name() => #{atom() => non_neg_integer()}}.
overview(Namespace) ->
    seshat:overview(Namespace).

-spec overview(atom(), name()) -> undefined | #{seshat:name() => integer()}.
overview(Namespace, Name) ->
    seshat:overview(Namespace, Name).

-spec counters(atom(), name(), [atom()]) ->
    #{seshat:name() => non_neg_integer()} | undefined.
counters(Namespace, Name, Fields) ->
    seshat:counters(Namespace, Name, Fields).
