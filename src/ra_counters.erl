%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2017-2021 VMware, Inc. or its affiliates.  All rights reserved.
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

%% holds static or rarely changing fields
-record(cfg, {}).

-record(?MODULE, {cfg :: #cfg{}}).

-opaque state() :: #?MODULE{}.
-type name() :: term().

-export_type([
              state/0
              ]).

-spec init() -> ok.
init() ->
    _ = ets:new(?MODULE, [set, named_table, public]),
    ok.

-spec new(name(),  [atom()]) -> counters:counters_ref().
new(Name, Fields)
  when is_list(Fields) ->
    Size = length(Fields),
    CRef = counters:new(Size, []),
    ok = register_counter(Name, CRef, Fields),
    CRef.

-spec fetch(name()) -> undefined | counters:counters_ref().
fetch(Name) ->
    case ets:lookup(?MODULE, Name) of
        [{Name, Ref, _}] ->
            Ref;
        _ ->
            undefined
    end.

-spec delete(term()) -> ok.
delete(Name) ->
    true = ets:delete(?MODULE, Name),
    ok.

-spec overview() -> #{name() => #{atom() => non_neg_integer()}}.
overview() ->
    ets:foldl(
      fun({Name, Ref, Fields}, Acc) ->
              Size = length(Fields),
              Values = [counters:get(Ref, I) || I <- lists:seq(1, Size)],
              Counters = maps:from_list(lists:zip(Fields, Values)),
              Acc#{Name => Counters}
      end, #{}, ?MODULE).

-spec overview(name()) -> #{atom() => non_neg_integer()}.
overview(Name) ->
    case ets:lookup(?MODULE, Name) of
        [{Name, Ref, Fields}] ->
              Size = length(Fields),
              Values = [counters:get(Ref, I) || I <- lists:seq(1, Size)],
              maps:from_list(lists:zip(Fields, Values));
        _ ->
            undefined
    end.


%% internal

register_counter(Name, Ref, Size) ->
    true = ets:insert(?MODULE, {Name, Ref, Size}),
    ok.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.
