%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2017-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%
%% @hidden
-module(ra_flru).

%% small fixed size simple lru cache
%% inefficient on larger sizes

-export([
         new/2,
         fetch/2,
         insert/3,
         evict/2,
         evict_all/1,
         size/1
         ]).

-define(MAX_SIZE, 5).

-type kv_item() :: {Key :: term(), Value :: term()}.

-type handler_fun() :: fun((kv_item()) -> ok).

-record(?MODULE, {max_size = ?MAX_SIZE :: non_neg_integer(),
                  items = [] :: [term()],
                  handler = fun (_) -> ok end :: handler_fun()}).

-opaque state() :: #?MODULE{}.

-export_type([
              state/0
              ]).

-spec new(non_neg_integer(), handler_fun()) ->
     state().
new(MaxSize, Handler) ->
    #?MODULE{handler = Handler,
             max_size = MaxSize}.

-spec fetch(term(), state()) ->
    {ok, term(), state()} | error.
fetch(Key, #?MODULE{items = [{Key, Value} | _]} = State) ->
    %% head optimisation
    {ok, Value, State};
fetch(Key, #?MODULE{items = Items0} = State0) ->
    case lists:keytake(Key, 1, Items0) of
        {value, {_, Value} = T, Items} ->
            {ok, Value, State0#?MODULE{items = [T | Items]}};
        false ->
            error
    end.

-spec insert(term(), term(), state()) -> state().
insert(Key, Value, #?MODULE{items = Items,
                            max_size = M,
                            handler = Handler} = State)
  when length(Items) =:= M ->
    %% cache is full, discard last item
    [Old | Rem] = lists:reverse(Items),
    %% call the handler
    ok = Handler(Old),
    State#?MODULE{items = [{Key, Value} | lists:reverse(Rem)]};
insert(Key, Value, #?MODULE{items = Items} = State) ->
    %% else just append it
    State#?MODULE{items = [{Key, Value} | Items]}.

-spec evict(Key :: term(), state()) ->
    {Evicted :: kv_item(), state()} | error.
evict(Key, #?MODULE{items = Items0,
                    handler = Handler} = State) ->
    case lists:keytake(Key, 1, Items0) of
        {value, T, Items} ->
            ok = Handler(T),
            {T, State#?MODULE{items = Items}};
        false ->
            error
    end.


-spec evict_all(state()) -> state().
evict_all(#?MODULE{items = Items,
                   handler = Handler}) ->
    [Handler(T) || T <- Items],
    #?MODULE{items = []}.

-spec size(state()) -> non_neg_integer().
size(#?MODULE{items = Items}) ->
    length(Items).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

evit_test() ->
    C0 = new(3, fun(I) -> ?debugFmt("~w evicted", [I]) end),
    C1 = insert(k1, v1, C0),
    C2 = insert(k2, v2, C1),
    {{k1, v1}, C3} = evict(k1, C2),
    error = evict(k3, C3),
    ok.


basics_test() ->
    C0 = new(3, fun(I) -> ?debugFmt("~w evicted", [I]) end),
    C1 = insert(k1, v1, C0),
    C2 = insert(k2, v2, C1),
    C3 = insert(k3, v3, C2),
    C4 = insert(k4, v4, C3),
    ?assertEqual(error, fetch(k1, C4)),
    {ok, v2, C5} = fetch(k2, C4),
    C6 = insert(k5, v5, C5),
    %% k2 should still be readable here
    {ok, v2, C7} = fetch(k2, C6),
    %% k3 should have been evicted
    error = fetch(k3, C7),


    ok.


-endif.
