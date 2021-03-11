%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2017-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%
%% @doc utilities to debug ra

-module(ra_dbg).

%% API
-export([replay_log/3, replay_log/4]).

%% exported for tests
-export([filter_duplicate_entries/1]).

%% @doc Replays log file for a given state machine module with an initial state.
%%
%% @param The location of the WAL file.
%% @param The Erlang module of the state machine.
%% @param The initial state.
%%
%% @returns The state once the log has been replayed.
-spec replay_log(string(), module(), term()) ->
  term().
replay_log(WalFile, Module, InitialState) ->
  replay_log(WalFile, Module, InitialState, fun(_, _) -> noop end).

%% @doc Replays log file for a given state machine module with an initial state.
%%
%% @param The location of the WAL file.
%% @param The Erlang module of the state machine.
%% @param The initial state.
%% @param A function to react to state and effects after each applied command
%%
%% @returns The state once the log has been replayed.
-spec replay_log(string(), module(), term(), Fun) -> term() when
  Fun :: fun((State :: term(), Effects :: term()) -> term()).
replay_log(WalFile, Module, InitialState, Func) ->
  Wal = filter_duplicate_entries(ra_log_wal:wal2list(WalFile)),
  WalFunc = fun({Index, Term, {'$usr', Metadata, Command, _}}, Acc) ->
                    Metadata1 = Metadata#{index => Index,
                                          term => Term},
                    case Module:apply(Metadata1, Command, Acc) of
                        {NewAcc, _Reply} ->
                            Func(NewAcc, undefined),
                            NewAcc;
                        {NewAcc, _Reply, Effects} ->
                            Func(NewAcc, Effects),
                            NewAcc
                    end;
               (_, Acc) ->
                    Acc
            end,
  lists:foldl(WalFunc, InitialState, Wal).

filter_duplicate_entries(WalInReverseOrder) ->
    {_IndexRegistry, OrderedAndFilteredWal} =
    lists:foldl(fun({Index, _Term, _Command} = Entry, {IndexRegistry, WalAcc}) ->
                        case maps:is_key(Index, IndexRegistry) of
                            true ->
                                {IndexRegistry, WalAcc};
                            false ->
                                {maps:put(Index, true, IndexRegistry),
                                 lists:append([Entry], WalAcc)}
                        end
                end, {#{}, []}, WalInReverseOrder),
    OrderedAndFilteredWal.

