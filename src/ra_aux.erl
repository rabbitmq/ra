%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term Broadcom refers to Broadcom Inc. and/or its subsidiaries.
-module(ra_aux).

-export([
         machine_state/1,
         effective_machine_version/1,
         leader_id/1,
         last_applied/1,
         current_term/1,
         members_info/1,
         overview/1,
         log_last_index_term/1,
         log_fetch/2,
         log_stats/1
        ]).

-include("ra.hrl").
-include("ra_server.hrl").

-opaque internal_state() :: ra_server:state().

-export_type([internal_state/0]).

-spec machine_state(ra_aux:internal_state()) -> term().
machine_state(State) ->
    maps:get(?FUNCTION_NAME, State).

-spec effective_machine_version(ra_aux:internal_state()) ->
    ra_machine:version().
effective_machine_version(#{cfg := Cfg}) ->
    Cfg#cfg.effective_machine_version.

-spec leader_id(ra_aux:internal_state()) -> undefined | ra_server_id().
leader_id(State) ->
    maps:get(?FUNCTION_NAME, State).

-spec last_applied(ra_aux:internal_state()) -> ra_index().
last_applied(State) ->
    maps:get(?FUNCTION_NAME, State).

-spec current_term(ra_aux:internal_state()) -> ra_term().
current_term(State) ->
    maps:get(?FUNCTION_NAME, State).

-spec members_info(ra_aux:internal_state()) -> ra_cluster().
members_info(State) ->
    ra_server:state_query(?FUNCTION_NAME, State).

-spec overview(ra_aux:internal_state()) -> map().
overview(State) ->
    ra_server:state_query(?FUNCTION_NAME, State).

-spec log_last_index_term(ra_aux:internal_state()) -> ra_idxterm().
log_last_index_term(#{log := Log}) ->
    ra_log:last_index_term(Log).

-spec log_fetch(ra_index(), ra_aux:internal_state()) ->
    {undefined |
     {ra_term(),
      CmdMetadata :: ra_server:command_meta(),
      Command :: term()}, ra_aux:internal_state()}.
log_fetch(Idx, #{log := Log0} = State)
  when is_integer(Idx) ->
    case ra_log:fetch(Idx, Log0) of
        {{Idx, Term, {'$usr', Meta, Cmd, _ReplyMode}}, Log} ->
            {{Term, Meta, Cmd}, State#{log => Log}};
        {_, Log} ->
            %% we only allow user commands to be read
            {undefined, State#{log => Log}}
    end.

-spec log_stats(ra_aux:internal_state()) -> ra_log:overview().
log_stats(#{log := Log}) ->
    ra_log:overview(Log).

