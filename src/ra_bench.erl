%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2017-2020 VMware, Inc. or its affiliates.  All rights reserved.
%%
%% @hidden
-module(ra_bench).

-behaviour(ra_machine).

-compile(inline_list_funcs).
-compile(inline).
-compile({no_auto_import, [apply/3]}).

-include("ra.hrl").

-include_lib("eunit/include/eunit.hrl").
-export([
         init/1,
         apply/3,

         % profile/0,
         % stop_profile/0

         prepare/0,
         run/1,
         run/0

        ]).


init(#{}) ->
    undefined.

% msg_ids are scoped per customer
% ra_indexes holds all raft indexes for enqueues currently on queue
apply(#{index := I}, {noop, _}, State) ->
    case I rem 100000 of
        0 ->
            {State, ok, {release_cursor, I, State}};
        _ ->
            {State, ok}
    end.

run() ->
    run(#{name => noop,
          seconds => 10,
          target => 50000,
          degree => 5,
          nodes => [node() | nodes()]}).


run(#{name := Name,
      seconds := Secs,
      target := Target,
      nodes := Nodes,
      degree := Degree}) ->

    io:format("starting nodes ~w~n", [Nodes]),
    {ok, ServerIds, []} = start(Name, Nodes),
    {ok,_, Leader} = ra:members(hd(ServerIds)),
    TotalOps = Secs * Target,
    Each = TotalOps div Degree,
    Start = os:system_time(millisecond),
    Pids =  [spawn_client(self(), Leader, Each) || _ <- lists:seq(1, Degree)],
    %% wait for each pid
    Wait = ((Secs * 1000) * 4),
    [begin
         receive
             {done, P} ->
                 io:format("~w is done ~n", [P]),
                 ok
         after  Wait ->
                   exit({timeout, P})
         end
     end || P <- Pids],
    End = os:system_time(millisecond),
    Taken = End - Start,
    io:format("benchmark completed: ~b ops in ~bms rate ~b ops/sec~n",
              [TotalOps, Taken, TotalOps div (Taken div 1000)]),
    _ = ra:delete_cluster(ServerIds),
    print_metrics(atom_to_binary(Name, utf8)),
    ok.

start(Name, Nodes) when is_atom(Name) ->
    ServerIds = [{Name, N} || N <- Nodes],
    Configs = [begin
                   rpc:call(N, ?MODULE, prepare, []),
                   Id = {Name, N},
                   UId = ra:new_uid(ra_lib:to_binary(Name)),
                   #{id => Id,
                     uid => UId,
                     cluster_name => Name,
                     metrics_key => atom_to_binary(Name, utf8),
                     log_init_args => #{uid => UId},
                     initial_members => ServerIds,
                     machine => {module, ?MODULE, #{}}}
               end || N <- Nodes],
    ra:start_cluster(Configs).

prepare() ->
    _ = application:ensure_all_started(ra),
    % error_logger:logfile(filename:join(ra_env:data_dir(), "log.log")),
    ok.

send_n(_, 0) -> ok;
send_n(Leader, N) ->
    ra:pipeline_command(Leader, {noop, <<>>}, make_ref(), normal),
    send_n(Leader, N-1).


client_loop(0, 0, _Leader) ->
    ok;
client_loop(Num, Sent, _Leader) ->
    receive
        {ra_event, Leader, {applied, Applied}} ->
            N = length(Applied),
            ToSend = min(Sent, N),
            send_n(Leader, ToSend),
            client_loop(Num - N, Sent - ToSend, Leader);
        {ra_event, _, {rejected, {not_leader, NewLeader, _}}} ->
            io:format("new leader ~w~n", [NewLeader]),
            send_n(NewLeader, 1),
            client_loop(Num, Sent, NewLeader);
        {ra_event, Leader, Evt} ->
            io:format("unexpected ra_event ~w~n", [Evt]),
            client_loop(Num, Sent,Leader)
    end.

spawn_client(Parent, Leader, Num) when Num >= 1000 ->
    spawn_link(
      fun () ->
              %% first send one 1000 noop commands
              %% then top up as they are applied
              send_n(Leader, 1000),
              ok = client_loop(Num, Num - 1000, Leader),
              Parent ! {done, self()}
      end).

print_metrics(Name) ->
    io:format("metrics ~p", [ets:lookup(ra_metrics, Name)]).


% profile() ->
%     GzFile = atom_to_list(node()) ++ ".gz",
%     lg:trace([noop, ra_server, ra_server_proc, ra_snapshot, ra_machine,
%               ra_log, ra_flru, ra_machine, ra_log_meta, ra_log_segment],
%              lg_file_tracer,
%              GzFile, #{running => false, mode => profile}),
%     ok.

% stop_profile() ->
%     lg:stop(),
%     Base = atom_to_list(node()),
%     GzFile = Base ++ ".gz.*",
%     lg_callgrind:profile_many(GzFile, Base ++ ".out",#{}),
%     ok.

