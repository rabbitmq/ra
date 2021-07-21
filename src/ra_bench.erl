%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2017-2021 VMware, Inc. or its affiliates.  All rights reserved.
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
         run/0,
         print_metrics/1

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
          target => 500,
          degree => 5,
          nodes => [node() | nodes()]}).

-define(DATA_SIZE, 256).

run(#{name := Name,
      seconds := Secs,
      target := Target,
      nodes := Nodes,
      degree := Degree} = Conf) ->

    io:format("starting servers on ~w", [Nodes]),
    {ok, ServerIds, []} = start(Name, Nodes),
    {ok,_, Leader} = ra:members(hd(ServerIds)),
    TotalOps = Secs * Target,
    Each = TotalOps div Degree,
    Start = erlang:system_time(millisecond),
    DataSize = maps:get(data_size, Conf, ?DATA_SIZE),
    Pids =  [spawn_client(self(), Leader, Each, DataSize) || _ <- lists:seq(1, Degree)],
    %% wait for each pid
    Wait = ((Secs * 10000) * 4),
    [begin
         receive
             {done, P} ->
                 io:format("~w is done ", [P]),
                 ok
         after  Wait ->
                   exit({timeout, P})
         end
     end || P <- Pids],
    End = erlang:system_time(millisecond),
    Taken = End - Start,
    io:format("benchmark completed: ~b ops in ~bms rate ~b ops/sec",
              [TotalOps, Taken, TotalOps div (Taken div 1000)]),

    BName = atom_to_binary(Name, utf8),
    [rpc:call(N, ?MODULE, print_metrics, [BName])
     || N <- Nodes],
    _ = ra:delete_cluster(ServerIds),
    %%
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
    ra:start_cluster(default, Configs).

prepare() ->
    _ = application:ensure_all_started(ra),
    % error_logger:logfile(filename:join(ra_env:data_dir(), "log.log")),
    ok.

send_n(_, _Data, 0) -> ok;
send_n(Leader, Data, N) ->
    ra:pipeline_command(Leader, {noop, Data}, make_ref(), low),
    send_n(Leader, Data, N-1).

-define(PIPE_SIZE, 200).

client_loop(0, 0, _Leader, _Data) ->
    ok;
client_loop(Num, Sent, _Leader, Data) ->
    receive
        {ra_event, Leader, {applied, Applied}} ->
            N = length(Applied),
            ToSend = min(Sent, N),
            send_n(Leader, Data, ToSend),
            client_loop(Num - N, Sent - ToSend, Leader, Data);
        {ra_event, _, {rejected, {not_leader, NewLeader, _}}} ->
            io:format("new leader ~w", [NewLeader]),
            send_n(NewLeader, Data, 1),
            client_loop(Num, Sent, NewLeader, Data);
        {ra_event, Leader, Evt} ->
            io:format("unexpected ra_event ~w", [Evt]),
            client_loop(Num, Sent, Leader, Data)
    end.

spawn_client(Parent, Leader, Num, DataSize) when Num >= ?PIPE_SIZE ->
    Data = crypto:strong_rand_bytes(DataSize),
    spawn_link(
      fun () ->
              %% first send one 1000 noop commands
              %% then top up as they are applied
              send_n(Leader, Data, ?PIPE_SIZE),
              ok = client_loop(Num, Num - ?PIPE_SIZE, Leader, Data),
              Parent ! {done, self()}
      end).

print_metrics(Name) ->
    io:format("Node ~w:", [node()]),
    io:format("metrics ~p~n", [ets:lookup(ra_metrics, Name)]),
    io:format("counters ~p", [ra_counters:overview()]).



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

