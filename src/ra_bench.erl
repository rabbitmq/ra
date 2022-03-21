%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2017-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%
%% @hidden
-module(ra_bench).

-behaviour(ra_machine).

-compile(inline_list_funcs).
-compile(inline).
-compile({no_auto_import, [apply/3]}).

-include("ra.hrl").

-include_lib("eunit/include/eunit.hrl").

-define(PIPE_SIZE, 500).
-define(DATA_SIZE, 256).

-export([
         init/1,
         apply/3,

         % profile/0,
         % stop_profile/0

         prepare/0,
         run/3,
         run/2,
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

run(Name, Nodes) ->
    run(Name, Nodes, ?PIPE_SIZE).

run(Name, Nodes, Pipe)
  when is_atom(Name)
       andalso is_list(Nodes) ->
    run(#{name => Name,
          seconds => 30,
          target => 20000, % commands / sec
          degree => 5,
          pipe => Pipe,
          nodes => Nodes}).

run() ->
    run(#{name => noop,
          seconds => 10,
          target => 20000, % commands / sec
          degree => 5,
          nodes => [node() | nodes()]}).

print_counter(Last, Counter) ->
    V = counters:get(Counter, 1),
    io:format("ops/sec: ~b~n", [ V - Last]),
    timer:sleep(1000),
    print_counter(V, Counter).


run(Nodes) when is_list(Nodes) ->
    run(#{name => noop,
          seconds => 30,
          target => 20000, % commands / sec
          degree => 5,
          nodes => Nodes});
run(#{name := Name,
      seconds := Secs,
      target := Target,
      nodes := Nodes,
      degree := Degree} = Conf) ->

    Pipe = maps:get(pipe, Conf, ?PIPE_SIZE),
    io:format("running ra benchmark config: ~p~n", [Conf]),
    io:format("starting servers on ~w~n", [Nodes]),
    {ok, ServerIds, []} = start(Name, Nodes),
    {ok, _, Leader} = ra:members(hd(ServerIds)),
    TotalOps = Secs * Target,
    Each = max(Pipe, TotalOps div Degree),
    DataSize = maps:get(data_size, Conf, ?DATA_SIZE),
    Counter = counters:new(1, [write_concurrency]),
    Pids =  [spawn_client(self(), Leader, Each, DataSize, Pipe, Counter)
             || _ <- lists:seq(1, Degree)],
    io:format("running bench mark...~n", []),
    Start = erlang:system_time(millisecond),
    [P ! go || P <- Pids],
    CounterPrinter = spawn(fun () ->
                                   print_counter(counters:get(Counter, 1), Counter)
                           end),
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
    exit(CounterPrinter, kill),
    io:format("benchmark completed: ~b ops in ~bms rate ~b ops/sec~n",
              [TotalOps, Taken, TotalOps div (Taken div 1000)]),

    BName = atom_to_binary(Name, utf8),
    _ = [rpc:call(N, ?MODULE, print_metrics, [BName])
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
    _ = ra_system:start_default(),
    % error_logger:logfile(filename:join(ra_env:data_dir(), "log.log")),
    ok.

send_n(_, _Data, 0, _Counter) -> ok;
send_n(Leader, Data, N, Counter) ->
    ra:pipeline_command(Leader, {noop, Data}, make_ref(), low),
    counters:add(Counter, 1, 1),
    send_n(Leader, Data, N-1, Counter).


client_loop(0, 0, _Leader, _Data, _Counter) ->
    ok;
client_loop(Num, Sent, _Leader, Data, Counter) ->
    receive
        {ra_event, Leader, {applied, Applied}} ->
            N = length(Applied),
            ToSend = min(Sent, N),
            send_n(Leader, Data, ToSend, Counter),
            client_loop(Num - N, Sent - ToSend, Leader, Data, Counter);
        {ra_event, _, {rejected, {not_leader, NewLeader, _}}} ->
            io:format("new leader ~w~n", [NewLeader]),
            send_n(NewLeader, Data, 1, Counter),
            client_loop(Num, Sent, NewLeader, Data, Counter);
        {ra_event, Leader, Evt} ->
            io:format("unexpected ra_event ~w~n", [Evt]),
            client_loop(Num, Sent, Leader, Data, Counter)
    end.

spawn_client(Parent, Leader, Num, DataSize, Pipe, Counter) ->
    Data = crypto:strong_rand_bytes(DataSize),
    spawn_link(
      fun () ->
              %% first send one 1000 noop commands
              %% then top up as they are applied
              receive
                  go ->
                      send_n(Leader, Data, Pipe, Counter),
                      ok = client_loop(Num, Num - Pipe, Leader, Data, Counter),
                      Parent ! {done, self()}
              end
      end).

print_metrics(Name) ->
    io:format("Node: ~w~n", [node()]),
    io:format("metrics ~p~n", [ets:lookup(ra_metrics, Name)]),
    io:format("counters ~p~n", [ra_counters:overview()]).



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

