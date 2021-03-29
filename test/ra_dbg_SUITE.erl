%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2017-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%
-module(ra_dbg_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

all() ->
  [
    {group, tests}
  ].

groups() ->
  [
    {tests, [], all_tests()}
  ].

all_tests() ->
  [
    replay,
    filter_entry_duplicate
  ].

init_per_suite(Config) ->
  Config.

end_per_suite(_Config) ->
  ok.

init_per_testcase(TestCase, Config) ->
    application:load(ra),
    Group = proplists:get_value(name, proplists:get_value(tc_group_properties, Config)),
    WorkDirectory = proplists:get_value(priv_dir, Config),
    ok = application:set_env(ra, data_dir, filename:join(WorkDirectory, atom_to_list(Group) ++ "-" ++ atom_to_list(TestCase))),
    Config.

end_per_testcase(_TestCase, _Config) ->
  application:stop(ra),
  ok.

replay(_Config) ->
  {Config, FinalState} = execute_state_machine(),
  WalFile = wal_file(),

  InitialState = ra_fifo:init(Config),
  Pid = spawn(?MODULE, report, [self(), 0]),
  %% check final state and replayed state are the same
  FinalState = ra_dbg:replay_log(WalFile, ra_fifo, InitialState,
                                 fun (_State, _Effects) ->
                                         Pid ! command_applied
                                 end),
  %% make sure the callback function has been called correctly
  Count = receive
            X -> X
          after 10000 ->
      timeout
          end,
  5 = Count,
  ok.

filter_entry_duplicate(_Config) ->
    execute_state_machine(),
    WalFile = wal_file(),

    WalInReverseOrder = ra_log_wal:wal2list(WalFile),
    Wal = lists:reverse(WalInReverseOrder),
    Wal = ra_dbg:filter_duplicate_entries(lists:append(WalInReverseOrder, WalInReverseOrder)),
    ok.

execute_state_machine() ->
  %% creating a new WAL file with ra_fifo
  [Srv] = Nodes = [{ra_dbg, node()}],
  ClusterId = ra_dbg,
  Config = #{name => ClusterId},
  Machine = {module, ra_fifo, Config},
  ra:start(),
  {ok, _, _} = ra:start_cluster(default, ClusterId, Machine, Nodes),

  {ok, _, _} = ra:process_command(Srv, {enqueue, self(), 1, <<"1">>}),
  {ok, _, _} = ra:process_command(Srv, {enqueue, self(), 2, <<"2">>}),
  {ok, _, _} = ra:process_command(Srv, {enqueue, self(), 3, <<"3">>}),

  ConsumerId = {<<"ctag1">>, self()},
  {ok, {dequeue, {MsgId, _}}, _} = ra:process_command(Srv, {checkout, {dequeue, unsettled}, ConsumerId}),

  {ok, _, _} = ra:process_command(Srv, {settle, [MsgId], ConsumerId}),
  {ok, FinalState, _} = ra:consistent_query(Srv, fun(State) -> State end),
  {Config, FinalState}.

wal_file() ->
  {ok, RaDataDir} = application:get_env(ra, data_dir),
  filename:join([RaDataDir, node(), "00000001.wal"]).

report(Pid, Count) ->
  receive
    _ ->
      report(Pid, Count + 1)
  after 1000 ->
    Pid ! Count
  end.
