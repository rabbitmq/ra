-module(ra_dbg_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

all() ->
  [replay].

init_per_suite(Config) ->
  application:load(ra),
  WorkDirectory = proplists:get_value(priv_dir, Config),
  ok = application:set_env(ra, data_dir, filename:join(WorkDirectory, "ra")),
  Config.

end_per_suite(Config) ->
  application:stop(ra),
  Config.

replay(_Config) ->
  %% creating a new WAL file with ra_fifo
  Nodes = [{ra_replay, node()}],
  ClusterId = ra_replay,
  Config = #{name => ClusterId},
  Machine = {module, ra_fifo, Config},
  application:ensure_all_started(ra),
  {ok, _, _} = ra:start_cluster(ClusterId, Machine, Nodes),

  {ok, _, _} = ra:process_command(ra_replay, {enqueue, self(), 1, <<"1">>}),
  {ok, _, _} = ra:process_command(ra_replay, {enqueue, self(), 2, <<"2">>}),
  {ok, _, _} = ra:process_command(ra_replay, {enqueue, self(), 3, <<"3">>}),

  ConsumerId = {<<"ctag1">>, self()},
  {ok, {dequeue, {MsgId, _}}, _} = ra:process_command(ra_replay, {checkout, {dequeue, unsettled}, ConsumerId}),

  {ok, _, _} = ra:process_command(ra_replay, {settle, [MsgId], ConsumerId}),
  {ok, FinalState, _} = ra:consistent_query(ra_replay, fun(State) -> State end),

  %% now replaying the WAL
  {ok, RaDataDir} = application:get_env(ra, data_dir),
  WalFile = filename:join([RaDataDir, node(), "00000001.wal"]),

  InitialState = ra_fifo:init(Config),
  Pid = spawn(?MODULE, report, [self(), 0]),
  %% check final state and replayed state are the same
  FinalState = ra_dbg:replay_log(WalFile, ra_fifo, InitialState, fun(_State, _Effects) -> Pid ! command_applied end),
  %% make sure the callback function has been called correctly
  Count = receive
            X -> X
          after 10000 ->
      timeout
          end,
  5 = Count,
  ok.

report(Pid, Count) ->
  receive
    _ ->
      report(Pid, Count + 1)
  after 1000 ->
    Pid ! Count
  end.
