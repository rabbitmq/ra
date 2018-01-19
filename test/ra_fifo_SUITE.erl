-module(ra_fifo_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

%% common ra_log tests to ensure behaviour is equivalent across
%% ra_log backends

all() ->
    [
     {group, tests}
    ].

all_tests() ->
    [
     first
    ].

groups() ->
    [
     {tests, [], all_tests()}
    ].

init_per_group(_, Config) ->
    PrivDir = ?config(priv_dir, Config),
    _ = application:load(ra),
    ok = application:set_env(ra, data_dir, PrivDir),
    application:ensure_all_started(ra),
    Config.

end_per_group(_, Config) ->
    _ = application:stop(ra),
    Config.

init_per_testcase(_TestCase, Config) ->
    case ets:info(ra_fifo_metrics) of
        undefined ->
            _ = ets:new(ra_fifo_metrics, [public, named_table, {write_concurrency, true}]);
        _ ->
            ok
    end,
    Config.

first(Config) ->
    PrivDir = ?config(priv_dir, Config),
    NodeId = {first, node()},
    Conf = #{id => NodeId,
             log_module => ra_log_file,
             log_init_args => #{data_dir => PrivDir, id => first},
             initial_nodes => [],
             machine => {module, ra_fifo}},
    _ = ets:insert(ra_fifo_metrics, {first, 0, 0, 0, 0}),
    _ = ra_nodes_sup:start_node(Conf),
    _ = ra:send_and_await_consensus(NodeId, {checkout, {auto, 10}, self()}),

    ra_log_wal:force_roll_over(ra_log_wal),
    % create segment the segment will trigger a snapshot
    timer:sleep(1000),

    _ = ra:send_and_await_consensus(NodeId, {enqueue, one}),
    receive
        {msg, MsgId, _} ->
            _ = ra:send_and_await_consensus(NodeId, {settle, MsgId, self()})
    after 5000 ->
              exit(await_msg_timeout)
    end,

    _ = ra_nodes_sup:stop_node(NodeId),
    _ = ra_nodes_sup:restart_node(NodeId),

    _ = ra:send_and_await_consensus(NodeId, {enqueue, two}),
    ct:pal("restarted node"),
    receive
        {msg, _, two} -> ok
    after 2000 ->
              exit(await_msg_timeout)
    end,
    ok.
