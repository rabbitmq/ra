-module(ra_fifo_client_SUITE).

-compile(export_all).

-export([
         ]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

%%%===================================================================
%%% Common Test callbacks
%%%===================================================================

all() ->
    [
     {group, tests}
    ].


all_tests() ->
    [
     flow
    ].

groups() ->
    [
     {tests, [], all_tests()}
    ].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(_Group, Config) ->
    Config.

end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(TestCase, Config) ->
    DataDir = filename:join(?config(priv_dir, Config), TestCase),
    [{data_dir, DataDir}, {cluster_id, TestCase} | Config].

end_per_testcase(_TestCase, _Config) ->
    ok.

%%%===================================================================
%%% Test cases
%%%===================================================================

conf({Name, _Node} = NodeId, Nodes) ->
    UId = atom_to_binary(Name, utf8),
    #{cluster_id => c1,
      id => NodeId,
      uid => UId,
      initial_nodes => Nodes,
      log_module => ra_log_file,
      log_init_args => #{uid => UId},
      machine => {module, ra_fifo, #{}}}.

flow(Config) ->
    % this tests that a consumer doesn't crash due to flow control being
    % imposed
    % the allowable pending limit grows in line with the sum of all
    % checkout num pending values
    PrivDir = ?config(data_dir, Config),
    ClusterId = ?config(cluster_id, Config),
    NodeIds = [{ClusterId, start_slave(N, PrivDir)} || N <- [s1, s2, s3]],
    Machine = {module, ra_fifo, #{}},
    {ok, NodeIds, []} = ra:start_cluster(ClusterId, Machine,
                                         #{metrics_handler => {ra_file_handle, default_handler}},
                                         NodeIds),
    NumMsg = 10000,
    EnqConf = #{cluster_id => ClusterId, nodes => NodeIds,
                num_messages => NumMsg, spec => {0, flow}},
    ConConf = #{cluster_id => ClusterId, nodes => NodeIds,
                notify => self(), prefetch => 1000,
                num_messages => NumMsg * 3, consumer_tag => <<"flow">>},
    {ok, Enq} = enqueuer:start_link(EnqConf),
    {ok, Enq2} = enqueuer:start_link(enq2, EnqConf),
    {ok, Enq3} = enqueuer:start_link(enq3, EnqConf),
    % the sleep allows messages to accumulate in the queue to provide a
    % full prefetch hit to the consumer on start
    timer:sleep(1000),
    {ok, _Con} = consumer:start_link(ConConf),
    % assert all were said to be started
    % assert all nodes are actually started
    receive consumer_done -> ok
    after 30000 ->
              exit(consumer_not_done)
    end,
    enqueuer:wait(Enq, 120000),
    enqueuer:wait(Enq2, 120000),
    enqueuer:wait(Enq3, 120000),

    % assert one node is leader
    [ok = slave:stop(S) || {_, S} <- NodeIds],
    ok.

%% Utility

node_setup(DataDir) ->
    LogFile = filename:join([DataDir, "ra.log"]),
    SaslFile = filename:join([DataDir, "ra_sasl.log"]),
    application:load(sasl),
    application:set_env(sasl, sasl_error_logger, {file, SaslFile}),
    application:stop(sasl),
    application:start(sasl),
    filelib:ensure_dir(LogFile),
    _ = error_logger:logfile({open, LogFile}),
    _ = error_logger:tty(false),
    ok.

get_current_host() ->
    {ok, H} = inet:gethostname(),
    list_to_atom(H).

make_node_name(N) ->
    {ok, H} = inet:gethostname(),
    list_to_atom(lists:flatten(io_lib:format("~s@~s", [N, H]))).

search_paths() ->
    Ld = code:lib_dir(),
    lists:filter(fun (P) -> string:prefix(P, Ld) =:= nomatch end,
                 code:get_path()).

start_slave(N, PrivDir) ->
    Dir0 = filename:join(PrivDir, N),
    Host = get_current_host(),
    Dir = "'\"" ++ Dir0 ++ "\"'",
    Pa = string:join(["-pa" | search_paths()] ++
                     ["-s ra -ra data_dir", Dir], " "),
    ct:pal("starting slave node with ~s~n", [Pa]),
    {ok, S} = slave:start_link(Host, N, Pa),
    _ = rpc:call(S, ?MODULE, node_setup, [Dir0]),
    _ = rpc:call(S, application, ensure_all_started, [ra]),
    S.
