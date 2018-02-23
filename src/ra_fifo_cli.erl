-module(ra_fifo_cli).

-export([main/1]).
-export([stop/0]).

main(Args) ->
    #{op := Op} = Opts = parse_args(Args),
    case Op of
        enqueue -> enqueue(Opts);
        dequeue -> dequeue(Opts);
        help    -> print_help(Opts);
        start_ra_cluster -> start_ra_cluster(Opts);
        stop_ra_cluster  -> stop_ra_cluster(Opts);
        start_erlang -> start_erlang(Opts);
        stop_erlang  -> stop_erlang(Opts);
        none    -> fail("Command required")
    end.

start_erlang(Opts) ->
    #{node := Node, data_dir := DataDir} = Opts,
    case Node of
        nonode -> fail("Node name required");
        _ -> ok
    end,
    case filelib:is_dir(DataDir) of
        false -> fail("Data dir should exist ~p~n", [DataDir]);
        true -> ok
    end,
    {ok, _} = start_distribution(Node),
    io:format("Starting node ~p~n", [node()]),
    ok = application:load(ra),
    ok = application:set_env(ra, data_dir, filename:join([DataDir, atom_to_list(node())])),
    {ok, _} = application:ensure_all_started(ra),
    spawn(fun() ->
        ets:new(ra_fifo_metrics, [public, named_table, {write_concurrency, true}]),
        receive stop -> ok end
    end),
    register(ra_control, self()),
    receive stop -> ok
    end.

stop() ->
    ra_control ! stop.

stop_erlang(Opts) ->
    start_distribution(cli),
    #{node := Node} = Opts,
    rpc:call(Node, ra_fifo_cli, stop, []).

start_ra_cluster(Opts) ->
    start_distribution(cli),
    #{nodes := Nodes} = Opts,
    case Nodes of
        [] -> fail("--nodes should contain a list of nodes");
        _  -> ok
    end,
    io:format("Starting ra cluster on nodes ~p~n", [Nodes]),
    lists:foreach(fun({Name, Node}) ->
        DataDir = rpc:call(Node, application, get_env, [ra, data_dir, none]),
        case ra:members({Name, Node}) of
            {error,noproc} -> continue;
            {ok, Nodes, _} -> fail("Cluster is already started");
            {ok, _, _} -> fail("Other cluster configuration is started")
        end,
        case DataDir of
            none -> fail("Data should be set on node start ~n");
            _ -> ok
        end,
        Config = #{ id => {Name, Node},
                    uid => atom_to_binary(Name, utf8),
                    initial_nodes => Nodes,
                    log_module => ra_log_file,
                    log_init_args =>
                        #{data_dir => filename:join([DataDir, atom_to_list(Node)]),
                          uid => atom_to_binary(Name, utf8)},
                    machine => {module, ra_fifo}
                    },
        io:format("Starting ra node ~p~n", [{Name, Node}]),
        ok = ct_rpc:call(Node, ra, start_node, [Config])
    end,
    Nodes),
    ok = ra:trigger_election(hd(Nodes)).

stop_ra_cluster(Opts) ->
    start_distribution(cli),
    #{nodes := Nodes} = Opts,
    case Nodes of
        [] -> fail("--nodes should contain a list of nodes");
        _  -> ok
    end,
    [ok = rpc:call(Node, ra, stop_node, [{Name, Node}]) || {Name, Node} <- Nodes].

print_help(_) ->
    io:format("Use ra_fifo to enqueue/dequeue messages.~n~n"
              "Usage: ~n"
              "ra_fifo_cli enqueue --message <msg_body> --nodes <nodes_config> --timeout <timeout_in_milliseconds> ~n"
              "ra_fifo_cli dequeue --nodes <nodes_config> ~n"
              "ra_fifo_cli start_erlang --node <node_name> --data-dir <dir>~n"
              "ra_fifo_cli stop_erlang --node <node_name>~n"
              "ra_fifo_cli start_ra_cluster --nodes <nodes_config>~n"
              "ra_fifo_cli stop_ra_cluster --nodes <nodes_config>~n"
              "~nWhere nodes_config is an erlang term representation of a list~n"
              "of ra nodes. E.g. '[{foo,foo@localhost},{foo,bar@localhost}]'~n"
              "Spaces in message bodies and node config are not supported~n").

enqueue(Opts) ->
    start_distribution(cli),
    #{message := Message,
      nodes := Nodes,
      timeout := Timeout} = Opts,
    case Message of
        undefined -> fail("--message required");
        _         -> ok
    end,
    case Nodes of
        [] -> fail("--nodes should contain a list of nodes");
        _  -> ok
    end,
    State = ra_fifo_client:init(Nodes),
    Ref = make_ref(),
    {ok, State1} = ra_fifo_client:enqueue(Ref, Message, State),
    wait_for_ack(State1, Ref, Message, Timeout).

wait_for_ack(State, Ref, Message, Timeout) ->
    receive
        {ra_event, From, Event} ->
            case ra_fifo_client:handle_ra_event(From, Event, State) of
                {internal, [], State2} ->
                    io:format("Empty return ~n", []),
                    wait_for_ack(State2, Ref, Message, Timeout);
                {internal, [Ref], _State2} ->
                    io:format("Enqueued ~p with ref ~p~n", [Message, Ref])
            end;
        Other ->
            fail("Unexpected message on publisher ~p", [Other])
    after Timeout ->
        fail("Timeout waiting for ack ~p", [Timeout])
    end.

dequeue(Opts) ->
    start_distribution(cli),
    #{nodes := Nodes} = Opts,
    case Nodes of
        [] -> fail("--nodes should contain a list of nodes");
        _  -> ok
    end,
    State = ra_fifo_client:init(Nodes),

    case ra_fifo_client:dequeue(<<"consumer_once">>, settled, State) of
        {timeout, _} -> fail("Timeout");
        {error, Err} -> fail("Error: ~p", [Err]);
        {ok, empty, _State1} -> io:format("Empty queue ~n");
        {ok, {_MsgId, {_, Msg}}, _State1} -> io:format("Got message: ~n~s~n", [Msg])
    end.

parse_args(Args) ->
    parse_args(Args,
               #{message => undefined,
                 nodes => [],
                 timeout => 5000,
                 op => none,
                 node => nonode,
                 data_dir => "/tmp"}).

parse_args([], Opts) ->
    Opts;
parse_args(["enqueue" | Other], Opts) ->
    parse_args(Other, Opts#{op := enqueue});
parse_args(["dequeue" | Other], Opts) ->
    parse_args(Other, Opts#{op := dequeue});
parse_args(["help" | Other], Opts) ->
    parse_args(Other, Opts#{op := help});

parse_args(["start_erlang" | Other], Opts) ->
    parse_args(Other, Opts#{op := start_erlang});
parse_args(["stop_erlang" | Other], Opts) ->
    parse_args(Other, Opts#{op := stop_erlang});

parse_args(["start_ra_cluster" | Other], Opts) ->
    parse_args(Other, Opts#{op := start_ra_cluster});
parse_args(["stop_ra_cluster" | Other], Opts) ->
    parse_args(Other, Opts#{op := stop_ra_cluster});

parse_args(["--help" | Other], Opts) ->
    parse_args(Other, Opts#{op := help});
parse_args(["--message", Msg | Other], Opts) ->
    parse_args(Other, Opts#{message := list_to_binary(Msg)});
parse_args(["--timeout", Timeout | Other], Opts) ->
    parse_args(Other, Opts#{timeout := list_to_integer(Timeout)});
parse_args(["--nodes", Nodes | Other], Opts) ->
    parse_args(Other, Opts#{nodes := parse_nodes(Nodes)});
parse_args(["--data-dir", DataDir | Other], Opts) ->
    parse_args(Other, Opts#{data_dir := DataDir});
parse_args(["--node", Node | Other], Opts) ->
    parse_args(Other, Opts#{node := list_to_atom(Node)}).

parse_nodes(Nodes) ->
    {ok, Tokens, _} = erl_scan:string(Nodes ++ "."),
    {ok, Term} = erl_parse:parse_term(Tokens),
    Term.

fail(Msg) ->
    fail(Msg, []).

fail(Msg, Args) ->
    io:format(standard_error, Msg ++ "~n", Args),
    halt(1).

start_distribution(Node) ->
    ensure_epmd(),
    case Node of
        cli ->
            RandNumber = rand:uniform(10000),
            RandNodeStr = "cli-" ++ integer_to_list(RandNumber),
            {ok, Names} = net_adm:names(net_adm:localhost()),
            case proplists:get_value(RandNodeStr, Names, undefined) of
                undefined ->
                    net_kernel:start([list_to_atom(RandNodeStr), shortnames]);
                _ ->
                    start_distribution(cli)
            end;
        _ ->
            net_kernel:start([Node, shortnames])
    end.

ensure_epmd() ->
    os:cmd("erl -sname epmd_starter -noshell -eval 'halt().'").
