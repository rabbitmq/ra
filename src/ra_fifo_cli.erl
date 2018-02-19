-module(ra_fifo_cli).

-export([main/1]).

main(Args) ->
    #{op := Op} = Opts = parse_args(Args),
    case Op of
        enqueue -> enqueue(Opts);
        dequeue -> dequeue(Opts);
        help    -> print_help(Opts);
        none    -> fail("Command required")
    end.

print_help(_) ->
    io:format("Use ra_fifo to enqueue/dequeue messages.~n~n"
              "Usage: ~n"
              "ra_fifo_cli enqueue --message <msg_body> --nodes <nodes_config> --timeout <timeout_in_milliseconds> ~n"
              "ra_fifo_cli dequeue --nodes <nodes_config> ~n~n"
              "Where nodes_config is an erlang term representation of a list~n"
              "of ra nodes. E.g. '[{foo,foo@localhost},{foo,bar@localhost}]'~n"
              "Spaces in message bodies and node config are not supported~n").

enqueue(Opts) ->
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
    #{nodes := Nodes} = Opts,
    case Nodes of
        [] -> fail("--nodes should contain a list of nodes");
        _  -> ok
    end,
    State = ra_fifo_client:init(Nodes),

    case ra_fifo_client:dequeue(<<"consumer_once">>, settled, State) of
        {timeout, _} -> fail("Timeout");
        {error, Err} -> fail("Error: ~p", [Err]);
        {ok, empty, _State1} -> fail("Empty queue");
        {ok, {_MsgId, {_, Msg}}, _State1} -> io:format("Got message ~p", [Msg])
    end.

parse_args(Args) ->
    parse_args(Args,
               #{message => undefined,
                 nodes => [],
                 timeout => 5000,
                 op => none}).

parse_args([], Opts) ->
    Opts;
parse_args(["enqueue" | Other], Opts) ->
    parse_args(Other, Opts#{op := enqueue});
parse_args(["dequeue" | Other], Opts) ->
    parse_args(Other, Opts#{op := dequeue});
parse_args(["help" | Other], Opts) ->
    parse_args(Other, Opts#{op := help});
parse_args(["--help" | Other], Opts) ->
    parse_args(Other, Opts#{op := help});
parse_args(["--message", Msg | Other], Opts) ->
    parse_args(Other, Opts#{message := list_to_binary(Msg)});
parse_args(["--timeout", Timeout | Other], Opts) ->
    parse_args(Other, Opts#{timeout := list_to_integer(Timeout)});
parse_args(["--nodes", Nodes | Other], Opts) ->
    parse_args(Other, Opts#{nodes := parse_nodes(Nodes)}).

parse_nodes(Nodes) ->
    {ok, Tokens, _} = erl_scan:string(Nodes ++ "."),
    {ok, Term} = erl_parse:parse_term(Tokens),
    Term.

fail(Msg) ->
    fail(Msg, []).

fail(Msg, Args) ->
    io:format(standard_error, Msg ++ "~n", Args),
    halt(1).