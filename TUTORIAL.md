# Ra: tutorial


### Hello World-ish tutorial.

1. Prepare the application config.

    RA erlang application requires a data directory to store write-ahead log file.
    You should set it using the `data_dir` application environment variable.
    `ok = application:set_env(ra, data_dir, "."), % Sets data directory to a current working directory`

1. Write a state machine.

    Before you can do anything you need to write a state machine to run inside a Ra cluster. State machines operate on incoming message and return an updated state. The simplest possible statemachine is one that accepts integers as command messages and simple adds them to the state and return the result. In short `fun erlang:'+'/2` would do the trick.

    initial_nodes should

    Now you've got a state machine function you can create a `ra_node_config()` map:

    ```
    NodeId = {node1, node()},
    Config = #{id => NodeId,
               uid => <<"node1">>,
               log_module => ra_log_memory,
               log_init_args => [{node1, node()}, {node2, node()}, {node3, node()}],
               initial_nodes => [{node1, node()}, {node2, node()}, {node3, node()}],
               machine => {simple, fun erlang:'+'/2, 0}}, % the "apply" function + the initial state
    ```

2. Start a cluster

    ```
    ok = ra:start_node(Config),
    ok = ra:start_node(Config#{id => {node2, node()}, uid => <<"node2">>}),
    ok = ra:start_node(Config#{id => {node3, node()}, uid => <<"node3">>}),
    % new ra nodes are completely passive
    % trigger election
    ok = ra:trigger_election({node1, node()}).

    ```

    Now you should have a working ra cluster.

3. Send a command

    ```
    {ok, IdxTerm, Leader} = ra:send_and_await_consensus({node1, node()}, 5),

    ```

4. Perform a query

    As we know the previous command achieved consensus a committed query to the leader is fine.

    ```
    {ok, {IdxTerm, 5}, Leader} = ra:committed_query({node1, node()}, fun (S) -> S end),
    ```
