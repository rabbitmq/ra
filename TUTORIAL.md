# Ra: tutorial


### Hello World-ish tutorial.

1. Write a state machine.

    Before you can do anything you need to write a state machine to run inside a Ra cluster. State machines operate on incoming message and return an updated state. The simplest possible statemachine is one that accepts integers as command messages and simple adds them to the state and return the result. In short `fun erlang:'+'/2` would do the trick.

    Now you've got a state machine function you can create a `ra_node_config()` map:

    ```
    NodeId = {node1, node()},
    Config = #{id => NodeId,
               log_module => ra_log_memory,
               log_init_args => [{node1, node()}, {node2, node()}, {node3, node()}],
               initial_nodes => [],
               machine => {simple, fun erlang:'+'/2, 0}}, % the "apply" function + the initial state
    ```

2. Start a cluster

    ```
    ok = ra:start_node(Config),
    ok = ra:start_node(Config#{id => {node2, node()}),
    ok = ra:start_node(Config#{id => {node3, node()}),
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

    As we know the previous command achieved consensus a dirty query to the leader is fine.

    ```
    {ok, {IdxTerm, 5}, Leader} = ra:dirty_query({node1, node()}, fun (S) -> S end),
    ```
