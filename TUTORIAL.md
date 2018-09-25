# Ra: tutorial


### Hello World-ish tutorial.

1. Prepare the application config.

    RA erlang application requires a data directory to store write-ahead log file.
    You should set it using the `data_dir` application environment variable.
    `ok = application:set_env(ra, data_dir, "."), % Sets data directory to a current working directory`

1. Write a state machine.

    Before you can do anything you need to write a state machine to run inside a Ra cluster. State machines operate on incoming message and return an updated state. The simplest possible statemachine is one that accepts integers as command messages and simple adds them to the state and return the result. In short `fun erlang:'+'/2` would do the trick.

    initial_members should

    Now you've got a state machine function you can create a `ra_server_config()` map:

    ```
    NodeId = {server1, node()},
    UId = <<"server1">>,
    Config = #{id => NodeId,
               uid => <<"server1">>,
               log_init_args => #{uid => UId},
               initial_members => [{server1, node()}, {server2, node()}, {server3, node()}],
               machine => {simple, fun erlang:'+'/2, 0}}, % the "apply" function + the initial state
    ```

2. Start a cluster

    ```
    ok = ra:start_server(Config),
    ok = ra:start_server(Config#{id => {server2, node()}, uid => <<"server2">>}),
    ok = ra:start_server(Config#{id => {server3, node()}, uid => <<"server3">>}),
    % new ra servers are completely passive
    % trigger election
    ok = ra:trigger_election({server1, node()}).

    ```

    Now you should have a working ra cluster.

3. Process a command

    ```
    {ok, Reply, Leader} = ra:process_command({server1, node()}, 5),

    ```

4. Perform a query

    As we know the previous command achieved consensus a local query to the leader is fine.

    ```
    {ok, {IdxTerm, 5}, Leader} = ra:local_query({server1, node()}, fun (S) -> S end),
    ```
