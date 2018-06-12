# Writing a state machine.

For `ra` to do anything useful you need to provide it with a state machine
implementation that solves a particular problem.

To implement a state machine to run in `ra` you need to implement the
`ra_machine` behaviour. There are two mandatory callbacks that need to be
implemented:

```erlang
-callback init(Conf :: machine_init_args()) -> {state(), effects()}.

-callback 'apply'(Index :: ra_index(), command(), State) ->
    {State, effects()} | {State, effects(), reply()}.
```

`init/1` returns the initial state when a new instance of the state machine
is created. It takes an arbitrary map of configuration parameters.

`apply/3` is the primary function that is called for every command in the
raft log. It takes the raft index (more on that later), a command and the
current state and either returns the new state and a list of effects (more on
effects later) or the new state, effects _and_ a reply that can be returned
to the caller _if_ they issued a synchronous call (see: `ra:send_and_await_consensus`).

There are also some optional callbacks that advanced state machines may choose to
implement.

## A simple KV store.

As an example we are going to write a simple key-value store that takes
`write` and `read` operations.

### Writing the store

Create a new erlang module named `ra_kv` using the `ra_machine` behaviour and
export the `init/1` and `apply/3` functions:

```erlang
-module(ra_kv).
-behaviour(ra_machine).
-export([init/1, apply/3]).
```

First we are going to define a type spec for the state and commands that we will
use. The state is simply a map of arbitrary keys and values. We can store anything.

```erlang
-opaque state() :: #{term() => term()}.

-type ra_kv_command() :: {write, Key :: term(), Value :: term()} |
                         {read, Key :: term()}.
```

To implement `init/1` simply return and empty map and an empty list of effects.
This is the initial state of our kv store.

```erlang
init(_Config) -> {#{}, []}.
```

To implement the `apply/3` function we need to handle each of the commands
we support.

```erlang
apply(_Index, {write, Key, Value}, State) ->
    {maps:put(Key, Value, State), []};
apply(_Index, {read, Key}, State) ->
    Reply = maps:get(Key, State, undefined),
    {State, [], Reply}.
```

For the `{write, Key, Value}` command we simply put the key and value into the
map and return the new state as well as an empty list of effects.

For `{read, Key}` we additional return the value of the key or `undefined` if
it does not exist so that a waiting caller can obtain the value.

An that is it! The state machine is finished.


### Running the state machine inside `ra`

To actually run this we need to configure a ra cluster to use the `ra_kv`
state machine and start it. The simplest way is to use the `ra:start_cluster/3`
function. It takes a ClusterId that can be an arbitrary term, we here use a
binary, a machine configuration and a list of nodes that define the initial
set of members.

```erlang
start() ->
    %% the initial cluster members
    Nodes = [{ra_kv1, node()}, {ra_kv2, node()}, {ra_kv3, node()}],
    %% an arbitrary cluster id
    ClusterId = <<"ra_kv">>,
    %% the config passed to `init/1`
    Config = #{},
    %% the machine configuration
    Machine = {module, ?MODULE, Config},
    %% ensure ra is started
    application:ensure_all_started(ra),
    %% start a cluster instance running the `ra_kv` machine
    ra:start_cluster(ClusterId, Machine, Nodes).
```

If you then start an erlang shell with `make shell` or similar and call
`ra_kv:start/0` you should hopefully be returned with something like:

```erlang
{ok,[{ra_kv1,nonode@nohost},
     {ra_kv2,nonode@nohost},
     {ra_kv3,nonode@nohost}],
    []}
```

Indicating that all nodes in the `ra` cluster were successfully started. The
last element of the tuple would contain the nodes that were not successfully
started. If a quorum of nodes could not be started the function would return
and error.

Now you can write your first value into the databas.

```erlang
2> ra:send_and_await_consensus(ra_kv1, {write, k, v}).
{ok,{2,1},ra_kv1}
3> ra:send_and_await_consensus(ra_kv1, {read, k}).
{ok,v,ra_kv1}
4> ra:send_and_await_consensus(ra_kv1, {write, k, v2}).
{ok,{4,1},ra_kv1}
5> ra:send_and_await_consensus(ra_kv1, {read, k}).
{ok,v2,ra_kv1}
```

`ra:send_and_await_consensus/2` blocks until the command has achieved consensus
and has been applied to the state machine on the leader node. It is the simplest
way to interact with `ra` but also the one with the highest latency.
To read values consistently we have no choice than to use it.
The return tuple has either the raft index and term the entry was added to the
raft log _or_ the return value optionally returned by the state machine. The
`{read, Key}` command returns the current value of the key.

### Providing a client api.

We have already added the `start/0` function to start a local ra cluster. It would
make sense to abstract interactions with the kv store behind a nicer interface
than calling `ra:send_and_await_consensus/2` directly.

```erlang
write(Key, Value) ->
    %% it would make sense to cache this to avoid redirection costs when this
    %% node happens not to be the current leader
    Node = ra_kv1,
    case ra:send_and_await_consensus(Node, {write, Key, Value}) of
        {ok, _, _} ->
            ok;
        Err ->
            Err
    end.

read(Key) ->
    Node = ra_kv1,
    case ra:send_and_await_consensus(Node, {read, Key}) of
        {ok, Value, _} ->
            {ok, Value};
        Err ->
            Err
    end.
```

## Effects

Effects are used to separate the state machine logic from the effects it wants
to take inside it's environment. Each call to the `apply/3` function can return
a list of effects for the leader to realise. This includes sending messages,
setting up node and process monitors and calling arbitrary functions.
Only the leader that first applies an entry will attempt the effect. Followers
process the same set of commands but simply throw away any effects returned by
the state machine.

How does ra ensure we not re-issue effects on recovery?

Each ra node persists it's `last_applied` index. When the node restarts it
replays it's log until this point and throws away any resulting effects as they
should already have been issued.

NB: as the `last_applied` index is only persisted periodically there is a small
chance that some effects may be issued multiple times when all the nodes in the
cluster crash suddenly and at the same time. It is worth taking this into account
when implementing your state machine. There is also a chance that effects will
never be issued or reach their recipients. Ra makes no allowance for this.
(See recommendation about using an ARQ protocol below).

### Send a message

The `{send_msg, pid(), Msg :: term()}` effects asynchronously sends a message
to the specified
`pid`. Not that `ra` uses `erlang:send/3` with the `no_connect` and `no_suspend`
options which are the least reliable message sending options. It does this so
that a state machine `send_msg` effect will never block the main `ra` process.
To ensure message reliability normal [Autmatic Repeat Query ARQ)](https://en.wikipedia.org/wiki/Automatic_repeat_request)
like protocols between the state machine and the receiver should be implemented
if needed.

### Monitors

Use '`{monitor, process | node, pid() | node()}` to ask the `ra` leader to
monitor a process or node. If `ra` receives a `DOWN` for a process it
is monitoring it will commit a `{down,  pid(), term()}` command to the log that
the state machine needs to handle. If it detects a monitored node as down or up
it will commit a `{nodeup | nodedown, node()}` command.

Use `{demonitor, process | node, pid() | node()}` to stop monitoring a process
or a node.

### Call a function

Use the `{modcall, module(), function(), Args :: [term()]}` to call an arbitrary
function. Care need to be taken not to block the `ra` process whilst doing so.
It is recommended that expensive operations are done in another process.

The `mod_call` effect is useful for e.g. updating an ets table of committed entries
or similar.

### Update the release cursor (Snapshotting)

To (potentially) trigger a snapshot return the `{release_cursor, RaftIndex, MachineState}`
effect. This is why the raft index is included in the `apply/3` function. Ra will
only create a snapshot if doing so will result in log segments being deleted.
