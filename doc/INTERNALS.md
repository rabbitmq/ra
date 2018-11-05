# Internals

## Concepts and Key Modules

Ra is an implementation of Raft, a distributed consensus protocol. Raft has multiple features
that work together. They are reflected in the Ra API.

Ra assumes that there can (and usually will be) multiple Raft clusters in a given Erlang node cluster.
In case of a messaging system, a topic or queue can be its own cluster. In a data store
a data partition can be its own Raft cluster. Any long lived stateful entity that the user
would like to replicate across cluster nodes can use a Raft cluster.

Raft clusters in a single cluster are logically independent but do share some Ra infrastructure
such as the write-ahead log. This is a practical decision that avoids a lot of concurrent fsync
operations that very significantly affect system throughput.

### Key Modules

[Ra API](https://rabbitmq.github.io/ra/) has two main modules:

 * `ra`: handles cluster formation, membership and functions that are not directly related to Ra state machine
 * [`ra_machine`](./STATE_MACHINE_TUTORIAL.md): a behavior that applications implement

Applications interact with Ra clusters by sending commands. The commands are processed by the
state machine. In response for every command a new machine state is returned.

The Ra server takes care of Raft leader election, log entry replication, peer failure
handling, log reinstallation, and so on. Applications are responsible for
expressing their logic in terms of Ra commands and effects.




## Ra State Machines

There are two mandatory [`ra_machine` callbacks](https://rabbitmq.github.io/ra/ra_machine.html) that need to be
implemented:

```erlang
-callback init(Conf :: machine_init_args()) -> state().

-callback 'apply'(command_meta_data(), command(), effects(), State) ->
    {State, effects(), reply()}.
```

### init/1

`init/1` returns the initial state when a new instance of the state machine
is created. It takes an arbitrary map of configuration parameters. The parameters
are application-specific.

### apply/4

`apply/4` is the primary function that is called for every command in the
Raft log. It takes Raft state machine metadata as a map (most importantly the Raft index and current term),
a command, a list of effects (see below) and the current state.

It must return the new state, a list of effects and a reply. Replies will be returned to the caller
only if they performed a synchronous call or they requested an asynchronous notification
using `ra:pipeline_command/{3/4}`.

``` erlang
-type effects() :: [effect()].
-type reply() :: term().
-type command() :: user_command() | builtin_command().
-type command_meta_data() :: ra_server:command_meta() | #{index := ra_index(),
                                                         term := ra_term()}.

-spec apply(machine(), command_meta_data(), command(), effects(), State) ->
    {State, effects(), reply()}.
```


## Effects

Effects are used to separate the state machine logic from the side effects it wants
to take inside its environment. Each call to the `apply/4` function can return
a list of effects for the leader to realise. This includes sending messages,
setting up server and process monitors and calling arbitrary functions.

In other words, effects are everything that's not related to Raft state machine
transitions.

### Effect Application and Failure Handling

Under normal operation only the leader that first applies an entry will attempt the effect.
Followers process the same set of commands but simply throw away any effects returned by
the state machine.

To ensure we not re-issue effects on recovery each `ra` server persists its `last_applied` index.
When the server restarts it replays it's log until this point and throws away any resulting effects as they
should already have been issued.

As the `last_applied` index is only persisted periodically there is a small
chance that some effects may be issued multiple times when all the servers in the
cluster fail at the same time. There is also a chance that effects will
never be issued or reach their recipients. Ra makes no allowance for this.

It is worth taking this into account when implementing a state machine.

The [Automatic Repeat Query (ARQ)](https://en.wikipedia.org/wiki/Automatic_repeat_request) protocol
can be used to implement reliable communication (Erlang message delivery) given the
above limitations.

A number of effects are available to the user.

### Sending a message

The `{send_msg, pid(), Msg :: term()}` effect asynchronously sends a message
to the specified `pid`.

`ra` uses `erlang:send/3` with the `no_connect` and `no_suspend`
options which is the least reliable way of doing it. It does this so
that a state machine `send_msg` effect will never block the main `ra` process.

To ensure message reliability, [Automatic Repeat Query (ARQ)](https://en.wikipedia.org/wiki/Automatic_repeat_request)-like protocols between the state machine and the receiver should be implemented
if needed.

### Monitoring

The `{monitor, process | node, pid() | node()}` effect will ask the `ra` leader to
monitor a process or node. If `ra` receives a `DOWN` for a process it
is monitoring it will commit a `{down,  pid(), term()}` command to the log that
the state machine needs to handle. If it detects a monitored node as down or up
it will commit a `{nodeup | nodedown, node()}` command to the log.

Use `{demonitor, process | node, pid() | node()}` to stop monitoring a process
or a node.

### Calling a function

The `{mod_call, module(), function(), Args :: [term()]}` to call an arbitrary
function. Care need to be taken not to block the `ra` process whilst doing so.
It is recommended that expensive operations are done in another process.

The `mod_call` effect is useful for e.g. updating an ETS table of committed entries
or similar.

### Updating the Release Cursor (Snapshotting)

The `{release_cursor, RaftIndex, MachineState}`
effect can be used to give Ra cluster members a hint to trigger a snapshot.
This effect, when emitted, is evaluated on all nodes and not just the leader.

It is not guaranteed that a snapshot will be taken. A decision to take
a snapshot or delay it is taken using a number of internal Ra state factors.
The goal is to minimise disk I/O activity when possible.



## Identity

Since Ra assumes multiple clusters running side by side in a given Erlang cluster,
each cluster and cluster member must have their own identities.

### Cluster Name

A cluster name in Ra is defined as `binary() | string() | atom()`.

The cluster name is mostly a "human-friendly" name for a Ra cluster.
Something that identifies the entity the cluster is meant to represent.
The cluster name isn't strictly part of a clusters identity.

For example, in RabbitMQ's quorum queues case cluster names are derived from queue's identity.

### Server ID

A Ra server is a Ra cluster member. Server ID is defined as a pair of `{atom(), node()}`.
Server ID combines a locally registered name and the Erlang node it resides on.

Since server IDs identify Ra cluster members, they need to be a
persistent addressable value that can be used as a target for sending
messages. A `pid()` would not work as it isn't persisted across
process restarts.

While it's more common for each server within a Ra cluster to
be started on a separate Erlang node, `ra` also supports the scenario
where multiple Ra servers within a cluster are placed on the same
Erlang node.

Ra servers are locally registered process to avoid depending on a
distributed registry (such as the `global` module) which does not
provide the same consistency and recovery guarantees.

### UID

Each Ra server also needs an ID that is unique to the local Erlang
node _and_ unique across incarnations of `ra` clusters with the same
cluster ID.

This is to handle the case where a Ra cluster with the same name is
deleted and then re-created with the same cluster id and server ids
shortly after. In this instance the write ahead log may contain
entries from the previous incarnation which means we could be mixing
entries written in the previous incarnation with ones written in the
current incarnation which obviously is unacceptable. Hence providing a
unique local identity is critical for correct operation.

The IDs are used to identify a Ra server's data on disk and must be
filename-safe and conform to the [base64url
standard](https://tools.ietf.org/html/rfc4648#section-5)

Ra cluster members also use IDs that are unique to that member.
The IDs assume Base64 URI-encoded binaries that can be safely used
as directory and file name values on disk.

When `ra:start_server/4` or `ra:start_cluster/3` are invoked, an UID
is automatically generated by Ra.

This is used for interactions with the write ahead log,
segment and snapshot writer processes who use the `ra_directory` module to
lookup the current `pid()` for a given ID.


A UID can be user-provided. Values that conform to the [base64url
standard](https://tools.ietf.org/html/rfc4648#section-5) must be used.


Here's an example of a compliant user-provided UID:


```
Config = #{cluster_name => <<"ra-cluster-1">>,
           server_id => {ra_cluster_1, ra1@snowman},
           uid => <<"ra_cluster_1_1519808362841">>
           ...},

```


## Raft Extensions and Deviations

Ra aims to fit well within the Erlang environment as well as provide good adaptive throughput.
Therefore it has deviated from the original Raft protocol in certain areas.

### Replication

Log replication in Ra is mostly asynchronous, so there is no actual use of RPC (as in the Raft paper) calls.
New entries are pipelined and followers reply after receiving a written event which incurs
a natural batching effects on the replies.

Followers include 3 non-standard fields in their Raft `AppendEntries` RPC replies:

* `last_index`, `last_term` - the index and term of the last fully written entry. The leader uses these
   to calculate the new `commit_index`.

* `next_index` - this is the next index the follower expects. For successful replies it is not set,
   or is ignored by the leader. It is set for unsuccessful replies and is used by the leader to update its `next_index`
   for the follower and resend entries from this point.

To avoid completely overwhelming a slow follower the leader will only
pipeline if the difference between the `next_index` and `match_index` is
below some limit (currently set to 1000).

Follower that are considered stale (i.e. the match_index is less then next_index - 1) are still
sent an append entries message periodically, although less frequently
than recommended in the Raft paper. This is done to ensure follower
liveness. In an idle system where all followers are in sync no further
messages will be sent to reduce network bandwidth usage.


### Failure Detection

Ra doesn't rely the Raft paper's approach to peer failure detection
where the leader periodically sends append entries messages to enforce its leadership.

Ra is designed to support potentially thousands of concurrently running clusters within an
Erlang cluster and having all these doing their own failure detection
has proven excessive in terms of bandwidth usage.

In Ra leaders will not send append entries unless there is an update to be
sent it means followers don't (typically) set election timers.

This leaves the question on how failures are detected and elections
are triggered.

Ra tries to make use as much of native Erlang failure detection
facilities as it can. Process or node failure scenario are handled using
Erlang monitors. Followers monitor the currently elected leader and if they receive a
'DOWN' message as they would in the case of a crash or sustained
network partition where Erlang distribution detects a node isn't
replying the follower _then_ sets a short, randomised election
timeout.

This only works well in crash-stop scenarios. For network partition
scenarios it would rely on Erlang distribution's net ticks mechanism to detect the partition
which could easily take 30-60 seconds by default to happen which is too slow.

The `ra` application uses a separate [node failure
detection library](https://github.com/rabbitmq/aten) developed alongside it.

The library, `aten`, monitors Erlang nodes. When it suspects an Erlang node is down
it notifies local `ra` servers of this. If this Erlang node hosts the currently
known `ra` leader the follower will start an election.

Ra implements a ["pre-vote" member state](https://ramcloud.stanford.edu/~ongaro/thesis.pdf)
that sits between the "follower" and "candidate" states.
This avoids cluster disruptions due to leader failure false positives.
