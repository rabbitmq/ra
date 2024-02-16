# Writing a State Machine

For `ra` to do anything useful you need to provide it with a state machine
implementation that solves a particular problem.

To implement a state machine that will be replicated using Raft and `ra`, implement the
`ra_machine` behaviour. There are two mandatory callbacks that need to be
implemented:

```erlang
-callback init(Conf :: machine_init_args()) -> state().

-callback 'apply'(command_meta_data(), command(), State) ->
    {State, reply(), effects() | effect()} | {State, reply()}.
```

`init/1` returns the initial state when a new instance of the state machine
is created. It takes an arbitrary map of configuration parameters.

`apply/3` is the primary function that is called for every command in the
raft log. It takes a meta data map containing the raft index and term (more on that later),
a command and the
current state and returns the new state, effects _and_ a reply that can be returned
to the caller _if_ they issued a synchronous call (see: `ra:process_command/2`).

There are also some optional callbacks that advanced state machines may choose to
implement.

## A simple KV Store

This example builds a simple key-value store that supports
`write` and `read` (or put and get) operations.

### Writing the Store

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

To implement `init/1` simply return an empty map as the initial state of our kv store.

```erlang
init(_Config) -> #{}.
```

To implement the `apply/3` function we need to handle each of the commands
we support.

```erlang
apply(_Meta, {write, Key, Value}, State) ->
    {maps:put(Key, Value, State), ok, _Effects = []};
apply(_Meta, {read, Key}, State) ->
    Reply = maps:get(Key, State, undefined),
    {State, Reply, _Effects = []}.
```

For the `{write, Key, Value}` command we simply put the key and value into the
map and return the new state, pass through the list of effects and an `ok`
return value.

For `{read, Key}` we additionally return the value of the key or `undefined` if
it does not exist so that a waiting caller can obtain the value.

And that is it! The state machine is finished.


### Running the state machine inside `ra`

To actually run this we need to configure a ra cluster to use the `ra_kv`
state machine and start it. The simplest way is to use the `ra:start_cluster/3`
function. It takes a ClusterName that can be a binary, string or atom,
a machine configuration and a list of servers that define the initial
set of members.

```erlang
start() ->
    %% the initial cluster members
    Members = [{ra_kv1, node()}, {ra_kv2, node()}, {ra_kv3, node()}],
    %% an arbitrary cluster name
    ClusterName = <<"ra_kv">>,
    %% the config passed to `init/1`, must be a `map`
    Config = #{},
    %% the machine configuration
    Machine = {module, ?MODULE, Config},
    %% ensure ra is started
    application:ensure_all_started(ra),
    %% start a cluster instance running the `ra_kv` machine
    ra:start_cluster(ClusterName, Machine, Members).
```

If you then start an erlang shell with `make shell` or similar and call
`ra_kv:start/0` you should hopefully be returned with something like:

```erlang
{ok,[{ra_kv3,nonode@nohost},
     {ra_kv2,nonode@nohost},
     {ra_kv1,nonode@nohost}],
    []}
```

Indicating that all servers in the `ra` cluster were successfully started. The
last element of the tuple would contain the servers that were not successfully
started. If a quorum of servers could not be started the function would return
and error.

Now you can write your first value into the cluster.

```erlang
2> ra:process_command(ra_kv1, {write, k, v}).
{ok, ok, {ra_kv1,nonode@nohost}}
3> ra:process_command(ra_kv1, {read, k}).
{ok, v, {ra_kv1,nonode@nohost}}
4> ra:process_command(ra_kv1, {write, k, v2}).
{ok, ok, {ra_kv1,nonode@nohost}}
5> ra:process_command(ra_kv1, {read, k}).
{ok, v2, {ra_kv1,nonode@nohost}}
```

`ra:process_command/2` blocks until the command has achieved consensus
and has been applied to the state machine on the leader server. It is the simplest
way to interact with `ra` but also the one with the highest latency.
To read values consistently we have no choice other than to use it.
The return tuple has either the raft index and term the entry was added to the
raft log _or_ the return value optionally returned by the state machine. The
`{read, Key}` command returns the current value of the key.

### Providing a client API

We have already added the `start/0` function to start a local ra cluster. It would
make sense to abstract interactions with the kv store behind a nicer interface
than calling `ra:process_command/2` directly.

```erlang
write(Key, Value) ->
    %% it would make sense to cache this to avoid redirection costs when this
    %% server happens not to be the current leader
    Server = ra_kv1,
    case ra:process_command(Server, {write, Key, Value}) of
        {ok, _, _} ->
            ok;
        Err ->
            Err
    end.

read(Key) ->
    Server = ra_kv1,
    case ra:process_command(Server, {read, Key}) of
        {ok, Value, _} ->
            {ok, Value};
        Err ->
            Err
    end.
```

## Effects

Effects are used to separate the state machine logic from the side effects it wants
to take inside it's environment. Each call to the `apply/3` function can return
a list of effects for the leader to realise. This includes sending messages,
setting up server and process monitors and calling arbitrary functions.

Effects should be a list sorted by execution order, i.e. the effect to be actioned
first should be at the head of the list.

Only the leader that first applies an entry will attempt the effect.
Followers process the same set of commands but simply throw away any effects
returned by the state machine unless specific effect provide the `local` option.


### Send a message

The `{send_msg, pid(), Msg :: term()}` effect asynchronously sends a message
to the specified
`pid`. Note that `ra` uses `erlang:send/3` with the `no_connect` and `no_suspend`
options which are the least reliable message sending options. It does this so
that a state machine `send_msg` effect will never block the main `ra` process.
To ensure message reliability normal [Automatic Repeat Query (ARQ)](https://en.wikipedia.org/wiki/Automatic_repeat_request)
like protocols between the state machine and the receiver should be implemented
if needed.

The `{send_msg, pid(), Msg :: term(), Options :: list()}` effect can be used to further
control how the effect is executed via options. The options are:

* `ra_event`: the message will be wrapped in a `ra_event` structure like
    `{ra_event, LeaderId, Msg}`. The `ra_event` format can be changed by providing
    a `ra_event_formatter` server configuration.
* `cast`: the message will be wrappen in a standard `{$gen_cast, Msg}` wrapper
    to conform to OTP `gen` conventions.
* `local`: if the target `pid()` is local to any member of the Ra cluster the delivery
of the message will be done from the local member even if this is a follower.
This way a network hop can be avoided. If the target `pid()` is on a node
without a Ra member it will be sent from the leader.

### Monitors

Use `{monitor, process | node, pid() | node()}` to ask the `ra` leader to
monitor a process or node. If `ra` receives a `DOWN` for a process it
is monitoring it will commit a `{down,  pid(), term()}` command to the log that
the state machine needs to handle. If it detects a monitored node as down or up
it will commit a `{nodeup | nodedown, node()}` command.

Use `{demonitor, process | node, pid() | node()}` to stop monitoring a process
or a node.

All monitors are invalidated when the leader changes. State machines should
re-issue monitor effects when becoming leader using the `state_enter/2`
callback.

### Call a function

Use the `{mod_call, module(), function(), Args :: [term()]}` to call an arbitrary
function. Care need to be taken not to block the `ra` process whilst doing so.
It is recommended that expensive operations are done in another process.

The `mod_call` effect is useful for e.g. updating an ETS table of committed entries
or similar.

### Setting a timer

The `{timer, Name :: term(), Time :: non_neg_integer() | infinity}` effects asks the Ra leader
to maintain a timer on behalf of the state machine and commit a `timeout` command
when the timer triggers. If setting the time to `infinity`, the timer will not be started
and any running timer with same name will be cancelled.

The timer is relative and setting another timer with the same name before the current 
timer runs out results in the current timer being reset.

All timers are invalidated when the leader changes. State machines should
re-issue timer effects when becoming leader using the `state_enter/2`
callback.

### Reading a log

Use `{log, Indexes :: [ra_index()], fun(([user_command()]) -> effects()}` to read
commands from the log from the specified indexes and return a list of effects.

Effectively this effect transforms log entries into effects.

Potential use cases could be when a command contains large binary data and you
don't want to keep this in memory but load it on demand when needed for a side-effect.

This is an advanced feature and will only work as long as the command is still
in the log. If a `release_cursor` has been emitted with an index higher than this,
the command may no longer be in the log and the function will not be called.

There is currently no facility for reading partial data from a snapshot.

### Updating the Release Cursor (Snapshotting)

The `{release_cursor, RaftIndex, MachineState}`
effect can be used to give Ra cluster members a hint to trigger a snapshot.
This effect, when emitted, is evaluated on all nodes and not just the leader.

It is not guaranteed that a snapshot will be taken. A decision to take
a snapshot or to delay it is taken using a number of internal Ra state factors.
The goal is to minimise disk I/O activity when possible.

### Checkpointing

Checkpoints are nearly the same concept as snapshots. Snapshotting truncates
the log up to the snapshot's index, which might be undesirable for machines
which read from the log with the `{log, Indexes, Fun}` effect mentioned above.

The `{checkpoint, RaftIndex, MachineState}` effect can be used as a hint to
trigger a checkpoint. Like snapshotting, this effect is evaluated on all nodes
and when a checkpoint is taken, the machine state is saved to disk and can be
used for recovery when the machine restarts. A checkpoint being written does
not trigger any log truncation though.

The `{release_cursor, RaftIndex}` effect can then be used to promote any
existing checkpoint older than or equal to `RaftIndex` into a proper snapshot,
and any log entries older than the checkpoint's index are then truncated.

These two effects are intended for machines that use the `{log, Indexes, Fun}`
effect and can substantially improve machine recovery time compared to
snapshotting alone, especially when the machine needs to keep old log entries
around for a long time.

## Optional `ra_machine` callbacks

Apart from the mandatory callbacks Ra supports some optional callbacks

* `state_enter(ra_server:ra_state() | eol, state()) -> effects().`

    This callback allows the ra machine impl to react to state changes in the ra
    server. The most common use of this callback is to re-issue monitor effects
    when entering the leader state as these are transient and when the leader
    changes they need to be issued again. This callback is called on all members
    in a cluster whenever they change their local state.
* `tick(TimeMs :: milliseconds(), state()) -> effects().`
    
    This callback is called periodically the interval of which  is controlled
    by the `tick_interval` server configuration. This callback can be used to
    trigger periodic actions.
* `init_aux/1`
* `handle_aux/5`

    ```
    -callback init_aux(Name :: atom()) -> term().

    -callback handle_aux(ra_server:ra_state(),
                         {call, From :: from()} | cast,
                         Command :: term(),
                         AuxState,
                         State) ->
        {reply, Reply :: term(), AuxState, State} |
        {reply, Reply :: term(), AuxState, State, effects()} |
        {no_reply, AuxState, State} |
        {no_reply, AuxState, State, effects()}
          when AuxState :: term(),
               State :: ra_aux:state().
   ```

    These two callbacks allow each server to maintain a local state machine
    in addition to the replicated and consistent state machine. They can be
    interacted with two ways:
    1. Using the `ra:aux_*` APIs to cast or call into the aux machine.
    2. By emitting `aux` effects from the state machine.

    Aux machines allow implementors a lot of flexibility that isn't otherwise
    offered by the more restrictive and deterministic state machine itself.

    The `ra_aux` module can be used to access internal Ra server state information
    such as the current members, the state machine state itself or even reading
    from the log.

* `overview(state()) -> map()`

    Use this to return an overvciew map the state machine which is returned
    when using e.g. `sys:get_status/1`
* `version/0`, `which_module/1`
    
    See the next section on State Machine Versioning


## State Machine Versioning

It is eventually necessary to make changes to the state machine
code. Any changes to a state machine that would result in a different end state
when the state is re-calculated from the log of entries (as is done when
restarting a ra server) should be considered breaking.

As Ra state machines need to be deterministic any changes to the logic inside
the `apply/3` function
 _needs to be enabled at the same index on all members of a Ra cluster_.

### Versioning API

Ra considers all state machines versioned starting with version 0. State machines
that need to be updated with breaking changes need to implement the optional
versioning parts of the `ra_machine` behaviour:

``` erlang
-type version() :: non_neg_integer().

-callback version() -> pos_integer().

-callback which_module(version()) -> module().

```

`version/0` returns the current version which is an integer that is
higher than any previously used version number. Whenever a breaking change is
made this should be incremented.

`which_module/1` maps a version to the module implementing it. This allows
developers to optionally keep entire modules for old versions instead of trying
to handle multiple versions in the same module.

E.g. when moving from version 0 of `my_machine` to version 1:

1. Copy and rename the `my_machine` module to `my_machine_v0`

2. Implement the breaking changes in the original module and bump the version.

``` erlang
version() -> 1.

which_module(1) -> my_machine;
which_module(0) -> my_machine_v0.

```

This would ensure that any entries added to the log are applied against the active machine version
at the time they were added, leading to a deterministic outcome.

For smaller (but still breaking) changes that can be handled in the original
module it is also possible to switch based on the `machine_version` key included in the meta
data passed to `apply/3`.

### Runtime Behaviour

New versions are enabled whenever there is a quorum of members with a higher
version and one of them is elected leader. The leader will commit the new version
to the log and each follower will move to the new version when this log entry
is applied. Followers that do not yet have the new version available will
receive log entries from the leader and update their logs but will not apply
log entries. When they are upgraded and have the new version, all outstanding
log entries will be applied. In practical terms this means that Ra nodes can be
upgraded one by one.

In order to be upgradeable, the state machine implementation will need to handle
the version bump in the form of a command that is passed to the `apply/3` callback:
`{machine_version, OldVersion, NewVersion}`. This provides an
opportunity to transform the state data into a new form, if needed. Note that the version
bump may be for several versions so it may be necessary to handle multiple
state transformations.
