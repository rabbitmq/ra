# Ra: A Raft Implementation for Erlang and Elixir

[![Hex.pm](https://img.shields.io/hexpm/v/ra)](https://hex.pm/packages/ra/)
![Actions](https://github.com/rabbitmq/ra/actions/workflows/erlang.yml/badge.svg)

Ra is a [Raft](https://raft.github.io/) implementation
by Team RabbitMQ. It is not tied to RabbitMQ and can be used in any Erlang or Elixir
project. It is, however, heavily inspired by and geared towards RabbitMQ needs.

Ra (by virtue of being a Raft implementation) is a library that allows users to implement [persistent, fault-tolerant and replicated state machines](https://en.wikipedia.org/wiki/State_machine_replication).

## Project Maturity

This library has been extensively tested and is suitable for production use.
This means the primary APIs (`ra`, `ra_machine` modules) and on disk formats
will be backwards-compatible going forwards in line with Semantic Versioning.
Care has been taken to version all on-disk data formats to enable frictionless
future upgrades.

### Status

The following Raft features are implemented:

 * Leader election
 * Log replication
 * Cluster membership changes: one server (member) at a time
 * Log compaction (with limitations and RabbitMQ-specific extensions)
 * Snapshot installation

### Safety Verification

Ra is [continuously tested](https://github.com/rabbitmq/ra-kv-store#jepsen-test) with the [Jepsen](https://github.com/jepsen-io/jepsen)
distributed system verification framework.

## Supported Erlang/OTP Versions

Ra supports the following Erlang/OTP versions:

 * `27.x`
 * `26.x`

Modern Erlang releases provide [distribution traffic fragmentation](https://www.erlang.org/blog/otp-22-highlights/#fragmented-distribution-messages)
which algorithms such as Raft significantly benefit from.


## Design Goals

 * Low footprint: use as few resources as possible, avoid process tree explosion
 * Able to run thousands of `ra` clusters within an Erlang node
 * Provide adequate performance for use as a basis for a distributed data service

## Use Cases

This library was primarily developed as the foundation of a replication layer for
[quorum queues](https://rabbitmq.com/quorum-queues.html) in RabbitMQ, and today
also powers [RabbitMQ streams](https://rabbitmq.com/streams.html) and [Khepri](https://github.com/rabbitmq/khepri). 

The design it aims to replace uses
a variant of [Chain Based Replication](https://www.cs.cornell.edu/home/rvr/papers/OSDI04.pdf)
which has two major shortcomings:

 * Replication algorithm is linear
 * Failure recovery procedure requires expensive topology changes

## Smallest Possible Usage Example

The example below assumes a few things:

 * You are familiar with the basics of [distributed Erlang](https://learnyousomeerlang.com/distribunomicon)
 * Three Erlang nodes are started on the local machine or reachable resolvable hosts.
   Their names are `ra1@hostname.local`, `ra2@hostname.local`, and `ra3@hostname.local` in the example below but your actual hostname will be different.
   Therefore the naming scheme is `ra{N}@{hostname}`. This is not a Ra requirement so you are
   welcome to use different node names and update the code accordingly.

Erlang nodes can be started using `rebar3 shell --name {node name}`. They will have Ra modules
on code path:

``` shell
# replace hostname.local with your actual hostname
rebar3 shell --name ra1@hostname.local
```

``` shell
# replace hostname.local with your actual hostname
rebar3 shell --name ra2@hostname.local
```

``` shell
# replace hostname.local with your actual hostname
rebar3 shell --name ra3@hostname.local
```

After Ra nodes form a cluster, state machine commands can be performed.

Here's what a small example looks like:

``` erlang
%% All servers in a Ra cluster are named processes on Erlang nodes.
%% The Erlang nodes must have distribution enabled and be able to
%% communicate with each other.
%% See https://learnyousomeerlang.com/distribunomicon if you are new to Erlang/OTP.

%% These Erlang nodes will host Ra nodes. They are the "seed" and assumed to
%% be running or come online shortly after Ra cluster formation is started with ra:start_cluster/4.
ErlangNodes = ['ra1@hostname.local', 'ra2@hostname.local', 'ra3@hostname.local'],

%% This will check for Erlang distribution connectivity. If Erlang nodes
%% cannot communicate with each other, Ra nodes would not be able to cluster or communicate
%% either.
[io:format("Attempting to communicate with node ~s, response: ~s~n", [N, net_adm:ping(N)]) || N <- ErlangNodes],

%% The Ra application has to be started on all nodes before it can be used.
[rpc:call(N, ra, start, []) || N <- ErlangNodes],

%% Create some Ra server IDs to pass to the configuration. These IDs will be
%% used to address Ra nodes in Ra API functions.
ServerIds = [{quick_start, N} || N <- ErlangNodes],

ClusterName = quick_start,
%% State machine that implements the logic and an initial state
Machine = {simple, fun erlang:'+'/2, 0},

%% Start a Ra cluster with an addition state machine that has an initial state of 0.
%% It's sufficient to invoke this function only on one Erlang node. For example, this
%% can be a "designated seed" node or the node that was first to start and did not discover
%% any peers after a few retries.
%%
%% Repeated startup attempts will fail even if the cluster is formed, has elected a leader
%% and is fully functional.
{ok, ServersStarted, _ServersNotStarted} = ra:start_cluster(default, ClusterName, Machine, ServerIds),

%% Add a number to the state machine.
%% Simple state machines always return the full state after each operation.
{ok, StateMachineResult, LeaderId} = ra:process_command(hd(ServersStarted), 5),

%% Use the leader id from the last command result for the next one
{ok, 12, LeaderId1} = ra:process_command(LeaderId, 7).
```

### Querying Machine State

Ra machines are only useful if their state can be queried. There are two types of queries:

 * Local queries return machine state on the target node
 * Leader queries return machine state from the leader node.
   If a follower node is queried, the query command will be redirected to the leader.

Local queries are much more efficient but can return out-of-date machine state.
Leader queries offer best possible machine state consistency but potentially require
sending a request to a remote node.

Use `ra:leader_query/{2,3}` to perform a leader query:

``` erlang
%% find current Raft cluster leader
{ok, _Members, LeaderId} = ra:members(quick_start),
%% perform a leader query on the leader node
QueryFun = fun(StateVal) -> StateVal end,
{ok, {_TermMeta, State}, LeaderId1} = ra:leader_query(LeaderId, QueryFun).
```

Similarly, use `ra:local_query/{2,3}` to perform a local query:

``` erlang
%% this is the replica hosted on the current Erlang node.
%% alternatively it can be constructed as {ClusterName, node()}
{ok, Members, _LeaderId} = ra:members(quick_start),
LocalReplicaId = lists:keyfind(node(), 2, Members),
%% perform a local query on the local node
QueryFun = fun(StateVal) -> StateVal end,
{ok, {_TermMeta, State}, LeaderId1} = ra:local_query(LocalReplicaId, QueryFun).
```

A query function is a single argument function that accepts current machine state
and returns any value (usually derived from the state).

Both `ra:leader_query/2` and `ra:local_query/2` return machine term metadata, a result returned by the query
function, and current cluster leader ID.


### Dynamically Changing Cluster Membership

Nodes can be added to or removed from a Ra cluster dynamically. Only one
cluster membership change at a time is allowed: concurrent changes
will be rejected by design.

In this example, instead of starting a "pre-formed" cluster,
a local server is started and then members are added by calling `ra:add_member/2`.

Start 3 Erlang nodes:

``` shell
# replace hostname.local with your actual hostname
rebar3 shell --name ra1@hostname.local
```

``` shell
# replace hostname.local with your actual hostname
rebar3 shell --name ra2@hostname.local
```

``` shell
# replace hostname.local with your actual hostname
rebar3 shell --name ra3@hostname.local
```

Start the ra application:

``` erlang
%% on ra1@hostname.local
ra:start().
% => ok
```

``` erlang
%% on ra2@hostname.local
ra:start().
% => ok
```

``` erlang
%% on ra3@hostname.local
ra:start().
% => ok
```

A single node cluster can be started from any node.

For the purpose of this example, `ra2@hostname.local` is used as the starting member:

``` erlang
ClusterName = dyn_members,
Machine = {simple, fun erlang:'+'/2, 0},

% Start a cluster
{ok, _, _} =  ra:start_cluster(default, ClusterName, Machine, [{dyn_members, 'ra2@hostname.local'}]).
```

After the cluster is formed, members can be added.

Add `ra1@hostname.local` by telling `ra2@hostname.local` about it
and starting a Ra replica (server) on `ra1@hostname.local` itself:

``` erlang
% Add member
{ok, _, _} = ra:add_member({dyn_members, 'ra2@hostname.local'}, {dyn_members, 'ra1@hostname.local'}),

% Start the server
ok = ra:start_server(default, ClusterName, {dyn_members, 'ra1@hostname.local'}, Machine, [{dyn_members, 'ra2@hostname.local'}]).
```

Add `ra3@hostname.local` to the cluster:

``` erlang
% Add a new member
{ok, _, _} = ra:add_member({dyn_members, 'ra2@hostname.local'}, {dyn_members, 'ra3@hostname.local'}),

% Start the server
ok = ra:start_server(default, ClusterName, {dyn_members, 'ra3@hostname.local'}, Machine, [{dyn_members, 'ra2@hostname.local'}]).
```

Check the members from any node:

``` erlang
ra:members({dyn_members, node()}).
% => {ok,[{dyn_members,'ra1@hostname.local'},
% =>      {dyn_members,'ra2@hostname.local'},
% =>      {dyn_members,'ra3@hostname.local'}],
% =>      {dyn_members,'ra2@hostname.local'}}
```

If a node wants to leave the cluster, it can use `ra:leave_and_terminate/3`
and specify itself as the target:

Temporarily add a new node, say `ra4@hostname.local`, to the cluster:

``` erlang
% Add a new member
{ok, _, _} = ra:add_member({dyn_members, 'ra2@hostname.local'}, {dyn_members, 'ra4@hostname.local'}),

% Start the server
ok = ra:start_server(default, ClusterName, {dyn_members, 'ra4@hostname.local'}, Machine, [{dyn_members, 'ra2@hostname.local'}]).

%% on ra4@hostname.local
ra:leave_and_terminate(default, {ClusterName, node()}, {ClusterName, node()}).

ra:members({ClusterName, node()}).
% => {ok,[{dyn_members,'ra1@hostname.local'},
% =>      {dyn_members,'ra2@hostname.local'},
% =>      {dyn_members,'ra3@hostname.local'}],
% =>      {dyn_members,'ra2@hostname.local'}}
```

### Other examples

See [Ra state machine tutorial](docs/internals/STATE_MACHINE_TUTORIAL.md)
for how to write more sophisticated state machines by implementing
the `ra_machine` behaviour.

A [Ra-based key/value store example](https://github.com/rabbitmq/ra-kv-store)
is available in a separate repository.


## Documentation

* [API reference](https://hex.pm/packages/ra)
* How to write a Ra state machine: [Ra state machine tutorial](docs/internals/STATE_MACHINE_TUTORIAL.md)
* Design and implementation details: [Ra internals guide](docs/internals/INTERNALS.md)

### Examples

* [Ra-based key/value store](https://github.com/rabbitmq/ra-kv-store)


## Configuration Reference

<table>
    <tr>
        <td>Key</td>
        <td>Description</td>
        <td>Data Type</td>
    </tr>
    <tr>
        <td>data_dir</td>
        <td>A directory name where Ra node will store its data</td>
        <td>Local directory path</td>
    </tr>
    <tr>
        <td>wal_data_dir</td>
        <td>
            A directory name where Ra will store it's WAL (Write Ahead Log) data.
            If unspecified, `data_dir` is used.
        </td>
        <td>Local directory path</td>
    </tr>
    <tr>
        <td>wal_max_size_bytes</td>
        <td>The maximum size of the WAL in bytes. Default: 512 MB</td>
        <td>Positive integer</td>
    </tr>
    <tr>
        <td>wal_max_entries</td>
        <td>The maximum number of entries per WAL file. Default: undefined</td>
        <td>Positive integer</td>
    </tr>
    <tr>
        <td>wal_compute_checksums</td>
        <td>Indicate whether the wal should compute and validate checksums. Default: `true`</td>
        <td>Boolean</td>
    </tr>
    <tr>
        <td>wal_sync_method</td>
        <td>
            <ul>
                <li>
                    <code>datasync</code>: used by default. Uses the <code>fdatasync(2)</code> system call after each batch. This avoids flushing
                    file meta-data after each write batch and thus may be slightly faster than sync on some system. When datasync is configured the wal will try to pre-allocate the entire WAL file.
                    Not all systems support <code>fdatasync</code>. Please consult system
                    documentation and configure it to use sync instead if it is not supported.
                </li>
                <li>
                    <code>sync</code>: uses the fsync system call after each batch.
                </li>
            </ul>
        </td>
        <td>Enumeration: <code>datasync</code> | <code>sync</code></td>
    </tr>
    <tr>
        <td>logger_module</td>
        <td>
            Allows the configuration of a custom logger module. The default is logger.
            The module must implement a function of the same signature
            as <a href="http://erlang.org/doc/man/logger.html#log-4">logger:log/4</a> (the variant that takes a format not the variant that takes a function).
        </td>
        <td>Atom</td>
    </tr>
    <tr>
        <td>wal_max_batch_size</td>
        <td>
            Controls the internal max batch size that the WAL will accept.
            Higher numbers may result in higher memory use. Default: 32768.
        </td>
        <td>Positive integer</td>
    </tr>
    <tr>
        <td>wal_hibernate_after</td>
        <td>
            Enables hibernation after a timeout of inactivity for the WAL process.
        </td>
        <td>Milliseconds</td>
    </tr>
    <tr>
        <td>metrics_key</td>
        <td>Metrics key. The key used to write metrics into the ra_metrics table.</td>
        <td>Atom</td>
    </tr>
    <tr>
        <td>low_priority_commands_flush_size</td>
        <td>
            When commands are pipelined using the low priority mode Ra tries to hold them
            back in favour of normal priority commands. This setting determines the number
            of low priority commands that are added to the log each flush cycle. Default: 25
        </td>
        <td>Positive integer</td>
    </tr>
    <tr>
        <td>segment_compute_checksums</td>
        <td>Indicate whether the segment writer should compute and validate checksums. Default: `true`</td>
        <td>Boolean</td>
    </tr>
</table>

## Logging

Ra will use default OTP `logger` by default, unless `logger_module`
configuration key is used to override.

To change log level to `debug` for all applications, use

``` erl
logger:set_primary_config(level, debug).
```

## Ra versioning

Ra attempts to follow [Semantic Versioning](https://semver.org/).

The modules that form part of the public API are:

* `ra`
* `ra_machine` (behaviour callbacks only)
* `ra_aux`
* `ra_system`
* `ra_counters` (counter keys may vary between minors)
* `ra_leaderboard`
* `ra_env`
* `ra_directory`
* `ra_flru`
* `ra_log_read_plan`

## Copyright and License

(c) 2017-2025 Broadcom. All Rights Reserved. The term "Broadcom" refers to
Broadcom Inc. and/or its subsidiaries.

Dual licensed under the Apache License Version 2.0 and
Mozilla Public License Version 2.0.

This means that the user can consider the library to be licensed under
**any of the licenses from the list** above. For example, you may
choose the Apache Public License 2.0 and include this library into a
commercial product.

See [LICENSE](./LICENSE) for details.
