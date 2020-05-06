# A Raft Implementation for Erlang and Elixir

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

### Build Status

[![Build Status](https://travis-ci.org/rabbitmq/ra.svg?branch=master)](https://travis-ci.org/rabbitmq/ra)

## Supported Erlang/OTP Versions

Ra requires Erlang/OTP 21.3+. Erlang 22+ is **highly recommended**
because of [distribution traffic fragmentation](http://blog.erlang.org/OTP-22-Highlights/).

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
%% The Ra application has to be started before it can be used.
ra:start(),

%% All servers in a Ra cluster are named processes on Erlang nodes.
%% The Erlang nodes must have distribution enabled and be able to
%% communicate with each other.
%% See https://learnyousomeerlang.com/distribunomicon if you are new to Erlang/OTP.

%% These Erlang nodes will host Ra nodes. They are the "seed" and assumed to
%% be running or come online shortly after Ra cluster formation is started with ra:start_cluster/3.
ErlangNodes = [ra1@hostname.local, ra2@hostname.local, ra3@hostname.local],

%% This will check for Erlang distribution connectivity. If Erlang nodes
%% cannot communicate with each other, Ra nodes would not be able to cluster or communicate
%% either.
[io:format("Attempting to communicate with node ~s, response: ~s~n", [N, net_adm:ping(N)]) || N <- ErlangNodes],

%% Create some Ra server IDs to pass to the configuration. These IDs will be
%% used to address Ra nodes in Ra API functions.
ServerIds = [{quick_start, N} || N <- ErlangNodes],

%% Start a simple distributed addition state machine with an initial state of 0
ClusterName = quick_start,
%% State machine that implements the logic
Machine = {simple, fun erlang:'+'/2, 0},
{ok, ServersStarted, _ServersNotStarted} = ra:start_cluster(ClusterName, Machine, ServerIds),

%% Add a number to the state machine.
%% Simple state machines always return the full state after each operation.
{ok, StateMachineResult, LeaderId} = ra:process_command(hd(ServersStarted), 5),

%% Use the leader id from the last command result for the next one
{ok, 12, LeaderId1} = ra:process_command(LeaderId, 7).
```

"Simple" state machines like the above can only take you so far. See [Ra state machine tutorial](docs/internals/STATE_MACHINE_TUTORIAL.md)
for how to write a state machine by implementing the `ra_machine` behaviour.

## Design Goals

 * Low footprint: use as few resources as possible, avoid process tree explosion
 * Able to run thousands of `ra` clusters within an Erlang node
 * Provide adequate performance for use as a basis for a distributed data service

## Use Cases

This library is primarily developed as the foundation for replication layer for
replicated queues in a future version of RabbitMQ. The design it aims to replace uses
a variant of [Chain Based Replication](https://www.cs.cornell.edu/home/rvr/papers/OSDI04.pdf)
which has two major shortcomings:

 * Replication algorithm is linear
 * Failure recovery procedure requires expensive topology changes

## Documentation

* API docs: https://rabbitmq.github.io/ra/
* How to write a Ra state machine: [Ra state machine tutorial](docs/internals/STATE_MACHINE_TUTORIAL.md)
* Design and implementation details: [Ra internals guide](docs/internals/INTERNALS.md)

### Examples

A number of examples can be found in a [separate repository](https://github.com/rabbitmq/ra-examples).

## Configuration

* `data_dir`:

A directory name where `ra` will store it's data.

* `wal_data_dir`:

A directory name where `ra` will store it's WAL (Write Ahead Log) data. If
unspecified, `data_dir` is used.

* `wal_max_size_bytes`:

The maximum size of the WAL in bytes. Default: 512Mb.

* `wal_compute_checksums`:

Indicate whether the wal should compute and validate checksums. Default: true

* `wal_write_strategy`:
    - `default`:

    The default. Actual `write(2)` system calls are delayed until a buffer is
    due to be
    flushed. Then it writes all the data in a single call then fsyncs. Fastest but
    incurs some additional memory use.

    - `o_sync`:

    Like `default` but will try to open the file with `O_SYNC` and thus wont
    need the additional `fsync(2)` system call. If it fails to open the file with this
    flag this mode falls back to `default`

* `wal_sync_method`:
    - `datasync`:

    The default. Uses the fdatasync system call after each batch. This avoids flushing
    file meta-data after each write batch and thus may be slightly faster than
    `sync` on some system. When datasync is configured the wal will try to
    pre allocate the entire WAL file.
    NB: not all systems support fdatasync. Please consult system
    documentation and configure it to use sync instead if it is not supported.

    - `sync`:

    Uses the fsync system call after each batch.

* `wal_max_batch_size`:

Controls the internal max batch size that the WAL will accept. Higher numbers may
result in higher memory use. Default: 32768.

* `logger_module`:

Allows the configuration of a custom logger module. The default is `logger`.
The module must implement a function of the same signature
as [logger:log/4](http://erlang.org/doc/man/logger.html#log-4) (the variant
that takes a format not the variant that takes a fun).

* `metrics_key`:

Metrics key. The key used to write metrics into the `ra_metrics` table.

* `low_priority_commands_flush_size`:

When commands are pipelined using the low priority mode Ra tries to hold them
back in favour of normal priority commands. This setting determines the number
of low priority commands that are added to the log each flush cycle. Default: 25

```
[{data_dir, "/tmp/ra-data"},
 {wal_max_size_bytes, 134217728},
 {wal_compute_checksums, true},
 {wal_write_strategy, default},
]
```

### Logging

Ra will use default OTP `logger` by default, unless `logger_module` configuration key is used to override.

To change log level to `debug` for all applications, use

``` erl
logger:set_primary_config(level, debug).
```


## Copyright and License

(c) 2017-2020, VMware Inc or its affiliates.

Double licensed under the ASL2 and MPL1.1.
See [LICENSE](./LICENSE) for details.
