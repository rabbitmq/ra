# A Raft Implementation for Erlang and Elixir

Ra is a [Raft](https://raft.github.io/) implementation
by Team RabbitMQ. It is not tied to RabbitMQ and can be used in any Erlang or Elixir
project. It is, however, heavily inspired by and geared towards RabbitMQ needs.

Ra (by virtue of being a Raft implementation) is a library that allows users to implement [persistent, fault-tolerant and replicated state machines](https://en.wikipedia.org/wiki/State_machine_replication).

## Project Maturity

This library is maturing and is currently in a pre-1.0 phase. This means that
the primary APIs (`ra`, `ra_machine` modules) and on disk formats are unlikely
to change significantly until 1.0 is tagged but _may_ need to be if deemed
necessary.

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

Ra requires Erlang/OTP 21.3+.

## Quick start

```erlang
%% All servers in a Ra cluster are named processes.
%% Create some Server Ids to pass to the configuration
ErlangNodes = [ra@node1, ra@node2, ra@node3]
ServerIds = [{quick_start, N} || N <- ErlangNodes]

%% start a simple distributed addition state machine with an initial state of 0
{ok, ServersStarted, ServersNotStarted} = ra:start_cluster(quick_start, {simple, fun erlang:'+'/2, 0}, ServerIds),

%% Add a number to the state machine
%% Simple state machines always return the full state after each operation
{ok, StateMachineResult, LeaderId} = ra:process_command(hd(ServersStarted), 5),

%% use the leader id from the last command result for the next
{ok, 12, LeaderId1} = ra:process_command(LeaderId, 7),

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

* `wal_max_size_bytes`:

The maximum size of the WAL (Write Ahead Log). Default: 128Mb.

* `wal_compute_checksums`:

Indicate whether the wal should compute and validate checksums. Default: true

* `wal_write_strategy`:
    - `default`:

    The default. Actual `write(2)` system calls are delayed until a buffer is
    due to be
    flushed. Then it writes all the data in a single call then fsyncs. Fastest but
    incurs some additional memory use.

    - `do_sync`:

    Like `default` but will try to open the file with `O_SYNC` and thus wont
    need the additional `fsync(2)` system call. If it fails to open the file with this
    flag this mode falls back to `default`


* `logger_module`:

Allows the configuration of a custom logger module. The default is `logger`.
The module must implement a function of the same signature
as [logger:log/4](http://erlang.org/doc/man/logger.html#log-4) (the variant
that takes a format not the variant that takes a fun).

* `metrics_key`:

Metrics key. The key used to write metrics into the `ra_metrics` table.


```
[{data_dir, "/tmp/ra-data"},
 {wal_max_size_bytes, 134217728},
 {wal_compute_checksums, true},
 {wal_write_strategy, default},
]
```

## Copyright and License

(c) 2017-2019, Pivotal Software Inc.

Double licensed under the ASL2 and MPL1.1.
See [LICENSE](./LICENSE) for details.
