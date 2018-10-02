# Ra: a Raft Implementation for Erlang and Elixir

## What is This

Ra is a [Raft](https://ramcloud.stanford.edu/~ongaro/thesis.pdf) implementation
by Team RabbitMQ. It is not tied to RabbitMQ and can be used in any Erlang or Elixir
project. It is, however, heavily inspired by and geared towards RabbitMQ needs.

Requires OTP-20 or above.


## Design Goals

 * Low footprint: use as few resources as possible, avoid process tree explosion
 * Able to run thousands of `ra` clusters within an Erlang node
 * Provide adequate performance for use as a basis for a distributed data service


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


## Use Cases

This library is primarily developed as the foundation for replication layer for
replicated queues in a future version of RabbitMQ. The design it aims to replace uses
a variant of [Chain Based Repliction](https://www.cs.cornell.edu/home/rvr/papers/OSDI04.pdf)
which has two major shortcomings:

 * Replication algorithm is linear
 * Failure recovery procedure requires expensive topology changes

## Documentation

* APO docs: https://rabbitmq.github.io/ra/
* How to write a ra statemachine: [doc/STATEMACHINE.md](doc/STATEMACHINE.md)
* Log implementation: [doc/LOG.md](doc/LOG.md)

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



Example:

```
[{data_dir, "/tmp/ra-data"},
 {wal_max_size_bytes, 134217728},
 {wal_compute_checksums, true},
 {wal_write_strategy, default},
]
```

## Internals

See: [doc/INTERNALS.md](doc/INTERNALS.md)

## Copyright and License

(c) 2017, Pivotal Software Inc.

Double licensed under the ASL2 and MPL1.1.
See [LICENSE](./LICENSE) for details.
