# Ra: a Raft Implementation for Erlang and Elixir

## What is This

Ra is a [Raft](https://ramcloud.stanford.edu/~ongaro/thesis.pdf) implementation
by Team RabbitMQ. It is not tied to RabbitMQ and can be used in any Erlang or Elixir
project. It is, however, heavily inspired by and geared towards RabbitMQ needs.


## Design Goals

 * Low footprint: use as little resources as possible, avoid process tree explosion
 * Able to run thousands of `ra` clusters within an Erlang node
 * Provide adequate performance for use as a basis for a distributed data service


## Project Maturity

This library is **under heavy development**. **Breaking changes** to the API and on disk storage format
are likely.

### Status

The following Raft features are implemented:

 * Leader election
 * Log replication
 * Cluster membership changes: one node (member) at a time
 * Log compaction (with limitations and RabbitMQ-specific extensions)
 * Snapshot installation

There are two storage backends:

* `ra_log_memory`: an in-memory log backend useful for testing
* `ra_log_file`: a disk-based backend


## Use Cases

This library is primarily developed as the foundation for replication layer for
mirrored queues in a future version of RabbitMQ. The design it aims to replace uses
a variant of [Chain Based Repliction](https://www.cs.cornell.edu/home/rvr/papers/OSDI04.pdf)
which has two major shortcomings:

 * Replication algorithm is linear
 * Failure recovery procedure requires expensive topology changes


## Design

TBD

### Raft Extensions and Deviations

TBD


## Copyright and License

(c) 2017, Pivotal Software Inc.

Double licensed under the ASL2 and MPL1.1.
See [LICENSE](./LICENSE) for details.
