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

`ra` aims to fit well within the erlang enviroment as well as provide good adaptive throughput. Therefore it has deviated from the original Raft protocol in certain areas.

#### Replication

Log replication in Ra is mostly asynchronous, so there is no actual use of `rpc` calls.
New entries are pipelined and followers reply after receiving a written event which incurs a natural batching effects on the replies. Followers include 3 non-standard fields in their replies:

* `last_index`, `last_term` - the index and term of the last fully written entry. The leader uses this to calculate the new `commit_index`.

* `next_index` - this is the next index the follower expects. For successful replies it is not set, or is ignored by the leader. It is set for unsuccessful replies and is used by the leader to update it's `next_index` for the follower and resend entries from this point.

To avoid completely overwhelming a slow follower the leader will only pipeline if the distance between the `next_index` and `match_index` is below some limit (currently set to 1000). Follower that are considered stale (i.e. the match_index is less then next_index - 1) are still sent an append entries message periodically, although less frequently than a traditional Raft system. This is done to ensure follower liveness. In an idle system where all followers are in sync no further messages will be sent.


#### Failure detection

Ra doesn't use Raft's standard approach where the leader periodically sends append entries messages to enforce it's leadership. Ra is designed to support potentially thousands of `ra` clusters within an erlang cluster and having all these doing their own failure detection has proven unstable and also means unnecessary use of network. As leaders will not send append entries unless there is an update to be sent it means followers don't (typically) set election timers.

This leaves the question on how failures are detected and elections are triggered.

Ra tries to make use as much of native erlang failure detection facilities as it can. The crash scenario is trivial to handle using erlang monitors. Followers monitor leaders and if they receive a 'DOWN' message as they would in the case of a crash or sustained network partition where distributed erlang detects a node isn't replying the follower _then_ sets a short, randomised election timeout.

This only works well in crash-stop scenarios. For network partition scenarios it would rely on distributed erlang to detect the partition which could easily take up to a minute to happen which is too slow.

The `ra` application provides a node failure detector that uses monitors erlang nodes. When it suspects an erlang node is down it notifies local `ra` nodes of this. If this erlang node is the node of the currently known `ra` leader the follower will start an election.


## Copyright and License

(c) 2017, Pivotal Software Inc.

Double licensed under the ASL2 and MPL1.1.
See [LICENSE](./LICENSE) for details.
