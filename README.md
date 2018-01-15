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


#### Replication

Log replication in Ra is mostly asyncronous, so there is no actual use of `rpc` calls.
New entries are pipelined and followers reply after receiving a written event. Thus
not all append entries requests will receive a response. Followers include 3 non-standard fields in their replies:

* last_index, last_term - the index and term of the last fully written entry. The leader uses this to calculate the new commit_index.

* next_index - this is the next_index the follower expects. For successful replies it is not set, and is ignored by the leader. It is set for unsuccessful replies and is used by the leader to update it's next_index for the follower and resend entries from this point.

Being asyncronous Ra uses pipelining of entries heaviliy, i.e. it does not wait for a confirmation from a follower before it sends the next batch of entries and updates it's next_index for the follower. Thus it is sensitive to scenarios where a follower is partition or down and the leader keeps sending entries that never are confirmed. Ra implements a strategy whereby the leader will only allow the distance between a follower's match_index and next_index to become so large, say e.g. 10000. Once the distance reaches the configured limit it will stop pipelining entries and instead periodically send empty append entries messages at the current commit index. The periodic append entries is there to ensure the follower (who doesn't set an election timer unless it suspects something is wrong) receives some kind of update it can respond to.

when next_index and match_index+1 are the same no further entries are sent


Failure detections

Ra doesn't use Raft's standard approach where the leader periodically sends append entries messages to enforce it's leadership. Ra is designed to support potentially thousands of ra clusters within an erlang cluster and having all these doing their own failure detection has proven unstable and also means unnecessary use of network. As leaders will not send append entries unless these is an update to be sent it meas followers don't (typically) set election timers.

So how are elections triggered?

Ra tries to make use as much of native erlang failure detection facilities as it can. The crash scenario is trivial to handle using erlang monitors. Followers monitor leaders and if they receive a 'DOWN' message as they would in the case of a crash or sustained network partition where distributed erlang detects a node isn't replying the follower _then_ sets a short, randomised election timeout.

This only works well in crash-stop scenarios. For network partition scenarios it would rely on distributed erlang to detect the partition which could easily take up to a minute to happen which is too slow.

The `ra` application provides a node failure detector that uses an adaptive algorithm to monitor erlang nodes that adjusts itself to network conditions. When the likelyhood of a node failure breaches the configurable limit the detector will notify ra nodes on it's local erlang node that a particular erlang node appears down, or otherwise unavailable. If this erlang nodes is the node the currently known ra leader nodes is on the follower will start a pre-vote election. 

Pre-vote - as stability is paramount - `ra` implements the pre-vote part of the Raft protocol. I.e. before incrementing it's term in response to a potential failure it will perform a pre-vote to check if it is likely to be elected leader should it trigger a full election.







CRASH -> straight to candidate
NODE_MAYBE_DOWN -> enter pre-vote loop



Scenarios:

A[L], B, p(C)
A reaches pipeline distance limit stops pipelinind
Partition is healed before C detects erlnode is partitioned.
A sends empty AE after some time
C replies with success=false and the next_index it expects (+ last_index)
//TODO: is there a realistic scenario where next_index and match_index _still_
// are too far apart here? Need to ensure follower has processed all pending
// written events before replying
A rewinds to next_index and re-enables pipelining.

A[L], B, p(C) ->
A is still pipelining
C detects A is partition and enters pre-vote (no success)
p(C) -> C
A receives pre-vote - declines (leader always declines pre-votes?)
C receives AE - replies success=false to rewind next_index
A rewinds next_index and all is well


p(A[L]), B, C ->
B detects A erlnode maybe down -> enters pre-vote
C replies with pre-vote = success
B starts election and is elected leader
p(A[L]) -> A[L] - leaders receives append entries with higher term and abdicates


TBD


## Copyright and License

(c) 2017, Pivotal Software Inc.

Double licensed under the ASL2 and MPL1.1.
See [LICENSE](./LICENSE) for details.
