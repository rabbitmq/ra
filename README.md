### RA

A [raft](https://ramcloud.stanford.edu/~ongaro/thesis.pdf) library with the
following design goals:

* Light-weight
    - As little resources used as possible, e.g. no process explosion.
    - The current implementation uses a single process for followers and two processes
for leaders: the main raft state machine and a  single "proxy" process to send
`AppendEntryRpc`s  to followers.
* Scalable
    - able to run thousands of `ra` clusters within the same erlang system
* Provide adequate performance for use as a basis for a distributed queue.



One of the potential uses for this library is as the replication layer for
RabbitMQ mirrored queues. The current design of RabbitMQ mirrored queues uses
a variant of [Chain Based Repliction](https://www.cs.cornell.edu/home/rvr/papers/OSDI04.pdf)
where the chain closes to form a ring. Once a command (e.g. `enqueue` or `dequeue`)
 is received back at the head of the chain it has been seen by all "mirrors" and
can be acted upon. Chain based replication provides ordering of operations which
is essential when operations do not commute. For example, given an empty queue
a `dequeue` followed by an `enqueue` does not result in the same end state as
an `enqueue` followed by a `dequeue`.

Chain based replication comes with a few
hard-to-solve complications, however. In order to provide continuity of service
it is reliant on having a good failure detection mechanism in order to detect
down or partition nodes so that the chain (or in our case, ring) can be reformed
without the failed node. Reliable failure detection is a very hard problem to solve,
if not impossible so although chain based replication is relatively easy to implement and
reason about you end up with complexity elsewhere.

Raft also provides similarly strong guarantees but instead of relying completely on failure
detection to ensure availability it uses leader election which is firstly less likely
to cause an issue in the first place and has a less severe effect when a false
positive is detected.

A ring is also sensitive to network latency between erlang nodes as the theoretical time it
takes to traverse the ring is the additive total of all the network hops in the ring whereas
in the case of raft where replication is done in parallel it is the round-trip
(request/response) time between the leader and the `n/2+1`th slowest node in the cluster. This
doesn't take into account the time taken to persist the message before replying however.
In the current RabbitMQ implementation each node in the ring doesn't wait for a
command to be persisted before passing it on which results in better performance
at the cost of safety.


#### Status

Experimental. The current feature set represents only what we needed in order to
evaluate the potential for specific RabbitMQ use cases. That said most of the
standard protocol is implemented and there is an in-flux high level API in
the `ra` module.

There are two storage backends:

* `ra_log_memory`
    - an in memory log backend for testing.
* `ra_log_file`
    - a simple file based log backend.
    - all logs are stored in a single file on disk that is never
compacted.
    - meta data is stored in a `dets` table.

### Design

The current design is relatively simple. The raft protocol itself can be modelled
as a simple state machine with 3 states: `leader`, `candidate` and `follower`. `ra`
uses `gen_statem` to implement this state machine (see: `ra_node_proc.erl`).

The majority of the raft logic is implemented in the `ra_node` module. This module
consists of mostly pure and thus testable code. Learning from a previous raft
implementation attempt this was a conscious and critical design decision.

Any side effects that need to be performed
(sending of messages, monitors, release of log entries etc) are returned from
one of the `ra_node:handle_*` functions as an `{effects, Effects}` tuple
and `ra_node_proc` will ensure these happen.

In addition to the main `gen_statem` process the leader spawn an additional
`ra_proxy` process that is responsible for periodically sending `AppendEntryRpc`s
to each of the followers. The reason for not doing so from the main process is
to ensure elections aren't trigged in case the main process is blocked or
suspended from some or other reason.

Currently `AppendEntryRpc` messages are sent by the `ra_proxy` process "faking"
 a `gen_statem` call request such that the `AppendyEntryResponse` is returned to
to the main process rather than the proxy. This is likely to need to change at
some point.

#### Performance

As we are developing this partly to evaluate it for RabbitMQ queueing applications
performance is a crucial factor to consider. 

Like most consensus protocols Raft relies on commands being persisted to
[stable storage](https://en.wikipedia.org/wiki/Stable_storage) on a majority of
nodes before considered committed. This is done in case the machine in the node
is on crashes and ensures the command is still known by the node after it restarts.
Stable storage implies the command has fully reached the disk and thus requires
`fsync` to have been called.

Our initial tests using a 3-node cluster and the simple `ra_fifo` queue state machine
implementation showed that we could only achieve a rate of around ~120 messages
per second using the `ra_log_file` persistence backend. The majority of the time
was spent waiting on synchronous IO to complete.

This test included the optimisation
suggested in the raft thesis where the leader delays it's own `fsync` call and instead
relies on the followers replies to commit the entry which ensure we only have to
wait for a single sequential `fsync` call.

1000/120 = ~8.3ms which is
roughly how long an `fsync` call could take on a given system plus some replication
network overhead. This suggest commands are processes in a very sequential
manner (every command waits for the previous command to fsync before being processed)
 which means batching optimisations are very likely to yield good benefits.


There are further optimisations that can be tried:
* Input write batching at the leader to try to commit more than one incoming command
at a time. Has latency cost.
* Write batching at the follower.
    - As followers write on their "main" thread it is possible that multiple "urgent"
append entries could already be available in their mailbox - especially after
blocking on an `fsync` for a while. This may allow us to buffer and reply only
once for the buffer.
* Leaders could have a separate writer process to ensure the main process never
has to block on a sync call and can handle follower replies etc.
* Shared storage between multiple `ra` clusters on the same erlang node.
    - This would allow for greater write batching and is similar to how
[cockroachdb](https://forum.cockroachlabs.com/t/design-consideration-for-hosting-thousands-of-range-replicas-per-node/630) achieves acceptable write performance for a system
with potentially thousands of small raft clusters.

Even with further optimisations it is likely that RabbitMQ would need at times
to relax durability requirements in order to achieve higher message rates whilst
still retaining some of the properties of the underlying protocol (arguably
without durability it's not really raft anymore).

Relaxing durability comes with some unpleasant problems. Consider the following
scenario 
 with 3 nodes, A, B and C where index 3 has been comitted by
consensus of nodes A and B but has not yet been synced to disk.

1.
    - A [1,2,3]: is leader.
    - B [1,2,3]
    - C [1,2]: is partitioned off and is running elections

2.
    - A [1,2,3]: is still leader
    - B [1,2]: crashes and restarts without index 3
    - C [1,2]: partition is healed and C sends vote_requests to A and B in a higher term

3.
    - A [1,2,3]: rejects C's vote_request due to having a  more up-to-date log
    - B [1,2]: votes for C
    - C [1,2]: votes for C - becomes leader and starts sending append entry rpcs

4.
    - A [1,2]: is forced to abdicate and discard committed entry 3
    - B [1,2]:
    - C [1,2]:

This is pretty bad. The crash of a _single_ node caused a committed entry (3) to be
lost.  The root cause of the problem is that the crash caused B to vote for a
stale candidate on restart. If B had not voted after restart
C could not have won the election. Hence if B's vote could somehow be disqualified we _may_
be able to achieve more safety around this particular scenario.

If we assume an `fsync` would always be performed during an orderly shutdown we could
detect that a shutdown was not orderly, or "dirty" by writing a marker file to disc during
node startup and removing it after `fsync` during an orderly shutdown. If the file is there
on start we assume shutdown was "dirty" and enter a mode where the node will not participate
in elections until some point in the future, possibly after another leader is known and
we've received all the entries up until the commit index of the first `AppendEntriesRpc`
receive after restart.

The aim of this would be to provide some safety as long as a majority of nodes do not
crash or exit in an uncontrolled manner.

Further investigation would need to be performed around this to establish viability.
