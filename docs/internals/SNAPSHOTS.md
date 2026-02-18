# Snapshots

Ra supports pluggable snapshot implementations by virtue of the `ra_snapshot`
behaviour. The default implementation uses `term_to_binary/2` to write the
snapshot to disk.

Snapshot transfer between the leader and followers use distributed
erlang and therefore it implements a "chunked transfer" approach where the
snapshot is divided up into fixed size blocks that are transferred one by one
so as to not block the distribution port when snapshot become very large.

## Snapshot Types

Ra supports three types of persistent state captures:

### Snapshots

Full snapshots are the primary mechanism for log compaction. When a snapshot is
taken, the log entries up to the snapshot index can be safely deleted. Snapshots
are replicated to followers and are used for recovery after crashes.

### Checkpoints

Checkpoints are similar to snapshots but are not replicated to followers. They
provide a way to capture state at a point in time without triggering log
truncation. Checkpoints can be promoted to full snapshots when needed.

### Recovery Checkpoints

Recovery checkpoints are a lightweight persistence mechanism designed to avoid
expensive log recovery after ordered shutdowns. Key characteristics:

- **Written only during ordered shutdown**: Never written during normal operation
- **Written synchronously**: Bypasses the worker process for immediate persistence
- **No fsync required**: Since they only occur during ordered shutdowns, the OS
  will flush data to disk
- **No live indexes stored**: Live indexes are always recovered from the last
  snapshot or checkpoint, not from recovery checkpoints
- **Not replicated**: Recovery checkpoints are local optimizations only

#### Configuration

Recovery checkpoints are controlled by the `min_recovery_checkpoint_interval`
configuration option (part of the mutable `ra_server` config):

- Default: `0` (feature disabled)
- When set to a positive integer N: A recovery checkpoint is written during
  shutdown if `LastApplied - HighestIdx >= N`, where `HighestIdx` is the maximum
  of the current snapshot index, current recovery checkpoint index, or highest
  checkpoint index.

#### Recovery Behavior

During server recovery (`ra_server:recover/1`), if a recovery checkpoint exists
with a higher index than `LastApplied`, its machine state is used to skip log
replay. Live indexes are always recovered from the last snapshot or checkpoint,
ensuring correct log compaction behavior.

If a recovery checkpoint's integrity check fails (CRC validation), the system
logs a warning and falls back to normal log replay.

#### Cleanup

Recovery checkpoints are automatically deleted when:
- A new recovery checkpoint is written (old one deleted after new one succeeds)
- A regular snapshot is taken with an index >= the recovery checkpoint's index

## The `ra_snapshot` behaviour

The `ra_snapshot` behaviour has 9 (!) callbacks:

- `prepare(ra_index(), State :: term()) -> Ref :: term()`:

This is called when the state machine has emitted a `release_cursor` effect
and Ra has decided it is time to take a snapshot. This is called inside the
Ra process and thus should not block unnecessarily. It can be used to trigger
checkpoints or similar in disk-based state machines.


- `write(Location :: file:filename(), meta(), Ref :: term()) -> ok | {error, term()}.`:

This is called in a separate process and should write the snapshot into the
directory specified by the Location argument.

- `begin_read(ChunkSizeBytes :: non_neg_integer(), Location :: file:filename()) ->
    {ok, Crc :: non_neg_integer(), Meta :: meta(), ReadState :: term()}
    | {error, term()}.`

This is called in a separate process when the leader needs to send a snapshot
to a follower. `begin_read` returns the meta data (index, term and cluster configuration)
as well as a continuation state that will be used to read chunks to be transferred.
This function also returns a checksum to validate data transfer.

- `read_chunk(ReadState, ChunkSizeBytes :: non_neg_integer(), Location :: file:filename()) ->
    {ok, Chunk :: term(), {next, ReadState} | last} | {error, term()}`

This function reads a chunk of data to be sent. The data is read using ReadState
initially received from `begin_read`. As long as it returns `{next, ReadState}`
it will be called again for the next chunk. When reading the last chunk the
function should return `last` instead.

- `begin_accept/accept_chunk/complete_accept`

These callbacks are used to implement the corresponding end of `read/2`.
`begin_accept` will be called by the follower when it first receives an
InstallSnapshotRpc message and should persist the meta data and return the
initial accept state. After this `accept_chunk/2` will be called for each received
chunk except the last which will call `complete_accept/2`. `complete_accept/2` should
validate that the integrity of the snapshot is good before returning.

- `recover/1`: is called at two different times. Immediately after a follower
has completed a transfer and on init to recover the state of a stored snapshot.


- `validate/1` should validate that an on-disk snapshot has no integrity faults.
This is called when a Ra server is recovering after a restart. If this fails,
the server will try to load the next available older snapshot, if available.

- `read_meta/1` should return the meta data for a snapshot. This includes the
Raft index and term as well as a list of member servers.


## On disk layout

Snapshots, checkpoints, and recovery checkpoints are stored in separate
directories inside the Ra server data directory. Each is a directory of the
format: `Term_Index` in 64 bit hex encoded and zero padded format.

### Directory Structure

```
<<ra_data_dir>>/<<server_uid>>/
├── snapshots/
│   └── 0000000000000014_0000000000253BEA/
│       └── snapshot.dat
├── checkpoints/
│   └── 0000000000000015_0000000000300000/
│       └── snapshot.dat
└── recovery_checkpoint/
    └── 0000000000000016_0000000000400000/
        └── snapshot.dat
```

### File Contents

- `snapshot.dat`: The serialized machine state (format depends on the
  `ra_snapshot` implementation)


