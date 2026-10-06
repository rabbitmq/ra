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


### Optional callbacks

A snapshot implementation can take over where, and how, its snapshots are
stored by implementing some optional callbacks. When absent `ra_snapshot` uses
one directory per snapshot, as described under "On disk layout".

- `write/5`: like `write/4` but also receives the live indexes of the snapshot
and is responsible for persisting them (`ra_snapshot` writes no `indexes`
file). `ra_snapshot` does not create the `Location` directory before calling
it. Used for snapshots only, not checkpoints. It returns `{ok, Bytes, durable}`
if the snapshot and its indexes are already durable, in which case nothing more
is done, or `{ok, Bytes, directory}` if it wrote to the `Location` directory
(which it created) and `ra_snapshot` should write the indexes file and
synchronise as usual.

- `list/1`: the names of all the snapshots the implementation holds for a
member, in the form `ra_snapshot:snapshot_name/2` makes. Defaults to the entries
of the snapshots directory. Each name is passed on as a `Location` to the other
callbacks (`validate/1`, `read_meta/1`, `recover/1` and so on) so an
implementation that does not keep snapshots in directories has to handle
`Location`s that do not exist.

- `delete/1` deletes a snapshot (or checkpoint) given its `Location`. Defaults
to a recursive delete of the directory.

- `indexes/1` returns the live indexes of a snapshot. Defaults to reading the
`indexes` file in the `Location` directory.

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




## The snapshot log

With many Ra clusters in one system, each taking small snapshots frequently, the
cost of the default layout is dominated by the file system rather than by the
data: each snapshot is a new directory with two files that all need creating,
syncing and later deleting. Measured on ext4 this was around 60KB of device
writes and three quarters of a journal commit for every 1KB snapshot, and
limited a node to a few hundred snapshots per second however many clusters it
hosted.

`ra_log_snap_store` keeps small snapshots in an append-only log shared by all
the members of a system instead. A single process batches the snapshots written
by all members into appends to one file with one `fsync` per batch. The default
snapshot module (`ra_log_snapshot`) uses it through the optional callbacks
above, so `ra_snapshot` and the rest of Ra do not know about it.

It is off by default. It is enabled by the `snapshot_store` key of the system
configuration, or the `snapshot_store` application environment key for the
default system:

```erlang
#{snapshot_store => #{max_size => 16384,            %% bytes, default 16KB
                      min_file_bytes => 67108864}}  %% default 64MB
```

A snapshot goes into the log if its encoded image plus live indexes is no
bigger than `max_size`. Larger snapshots, all checkpoints and recovery
checkpoints, and any snapshot that the log fails to take (e.g. the disk is
full), are written as directories as before. A member can have snapshots in
either, the newest wins when it starts.

### How the log works

- Files are `<data_dir>/snapshot_store/NNNNNNNN.snap`. Each starts with a header
and holds records (member uid, index, term, snapshot image, live indexes) with
a CRC each. Batches are padded to 4KB boundaries.
- Only the latest snapshot of each member is live; a newer one supersedes it,
nothing is deleted individually. An in memory ETS table points at the live
record of each member. It is updated only after the batch that has the record
has been fsynced, so an entry always refers to durable data.
- When the records in the active file (not counting padding) reach `max(min_file_bytes, 2 * live bytes)` it is rolled
over to a new file. The writer then retires the oldest file: it copies the
records in it that are still live into the next batches it writes and deletes
the file once those are durable. Space use is bounded by a small multiple of the
live data and every byte is rewritten about once.
- If a write or fsync fails the callers get an error (and fall back to a
directory), nothing is published, the fsync is not retried, and a new file is
started. The header of the new file records how much of the previous one was
acknowledged so that anything written after that is ignored when recovering.
- On start the files are scanned in order and the newest record of each member
is kept. A record is dead if its member's directory is gone (any other error
looking for it counts as alive). A record whose contents do not validate is
skipped, the ones after it are independent; one whose length cannot be trusted
ends the scan of that file. A file with a damaged header is set aside as
`.bad`, not deleted, unless it is the newest and tiny (a file that was being
created when the node stopped). A file is never appended to after a restart.
- The file being retired is never deleted while a member's snapshot still points
into it. If part of it can not be read the file is kept and an error is logged.
Read errors are retried after a delay.
- Deleting a snapshot (`release`) removes a member's entry only if it is that
exact snapshot (index and term). The snapshot being written when a failure
happens is deleted by `ra_snapshot`, which must not take the current one.
- Reads (recovery, validation, sending a snapshot to a follower) go through the
ETS table, with the whole snapshot read into memory. A snapshot that has been
superseded since it was looked up gives `{error, superseded}`.
- The ETS table only becomes visible once recovery of the files is complete.
- Whether a snapshot log is configured is recorded by `ra_log_sup` (not by the
log process) so that it is known while the log is restarting. A member that
starts while the log is not answering fails to start, and is retried by its
supervisor, rather than start without a snapshot that its (truncated) log
depends on. Taking a snapshot while the log is not answering writes a directory
instead.
- `min_file_bytes` is raised to at least four blocks (16KB): with less, copying
the live data of a file forward could make the next file roll immediately.

### Metrics and health

The log registers counters with `ra_counters`, like the WAL does, under its
process name (`ra_counters:overview(Name)`, labelled with the system and module
so they are exported with the other Ra metrics). `ra:overview(System)` also
returns `snapshot_store` with the same values and the health when it is
configured.

| counter | |
|---|---|
| `puts` | snapshots appended |
| `batches` | batches written, one fsync each; `puts / batches` is how well it batches |
| `bytes_written` | bytes appended, including records copied forward and padding |
| `copies` | records copied forward when retiring files; `copies / puts` is the extra work of reclaiming space |
| `rolls`, `retired_files` | files rolled over and deleted |
| `retire_blocked` | files kept because snapshots in them could not be copied out |
| `errors` | batches that failed to be written, or files that could not be created |
| `stale_puts` | snapshots refused as older than the member's current one |
| `corrupt_records` | invalid records skipped when recovering or retiring |
| `fsync_time_us` | time spent writing and syncing; divide by `batches` for the average |
| `live_bytes`, `entries`, `files` | gauges: size of the live snapshots, members with one, files on disk |
| `recovery_time_ms` | gauge: how long recovering the files took at start |
| `degraded` | gauge: 1 if the log is unhealthy |

`ra_log_snap_store:status/1` says why it is unhealthy, one or more of:
`no_active_file` (it could not create a file after a failure so puts fail, and
members fall back to directories), `write_errors` (the last batch could not be
written), `retire_read_errors` (files could not be read to retire them, it is
retrying), `files_blocked` (files are kept because snapshots in them could not
be copied out, this lasts until restart and needs a look at the disk). Changes
in health are logged. Alert on `degraded`.

### Turning it off

Snapshots that exist only in the log would be invisible without it while their
members' logs are already truncated. So when a system starts without
`snapshot_store` and snapshot log files from an earlier run exist, the live
snapshots in them are first written back as ordinary snapshot directories (and
synced) and then the log files are removed. Each snapshot is written to a
staging directory in the member's directory and renamed into place, so an
interrupted move never leaves a partial snapshot where the member looks for
one, and it can safely be run again. A snapshot directory that is already there
only counts if it validates. A snapshot in the log that is itself damaged is
reported and skipped; any other failure leaves the log in place and startup
fails.

Older versions of Ra do not know about the log. To downgrade, disable the
feature, restart the system so that its snapshots are moved out, then
downgrade.
