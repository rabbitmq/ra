# Snapshots

Ra supports pluggable snapshot implementations by virtue of the `ra_snapshot`
behaviour. The default implementation uses `term_to_binary/2` to write the
snapshot to disk.

Snapshot transfer between the leader and followers use distributed
erlang and therefor it implements a "chunked transfer" approach where the
snapshot is divided up into fixed size blocks that are transferred one by one
so as to not block the distribution port when snapshot become very large.

## The `ra_snapshot` behaviour

The `ra_snapshot` behaviour has 9 (!) callbacks:

- `prepare(ra_index(), State :: term()) -> Ref :: term()`:

This is called when the state machine has emitted a `release_cursor` effect
and Ra has decided it is time to take a snapshot. This is called inside the
Ra process and thus should not block unnecessarily. It can be used to trigger
checkpoints or similar in disk-based state machines.


- `write(Location :: file:filename(), meta(), Ref :: term()) -> ok | {error, term()}.`:

This is called in a separate process and should write the snapshot into the
directory specificied by the Location argument.

- `read(ChunkSizeBytes :: non_neg_integer(), Location :: file:filename()) ->
    {ok, Meta :: meta(), ChunkState :: term(), [fun((ChunkState) -> {term(), ChunkState)] | {error, term()}.`

This is called in a separate process when the leader needs to send a snapshot
to a follower. `read` returns the meta data (index, term and cluster configuration)
as well as a list of "chunk thunks" that will be called one by one to retrieve
the chunks to be transferred. This function can be stateful and thus threads
a "chunk state" through as well.

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

Snapshots are stored inside a `snapshots` directory inside the Ra server
data directory. Each snapshot is a directory of the format: `Term_Index` in
64 bit hex encoded and zero padded format. E.g.:

```
<<ra_data_dir>>\2F_QXJY4UDOE1SI0\snapshots\0000000000000014_0000000000253BEA
```


