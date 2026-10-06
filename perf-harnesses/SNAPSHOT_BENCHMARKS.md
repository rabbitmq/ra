# Snapshot benchmarks

Two standalone Erlang modules used to evaluate the snapshot log
(`ra_log_snap_store`, see `docs/internals/SNAPSHOTS.md`). They are not part of
the build or the test suites. Run them on the file system you care about
(Linux, ext4); `fsync` on macOS does not flush the device so the numbers there
mean nothing.

`snap_fs_bench` models the file operations only and needs nothing from Ra.
`snap_e2e_bench` runs real Ra servers and needs a compiled ra
(`rebar3 compile` at the repo root) on the code path.

Raise the open file limit first (`ulimit -n 65536`), the 10k cluster runs need
it.

## `snap_fs_bench`

Self-contained (no Ra modules needed), so it does not need `rebar3 compile`.
The directory **must be on the filesystem under test**. Do not run it on
macOS for decisions, `fsync` there does not flush the device.

```sh
erlc -o /tmp snap_fs_bench.erl
iostat -x 1 > iostat.log &     # optional, in parallel
erl -noshell -pa /tmp -eval '
  snap_fs_bench:run("/mnt/ext4/bench",
                    #{clusters => [100, 1000, 10000],
                      sizes => [1024, 8192, 65536],
                      rounds => 3,
                      device => "nvme0n1",   %% adds diskstats + jbd2 deltas
                      csv => "results.csv"}),
  halt().'
```

Scenarios: `a` today (dir per snapshot, serial syncs), `b` concurrent syncs,
`b2` phased concurrent syncs (all file fsyncs, then all dir syncs), `c` flat
file per snapshot with embedded indexes, `c2` flat + phased, `d` shared
snapshot log (single writer, batch fsync, roll at max(MinBytes, 2*live),
copy-forward compaction). Use `scenarios => [a, c2, d]` etc. to subset.

Columns: snapshots/s, per-snapshot latency (write to durable) p50/p99, block
layer write ios / merges / merge% / MB written / flushes (from
`/proc/diskstats`), jbd2 transaction count delta, p99 fsync latency of a
background WAL-like writer on the same filesystem, write amplification for
`d` ((appended + compacted) / submitted), and bytes left on disk at the end.

Fixed-duration / open-loop mode (use this for WAL-latency comparisons, so
every scenario is sampled over a comparable window; the WAL p99 only counts
samples taken while the snapshot load was running):

```sh
erl -noshell -pa /tmp -eval '
  snap_fs_bench:run("/mnt/ext4/bench",
                    #{clusters => [1000], sizes => [1024, 16384],
                      duration => 60,        %% seconds per scenario
                      interval_ms => 2000,   %% each cluster every 2s = 500 snaps/s offered
                      skew => zipf,          %% optional: few hot, many cold clusters
                      scenarios => [a, c2, d], cap => 4,
                      device => "nvme0n1", csv => "open.csv"}), halt().'
```

Notes: `cap` (default 256) limits concurrent syncs per worker batch in `b`,
`b2` and `c2`; use it to test whether a bounded version keeps the throughput
without hurting WAL tail latency. The CSV has every column the console table
has (`write_amp`, `puts_per_batch`, `jbd2_tx`, `space_mb`, ...). Disk, flush
and jbd2 deltas include the background WAL writer's own I/O while it runs, so
take I/O counts from a `wal => false` pass and WAL latency from a `wal =>
true` pass. `wal => false` turns the background WAL writer off. `pool` sets the
sync worker pool size (default schedulers div 4, as `ra_log_sync`).
`store_min_bytes` (default 64MB) is the minimum size before `d` rolls; lower
it to exercise compaction with few clusters. The compactor's work is
included in the disk deltas (the run waits for it) but not in the latency
numbers.

## `snap_e2e_bench`

The synthetic `snap_fs_bench` models the file operations. This one runs real
Ra servers in a system with and without the snapshot log
(`snapshot_store` system config) and reports commands/s, snapshots/s (sum of
the `snapshots_written` counters), and block layer writes, flushes and MB
written for the run. Needs a compiled ra on the code path.

```sh
erlc -o /tmp snap_e2e_bench.erl
ulimit -n 65536
erl -noshell -pa /tmp -pa ../_build/default/lib/*/ebin -eval '
  snap_e2e_bench:run("/mnt/ext4/e2e",
                     #{members => 1000, state_size => 1024, commands => 100,
                       snapshot_every => 5, modes => [directories, log],
                       device => "nvme0n1"}), halt().'
```

Every command emits a `release_cursor`, so with `snapshot_every => 5` each
member snapshots every 5 commands. `state_size` sets the snapshot size.

`done %` is how many of the snapshots the workload asked for were taken: Ra
skips a snapshot if the previous one is still being written, so a slow
snapshot path takes fewer rather than slowing the commands down. Add `none` to
`modes` for a run with no snapshots at all: its device writes, flushes and MB
are the WAL and segment baseline to subtract from the other modes. Use enough
`commands` for runs of tens of seconds and repeat them, short runs are noisy.

## `snap_store_bench`

Drives the real `ra_log_snap_store` directly (no Ra servers) with an open loop
load, to find out what one store process can take. Each of `clients` members
puts a snapshot of `size` bytes every `clients * 1000 / rate` ms for `duration`
seconds, whilst a WAL-like writer (4KB append + fdatasync) shares the file
system. The first row of the output has no snapshots, it is the WAL writer's
fsync latency on its own to compare with.

`in sync %` is high (70 to 90) at any real load and does not mean the store is
saturated: a group commit store starts its next batch as soon as the last fsync
returns, so it is always mostly in fsync and what grows with the load is the
number of puts per batch (`put/bat`). It is saturated when `put/bat` reaches 1024
(the most a batch can have), when `done/s` falls short of `offered`, or when the
latencies climb with the load. `fsync ms` is the cost of one batch and sets the
latency floor (a put waits for the batch in progress and then its own).

```sh
erlc -o /tmp snap_store_bench.erl
ulimit -n 65536
erl -noshell -pa /tmp -pa ../_build/default/lib/*/ebin -eval '
  snap_store_bench:run("/mnt/ext4/store",
                       #{rates => [1000, 5000, 10000, 20000],
                         clients => 10000, size => 1024, duration => 30,
                         device => "nvme0n1"}), halt().'
```

## `snap_env_probe` and `run_snapshot_benchmarks.sh`

`snap_env_probe` measures the raw cost of what snapshots are made of on the
machine: append + fdatasync (what the log and the WAL pay), and a directory
snapshot (mkdir, two files, fsync, directory syncs, delete the previous one)
alone and 8 and 32 at once. Consumer NVMe, enterprise NVMe with power loss
protection, and network volumes differ by orders of magnitude here, which is
why the other results can only be read against it.

`run_snapshot_benchmarks.sh` runs all of it and collects the environment (cpu,
file system and mount options, device, kernel, ra commit) in one directory to
send back:

```sh
rebar3 compile
perf-harnesses/run_snapshot_benchmarks.sh /mnt/ext4/bench nvme0n1 quick   # about 20 min
perf-harnesses/run_snapshot_benchmarks.sh /mnt/ext4/bench nvme0n1 full    # about 1.5 h
```
