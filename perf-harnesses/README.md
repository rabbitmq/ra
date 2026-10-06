# Benchmark harnesses

Ad-hoc modules written for the log subsystem performance review
(`../ra-log-perf-review.md`). They are **not** part of the build or the test
suites — they were run out of a scratch directory against the compiled beams.
Kept because every number in the review came from one of them.

## Running

```sh
cd perf-harnesses
rebar3 compile                 # from the repo root first, to build ra
erlc -o . <module>.erl
erl -noshell -pa . -pa ../_build/default/lib/*/ebin \
    -eval '<module>:run(), halt().'
```

Some need the test profile instead (`../_build/test/lib/*/ebin`) because they
use `proper` or the test beams.

**`-pa` ordering matters.** Each `-pa` is *prepended*, so the last flag ends up
first on the code path. To shadow a module from `ra` with a local copy, put the
local directory last. Getting this wrong silently measures the wrong code — see
`mutate.sh` for a working example.

## What each one measures

| module | what |
|---|---|
| `micro_bench` | `ra_seq` ops, ETS snapshot-state lookup, serialisation, checksums, segment open/read |
| `seq_bench` | `ra_seq:remove_prefix/2` and `add/2` at various sizes vs range-aware alternatives |
| `seq_diff` | differential test of patched vs original `ra_seq` (needs `ra_seq_orig`, see below) |
| `seg_bench`, `seg_bench2` | segment open cost by step; map vs binary index; index parse strategies |
| `wal_bench` | end-to-end WAL throughput with N concurrent writer processes |
| `wal_cpu` | WAL hot path via direct `handle_batch/2` calls, reductions per entry |
| `wal_scale` | separates total writers from writers-per-batch; the harness to use for WAL work |
| `rec_bench` | WAL record shape: nested iolist vs per-record binaries |
| `cksum_bench`, `cksum2`, `cksum3` | per-record vs per-batch WAL checksum, wall clock and reductions |
| `notify_bench` | cost of the per-writer `written` send vs accumulating and fanning out |
| `segw_bench`, `segw2` | segment writer per-writer fixed costs, and vs directory size |
| `sched_bench` | chunk barrier vs longest-first vs work queue across work distributions |
| `segrefs_bench` | segref bookkeeping vs segment count |
| `info2_bench` | `info/2` membership test vs merge scan |
| `livecheck` | differential check of `info/2` `live_size` against a model |
| `snap_fs_bench` | many-clusters snapshot filesystem cost: today's dir layout vs concurrent syncs vs flat file vs shared snapshot log (see below) |
| `snap_e2e_bench` | snapshots through real Ra servers (many single member clusters), directories vs the snapshot log (see below) |
| `snap_bench` | snapshot `validate`/`read_meta` vs CRC-only alternatives |
| `fold_bench`, `init_bench` | fold sequential vs random; startup `info/1`; open-segment memory |
| `misc_bench` | `ra_seq:in/2` early exit, write shapes |
| `log_bench` | per-append micro costs and the `written`-event share |
| `meta_bench` | `ra_log_meta` store throughput, dets vs ets |
| `mutate.sh` | mutation-tests the `ra_seq` property suite by reintroducing known regressions |

`seq_diff` needs a copy of the *original* `ra_seq` under a different name:

```sh
git show 5b35391:src/ra_seq.erl | sed 's/^-module(ra_seq)\./-module(ra_seq_orig)./' > ra_seq_orig.erl
```

## `snap_fs_bench` (Linux ext4 run)

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

## Which of these is worth keeping

`wal_scale` and `wal_cpu` are the two with ongoing value: reductions per entry
is deterministic and reproduces to ±0.2 out of ~57, so it works as a
regression signal. `wal_bench` does not — see the methodology notes in the
handover doc.

If any of these are to become permanent they need porting into `test/` with
proper CT scaffolding; as written they hardcode `/tmp` paths and print to
stdout.
