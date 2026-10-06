# Snapshot log benchmarks: running them on another machine

Branch `snap-store` adds `ra_log_snap_store`: one process per system that
appends small snapshots from all members to shared files, one fsync per batch,
instead of a directory + two files + dir syncs per snapshot (enable with the
`snapshot_store` system config key; design and behaviour in
`docs/internals/SNAPSHOTS.md`). On a NUC (consumer NVMe, ~5 ms fsync) the
directory path topped out at a few hundred snapshots/s however many clusters
there were; the log did 20k/s untroubled. We want to know how this holds on
hardware with a faster, more production-like fsync, where both paths speed up
and the ratio may well differ.

## Needs

* Linux, otherwise idle, a directory on the file system/device under test (not
  tmpfs), ~10 GB free (30 GB for `full`). The runs write tens of GB in total.
* OTP 26/27 and rebar3. No root needed (`nvme-cli` is used for the drive model
  if present).

## Run

```sh
git fetch && git checkout snap-store && git pull && rebar3 compile
perf-harnesses/run_snapshot_benchmarks.sh /mnt/ext4/bench nvme0n1 smoke   # ~2 min, checks it all works
perf-harnesses/run_snapshot_benchmarks.sh /mnt/ext4/bench nvme0n1 quick   # ~20 min
perf-harnesses/run_snapshot_benchmarks.sh /mnt/ext4/bench nvme0n1 full    # ~1.5 h, optional
```

The device is the name in `/proc/diskstats` (`nvme0n1`, not a partition; for
LVM/RAID give the underlying device and say so). Only `probe`, `store`, `e2e`
and `fs` under the directory are used and emptied. Results land in
`./snapshot-bench-<host>-<date>/`. Run one at a time. A fresh `rebar3 compile`
matters: a stale build shows as zero `fsync ms`/`files`/`put/bat` columns.

## Send back

`tar czf results.tgz snapshot-bench-*`, plus what the script cannot know: bare
metal/VM/cloud and volume type, drive model and whether it has power loss
protection, RAID/LVM/encryption/network storage in the path, CPU governor if not
`performance`, anything else running, and anything odd.

## What is in it

| file | |
|---|---|
| `environment.txt` | cpu, kernel, fs and mount options, device queue settings, ra commit |
| `01-probe` | raw `append 4KB + fdatasync` p50/p99 and directory-snapshot cost alone and 8/32 at once. Read everything else against this: the append p50 is the floor for log latency |
| `02/03/04-store-{1,8,16}KB` | one real `ra_log_snap_store` driven open loop at increasing rates (10k members), a WAL-like writer on the same fs. First row (rate `-`) is that writer alone, the baseline for `wal p99` and for the disk columns |
| `05-e2e-N-members` | real servers, a snapshot every 5 commands: `none` (WAL/segment baseline), `directories`, `log` |
| `06-e2e-sizes` | the same at 8000/15000/20000 bytes (the last is over the 16 KB `max_size`, `log` should look like `directories`) |
| `07-fs-alternatives` | (`full`) synthetic model of the alternatives that were considered |

Reading the store runs: `done/s` vs `offered` (should match), `p50/p99`,
`put/bat`, `fsync ms`, `B/put`, `files` (should stay small), and the disk columns
minus the baseline row. `in sync %` is high (70-90) at any real load, it does not
mean saturated. **Saturated** is `put/bat` reaching 1024 (the batch limit),
`done/s` falling short of `offered`, or latency climbing with load; where that
happens per size is the main result we want. The e2e runs: `done %` is the share
of requested snapshots actually taken (Ra skips one while the last is still
being written, so a slow path takes fewer instead of slowing commands); subtract
the `none` row's disk columns from the others for per-snapshot I/O.

## By hand

```sh
cd perf-harnesses && erlc -o /tmp snap_env_probe.erl snap_store_bench.erl snap_e2e_bench.erl
ulimit -n 65536
erl -noshell -pa /tmp -eval 'snap_env_probe:run("/mnt/ext4/bench/probe"), halt().'
erl -noshell -pa /tmp -pa ../_build/default/lib/*/ebin -eval '
  snap_store_bench:run("/mnt/ext4/bench/store", #{rates => [10000, 40000, 80000],
    clients => 10000, size => 1024, duration => 30, device => "nvme0n1"}), halt().'
erl -noshell -pa /tmp -pa ../_build/default/lib/*/ebin -eval '
  snap_e2e_bench:run("/mnt/ext4/bench/e2e", #{members => 5000, state_size => 1024,
    commands => 400, snapshot_every => 5, modes => [none, directories, log],
    device => "nvme0n1"}), halt().'
```

Options are at the top of each `.erl`. Worth trying rates well past where the
log tops out on your machine, and 10000 members if there is memory for it.
Absolute paths only (Erlang does not expand `~`). `emfile`: the hard `nofile`
limit is too low. Zero disk columns: wrong device name.

## NUC reference

One store process, 1 KB snapshots, 10000 members, fsync ~5.4 ms:

| offered/s | achieved/s | p50 | p99 | puts/batch | WAL p99 (5.4 alone) |
|---|---|---|---|---|---|
| 1,000 | 999 | 10.2 ms | 14.2 ms | 6.9 | 6.1 ms |
| 10,000 | 9,992 | 11.4 ms | 25.0 ms | 68 | 6.2 ms |
| 20,000 | 19,989 | 11.3 ms | 33.2 ms | 137 | 7.0 ms |

Device writes per snapshot: ~1.3-1.6 KB with the log, ~62 KB as directories.
Real servers (1000 members, 1 KB): 80% of requested snapshots taken with the
log, 7% as directories.
