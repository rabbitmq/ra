# Running the snapshot benchmarks on a new machine

This is for whoever is running the snapshot log benchmarks on a Linux machine
that is not the one they were developed on. It should take about an hour of
your time, most of it waiting. You do not need to know Ra or Erlang.

## What this is about

Ra writes a snapshot of a state machine from time to time so that its log can
be truncated. With many Ra clusters on one node (thousands of RabbitMQ
quorum queues) that adds up to a lot of small snapshots, and the cost turned
out not to be the data but the file system: every snapshot is a new directory
with two files that each need creating, syncing and later deleting, which
costs tens of KB of device writes and most of a journal commit per snapshot.

The `snap-store` branch adds a **snapshot log**: one process that appends the
small snapshots of all clusters to a few shared files with one `fsync` per
batch. We want to know, on hardware that is closer to production than the
developer's NUC (consumer NVMe, fsync about 5 ms):

1. **How fast is the log, and where does it top out?** One process, driven
   directly, at increasing rates.
2. **Is it faster than the current way, and does it scale better?** Real Ra
   servers snapshotting, with directories and with the log, and with no
   snapshots at all as the baseline.
3. **What does the device itself do?** Raw `fsync` cost, so the numbers can be
   read against the hardware. This is the single most important thing for
   comparing machines.
4. **What happens around the size limit?** Snapshots above `max_size` (16 KB by
   default) still use directories.

The results from the NUC, for comparison, are at the end.

## What you need

* A Linux machine that is **otherwise idle** while the benchmarks run. Not a
  shared or production host: they hammer the disk and will disturb anything else
  using it, and anything else using it will disturb them.
* A directory on the **file system and device you want measured** (ext4 on the
  NVMe, or whatever you care about). Not `/tmp` if that is tmpfs, and not a
  network mount unless that is what you want to measure.
  `df -hT <dir>` should show the file system you expect.
* About **10 GB free** there for `quick` mode, 30 GB for `full`. The benchmarks
  write more than that over their run (tens of GB), which is worth knowing on a
  drive with a small endurance rating.
* Erlang/OTP **26 or 27** (the versions Ra supports; 28 and later usually work
  too), `rebar3`, `git`, and a C compiler is not needed.
* Root is not needed. `nvme-cli` (`nvme id-ctrl`) is used for the drive model if
  it is installed, and skipped if not.

## Steps

```sh
git clone <the ra repository> && cd ra     # or an existing checkout
git fetch && git checkout snap-store && git pull
git log --oneline -1                       # note this, it goes in your report
rebar3 compile
```

Check that everything is set up, which takes a couple of minutes and whose
numbers mean nothing:

```sh
perf-harnesses/run_snapshot_benchmarks.sh /mnt/ext4/bench nvme0n1 smoke
```

* The first argument is a directory on the file system under test (created if
  it does not exist; only the subdirectories `probe`, `store`, `e2e` and `fs` of
  it are used and they are emptied).
* The second is the block device **name** as in `/sys/block/` and
  `/proc/diskstats` (`lsblk` shows it): `nvme0n1`, not `/dev/nvme0n1` and not a
  partition like `nvme0n1p2`. For LVM or RAID use the underlying device and note
  that in your report.

It prints where its results are. Look in that directory: there should be six
`.txt` files and `environment.txt`, and none of the outputs should contain
`Error!` or `Runtime terminating`. If they do, see Troubleshooting.

Then the real run:

```sh
perf-harnesses/run_snapshot_benchmarks.sh /mnt/ext4/bench nvme0n1 quick   # about 20 minutes
```

and, if you can leave it for longer and the first run looked sensible:

```sh
perf-harnesses/run_snapshot_benchmarks.sh /mnt/ext4/bench nvme0n1 full    # about 1.5 hours
```

Each run makes its own directory `snapshot-bench-<host>-<date>` in the current
directory. Run them one at a time, not in parallel.

While it runs the output scrolls past; the same text is saved in the files.
There is nothing to watch for except it stopping, so it can be left alone.

## What to send back

1. The `snapshot-bench-...` directory (`tar czf results.tgz snapshot-bench-*`).
2. A few lines on the machine that the script cannot know:
   * bare metal, virtual machine or cloud instance (which type), and if cloud
     what kind of volume;
   * the drive: model, and whether it has **power loss protection** (enterprise
     NVMe usually does, consumer drives do not) if you know;
   * any RAID, LVM, encryption or network storage in the path;
   * the CPU governor (`cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor`)
     if it is not `performance`;
   * anything else that was running.
3. Anything that looked strange, even if you are not sure it matters.

## What is in the results

| file | what it is |
|---|---|
| `environment.txt` | CPU, memory, kernel, file system and mount options, device settings, the ra commit |
| `01-probe.txt` | the raw cost of the operations snapshots are made of (see below) |
| `02-store-1KB.txt` | one snapshot log process driven directly at increasing rates, 1 KB snapshots |
| `03-store-8KB.txt`, `04-store-16KB.txt` | the same with bigger snapshots |
| `05-e2e-<N>-members.txt` | real Ra servers: no snapshots, snapshots as directories, snapshots in the log |
| `06-e2e-sizes.txt` | the same with the state sizes either side of the 16 KB limit |
| `07-fs-alternatives.txt` | (`full` only) a model of the alternatives that were considered |

### `01-probe.txt`

The two rows to look at:

* `append 4KB + fdatasync`: what one batch of the log (and the WAL) costs. **The
  p50 here is the floor for the latency of a snapshot in the log** and sets how
  many batches per second it can do. On the developer's NUC it is about 5 ms; an
  enterprise NVMe with power loss protection can be well under 1 ms.
* `dir snapshot, N at once`: what one snapshot costs as a directory, alone and
  with 8 and 32 at the same time. `ops/s` is how many directory snapshots the
  device can do. This is what the log competes with.

### `02`, `03`, `04`: the log on its own

One row per offered rate. The first row, with `-` as the rate, has no snapshots:
it is a WAL-like writer alone on the device, which is what the `wal p99`
column of the other rows should be compared with.

| column | meaning |
|---|---|
| `offered`, `done/s` | snapshots per second asked for and achieved. They should match until the log tops out |
| `p50 ms`, `p99 ms`, `max ms` | how long a snapshot took to be durable |
| `put/bat` | snapshots per batch (per fsync). Grows with the load, the most it can be is 1024 |
| `fsync ms` | time per batch writing and syncing |
| `in sync %` | share of the run spent in fsync. High (70 to 90) at any real load, it does **not** mean saturated |
| `B/put` | bytes appended per snapshot, including padding and copies when files are retired |
| `files` | log files at the end, should be small |
| `disk wr`, `flushes`, `MB wr` | block device writes, flushes and megabytes over the run. Includes the WAL-like writer, so subtract the first row |
| `wal p99` | fsync latency (p99) of the WAL-like writer while the snapshots were being written |

**The log is saturated** when `put/bat` reaches 1024, or `done/s` falls short of
`offered`, or the latencies climb steeply with the load. Where that is, for
each size, is the main thing we want from these files.

### `05`, `06`: real Ra servers

Each member applies commands and takes a snapshot every 5 of them. One table per
run with a row per mode:

| column | meaning |
|---|---|
| `cmds/s` | commands applied per second overall |
| `snaps/s`, `snaps` | snapshots taken |
| `done %` | share of the snapshots the workload asked for that were taken. Ra skips a snapshot if the previous one is still being written, so a slow snapshot path takes fewer rather than slowing the commands |
| `disk wr`, `flushes`, `MB wr` | block device activity. The `none` row is the baseline (the WAL and segments) to subtract from the other two |

What we hope to see is `directories` with a low `done %` and high device
traffic per snapshot, and `log` with a high `done %` and much less. The
interesting results are where that does **not** hold.

## Troubleshooting

* **`Error!`, `Runtime terminating`, `badarg`, `undef`** in an output file: the
  checkout is not built (run `rebar3 compile` again), or an old build is in
  `_build`. `git status` and `git log --oneline -1` should show the `snap-store`
  branch with no unexpected changes. Send the file.
* **`emfile` or "too many open files"**: the script tries `ulimit -n 65536`. If
  that fails, raise the hard limit (`/etc/security/limits.conf`) and log in again.
* **Columns of zeros** in `fsync ms`, `files` or `put/bat`: a stale build, see
  the first point.
* **`disk wr`, `flushes` and `MB wr` are all zero**: the device name is not the
  one in `/proc/diskstats`. `cat /proc/diskstats | grep -w <name>` should print a
  line.
* **Everything is very fast and the drive is clearly a spinning disk or a
  network volume, or very slow**: that is a result, not a failure. Say what it
  is.
* **It was interrupted:** run it again; it empties its scratch directories first.
  The partial result directory can be deleted.
* **Erlang paths:** `~` is not expanded by Erlang. Give absolute paths.

## Running one thing by hand

From the `perf-harnesses` directory, after `rebar3 compile` at the top and with
`ulimit -n 65536`:

```sh
erlc -o /tmp snap_env_probe.erl snap_store_bench.erl snap_e2e_bench.erl

# the raw device (about 20 s)
erl -noshell -pa /tmp -eval 'snap_env_probe:run("/mnt/ext4/bench/probe"), halt().'

# the log on its own, at the rates you choose, for 30 s each
erl -noshell -pa /tmp -pa ../_build/default/lib/*/ebin -eval '
  snap_store_bench:run("/mnt/ext4/bench/store",
    #{rates => [10000, 20000, 40000, 80000], clients => 10000,
      size => 1024, duration => 30, device => "nvme0n1"}), halt().'

# real servers
erl -noshell -pa /tmp -pa ../_build/default/lib/*/ebin -eval '
  snap_e2e_bench:run("/mnt/ext4/bench/e2e",
    #{members => 5000, state_size => 1024, commands => 400,
      snapshot_every => 5, modes => [none, directories, log],
      device => "nvme0n1"}), halt().'
```

The options of each are described at the top of its `.erl` file and in
`SNAPSHOT_BENCHMARKS.md`. It is worth trying rates well above where the log
tops out on your machine, and more members (10000) if the machine has the
memory, since the interesting question is how it scales.

## For comparison: the developer's NUC

Consumer NVMe, `fsync` about 5.4 ms, a handful of cores. One log process, 1 KB
snapshots, 10000 members:

| offered/s | achieved/s | p50 | p99 | puts per batch | WAL p99 (5.4 ms alone) |
|---|---|---|---|---|---|
| 1,000 | 999 | 10.2 ms | 14.2 ms | 6.9 | 6.1 ms |
| 5,000 | 4,997 | 10.7 ms | 26.7 ms | 34 | 6.2 ms |
| 10,000 | 9,992 | 11.4 ms | 25.0 ms | 68 | 6.2 ms |
| 20,000 | 19,989 | 11.3 ms | 33.2 ms | 137 | 7.0 ms |

It did not top out; at 20,000/s a batch had 137 of a possible 1,024 snapshots.
Per snapshot, device writes were about 1.3 to 1.6 KB with the log against about
62 KB as directories, and the real servers took 80% of the snapshots they were
asked for with the log against 7% as directories (1000 members, 1 KB state).
Directories topped out at a few hundred snapshots per second however many
clusters there were.

What we do not know, and why you are being asked, is how much of that carries
over to a drive with fast `fsync`, where both ways get faster and the ratio
between them could be very different.
