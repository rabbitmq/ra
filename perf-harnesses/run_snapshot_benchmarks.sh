#!/bin/sh
# Runs the snapshot benchmarks on a machine and gathers everything needed to
# read the results into one directory to send back.
#
#   run_snapshot_benchmarks.sh <directory on the file system under test> \
#                              <block device, e.g. nvme0n1> [smoke|quick|full]
#
# smoke takes a couple of minutes and only checks that everything works, quick
# (default) takes about 20 minutes, full about 1.5 hours.
# Run it from a checkout of the snap-store branch with ra compiled
# (rebar3 compile). The directory is used for scratch files and emptied.
set -u

DIR=${1:?directory on the file system under test}
DEV=${2:?block device name, e.g. nvme0n1}
MODE=${3:-quick}

HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/.." && pwd)
OUT="$PWD/snapshot-bench-$(hostname)-$(date +%Y%m%d-%H%M%S)"
mkdir -p "$OUT" "$DIR" || exit 1
EBIN="$ROOT/_build/default/lib/*/ebin"

# no crash dumps in the directory the script is run from
export ERL_CRASH_DUMP=/dev/null
ulimit -n 65536 2>/dev/null || echo "warning: could not raise the open file limit" >&2

case $MODE in
    smoke)
        # a couple of minutes, to check that everything is set up, the numbers
        # mean nothing
        DUR=3; RATES="1000"
        MEMBERS="30"; CMDS=50; SIZE_MEMBERS=30 ;;
    full)
        DUR=60; RATES="5000, 10000, 20000, 30000, 40000, 60000"
        MEMBERS="1000, 5000"; CMDS=1000; SIZE_MEMBERS=1000 ;;
    *)
        DUR=20; RATES="5000, 10000, 20000, 40000"
        MEMBERS="1000"; CMDS=500; SIZE_MEMBERS=1000 ;;
esac

echo "results in $OUT"

# --- the machine ---------------------------------------------------------
{
    echo "== date";           date -u
    echo "== host";           hostname
    echo "== kernel";         uname -a
    echo "== git";            (cd "$ROOT" && git log --oneline -1 && git status --short | head -5)
    echo "== otp";            erl -noshell -eval 'io:format("~s ~s~n",[erlang:system_info(otp_release), erlang:system_info(system_architecture)]), halt().'
    echo "== schedulers";     erl -noshell -eval 'io:format("~p~n",[erlang:system_info(schedulers)]), halt().'
    echo "== cpu";            lscpu 2>/dev/null | head -20
    echo "== memory";         free -m 2>/dev/null
    echo "== file system";    df -hT "$DIR" 2>/dev/null; findmnt -T "$DIR" 2>/dev/null
    echo "== device";         ls -l /sys/block/"$DEV"/device 2>/dev/null
    for f in device/model device/firmware_rev queue/rotational queue/write_cache \
             queue/scheduler queue/nr_requests queue/logical_block_size \
             queue/physical_block_size queue/fua; do
        printf "%s: " "$f"; cat /sys/block/"$DEV"/$f 2>/dev/null || echo "-"
    done
    echo "== nvme";           (nvme id-ctrl /dev/"$DEV" 2>/dev/null | grep -E "^(mn|fr|vwc|oncs) " ) || echo "nvme-cli not available"
    echo "== jbd2";           ls /proc/fs/jbd2 2>/dev/null; head -5 /proc/fs/jbd2/*/info 2>/dev/null
    echo "== ulimit -n";      ulimit -n
} > "$OUT/environment.txt" 2>&1

# --- compile -------------------------------------------------------------
mkdir -p "$OUT/ebin"
for m in snap_env_probe snap_store_bench snap_e2e_bench snap_fs_bench; do
    erlc -o "$OUT/ebin" "$HERE/$m.erl" || exit 1
done

erl_run() {
    # name, expression
    NAME=$1; shift
    echo "=== $NAME"
    erl -noshell -pa "$OUT/ebin" -pa $EBIN -eval "$1" 2>&1 | tee "$OUT/$NAME.txt"
    sync
}

# --- 1. what the device and file system do ------------------------------
erl_run 01-probe "snap_env_probe:run(\"$DIR/probe\", #{seconds => 5}), halt()."

# --- 2. one store process, driven directly, open loop -------------------
erl_run 02-store-1KB "snap_store_bench:run(\"$DIR/store\", #{rates => [$RATES], clients => 10000, size => 1024, duration => $DUR, device => \"$DEV\"}), halt()."
erl_run 03-store-8KB "snap_store_bench:run(\"$DIR/store\", #{rates => [5000, 10000], clients => 10000, size => 8192, duration => $DUR, device => \"$DEV\"}), halt()."
erl_run 04-store-16KB "snap_store_bench:run(\"$DIR/store\", #{rates => [2000, 5000], clients => 10000, size => 16000, duration => $DUR, device => \"$DEV\"}), halt()."

# --- 3. real Ra servers: no snapshots, directories, the log -------------
for M in $(echo "$MEMBERS" | tr ',' ' '); do
    erl_run "05-e2e-$M-members" "snap_e2e_bench:run(\"$DIR/e2e\", #{members => $M, state_size => 1024, commands => $CMDS, snapshot_every => 5, modes => [none, directories, log], device => \"$DEV\"}), halt()."
done

# --- 4. around the size limit (16KB): 8000 and 15000 go to the log, 20000 does not
erl_run 06-e2e-sizes "[snap_e2e_bench:run(\"$DIR/e2e\", #{members => $SIZE_MEMBERS, state_size => S, commands => $CMDS, snapshot_every => 5, modes => [directories, log], device => \"$DEV\"}) || S <- [8000, 15000, 20000]], halt()."

# --- 5. the synthetic comparison of the alternatives (optional, slower) --
if [ "$MODE" = full ]; then
    erl_run 07-fs-alternatives "snap_fs_bench:run(\"$DIR/fs\", #{clusters => [1000], sizes => [1024, 8192], duration => 30, interval_ms => 2000, scenarios => [a, c2, d], cap => 4, device => \"$DEV\"}), halt()."
fi

rm -rf "$DIR/probe" "$DIR/store" "$DIR/e2e" "$DIR/fs" "$OUT/ebin"
echo
echo "done. Send the directory: $OUT"
ls -l "$OUT"
