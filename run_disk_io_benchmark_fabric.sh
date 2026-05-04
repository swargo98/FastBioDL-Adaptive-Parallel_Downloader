#!/bin/bash
# Run the storage-tier characterization benchmark on a Fabric (FABRIC
# testbed) slice. Mirrors run_benchmark_max_jobs_gridsearch_fabric.sh.
# Tiers: NVMe (RAID0) and HDD; cross-tier pair NVMe -> HDD is the
# move-stage workload on Fabric.
#
# Usage:
#   ./run_disk_io_benchmark_fabric.sh
#
# Optional environment overrides:
#   PYTHON_BIN      Python interpreter                      (default python3)
#   NVME_MOUNT      NVMe mount point                        (default /mnt/raid0)
#   HDD_MOUNT       HDD  mount point                        (default /home/ubuntu/...)
#   REPEATS         repeats per (test, tier, numjobs)       (default 3)
#   NUMJOBS         comma-separated numjobs sweep           (default 1,2,4,8,16)
#   SIZE            per-job working set                     (default 4G)
#   RUNTIME         fio runtime per invocation in seconds   (default 30)
#   RAMP            ramp_time skipped before measurement    (default 5)
#   METADATA_FILES  files for metadata create/unlink test   (default 20000)
#   SKIP            comma-separated tests to skip           (default empty)
#   JOB_ID          tag used in temp dir names              (default $$)

set -uo pipefail

PYTHON_BIN="${PYTHON_BIN:-python3}"

if ! command -v fio >/dev/null 2>&1; then
    echo "[ERROR] fio not on PATH. apt-get install fio (or build from source)." >&2
    exit 1
fi

REPO_DIR="${REPO_DIR:-$PWD}"
cd "$REPO_DIR" || exit 1

# ---------------------------------------------------------------------------
# Storage layout: NVMe (RAID0) + HDD
# ---------------------------------------------------------------------------
NVME_MOUNT="${NVME_MOUNT:-/mnt/raid0}"
HDD_MOUNT="${HDD_MOUNT:-/home/ubuntu/FastBioDL-Adaptive-Parallel_Downloader}"
JOB_ID="${JOB_ID:-$$}"

for tier in "$NVME_MOUNT" "$HDD_MOUNT"; do
    if [[ ! -d "$tier" ]]; then
        echo "[ERROR] Storage mount not found: $tier" >&2
        echo "        Set NVME_MOUNT and HDD_MOUNT to your Fabric slice paths." >&2
        exit 1
    fi
done

NVME_BASE="$NVME_MOUNT/$USER/disk_io_bench/$JOB_ID"
HDD_BASE="$HDD_MOUNT/$USER/disk_io_bench/$JOB_ID"

mkdir -p "$NVME_BASE" "$HDD_BASE" || { echo "[ERROR] Could not create base directories"; exit 1; }

NVME_TIER="$NVME_BASE/nvme_tier"
HDD_TIER="$HDD_BASE/hdd_tier"
RESULTS_OUT="$HDD_BASE/results"
LOG_DIR="$HDD_BASE/fio_logs"

mkdir -p "$NVME_TIER" "$HDD_TIER" "$RESULTS_OUT" "$LOG_DIR"

JSON_OUT="$RESULTS_OUT/disk_io_benchmark_fabric.json"

REPEATS="${REPEATS:-3}"
NUMJOBS="${NUMJOBS:-1,2,4,8,16}"
SIZE="${SIZE:-4G}"
RUNTIME="${RUNTIME:-30}"
RAMP="${RAMP:-5}"
METADATA_FILES="${METADATA_FILES:-20000}"
SKIP="${SKIP:-}"

echo "=========================================="
echo "Repo:            $REPO_DIR"
echo "Python:          $PYTHON_BIN"
echo "fio:             $(command -v fio) ($(fio --version 2>/dev/null | head -n1))"
echo "NVMe mount:      $NVME_MOUNT"
echo "HDD  mount:      $HDD_MOUNT"
echo "NVMe tier:       $NVME_TIER"
echo "HDD  tier:       $HDD_TIER"
echo "Repeats:         $REPEATS"
echo "numjobs sweep:   $NUMJOBS"
echo "size per job:    $SIZE"
echo "runtime:         ${RUNTIME}s    ramp: ${RAMP}s"
echo "metadata files:  $METADATA_FILES"
echo "skip:            ${SKIP:-<none>}"
echo "Results:         $RESULTS_OUT"
echo "=========================================="

EXTRA=()
if [[ -n "$SKIP" ]]; then
    EXTRA+=( --skip "$SKIP" )
fi

"$PYTHON_BIN" -u disk_io_benchmark.py \
    --tier "nvme=$NVME_TIER" \
    --tier "hdd=$HDD_TIER" \
    --repeats "$REPEATS" \
    --numjobs "$NUMJOBS" \
    --size "$SIZE" \
    --runtime "$RUNTIME" \
    --ramp "$RAMP" \
    --metadata-files "$METADATA_FILES" \
    --json-out "$JSON_OUT" \
    --log-dir "$LOG_DIR" \
    "${EXTRA[@]}"
status=$?

cp "$REPO_DIR/run_disk_io_benchmark_fabric.sh" "$RESULTS_OUT/" 2>/dev/null || true
cp "$REPO_DIR/disk_io_benchmark.py" "$RESULTS_OUT/" 2>/dev/null || true

rm -rf "$NVME_TIER" "$HDD_TIER" 2>/dev/null || true

echo "Benchmark exit code: $status"
echo "Results in $RESULTS_OUT"
exit "$status"
