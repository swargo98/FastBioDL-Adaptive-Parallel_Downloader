#!/usr/bin/env bash
###############################################################################
# run_placement_benchmarks_labserver.sh
#
# Launches placement + mover benchmarks on a local lab server.
#
# Tier mapping (defaults; override via env vars):
#   fast  = /mnt/nvme/$USER/scratch    (NVMe SSD)
#   slow  = /mnt/hdd/$USER/scratch     (spinning disk or HDD-backed RAID)
#   dest  = /mnt/hdd/$USER/results     (persistent; same device as slow)
#
# The destination equals slow by default because on a lab server the HDD
# is typically the durable tier.  If your lab server keeps results on NVMe
# and only uses HDD for archival staging, override DEST_DIR=$FAST_DIR/...
#
# Usage
#   ./run_placement_benchmarks_labserver.sh SRR_SMALL SRR_MEDIUM SRR_LARGE
#
#   With overrides (typical):
#     FAST_DIR=/data/nvme/$USER SLOW_DIR=/data/hdd/$USER \
#       THREADS=16 REPS=5 \
#       ./run_placement_benchmarks_labserver.sh SRR1 SRR2 SRR3
#
# Environment variables honored:
#   FAST_DIR   (default: /mnt/nvme/$USER/scratch)
#   SLOW_DIR   (default: /mnt/hdd/$USER/scratch)
#   DEST_DIR   (default: $SLOW_DIR/results)
#   THREADS    (default: number of CPU cores, capped at 16)
#   REPS       (default: 3)
#   RESULTS_ROOT (default: ./benchmark_results_labserver)
#   CELLS      (default: all)
#   METHODS    (default: shutil_seq,shutil_parallel,cp,filemover)
###############################################################################

set -euo pipefail

# ── Arguments ────────────────────────────────────────────────────────────
ACC_SMALL="${1:-}"
ACC_MEDIUM="${2:-}"
ACC_LARGE="${3:-}"
if [[ -z "$ACC_SMALL" || -z "$ACC_MEDIUM" || -z "$ACC_LARGE" ]]; then
    echo "Usage: $0 <ACCESSION_SMALL> <ACCESSION_MEDIUM> <ACCESSION_LARGE>" >&2
    exit 2
fi

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$REPO_DIR"

# ── Default thread count: min(nproc, 16) to mirror typical HPC allocations ──
detect_threads() {
    local n
    n="$(nproc 2>/dev/null || echo 8)"
    if (( n > 16 )); then n=16; fi
    echo "$n"
}

FAST_DIR="${FAST_DIR:-/home/rs75c/FastBioDL-Adaptive-Parallel_Downloader/benchmark_slow_storage}"
SLOW_DIR="${SLOW_DIR:-/mnt/storage/benchmark_fast_storage}"
DEST_DIR="${DEST_DIR:-$SLOW_DIR/results}"
THREADS="${THREADS:-$(detect_threads)}"
REPS="${REPS:-3}"
CELLS="${CELLS:-all}"
METHODS="${METHODS:-shutil_seq,shutil_parallel,cp,filemover}"

JOB_TAG="$(date +%Y%m%d_%H%M%S)_$(hostname -s)"
RESULTS_ROOT="${RESULTS_ROOT:-$(dirname "$REPO_DIR")/benchmark_results_labserver}"
RUN_OUT="$RESULTS_ROOT/placement_${JOB_TAG}"
mkdir -p "$RUN_OUT" "$FAST_DIR" "$SLOW_DIR" "$DEST_DIR"

# ── Environment ──────────────────────────────────────────────────────────
PYTHON_BIN="$(command -v python3)"
if [[ -z "${VIRTUAL_ENV:-}" && -z "${CONDA_DEFAULT_ENV:-}" ]]; then
    if command -v conda >/dev/null 2>&1; then
        CONDA_BASE="$(conda info --base 2>/dev/null || true)"
        if [[ -n "$CONDA_BASE" && -f "$CONDA_BASE/etc/profile.d/conda.sh" ]]; then
            set +u
            source "$CONDA_BASE/etc/profile.d/conda.sh"
            if conda env list | awk '{print $1}' | grep -qx fastbiodl; then
                conda activate fastbiodl
                PYTHON_BIN="$(command -v python3)"
            fi
            set -u
        fi
    fi
fi

if [[ -d "$REPO_DIR/sratoolkit.3.1.0-ubuntu64/bin" ]]; then
    export PATH="$REPO_DIR/sratoolkit.3.1.0-ubuntu64/bin:$PATH"
fi

"$PYTHON_BIN" -c "import aiohttp, numpy" || {
    echo "[ERROR] Missing Python dependencies (aiohttp, numpy)." >&2
    exit 1
}

# ── Detect filesystem types for logging ──────────────────────────────────
{
    echo "============================================================"
    echo "  Lab-server placement + mover benchmark"
    echo "============================================================"
    echo "Host:         $(hostname)"
    echo "Date:         $(date -Is)"
    echo "Repo:         $REPO_DIR"
    echo "Python:       $PYTHON_BIN"
    echo "FAST_DIR:     $FAST_DIR"
    echo "SLOW_DIR:     $SLOW_DIR"
    echo "DEST_DIR:     $DEST_DIR"
    echo "THREADS:      $THREADS"
    echo "REPS:         $REPS"
    echo "CELLS:        $CELLS"
    echo "METHODS:      $METHODS"
    echo "Accessions:   $ACC_SMALL (small)  $ACC_MEDIUM (medium)  $ACC_LARGE (large)"
    echo "Results:      $RUN_OUT"
    echo "------------------------------------------------------------"
    echo "Storage info:"
    df -h "$FAST_DIR" "$SLOW_DIR" "$DEST_DIR" 2>/dev/null || true
    echo "------------------------------------------------------------"
    echo "Mount / device:"
    for d in "$FAST_DIR" "$SLOW_DIR" "$DEST_DIR"; do
        findmnt -n -o SOURCE,TARGET,FSTYPE,OPTIONS "$d" 2>/dev/null || true
    done
    echo "------------------------------------------------------------"
    echo "Block devices (rotational flag: 1=HDD, 0=SSD/NVMe):"
    for d in "$FAST_DIR" "$SLOW_DIR" "$DEST_DIR"; do
        src="$(findmnt -n -o SOURCE "$d" 2>/dev/null || echo unknown)"
        dev="$(basename "$src" | sed 's/[0-9]*$//')"
        rot_file="/sys/block/$dev/queue/rotational"
        if [[ -r "$rot_file" ]]; then
            rot="$(cat "$rot_file")"
            echo "  $d -> $src (/dev/$dev)  rotational=$rot"
        else
            echo "  $d -> $src (/dev/$dev)  rotational=unknown"
        fi
    done
    echo "------------------------------------------------------------"
    which fasterq-dump pigz cp || true
    echo "============================================================"
} | tee "$RUN_OUT/environment.txt"

overall_status=0

for ACC in "$ACC_SMALL" "$ACC_MEDIUM" "$ACC_LARGE"; do
    echo ""
    echo "============================================================"
    echo "  Accession: $ACC"
    echo "============================================================"

    echo "[placement_conversion] Starting for $ACC ..."
    "$PYTHON_BIN" "$REPO_DIR/benchmark_placement_conversion.py" \
        "$ACC" \
        --fast-dir "$FAST_DIR" \
        --slow-dir "$SLOW_DIR" \
        --dest-dir "$DEST_DIR" \
        --cells "$CELLS" \
        --threads "$THREADS" \
        --reps "$REPS" \
        --json-out "$RUN_OUT/placement_conversion_${ACC}.json" \
        --cleanup \
    |& tee "$RUN_OUT/placement_conversion_${ACC}.log" \
    || { echo "[WARN] placement_conversion failed for $ACC (rc=$?)"; overall_status=1; }

    echo ""

    echo "[placement_mover: nvme -> hdd] Starting for $ACC ..."
    "$PYTHON_BIN" "$REPO_DIR/benchmark_placement_mover.py" \
        "$ACC" \
        --src-dir "$FAST_DIR" \
        --dst-dir "$SLOW_DIR" \
        --methods "$METHODS" \
        --threads "$THREADS" \
        --reps "$REPS" \
        --json-out "$RUN_OUT/placement_mover_nvme2hdd_${ACC}.json" \
        --cleanup \
    |& tee "$RUN_OUT/placement_mover_nvme2hdd_${ACC}.log" \
    || { echo "[WARN] placement_mover nvme->hdd failed for $ACC (rc=$?)"; overall_status=1; }

    echo ""

    # HDD -> NVMe is the reverse direction.  Pre-download is done on NVMe
    # (fast) for speed, then golden set is copied to HDD as the source.
    echo "[placement_mover: hdd -> nvme] Starting for $ACC ..."
    "$PYTHON_BIN" "$REPO_DIR/benchmark_placement_mover.py" \
        "$ACC" \
        --src-dir "$SLOW_DIR" \
        --dst-dir "$FAST_DIR" \
        --prep-dir "$FAST_DIR" \
        --methods "$METHODS" \
        --threads "$THREADS" \
        --reps "$REPS" \
        --json-out "$RUN_OUT/placement_mover_hdd2nvme_${ACC}.json" \
        --cleanup \
    |& tee "$RUN_OUT/placement_mover_hdd2nvme_${ACC}.log" \
    || { echo "[WARN] placement_mover hdd->nvme failed for $ACC (rc=$?)"; overall_status=1; }

    echo ""
done

cp "$REPO_DIR/benchmark_placement_conversion.py" "$RUN_OUT/" 2>/dev/null || true
cp "$REPO_DIR/benchmark_placement_mover.py"      "$RUN_OUT/" 2>/dev/null || true
cp "$0"                                          "$RUN_OUT/" 2>/dev/null || true

echo ""
echo "============================================================"
echo "  All benchmarks complete.  exit=$overall_status"
echo "  Results: $RUN_OUT"
echo "============================================================"
exit "$overall_status"
