#!/usr/bin/env bash
###############################################################################
# run_placement_benchmarks_fabric.sh
#
# Launches placement + mover benchmarks on a FABRIC testbed.
#
# Tier mapping on FABRIC:
#   fast  = /mnt/raid0/$USER/scratch   (RAID0 across multiple NVMes/SSDs)
#   slow  = /var/tmp/$USER/scratch     (single local disk)
#   dest  = /mnt/raid0/$USER/results   (persistent-ish, same device as fast)
#
# Rationale for dest == fast on FABRIC: testbeds typically do not have a
# separate persistent storage tier distinct from the RAID0.  The "slow"
# tier here is the single-disk root filesystem, not a durable archive.
# Users deploying FastBioDL on FABRIC write their final .fastq.gz to the
# RAID0 mount.  If you want to model a different destination (e.g. export
# to an S3 or NFS mount), override DEST_DIR.
#
# Usage
#   ./run_placement_benchmarks_fabric.sh SRR_SMALL SRR_MEDIUM SRR_LARGE
#
#   With overrides:
#     FAST_DIR=/mnt/raid0/foo SLOW_DIR=/tmp/foo DEST_DIR=/mnt/nfs/foo \
#       THREADS=16 REPS=5 \
#       ./run_placement_benchmarks_fabric.sh SRR1 SRR2 SRR3
#
# Environment variables honored:
#   FAST_DIR   (default: /mnt/raid0/$USER/scratch)
#   SLOW_DIR   (default: /var/tmp/$USER/scratch)
#   DEST_DIR   (default: $FAST_DIR/results)
#   THREADS    (default: 8)
#   REPS       (default: 3)
#   RESULTS_ROOT (default: ./benchmark_results_fabric)
#   CELLS      (default: all)  -- e.g. "FFF,FFS,SSS" to skip some
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

# ── Paths ────────────────────────────────────────────────────────────────
REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$REPO_DIR"

FAST_DIR="${FAST_DIR:-/mnt/raid0/benchmark/fast}"
SLOW_DIR="${SLOW_DIR:-/home/ubuntu/FastBioDL-Adaptive-Parallel_Downloader}"
DEST_DIR="${DEST_DIR:-$SLOW_DIR/results}"
THREADS="${THREADS:-8}"
REPS="${REPS:-3}"
CELLS="${CELLS:-all}"
METHODS="${METHODS:-shutil_seq,shutil_parallel,cp,filemover}"

JOB_TAG="$(date +%Y%m%d_%H%M%S)_$(hostname -s)"
RESULTS_ROOT="${RESULTS_ROOT:-$(dirname "$REPO_DIR")/benchmark_results_fabric}"
RUN_OUT="$RESULTS_ROOT/placement_${JOB_TAG}"
mkdir -p "$RUN_OUT" "$FAST_DIR" "$SLOW_DIR" "$DEST_DIR"

# ── Environment ──────────────────────────────────────────────────────────
# FABRIC VMs usually ship with a system Python and may or may not have
# conda installed.  We prefer an active virtualenv or conda env if one is
# already activated; otherwise we fall back to system python3.

PYTHON_BIN="$(command -v python3)"
if [[ -z "${VIRTUAL_ENV:-}" && -z "${CONDA_DEFAULT_ENV:-}" ]]; then
    # Try conda 'fastbiodl' env if conda is available
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

# Add bundled sratoolkit to PATH if present
if [[ -d "$REPO_DIR/sratoolkit.3.1.0-ubuntu64/bin" ]]; then
    export PATH="$REPO_DIR/sratoolkit.3.1.0-ubuntu64/bin:$PATH"
fi

"$PYTHON_BIN" -c "import aiohttp, numpy" || {
    echo "[ERROR] Missing Python dependencies (aiohttp, numpy)." >&2
    exit 1
}

# ── Banner ───────────────────────────────────────────────────────────────
{
    echo "============================================================"
    echo "  FABRIC placement + mover benchmark"
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
    echo "Mount points:"
    findmnt "$FAST_DIR" 2>/dev/null || true
    findmnt "$SLOW_DIR" 2>/dev/null || true
    findmnt "$DEST_DIR" 2>/dev/null || true
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

    # ── Benchmark 1: placement (conversion) ─────────────────────────
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

    # ── Benchmark 2: mover (src=fast -> dst=slow) ───────────────────
    echo "[placement_mover: fast -> slow] Starting for $ACC ..."
    "$PYTHON_BIN" "$REPO_DIR/benchmark_placement_mover.py" \
        "$ACC" \
        --src-dir "$FAST_DIR" \
        --dst-dir "$SLOW_DIR" \
        --methods "$METHODS" \
        --threads "$THREADS" \
        --reps "$REPS" \
        --json-out "$RUN_OUT/placement_mover_f2s_${ACC}.json" \
        --cleanup \
    |& tee "$RUN_OUT/placement_mover_f2s_${ACC}.log" \
    || { echo "[WARN] placement_mover f2s failed for $ACC (rc=$?)"; overall_status=1; }

    echo ""

    # ── Benchmark 3: mover (src=slow -> dst=fast) ───────────────────
    # This measures the reverse: if a user's SRA happens to be on the
    # slow tier and they want to pull it to fast scratch for processing.
    echo "[placement_mover: slow -> fast] Starting for $ACC ..."
    "$PYTHON_BIN" "$REPO_DIR/benchmark_placement_mover.py" \
        "$ACC" \
        --src-dir "$SLOW_DIR" \
        --dst-dir "$FAST_DIR" \
        --prep-dir "$FAST_DIR" \
        --methods "$METHODS" \
        --threads "$THREADS" \
        --reps "$REPS" \
        --json-out "$RUN_OUT/placement_mover_s2f_${ACC}.json" \
        --cleanup \
    |& tee "$RUN_OUT/placement_mover_s2f_${ACC}.log" \
    || { echo "[WARN] placement_mover s2f failed for $ACC (rc=$?)"; overall_status=1; }

    echo ""
done

# ── Copy source files for reproducibility ────────────────────────────────
cp "$REPO_DIR/benchmark_placement_conversion.py" "$RUN_OUT/" 2>/dev/null || true
cp "$REPO_DIR/benchmark_placement_mover.py"      "$RUN_OUT/" 2>/dev/null || true
cp "$0"                                          "$RUN_OUT/" 2>/dev/null || true

echo ""
echo "============================================================"
echo "  All benchmarks complete.  exit=$overall_status"
echo "  Results: $RUN_OUT"
echo "============================================================"
exit "$overall_status"
