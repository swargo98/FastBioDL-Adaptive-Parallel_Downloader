#!/bin/bash
#SBATCH --job-name=staging_mover_bench
#SBATCH --account=umr115
#SBATCH --partition=compute
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=16
#SBATCH --mem=64G
#SBATCH --time=24:00:00
#SBATCH --output=slurm_%j.out
#SBATCH --error=slurm_%j.err

###############################################################################
# run_staging_mover_benchmarks_expanse.sh
#
# Runs benchmark_nvme_staging.py and benchmark_mover_transfer.py for three
# accessions covering small, medium, and large SRA files.
#
# Usage:
#   sbatch run_staging_mover_benchmarks_expanse.sh SRR_SMALL SRR_MEDIUM SRR_LARGE
#
# Or with custom thread count and rep count:
#   THREADS=16 REPS=5 sbatch run_staging_mover_benchmarks_expanse.sh SRR1 SRR2 SRR3
###############################################################################

set -euo pipefail

# ── Environment ────────────────────────────────────────────────────────────
module purge
module load slurm cpu/0.17.3b anaconda3/2021.05

CONDA_BASE="$(conda info --base 2>/dev/null || true)"
if [[ -z "$CONDA_BASE" || ! -f "$CONDA_BASE/etc/profile.d/conda.sh" ]]; then
    echo "[ERROR] Could not locate conda.sh after loading the anaconda module." >&2
    exit 1
fi
set +u
source "$CONDA_BASE/etc/profile.d/conda.sh"
conda activate fastbiodl
set -u

REPO_DIR="${SLURM_SUBMIT_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)}"
cd "$REPO_DIR"

export PATH="$REPO_DIR/sratoolkit.3.1.0-ubuntu64/bin:$PATH"
PYTHON_BIN="$(command -v python3)"

"$PYTHON_BIN" -c "import aiohttp" || { echo "[ERROR] aiohttp missing"; exit 1; }

# ── Arguments ──────────────────────────────────────────────────────────────
ACC_SMALL="${1:-}"
ACC_MEDIUM="${2:-}"
ACC_LARGE="${3:-}"

if [[ -z "$ACC_SMALL" || -z "$ACC_MEDIUM" || -z "$ACC_LARGE" ]]; then
    echo "Usage: sbatch $0 <ACCESSION_SMALL> <ACCESSION_MEDIUM> <ACCESSION_LARGE>" >&2
    exit 2
fi

THREADS="${THREADS:-8}"
REPS="${REPS:-3}"

# ── Storage setup ──────────────────────────────────────────────────────────
pick_local_scratch() {
    local candidate
    if [[ -n "${LOCAL_SCRATCH:-}" ]]; then
        candidate="${LOCAL_SCRATCH}"
        mkdir -p "$candidate" 2>/dev/null && { echo "$candidate"; return 0; }
    fi
    if [[ -n "${SLURM_TMPDIR:-}" ]]; then
        candidate="${SLURM_TMPDIR}"
        mkdir -p "$candidate" 2>/dev/null && { echo "$candidate"; return 0; }
    fi
    for candidate in "/scratch/$USER/job_$SLURM_JOB_ID" "/tmp/$USER/job_$SLURM_JOB_ID"; do
        mkdir -p "$candidate" 2>/dev/null && { echo "$candidate"; return 0; }
    done
    return 1
}

NVME_DIR="$(pick_local_scratch)" || {
    echo "[ERROR] Cannot create local scratch directory." >&2
    exit 1
}

RESULTS_ROOT="${RESULTS_ROOT:-$(dirname "$REPO_DIR")/benchmark_results}"
RUN_OUT="$RESULTS_ROOT/staging_mover_benchmarks_${SLURM_JOB_ID}"
mkdir -p "$RUN_OUT"

export LOCAL_SCRATCH="$NVME_DIR"
export SLURM_JOB_ID="${SLURM_JOB_ID}"

echo "============================================================"
echo "  Staging + Mover benchmark batch"
echo "============================================================"
echo "Repository:    $REPO_DIR"
echo "Python:        $PYTHON_BIN"
echo "NVMe scratch:  $NVME_DIR"
echo "Results:       $RUN_OUT"
echo "Threads:       $THREADS"
echo "Reps per mode: $REPS"
echo "Accessions:    $ACC_SMALL (small)  $ACC_MEDIUM (medium)  $ACC_LARGE (large)"
echo ""
which fasterq-dump pigz cp || true
echo "============================================================"

overall_status=0

for ACC in "$ACC_SMALL" "$ACC_MEDIUM" "$ACC_LARGE"; do
    echo ""
    echo "============================================================"
    echo "  Accession: $ACC"
    echo "============================================================"

    # ── Benchmark 1: NVMe staging ──────────────────────────────────────
    echo "[nvme_staging] Starting for $ACC ..."
    "$PYTHON_BIN" "$REPO_DIR/benchmark_nvme_staging.py" \
        "$ACC" \
        --nvme-dir "$NVME_DIR" \
        --lustre-dir "$RUN_OUT" \
        --threads "$THREADS" \
        --reps "$REPS" \
        --json-out "$RUN_OUT/nvme_staging_${ACC}.json" \
        --cleanup \
    || { echo "[WARN] benchmark_nvme_staging.py failed for $ACC (rc=$?)"; overall_status=1; }

    echo ""

    # ── Benchmark 2: mover transfer ───────────────────────────────────
    echo "[mover_transfer] Starting for $ACC ..."
    "$PYTHON_BIN" "$REPO_DIR/benchmark_mover_transfer.py" \
        "$ACC" \
        --nvme-dir "$NVME_DIR" \
        --lustre-dir "$RUN_OUT" \
        --threads "$THREADS" \
        --reps "$REPS" \
        --json-out "$RUN_OUT/mover_transfer_${ACC}.json" \
        --cleanup \
    || { echo "[WARN] benchmark_mover_transfer.py failed for $ACC (rc=$?)"; overall_status=1; }

    echo ""
done

# ── Copy artifacts to results ──────────────────────────────────────────────
echo "Copying scripts and logs to $RUN_OUT ..."
cp "$REPO_DIR/benchmark_nvme_staging.py"   "$RUN_OUT/" 2>/dev/null || true
cp "$REPO_DIR/benchmark_mover_transfer.py" "$RUN_OUT/" 2>/dev/null || true
cp "$0"                                    "$RUN_OUT/" 2>/dev/null || true
if [[ -f "slurm_${SLURM_JOB_ID}.out" ]]; then
    cp "slurm_${SLURM_JOB_ID}.out" "$RUN_OUT/" 2>/dev/null || true
fi
if [[ -f "slurm_${SLURM_JOB_ID}.err" ]]; then
    cp "slurm_${SLURM_JOB_ID}.err" "$RUN_OUT/" 2>/dev/null || true
fi

echo ""
echo "============================================================"
echo "  All benchmarks complete.  exit=$overall_status"
echo "  Results: $RUN_OUT"
echo "============================================================"
exit "$overall_status"
