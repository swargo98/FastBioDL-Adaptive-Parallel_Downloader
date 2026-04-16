#!/bin/bash
#SBATCH --job-name=placement_bench
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
# run_placement_benchmarks_expanse.sh
#
# Launches the generalized placement + mover benchmarks on SDSC Expanse.
#
# Tier mapping on Expanse:
#   fast  = local NVMe / node-local scratch
#   slow  = Lustre scratch
#   dest  = Lustre results directory
#
# This makes the benchmark uniform with the FABRIC / lab-server setup:
#   - conversion benchmark uses the full 8-cell factorial over
#       (sra_loc, fasterq_out, pigz_out) = F/S × F/S × F/S
#   - mover benchmark measures both fast -> slow and slow -> fast
#
# Typical usage:
#   sbatch run_placement_benchmarks_expanse.sh SRR_SMALL SRR_MEDIUM SRR_LARGE
#
# With overrides:
#   LUSTRE_ROOT=/expanse/lustre/scratch/$USER/myproj \
#   THREADS=16 REPS=5 CELLS=all \
#   sbatch run_placement_benchmarks_expanse.sh SRR1 SRR2 SRR3
#
# Environment variables honored:
#   FAST_DIR      default: auto-picked node-local scratch
#   LUSTRE_ROOT   default: /expanse/lustre/scratch/$USER/temp_project
#   SLOW_DIR      default: $LUSTRE_ROOT/scratch
#   DEST_DIR      default: $LUSTRE_ROOT/results
#   RESULTS_ROOT  default: $LUSTRE_ROOT/benchmark_results
#   THREADS       default: min($SLURM_CPUS_PER_TASK, 16)
#   REPS          default: 3
#   CELLS         default: all
#   METHODS       default: shutil_seq,shutil_parallel,cp,filemover
###############################################################################

set -euo pipefail

# ── Arguments ────────────────────────────────────────────────────────────
ACC_SMALL="${1:-}"
ACC_MEDIUM="${2:-}"
ACC_LARGE="${3:-}"
if [[ -z "$ACC_SMALL" || -z "$ACC_MEDIUM" || -z "$ACC_LARGE" ]]; then
    echo "Usage: sbatch $0 <ACCESSION_SMALL> <ACCESSION_MEDIUM> <ACCESSION_LARGE>" >&2
    exit 2
fi

REPO_DIR="${SLURM_SUBMIT_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)}"
cd "$REPO_DIR"

# ── Default thread count: mirror SLURM allocation, capped at 16 ─────────
detect_threads() {
    local n
    n="${SLURM_CPUS_PER_TASK:-$(nproc 2>/dev/null || echo 8)}"
    if (( n > 16 )); then n=16; fi
    echo "$n"
}

# ── Environment setup ────────────────────────────────────────────────────
module purge
module load slurm cpu/0.17.3b anaconda3/2021.05

CONDA_BASE="$(conda info --base 2>/dev/null || true)"
if [[ -z "$CONDA_BASE" || ! -f "$CONDA_BASE/etc/profile.d/conda.sh" ]]; then
    echo "[ERROR] Could not locate conda.sh after loading the anaconda module." >&2
    exit 1
fi

export PS1="${PS1:-}"
set +u
source "$CONDA_BASE/etc/profile.d/conda.sh"
conda activate fastbiodl
set -u

if [[ -d "$REPO_DIR/sratoolkit.3.1.0-ubuntu64/bin" ]]; then
    export PATH="$REPO_DIR/sratoolkit.3.1.0-ubuntu64/bin:$PATH"
fi

PYTHON_BIN="$(command -v python3)"
"$PYTHON_BIN" -c "import aiohttp, numpy" || {
    echo "[ERROR] Missing Python dependencies (aiohttp, numpy)." >&2
    exit 1
}

# ── Storage setup ────────────────────────────────────────────────────────
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
    for candidate in \
        "/scratch/$USER/job_${SLURM_JOB_ID:-manual}" \
        "/tmp/$USER/job_${SLURM_JOB_ID:-manual}"; do
        mkdir -p "$candidate" 2>/dev/null && { echo "$candidate"; return 0; }
    done
    return 1
}

FAST_DIR="${FAST_DIR:-}"
if [[ -z "$FAST_DIR" ]]; then
    FAST_DIR="$(pick_local_scratch)" || {
        echo "[ERROR] Cannot create local scratch directory." >&2
        exit 1
    }
fi

LUSTRE_ROOT="${LUSTRE_ROOT:-/expanse/lustre/scratch/$USER/temp_project}"
SLOW_DIR="${SLOW_DIR:-$LUSTRE_ROOT/scratch}"
DEST_DIR="${DEST_DIR:-$LUSTRE_ROOT/results}"
RESULTS_ROOT="${RESULTS_ROOT:-$LUSTRE_ROOT/benchmark_results}"
THREADS="${THREADS:-$(detect_threads)}"
REPS="${REPS:-3}"
CELLS="${CELLS:-all}"
METHODS="${METHODS:-shutil_seq,shutil_parallel,cp,filemover}"

JOB_TAG="$(date +%Y%m%d_%H%M%S)_${SLURM_JOB_ID:-manual}_$(hostname -s)"
RUN_OUT="$RESULTS_ROOT/placement_${JOB_TAG}"
mkdir -p "$RUN_OUT" "$FAST_DIR" "$SLOW_DIR" "$DEST_DIR"

# ── Banner / environment log ────────────────────────────────────────────
{
    echo "============================================================"
    echo "  Expanse placement + mover benchmark"
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
    echo "which fasterq-dump pigz cp:"
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
    "$PYTHON_BIN" "$REPO_DIR/benchmark_placement_conversion_expanse.py" \
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

    echo "[placement_mover: nvme -> lustre] Starting for $ACC ..."
    "$PYTHON_BIN" "$REPO_DIR/benchmark_placement_mover_expanse.py" \
        "$ACC" \
        --src-dir "$FAST_DIR" \
        --dst-dir "$SLOW_DIR" \
        --methods "$METHODS" \
        --threads "$THREADS" \
        --reps "$REPS" \
        --json-out "$RUN_OUT/placement_mover_nvme2lustre_${ACC}.json" \
        --cleanup \
    |& tee "$RUN_OUT/placement_mover_nvme2lustre_${ACC}.log" \
    || { echo "[WARN] placement_mover nvme->lustre failed for $ACC (rc=$?)"; overall_status=1; }

    echo ""

    echo "[placement_mover: lustre -> nvme] Starting for $ACC ..."
    "$PYTHON_BIN" "$REPO_DIR/benchmark_placement_mover_expanse.py" \
        "$ACC" \
        --src-dir "$SLOW_DIR" \
        --dst-dir "$FAST_DIR" \
        --prep-dir "$FAST_DIR" \
        --methods "$METHODS" \
        --threads "$THREADS" \
        --reps "$REPS" \
        --json-out "$RUN_OUT/placement_mover_lustre2nvme_${ACC}.json" \
        --cleanup \
    |& tee "$RUN_OUT/placement_mover_lustre2nvme_${ACC}.log" \
    || { echo "[WARN] placement_mover lustre->nvme failed for $ACC (rc=$?)"; overall_status=1; }

    echo ""
done

# ── Copy source files for reproducibility ────────────────────────────────
cp "$REPO_DIR/benchmark_placement_conversion_expanse.py" "$RUN_OUT/" 2>/dev/null || true
cp "$REPO_DIR/benchmark_placement_mover_expanse.py"      "$RUN_OUT/" 2>/dev/null || true
cp "$0"                                                  "$RUN_OUT/" 2>/dev/null || true
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
