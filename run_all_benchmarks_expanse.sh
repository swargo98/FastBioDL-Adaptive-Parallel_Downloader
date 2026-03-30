#!/bin/bash
#SBATCH --job-name=all_benchmarks
#SBATCH --account=umr115         # e.g. abc123 or TG-ABC123456
#SBATCH --partition=compute               # exclusive node, 128 cores, 1 TB NVMe
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=16                # covers the default thread counts used by the benchmarks
#SBATCH --mem=64G
#SBATCH --time=24:00:00                  # increase if needed for repeated multi-dataset runs
#SBATCH --output=slurm_%j.out
#SBATCH --error=slurm_%j.err

set -euo pipefail

# ─── Environment ───────────────────────────────────────────────────────────
module purge
module load slurm cpu/0.17.3b anaconda3/2021.05

CONDA_BASE="$(conda info --base 2>/dev/null || true)"
if [[ -z "$CONDA_BASE" || ! -f "$CONDA_BASE/etc/profile.d/conda.sh" ]]; then
    echo "[ERROR] Could not locate conda.sh after loading the anaconda module." >&2
    exit 1
fi
source "$CONDA_BASE/etc/profile.d/conda.sh"
conda activate fastbiodl

REPO_DIR="${SLURM_SUBMIT_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)}"
cd "$REPO_DIR"

# Add sra-toolkit to PATH
export PATH="$REPO_DIR/sratoolkit.3.1.0-ubuntu64/bin:$PATH"
PYTHON_BIN="$(command -v python3)"

if ! "$PYTHON_BIN" -c "import aiohttp" >/dev/null 2>&1; then
    echo "[ERROR] aiohttp is not importable from $PYTHON_BIN. Conda env activation failed." >&2
    exit 1
fi

# ─── Storage setup ─────────────────────────────────────────────────────────
pick_local_scratch() {
    local candidate

    if [[ -n "${LOCAL_SCRATCH:-}" ]]; then
        candidate="${LOCAL_SCRATCH}"
        if mkdir -p "$candidate" 2>/dev/null; then
            echo "$candidate"
            return 0
        fi
    fi

    if [[ -n "${SLURM_TMPDIR:-}" ]]; then
        candidate="${SLURM_TMPDIR}"
        if mkdir -p "$candidate" 2>/dev/null; then
            echo "$candidate"
            return 0
        fi
    fi

    for candidate in "/scratch/$USER/job_$SLURM_JOB_ID" "/tmp/$USER/job_$SLURM_JOB_ID"; do
        if mkdir -p "$candidate" 2>/dev/null; then
            echo "$candidate"
            return 0
        fi
    done

    return 1
}

NVME_DIR="$(pick_local_scratch)" || {
    echo "[ERROR] Unable to create a writable local scratch directory." >&2
    exit 1
}

# Final output: Lustre, persists after job
RESULTS_ROOT="${RESULTS_ROOT:-$(dirname "$REPO_DIR")/benchmark_results}"
RUN_OUT="$RESULTS_ROOT/run_all_benchmarks_${SLURM_JOB_ID}"
mkdir -p "$RUN_OUT"
mkdir -p logs

# Export so the Python code can pick them up
export LOCAL_SCRATCH="$NVME_DIR"
export SLURM_JOB_ID="$SLURM_JOB_ID"

# ─── Optional: your NCBI credentials for higher rate limits ────────────────
# export NCBI_EMAIL="your@email.com"
# export NCBI_API_KEY="your_key"

echo "=== Expanse benchmark batch job starting ==="
echo "Repository: $REPO_DIR"
echo "Python: $PYTHON_BIN"
echo "Local scratch: $NVME_DIR"
echo "Results root: $RUN_OUT"

status=0
bash "$REPO_DIR/run_all_benchmarks.sh" || status=$?

echo "=== Copying logs and run metadata to Lustre ==="
if [[ -d "$REPO_DIR/logs" ]]; then
    cp -r "$REPO_DIR/logs" "$RUN_OUT/"
fi
if [[ -f "$REPO_DIR/clear_files_deletion.log" ]]; then
    cp "$REPO_DIR/clear_files_deletion.log" "$RUN_OUT/"
fi
cp "$REPO_DIR/run_all_benchmarks.sh" "$RUN_OUT/"
cp "$REPO_DIR/run_all_benchmarks_expanse.sh" "$RUN_OUT/"
if [[ -f "slurm_${SLURM_JOB_ID}.out" ]]; then
    cp "slurm_${SLURM_JOB_ID}.out" "$RUN_OUT/"
fi
if [[ -f "slurm_${SLURM_JOB_ID}.err" ]]; then
    cp "slurm_${SLURM_JOB_ID}.err" "$RUN_OUT/"
fi

echo "=== Done. Results in $RUN_OUT ==="
exit "$status"
