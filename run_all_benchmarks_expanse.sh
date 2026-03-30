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
conda activate fastbiodl

REPO_DIR="/expanse/lustre/scratch/$USER/temp_project/FastBioDL-Adaptive-Parallel_Downloader"
cd "$REPO_DIR"

# Add sra-toolkit to PATH
export PATH="$REPO_DIR/sratoolkit.3.1.0-ubuntu64/bin:$PATH"

# ─── Storage setup ─────────────────────────────────────────────────────────
# Local NVMe: fast scratch, wiped at job end
NVME_DIR="/scratch/$USER/temp_project/job_$SLURM_JOB_ID"
mkdir -p "$NVME_DIR"

# Final output: Lustre, persists after job
RESULTS_ROOT="/expanse/lustre/scratch/$USER/temp_project/benchmark_results"
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
