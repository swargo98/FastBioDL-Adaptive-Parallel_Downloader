#!/bin/bash
#SBATCH --job-name=max_jobs_gridsearch
#SBATCH --account=umr115
#SBATCH --partition=compute
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=16
#SBATCH --mem=64G
#SBATCH --time=12:00:00
#SBATCH --output=slurm_%j.out
#SBATCH --error=slurm_%j.err

set -uo pipefail

PYTHON_BIN="/home/rswargo/.conda/envs/fastbiodl/bin/python"
export PATH="/home/rswargo/.conda/envs/fastbiodl/bin:$PATH"

"$PYTHON_BIN" -c "import aiohttp" || { echo "[ERROR] aiohttp missing"; exit 1; }

ACCESSION="${1:-}"
if [[ -z "$ACCESSION" ]]; then
    echo "Usage: sbatch run_benchmark_max_jobs_gridsearch_expanse.sh <ACCESSION>" >&2
    exit 2
fi

REPO_DIR="${SLURM_SUBMIT_DIR:-$PWD}"
cd "$REPO_DIR" || exit 1

NVME_DIR="/scratch/$USER/job_$SLURM_JOB_ID"
if ! mkdir -p "$NVME_DIR" 2>/dev/null; then
    NVME_DIR="/tmp/$USER/job_$SLURM_JOB_ID"
    mkdir -p "$NVME_DIR" || { echo "[ERROR] Could not create local scratch"; exit 1; }
fi

LUSTRE_OUT="/expanse/lustre/scratch/$USER/temp_project/results/$SLURM_JOB_ID"
mkdir -p "$LUSTRE_OUT" || { echo "[ERROR] Could not create results directory: $LUSTRE_OUT"; exit 1; }

export LOCAL_SCRATCH="$NVME_DIR"
export GRIDSEARCH_RESULTS_DIR="$LUSTRE_OUT"

WORK_ROOT="$NVME_DIR/fastbiodl_gridsearch_${ACCESSION}"
JSON_OUT="$LUSTRE_OUT/benchmark_max_jobs_${ACCESSION}.json"
THREADS="${THREADS:-${SLURM_CPUS_PER_TASK:-16}}"

echo "Repo:      $REPO_DIR"
echo "Python:    $PYTHON_BIN"
echo "Accession: $ACCESSION"
echo "NVMe:      $NVME_DIR"
echo "Work root: $WORK_ROOT"
echo "Results:   $LUSTRE_OUT"
which aria2c fasterq-dump pigz

"$PYTHON_BIN" benchmark_max_jobs_gridsearch.py \
    "$ACCESSION" \
    --threads "$THREADS" \
    --work-root "$WORK_ROOT" \
    --json-out "$JSON_OUT" \
    --cleanup-work-root
status=$?

cp "$REPO_DIR/run_benchmark_max_jobs_gridsearch_expanse.sh" "$LUSTRE_OUT/" 2>/dev/null || true
cp "$REPO_DIR/benchmark_max_jobs_gridsearch.py" "$LUSTRE_OUT/" 2>/dev/null || true
cp "slurm_${SLURM_JOB_ID}.out" "slurm_${SLURM_JOB_ID}.err" "$LUSTRE_OUT/" 2>/dev/null || true

echo "Grid search exit code: $status"
echo "Results in $LUSTRE_OUT"
exit "$status"
