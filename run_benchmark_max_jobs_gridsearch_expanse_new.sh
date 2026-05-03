#!/bin/bash
# Run the multi-accession, multi-repeat benchmark on SDSC Expanse.
# Storage layout: sra_out=Lustre, fastq_out=Lustre, pigz_out=NVMe.
#
# Usage:
#   sbatch run_benchmark_max_jobs_gridsearch_expanse.sh ACC1 ACC2 ACC3
#
# Optional environment overrides:
#   THREADS         threads passed to fasterq-dump and pigz (default 8)
#   REPEATS         repeats per (accession, configuration) (default 3)
#   SRA_OUT_DIR     override default SRA tier path
#   FASTQ_OUT_DIR   override default FASTQ tier path
#   PIGZ_OUT_DIR    override default pigz tier path

#SBATCH --job-name=max_jobs_bench
#SBATCH --account=umr115
#SBATCH --partition=compute
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=64
#SBATCH --mem=64G
#SBATCH --time=24:00:00
#SBATCH --output=slurm_%j.out
#SBATCH --error=slurm_%j.err

set -uo pipefail

PYTHON_BIN="/home/rswargo/.conda/envs/fastbiodl/bin/python"
export PATH="/home/rswargo/.conda/envs/fastbiodl/bin:$PATH"

"$PYTHON_BIN" -c "import aiohttp" || { echo "[ERROR] aiohttp missing"; exit 1; }

if [[ $# -lt 1 ]]; then
    echo "Usage: sbatch run_benchmark_max_jobs_gridsearch_expanse.sh ACC1 [ACC2 ...]" >&2
    exit 2
fi

ACCESSIONS=( "$@" )
ACC_TAG=$(IFS=_; echo "${ACCESSIONS[*]}")

REPO_DIR="${SLURM_SUBMIT_DIR:-$PWD}"
cd "$REPO_DIR" || exit 1

# ---------------------------------------------------------------------------
# Storage layout: Lustre / Lustre / NVMe
# ---------------------------------------------------------------------------
LUSTRE_BASE="/expanse/lustre/scratch/$USER/temp_project/fastbiodl_bench/${SLURM_JOB_ID:-local}"
NVME_BASE="/scratch/$USER/job_${SLURM_JOB_ID:-$$}"

if ! mkdir -p "$NVME_BASE" 2>/dev/null; then
    NVME_BASE="/tmp/$USER/job_${SLURM_JOB_ID:-$$}"
    mkdir -p "$NVME_BASE" || { echo "[ERROR] Could not create local scratch"; exit 1; }
fi

mkdir -p "$LUSTRE_BASE" || { echo "[ERROR] Could not create Lustre base: $LUSTRE_BASE"; exit 1; }

SRA_OUT_DIR="${SRA_OUT_DIR:-$LUSTRE_BASE/sra}"
FASTQ_OUT_DIR="${FASTQ_OUT_DIR:-$LUSTRE_BASE/fastq}"
PIGZ_OUT_DIR="${PIGZ_OUT_DIR:-$NVME_BASE/pigz}"
LUSTRE_OUT="$LUSTRE_BASE/results"
WORK_ROOT="$NVME_BASE/work"

mkdir -p "$SRA_OUT_DIR" "$FASTQ_OUT_DIR" "$PIGZ_OUT_DIR" "$LUSTRE_OUT" "$WORK_ROOT"

export LOCAL_SCRATCH="$NVME_BASE"
export GRIDSEARCH_RESULTS_DIR="$LUSTRE_OUT"

JSON_OUT="$LUSTRE_OUT/benchmark_max_jobs_${ACC_TAG}.json"
THREADS="${THREADS:-8}"
REPEATS="${REPEATS:-3}"

echo "=========================================="
echo "Repo:           $REPO_DIR"
echo "Python:         $PYTHON_BIN"
echo "Accessions:     ${ACCESSIONS[*]}"
echo "Repeats:        $REPEATS"
echo "Threads:        $THREADS"
echo "Storage layout: Lustre / Lustre / NVMe"
echo "  SRA   out:    $SRA_OUT_DIR"
echo "  FASTQ out:    $FASTQ_OUT_DIR"
echo "  PIGZ  out:    $PIGZ_OUT_DIR"
echo "Work root:      $WORK_ROOT"
echo "Results:        $LUSTRE_OUT"
echo "=========================================="
which aria2c fasterq-dump pigz || true

"$PYTHON_BIN" benchmark_max_jobs_gridsearch_new.py \
    "${ACCESSIONS[@]}" \
    --repeats "$REPEATS" \
    --threads "$THREADS" \
    --work-root "$WORK_ROOT" \
    --sra-out-dir "$SRA_OUT_DIR" \
    --fastq-out-dir "$FASTQ_OUT_DIR" \
    --pigz-out-dir "$PIGZ_OUT_DIR" \
    --json-out "$JSON_OUT" \
    --cleanup-work-root
status=$?

cp "$REPO_DIR/run_benchmark_max_jobs_gridsearch_expanse_new.sh" "$LUSTRE_OUT/" 2>/dev/null || true
cp "$REPO_DIR/benchmark_max_jobs_gridsearch_new.py" "$LUSTRE_OUT/" 2>/dev/null || true
cp "slurm_${SLURM_JOB_ID}.out" "slurm_${SLURM_JOB_ID}.err" "$LUSTRE_OUT/" 2>/dev/null || true

echo "Benchmark exit code: $status"
echo "Results in $LUSTRE_OUT"
exit "$status"
