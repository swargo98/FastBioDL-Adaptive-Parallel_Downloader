#!/bin/bash
# Run the storage-tier characterization benchmark on SDSC Expanse.
# Mirrors the layout of run_benchmark_max_jobs_gridsearch_expanse_new.sh:
# Lustre (scratch) and NVMe (final) tiers are exercised, plus the Lustre
# to NVMe cross-tier pair which corresponds to the move stage on Expanse.
#
# Usage:
#   sbatch run_disk_io_benchmark_expanse.sh
#
# Optional environment overrides:
#   REPEATS         repeats per (test, tier, numjobs)            (default 3)
#   NUMJOBS         comma-separated numjobs sweep                (default 1,2,4,8,16)
#   SIZE            per-job working set                          (default 4G)
#   RUNTIME         fio runtime per invocation in seconds        (default 30)
#   RAMP            ramp_time skipped before measurement         (default 5)
#   METADATA_FILES  files for the metadata create/unlink test    (default 20000)
#   SKIP            comma-separated tests to skip                (default empty)

#SBATCH --job-name=disk_io_bench
#SBATCH --account=umr115
#SBATCH --partition=compute
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=64
#SBATCH --mem=64G
#SBATCH --time=06:00:00
#SBATCH --output=slurm_%j.out
#SBATCH --error=slurm_%j.err

set -uo pipefail

PYTHON_BIN="/home/rswargo/.conda/envs/fastbiodl/bin/python"
export PATH="/home/rswargo/.conda/envs/fastbiodl/bin:$PATH"

if ! command -v fio >/dev/null 2>&1; then
    echo "[ERROR] fio not on PATH. Install into the conda env or load a fio module." >&2
    exit 1
fi

REPO_DIR="${SLURM_SUBMIT_DIR:-$PWD}"
cd "$REPO_DIR" || exit 1

# ---------------------------------------------------------------------------
# Storage layout (mirrors the SeqFlux Expanse layout exactly).
#   Lustre = scratch tier on Expanse (where SRA + intermediate FASTQ live).
#   NVMe   = node-local final-output tier.
# ---------------------------------------------------------------------------
LUSTRE_BASE="/expanse/lustre/scratch/$USER/temp_project/disk_io_bench/${SLURM_JOB_ID:-local}"
NVME_BASE="/scratch/$USER/job_${SLURM_JOB_ID:-$$}"

if ! mkdir -p "$NVME_BASE" 2>/dev/null; then
    NVME_BASE="/tmp/$USER/job_${SLURM_JOB_ID:-$$}"
    mkdir -p "$NVME_BASE" || { echo "[ERROR] Could not create local scratch"; exit 1; }
fi

mkdir -p "$LUSTRE_BASE" || { echo "[ERROR] Could not create Lustre base: $LUSTRE_BASE"; exit 1; }

LUSTRE_TIER="$LUSTRE_BASE/lustre_tier"
NVME_TIER="$NVME_BASE/nvme_tier"
RESULTS_OUT="$LUSTRE_BASE/results"
LOG_DIR="$LUSTRE_BASE/fio_logs"

mkdir -p "$LUSTRE_TIER" "$NVME_TIER" "$RESULTS_OUT" "$LOG_DIR"

JSON_OUT="$RESULTS_OUT/disk_io_benchmark_expanse.json"

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
echo "Lustre tier:     $LUSTRE_TIER"
echo "NVMe   tier:     $NVME_TIER"
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

"$PYTHON_BIN" disk_io_benchmark.py \
    --tier "lustre=$LUSTRE_TIER" \
    --tier "nvme=$NVME_TIER" \
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

cp "$REPO_DIR/run_disk_io_benchmark_expanse.sh" "$RESULTS_OUT/" 2>/dev/null || true
cp "$REPO_DIR/disk_io_benchmark.py" "$RESULTS_OUT/" 2>/dev/null || true
cp "slurm_${SLURM_JOB_ID}.out" "slurm_${SLURM_JOB_ID}.err" "$RESULTS_OUT/" 2>/dev/null || true

# The fio test files have already been removed by the harness, but the
# directories themselves are left behind as cheap evidence that the run
# happened. Clean them up here.
rm -rf "$LUSTRE_TIER" "$NVME_TIER" 2>/dev/null || true

echo "Benchmark exit code: $status"
echo "Results in $RESULTS_OUT"
exit "$status"
