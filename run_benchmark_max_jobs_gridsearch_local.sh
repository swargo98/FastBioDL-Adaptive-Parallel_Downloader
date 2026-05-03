#!/bin/bash
# Run the multi-accession, multi-repeat benchmark on a local lab server.
# Storage layout: sra_out=NVMe, fastq_out=NVMe, pigz_out=HDD.
#
# Usage:
#   ./run_benchmark_max_jobs_gridsearch_local.sh ACC1 ACC2 ACC3
#
# Optional environment overrides:
#   PYTHON_BIN      Python interpreter (default: python3)
#   THREADS         threads passed to fasterq-dump and pigz (default 8)
#   REPEATS         repeats per (accession, configuration) (default 3)
#   NVME_MOUNT      NVMe mount point (default /mnt/nvme)
#   HDD_MOUNT       HDD  mount point (default /mnt/hdd)
#   SRA_OUT_DIR     override default SRA tier path
#   FASTQ_OUT_DIR   override default FASTQ tier path
#   PIGZ_OUT_DIR    override default pigz tier path
#
# Adjust NVME_MOUNT and HDD_MOUNT to match the actual mount points on the lab
# server. The defaults here are placeholders and will fail fast if those paths
# do not exist.

set -uo pipefail

PYTHON_BIN="${PYTHON_BIN:-python3}"

"$PYTHON_BIN" -c "import aiohttp" || { echo "[ERROR] aiohttp missing in $PYTHON_BIN"; exit 1; }

if [[ $# -lt 1 ]]; then
    echo "Usage: $0 ACC1 [ACC2 ...]" >&2
    exit 2
fi

ACCESSIONS=( "$@" )
ACC_TAG=$(IFS=_; echo "${ACCESSIONS[*]}")

REPO_DIR="${REPO_DIR:-$PWD}"
cd "$REPO_DIR" || exit 1

# ---------------------------------------------------------------------------
# Storage layout: NVMe / NVMe / HDD
# ---------------------------------------------------------------------------
NVME_MOUNT="${NVME_MOUNT:-/mnt/nvme}"
HDD_MOUNT="${HDD_MOUNT:-/mnt/hdd}"
JOB_ID="${JOB_ID:-$$}"

for tier in "$NVME_MOUNT" "$HDD_MOUNT"; do
    if [[ ! -d "$tier" ]]; then
        echo "[ERROR] Storage mount not found: $tier" >&2
        echo "        Set NVME_MOUNT and HDD_MOUNT to the lab server mount paths." >&2
        exit 1
    fi
done

NVME_BASE="$NVME_MOUNT/$USER/fastbiodl_bench/$JOB_ID"
HDD_BASE="$HDD_MOUNT/$USER/fastbiodl_bench/$JOB_ID"

mkdir -p "$NVME_BASE" "$HDD_BASE" || { echo "[ERROR] Could not create base directories"; exit 1; }

SRA_OUT_DIR="${SRA_OUT_DIR:-$NVME_BASE/sra}"
FASTQ_OUT_DIR="${FASTQ_OUT_DIR:-$NVME_BASE/fastq}"
PIGZ_OUT_DIR="${PIGZ_OUT_DIR:-$HDD_BASE/pigz}"
RESULTS_OUT="$HDD_BASE/results"
WORK_ROOT="$NVME_BASE/work"

mkdir -p "$SRA_OUT_DIR" "$FASTQ_OUT_DIR" "$PIGZ_OUT_DIR" "$RESULTS_OUT" "$WORK_ROOT"

export LOCAL_SCRATCH="$NVME_BASE"
export GRIDSEARCH_RESULTS_DIR="$RESULTS_OUT"

JSON_OUT="$RESULTS_OUT/benchmark_max_jobs_${ACC_TAG}.json"
THREADS="${THREADS:-8}"
REPEATS="${REPEATS:-3}"

echo "=========================================="
echo "Repo:           $REPO_DIR"
echo "Python:         $PYTHON_BIN"
echo "Accessions:     ${ACCESSIONS[*]}"
echo "Repeats:        $REPEATS"
echo "Threads:        $THREADS"
echo "Storage layout: NVMe / NVMe / HDD"
echo "  NVMe mount:   $NVME_MOUNT"
echo "  HDD  mount:   $HDD_MOUNT"
echo "  SRA   out:    $SRA_OUT_DIR"
echo "  FASTQ out:    $FASTQ_OUT_DIR"
echo "  PIGZ  out:    $PIGZ_OUT_DIR"
echo "Work root:      $WORK_ROOT"
echo "Results:        $RESULTS_OUT"
echo "=========================================="
which aria2c fasterq-dump pigz || true

"$PYTHON_BIN" benchmark_max_jobs_gridsearch.py \
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

cp "$REPO_DIR/run_benchmark_max_jobs_gridsearch_local.sh" "$RESULTS_OUT/" 2>/dev/null || true
cp "$REPO_DIR/benchmark_max_jobs_gridsearch.py" "$RESULTS_OUT/" 2>/dev/null || true

echo "Benchmark exit code: $status"
echo "Results in $RESULTS_OUT"
exit "$status"
