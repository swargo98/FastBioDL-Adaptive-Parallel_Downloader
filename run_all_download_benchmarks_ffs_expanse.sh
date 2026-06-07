#!/bin/bash
# Run download-only SRA benchmarks on SDSC Expanse.
#
# Storage layout (download-only FFS, matching the Expanse FFS script):
#   SRA download -> Lustre
#   FastBioDL work -> node-local scratch
#   Results/logs -> Lustre results directory
#
# Loop order (matches run_all_benchmarks.sh semantics):
#   for accession_list in {large, medium, small}:
#     for run_count in 1..3:
#       for tool in {fastbiodl, kingfisher, pysradb, sratools}:
#         download, then clean
#
# Usage:
#   sbatch run_all_download_benchmarks_ffs_expanse.sh

#SBATCH --job-name=dl_ffs_bench
#SBATCH --account=umr115
#SBATCH --partition=compute
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=64
#SBATCH --mem=64G
#SBATCH --time=48:00:00
#SBATCH --output=slurm_%j.out
#SBATCH --error=slurm_%j.err

set -uo pipefail

export PS1="${PS1:-}"

module purge
module load slurm cpu/0.17.3b anaconda3/2021.05

CONDA_BASE="$(conda info --base 2>/dev/null || true)"
if [[ -z "$CONDA_BASE" || ! -f "$CONDA_BASE/etc/profile.d/conda.sh" ]]; then
    echo "[ERROR] Could not locate conda.sh after loading the anaconda module." >&2
    exit 1
fi
# shellcheck disable=SC1091
source "$CONDA_BASE/etc/profile.d/conda.sh"
conda activate fastbiodl

export PATH="${CONDA_PREFIX}/bin:$PATH"

REPO_DIR="${SLURM_SUBMIT_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)}"
cd "$REPO_DIR" || exit 1

export PATH="$REPO_DIR/sratoolkit.3.1.0-ubuntu64/bin:$PATH"

PYTHON_BIN="${CONDA_PREFIX}/bin/python3"
THREADS="${THREADS:-8}"
FASTBIODL_WORKERS="${FASTBIODL_WORKERS:-20}"
REPEATS="${REPEATS:-3}"
SLEEP_BETWEEN="${SLEEP_BETWEEN:-60}"
LOG_INTERVAL="${LOG_INTERVAL:-1}"
LOOKUP_WORKERS="${LOOKUP_WORKERS:-3}"
SEGMENT_SIZE_MB="${SEGMENT_SIZE_MB:-512}"
MAX_SEGMENTS="${MAX_SEGMENTS:-8}"
MAX_RETRIES="${MAX_RETRIES:-3}"
PROBING_SEC="${PROBING_SEC:-5}"
PREFETCH_MAX_SIZE="${PREFETCH_MAX_SIZE:-100G}"
REQUEST_TIMEOUT="${REQUEST_TIMEOUT:-1200}"
CHUNK_SIZE="${CHUNK_SIZE:-4194304}"

if ! "$PYTHON_BIN" -c "import aiohttp, requests" >/dev/null 2>&1; then
    echo "[ERROR] aiohttp or requests not importable from $PYTHON_BIN. Conda env activation failed." >&2
    exit 1
fi

ACCESSION_DIR="${ACCESSION_DIR:-$REPO_DIR}"
LUSTRE_BASE="/expanse/lustre/scratch/$USER/temp_project/fastbiodl_ffs_download/${SLURM_JOB_ID:-local}"

pick_local_scratch() {
    local candidate

    if [[ -n "${LOCAL_SCRATCH_OVERRIDE:-}" ]]; then
        candidate="${LOCAL_SCRATCH_OVERRIDE}"
        if mkdir -p "$candidate" 2>/dev/null; then
            echo "$candidate"; return 0
        fi
    fi

    if [[ -n "${SLURM_TMPDIR:-}" ]]; then
        candidate="${SLURM_TMPDIR}"
        if mkdir -p "$candidate" 2>/dev/null; then
            echo "$candidate"; return 0
        fi
    fi

    for candidate in "/scratch/$USER/job_${SLURM_JOB_ID:-$$}" "/tmp/$USER/job_${SLURM_JOB_ID:-$$}"; do
        if mkdir -p "$candidate" 2>/dev/null; then
            echo "$candidate"; return 0
        fi
    done

    return 1
}

NVME_BASE="$(pick_local_scratch)" || {
    echo "[ERROR] Unable to create a writable local scratch directory." >&2
    exit 1
}

SRA_OUT_DIR="$LUSTRE_BASE/sra"
WORK_ROOT="$NVME_BASE/work"
RESULTS_ROOT="${RESULTS_ROOT:-$(dirname "$REPO_DIR")/benchmark_results}"
RESULTS_OUT="$RESULTS_ROOT/run_all_download_benchmarks_ffs_${SLURM_JOB_ID:-local}"

mkdir -p "$SRA_OUT_DIR" "$WORK_ROOT" "$RESULTS_OUT" logs || {
    echo "[ERROR] could not create benchmark dirs" >&2
    exit 1
}

export LOCAL_SCRATCH="$WORK_ROOT"

echo "=========================================="
echo "Repo              : $REPO_DIR"
echo "Python            : $PYTHON_BIN"
echo "Threads           : $THREADS"
echo "FastBioDL workers : $FASTBIODL_WORKERS"
echo "Repeats           : $REPEATS"
echo "Layout            : download-only, SRA on Lustre"
echo "  SRA download    : $SRA_OUT_DIR"
echo "  Work            : $WORK_ROOT"
echo "  Results         : $RESULTS_OUT"
echo "=========================================="
which aria2c prefetch 2>/dev/null || true

to_human() {
    local bytes="$1"
    if command -v numfmt >/dev/null 2>&1; then
        numfmt --to=iec --suffix=B "$bytes"
    else
        echo "${bytes}B"
    fi
}

cleanup_dirs() {
    local total=0
    for d in "$SRA_OUT_DIR" "$WORK_ROOT"; do
        if [[ -d "$d" ]]; then
            local sz
            sz=$(du -sb "$d" 2>/dev/null | awk '{print $1}')
            sz=${sz:-0}
            total=$((total + sz))
            find "$d" -mindepth 1 -maxdepth 1 -exec rm -rf -- {} + 2>/dev/null || true
        fi
    done
    echo "$total"
}

invoke_download() {
    local tool="$1"
    local acc_file="$2"
    local acc_tag="$3"
    local run="$4"

    "$PYTHON_BIN" "$REPO_DIR/benchmark_download_only.py" \
        -i "$acc_file" \
        --tool "$tool" \
        --sra-dir "$SRA_OUT_DIR" \
        --results-dir "$RESULTS_OUT" \
        --platform "expanse" \
        --accession-tag "$acc_tag" \
        --run-index "$run" \
        --threads "$THREADS" \
        --fastbiodl-workers "$FASTBIODL_WORKERS" \
        --lookup-workers "$LOOKUP_WORKERS" \
        --log-interval "$LOG_INTERVAL" \
        --segment-size-mb "$SEGMENT_SIZE_MB" \
        --max-segments "$MAX_SEGMENTS" \
        --max-retries "$MAX_RETRIES" \
        --probing-sec "$PROBING_SEC" \
        --prefetch-max-size "$PREFETCH_MAX_SIZE" \
        --request-timeout "$REQUEST_TIMEOUT" \
        --chunk-size "$CHUNK_SIZE"
}

accession_lists=(
    "$ACCESSION_DIR/accessions_small_PRJNA916347.txt"
)

tools=( "fastbiodl")

total_deleted_bytes=0
overall_status=0

for acc_file in "${accession_lists[@]}"; do
    if [[ ! -f "$acc_file" ]]; then
        echo "[ERROR] Missing accession list: $acc_file" >&2
        overall_status=1
        continue
    fi
    acc_tag="$(basename "$acc_file" .txt)"

    for ((run=1; run<=REPEATS; run++)); do
        for tool in "${tools[@]}"; do
            echo "============================================================"
            echo "[$(date '+%F %T')] download-only acc=$acc_tag  tool=$tool  run=$run/$REPEATS"
            echo "============================================================"

            invoke_download "$tool" "$acc_file" "$acc_tag" "$run"
            ec=$?
            if (( ec != 0 )); then
                echo "[WARN] $tool download with $acc_tag (run $run) exited rc=$ec"
                overall_status=$ec
            fi

            cleaned=$(cleanup_dirs)
            total_deleted_bytes=$((total_deleted_bytes + cleaned))
            echo "[clean] reclaimed $(to_human "$cleaned") after $tool download run $run"

            sleep "$SLEEP_BETWEEN"
        done
    done
done

cp "$REPO_DIR/run_all_download_benchmarks_ffs_expanse.sh" "$RESULTS_OUT/" 2>/dev/null || true
cp "$REPO_DIR/benchmark_download_only.py" "$RESULTS_OUT/" 2>/dev/null || true
if [[ -n "${SLURM_JOB_ID:-}" && -f "slurm_${SLURM_JOB_ID}.out" ]]; then
    cp "slurm_${SLURM_JOB_ID}.out" "$RESULTS_OUT/" 2>/dev/null || true
fi
if [[ -n "${SLURM_JOB_ID:-}" && -f "slurm_${SLURM_JOB_ID}.err" ]]; then
    cp "slurm_${SLURM_JOB_ID}.err" "$RESULTS_OUT/" 2>/dev/null || true
fi

echo "Total bytes deleted across iterations: $(to_human "$total_deleted_bytes") ($total_deleted_bytes bytes)"
echo "Download-only results aggregated in: $RESULTS_OUT"
exit "$overall_status"
