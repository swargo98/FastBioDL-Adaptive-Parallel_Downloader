#!/usr/bin/env bash
# Run download-only SRA benchmarks on a local lab server.
#
# Storage layout (download-only FFS):
#   SRA download -> NVMe (fast)
#   Results/logs -> HDD/result tier
#
# Loop order (matches run_all_benchmarks.sh semantics):
#   for accession_list in {large, medium, small}:
#     for run_count in 1..3:
#       for tool in {fastbiodl, kingfisher, pysradb, sratools}:
#         download, then clean
#
# Usage:
#   ./run_all_download_benchmarks_ffs_local.sh
#
# Optional environment overrides:
#   PYTHON_BIN          Python interpreter (default: python3)
#   THREADS             workers/connections for kingfisher and pysradb (default 8)
#   FASTBIODL_WORKERS   FastBioDL downloader worker limit (default $THREADS)
#   REPEATS             repeats per (accession_list, tool) (default 3)
#   NVME_MOUNT          fast-tier root (default /)
#   HDD_MOUNT           result/slow-tier root (default /mnt/storage)
#   RESULTS_ROOT        where final logs/results are aggregated
#   ACCESSION_DIR       directory containing accession lists (default $REPO_DIR)
#   SLEEP_BETWEEN       sleep between iterations in seconds (default 60)
#   LOG_INTERVAL        throughput sample interval in seconds (default 1)

set -uo pipefail

PYTHON_BIN="${PYTHON_BIN:-python3}"
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

"$PYTHON_BIN" -c "import aiohttp, requests" || {
    echo "[ERROR] aiohttp or requests missing in $PYTHON_BIN" >&2
    exit 1
}

REPO_DIR="${REPO_DIR:-$PWD}"
cd "$REPO_DIR" || { echo "[ERROR] cannot cd to $REPO_DIR" >&2; exit 1; }

ACCESSION_DIR="${ACCESSION_DIR:-$REPO_DIR}"

NVME_MOUNT="${NVME_MOUNT:-/}"
HDD_MOUNT="${HDD_MOUNT:-/mnt/storage}"
JOB_ID="${JOB_ID:-$$}"

for tier in "$NVME_MOUNT" "$HDD_MOUNT"; do
    if [[ ! -d "$tier" ]]; then
        echo "[ERROR] Storage mount not found: $tier" >&2
        echo "        Set NVME_MOUNT and HDD_MOUNT to the lab server paths." >&2
        exit 1
    fi
done

NVME_BASE="$NVME_MOUNT/$USER/fastbiodl_ffs_download/$JOB_ID"
HDD_BASE="$HDD_MOUNT/$USER/fastbiodl_ffs_download/$JOB_ID"
SRA_OUT_DIR="$NVME_BASE/sra"
WORK_ROOT="$NVME_BASE/work"
RESULTS_OUT="${RESULTS_ROOT:-$HDD_BASE/results}"

mkdir -p "$SRA_OUT_DIR" "$WORK_ROOT" "$RESULTS_OUT" || {
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
echo "Layout            : download-only, SRA on NVMe"
echo "  NVMe mount      : $NVME_MOUNT"
echo "  HDD  mount      : $HDD_MOUNT"
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
        --platform "local" \
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
    "$ACCESSION_DIR/accessions_large_PRJNA251383.txt"
    "$ACCESSION_DIR/accessions_medium_PRJNA353374.txt"
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

cp "$0" "$RESULTS_OUT/" 2>/dev/null || true
cp "$REPO_DIR/benchmark_download_only.py" "$RESULTS_OUT/" 2>/dev/null || true

echo "Total bytes deleted across iterations: $(to_human "$total_deleted_bytes") ($total_deleted_bytes bytes)"
echo "Download-only results aggregated in: $RESULTS_OUT"
exit "$overall_status"
