#!/usr/bin/env bash
# Run the four end-to-end SRA benchmarks on a FABRIC testbed slice.
#
# Storage layout (FFS, Fast/Fast/Slow):
#   SRA download    -> NVMe  (fast)
#   FASTQ stage     -> NVMe  (fast)
#   pigz output     -> HDD   (slow, final destination)
#
# Loop order (matches run_all_benchmarks.sh semantics):
#   for accession_list in {large, medium, small}:
#     for run_count in 1..3:
#       for tool in {fastbiodl, kingfisher, pysradb, sratools}:
#         run tool with FFS-pinned directories, then clean
#
# Usage:
#   ./run_all_benchmarks_ffs_fabric.sh
#
# Optional environment overrides:
#   PYTHON_BIN     Python interpreter (default: python3)
#   THREADS        threads for fasterq-dump / pigz / tool concurrency (default 8)
#   REPEATS        repeats per (accession_list, tool) (default 3)
#   NVME_MOUNT     fast-tier root (default /mnt/raid0)
#   HDD_MOUNT      slow-tier root (default /home/ubuntu/FastBioDL-Adaptive-Parallel_Downloader)
#   RESULTS_ROOT   where final logs/results are aggregated
#   ACCESSION_DIR  directory containing accession lists (default $REPO_DIR)
#   SLEEP_BETWEEN  sleep between iterations in seconds (default 60)

set -uo pipefail

PYTHON_BIN="${PYTHON_BIN:-python3}"
THREADS="${THREADS:-8}"
REPEATS="${REPEATS:-3}"
SLEEP_BETWEEN="${SLEEP_BETWEEN:-60}"

"$PYTHON_BIN" -c "import aiohttp" || { echo "[ERROR] aiohttp missing in $PYTHON_BIN" >&2; exit 1; }

REPO_DIR="${REPO_DIR:-$PWD}"
cd "$REPO_DIR" || { echo "[ERROR] cannot cd to $REPO_DIR" >&2; exit 1; }

ACCESSION_DIR="${ACCESSION_DIR:-$REPO_DIR}"

# --- Storage layout: NVMe / NVMe / HDD --------------------------------------
NVME_MOUNT="${NVME_MOUNT:-/mnt/raid0}"
HDD_MOUNT="${HDD_MOUNT:-/home/ubuntu/FastBioDL-Adaptive-Parallel_Downloader}"
JOB_ID="${JOB_ID:-$$}"

for tier in "$NVME_MOUNT" "$HDD_MOUNT"; do
    if [[ ! -d "$tier" ]]; then
        echo "[ERROR] Storage mount not found: $tier" >&2
        echo "        Set NVME_MOUNT and HDD_MOUNT to the Fabric slice paths." >&2
        exit 1
    fi
done

NVME_BASE="$NVME_MOUNT/$USER/fastbiodl_ffs/$JOB_ID"
HDD_BASE="$HDD_MOUNT/$USER/fastbiodl_ffs/$JOB_ID"
mkdir -p "$NVME_BASE" "$HDD_BASE" || { echo "[ERROR] could not create base dirs" >&2; exit 1; }

SRA_OUT_DIR="$NVME_BASE/sra"
FASTQ_OUT_DIR="$NVME_BASE/fastq"
PIGZ_OUT_DIR="$HDD_BASE/pigz"
RESULTS_OUT="${RESULTS_ROOT:-$HDD_BASE/results}"
WORK_ROOT="$NVME_BASE/work"

mkdir -p "$SRA_OUT_DIR" "$FASTQ_OUT_DIR" "$PIGZ_OUT_DIR" "$RESULTS_OUT" "$WORK_ROOT"

export LOCAL_SCRATCH="$WORK_ROOT"

echo "=========================================="
echo "Repo            : $REPO_DIR"
echo "Python          : $PYTHON_BIN"
echo "Threads         : $THREADS"
echo "Repeats         : $REPEATS"
echo "Layout (FFS)    : NVMe / NVMe / HDD"
echo "  NVMe mount    : $NVME_MOUNT"
echo "  HDD  mount    : $HDD_MOUNT"
echo "  SRA   (fast)  : $SRA_OUT_DIR"
echo "  FASTQ (fast)  : $FASTQ_OUT_DIR"
echo "  PIGZ  (slow)  : $PIGZ_OUT_DIR"
echo "  Work  (fast)  : $WORK_ROOT"
echo "  Results       : $RESULTS_OUT"
echo "=========================================="
which aria2c fasterq-dump pigz prefetch 2>/dev/null || true

invoke_fastbiodl() {
    local acc_file="$1"
    "$PYTHON_BIN" "$REPO_DIR/fastbiodl_upgrade.py" \
        -i "$acc_file" \
        --sra-dir "$SRA_OUT_DIR" \
        --fastq-dir "$FASTQ_OUT_DIR" \
        --out-dir "$PIGZ_OUT_DIR"
}

invoke_kingfisher() {
    local acc_file="$1"
    "$PYTHON_BIN" "$REPO_DIR/benchmark_aria2c_kingfisher.py" \
        -i "$acc_file" \
        --sra-dir "$SRA_OUT_DIR" \
        --fastq-dir "$FASTQ_OUT_DIR" \
        --out-dir "$PIGZ_OUT_DIR" \
        --threads "$THREADS"
}

invoke_pysradb() {
    local acc_file="$1"
    "$PYTHON_BIN" "$REPO_DIR/benchmark_pysradb.py" \
        -i "$acc_file" \
        --sra-dir "$SRA_OUT_DIR" \
        --fastq-dir "$FASTQ_OUT_DIR" \
        --out-dir "$PIGZ_OUT_DIR" \
        --t "$THREADS" \
        --threads "$THREADS"
}

invoke_sratools() {
    local acc_file="$1"
    "$PYTHON_BIN" "$REPO_DIR/benchmark_sratools.py" \
        -i "$acc_file" \
        --sra-dir "$SRA_OUT_DIR" \
        --fastq-dir "$FASTQ_OUT_DIR" \
        --out-dir "$PIGZ_OUT_DIR" \
        --threads "$THREADS"
}

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
    for d in "$SRA_OUT_DIR" "$FASTQ_OUT_DIR" "$PIGZ_OUT_DIR" "$WORK_ROOT"; do
        if [[ -d "$d" ]]; then
            local sz
            sz=$(du -sb "$d" 2>/dev/null | awk '{print $1}')
            sz=${sz:-0}
            total=$((total + sz))
            rm -rf "$d"/* 2>/dev/null || true
        fi
    done
    echo "$total"
}

accession_lists=(
    # "$ACCESSION_DIR/accessions_large_PRJNA251383.txt"
    # "$ACCESSION_DIR/accessions_medium_PRJNA353374.txt"
    "$ACCESSION_DIR/accessions_small_PRJNA916347.txt"
)

tools=( "fastbiodl" "kingfisher" "pysradb" "sratools" )

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
            echo "[$(date '+%F %T')] acc=$acc_tag  tool=$tool  run=$run/$REPEATS"
            echo "============================================================"

            invoke_$tool "$acc_file"
            ec=$?
            if (( ec != 0 )); then
                echo "[WARN] $tool with $acc_tag (run $run) exited rc=$ec"
                overall_status=$ec
            fi

            cleaned=$(cleanup_dirs)
            total_deleted_bytes=$((total_deleted_bytes + cleaned))
            echo "[clean] reclaimed $(to_human "$cleaned") after $tool run $run"

            sleep "$SLEEP_BETWEEN"
        done
    done
done

echo "============================================================"
echo "Aggregating logs into $RESULTS_OUT"
echo "============================================================"
if [[ -d "$REPO_DIR/logs" ]]; then
    cp -r "$REPO_DIR/logs" "$RESULTS_OUT/" 2>/dev/null || true
fi
cp "$0" "$RESULTS_OUT/" 2>/dev/null || true

echo "Total bytes deleted across iterations: $(to_human "$total_deleted_bytes") ($total_deleted_bytes bytes)"
echo "Results aggregated in: $RESULTS_OUT"
exit "$overall_status"
