#!/bin/bash
# Run the four end-to-end SRA benchmarks on SDSC Expanse.
#
# Storage layout (FFS, per user configuration on Expanse):
#   SRA download    -> Lustre (fast network FS, --sra-dir)
#   FASTQ stage     -> Lustre (fast network FS, --fastq-dir)
#   pigz output     -> NVMe   (node-local scratch, --out-dir; "slow" tier
#                              in the FFS taxonomy because it is the durable
#                              destination for compressed outputs).
#
# Note on the F/F/S naming on Expanse: NVMe is typically faster than Lustre,
# but this script honors the requested layout where pigz output is staged on
# node-local NVMe so that final artifacts remain on the compute node for the
# duration of the job. Final results are copied off to Lustre at job end.
#
# Loop order:
#   for accession_list in {large, medium, small}:
#     for run_count in 1..3:
#       for tool in {fastbiodl, kingfisher, pysradb, sratools}:
#         run tool with FFS-pinned directories, then clean
#
# Usage:
#   sbatch run_all_benchmarks_ffs_expanse.sh

#SBATCH --job-name=all_ffs_bench
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

# --- Environment -----------------------------------------------------------
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

# Add sra-toolkit to PATH (matches the existing run_all_benchmarks_expanse.sh)
export PATH="$REPO_DIR/sratoolkit.3.1.0-ubuntu64/bin:$PATH"

PYTHON_BIN="${CONDA_PREFIX}/bin/python3"
THREADS="${THREADS:-8}"
REPEATS="${REPEATS:-3}"
SLEEP_BETWEEN="${SLEEP_BETWEEN:-60}"

if ! "$PYTHON_BIN" -c "import aiohttp" >/dev/null 2>&1; then
    echo "[ERROR] aiohttp not importable from $PYTHON_BIN. Conda env activation failed." >&2
    exit 1
fi

ACCESSION_DIR="${ACCESSION_DIR:-$REPO_DIR}"

# --- Storage layout: Lustre (fast) / Lustre (fast) / NVMe (slow) -----------
LUSTRE_BASE="/expanse/lustre/scratch/$USER/temp_project/fastbiodl_ffs/${SLURM_JOB_ID:-local}"

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

mkdir -p "$LUSTRE_BASE" || { echo "[ERROR] could not create $LUSTRE_BASE" >&2; exit 1; }

SRA_OUT_DIR="$LUSTRE_BASE/sra"      # fast tier (per FFS labeling on Expanse)
FASTQ_OUT_DIR="$LUSTRE_BASE/fastq"  # fast tier
PIGZ_OUT_DIR="$NVME_BASE/pigz"      # slow tier (node-local scratch)
WORK_ROOT="$NVME_BASE/work"         # fastbiodl converter scratch (fast tier device, kept local)

RESULTS_ROOT="${RESULTS_ROOT:-$(dirname "$REPO_DIR")/benchmark_results}"
RESULTS_OUT="$RESULTS_ROOT/run_all_benchmarks_ffs_${SLURM_JOB_ID:-local}"

mkdir -p "$SRA_OUT_DIR" "$FASTQ_OUT_DIR" "$PIGZ_OUT_DIR" "$WORK_ROOT" "$RESULTS_OUT" logs

# fastbiodl converter scratch comes from LOCAL_SCRATCH; keep it on node-local NVMe
# so the converter's fast-path uses NVMe even though the SRA / FASTQ staging
# directories pinned for the other tools live on Lustre. fastbiodl will then
# write .fastq.gz directly to PIGZ_OUT_DIR (slow tier) via the new
# compressed_output_dir parameter wired in converter.py.
export LOCAL_SCRATCH="$WORK_ROOT"

echo "=========================================="
echo "Repo            : $REPO_DIR"
echo "Python          : $PYTHON_BIN"
echo "Threads         : $THREADS"
echo "Repeats         : $REPEATS"
echo "Layout (FFS)    : Lustre / Lustre / NVMe"
echo "  SRA   (fast)  : $SRA_OUT_DIR"
echo "  FASTQ (fast)  : $FASTQ_OUT_DIR"
echo "  PIGZ  (slow)  : $PIGZ_OUT_DIR"
echo "  Work  (NVMe)  : $WORK_ROOT"
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
    "$ACCESSION_DIR/accessions_large_PRJNA251383.txt"
    "$ACCESSION_DIR/accessions_medium_PRJNA353374.txt"
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
echo "Aggregating logs and run metadata into $RESULTS_OUT"
echo "============================================================"
if [[ -d "$REPO_DIR/logs" ]]; then
    cp -r "$REPO_DIR/logs" "$RESULTS_OUT/" 2>/dev/null || true
fi
cp "$REPO_DIR/run_all_benchmarks_ffs_expanse.sh" "$RESULTS_OUT/" 2>/dev/null || true
if [[ -f "slurm_${SLURM_JOB_ID}.out" ]]; then
    cp "slurm_${SLURM_JOB_ID}.out" "$RESULTS_OUT/" 2>/dev/null || true
fi
if [[ -f "slurm_${SLURM_JOB_ID}.err" ]]; then
    cp "slurm_${SLURM_JOB_ID}.err" "$RESULTS_OUT/" 2>/dev/null || true
fi

echo "Total bytes deleted across iterations: $(to_human "$total_deleted_bytes") ($total_deleted_bytes bytes)"
echo "Results aggregated in: $RESULTS_OUT"
exit "$overall_status"
