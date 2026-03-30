#!/bin/bash
#SBATCH --job-name=fastbiodl_benchmark
#SBATCH --account=umr115         # e.g. abc123 or TG-ABC123456
#SBATCH --partition=compute               # exclusive node, 128 cores, 1 TB NVMe
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=16               # adjust based on your --threads setting
#SBATCH --mem=64G
#SBATCH --time=08:00:00                  # 8 hours; adjust per dataset size
#SBATCH --output=logs/slurm_%j.out
#SBATCH --error=logs/slurm_%j.err

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
PYTHON_BIN="$(command -v python)"

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
LUSTRE_OUT="${LUSTRE_OUT:-$(dirname "$REPO_DIR")/benchmark_results}"
mkdir -p "$LUSTRE_OUT"
mkdir -p logs/fastbiodl logs/kingfisher

# Export so the Python code can pick them up
export LOCAL_SCRATCH="$NVME_DIR"
export SLURM_JOB_ID="$SLURM_JOB_ID"

echo "=== Environment ==="
echo "Repository: $REPO_DIR"
echo "Python: $PYTHON_BIN"
echo "Local scratch: $NVME_DIR"
echo "Results dir: $LUSTRE_OUT"

# ─── Optional: your NCBI credentials for higher rate limits ────────────────
# export NCBI_EMAIL="your@email.com"
# export NCBI_API_KEY="your_key"

# ─── Run: FastBioDL medium ─────────────────────────────────────────────────
echo "=== FastBioDL medium ==="
"$PYTHON_BIN" fastbiodl_upgrade.py \
    -i accessions_medium_PRJNA353374.txt \
    -o "$LUSTRE_OUT/fastbiodl_medium" \
    --segment-size 512 \
    --max-segments 8 \
    --max-retries 3

# ─── Run: aria2c iterative medium ──────────────────────────────────────────
echo "=== aria2c iterative medium ==="
mkdir -p "$NVME_DIR/aria2c_medium/sra" \
         "$NVME_DIR/aria2c_medium/fastq"
mkdir -p "$LUSTRE_OUT/aria2c_medium"

"$PYTHON_BIN" benchmark_aria2c_kingfisher.py \
    -i accessions_medium_PRJNA353374.txt \
    --sra-dir  "$NVME_DIR/aria2c_medium/sra" \
    --fastq-dir "$NVME_DIR/aria2c_medium/fastq" \
    --out-dir  "$LUSTRE_OUT/aria2c_medium" \
    --threads 16

# ─── Copy all JSON results and logs to Lustre ──────────────────────────────
echo "=== Copying results to Lustre ==="
cp -r logs/ "$LUSTRE_OUT/"

echo "=== Done. Results in $LUSTRE_OUT ==="
